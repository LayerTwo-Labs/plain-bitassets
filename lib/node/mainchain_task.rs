//! Task to communicate with mainchain node

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bitcoin::{self, hashes::Hash as _};
use futures::{
    StreamExt,
    channel::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
};
use sneed::{EnvError, RwTxnError};
use thiserror::Error;
use tokio::{
    spawn,
    task::{self, JoinHandle},
};

use crate::{
    archive::{self, Archive},
    types::proto::{self, mainchain},
};

/// Request data from the mainchain node
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum Request {
    /// Request missing mainchain ancestor header/infos
    AncestorInfos(bitcoin::BlockHash),
}

/// Error included in a response
#[derive(Debug, Error)]
pub enum ResponseError {
    #[error("Archive error")]
    Archive(#[from] archive::Error),
    #[error("Database env error")]
    DbEnv(#[from] EnvError),
    #[error("Database write error")]
    DbWrite(#[from] sneed::rwtxn::Error),
    #[error("CUSF Mainchain proto error")]
    Mainchain(#[from] proto::Error),
    #[error("Mainchain block {0} was not available")]
    MainchainBlockUnavailable(bitcoin::BlockHash),
    #[error(
        "Mainchain ancestor response for {0} changed while it was being synchronized"
    )]
    InconsistentMainchainAncestors(bitcoin::BlockHash),
}

pub(super) type Event = Result<mainchain::Event, ResponseError>;

/// Response indicating that a request has been fulfilled
#[derive(Debug)]
pub(super) enum Response {
    /// Response bool indicates if the requested header was available
    AncestorInfos(bitcoin::BlockHash, Result<bool, ResponseError>),
}

impl From<&Response> for Request {
    fn from(resp: &Response) -> Self {
        match resp {
            Response::AncestorInfos(block_hash, _) => {
                Request::AncestorInfos(*block_hash)
            }
        }
    }
}

#[derive(Debug, Error)]
enum Error {
    #[error("CUSF mainchain event subscription error")]
    SubscribeEvents(#[source] proto::Error),
    #[error("CUSF mainchain event stream closed")]
    EventStreamClosed,
    #[error("Send mainchain event error")]
    SendEvent,
    #[error("Send response error")]
    SendResponse(Box<Response>),
    #[error("Send response error (oneshot)")]
    SendResponseOneshot(Box<Response>),
}

struct MainchainTask<Transport = tonic::transport::Channel> {
    env: sneed::Env,
    archive: Archive,
    mainchain: proto::mainchain::ValidatorClient<Transport>,
    // receive a request, and optional oneshot sender to send the result to
    // instead of sending on `response_tx`
    request_rx: UnboundedReceiver<(Request, Option<oneshot::Sender<Response>>)>,
    response_tx: UnboundedSender<Response>,
    event_tx: UnboundedSender<Event>,
}

impl<Transport> MainchainTask<Transport>
where
    Transport: proto::Transport,
    proto::mainchain::ValidatorClient<Transport>: Clone,
{
    /// Request ancestor header info and block info from the mainchain node,
    /// including the specified header.
    /// Returns `false` if the specified block was not available.
    async fn request_ancestor_infos(
        env: &sneed::Env,
        archive: &Archive,
        cusf_mainchain: &mut proto::mainchain::ValidatorClient<Transport>,
        block_hash: bitcoin::BlockHash,
    ) -> Result<bool, ResponseError> {
        if block_hash == bitcoin::BlockHash::all_zeros() {
            return Ok(true);
        } else {
            let rotxn = env.read_txn().map_err(EnvError::from)?;
            if archive
                .try_get_main_header_info(&rotxn, &block_hash)?
                .is_some()
            {
                return Ok(true);
            }
        }
        #[derive(Clone, Copy)]
        struct AncestorRange {
            newest: bitcoin::BlockHash,
            exclusive_parent: bitcoin::BlockHash,
            len: usize,
        }

        let mut current_block_hash = block_hash;
        let mut current_height = None;
        // Discover bounded batch ranges first. Keeping only two hashes and a
        // length per range prevents initial sync from retaining every L1
        // BlockInfo while still allowing parent-first archive writes.
        let mut ranges = Vec::<AncestorRange>::new();
        tracing::debug!(%block_hash, "requesting ancestor headers/info");
        const LOG_PROGRESS_INTERVAL: Duration = Duration::from_secs(5);
        const BATCH_REQUEST_SIZE: u32 = 1000;
        let mut progress_logged = Instant::now();
        let mut range_newest = current_block_hash;
        let mut range_len = 0_usize;
        loop {
            if let Some(current_height) = current_height {
                let now = Instant::now();
                if now.duration_since(progress_logged) >= LOG_PROGRESS_INTERVAL
                {
                    progress_logged = now;
                    tracing::debug!(
                        %block_hash,
                        "requesting ancestor headers: {current_block_hash}({current_height} remaining)");
                }
                tracing::trace!(%block_hash, "requesting ancestor headers: {current_block_hash}({current_height})")
            }
            let remaining_in_range = BATCH_REQUEST_SIZE as usize - range_len;
            let Some(header_infos_resp) = cusf_mainchain
                .get_block_header_infos(
                    current_block_hash,
                    u32::try_from(remaining_in_range - 1)
                        .expect("range capacity is at most 1000"),
                )
                .await?
            else {
                return Ok(false);
            };
            let response_len = 1 + header_infos_resp.tail.len();
            if header_infos_resp.head.block_hash != current_block_hash
                || response_len > remaining_in_range
            {
                return Err(ResponseError::InconsistentMainchainAncestors(
                    block_hash,
                ));
            }
            range_len += response_len;
            let oldest_header = header_infos_resp.last();
            let exclusive_parent = oldest_header.prev_block_hash;
            current_block_hash = exclusive_parent;
            current_height = oldest_header.height.checked_sub(1);
            let reached_known_ancestor =
                if current_block_hash == bitcoin::BlockHash::all_zeros() {
                    true
                } else {
                    let rotxn = env.read_txn().map_err(EnvError::from)?;
                    archive
                        .try_get_main_header_info(&rotxn, &current_block_hash)?
                        .is_some()
                };
            if range_len == BATCH_REQUEST_SIZE as usize
                || reached_known_ancestor
            {
                ranges.push(AncestorRange {
                    newest: range_newest,
                    exclusive_parent,
                    len: range_len,
                });
                range_newest = current_block_hash;
                range_len = 0;
            }
            if reached_known_ancestor {
                break;
            } else {
                debug_assert!(range_len < BATCH_REQUEST_SIZE as usize);
            }
        }
        // Refetch and commit each bounded range, oldest first. A server may
        // return fewer ancestors than requested, so fill the discovered range
        // with as many bounded subrequests as necessary.
        tracing::trace!(%block_hash, "storing ancestor headers/info");
        for range in ranges.into_iter().rev() {
            let mut current_block_hash = range.newest;
            let mut block_infos = Vec::with_capacity(range.len);
            while block_infos.len() < range.len {
                let remaining = range.len - block_infos.len();
                let max_ancestors = u32::try_from(remaining - 1)
                    .expect("discovered range length is at most 1000");
                let Some(block_infos_resp) = cusf_mainchain
                    .get_block_infos(current_block_hash, max_ancestors)
                    .await?
                else {
                    return Ok(false);
                };
                if block_infos_resp.head.0.block_hash != current_block_hash
                    || block_infos_resp.tail.len() + 1 > remaining
                {
                    return Err(ResponseError::InconsistentMainchainAncestors(
                        block_hash,
                    ));
                }
                let (oldest_header, _) = block_infos_resp.last();
                current_block_hash = oldest_header.prev_block_hash;
                block_infos.extend(block_infos_resp);
                if current_block_hash == range.exclusive_parent {
                    if block_infos.len() != range.len {
                        return Err(
                            ResponseError::InconsistentMainchainAncestors(
                                block_hash,
                            ),
                        );
                    }
                    break;
                }
                if block_infos.len() == range.len
                    || current_block_hash == bitcoin::BlockHash::all_zeros()
                {
                    return Err(ResponseError::InconsistentMainchainAncestors(
                        block_hash,
                    ));
                }
            }
            task::block_in_place(|| {
                let mut rwtxn = env.write_txn().map_err(EnvError::from)?;
                for (header_info, block_info) in block_infos.into_iter().rev() {
                    archive.put_main_header_info(&mut rwtxn, &header_info)?;
                    archive.put_main_block_info(
                        &mut rwtxn,
                        header_info.block_hash,
                        &block_info,
                    )?;
                }
                rwtxn.commit().map_err(RwTxnError::from)?;
                Ok::<_, ResponseError>(())
            })?;
        }
        tracing::trace!(%block_hash, "stored ancestor headers/info");
        Ok(true)
    }

    async fn prepare_event(
        &mut self,
        event: mainchain::Event,
    ) -> Result<mainchain::Event, ResponseError> {
        match &event {
            mainchain::Event::ConnectBlock {
                header_info,
                block_info,
            } => {
                let parent_available = Self::request_ancestor_infos(
                    &self.env,
                    &self.archive,
                    &mut self.mainchain,
                    header_info.prev_block_hash,
                )
                .await?;
                if !parent_available {
                    return Err(ResponseError::MainchainBlockUnavailable(
                        header_info.prev_block_hash,
                    ));
                }
                task::block_in_place(|| {
                    let mut rwtxn =
                        self.env.write_txn().map_err(EnvError::from)?;
                    self.archive
                        .put_main_header_info(&mut rwtxn, header_info)?;
                    self.archive.put_main_block_info(
                        &mut rwtxn,
                        header_info.block_hash,
                        block_info,
                    )?;
                    rwtxn.commit().map_err(RwTxnError::from)?;
                    Result::<(), ResponseError>::Ok(())
                })?;
            }
            mainchain::Event::DisconnectBlock { block_hash } => {
                // Keep disconnected mainchain blocks in the archive. Their
                // header is needed to identify the surviving parent, and the
                // retained fork data may become canonical again later.
                let block_available = Self::request_ancestor_infos(
                    &self.env,
                    &self.archive,
                    &mut self.mainchain,
                    *block_hash,
                )
                .await?;
                if !block_available {
                    return Err(ResponseError::MainchainBlockUnavailable(
                        *block_hash,
                    ));
                }
            }
        }
        Ok(event)
    }

    async fn current_tip_event(
        &mut self,
    ) -> Result<mainchain::Event, ResponseError> {
        let header_info = self.mainchain.get_chain_tip().await?;
        let block_available = Self::request_ancestor_infos(
            &self.env,
            &self.archive,
            &mut self.mainchain,
            header_info.block_hash,
        )
        .await?;
        if !block_available {
            return Err(ResponseError::MainchainBlockUnavailable(
                header_info.block_hash,
            ));
        }
        let block_info = task::block_in_place(|| {
            let rotxn = self.env.read_txn().map_err(EnvError::from)?;
            self.archive
                .get_main_block_info(&rotxn, &header_info.block_hash)
                .map_err(ResponseError::from)
        })?;
        Ok(mainchain::Event::ConnectBlock {
            header_info,
            block_info,
        })
    }

    async fn run(mut self) -> Result<(), Error> {
        let mut event_client = self.mainchain.clone();
        let mut events = event_client
            .subscribe_events()
            .await
            .map_err(Error::SubscribeEvents)?;
        let current_tip_event = self.current_tip_event().await;
        self.event_tx
            .unbounded_send(current_tip_event)
            .map_err(|_| Error::SendEvent)?;
        loop {
            tokio::select! {
                request = self.request_rx.next() => {
                    let Some((request, response_tx)) = request else {
                        return Ok(());
                    };
                    match request {
                        Request::AncestorInfos(main_block_hash) => {
                            let res = Self::request_ancestor_infos(
                                &self.env,
                                &self.archive,
                                &mut self.mainchain,
                                main_block_hash,
                            )
                            .await;
                            let response =
                                Response::AncestorInfos(main_block_hash, res);
                            if let Some(response_tx) = response_tx {
                                response_tx.send(response).map_err(|resp| {
                                    Error::SendResponseOneshot(Box::new(resp))
                                })?;
                            } else {
                                self.response_tx.unbounded_send(response).map_err(
                                    |err| {
                                        let resp = err.into_inner();
                                        Error::SendResponse(Box::new(resp))
                                    },
                                )?;
                            }
                        }
                    }
                }
                event = events.next() => {
                    let event = event.ok_or(Error::EventStreamClosed)?;
                    let event = match event {
                        Ok(event) => self.prepare_event(event).await,
                        Err(err) => Err(err.into()),
                    };
                    self.event_tx
                        .unbounded_send(event)
                        .map_err(|_| Error::SendEvent)?;
                }
            }
        }
    }
}

/// Handle to the task to communicate with mainchain node.
/// Task is aborted on drop.
#[derive(Clone)]
pub(super) struct MainchainTaskHandle {
    task: Arc<JoinHandle<()>>,
    // send a request, and optional oneshot sender to receive the result on the
    // corresponding oneshot receiver
    request_tx:
        mpsc::UnboundedSender<(Request, Option<oneshot::Sender<Response>>)>,
}

impl MainchainTaskHandle {
    pub fn new<Transport>(
        env: sneed::Env,
        archive: Archive,
        mainchain: mainchain::ValidatorClient<Transport>,
    ) -> (
        Self,
        mpsc::UnboundedReceiver<Response>,
        mpsc::UnboundedReceiver<Event>,
    )
    where
        Transport: proto::Transport + Send + 'static,
        mainchain::ValidatorClient<Transport>: Clone,
        <Transport as tonic::client::GrpcService<tonic::body::Body>>::Future:
            Send,
    {
        let (request_tx, request_rx) = mpsc::unbounded();
        let (response_tx, response_rx) = mpsc::unbounded();
        let (event_tx, event_rx) = mpsc::unbounded();
        let task = MainchainTask {
            env,
            archive,
            mainchain,
            request_rx,
            response_tx,
            event_tx,
        };
        let task = spawn(async move {
            if let Err(err) = task.run().await {
                let err = anyhow::Error::from(err);
                tracing::error!("Mainchain task error: {err:#}");
            }
        });
        let task_handle = MainchainTaskHandle {
            task: Arc::new(task),
            request_tx,
        };
        (task_handle, response_rx, event_rx)
    }

    /// Send a request
    pub fn request(&self, request: Request) -> Result<(), Request> {
        self.request_tx
            .unbounded_send((request, None))
            .map_err(|err| {
                let (request, _) = err.into_inner();
                request
            })
    }

    /// Send a request, and receive the response on a oneshot receiver instead
    /// of the response stream
    pub fn request_oneshot(
        &self,
        request: Request,
    ) -> Result<oneshot::Receiver<Response>, Request> {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        let () = self
            .request_tx
            .unbounded_send((request, Some(oneshot_tx)))
            .map_err(|err| {
                let (request, _) = err.into_inner();
                request
            })?;
        Ok(oneshot_rx)
    }
}

impl Drop for MainchainTaskHandle {
    // If only one reference exists (ie. within self), abort the net task.
    fn drop(&mut self) {
        // use `Arc::get_mut` since `Arc::into_inner` requires ownership of the
        // Arc, and cloning would increase the reference count
        if let Some(task) = Arc::get_mut(&mut self.task) {
            task.abort()
        }
    }
}
