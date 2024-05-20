use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt::Debug,
    net::SocketAddr,
    path::Path,
    sync::Arc,
};

#[cfg(all(not(target_os = "windows"), feature = "zmq"))]
use async_zmq::SinkExt;
use bip300301::{
    bitcoin::{self, hashes::Hash},
    DepositInfo, Header as BitcoinHeader,
};
use fallible_iterator::{FallibleIterator, IteratorExt};
use fraction::Fraction;
use futures::{stream, StreamExt, TryFutureExt};
use heed::RwTxn;
#[cfg(all(not(target_os = "windows"), feature = "zmq"))]
use tokio::sync::mpsc;
use tokio::{task::JoinHandle, time::Duration};
use tokio_stream::StreamNotifyClose;
use tokio_util::task::LocalPoolHandle;

use crate::{
    archive::{self, Archive},
    mempool::{self, MemPool},
    net::{
        self, Net, PeerConnectionInfo, PeerInfoRx, PeerRequest, PeerResponse,
    },
    state::{
        self, AmmPair, AmmPoolState, BitAssetSeqId, DutchAuctionBidError,
        DutchAuctionState, State,
    },
    types::*,
};

pub const THIS_SIDECHAIN: u8 = 4;

#[derive(Debug, thiserror::Error)]
pub enum GetAmmPoolStateError {
    #[error(
        "`asset0` and `asset1` must be distinct and lexicographically ordered"
    )]
    InvalidOrdering,
}

#[derive(Debug, thiserror::Error)]
pub enum GetAmmMintError {
    #[error(
        "`asset0` and `asset1` must be distinct and lexicographically ordered"
    )]
    InvalidOrdering,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("address parse error")]
    AddrParse(#[from] std::net::AddrParseError),
    #[error("AMM mint error")]
    AmmMint(#[from] GetAmmMintError),
    #[error("Get AMM pool state error")]
    AmmPoolState(#[from] GetAmmPoolStateError),
    #[error("archive error")]
    Archive(#[from] archive::Error),
    #[error("bincode error")]
    Bincode(#[from] bincode::Error),
    #[error("drivechain error")]
    Drivechain(#[from] bip300301::Error),
    #[error("heed error")]
    Heed(#[from] heed::Error),
    #[error("quinn error")]
    Io(#[from] std::io::Error),
    #[error("mempool error")]
    MemPool(#[from] mempool::Error),
    #[error("net error")]
    Net(#[from] net::Error),
    #[error("peer info stream closed")]
    PeerInfoRxClosed,
    #[error("state error")]
    State(#[from] state::Error),
}

#[cfg(all(not(target_os = "windows"), feature = "zmq"))]
#[derive(Debug)]
struct ZmqPubHandler {
    tx: mpsc::UnboundedSender<Vec<async_zmq::Message>>,
    _handle: JoinHandle<()>,
}

#[cfg(all(not(target_os = "windows"), feature = "zmq"))]
impl ZmqPubHandler {
    // run the handler, obtaining a sender sink and the handler task
    fn new(socket_addr: SocketAddr) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<async_zmq::Message>>();
        let handle = tokio::task::spawn(async move {
            let mut zmq_pub =
                async_zmq::publish(&format!("tcp://{socket_addr}"))
                    .unwrap()
                    .bind()
                    .unwrap();

            while let Some(msgs) = rx.recv().await {
                let () = zmq_pub.send(msgs.into()).await.unwrap();
            }
        });
        Self {
            tx,
            _handle: handle,
        }
    }
}

/// Attempt to verify bmm for the provided header,
/// and store the verification result
async fn verify_bmm(
    env: &heed::Env,
    archive: &Archive,
    drivechain: &bip300301::Drivechain,
    header: Header,
) -> Result<bool, Error> {
    use jsonrpsee::types::error::ErrorCode as JsonrpseeErrorCode;
    const VERIFY_BMM_POLL_INTERVAL: Duration = Duration::from_secs(15);
    let block_hash = header.hash();
    let res = {
        let rotxn = env.read_txn()?;
        archive.try_get_bmm_verification(&rotxn, block_hash)?
    };
    if let Some(res) = res {
        return Ok(res);
    }
    let res = match drivechain
        .verify_bmm(
            &header.prev_main_hash,
            &block_hash.into(),
            VERIFY_BMM_POLL_INTERVAL,
        )
        .await
    {
        Ok(()) => true,
        Err(bip300301::Error::Jsonrpsee(jsonrpsee::core::Error::Call(err)))
            if JsonrpseeErrorCode::from(err.code())
                == JsonrpseeErrorCode::ServerError(-1) =>
        {
            false
        }
        Err(err) => return Err(Error::from(err)),
    };
    let mut rwtxn = env.write_txn()?;
    let () = archive.put_bmm_verification(&mut rwtxn, block_hash, res)?;
    rwtxn.commit()?;
    Ok(res)
}

/// Request ancestor headers from the mainchain node,
/// including the specified header
async fn request_ancestor_headers(
    env: &heed::Env,
    archive: &Archive,
    drivechain: &bip300301::Drivechain,
    mut block_hash: bitcoin::BlockHash,
) -> Result<(), Error> {
    let mut headers: Vec<BitcoinHeader> = Vec::new();
    loop {
        if block_hash == bitcoin::BlockHash::all_zeros() {
            break;
        } else {
            let rotxn = env.read_txn()?;
            if archive.try_get_main_header(&rotxn, block_hash)?.is_some() {
                break;
            }
        }
        let header = drivechain.get_header(block_hash).await?;
        block_hash = header.prev_blockhash;
        headers.push(header);
    }
    if headers.is_empty() {
        Ok(())
    } else {
        let mut rwtxn = env.write_txn()?;
        headers.into_iter().rev().try_for_each(|header| {
            archive.put_main_header(&mut rwtxn, &header)
        })?;
        rwtxn.commit()?;
        Ok(())
    }
}

/// Request any missing two way peg data up to the specified block hash.
/// All ancestor headers must exist in the archive.
// TODO: deposits only for now
#[allow(dead_code)]
async fn request_two_way_peg_data(
    env: &heed::Env,
    archive: &Archive,
    drivechain: &bip300301::Drivechain,
    block_hash: bitcoin::BlockHash,
) -> Result<(), Error> {
    // last block for which deposit info is known
    let last_known_deposit_info = {
        let rotxn = env.read_txn()?;
        #[allow(clippy::let_and_return)]
        let last_known_deposit_info = archive
            .main_ancestors(&rotxn, block_hash)
            .find(|block_hash| {
                let deposits = archive.try_get_deposits(&rotxn, *block_hash)?;
                Ok(deposits.is_some())
            })?;
        last_known_deposit_info
    };
    if last_known_deposit_info == Some(block_hash) {
        return Ok(());
    }
    let two_way_peg_data = drivechain
        .get_two_way_peg_data(block_hash, last_known_deposit_info)
        .await?;
    let mut rwtxn = env.write_txn()?;
    // Deposits by block, first-to-last within each block
    let deposits_by_block: HashMap<bitcoin::BlockHash, Vec<DepositInfo>> = {
        let mut deposits = HashMap::<_, Vec<_>>::new();
        two_way_peg_data.deposits.into_iter().for_each(|deposit| {
            deposits
                .entry(deposit.block_hash)
                .or_default()
                .push(deposit)
        });
        let () = archive
            .main_ancestors(&rwtxn, block_hash)
            .take_while(|block_hash| {
                Ok(last_known_deposit_info != Some(*block_hash))
            })
            .for_each(|block_hash| {
                let _ = deposits.entry(block_hash).or_default();
                Ok(())
            })?;
        deposits
    };
    deposits_by_block
        .into_iter()
        .try_for_each(|(block_hash, deposits)| {
            archive.put_deposits(&mut rwtxn, block_hash, deposits)
        })?;
    rwtxn.commit()?;
    Ok(())
}

async fn connect_tip_(
    rwtxn: &mut RwTxn<'_>,
    archive: &Archive,
    drivechain: &bip300301::Drivechain,
    mempool: &MemPool,
    state: &State,
    header: &Header,
    body: &Body,
) -> Result<(), Error> {
    let last_deposit_block_hash = state.get_last_deposit_block_hash(rwtxn)?;
    let two_way_peg_data = drivechain
        .get_two_way_peg_data(header.prev_main_hash, last_deposit_block_hash)
        .await?;
    let block_hash = header.hash();
    let _fees: u64 = state.validate_block(rwtxn, header, body)?;
    if tracing::enabled!(tracing::Level::DEBUG) {
        let merkle_root = body.compute_merkle_root();
        let height = state.get_height(rwtxn)?;
        let () = state.connect_block(rwtxn, header, body)?;
        tracing::debug!(%height, %merkle_root, %block_hash,
                            "connected body")
    } else {
        let () = state.connect_block(rwtxn, header, body)?;
    }
    let () = state.connect_two_way_peg_data(rwtxn, &two_way_peg_data)?;
    let () = archive.put_header(rwtxn, header)?;
    let () = archive.put_body(rwtxn, block_hash, body)?;
    for transaction in &body.transactions {
        let () = mempool.delete(rwtxn, transaction.txid())?;
    }
    Ok(())
}

async fn disconnect_tip_(
    rwtxn: &mut RwTxn<'_>,
    archive: &Archive,
    drivechain: &bip300301::Drivechain,
    mempool: &MemPool,
    state: &State,
) -> Result<(), Error> {
    let tip_block_hash = state.get_tip(rwtxn)?;
    let tip_header = archive.get_header(rwtxn, tip_block_hash)?;
    let tip_body = archive.get_body(rwtxn, tip_block_hash)?;
    let height = state.get_height(rwtxn)?;
    let two_way_peg_data = {
        let start_block_hash = state
            .deposit_blocks
            .rev_iter(rwtxn)?
            .transpose_into_fallible()
            .find_map(|(_, (block_hash, applied_height))| {
                if applied_height < height - 1 {
                    Ok(Some(block_hash))
                } else {
                    Ok(None)
                }
            })?;
        drivechain
            .get_two_way_peg_data(tip_header.prev_main_hash, start_block_hash)
            .await?
    };
    let () = state.disconnect_two_way_peg_data(rwtxn, &two_way_peg_data)?;
    let () = state.disconnect_tip(rwtxn, &tip_header, &tip_body)?;
    for transaction in tip_body.authorized_transactions().iter().rev() {
        mempool.put(rwtxn, transaction)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn submit_block(
    env: &heed::Env,
    archive: &Archive,
    drivechain: &bip300301::Drivechain,
    mempool: &MemPool,
    state: &State,
    #[cfg(all(not(target_os = "windows"), feature = "zmq"))]
    zmq_pub_handler: &ZmqPubHandler,
    header: &Header,
    body: &Body,
) -> Result<(), Error> {
    // Request mainchain headers if they do not exist
    request_ancestor_headers(env, archive, drivechain, header.prev_main_hash)
        .await?;
    let mut rwtxn = env.write_txn()?;
    let () = connect_tip_(
        &mut rwtxn, archive, drivechain, mempool, state, header, body,
    )
    .await?;
    let bundle = state.get_pending_withdrawal_bundle(&rwtxn)?;
    #[cfg(all(not(target_os = "windows"), feature = "zmq"))]
    let height = state.get_height(&rwtxn)?;
    rwtxn.commit()?;
    #[cfg(all(not(target_os = "windows"), feature = "zmq"))]
    {
        let block_hash = header.hash();
        let zmq_msgs = vec![
            "hashblock".into(),
            block_hash.0[..].into(),
            height.to_le_bytes()[..].into(),
        ];
        zmq_pub_handler.tx.send(zmq_msgs).unwrap();
    }
    if let Some((bundle, _)) = bundle {
        let () = drivechain
            .broadcast_withdrawal_bundle(bundle.transaction)
            .await?;
    }
    Ok(())
}

/// Re-org to the specified tip. The new tip block and all ancestor blocks
/// must exist in the node's archive.
async fn reorg_to_tip(
    env: &heed::Env,
    archive: &Archive,
    drivechain: &bip300301::Drivechain,
    mempool: &MemPool,
    state: &State,
    #[cfg(all(not(target_os = "windows"), feature = "zmq"))]
    zmq_pub_handler: &ZmqPubHandler,
    new_tip: BlockHash,
) -> Result<(), Error> {
    let mut rwtxn = env.write_txn()?;
    let tip = state.get_tip(&rwtxn)?;
    let tip_height = state.get_height(&rwtxn)?;
    let common_ancestor = archive.last_common_ancestor(&rwtxn, tip, new_tip)?;
    // Check that all necessary bodies exist before disconnecting tip
    let blocks_to_apply: Vec<(Header, Body)> = archive
        .ancestors(&rwtxn, new_tip)
        .take_while(|block_hash| Ok(*block_hash != common_ancestor))
        .map(|block_hash| {
            let header = archive.get_header(&rwtxn, block_hash)?;
            let body = archive.get_body(&rwtxn, block_hash)?;
            Ok((header, body))
        })
        .collect()?;
    // Disconnect tip until common ancestor is reached
    let common_ancestor_height = archive.get_height(&rwtxn, common_ancestor)?;
    for _ in 0..tip_height - common_ancestor_height {
        let () =
            disconnect_tip_(&mut rwtxn, archive, drivechain, mempool, state)
                .await?;
    }
    let tip = state.get_tip(&rwtxn)?;
    assert_eq!(tip, common_ancestor);
    // Apply blocks until new tip is reached
    for (header, body) in blocks_to_apply.iter().rev() {
        let () = connect_tip_(
            &mut rwtxn, archive, drivechain, mempool, state, header, body,
        )
        .await?;
    }
    let tip = state.get_tip(&rwtxn)?;
    assert_eq!(tip, new_tip);
    rwtxn.commit()?;
    tracing::info!("reorged to tip: {new_tip}");
    #[cfg(all(not(target_os = "windows"), feature = "zmq"))]
    {
        for (idx, (header, _body)) in
            blocks_to_apply.into_iter().rev().enumerate()
        {
            let block_hash = header.hash();
            let height = common_ancestor_height + idx as u32 + 1;
            let zmq_msgs = vec![
                "hashblock".into(),
                block_hash.0[..].into(),
                height.to_le_bytes()[..].into(),
            ];
            zmq_pub_handler.tx.send(zmq_msgs).unwrap();
        }
    }
    Ok(())
}

#[derive(Clone)]
struct NetTaskContext {
    env: heed::Env,
    archive: Archive,
    drivechain: bip300301::Drivechain,
    mempool: MemPool,
    net: Net,
    state: State,
    #[cfg(all(not(target_os = "windows"), feature = "zmq"))]
    zmq_pub_handler: Arc<ZmqPubHandler>,
}

struct NetTask {
    ctxt: NetTaskContext,
    peer_info_rx: PeerInfoRx,
}

impl NetTask {
    const VERIFY_BMM_POLL_INTERVAL: Duration = Duration::from_secs(15);

    async fn handle_response(
        ctxt: &NetTaskContext,
        addr: SocketAddr,
        resp: PeerResponse,
        req: PeerRequest,
    ) -> Result<(), Error> {
        match (req, resp) {
            (
                req @ PeerRequest::GetBlock { block_hash },
                ref resp @ PeerResponse::Block {
                    ref header,
                    ref body,
                },
            ) => {
                let tip = {
                    let rotxn = ctxt.env.read_txn()?;
                    ctxt.state.get_tip(&rotxn)?
                };
                if header.hash() != block_hash {
                    // Invalid response
                    tracing::warn!(%addr, ?req, ?resp,"Invalid response from peer; unexpected block hash");
                    let () = ctxt.net.remove_active_peer(addr);
                    return Ok(());
                }
                // Verify BMM
                // TODO: Spawn a task for this
                let () = ctxt
                    .drivechain
                    .verify_bmm(
                        &header.prev_main_hash,
                        &block_hash.into(),
                        Self::VERIFY_BMM_POLL_INTERVAL,
                    )
                    .await?;
                if header.prev_side_hash == tip {
                    submit_block(
                        &ctxt.env,
                        &ctxt.archive,
                        &ctxt.drivechain,
                        &ctxt.mempool,
                        &ctxt.state,
                        #[cfg(all(
                            not(target_os = "windows"),
                            feature = "zmq"
                        ))]
                        &ctxt.zmq_pub_handler,
                        header,
                        body,
                    )
                    .await
                } else {
                    let mut rwtxn = ctxt.env.write_txn()?;
                    let () = ctxt.archive.put_header(&mut rwtxn, header)?;
                    let () =
                        ctxt.archive.put_body(&mut rwtxn, block_hash, body)?;
                    rwtxn.commit()?;
                    Ok(())
                }
            }
            (
                PeerRequest::GetBlock {
                    block_hash: req_block_hash,
                },
                PeerResponse::NoBlock {
                    block_hash: resp_block_hash,
                },
            ) if req_block_hash == resp_block_hash => Ok(()),
            (
                ref req @ PeerRequest::GetHeaders {
                    ref start,
                    end,
                    height: Some(height),
                },
                PeerResponse::Headers(headers),
            ) => {
                // check that the end header is as requested
                let Some(end_header) = headers.last() else {
                    tracing::warn!(%addr, ?req, "Invalid response from peer; missing end header");
                    let () = ctxt.net.remove_active_peer(addr);
                    return Ok(());
                };
                if end_header.hash() != end {
                    tracing::warn!(%addr, ?req, ?end_header,"Invalid response from peer; unexpected end header");
                    let () = ctxt.net.remove_active_peer(addr);
                    return Ok(());
                }
                // Must be at least one header due to previous check
                let start_hash = headers.first().unwrap().prev_side_hash;
                // check that the first header is after a start block
                if !(start.contains(&start_hash)
                    || start_hash == BlockHash::default())
                {
                    tracing::warn!(%addr, ?req, ?start_hash, "Invalid response from peer; invalid start hash");
                    let () = ctxt.net.remove_active_peer(addr);
                    return Ok(());
                }
                // check that the end header height is as expected
                {
                    let rotxn = ctxt.env.read_txn()?;
                    let start_height =
                        ctxt.archive.get_height(&rotxn, start_hash)?;
                    if start_height + headers.len() as u32 != height {
                        tracing::warn!(%addr, ?req, ?start_hash, "Invalid response from peer; invalid end height");
                        let () = ctxt.net.remove_active_peer(addr);
                        return Ok(());
                    }
                }
                // check that headers are sequential based on prev_side_hash
                let mut prev_side_hash = start_hash;
                for header in &headers {
                    if header.prev_side_hash != prev_side_hash {
                        tracing::warn!(%addr, ?req, ?headers,"Invalid response from peer; non-sequential headers");
                        let () = ctxt.net.remove_active_peer(addr);
                        return Ok(());
                    }
                    prev_side_hash = header.hash();
                }
                // Request mainchain headers
                tokio::spawn({
                    let ctxt = ctxt.clone();
                    let prev_main_hash = headers.last().unwrap().prev_main_hash;
                    async move {
                        if let Err(err) = request_ancestor_headers(
                            &ctxt.env,
                            &ctxt.archive,
                            &ctxt.drivechain,
                            prev_main_hash,
                        )
                        .await
                        {
                            let err = anyhow::anyhow!(err);
                            tracing::error!(%addr, err = format!("{err:#}"), "Request ancestor headers error");
                        }
                    }
                });
                // Verify BMM
                tokio::spawn({
                    let ctxt = ctxt.clone();
                    let headers = headers.clone();
                    async move {
                        for header in headers.clone() {
                            match verify_bmm(
                                &ctxt.env,
                                &ctxt.archive,
                                &ctxt.drivechain,
                                header.clone(),
                            )
                            .await
                            {
                                Ok(true) => (),
                                Ok(false) => {
                                    tracing::warn!(
                                        %addr,
                                        ?header,
                                        ?headers,
                                        "Invalid response from peer; BMM verification failed"
                                    );
                                    let () = ctxt.net.remove_active_peer(addr);
                                    break;
                                }
                                Err(err) => {
                                    let err = anyhow::anyhow!(err);
                                    tracing::error!(%addr, err = format!("{err:#}"), "Verify BMM error");
                                }
                            }
                        }
                    }
                });
                // Store new headers
                let mut rwtxn = ctxt.env.write_txn()?;
                for header in headers {
                    let block_hash = header.hash();
                    if ctxt
                        .archive
                        .try_get_header(&rwtxn, block_hash)?
                        .is_none()
                    {
                        if header.prev_side_hash == BlockHash::default()
                            || ctxt
                                .archive
                                .try_get_header(&rwtxn, header.prev_side_hash)?
                                .is_some()
                        {
                            ctxt.archive.put_header(&mut rwtxn, &header)?;
                        } else {
                            break;
                        }
                    }
                }
                rwtxn.commit()?;
                Ok(())
            }
            (
                PeerRequest::GetHeaders {
                    start: _,
                    end,
                    height: _,
                },
                PeerResponse::NoHeader { block_hash },
            ) if end == block_hash => Ok(()),
            (
                PeerRequest::PushTransaction { transaction: _ },
                PeerResponse::TransactionAccepted(_),
            ) => Ok(()),
            (
                PeerRequest::PushTransaction { transaction: _ },
                PeerResponse::TransactionRejected(_),
            ) => Ok(()),
            (
                req @ (PeerRequest::GetBlock { .. }
                | PeerRequest::GetHeaders { .. }
                | PeerRequest::Heartbeat(_)
                | PeerRequest::PushTransaction { .. }),
                resp,
            ) => {
                // Invalid response
                tracing::warn!(%addr, ?req, ?resp,"Invalid response from peer");
                let () = ctxt.net.remove_active_peer(addr);
                Ok(())
            }
        }
    }

    async fn run(self) -> Result<(), Error> {
        enum MailboxItem {
            AcceptConnection(Result<(), Error>),
            PeerInfo(Option<(SocketAddr, Option<PeerConnectionInfo>)>),
        }
        let accept_connections = stream::try_unfold((), |()| {
            let env = self.ctxt.env.clone();
            let net = self.ctxt.net.clone();
            let fut = async move {
                let () = net.accept_incoming(env).await?;
                Result::<_, Error>::Ok(Some(((), ())))
            };
            Box::pin(fut)
        })
        .map(MailboxItem::AcceptConnection);
        let peer_info_stream = StreamNotifyClose::new(self.peer_info_rx)
            .map(MailboxItem::PeerInfo);
        let mut mailbox_stream =
            stream::select(accept_connections, peer_info_stream);
        while let Some(mailbox_item) = mailbox_stream.next().await {
            match mailbox_item {
                MailboxItem::AcceptConnection(res) => res?,
                MailboxItem::PeerInfo(None) => {
                    return Err(Error::PeerInfoRxClosed)
                }
                MailboxItem::PeerInfo(Some((addr, None))) => {
                    // peer connection is closed, remove it
                    tracing::warn!(%addr, "Connection to peer closed");
                    let () = self.ctxt.net.remove_active_peer(addr);
                    continue;
                }
                MailboxItem::PeerInfo(Some((addr, Some(peer_info)))) => {
                    match peer_info {
                        PeerConnectionInfo::Error(err) => {
                            let err = anyhow::anyhow!(err);
                            tracing::error!(%addr, err = format!("{err:#}"), "Peer connection error");
                            let () = self.ctxt.net.remove_active_peer(addr);
                        }
                        PeerConnectionInfo::NeedBmmVerification(
                            block_hashes,
                        ) => {
                            let headers: Vec<_> = {
                                let rotxn = self.ctxt.env.read_txn()?;
                                block_hashes
                                    .into_iter()
                                    .map(|block_hash| {
                                        self.ctxt
                                            .archive
                                            .get_header(&rotxn, block_hash)
                                    })
                                    .transpose_into_fallible()
                                    .collect()?
                            };
                            tokio::spawn({
                                let ctxt = self.ctxt.clone();
                                async move {
                                    for header in headers {
                                        if let Err(err) = verify_bmm(
                                            &ctxt.env,
                                            &ctxt.archive,
                                            &ctxt.drivechain,
                                            header,
                                        )
                                        .await
                                        {
                                            let err = anyhow::anyhow!(err);
                                            tracing::error!(%addr, err = format!("{err:#}"), "Verify BMM error")
                                        }
                                    }
                                }
                            });
                        }
                        PeerConnectionInfo::NeedMainchainAncestors(
                            block_hash,
                        ) => {
                            tokio::spawn({
                                let ctxt = self.ctxt.clone();
                                async move {
                                    let () = request_ancestor_headers(&ctxt.env, &ctxt.archive, &ctxt.drivechain, block_hash)
                                    .unwrap_or_else(move |err| {
                                        let err = anyhow::anyhow!(err);
                                        tracing::error!(%addr, err = format!("{err:#}"), "Request ancestor headers error");
                                    }).await;
                                }
                            });
                        }
                        PeerConnectionInfo::NewTipReady(new_tip) => {
                            let () = reorg_to_tip(
                                &self.ctxt.env,
                                &self.ctxt.archive,
                                &self.ctxt.drivechain,
                                &self.ctxt.mempool,
                                &self.ctxt.state,
                                #[cfg(all(
                                    not(target_os = "windows"),
                                    feature = "zmq"
                                ))]
                                &self.ctxt.zmq_pub_handler,
                                new_tip,
                            )
                            .await?;
                        }
                        PeerConnectionInfo::NewTransaction(new_tx) => {
                            let mut rwtxn = self.ctxt.env.write_txn()?;
                            self.ctxt.mempool.put(&mut rwtxn, &new_tx)?;
                            rwtxn.commit()?;
                            // broadcast
                            let () = self
                                .ctxt
                                .net
                                .push_tx(HashSet::from_iter([addr]), new_tx);
                        }
                        PeerConnectionInfo::Response(resp, req) => {
                            let () = Self::handle_response(
                                &self.ctxt, addr, resp, req,
                            )
                            .await?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

pub type FilledTransactionWithPosition =
    (Authorized<FilledTransaction>, Option<TxIn>);

#[derive(Clone)]
pub struct Node {
    archive: Archive,
    drivechain: bip300301::Drivechain,
    env: heed::Env,
    _local_pool: LocalPoolHandle,
    mempool: MemPool,
    net: Net,
    net_task: Arc<JoinHandle<()>>,
    state: State,
    #[cfg(all(not(target_os = "windows"), feature = "zmq"))]
    zmq_pub_handler: Arc<ZmqPubHandler>,
}

impl Node {
    pub fn new(
        bind_addr: SocketAddr,
        datadir: &Path,
        main_addr: SocketAddr,
        password: &str,
        user: &str,
        local_pool: LocalPoolHandle,
        #[cfg(all(not(target_os = "windows"), feature = "zmq"))]
        zmq_addr: SocketAddr,
    ) -> Result<Self, Error> {
        let env_path = datadir.join("data.mdb");
        // let _ = std::fs::remove_dir_all(&env_path);
        std::fs::create_dir_all(&env_path)?;
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(1024 * 1024 * 1024) // 1GB
                .max_dbs(
                    State::NUM_DBS
                        + Archive::NUM_DBS
                        + MemPool::NUM_DBS
                        + Net::NUM_DBS,
                )
                .open(env_path)?
        };
        let archive = Archive::new(&env)?;
        let drivechain = bip300301::Drivechain::new(
            THIS_SIDECHAIN,
            main_addr,
            user,
            password,
        )?;
        let mempool = MemPool::new(&env)?;
        let state = State::new(&env)?;
        #[cfg(all(not(target_os = "windows"), feature = "zmq"))]
        let zmq_pub_handler = Arc::new(ZmqPubHandler::new(zmq_addr));
        let (net, peer_info_rx) =
            Net::new(&env, archive.clone(), state.clone(), bind_addr)?;
        let net_task = local_pool.spawn_pinned({
            let ctxt = NetTaskContext {
                env: env.clone(),
                archive: archive.clone(),
                drivechain: drivechain.clone(),
                mempool: mempool.clone(),
                net: net.clone(),
                state: state.clone(),
                #[cfg(all(not(target_os = "windows"), feature = "zmq"))]
                zmq_pub_handler: zmq_pub_handler.clone(),
            };
            || {
                NetTask { ctxt, peer_info_rx }.run().unwrap_or_else(|err| {
                    let err = anyhow::anyhow!(err);
                    tracing::error!(err = format!("{err:#}"))
                })
            }
        });
        Ok(Self {
            archive,
            drivechain,
            env,
            _local_pool: local_pool,
            mempool,
            net,
            net_task: Arc::new(net_task),
            state,
            #[cfg(all(not(target_os = "windows"), feature = "zmq"))]
            zmq_pub_handler,
        })
    }

    pub fn drivechain(&self) -> &bip300301::Drivechain {
        &self.drivechain
    }

    pub async fn get_best_parentchain_hash(
        &self,
    ) -> Result<bitcoin::BlockHash, Error> {
        use bip300301::MainClient;
        let res = self
            .drivechain
            .client
            .getbestblockhash()
            .await
            .map_err(bip300301::Error::Jsonrpsee)?;
        Ok(res)
    }

    pub fn get_tip_height(&self) -> Result<u32, Error> {
        let rotxn = self.env.read_txn()?;
        Ok(self.state.get_height(&rotxn)?)
    }

    pub fn try_get_amm_price(
        &self,
        base: AssetId,
        quote: AssetId,
    ) -> Result<Option<Fraction>, Error> {
        let txn = self.env.read_txn()?;
        let amm_pair = AmmPair::new(base, quote);
        let Some(AmmPoolState {
            reserve0,
            reserve1,
            outstanding_lp_tokens: _,
            ..
        }) = self.state.amm_pools.get(&txn, &amm_pair)?
        else {
            return Ok(None);
        };
        if reserve0 == 0 || reserve1 == 0 {
            return Ok(None);
        }
        if base < quote {
            Ok(Some(Fraction::new(reserve1, reserve0)))
        } else {
            Ok(Some(Fraction::new(reserve0, reserve1)))
        }
    }

    pub fn try_get_amm_pool_state(
        &self,
        pair: AmmPair,
    ) -> Result<Option<AmmPoolState>, Error> {
        let txn = self.env.read_txn()?;
        let res = self.state.amm_pools.get(&txn, &pair)?;
        Ok(res)
    }

    pub fn get_amm_pool_state(
        &self,
        pair: AmmPair,
    ) -> Result<AmmPoolState, Error> {
        let res = self.try_get_amm_pool_state(pair)?.ok_or_else(|| {
            state::Error::MissingAmmPoolState {
                asset0: pair.asset0(),
                asset1: pair.asset1(),
            }
        })?;
        Ok(res)
    }

    /// List all BitAssets and their current data
    pub fn bitassets(
        &self,
    ) -> Result<Vec<(BitAssetSeqId, BitAssetId, BitAssetData)>, Error> {
        let txn = self.env.read_txn()?;
        let bitasset_ids_to_data: HashMap<_, _> = self
            .state
            .bitassets
            .iter(&txn)?
            .map(|res| {
                res.map(|(bitasset_id, bitasset_data)| {
                    (bitasset_id, bitasset_data.current())
                })
            })
            .collect::<Result<_, _>>()?;
        let res = self
            .state
            .bitasset_seq_to_bitasset
            .iter(&txn)?
            .map(|res| {
                res.map_err(Error::Heed).and_then(
                    |(bitasset_seq_id, bitasset_id)| {
                        let bitasset_data =
                            bitasset_ids_to_data.get(&bitasset_id).ok_or(
                                Error::State(state::Error::MissingBitAsset {
                                    bitasset: bitasset_id,
                                }),
                            )?;
                        Ok((
                            bitasset_seq_id,
                            bitasset_id,
                            bitasset_data.clone(),
                        ))
                    },
                )
            })
            .collect::<Result<_, _>>()?;
        Ok(res)
    }

    /// List all dutch auctions and their current state
    pub fn dutch_auctions(
        &self,
    ) -> Result<Vec<(DutchAuctionId, DutchAuctionState)>, Error> {
        let txn = self.env.read_txn()?;
        let res = self
            .state
            .dutch_auctions
            .iter(&txn)?
            .collect::<Result<_, _>>()?;
        Ok(res)
    }

    pub fn try_get_dutch_auction_state(
        &self,
        auction_id: DutchAuctionId,
    ) -> Result<Option<DutchAuctionState>, Error> {
        let txn = self.env.read_txn()?;
        let res = self.state.dutch_auctions.get(&txn, &auction_id)?;
        Ok(res)
    }

    pub fn get_dutch_auction_state(
        &self,
        auction_id: DutchAuctionId,
    ) -> Result<DutchAuctionState, Error> {
        self.try_get_dutch_auction_state(auction_id).and_then(
            |dutch_auction_state| {
                dutch_auction_state.ok_or_else(|| {
                    Error::State(DutchAuctionBidError::MissingAuction.into())
                })
            },
        )
    }

    pub fn get_tip(&self) -> Result<BlockHash, Error> {
        let rotxn = self.env.read_txn()?;
        Ok(self.state.get_tip(&rotxn)?)
    }

    pub fn try_get_height(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<u32>, Error> {
        let rotxn = self.env.read_txn()?;
        Ok(self.archive.try_get_height(&rotxn, block_hash)?)
    }

    pub fn get_height(&self, block_hash: BlockHash) -> Result<u32, Error> {
        let rotxn = self.env.read_txn()?;
        Ok(self.archive.get_height(&rotxn, block_hash)?)
    }

    /// Get blocks in which a tx was included, and tx index within those blocks
    pub fn get_tx_inclusions(
        &self,
        txid: Txid,
    ) -> Result<BTreeMap<BlockHash, u32>, Error> {
        let rotxn = self.env.read_txn()?;
        Ok(self.archive.get_tx_inclusions(&rotxn, txid)?)
    }

    /// Returns true if the second specified block is a descendant of the first
    /// specified block
    /// Returns an error if either of the specified block headers do not exist
    /// in the archive.
    pub fn is_descendant(
        &self,
        ancestor: BlockHash,
        descendant: BlockHash,
    ) -> Result<bool, Error> {
        let rotxn = self.env.read_txn()?;
        Ok(self.archive.is_descendant(&rotxn, ancestor, descendant)?)
    }

    /** Resolve bitasset data at the specified block height.
     * Returns an error if it does not exist.rror if it does not exist. */
    pub fn get_bitasset_data_at_block_height(
        &self,
        bitasset: &BitAssetId,
        height: u32,
    ) -> Result<BitAssetData, Error> {
        let txn = self.env.read_txn()?;
        Ok(self
            .state
            .get_bitasset_data_at_block_height(&txn, bitasset, height)?)
    }

    /// resolve current bitasset data, if it exists
    pub fn try_get_current_bitasset_data(
        &self,
        bitasset: &BitAssetId,
    ) -> Result<Option<BitAssetData>, Error> {
        let txn = self.env.read_txn()?;
        Ok(self.state.try_get_current_bitasset_data(&txn, bitasset)?)
    }

    /// Resolve current bitasset data. Returns an error if it does not exist.
    pub fn get_current_bitasset_data(
        &self,
        bitasset: &BitAssetId,
    ) -> Result<BitAssetData, Error> {
        let txn = self.env.read_txn()?;
        Ok(self.state.get_current_bitasset_data(&txn, bitasset)?)
    }

    pub fn submit_transaction(
        &self,
        transaction: AuthorizedTransaction,
    ) -> Result<(), Error> {
        {
            let mut rwtxn = self.env.write_txn()?;
            self.state.validate_transaction(&rwtxn, &transaction)?;
            self.mempool.put(&mut rwtxn, &transaction)?;
            rwtxn.commit()?;
        }
        self.net.push_tx(Default::default(), transaction);
        Ok(())
    }

    pub fn get_spent_utxos<'a, OutPoints>(
        &self,
        outpoints: OutPoints,
    ) -> Result<Vec<(OutPoint, SpentOutput)>, Error>
    where
        OutPoints: IntoIterator<Item = &'a OutPoint>,
    {
        let txn = self.env.read_txn()?;
        let mut spent = vec![];
        for outpoint in outpoints {
            if let Some(output) = self.state.stxos.get(&txn, outpoint)? {
                spent.push((*outpoint, output));
            }
        }
        Ok(spent)
    }

    pub fn get_unconfirmed_spent_utxos<'a, OutPoints>(
        &self,
        outpoints: OutPoints,
    ) -> Result<Vec<(OutPoint, InPoint)>, Error>
    where
        OutPoints: IntoIterator<Item = &'a OutPoint>,
    {
        let txn = self.env.read_txn()?;
        let mut spent = vec![];
        for outpoint in outpoints {
            if let Some(inpoint) =
                self.mempool.spent_utxos.get(&txn, outpoint)?
            {
                spent.push((*outpoint, inpoint));
            }
        }
        Ok(spent)
    }

    pub fn get_unconfirmed_utxos_by_addresses(
        &self,
        addresses: &HashSet<Address>,
    ) -> Result<HashMap<OutPoint, Output>, Error> {
        let txn = self.env.read_txn()?;
        let mut res = HashMap::new();
        let () = addresses.iter().try_for_each(|addr| {
            let utxos = self.mempool.get_unconfirmed_utxos(&txn, addr)?;
            res.extend(utxos);
            Result::<(), Error>::Ok(())
        })?;
        Ok(res)
    }

    pub fn get_utxos_by_addresses(
        &self,
        addresses: &HashSet<Address>,
    ) -> Result<HashMap<OutPoint, FilledOutput>, Error> {
        let txn = self.env.read_txn()?;
        let utxos = self.state.get_utxos_by_addresses(&txn, addresses)?;
        Ok(utxos)
    }

    pub fn try_get_header(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<Header>, Error> {
        let txn = self.env.read_txn()?;
        Ok(self.archive.try_get_header(&txn, block_hash)?)
    }

    pub fn get_header(&self, block_hash: BlockHash) -> Result<Header, Error> {
        let txn = self.env.read_txn()?;
        Ok(self.archive.get_header(&txn, block_hash)?)
    }

    /// Get the block hash at the specified height in the current chain,
    /// if it exists
    pub fn try_get_block_hash(
        &self,
        height: u32,
    ) -> Result<Option<BlockHash>, Error> {
        let rotxn = self.env.read_txn()?;
        let tip = self.state.get_tip(&rotxn)?;
        let tip_height = self.state.get_height(&rotxn)?;
        if tip_height >= height {
            self.archive
                .ancestors(&rotxn, tip)
                .nth((tip_height - height) as usize)
                .map_err(Error::from)
        } else {
            Ok(None)
        }
    }

    pub fn try_get_body(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<Body>, Error> {
        let rotxn = self.env.read_txn()?;
        Ok(self.archive.try_get_body(&rotxn, block_hash)?)
    }

    pub fn get_body(&self, block_hash: BlockHash) -> Result<Body, Error> {
        let rotxn = self.env.read_txn()?;
        Ok(self.archive.get_body(&rotxn, block_hash)?)
    }

    /// Get the block corresponding to the provided blockhash, if it exists
    pub fn get_block(&self, block_hash: BlockHash) -> Result<Block, Error> {
        let rotxn = self.env.read_txn()?;
        Ok(self.archive.get_block(&rotxn, block_hash)?)
    }

    pub fn get_all_transactions(
        &self,
    ) -> Result<Vec<AuthorizedTransaction>, Error> {
        let rotxn = self.env.read_txn()?;
        let transactions = self.mempool.take_all(&rotxn)?;
        Ok(transactions)
    }

    /// Get total sidechain wealth in Bitcoin
    pub fn get_sidechain_wealth(&self) -> Result<bitcoin::Amount, Error> {
        let rotxn = self.env.read_txn()?;
        Ok(self.state.sidechain_wealth(&rotxn)?)
    }

    pub fn get_transactions(
        &self,
        number: usize,
    ) -> Result<(Vec<Authorized<FilledTransaction>>, u64), Error> {
        let mut rwtxn = self.env.write_txn()?;
        let transactions = self.mempool.take(&rwtxn, number)?;
        let mut fee: u64 = 0;
        let mut returned_transactions = vec![];
        let mut spent_utxos = HashSet::new();
        for transaction in transactions {
            let inputs: HashSet<_> =
                transaction.transaction.inputs.iter().copied().collect();
            if !spent_utxos.is_disjoint(&inputs) {
                println!("UTXO double spent");
                self.mempool
                    .delete(&mut rwtxn, transaction.transaction.txid())?;
                continue;
            }
            if self
                .state
                .validate_transaction(&rwtxn, &transaction)
                .is_err()
            {
                self.mempool
                    .delete(&mut rwtxn, transaction.transaction.txid())?;
                continue;
            }
            let filled_transaction = self
                .state
                .fill_authorized_transaction(&rwtxn, transaction)?;
            let bitcoin_value_in: u64 = filled_transaction
                .transaction
                .spent_utxos
                .iter()
                .map(GetBitcoinValue::get_bitcoin_value)
                .sum();
            let bitcoin_value_out: u64 = filled_transaction
                .transaction
                .transaction
                .outputs
                .iter()
                .map(GetBitcoinValue::get_bitcoin_value)
                .sum();
            fee += bitcoin_value_in - bitcoin_value_out;
            spent_utxos.extend(filled_transaction.transaction.inputs());
            returned_transactions.push(filled_transaction);
        }
        rwtxn.commit()?;
        Ok((returned_transactions, fee))
    }

    /// get a transaction from the archive or mempool, if it exists
    pub fn try_get_transaction(
        &self,
        txid: Txid,
    ) -> Result<Option<Transaction>, Error> {
        let rotxn = self.env.read_txn()?;
        if let Some((block_hash, txin)) = self
            .archive
            .get_tx_inclusions(&rotxn, txid)?
            .first_key_value()
        {
            let body = self.archive.get_body(&rotxn, *block_hash)?;
            let tx = body.transactions.into_iter().nth(*txin as usize).unwrap();
            Ok(Some(tx))
        } else if let Some(auth_tx) =
            self.mempool.transactions.get(&rotxn, &txid)?
        {
            Ok(Some(auth_tx.transaction))
        } else {
            Ok(None)
        }
    }

    /// get a filled transaction from the archive/state or mempool,
    /// and the tx index, if the transaction exists
    /// and can be filled with the current state.
    /// a tx index of `None` indicates that the tx is in the mempool.
    pub fn try_get_filled_transaction(
        &self,
        txid: Txid,
    ) -> Result<Option<FilledTransactionWithPosition>, Error> {
        let rotxn = self.env.read_txn()?;
        let tip = self.state.get_tip(&rotxn)?;
        let inclusions = self.archive.get_tx_inclusions(&rotxn, txid)?;
        if let Some((block_hash, idx)) =
            inclusions.into_iter().try_find(|(block_hash, _)| {
                self.archive.is_descendant(&rotxn, *block_hash, tip)
            })?
        {
            let body = self.archive.get_body(&rotxn, block_hash)?;
            let auth_txs = body.authorized_transactions();
            let auth_tx = auth_txs.into_iter().nth(idx as usize).unwrap();
            let filled_tx = self
                .state
                .fill_transaction_from_stxos(&rotxn, auth_tx.transaction)?;
            let auth_tx = Authorized {
                transaction: filled_tx,
                authorizations: auth_tx.authorizations,
            };
            let txin = TxIn { block_hash, idx };
            let res = (auth_tx, Some(txin));
            return Ok(Some(res));
        }
        if let Some(auth_tx) = self.mempool.transactions.get(&rotxn, &txid)? {
            match self.state.fill_authorized_transaction(&rotxn, auth_tx) {
                Ok(filled_tx) => {
                    let res = (filled_tx, None);
                    Ok(Some(res))
                }
                Err(state::Error::NoUtxo { .. }) => Ok(None),
                Err(err) => Err(err.into()),
            }
        } else {
            Ok(None)
        }
    }

    pub fn get_pending_withdrawal_bundle(
        &self,
    ) -> Result<Option<WithdrawalBundle>, Error> {
        let rotxn = self.env.read_txn()?;
        let bundle = self
            .state
            .get_pending_withdrawal_bundle(&rotxn)?
            .map(|(bundle, _)| bundle);
        Ok(bundle)
    }

    pub async fn connect_tip(
        &self,
        header: &Header,
        body: &Body,
    ) -> Result<(), Error> {
        let mut rwtxn = self.env.write_txn()?;
        let () = connect_tip_(
            &mut rwtxn,
            &self.archive,
            &self.drivechain,
            &self.mempool,
            &self.state,
            header,
            body,
        )
        .await?;
        rwtxn.commit()?;
        Ok(())
    }

    pub fn connect_peer(&self, addr: SocketAddr) -> Result<(), Error> {
        self.net
            .connect_peer(self.env.clone(), addr)
            .map_err(Error::from)
    }

    pub async fn submit_block(
        &self,
        header: &Header,
        body: &Body,
    ) -> Result<(), Error> {
        submit_block(
            &self.env,
            &self.archive,
            &self.drivechain,
            &self.mempool,
            &self.state,
            #[cfg(all(not(target_os = "windows"), feature = "zmq"))]
            &self.zmq_pub_handler,
            header,
            body,
        )
        .await
    }

    pub async fn disconnect_tip(&self) -> Result<(), Error> {
        let mut rwtxn = self.env.write_txn()?;
        let () = disconnect_tip_(
            &mut rwtxn,
            &self.archive,
            &self.drivechain,
            &self.mempool,
            &self.state,
        )
        .await?;
        rwtxn.commit()?;
        Ok(())
    }
}

impl Drop for Node {
    // If only one reference exists (ie. within self), abort the net task.
    fn drop(&mut self) {
        // use `Arc::get_mut` since `Arc::into_inner` requires ownership of the
        // Arc, and cloning would increase the reference count
        if let Some(task) = Arc::get_mut(&mut self.net_task) {
            task.abort()
        }
    }
}
