//! Task to manage peers and their responses

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bitcoin::hashes::Hash as _;
use error_fatality::{Nested as _, Split};
use fallible_iterator::FallibleIterator;
use futures::{
    StreamExt,
    channel::{
        mpsc::{self, TrySendError, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    stream,
};
use parking_lot::RwLock;
use sneed::{
    DbError, EnvError, RwTxn, RwTxnError, db, env::error as env_error,
    rwtxn::error as rwtxn_error,
};
use thiserror::Error;
use tokio::task::{self, JoinHandle};
use tokio_stream::{StreamNotifyClose, wrappers::IntervalStream};

use super::mainchain_task::{self, MainchainTaskHandle};
use crate::{
    archive::{
        self, ACCUMULATOR_PRUNE_INTERVAL_BLOCKS,
        ACCUMULATOR_REORG_HORIZON_BLOCKS, ACCUMULATOR_SNAPSHOT_INTERVAL_BLOCKS,
        Archive, TXDB_PRUNE_INTERVAL_BLOCKS, TXDB_RETENTION_SECS,
    },
    mempool::{self, MemPool},
    net::{
        self, Net, PeerConnectionError, PeerConnectionInfo,
        PeerConnectionMailboxError, PeerConnectionMessage, PeerInfoRx,
        PeerRequest, PeerResponse, PeerStateId, peer_message,
    },
    state::{self, State},
    types::{
        Accumulator, AccumulatorDiff, BlockHash, Body, Header, InPoint, M6id,
        OutPoint, Tip,
        proto::{self, mainchain},
    },
    util::{ErrorChain, join_set},
};

const WITHDRAWAL_METADATA_RETRY_INTERVAL: Duration = Duration::from_secs(30);

#[allow(clippy::duplicated_attributes)]
#[derive(transitive::Transitive, Debug, Error)]
#[transitive(
    from(db::error::IterInit, DbError),
    from(db::error::IterItem, DbError),
    from(env_error::WriteTxn, EnvError),
    from(rwtxn_error::Commit, RwTxnError)
)]
pub enum Error {
    #[error("archive error")]
    Archive(#[from] archive::Error),
    #[error("CUSF mainchain proto error")]
    CusfMainchain(#[from] proto::Error),
    #[error(transparent)]
    Db(#[from] DbError),
    #[error("Database env error")]
    DbEnv(#[from] EnvError),
    #[error("Database write error")]
    DbWrite(#[from] RwTxnError),
    #[error("Forward mainchain task request failed")]
    ForwardMainchainTaskRequest,
    #[error("mainchain event error")]
    MainchainEvent(#[source] Box<mainchain_task::ResponseError>),
    #[error("mainchain event stream closed")]
    MainchainEventRxClosed,
    #[error(
        "raw mainchain disconnect requires {disconnects} sidechain disconnects, exceeding the retained accumulator horizon of {max_disconnects}"
    )]
    MainchainDisconnectBeyondHorizon {
        disconnects: u64,
        max_disconnects: u32,
    },
    #[error(
        "raw mainchain disconnect requires rewinding to sidechain height {target_height:?}, below the retained accumulator floor {retained_floor}"
    )]
    MainchainDisconnectBelowAccumulatorFloor {
        target_height: Option<u32>,
        retained_floor: u32,
    },
    #[error("state error while reconciling with the canonical mainchain")]
    MainchainReconciliationState(#[source] Box<state::Error>),
    #[error(
        "sync blocked waiting for withdrawal bundle metadata for {m6id}, referenced by mainchain block {event_block_hash}"
    )]
    MissingWithdrawalBundleMetadata {
        event_block_hash: bitcoin::BlockHash,
        m6id: M6id,
    },
    #[error("invalid block")]
    InvalidBlock(#[source] Box<state::Error>),
    #[error(
        "mainchain interval start {start:?} is not an ancestor of interval end {end}"
    )]
    InvalidMainchainInterval {
        start: bitcoin::BlockHash,
        end: bitcoin::BlockHash,
    },
    #[error("mempool error")]
    MemPool(#[from] mempool::Error),
    #[error("Net error")]
    Net(#[from] Box<net::Error>),
    #[error("peer info stream closed")]
    PeerInfoRxClosed,
    #[error("Receive mainchain task response cancelled")]
    ReceiveMainchainTaskResponse,
    #[error("Receive reorg result cancelled (oneshot)")]
    ReceiveReorgResultOneshot(#[source] oneshot::Canceled),
    #[error("Send mainchain task request failed")]
    SendMainchainTaskRequest,
    #[error("Send new tip ready failed")]
    SendNewTipReady(#[source] TrySendError<NewTipReadyMessage>),
    #[error("Send reorg result error (oneshot)")]
    SendReorgResultOneshot,
    #[error("state error")]
    State(#[from] Box<state::Error>),
    #[error(transparent)]
    Utreexo(#[from] crate::types::UtreexoError),
}

impl From<state::Error> for Error {
    fn from(err: state::Error) -> Self {
        Self::State(Box::new(err))
    }
}

#[cfg(feature = "zmq")]
#[derive(Debug)]
pub(super) struct ZmqPubHandler {
    pub(super) tx: mpsc::UnboundedSender<zeromq::ZmqMessage>,
    _handle: JoinHandle<()>,
}

#[cfg(feature = "zmq")]
impl ZmqPubHandler {
    // run the handler, obtaining a sender sink and the handler task
    pub async fn new(
        socket_addr: SocketAddr,
    ) -> Result<Self, zeromq::ZmqError> {
        use futures::TryFutureExt as _;
        use zeromq::Socket as _;
        let (tx, rx) = mpsc::unbounded::<zeromq::ZmqMessage>();
        let zmq_pub_addr = format!("tcp://{socket_addr}");
        let mut zmq_pub = zeromq::PubSocket::new();
        let _zmq_endpoint = zmq_pub.bind(&zmq_pub_addr).await?;
        let handle = tokio::task::spawn({
            rx.map(Ok)
                .forward(futures::sink::unfold(
                    zmq_pub,
                    |mut zmq_pub, zmq_msg| async {
                        zeromq::SocketSend::send(&mut zmq_pub, zmq_msg).await?;
                        Ok(zmq_pub)
                    },
                ))
                .unwrap_or_else(|err: zeromq::ZmqError| {
                    let err = anyhow::Error::from(err);
                    tracing::error!("{err:#}");
                })
        });
        Ok(Self {
            tx,
            _handle: handle,
        })
    }
}

impl From<net::Error> for Error {
    fn from(err: net::Error) -> Self {
        Self::Net(Box::new(err))
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

/// Return the hashes in `(exclusive_start, inclusive_end]`, newest-to-oldest.
///
/// Keeping only hashes here is important: L1 intervals are not bounded by the
/// sidechain reorg horizon, and `BlockInfo` can contain arbitrarily many
/// deposits and withdrawal events.
fn mainchain_interval(
    archive: &Archive,
    rotxn: &sneed::RoTxn,
    exclusive_start: Option<bitcoin::BlockHash>,
    inclusive_end: bitcoin::BlockHash,
) -> Result<Vec<bitcoin::BlockHash>, Error> {
    let exclusive_start = exclusive_start
        .filter(|block_hash| *block_hash != bitcoin::BlockHash::all_zeros());
    if exclusive_start == Some(inclusive_end) {
        return Ok(Vec::new());
    }
    let mut interval = Vec::new();
    let mut ancestors = archive.main_ancestors(rotxn, inclusive_end);
    while let Some(block_hash) = ancestors.next()? {
        if Some(block_hash) == exclusive_start {
            return Ok(interval);
        }
        interval.push(block_hash);
    }
    if let Some(start) = exclusive_start {
        return Err(Error::InvalidMainchainInterval {
            start,
            end: inclusive_end,
        });
    }
    Ok(interval)
}

fn state_error_is_infrastructure(err: &state::Error) -> bool {
    matches!(
        err,
        state::Error::Db(_)
            | state::Error::BorshSerialize(_)
            | state::Error::Amm(state::error::Amm::Db(_))
            | state::Error::BitAsset(state::error::BitAsset::Db(_))
            | state::Error::DutchAuction(state::error::DutchAuction::Db(_))
            | state::Error::ConnectWithdrawalBundleSubmitted(
                state::error::ConnectWithdrawalBundleSubmitted::Db(_)
            )
    )
}

fn classify_prevalidation_error(err: state::Error) -> Error {
    if state_error_is_infrastructure(&err) {
        Error::State(Box::new(err))
    } else {
        Error::InvalidBlock(Box::new(err))
    }
}

fn classify_two_way_peg_error(err: state::Error) -> Error {
    if let Some((event_block_hash, m6id)) =
        err.missing_withdrawal_bundle_metadata()
    {
        Error::MissingWithdrawalBundleMetadata {
            event_block_hash,
            m6id,
        }
    } else {
        Error::State(Box::new(err))
    }
}

fn classify_mainchain_reconciliation_error(err: Error) -> Error {
    match err {
        Error::State(err) => Error::MainchainReconciliationState(err),
        err => err,
    }
}

fn accumulator_common_is_retained(
    common_ancestor_height: Option<u32>,
    retained_reorg_floor: u32,
) -> bool {
    common_ancestor_height.is_some_and(|height| height >= retained_reorg_floor)
        || (common_ancestor_height.is_none() && retained_reorg_floor == 0)
}

fn accumulator_reorg_is_within_horizon(disconnects: u64) -> bool {
    disconnects <= u64::from(ACCUMULATOR_REORG_HORIZON_BLOCKS)
}

/// Return the newest verified BMM inclusion for `side_block` that is still on
/// the ancestry of the specified canonical mainchain tip.
fn best_main_verification_on_lineage(
    archive: &Archive,
    rotxn: &sneed::RoTxn,
    side_block: BlockHash,
    main_tip: bitcoin::BlockHash,
) -> Result<Option<bitcoin::BlockHash>, Error> {
    Ok(archive.try_get_best_main_verification_on_lineage(
        rotxn, side_block, main_tip,
    )?)
}

fn tip_for_side_block(
    archive: &Archive,
    rotxn: &sneed::RoTxn,
    side_block: BlockHash,
    main_tip: Option<bitcoin::BlockHash>,
) -> Result<Option<Tip>, Error> {
    let main_block_hash = if let Some(main_tip) = main_tip {
        best_main_verification_on_lineage(archive, rotxn, side_block, main_tip)?
    } else {
        archive.try_get_best_main_verification(rotxn, side_block)?
    };
    Ok(main_block_hash.map(|main_block_hash| Tip {
        block_hash: side_block,
        main_block_hash,
    }))
}

fn side_tip_on_mainchain_lineage(
    archive: &Archive,
    rotxn: &sneed::RoTxn,
    current_tip: BlockHash,
    main_tip: bitcoin::BlockHash,
) -> Result<Option<BlockHash>, Error> {
    let mut ancestors = archive.ancestors(rotxn, current_tip);
    while let Some(side_block) = ancestors.next()? {
        if best_main_verification_on_lineage(
            archive, rotxn, side_block, main_tip,
        )?
        .is_some()
        {
            return Ok(Some(side_block));
        }
    }
    Ok(None)
}

#[allow(clippy::too_many_arguments)]
fn connect_tip_with_two_way_peg_<F>(
    rwtxn: &mut RwTxn<'_>,
    archive: &Archive,
    mempool: &MemPool,
    state: &State,
    header: &Header,
    body: &Body,
    connect_two_way_peg: F,
    accumulator: &mut Accumulator,
) -> Result<(), Error>
where
    F: FnOnce(&mut RwTxn<'_>) -> Result<AccumulatorDiff, Error>,
{
    let block_hash = header.hash();
    let prevalidated = state
        .prevalidate_block(rwtxn, header, body)
        .map_err(classify_prevalidation_error)?;
    let block_height = prevalidated.next_height;
    let merkle_root = prevalidated.computed_merkle_root;
    let filled_txs = &prevalidated.filled_transactions;
    let mut accumulator_diff = prevalidated.accumulator_diff.clone();
    state.connect_prevalidated_block(rwtxn, header, body, &prevalidated)?;
    if tracing::enabled!(tracing::Level::DEBUG) {
        tracing::debug!(height = block_height, %merkle_root, %block_hash, "connected body")
    }
    let peg_accumulator_diff = connect_two_way_peg(rwtxn)?;
    accumulator_diff.merge(peg_accumulator_diff);
    archive.put_accumulator_diff(rwtxn, block_hash, &accumulator_diff)?;
    let () = archive.put_header(rwtxn, header)?;
    let () = archive.put_body(rwtxn, block_hash, body)?;
    let unix_stamp = unix_now();
    let () = archive.put_txdb_for_connected_block(
        rwtxn,
        block_hash,
        block_height,
        body,
        filled_txs,
        unix_stamp,
    )?;
    if block_height > 0 && block_height % TXDB_PRUNE_INTERVAL_BLOCKS == 0 {
        let cutoff_unix = unix_stamp.saturating_sub(TXDB_RETENTION_SECS);
        archive.prune_txdb_older_than(rwtxn, cutoff_unix)?;
    }
    accumulator.apply_diff(accumulator_diff)?;
    if block_height > 0
        && block_height % ACCUMULATOR_SNAPSHOT_INTERVAL_BLOCKS == 0
    {
        archive.put_accumulator(
            rwtxn,
            block_hash,
            block_height,
            accumulator,
        )?;
    }
    if block_height > 0 && block_height % ACCUMULATOR_PRUNE_INTERVAL_BLOCKS == 0
    {
        let reorg_floor =
            block_height.saturating_sub(ACCUMULATOR_REORG_HORIZON_BLOCKS);
        let snapshot_anchor =
            reorg_floor - reorg_floor % ACCUMULATOR_SNAPSHOT_INTERVAL_BLOCKS;
        archive.prune_accumulator_older_than(rwtxn, snapshot_anchor)?;
        archive.prune_accumulator_diffs_older_than(rwtxn, snapshot_anchor)?;
        archive.advance_accumulator_reorg_floor(rwtxn, snapshot_anchor)?;
    }
    for transaction in &body.transactions {
        for input in &transaction.inputs {
            if let Some(InPoint::Regular { txid, .. }) = mempool
                .spent_utxos
                .try_get(rwtxn, input)
                .map_err(DbError::from)?
            {
                // A newly connected block wins over conflicting mempool
                // transactions and all of their descendants.
                mempool.delete(rwtxn, txid)?;
            }
        }
        let () = mempool.delete(rwtxn, transaction.txid())?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
fn connect_tip_(
    rwtxn: &mut RwTxn<'_>,
    archive: &Archive,
    mempool: &MemPool,
    state: &State,
    header: &Header,
    body: &Body,
    two_way_peg_data: &mainchain::TwoWayPegData,
    accumulator: &mut Accumulator,
) -> Result<(), Error> {
    connect_tip_with_two_way_peg_(
        rwtxn,
        archive,
        mempool,
        state,
        header,
        body,
        |rwtxn| {
            state
                .connect_two_way_peg_data(rwtxn, two_way_peg_data)
                .map_err(classify_two_way_peg_error)
        },
        accumulator,
    )
}

#[allow(clippy::too_many_arguments)]
fn connect_tip_with_mainchain_blocks_(
    rwtxn: &mut RwTxn<'_>,
    archive: &Archive,
    mempool: &MemPool,
    state: &State,
    header: &Header,
    body: &Body,
    mainchain_blocks: &[bitcoin::BlockHash],
    accumulator: &mut Accumulator,
) -> Result<(), Error> {
    connect_tip_with_two_way_peg_(
        rwtxn,
        archive,
        mempool,
        state,
        header,
        body,
        |rwtxn| {
            let mut context = state
                .begin_connect_two_way_peg_data(rwtxn)
                .map_err(classify_two_way_peg_error)?;
            // The hashes are oldest-to-newest, matching `TwoWayPegData`'s
            // connection order. Deserialize and discard one BlockInfo at a
            // time so the interval does not determine peak memory.
            for block_hash in mainchain_blocks {
                let block_info =
                    archive.get_main_block_info(rwtxn, block_hash)?;
                state
                    .connect_two_way_peg_block_info(
                        rwtxn,
                        *block_hash,
                        &block_info,
                        &mut context,
                    )
                    .map_err(classify_two_way_peg_error)?;
            }
            state
                .finish_connect_two_way_peg_data(rwtxn, context)
                .map_err(classify_two_way_peg_error)
        },
        accumulator,
    )
}

fn disconnect_tip_(
    rwtxn: &mut RwTxn<'_>,
    archive: &Archive,
    mempool: &MemPool,
    state: &State,
) -> Result<(), Error> {
    let tip_block_hash =
        state.try_get_tip(rwtxn)?.ok_or(state::Error::NoTip)?;
    let tip_header = archive.get_header(rwtxn, tip_block_hash)?;
    let tip_body = archive.get_body(rwtxn, tip_block_hash)?;
    // A side block applies exactly the mainchain interval after its parent's
    // frontier through its own frontier. Derive that interval from headers,
    // then reverse it one BlockInfo at a time.
    let start_block_hash = tip_header
        .prev_side_hash
        .map(|parent| archive.get_header(rwtxn, parent))
        .transpose()?
        .map(|parent| parent.prev_main_hash);
    let mainchain_blocks = mainchain_interval(
        archive,
        rwtxn,
        start_block_hash,
        tip_header.prev_main_hash,
    )?;
    let mut context = state.begin_disconnect_two_way_peg_data(rwtxn)?;
    // `mainchain_interval` is already newest-to-oldest.
    for block_hash in mainchain_blocks {
        let block_info = archive.get_main_block_info(rwtxn, &block_hash)?;
        state.disconnect_two_way_peg_block_info(
            rwtxn,
            block_hash,
            &block_info,
            &mut context,
        )?;
    }
    state.finish_disconnect_two_way_peg_data(rwtxn, context)?;
    archive.prune_txdb_block(rwtxn, tip_block_hash)?;
    let () = state.disconnect_tip(rwtxn, &tip_header, &tip_body)?;
    for transaction in tip_body.authorized_transactions() {
        let mut conflicts = false;
        for input in &transaction.transaction.inputs {
            if mempool
                .spent_utxos
                .try_get(rwtxn, input)
                .map_err(DbError::from)?
                .is_some()
            {
                conflicts = true;
                break;
            }
        }
        if conflicts {
            let txid = transaction.transaction.txid();
            for vout in 0..transaction.transaction.outputs.len() {
                let outpoint = OutPoint::Regular {
                    txid,
                    vout: vout as u32,
                };
                if let Some(InPoint::Regular {
                    txid: child_txid, ..
                }) = mempool
                    .spent_utxos
                    .try_get(rwtxn, &outpoint)
                    .map_err(DbError::from)?
                {
                    mempool.delete(rwtxn, child_txid)?;
                }
            }
            tracing::debug!(
                %txid,
                "not restoring disconnected transaction due to a mempool conflict"
            );
        } else {
            mempool.put(rwtxn, &transaction)?;
        }
    }
    Ok(())
}

/// Re-org to the specified tip, if it is better than the current tip.
/// The new tip block and all ancestor blocks must exist in the node's archive.
/// A result of `Ok(true)` indicates a successful re-org.
/// A result of `Ok(false)` indicates that no re-org was attempted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReorgErrorClass {
    Blocked,
    Fatal,
    InvalidBlock,
    Rejected,
}

fn reorg_error_class(err: &Error) -> ReorgErrorClass {
    match err {
        Error::MissingWithdrawalBundleMetadata { .. } => {
            ReorgErrorClass::Blocked
        }
        Error::InvalidBlock(_) => ReorgErrorClass::InvalidBlock,
        Error::State(err) if !state_error_is_infrastructure(err) => {
            ReorgErrorClass::Rejected
        }
        _ => ReorgErrorClass::Fatal,
    }
}

#[cfg(test)]
fn is_fatal_reorg_error(err: &Error) -> bool {
    reorg_error_class(err) == ReorgErrorClass::Fatal
}

fn replace_accumulator_at(
    archive: &Archive,
    rotxn: &sneed::RoTxn,
    target: Option<BlockHash>,
    accumulator: &mut Accumulator,
) -> Result<(), Error> {
    drop(std::mem::take(accumulator));
    *accumulator = archive.accumulator_at(rotxn, target)?;
    Ok(())
}

fn restore_committed_accumulator(
    env: &sneed::Env,
    archive: &Archive,
    state: &State,
    accumulator: &mut Accumulator,
) -> Result<(), Error> {
    let rotxn = env.read_txn().map_err(EnvError::from)?;
    let committed_tip = state.try_get_tip(&rotxn)?;
    replace_accumulator_at(archive, &rotxn, committed_tip, accumulator)
}

fn with_accumulator_recovery<T>(
    env: &sneed::Env,
    archive: &Archive,
    state: &State,
    accumulator: &mut Accumulator,
    operation: impl FnOnce(&mut Accumulator) -> Result<T, Error>,
) -> Result<T, Error> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        operation(accumulator)
    }));
    match result {
        Ok(result) => {
            if result.is_err()
                && let Err(restore_err) = restore_committed_accumulator(
                    env,
                    archive,
                    state,
                    accumulator,
                )
            {
                tracing::error!(
                    err = format!("{restore_err:#}"),
                    "failed to restore accumulator after aborted reorg"
                );
                return Err(restore_err);
            }
            result
        }
        Err(panic) => {
            if let Err(restore_err) =
                restore_committed_accumulator(env, archive, state, accumulator)
            {
                tracing::error!(
                    err = format!("{restore_err:#}"),
                    "failed to restore accumulator after panicked reorg"
                );
            }
            std::panic::resume_unwind(panic)
        }
    }
}

fn disconnect_for_mainchain_tip(
    env: &sneed::Env,
    archive: &Archive,
    mempool: &MemPool,
    state: &State,
    main_tip: bitcoin::BlockHash,
) -> Result<bool, Error> {
    let mut accumulator = state.utreexo_accumulator.lock();
    with_accumulator_recovery(
        env,
        archive,
        state,
        &mut accumulator,
        |accumulator| {
            disconnect_for_mainchain_tip_inner(
                env,
                archive,
                mempool,
                state,
                main_tip,
                accumulator,
            )
        },
    )
    .map_err(classify_mainchain_reconciliation_error)
}

fn disconnect_for_mainchain_tip_inner(
    env: &sneed::Env,
    archive: &Archive,
    mempool: &MemPool,
    state: &State,
    main_tip: bitcoin::BlockHash,
    accumulator: &mut Accumulator,
) -> Result<bool, Error> {
    let mut rwtxn = env.write_txn().map_err(EnvError::from)?;
    let Some(current_tip) = state.try_get_tip(&rwtxn)? else {
        return Ok(false);
    };
    let target =
        side_tip_on_mainchain_lineage(archive, &rwtxn, current_tip, main_tip)?;
    if target == Some(current_tip) {
        return Ok(false);
    }

    let current_height =
        state.try_get_height(&rwtxn)?.ok_or(state::Error::NoTip)?;
    let target_height = target
        .map(|target| archive.get_height(&rwtxn, target))
        .transpose()?;
    let disconnects = target_height
        .map_or(u64::from(current_height) + 1, |target_height| {
            u64::from(current_height - target_height)
        });
    if !accumulator_reorg_is_within_horizon(disconnects) {
        return Err(Error::MainchainDisconnectBeyondHorizon {
            disconnects,
            max_disconnects: ACCUMULATOR_REORG_HORIZON_BLOCKS,
        });
    }
    let retained_floor = archive.accumulator_reorg_floor(&rwtxn)?;
    if !accumulator_common_is_retained(target_height, retained_floor) {
        return Err(Error::MainchainDisconnectBelowAccumulatorFloor {
            target_height,
            retained_floor,
        });
    }

    tracing::info!(
        %current_tip,
        ?target,
        %main_tip,
        disconnects,
        "rewinding sidechain after mainchain disconnect"
    );
    replace_accumulator_at(archive, &rwtxn, target, accumulator)?;
    for _ in 0..disconnects {
        disconnect_tip_(&mut rwtxn, archive, mempool, state)?;
    }
    assert_eq!(state.try_get_tip(&rwtxn)?, target);
    rwtxn.commit().map_err(RwTxnError::from)?;
    Ok(true)
}

fn ready_mainchain_candidate(
    archive: &Archive,
    state: &State,
    rotxn: &sneed::RoTxn,
    side_block: BlockHash,
    main_tip: bitcoin::BlockHash,
) -> Result<Option<Tip>, Error> {
    if archive.try_get_header(rotxn, side_block)?.is_none() {
        return Ok(None);
    }
    let Some(main_block_hash) = best_main_verification_on_lineage(
        archive, rotxn, side_block, main_tip,
    )?
    else {
        return Ok(None);
    };
    let current_tip = state.try_get_tip(rotxn)?;
    let common_ancestor = current_tip
        .map(|current_tip| {
            archive.last_common_ancestor(rotxn, current_tip, side_block)
        })
        .transpose()?
        .flatten();
    if !archive
        .get_missing_bodies(rotxn, side_block, common_ancestor)?
        .is_empty()
    {
        return Ok(None);
    }
    Ok(Some(Tip {
        block_hash: side_block,
        main_block_hash,
    }))
}

/// Find the best fully archived sidechain tip verified anywhere on the
/// specified canonical mainchain lineage. Used for startup reconciliation,
/// where intermediate mainchain connect events may have happened while this
/// node was offline.
fn best_ready_mainchain_candidate(
    archive: &Archive,
    state: &State,
    rotxn: &sneed::RoTxn,
    main_tip: bitcoin::BlockHash,
) -> Result<Option<Tip>, Error> {
    let mut best = None;
    for verified_tip in archive.get_mainchain_verified_tips(rotxn, main_tip)? {
        let Some(candidate) = ready_mainchain_candidate(
            archive,
            state,
            rotxn,
            verified_tip.block_hash,
            main_tip,
        )?
        else {
            continue;
        };
        best = match best {
            None => Some(candidate),
            Some(current_best) => archive
                .better_tip(rotxn, current_best, candidate)?
                .or(Some(current_best)),
        };
    }
    Ok(best)
}

fn reorg_to_tip_on_mainchain(
    env: &sneed::Env,
    archive: &Archive,
    mempool: &MemPool,
    state: &State,
    #[cfg(feature = "zmq")] zmq_pub_handler: &ZmqPubHandler,
    new_tip: Tip,
    main_tip: bitcoin::BlockHash,
) -> Result<bool, Error> {
    // Keep accumulator readers blocked until the database and forest agree.
    // If the database transaction aborts, reconstruct the forest from the
    // committed tip before releasing the lock.
    let mut accumulator = state.utreexo_accumulator.lock();
    with_accumulator_recovery(
        env,
        archive,
        state,
        &mut accumulator,
        |accumulator| {
            reorg_to_tip_inner(
                env,
                archive,
                mempool,
                state,
                #[cfg(feature = "zmq")]
                zmq_pub_handler,
                new_tip,
                main_tip,
                accumulator,
            )
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn reorg_to_tip_inner(
    env: &sneed::Env,
    archive: &Archive,
    mempool: &MemPool,
    state: &State,
    #[cfg(feature = "zmq")] zmq_pub_handler: &ZmqPubHandler,
    mut new_tip: Tip,
    main_tip: bitcoin::BlockHash,
    accumulator: &mut Accumulator,
) -> Result<bool, Error> {
    let mut rwtxn = env.write_txn().map_err(EnvError::from)?;
    let Some(main_block_hash) = best_main_verification_on_lineage(
        archive,
        &rwtxn,
        new_tip.block_hash,
        main_tip,
    )?
    else {
        tracing::debug!(?new_tip, %main_tip, "candidate tip is not verified on the current mainchain");
        return Ok(false);
    };
    new_tip.main_block_hash = main_block_hash;
    let tip_height = state.try_get_height(&rwtxn)?;
    let tip_hash = state.try_get_tip(&rwtxn)?;
    let comparable_tip = tip_hash
        .map(|tip_hash| {
            let bmm_verification = best_main_verification_on_lineage(
                archive, &rwtxn, tip_hash, main_tip,
            )?;
            Ok::<_, Error>(bmm_verification.map(|main_block_hash| Tip {
                block_hash: tip_hash,
                main_block_hash,
            }))
        })
        .transpose()?
        .flatten();
    let mainchain_reconciliation_required =
        tip_hash.is_some() && comparable_tip.is_none();
    if let Some(tip) = comparable_tip {
        // check that new tip is better than current tip
        if archive.better_tip(&rwtxn, tip, new_tip)? != Some(new_tip) {
            tracing::debug!(
                ?tip,
                ?new_tip,
                "New tip is not better than current tip"
            );
            return Ok(false);
        }
    }
    let common_ancestor = if let Some(tip_hash) = tip_hash {
        archive.last_common_ancestor(&rwtxn, tip_hash, new_tip.block_hash)?
    } else {
        None
    };
    let common_ancestor_height = common_ancestor
        .map(|common_ancestor| archive.get_height(&rwtxn, common_ancestor))
        .transpose()?;
    let disconnects = tip_height.map_or(0_u64, |tip_height| {
        common_ancestor_height.map_or(u64::from(tip_height) + 1, |height| {
            u64::from(tip_height - height)
        })
    });
    if !accumulator_reorg_is_within_horizon(disconnects) {
        if mainchain_reconciliation_required {
            return Err(Error::MainchainDisconnectBeyondHorizon {
                disconnects,
                max_disconnects: ACCUMULATOR_REORG_HORIZON_BLOCKS,
            });
        }
        tracing::warn!(
            ?new_tip,
            disconnects,
            max_disconnects = ACCUMULATOR_REORG_HORIZON_BLOCKS,
            "refusing reorg beyond the retained accumulator horizon"
        );
        return Ok(false);
    }
    let retained_reorg_floor = archive.accumulator_reorg_floor(&rwtxn)?;
    let common_ancestor_is_retained = accumulator_common_is_retained(
        common_ancestor_height,
        retained_reorg_floor,
    );
    if disconnects != 0 && !common_ancestor_is_retained {
        if mainchain_reconciliation_required {
            return Err(Error::MainchainDisconnectBelowAccumulatorFloor {
                target_height: common_ancestor_height,
                retained_floor: retained_reorg_floor,
            });
        }
        tracing::warn!(
            ?new_tip,
            ?common_ancestor,
            ?common_ancestor_height,
            retained_reorg_floor,
            "refusing reorg below the retained accumulator floor"
        );
        return Ok(false);
    }
    // Keep only hashes here. Replacement branches can be much longer than the
    // disconnect horizon, so retaining every deserialized body would make peak
    // memory proportional to the entire branch.
    let blocks_to_apply: Vec<BlockHash> = archive
        .ancestors(&rwtxn, new_tip.block_hash)
        .take_while(|block_hash| {
            Ok(common_ancestor
                .is_none_or(|common_ancestor| *block_hash != common_ancestor))
        })
        .collect()?;
    // Check that all necessary bodies exist before disconnecting tip, while
    // dropping each deserialized body before loading the next one.
    for block_hash in &blocks_to_apply {
        drop(archive.get_body(&rwtxn, *block_hash)?);
    }
    // Disconnect tip until common ancestor is reached
    if disconnects != 0 {
        tracing::debug!(
            ?tip_hash,
            ?tip_height,
            ?common_ancestor,
            ?common_ancestor_height,
            "Disconnecting tip until common ancestor is reached"
        );
        replace_accumulator_at(archive, &rwtxn, common_ancestor, accumulator)?;
        for _ in 0..disconnects {
            let () = disconnect_tip_(&mut rwtxn, archive, mempool, state)?;
        }
    }
    {
        let tip_hash = state.try_get_tip(&rwtxn)?;
        assert_eq!(tip_hash, common_ancestor);
    }
    let common_ancestor_prev_main_hash = common_ancestor
        .map(|common_ancestor| archive.get_header(&rwtxn, common_ancestor))
        .transpose()?
        .map(|header| header.prev_main_hash);
    let mut mainchain_block_batch =
        if let Some(block_hash) = blocks_to_apply.first() {
            let header = archive.get_header(&rwtxn, *block_hash)?;
            mainchain_interval(
                archive,
                &rwtxn,
                common_ancestor_prev_main_hash,
                header.prev_main_hash,
            )?
        } else {
            Vec::new()
        };
    let mut applied_main_frontier = common_ancestor_prev_main_hash
        .unwrap_or_else(bitcoin::BlockHash::all_zeros);
    // Apply every replacement block in the same transaction as the
    // disconnects. Committing inside this loop would expose a partial reorg.
    for block_hash in blocks_to_apply.iter().rev() {
        let header = archive.get_header(&rwtxn, *block_hash)?;
        let body = archive.get_body(&rwtxn, *block_hash)?;
        let mainchain_blocks = {
            let mut mainchain_blocks = Vec::new();
            if header.prev_main_hash != applied_main_frontier {
                let mut reached_frontier = false;
                while let Some(block_hash) = mainchain_block_batch.pop() {
                    mainchain_blocks.push(block_hash);
                    if block_hash == header.prev_main_hash {
                        reached_frontier = true;
                        break;
                    }
                }
                if !reached_frontier {
                    return Err(Error::InvalidMainchainInterval {
                        start: applied_main_frontier,
                        end: header.prev_main_hash,
                    });
                }
            }
            mainchain_blocks
        };
        applied_main_frontier = header.prev_main_hash;
        let () = match connect_tip_with_mainchain_blocks_(
            &mut rwtxn,
            archive,
            mempool,
            state,
            &header,
            &body,
            &mainchain_blocks,
            accumulator,
        ) {
            Ok(()) => (),
            Err(err) => {
                if matches!(err, Error::InvalidBlock(_)) {
                    // The stored body for this block failed validation (e.g. a peer
                    // supplied a body whose contents do not match the header's merkle
                    // root). Abort the reorg and discard the invalid body from the
                    // archive so that the block is reported missing again and the real
                    // body is re-requested, instead of the archive staying poisoned.
                    drop(rwtxn);
                    let mut rwtxn = env.write_txn()?;
                    let () = archive.delete_body(
                        &mut rwtxn,
                        header.hash(),
                        &body,
                    )?;
                    rwtxn.commit()?;
                }
                return Err(err);
            }
        };
    }
    let tip = state.try_get_tip(&rwtxn)?;
    assert_eq!(tip, Some(new_tip.block_hash));
    rwtxn.commit().map_err(RwTxnError::from)?;
    tracing::info!("synced to tip: {}", new_tip.block_hash);
    #[cfg(feature = "zmq")]
    {
        for (idx, block_hash) in blocks_to_apply.into_iter().rev().enumerate() {
            let height =
                common_ancestor_height.map(|h| h + 1).unwrap_or(0) + idx as u32;
            let mut zmq_msg = zeromq::ZmqMessage::from("hashblock");
            zmq_msg.push_back(bytes::Bytes::copy_from_slice(&block_hash.0));
            zmq_msg.push_back(bytes::Bytes::copy_from_slice(
                &height.to_le_bytes(),
            ));
            zmq_pub_handler.tx.unbounded_send(zmq_msg).unwrap();
        }
    }
    Ok(true)
}

#[derive(Clone)]
struct NetTaskContext {
    env: sneed::Env,
    archive: Archive,
    mainchain_task: MainchainTaskHandle,
    mempool: MemPool,
    net: Net,
    state: State,
    #[cfg(feature = "zmq")]
    zmq_pub_handler: Arc<ZmqPubHandler>,
}

/// Message indicating a tip that is ready to reorg to, with the address of the
/// peer connection that caused the request, if it originated from a peer.
/// If the request originates from this node, then the socket address is
/// None.
/// An optional oneshot sender can be used receive the result of attempting
/// to reorg to the new tip, on the corresponding oneshot receiver.
type NewTipReadyMessage =
    (Tip, Option<SocketAddr>, Option<oneshot::Sender<bool>>);

/// Tips whose atomic application is waiting for branch-independent withdrawal
/// bundle metadata. The optional address identifies the peer that supplied the
/// tip, if any, and is retained for the retry.
type MetadataBlockedTips = HashMap<M6id, HashMap<Tip, Option<SocketAddr>>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MainchainConnectKind {
    Bootstrap,
    Next,
    Stale,
}

fn classify_mainchain_connect(
    current_tip: Option<bitcoin::BlockHash>,
    block_hash: bitcoin::BlockHash,
    prev_block_hash: bitcoin::BlockHash,
) -> MainchainConnectKind {
    match current_tip {
        None => MainchainConnectKind::Bootstrap,
        Some(current_tip) if block_hash == current_tip => {
            MainchainConnectKind::Stale
        }
        Some(current_tip) if prev_block_hash == current_tip => {
            MainchainConnectKind::Next
        }
        Some(_) => MainchainConnectKind::Stale,
    }
}

fn block_tip_on_withdrawal_bundle_metadata(
    ctxt: &NetTaskContext,
    blocked_tips: &mut MetadataBlockedTips,
    missing_withdrawal_bundles: &RwLock<HashSet<M6id>>,
    tip: Tip,
    source: Option<SocketAddr>,
    event_block_hash: bitcoin::BlockHash,
    m6id: M6id,
) {
    blocked_tips
        .entry(m6id)
        .or_default()
        .entry(tip)
        .or_insert(source);
    missing_withdrawal_bundles.write().insert(m6id);
    let requested_from = ctxt.net.request_withdrawal_bundle(m6id, source);
    tracing::warn!(
        ?tip,
        ?source,
        %event_block_hash,
        %m6id,
        requested_from,
        "atomic reorg blocked pending withdrawal bundle metadata"
    );
}

struct NetTask {
    ctxt: NetTaskContext,
    missing_withdrawal_bundles: Arc<RwLock<HashSet<M6id>>>,
    /// Receive a request to forward to the mainchain task, with the address of
    /// the peer connection that caused the request, and the peer state ID of
    /// the request
    forward_mainchain_task_request_rx:
        UnboundedReceiver<(mainchain_task::Request, SocketAddr, PeerStateId)>,
    /// Push a request to forward to the mainchain task, with the address of
    /// the peer connection that caused the request, and the peer state ID of
    /// the request
    forward_mainchain_task_request_tx:
        UnboundedSender<(mainchain_task::Request, SocketAddr, PeerStateId)>,
    mainchain_task_response_rx: UnboundedReceiver<mainchain_task::Response>,
    mainchain_event_rx: UnboundedReceiver<mainchain_task::Event>,
    /// Receive a tip that is ready to reorg to, with the address of the peer
    /// connection that caused the request, if it originated from a peer.
    /// If the request originates from this node, then the socket address is
    /// None.
    /// An optional oneshot sender can be used receive the result of attempting
    /// to reorg to the new tip, on the corresponding oneshot receiver.
    new_tip_ready_rx: UnboundedReceiver<NewTipReadyMessage>,
    /// Push a tip that is ready to reorg to, with the address of the peer
    /// connection that caused the request, if it originated from a peer.
    /// If the request originates from this node, then the socket address is
    /// None.
    /// An optional oneshot sender can be used receive the result of attempting
    /// to reorg to the new tip, on the corresponding oneshot receiver.
    new_tip_ready_tx: UnboundedSender<NewTipReadyMessage>,
    peer_info_rx: PeerInfoRx,
}

impl NetTask {
    #[allow(clippy::too_many_arguments)]
    fn handle_response(
        ctxt: &NetTaskContext,
        // Attempt to switch to a descendant tip once a body has been
        // stored, if all other ancestor bodies are available.
        // Each descendant tip maps to the peers that sent that tip.
        descendant_tips: &mut HashMap<
            crate::types::BlockHash,
            HashMap<Tip, HashSet<SocketAddr>>,
        >,
        canonical_main_tip: Option<bitcoin::BlockHash>,
        new_tip_ready_tx: &UnboundedSender<NewTipReadyMessage>,
        blocked_tips: &mut MetadataBlockedTips,
        missing_withdrawal_bundles: &RwLock<HashSet<M6id>>,
        addr: SocketAddr,
        resp: PeerResponse,
        req: PeerRequest,
    ) -> Result<(), Error> {
        tracing::debug!(?req, ?resp, "starting response handler");
        match (req, resp) {
            (
                PeerRequest::GetBlock(
                    req @ peer_message::GetBlockRequest {
                        block_hash,
                        descendant_tip: Some(descendant_tip),
                        ancestor,
                        peer_state_id: Some(peer_state_id),
                    },
                ),
                ref resp @ PeerResponse::Block {
                    ref header,
                    ref body,
                },
            ) => {
                if header.hash() != block_hash {
                    // Invalid response
                    tracing::warn!(%addr, ?req, ?resp,"Invalid response from peer; unexpected block hash");
                    let () = ctxt.net.remove_active_peer(addr);
                    return Ok::<_, Error>(());
                }
                {
                    let mut rwtxn =
                        ctxt.env.write_txn().map_err(EnvError::from)?;
                    let () =
                        ctxt.archive.put_body(&mut rwtxn, block_hash, body)?;
                    rwtxn.commit().map_err(RwTxnError::from)?;
                }
                // Notify the peer connection if all requested block bodies are
                // now available
                {
                    let rotxn = ctxt.env.read_txn().map_err(EnvError::from)?;
                    let ancestor_height = if let Some(ancestor) = ancestor {
                        Some(ctxt.archive.get_height(&rotxn, ancestor)?)
                    } else {
                        None
                    };
                    let earliest_missing_body = ctxt
                        .archive
                        .iter_missing_bodies(
                            &rotxn,
                            block_hash,
                            ancestor_height.map_or(0, |height| height + 1),
                        )
                        .next()?;
                    if let Some(earliest_missing_body) = earliest_missing_body {
                        descendant_tips
                            .entry(earliest_missing_body)
                            .or_default()
                            .entry(descendant_tip)
                            .or_default()
                            .insert(addr);
                    } else {
                        let message = PeerConnectionMessage::BodiesAvailable(
                            peer_state_id,
                        );
                        let _: bool =
                            ctxt.net.push_internal_message(message, addr);
                    }
                }
                // Check if any new tips can be applied,
                // and send new tip ready if so
                {
                    let rotxn = ctxt.env.read_txn().map_err(EnvError::from)?;
                    let tip = ctxt
                        .state
                        .try_get_tip(&rotxn)?
                        .map(|tip_hash| {
                            tip_for_side_block(
                                &ctxt.archive,
                                &rotxn,
                                tip_hash,
                                canonical_main_tip,
                            )?
                            .ok_or_else(|| {
                                Error::Archive(archive::Error::NoBmmResult(
                                    tip_hash,
                                ))
                            })
                        })
                        .transpose()?;
                    let descendant_tip = if canonical_main_tip.is_some() {
                        tip_for_side_block(
                            &ctxt.archive,
                            &rotxn,
                            descendant_tip.block_hash,
                            canonical_main_tip,
                        )?
                    } else {
                        Some(descendant_tip)
                    };
                    let block_tip = if let Some(descendant_tip) = descendant_tip
                    {
                        // Find the newest verification of this block that is
                        // an ancestor of the advertised descendant's
                        // verification.
                        best_main_verification_on_lineage(
                            &ctxt.archive,
                            &rotxn,
                            block_hash,
                            descendant_tip.main_block_hash,
                        )?
                        .map(|main_block_hash| Tip {
                            block_hash,
                            main_block_hash,
                        })
                    } else {
                        None
                    };

                    if let Some(block_tip) = block_tip
                        && header.prev_side_hash
                            == tip.map(|tip| tip.block_hash)
                    {
                        tracing::trace!(
                            ?block_tip,
                            origin = %addr,
                            "sending new tip ready"
                        );
                        let () = new_tip_ready_tx
                            .unbounded_send((block_tip, Some(addr), None))
                            .map_err(Error::SendNewTipReady)?;
                    }
                    let Some(block_descendant_tips) =
                        descendant_tips.remove(&block_hash)
                    else {
                        return Ok(());
                    };
                    for (descendant_tip, sources) in block_descendant_tips {
                        let descendant_tip = if canonical_main_tip.is_some() {
                            let Some(descendant_tip) = tip_for_side_block(
                                &ctxt.archive,
                                &rotxn,
                                descendant_tip.block_hash,
                                canonical_main_tip,
                            )?
                            else {
                                continue;
                            };
                            descendant_tip
                        } else {
                            descendant_tip
                        };
                        let common_ancestor_height = if let Some(tip) = tip
                            && let Some(common_ancestor) =
                                ctxt.archive.last_common_ancestor(
                                    &rotxn,
                                    descendant_tip.block_hash,
                                    tip.block_hash,
                                )? {
                            Some(
                                ctxt.archive
                                    .get_height(&rotxn, common_ancestor)?,
                            )
                        } else {
                            None
                        };
                        let earliest_missing_body = ctxt
                            .archive
                            .iter_missing_bodies(
                                &rotxn,
                                descendant_tip.block_hash,
                                common_ancestor_height
                                    .map_or(0, |height| height + 1),
                            )
                            .next()?;
                        // If a better tip is ready, send a notification
                        'better_tip: {
                            let next_tip = if let Some(earliest_missing_body) =
                                earliest_missing_body
                            {
                                descendant_tips
                                    .entry(earliest_missing_body)
                                    .or_default()
                                    .entry(descendant_tip)
                                    .or_default()
                                    .extend(sources.iter().cloned());

                                // Parent of the earlist missing body
                                ctxt.archive
                                    .get_header(&rotxn, earliest_missing_body)?
                                    .prev_side_hash
                                    .map(|tip_hash| {
                                        tip_for_side_block(
                                            &ctxt.archive,
                                            &rotxn,
                                            tip_hash,
                                            canonical_main_tip,
                                        )
                                    })
                                    .transpose()?
                                    .flatten()
                            } else {
                                Some(descendant_tip)
                            };
                            let Some(next_tip) = next_tip else {
                                break 'better_tip;
                            };
                            if let Some(tip) = tip
                                && ctxt
                                    .archive
                                    .better_tip(&rotxn, tip, next_tip)?
                                    != Some(next_tip)
                            {
                                break 'better_tip;
                            } else {
                                tracing::debug!(
                                    new_tip = ?next_tip,
                                    "sending new tip ready to sources"
                                );
                                for addr in sources {
                                    tracing::trace!(%addr, new_tip = ?next_tip, "sending new tip ready");
                                    let () = new_tip_ready_tx
                                        .unbounded_send((
                                            next_tip,
                                            Some(addr),
                                            None,
                                        ))
                                        .map_err(Error::SendNewTipReady)?;
                                }
                            }
                        }
                    }
                }
                Ok(())
            }
            (
                PeerRequest::GetBlock(peer_message::GetBlockRequest {
                    block_hash: req_block_hash,
                    descendant_tip: Some(_),
                    ancestor: _,
                    peer_state_id: Some(_),
                }),
                PeerResponse::NoBlock {
                    block_hash: resp_block_hash,
                },
            ) if req_block_hash == resp_block_hash => Ok(()),
            (
                PeerRequest::GetHeaders(
                    ref req @ peer_message::GetHeadersRequest {
                        ref start,
                        end,
                        height: Some(height),
                        peer_state_id: Some(peer_state_id),
                    },
                ),
                PeerResponse::Headers(headers),
            ) => {
                // check that the end header is as requested
                let Some(end_header) = headers.last() else {
                    tracing::warn!(%addr, ?req, "Invalid response from peer; missing end header");
                    let () = ctxt.net.remove_active_peer(addr);
                    return Ok(());
                };
                let end_header_hash = end_header.hash();
                if end_header_hash != end {
                    tracing::warn!(%addr, ?req, ?end_header,"Invalid response from peer; unexpected end header");
                    let () = ctxt.net.remove_active_peer(addr);
                    return Ok(());
                }
                // Must be at least one header due to previous check
                let start_hash = headers.first().unwrap().prev_side_hash;
                // check that the first header is after a start block
                if let Some(start_hash) = start_hash
                    && !start.contains(&start_hash)
                {
                    tracing::warn!(%addr, ?req, %start_hash, "Invalid response from peer; invalid start hash");
                    let () = ctxt.net.remove_active_peer(addr);
                    return Ok(());
                }
                // check that the end header height is as expected
                {
                    let rotxn = ctxt.env.read_txn().map_err(EnvError::from)?;
                    let start_height = if let Some(start_hash) = start_hash {
                        Some(ctxt.archive.get_height(&rotxn, start_hash)?)
                    } else {
                        None
                    };
                    let end_height = match start_height {
                        Some(start_height) => {
                            start_height + headers.len() as u32
                        }
                        None => headers.len() as u32 - 1,
                    };
                    if end_height != height {
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
                    prev_side_hash = Some(header.hash());
                }
                // Store new headers
                let () = tokio::task::block_in_place(|| {
                    let mut rwtxn =
                        ctxt.env.write_txn().map_err(EnvError::from)?;
                    for header in &headers {
                        let block_hash = header.hash();
                        if ctxt
                            .archive
                            .try_get_header(&rwtxn, block_hash)?
                            .is_none()
                        {
                            if let Some(parent) = header.prev_side_hash
                                && ctxt
                                    .archive
                                    .try_get_header(&rwtxn, parent)?
                                    .is_none()
                            {
                                break;
                            } else {
                                ctxt.archive.put_header(&mut rwtxn, header)?;
                            }
                        }
                    }
                    rwtxn.commit().map_err(RwTxnError::from)?;
                    Ok::<_, Error>(())
                })?;
                // Notify peer connection that headers are available
                let message = PeerConnectionMessage::Headers(peer_state_id);
                let _: bool = ctxt.net.push_internal_message(message, addr);
                Ok(())
            }
            (
                PeerRequest::GetHeaders(peer_message::GetHeadersRequest {
                    start: _,
                    end,
                    height: _,
                    peer_state_id: _,
                }),
                PeerResponse::NoHeader { block_hash },
            ) if end == block_hash => Ok(()),
            (
                PeerRequest::GetWithdrawalBundle(
                    peer_message::GetWithdrawalBundleRequest {
                        m6id: requested_m6id,
                    },
                ),
                PeerResponse::WithdrawalBundle {
                    m6id: response_m6id,
                    metadata,
                },
            ) => {
                if requested_m6id != response_m6id
                    || metadata.compute_m6id() != requested_m6id
                    || !metadata.has_valid_inputs_commitment()
                    || !metadata.is_within_size_limit()
                {
                    tracing::warn!(
                        %addr,
                        %requested_m6id,
                        %response_m6id,
                        "invalid withdrawal bundle metadata response"
                    );
                    let () = ctxt.net.remove_active_peer(addr);
                    return Ok(());
                }
                let mut rwtxn = ctxt.env.write_txn().map_err(EnvError::from)?;
                ctxt.state
                    .put_withdrawal_bundle_metadata(&mut rwtxn, &metadata)?;
                rwtxn.commit().map_err(RwTxnError::from)?;

                let retry_tips = blocked_tips.remove(&requested_m6id);
                missing_withdrawal_bundles.write().remove(&requested_m6id);
                let retry_count = retry_tips.as_ref().map_or(0, HashMap::len);
                tracing::info!(
                    %requested_m6id,
                    %addr,
                    retry_count,
                    "recovered withdrawal bundle metadata"
                );
                if let Some(retry_tips) = retry_tips {
                    for (tip, source) in retry_tips {
                        new_tip_ready_tx
                            .unbounded_send((tip, source, None))
                            .map_err(Error::SendNewTipReady)?;
                    }
                }
                Ok(())
            }
            (
                PeerRequest::GetWithdrawalBundle(
                    peer_message::GetWithdrawalBundleRequest {
                        m6id: requested_m6id,
                    },
                ),
                PeerResponse::NoWithdrawalBundle {
                    m6id: response_m6id,
                },
            ) if requested_m6id == response_m6id => {
                tracing::trace!(
                    %addr,
                    %requested_m6id,
                    "peer does not have withdrawal bundle metadata"
                );
                Ok(())
            }
            (
                PeerRequest::PushTransaction(
                    peer_message::PushTransactionRequest { transaction: _ },
                ),
                PeerResponse::TransactionAccepted(_),
            ) => Ok(()),
            (
                PeerRequest::PushTransaction(
                    peer_message::PushTransactionRequest { transaction: _ },
                ),
                PeerResponse::TransactionRejected(_),
            ) => Ok(()),
            (
                req @ (PeerRequest::GetBlock { .. }
                | PeerRequest::GetHeaders { .. }
                | PeerRequest::GetWithdrawalBundle { .. }
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
        tracing::debug!("starting net task");
        #[derive(Debug)]
        enum MailboxItem {
            AcceptConnection(
                Result<
                    Option<SocketAddr>,
                    <net::error::AcceptConnection as Split>::Fatal,
                >,
            ),
            // Forward a mainchain task request, along with the peer that
            // caused the request, and the peer state ID of the request
            ForwardMainchainTaskRequest(
                mainchain_task::Request,
                SocketAddr,
                PeerStateId,
            ),
            MainchainTaskResponse(mainchain_task::Response),
            MainchainEvent(Option<mainchain_task::Event>),
            // Apply new tip from peer or self.
            // An optional oneshot sender can be used receive the result of
            // attempting to reorg to the new tip, on the corresponding oneshot
            // receiver.
            NewTipReady(Tip, Option<SocketAddr>, Option<oneshot::Sender<bool>>),
            PeerInfo(Option<(SocketAddr, Option<PeerConnectionInfo>)>),
            RetryMissingWithdrawalBundles,
            // Signal to reconnect to a peer
            ReconnectPeer(SocketAddr),
        }
        let accept_connections = stream::try_unfold((), |()| {
            let env = self.ctxt.env.clone();
            let net = self.ctxt.net.clone();
            let fut = async move {
                let maybe_socket_addr =
                    net.accept_incoming(env).await.into_nested()?;
                // / Return:
                // - The value to yield (maybe_socket_addr)
                // - The state for the next iteration (())
                // Wrapped in Result and Option
                Result::<_, _>::Ok(Some((maybe_socket_addr, ())))
            };
            Box::pin(fut)
        })
        .filter_map(async |item| match item {
            Ok(Ok(maybe_socket_addr)) => Some(Ok(maybe_socket_addr)),
            Ok(Err(non_fatal_err)) => {
                // type the error explicitly
                let non_fatal_err:
                    <net::error::AcceptConnection as Split>::Jfyi =
                    non_fatal_err;
                let non_fatal_err = anyhow::Error::from(non_fatal_err);
                tracing::error!(
                    "Failed to accept connection: {non_fatal_err:#}"
                );
                None
            }
            Err(fatal_err) => Some(Err(fatal_err)),
        })
        .map(MailboxItem::AcceptConnection);
        let forward_request_stream = self
            .forward_mainchain_task_request_rx
            .map(|(request, addr, peer_state_id)| {
                MailboxItem::ForwardMainchainTaskRequest(
                    request,
                    addr,
                    peer_state_id,
                )
            });
        let mainchain_task_response_stream = self
            .mainchain_task_response_rx
            .map(MailboxItem::MainchainTaskResponse);
        let mainchain_event_stream =
            StreamNotifyClose::new(self.mainchain_event_rx)
                .map(MailboxItem::MainchainEvent);
        let new_tip_ready_stream =
            self.new_tip_ready_rx.map(|(block_hash, addr, resp_tx)| {
                MailboxItem::NewTipReady(block_hash, addr, resp_tx)
            });
        let peer_info_stream = StreamNotifyClose::new(self.peer_info_rx)
            .map(MailboxItem::PeerInfo);
        let (reconnect_peer_spawner, reconnect_peer_rx) = join_set::new();
        let reconnect_peer_stream = reconnect_peer_rx
            .map(|addr| MailboxItem::ReconnectPeer(addr.unwrap()));
        let mut metadata_retry_interval =
            tokio::time::interval(WITHDRAWAL_METADATA_RETRY_INTERVAL);
        metadata_retry_interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let metadata_retry_stream =
            IntervalStream::new(metadata_retry_interval)
                .map(|_| MailboxItem::RetryMissingWithdrawalBundles);
        let mut mailbox_stream = stream::select_all([
            accept_connections.boxed(),
            forward_request_stream.boxed(),
            mainchain_task_response_stream.boxed(),
            mainchain_event_stream.boxed(),
            new_tip_ready_stream.boxed(),
            peer_info_stream.boxed(),
            reconnect_peer_stream.boxed(),
            metadata_retry_stream.boxed(),
        ]);
        // Attempt to switch to a descendant tip once a body has been
        // stored, if all other ancestor bodies are available.
        // Each descendant tip maps to the peers that sent that tip.
        let mut descendant_tips = HashMap::<
            crate::types::BlockHash,
            HashMap<Tip, HashSet<SocketAddr>>,
        >::new();
        let mut metadata_blocked_tips = MetadataBlockedTips::new();
        // Map associating mainchain task requests with the peer(s) that
        // caused the request, and the request peer state ID
        let mut mainchain_task_request_sources = HashMap::<
            mainchain_task::Request,
            HashSet<(SocketAddr, PeerStateId)>,
        >::new();
        let mut canonical_main_tip = None;
        let mut deferred_tip_ready = Vec::<NewTipReadyMessage>::new();
        while let Some(mailbox_item) = mailbox_stream.next().await {
            tracing::trace!(?mailbox_item, "received new mailbox item");
            match mailbox_item {
                MailboxItem::AcceptConnection(res) => match res {
                    // We received a connection new incoming network connection, but no peer
                    // was added
                    Ok(None) => {
                        continue;
                    }
                    Ok(Some(addr)) => {
                        tracing::trace!(%addr, "accepted new incoming connection");
                        for m6id in self
                            .missing_withdrawal_bundles
                            .read()
                            .iter()
                            .copied()
                        {
                            self.ctxt
                                .net
                                .request_withdrawal_bundle(m6id, Some(addr));
                        }
                    }
                    Err(fatal_err) => {
                        // explicitly type error
                        let fatal_err: <net::error::AcceptConnection as Split>::Fatal =
                            fatal_err;
                        let fatal_err = anyhow::Error::from(fatal_err);
                        tracing::error!(
                            "failed to accept connection: {fatal_err:#}"
                        );
                    }
                },
                MailboxItem::ForwardMainchainTaskRequest(
                    request,
                    peer,
                    peer_state_id,
                ) => {
                    mainchain_task_request_sources
                        .entry(request)
                        .or_default()
                        .insert((peer, peer_state_id));
                    let () = self
                        .ctxt
                        .mainchain_task
                        .request(request)
                        .map_err(|_| Error::SendMainchainTaskRequest)?;
                }
                MailboxItem::MainchainTaskResponse(response) => {
                    let request = (&response).into();
                    match response {
                        mainchain_task::Response::AncestorInfos(
                            block_hash,
                            res,
                        ) => {
                            let Some(sources) =
                                mainchain_task_request_sources.remove(&request)
                            else {
                                continue;
                            };
                            let res = res.map_err(Arc::new);
                            for (addr, peer_state_id) in sources {
                                let message = match res {
                                    Ok(true) => PeerConnectionMessage::MainchainAncestors(
                                        peer_state_id,
                                    ),
                                    Ok(false) => PeerConnectionMessage::MainchainAncestorsError(
                                        anyhow::anyhow!("Requested block was not available: {block_hash}")
                                    ),
                                    Err(ref err) => PeerConnectionMessage::MainchainAncestorsError(
                                        anyhow::Error::from(err.clone())
                                    )
                                };
                                let _: bool = self
                                    .ctxt
                                    .net
                                    .push_internal_message(message, addr);
                            }
                        }
                    }
                }
                MailboxItem::MainchainEvent(None) => {
                    return Err(Error::MainchainEventRxClosed);
                }
                MailboxItem::MainchainEvent(Some(Err(err))) => {
                    return Err(Error::MainchainEvent(Box::new(err)));
                }
                MailboxItem::MainchainEvent(Some(Ok(event))) => match event {
                    mainchain::Event::ConnectBlock {
                        header_info,
                        block_info,
                    } => {
                        let main_tip = header_info.block_hash;
                        let connect_kind = classify_mainchain_connect(
                            canonical_main_tip,
                            main_tip,
                            header_info.prev_block_hash,
                        );
                        if connect_kind == MainchainConnectKind::Stale {
                            tracing::trace!(
                                %main_tip,
                                ?canonical_main_tip,
                                "ignoring stale mainchain connect after initial tip reconciliation"
                            );
                            continue;
                        }
                        let mut attempted_tip = None;
                        let reorg_result = task::block_in_place(|| {
                            let rotxn = self
                                .ctxt
                                .env
                                .read_txn()
                                .map_err(EnvError::from)?;
                            let candidate = if connect_kind
                                == MainchainConnectKind::Bootstrap
                            {
                                best_ready_mainchain_candidate(
                                    &self.ctxt.archive,
                                    &self.ctxt.state,
                                    &rotxn,
                                    main_tip,
                                )?
                            } else if let Some(side_block) =
                                block_info.bmm_commitment
                            {
                                ready_mainchain_candidate(
                                    &self.ctxt.archive,
                                    &self.ctxt.state,
                                    &rotxn,
                                    side_block,
                                    main_tip,
                                )?
                            } else {
                                None
                            };
                            drop(rotxn);
                            if let Some(candidate) = candidate {
                                attempted_tip = Some(candidate);
                                reorg_to_tip_on_mainchain(
                                    &self.ctxt.env,
                                    &self.ctxt.archive,
                                    &self.ctxt.mempool,
                                    &self.ctxt.state,
                                    #[cfg(feature = "zmq")]
                                    &self.ctxt.zmq_pub_handler,
                                    candidate,
                                    main_tip,
                                )
                            } else {
                                disconnect_for_mainchain_tip(
                                    &self.ctxt.env,
                                    &self.ctxt.archive,
                                    &self.ctxt.mempool,
                                    &self.ctxt.state,
                                    main_tip,
                                )
                            }
                        });
                        match reorg_result {
                            Ok(_) => {}
                            Err(err) => match reorg_error_class(&err) {
                                ReorgErrorClass::Blocked => {
                                    let Error::MissingWithdrawalBundleMetadata {
                                            event_block_hash,
                                            m6id,
                                        } = err
                                        else {
                                            unreachable!()
                                        };
                                    let tip = attempted_tip.expect(
                                            "metadata can only block candidate application",
                                        );
                                    block_tip_on_withdrawal_bundle_metadata(
                                        &self.ctxt,
                                        &mut metadata_blocked_tips,
                                        &self.missing_withdrawal_bundles,
                                        tip,
                                        None,
                                        event_block_hash,
                                        m6id,
                                    );
                                }
                                ReorgErrorClass::InvalidBlock
                                | ReorgErrorClass::Rejected => {
                                    tracing::warn!(
                                        %main_tip,
                                        err = format!(
                                            "{:#}",
                                            ErrorChain::new(&err)
                                        ),
                                        "sidechain candidate rejected while processing mainchain event"
                                    );
                                    task::block_in_place(|| {
                                        disconnect_for_mainchain_tip(
                                            &self.ctxt.env,
                                            &self.ctxt.archive,
                                            &self.ctxt.mempool,
                                            &self.ctxt.state,
                                            main_tip,
                                        )
                                    })?;
                                }
                                ReorgErrorClass::Fatal => return Err(err),
                            },
                        }
                        canonical_main_tip = Some(main_tip);
                        self.ctxt.net.set_canonical_main_tip(main_tip);
                        if connect_kind == MainchainConnectKind::Bootstrap {
                            for message in deferred_tip_ready.drain(..) {
                                self.new_tip_ready_tx
                                    .unbounded_send(message)
                                    .map_err(Error::SendNewTipReady)?;
                            }
                        }
                    }
                    mainchain::Event::DisconnectBlock { block_hash } => {
                        if canonical_main_tip
                            .is_some_and(|main_tip| main_tip != block_hash)
                        {
                            tracing::trace!(
                                %block_hash,
                                ?canonical_main_tip,
                                "ignoring stale mainchain disconnect after initial tip reconciliation"
                            );
                            continue;
                        }
                        let main_tip = {
                            let rotxn = self
                                .ctxt
                                .env
                                .read_txn()
                                .map_err(EnvError::from)?;
                            self.ctxt
                                .archive
                                .try_get_main_header_info(&rotxn, &block_hash)?
                                .ok_or(archive::Error::NoMainHeaderInfo(
                                    block_hash,
                                ))?
                                .prev_block_hash
                        };
                        let mut attempted_tip = None;
                        let disconnect_result = task::block_in_place(|| {
                            let rotxn = self
                                .ctxt
                                .env
                                .read_txn()
                                .map_err(EnvError::from)?;
                            let candidate = best_ready_mainchain_candidate(
                                &self.ctxt.archive,
                                &self.ctxt.state,
                                &rotxn,
                                main_tip,
                            )?;
                            drop(rotxn);
                            if let Some(candidate) = candidate {
                                attempted_tip = Some(candidate);
                                reorg_to_tip_on_mainchain(
                                    &self.ctxt.env,
                                    &self.ctxt.archive,
                                    &self.ctxt.mempool,
                                    &self.ctxt.state,
                                    #[cfg(feature = "zmq")]
                                    &self.ctxt.zmq_pub_handler,
                                    candidate,
                                    main_tip,
                                )
                            } else {
                                disconnect_for_mainchain_tip(
                                    &self.ctxt.env,
                                    &self.ctxt.archive,
                                    &self.ctxt.mempool,
                                    &self.ctxt.state,
                                    main_tip,
                                )
                            }
                        });
                        if let Err(err) = disconnect_result {
                            match reorg_error_class(&err) {
                                ReorgErrorClass::Blocked => {
                                    let Error::MissingWithdrawalBundleMetadata {
                                        event_block_hash,
                                        m6id,
                                    } = err
                                    else {
                                        unreachable!()
                                    };
                                    let tip = attempted_tip.expect(
                                        "metadata can only block candidate application",
                                    );
                                    block_tip_on_withdrawal_bundle_metadata(
                                        &self.ctxt,
                                        &mut metadata_blocked_tips,
                                        &self.missing_withdrawal_bundles,
                                        tip,
                                        None,
                                        event_block_hash,
                                        m6id,
                                    );
                                }
                                ReorgErrorClass::InvalidBlock
                                | ReorgErrorClass::Rejected => {
                                    tracing::warn!(
                                        %block_hash,
                                        %main_tip,
                                        err = format!(
                                            "{:#}",
                                            ErrorChain::new(&err)
                                        ),
                                        "sidechain rewind rejected while processing mainchain disconnect"
                                    );
                                    if attempted_tip.is_some() {
                                        let fallback_result =
                                            task::block_in_place(|| {
                                                disconnect_for_mainchain_tip(
                                                    &self.ctxt.env,
                                                    &self.ctxt.archive,
                                                    &self.ctxt.mempool,
                                                    &self.ctxt.state,
                                                    main_tip,
                                                )
                                            });
                                        if let Err(fallback_err) =
                                            fallback_result
                                        {
                                            match reorg_error_class(
                                                &fallback_err,
                                            ) {
                                                ReorgErrorClass::Fatal
                                                | ReorgErrorClass::Blocked => {
                                                    return Err(fallback_err);
                                                }
                                                ReorgErrorClass::InvalidBlock
                                                | ReorgErrorClass::Rejected => {
                                                    tracing::warn!(
                                                        %block_hash,
                                                        %main_tip,
                                                        err = format!(
                                                            "{:#}",
                                                            ErrorChain::new(
                                                                &fallback_err
                                                            )
                                                        ),
                                                        "fallback sidechain rewind rejected while processing mainchain disconnect"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                                ReorgErrorClass::Fatal => return Err(err),
                            }
                        }
                        canonical_main_tip = Some(main_tip);
                        self.ctxt.net.set_canonical_main_tip(main_tip);
                    }
                },
                MailboxItem::NewTipReady(new_tip, addr, resp_tx) => {
                    let Some(main_tip) = canonical_main_tip else {
                        deferred_tip_ready.push((new_tip, addr, resp_tx));
                        continue;
                    };
                    let reorg_result = task::block_in_place(|| {
                        reorg_to_tip_on_mainchain(
                            &self.ctxt.env,
                            &self.ctxt.archive,
                            &self.ctxt.mempool,
                            &self.ctxt.state,
                            #[cfg(feature = "zmq")]
                            &self.ctxt.zmq_pub_handler,
                            new_tip,
                            main_tip,
                        )
                    });
                    let reorg_applied = match reorg_result {
                        Ok(applied) => applied,
                        Err(err) => {
                            match reorg_error_class(&err) {
                                ReorgErrorClass::Blocked => {
                                    let Error::MissingWithdrawalBundleMetadata {
                                        event_block_hash,
                                        m6id,
                                    } = err
                                    else {
                                        unreachable!()
                                    };
                                    block_tip_on_withdrawal_bundle_metadata(
                                        &self.ctxt,
                                        &mut metadata_blocked_tips,
                                        &self.missing_withdrawal_bundles,
                                        new_tip,
                                        addr,
                                        event_block_hash,
                                        m6id,
                                    );
                                }
                                ReorgErrorClass::InvalidBlock => {
                                    tracing::warn!(
                                        ?new_tip,
                                        ?addr,
                                        err = format!(
                                            "{:#}",
                                            ErrorChain::new(&err)
                                        ),
                                        "rejecting invalid tip from peer"
                                    );
                                    if let Some(addr) = addr {
                                        let () = self
                                            .ctxt
                                            .net
                                            .remove_active_peer(addr);
                                    }
                                }
                                ReorgErrorClass::Rejected => {
                                    tracing::warn!(
                                        ?new_tip,
                                        ?addr,
                                        err = format!(
                                            "{:#}",
                                            ErrorChain::new(&err)
                                        ),
                                        "tip rejected by current state"
                                    );
                                }
                                ReorgErrorClass::Fatal => return Err(err),
                            }
                            false
                        }
                    };
                    if let Some(resp_tx) = resp_tx {
                        let () = resp_tx
                            .send(reorg_applied)
                            .map_err(|_| Error::SendReorgResultOneshot)?;
                    }
                }
                MailboxItem::PeerInfo(None) => {
                    return Err(Error::PeerInfoRxClosed);
                }
                MailboxItem::PeerInfo(Some((addr, None))) => {
                    // peer connection is closed, remove it
                    tracing::warn!(%addr, "Connection to peer closed");
                    let () = self.ctxt.net.remove_active_peer(addr);
                    continue;
                }
                MailboxItem::PeerInfo(Some((addr, Some(peer_info)))) => {
                    tracing::trace!(%addr, ?peer_info, "mailbox item: received PeerInfo");
                    match peer_info {
                        PeerConnectionInfo::Error(
                            PeerConnectionError::Mailbox(
                                PeerConnectionMailboxError::HeartbeatTimeout,
                            ),
                        ) => {
                            const RECONNECT_DELAY: Duration =
                                Duration::from_secs(10);
                            // Attempt to reconnect if a valid message was
                            // received successfully
                            let Some(received_msg_successfully) =
                                self.ctxt.net.try_with_active_peer_connection(
                                    addr,
                                    |conn_handle| {
                                        conn_handle.received_msg_successfully()
                                    },
                                )
                            else {
                                continue;
                            };
                            let () = self.ctxt.net.remove_active_peer(addr);
                            if !received_msg_successfully {
                                continue;
                            }
                            reconnect_peer_spawner.spawn(async move {
                                tokio::time::sleep(RECONNECT_DELAY).await;
                                addr
                            });
                        }
                        PeerConnectionInfo::Error(err) => {
                            let err = anyhow::anyhow!(err);
                            tracing::error!(%addr, err = format!("{err:#}"), "Peer connection error");
                            let () = self.ctxt.net.remove_active_peer(addr);
                        }
                        PeerConnectionInfo::NeedMainchainAncestors {
                            main_hash,
                            peer_state_id,
                        } => {
                            let request =
                                mainchain_task::Request::AncestorInfos(
                                    main_hash,
                                );
                            let () = self
                                .forward_mainchain_task_request_tx
                                .unbounded_send((request, addr, peer_state_id))
                                .map_err(|_| {
                                    Error::ForwardMainchainTaskRequest
                                })?;
                        }
                        PeerConnectionInfo::NewTipReady(new_tip) => {
                            tracing::debug!(
                                ?new_tip,
                                %addr,
                                "mailbox item: received NewTipReady from peer, sending on channel"
                            );
                            self.new_tip_ready_tx
                                .unbounded_send((new_tip, Some(addr), None))
                                .map_err(Error::SendNewTipReady)?;
                        }
                        PeerConnectionInfo::NewTransaction(new_tx) => {
                            let mut rwtxn = self
                                .ctxt
                                .env
                                .write_txn()
                                .map_err(EnvError::from)?;
                            self.ctxt.mempool.put(&mut rwtxn, &new_tx)?;
                            rwtxn.commit().map_err(RwTxnError::from)?;
                            // broadcast
                            let () = self
                                .ctxt
                                .net
                                .push_tx(HashSet::from_iter([addr]), new_tx);
                        }
                        PeerConnectionInfo::Response(boxed) => {
                            let (resp, req) = *boxed;
                            tracing::trace!(
                                resp = format!("{resp:#?}"),
                                req = format!("{req:#?}"),
                                "mail box: received PeerConnectionInfo::Response"
                            );
                            let () = tokio::task::block_in_place(|| {
                                Self::handle_response(
                                    &self.ctxt,
                                    &mut descendant_tips,
                                    canonical_main_tip,
                                    &self.new_tip_ready_tx,
                                    &mut metadata_blocked_tips,
                                    &self.missing_withdrawal_bundles,
                                    addr,
                                    resp,
                                    req,
                                )
                            })?;
                        }
                    }
                }
                MailboxItem::RetryMissingWithdrawalBundles => {
                    let missing: Vec<_> = self
                        .missing_withdrawal_bundles
                        .read()
                        .iter()
                        .copied()
                        .collect();
                    for m6id in missing {
                        self.ctxt.net.request_withdrawal_bundle(m6id, None);
                    }
                }
                MailboxItem::ReconnectPeer(peer_address) => {
                    match self
                        .ctxt
                        .net
                        .connect_peer(self.ctxt.env.clone(), peer_address)
                    {
                        Ok(()) => {
                            for m6id in self
                                .missing_withdrawal_bundles
                                .read()
                                .iter()
                                .copied()
                            {
                                self.ctxt.net.request_withdrawal_bundle(
                                    m6id,
                                    Some(peer_address),
                                );
                            }
                        }
                        Err(err) => {
                            let err = anyhow::Error::from(err);
                            tracing::error!(
                                %peer_address,
                                "Failed to connect to peer: {err:#}"
                            )
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// Handle to the net task.
/// Task is aborted on drop.
#[derive(Clone)]
pub(super) struct NetTaskHandle {
    task: Arc<JoinHandle<()>>,
    missing_withdrawal_bundles: Arc<RwLock<HashSet<M6id>>>,
    /// Push a tip that is ready to reorg to, with the address of the peer
    /// connection that caused the request, if it originated from a peer.
    /// If the request originates from this node, then the socket address is
    /// None.
    /// An optional oneshot sender can be used receive the result of attempting
    /// to reorg to the new tip, on the corresponding oneshot receiver.
    new_tip_ready_tx: UnboundedSender<NewTipReadyMessage>,
}

impl NetTaskHandle {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        runtime: &tokio::runtime::Runtime,
        env: sneed::Env,
        archive: Archive,
        mainchain_task: MainchainTaskHandle,
        mainchain_task_response_rx: UnboundedReceiver<mainchain_task::Response>,
        mainchain_event_rx: UnboundedReceiver<mainchain_task::Event>,
        mempool: MemPool,
        net: Net,
        peer_info_rx: PeerInfoRx,
        state: State,
        #[cfg(feature = "zmq")] zmq_pub_handler: Arc<ZmqPubHandler>,
    ) -> Self {
        let ctxt = NetTaskContext {
            env,
            archive,
            mainchain_task,
            mempool,
            net,
            state,
            #[cfg(feature = "zmq")]
            zmq_pub_handler,
        };
        let (
            forward_mainchain_task_request_tx,
            forward_mainchain_task_request_rx,
        ) = mpsc::unbounded();
        let (new_tip_ready_tx, new_tip_ready_rx) = mpsc::unbounded();
        let missing_withdrawal_bundles = Arc::new(RwLock::new(HashSet::new()));
        let task = NetTask {
            ctxt,
            missing_withdrawal_bundles: Arc::clone(&missing_withdrawal_bundles),
            forward_mainchain_task_request_tx,
            forward_mainchain_task_request_rx,
            mainchain_task_response_rx,
            mainchain_event_rx,
            new_tip_ready_tx: new_tip_ready_tx.clone(),
            new_tip_ready_rx,
            peer_info_rx,
        };
        let task = runtime.spawn(async {
            if let Err(err) = task.run().await {
                let err = anyhow::Error::from(err);
                tracing::error!("Net task error: {err:#}");
            }
        });
        NetTaskHandle {
            task: Arc::new(task),
            missing_withdrawal_bundles,
            new_tip_ready_tx,
        }
    }

    pub fn missing_withdrawal_bundles(&self) -> HashSet<M6id> {
        self.missing_withdrawal_bundles.read().clone()
    }

    /// Push a tip that is ready to reorg to, and await successful application.
    /// A result of Ok(true) indicates that the tip was applied and reorged
    /// to successfully.
    /// A result of Ok(false) indicates that the tip was not reorged to.
    pub async fn new_tip_ready_confirm(
        &self,
        new_tip: Tip,
    ) -> Result<bool, Error> {
        tracing::debug!(?new_tip, "sending new tip ready confirm");
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        let () = self
            .new_tip_ready_tx
            .unbounded_send((new_tip, None, Some(oneshot_tx)))
            .map_err(Error::SendNewTipReady)?;
        oneshot_rx.await.map_err(Error::ReceiveReorgResultOneshot)
    }
}

impl Drop for NetTaskHandle {
    // If only one reference exists (ie. within self), abort the net task.
    fn drop(&mut self) {
        // use `Arc::get_mut` since `Arc::into_inner` requires ownership of the
        // Arc, and cloning would increase the reference count
        if let Some(task) = Arc::get_mut(&mut self.task) {
            tracing::debug!("dropping net task handle, aborting task");
            task.abort()
        }
    }
}

#[cfg(test)]
mod test {
    use bitcoin::hashes::Hash as _;

    #[cfg(feature = "zmq")]
    use super::ZmqPubHandler;
    use super::{
        Error, MainchainConnectKind, ReorgErrorClass,
        accumulator_common_is_retained, accumulator_reorg_is_within_horizon,
        best_ready_mainchain_candidate, classify_mainchain_connect,
        classify_mainchain_reconciliation_error, connect_tip_,
        disconnect_for_mainchain_tip, disconnect_tip_, is_fatal_reorg_error,
        mainchain_interval, ready_mainchain_candidate, reorg_error_class,
        reorg_to_tip_on_mainchain, with_accumulator_recovery,
    };
    use crate::{
        archive::Archive,
        mempool::MemPool,
        state::{self, State},
        types::{
            Accumulator, AccumulatorDiff, Address, BitcoinOutputContent, Body,
            FilledOutput, Header, M6id, OutPoint, Output, OutputContent, Tip,
            WithdrawalBundleEvent, WithdrawalBundleEventStatus,
            WithdrawalOutputContent, proto::mainchain, utreexo_leaf_hash,
        },
    };

    fn temp_env(
        test_name: &str,
    ) -> anyhow::Result<(temp_dir::TempDir, sneed::Env)> {
        let temp_dir = temp_dir::TempDir::with_prefix(format!(
            "plain-bitassets-{test_name}-{}",
            std::process::id()
        ))?;
        let mut opts = heed::EnvOpenOptions::new();
        opts.map_size(64 * 1024 * 1024)
            .max_dbs(State::NUM_DBS + Archive::NUM_DBS + MemPool::NUM_DBS);
        let env = unsafe { sneed::Env::open(&opts, temp_dir.path()) }?;
        Ok((temp_dir, env))
    }

    #[test]
    fn connect_tip_persists_and_applies_accumulator_diffs() -> anyhow::Result<()>
    {
        let (_temp_dir, env) =
            temp_env("connect_tip_persists_and_applies_accumulator_diffs")?;
        let archive = Archive::new(&env)?;
        let state = State::new(&env, Accumulator::default())?;
        let mut accumulator = Accumulator::default();
        let mempool = MemPool::new(&env)?;
        let coinbase = Output::new(
            Address::ALL_ZEROS,
            OutputContent::Bitcoin(BitcoinOutputContent(bitcoin::Amount::ZERO)),
        );
        let body = Body {
            coinbase: vec![coinbase],
            transactions: Vec::new(),
            authorizations: Vec::new(),
        };
        let header = Header {
            merkle_root: Body::compute_merkle_root(
                &body.coinbase,
                &body.transactions,
            ),
            prev_side_hash: None,
            prev_main_hash: bitcoin::BlockHash::all_zeros(),
        };
        let block_hash = header.hash();
        let outpoint = OutPoint::Coinbase {
            merkle_root: header.merkle_root,
            vout: 0,
        };
        let output = FilledOutput::new_bitcoin_value(
            Address::ALL_ZEROS,
            bitcoin::Amount::ZERO,
        );
        let mut expected_diff = AccumulatorDiff::default();
        expected_diff.insert(utreexo_leaf_hash(&outpoint, &output));
        let deposit_outpoint = bitcoin::OutPoint {
            txid: bitcoin::Txid::from_byte_array([2; 32]),
            vout: 0,
        };
        let deposit_output = FilledOutput::new_bitcoin_value(
            Address::ALL_ZEROS,
            bitcoin::Amount::from_sat(100),
        );
        expected_diff.insert(utreexo_leaf_hash(
            &OutPoint::Deposit(deposit_outpoint),
            &deposit_output,
        ));
        let mut expected = Accumulator::default();
        expected.apply_diff(expected_diff.clone())?;
        let mut two_way_peg_data = mainchain::TwoWayPegData::default();
        two_way_peg_data.block_info.insert(
            header.prev_main_hash,
            mainchain::BlockInfo {
                bmm_commitment: None,
                events: vec![mainchain::BlockEvent::Deposit(
                    mainchain::Deposit {
                        tx_index: 0,
                        outpoint: deposit_outpoint,
                        output: deposit_output,
                    },
                )],
            },
        );

        let mut rwtxn = env.write_txn()?;
        connect_tip_(
            &mut rwtxn,
            &archive,
            &mempool,
            &state,
            &header,
            &body,
            &two_way_peg_data,
            &mut accumulator,
        )?;
        rwtxn.commit()?;

        let rotxn = env.read_txn()?;
        assert_eq!(
            archive.get_accumulator_diff(&rotxn, block_hash)?,
            expected_diff
        );
        assert_eq!(accumulator.get_roots(), expected.get_roots());

        drop(rotxn);
        let empty_body = Body {
            coinbase: Vec::new(),
            transactions: Vec::new(),
            authorizations: Vec::new(),
        };
        let child_header = Header {
            merkle_root: Body::compute_merkle_root(
                &empty_body.coinbase,
                &empty_body.transactions,
            ),
            prev_side_hash: Some(block_hash),
            prev_main_hash: header.prev_main_hash,
        };
        let child_hash = child_header.hash();
        let mut rwtxn = env.write_txn()?;
        connect_tip_(
            &mut rwtxn,
            &archive,
            &mempool,
            &state,
            &child_header,
            &empty_body,
            &mainchain::TwoWayPegData::default(),
            &mut accumulator,
        )?;
        rwtxn.commit()?;

        let rotxn = env.read_txn()?;
        assert!(archive.get_accumulator_diff(&rotxn, child_hash)?.is_empty());
        assert_eq!(accumulator.get_roots(), expected.get_roots());
        Ok(())
    }

    #[test]
    fn mainchain_interval_rejects_unrelated_boundary() -> anyhow::Result<()> {
        let (_temp_dir, env) =
            temp_env("mainchain_interval_rejects_unrelated_boundary")?;
        let archive = Archive::new(&env)?;
        let main_a = bitcoin::BlockHash::from_byte_array([31; 32]);
        let main_b = bitcoin::BlockHash::from_byte_array([32; 32]);
        let mut rwtxn = env.write_txn()?;
        for (block_hash, work) in [(main_a, 1), (main_b, 2)] {
            archive.put_main_header_info(
                &mut rwtxn,
                &mainchain::BlockHeaderInfo {
                    block_hash,
                    prev_block_hash: bitcoin::BlockHash::all_zeros(),
                    height: 1,
                    work: bitcoin::Work::from_le_bytes([work; 32]),
                },
            )?;
            archive.put_main_block_info(
                &mut rwtxn,
                block_hash,
                &mainchain::BlockInfo::default(),
            )?;
        }
        rwtxn.commit()?;

        let rotxn = env.read_txn()?;
        assert!(matches!(
            mainchain_interval(&archive, &rotxn, Some(main_a), main_b),
            Err(Error::InvalidMainchainInterval { start, end })
                if start == main_a && end == main_b
        ));
        Ok(())
    }

    #[test]
    fn mainchain_connects_are_sequential_after_bootstrap() {
        let current = bitcoin::BlockHash::from_byte_array([1; 32]);
        let child = bitcoin::BlockHash::from_byte_array([2; 32]);
        let sibling = bitcoin::BlockHash::from_byte_array([3; 32]);
        let parent = bitcoin::BlockHash::from_byte_array([4; 32]);

        assert_eq!(
            classify_mainchain_connect(None, current, parent),
            MainchainConnectKind::Bootstrap
        );
        assert_eq!(
            classify_mainchain_connect(Some(current), current, parent),
            MainchainConnectKind::Stale
        );
        assert_eq!(
            classify_mainchain_connect(Some(current), child, current),
            MainchainConnectKind::Next
        );
        assert_eq!(
            classify_mainchain_connect(Some(current), sibling, parent),
            MainchainConnectKind::Stale
        );
    }

    #[test]
    fn startup_candidate_uses_full_canonical_mainchain_lineage()
    -> anyhow::Result<()> {
        let (_temp_dir, env) =
            temp_env("startup_candidate_uses_full_mainchain_lineage")?;
        let archive = Archive::new(&env)?;
        let state = State::new(&env, Accumulator::default())?;
        let main1 = bitcoin::BlockHash::from_byte_array([11; 32]);
        let main2 = bitcoin::BlockHash::from_byte_array([12; 32]);
        let fork2 = bitcoin::BlockHash::from_byte_array([13; 32]);
        let body = Body {
            coinbase: Vec::new(),
            transactions: Vec::new(),
            authorizations: Vec::new(),
        };
        let canonical_header = Header {
            merkle_root: Body::compute_merkle_root(
                &body.coinbase,
                &body.transactions,
            ),
            prev_side_hash: None,
            prev_main_hash: bitcoin::BlockHash::all_zeros(),
        };
        let canonical_side = canonical_header.hash();
        let fork_body = Body {
            coinbase: vec![Output {
                address: Address::ALL_ZEROS,
                content: OutputContent::Bitcoin(BitcoinOutputContent(
                    bitcoin::Amount::ZERO,
                )),
                memo: vec![1],
            }],
            transactions: Vec::new(),
            authorizations: Vec::new(),
        };
        let fork_header = Header {
            merkle_root: Body::compute_merkle_root(
                &fork_body.coinbase,
                &fork_body.transactions,
            ),
            prev_side_hash: None,
            prev_main_hash: main1,
        };
        let fork_side = fork_header.hash();

        let mut rwtxn = env.write_txn()?;
        for header_info in [
            mainchain::BlockHeaderInfo {
                block_hash: main1,
                prev_block_hash: bitcoin::BlockHash::all_zeros(),
                height: 1,
                work: bitcoin::Work::from_le_bytes([1; 32]),
            },
            mainchain::BlockHeaderInfo {
                block_hash: main2,
                prev_block_hash: main1,
                height: 2,
                work: bitcoin::Work::from_le_bytes([1; 32]),
            },
            mainchain::BlockHeaderInfo {
                block_hash: fork2,
                prev_block_hash: main1,
                height: 2,
                work: bitcoin::Work::from_le_bytes([2; 32]),
            },
        ] {
            archive.put_main_header_info(&mut rwtxn, &header_info)?;
        }
        for (main_hash, commitment) in [
            (main1, Some(canonical_side)),
            (main2, None),
            (fork2, Some(fork_side)),
        ] {
            archive.put_main_block_info(
                &mut rwtxn,
                main_hash,
                &mainchain::BlockInfo {
                    bmm_commitment: commitment,
                    events: Vec::new(),
                },
            )?;
        }
        archive.put_header(&mut rwtxn, &canonical_header)?;
        archive.put_body(&mut rwtxn, canonical_side, &body)?;
        archive.put_header(&mut rwtxn, &fork_header)?;
        archive.put_body(&mut rwtxn, fork_side, &fork_body)?;
        rwtxn.commit()?;

        let rotxn = env.read_txn()?;
        assert_eq!(
            archive.get_mainchain_verified_tips(&rotxn, main2)?,
            vec![Tip {
                block_hash: canonical_side,
                main_block_hash: main1,
            }]
        );
        assert_eq!(
            best_ready_mainchain_candidate(&archive, &state, &rotxn, main2)?,
            Some(Tip {
                block_hash: canonical_side,
                main_block_hash: main1,
            })
        );
        Ok(())
    }

    #[test]
    fn cumulative_reorg_cannot_cross_retained_floor() {
        assert!(accumulator_common_is_retained(Some(100), 100));
        assert!(accumulator_common_is_retained(Some(101), 100));
        assert!(!accumulator_common_is_retained(Some(99), 100));
        assert!(accumulator_common_is_retained(None, 0));
        assert!(!accumulator_common_is_retained(None, 1));
    }

    #[test]
    fn reorg_horizon_is_inclusive() {
        let horizon =
            u64::from(crate::archive::ACCUMULATOR_REORG_HORIZON_BLOCKS);
        assert!(accumulator_reorg_is_within_horizon(horizon));
        assert!(!accumulator_reorg_is_within_horizon(horizon + 1));
    }

    #[test]
    fn panicked_reorg_restores_committed_accumulator() -> anyhow::Result<()> {
        let (_temp_dir, env) =
            temp_env("panicked_reorg_restores_committed_accumulator")?;
        let archive = Archive::new(&env)?;
        let state = State::new(&env, Accumulator::default())?;
        let mut accumulator = Accumulator::default();
        let mut initial_diff = AccumulatorDiff::default();
        initial_diff.insert([1; 32]);
        accumulator.apply_diff(initial_diff)?;

        let panic =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                drop(with_accumulator_recovery(
                    &env,
                    &archive,
                    &state,
                    &mut accumulator,
                    |accumulator| -> Result<(), Error> {
                        let mut uncommitted_diff = AccumulatorDiff::default();
                        uncommitted_diff.insert([2; 32]);
                        accumulator.apply_diff(uncommitted_diff)?;
                        panic!("injected reorg panic");
                    },
                ));
            }));

        assert!(panic.is_err());
        assert_eq!(
            bincode::serialize(&accumulator)?,
            bincode::serialize(&Accumulator::default())?
        );
        Ok(())
    }

    #[test]
    fn l2_reorg_replays_finalized_withdrawal_and_later_deposit()
    -> anyhow::Result<()> {
        let (_temp_dir, env) = temp_env(
            "l2_reorg_replays_finalized_withdrawal_and_later_deposit",
        )?;
        let archive = Archive::new(&env)?;
        let state = State::new(&env, Accumulator::default())?;
        let mempool = MemPool::new(&env)?;
        #[cfg(feature = "zmq")]
        let zmq_runtime = tokio::runtime::Runtime::new()?;
        #[cfg(feature = "zmq")]
        let (zmq_tx, _zmq_rx) = futures::channel::mpsc::unbounded();
        #[cfg(feature = "zmq")]
        let zmq_pub_handler = ZmqPubHandler {
            tx: zmq_tx,
            _handle: zmq_runtime.spawn(async {}),
        };

        let main1 = bitcoin::BlockHash::from_byte_array([101; 32]);
        let main2 = bitcoin::BlockHash::from_byte_array([102; 32]);
        let main3 = bitcoin::BlockHash::from_byte_array([103; 32]);
        let main4 = bitcoin::BlockHash::from_byte_array([104; 32]);
        let old_main5 = bitcoin::BlockHash::from_byte_array([105; 32]);
        let new_main5 = bitcoin::BlockHash::from_byte_array([106; 32]);
        let new_main6 = bitcoin::BlockHash::from_byte_array([107; 32]);
        let new_main7 = bitcoin::BlockHash::from_byte_array([108; 32]);
        let empty_body = Body {
            coinbase: Vec::new(),
            transactions: Vec::new(),
            authorizations: Vec::new(),
        };
        let withdrawal_content = WithdrawalOutputContent {
            value: bitcoin::Amount::ZERO,
            main_fee: bitcoin::Amount::ZERO,
            main_address: bitcoin::Address::p2pkh(
                bitcoin::PubkeyHash::hash(b"reorg withdrawal"),
                bitcoin::NetworkKind::Test,
            )
            .into_unchecked(),
        };
        let withdrawal_body = Body {
            coinbase: vec![Output {
                address: Address::ALL_ZEROS,
                content: OutputContent::Withdrawal(withdrawal_content),
                memo: vec![1],
            }],
            transactions: Vec::new(),
            authorizations: Vec::new(),
        };
        let genesis = Header {
            merkle_root: Body::compute_merkle_root(
                &withdrawal_body.coinbase,
                &withdrawal_body.transactions,
            ),
            prev_side_hash: None,
            prev_main_hash: bitcoin::BlockHash::all_zeros(),
        };
        let common1 = Header {
            merkle_root: Body::compute_merkle_root(&[], &[]),
            prev_side_hash: Some(genesis.hash()),
            prev_main_hash: main1,
        };
        let common2 = Header {
            merkle_root: common1.merkle_root,
            prev_side_hash: Some(common1.hash()),
            prev_main_hash: main2,
        };
        let common3 = Header {
            merkle_root: common1.merkle_root,
            prev_side_hash: Some(common2.hash()),
            prev_main_hash: main3,
        };
        let old_tip_header = Header {
            merkle_root: common1.merkle_root,
            prev_side_hash: Some(common3.hash()),
            prev_main_hash: main4,
        };
        let replacement_header = Header {
            merkle_root: common1.merkle_root,
            prev_side_hash: Some(common3.hash()),
            prev_main_hash: new_main6,
        };
        let old_tip = old_tip_header.hash();
        let replacement_tip = replacement_header.hash();

        let mut rwtxn = env.write_txn()?;
        for (block_hash, prev_block_hash, height, work) in [
            (main1, bitcoin::BlockHash::all_zeros(), 1, 1_u8),
            (main2, main1, 2, 2),
            (main3, main2, 3, 3),
            (main4, main3, 4, 4),
            (old_main5, main4, 5, 5),
        ] {
            archive.put_main_header_info(
                &mut rwtxn,
                &mainchain::BlockHeaderInfo {
                    block_hash,
                    prev_block_hash,
                    height,
                    work: bitcoin::Work::from_le_bytes([work; 32]),
                },
            )?;
        }
        for (block_hash, commitment) in [
            (main1, genesis.hash()),
            (main2, common1.hash()),
            (main3, common2.hash()),
            (main4, common3.hash()),
            (old_main5, old_tip),
        ] {
            archive.put_main_block_info(
                &mut rwtxn,
                block_hash,
                &mainchain::BlockInfo {
                    bmm_commitment: Some(commitment),
                    events: Vec::new(),
                },
            )?;
        }

        let mut accumulator = Accumulator::default();
        for (header, body) in [
            (&genesis, &withdrawal_body),
            (&common1, &empty_body),
            (&common2, &empty_body),
            (&common3, &empty_body),
            (&old_tip_header, &empty_body),
        ] {
            connect_tip_(
                &mut rwtxn,
                &archive,
                &mempool,
                &state,
                header,
                body,
                &mainchain::TwoWayPegData::default(),
                &mut accumulator,
            )?;
        }
        let (bundle, creation_height) = state
            .try_get_pending_withdrawal_bundle(&rwtxn)?
            .expect("height-four old branch should create a bundle");
        assert_eq!(creation_height, 4);
        let m6id = bundle.compute_m6id();
        assert!(
            state
                .try_get_withdrawal_bundle_metadata(&rwtxn, m6id)?
                .is_some()
        );
        rwtxn.commit()?;
        *state.utreexo_accumulator.lock() = accumulator;

        let later_deposit_outpoint = bitcoin::OutPoint {
            txid: bitcoin::Txid::from_byte_array([109; 32]),
            vout: 0,
        };
        let later_deposit_output = FilledOutput::new_bitcoin_value(
            Address::ALL_ZEROS,
            bitcoin::Amount::from_sat(50),
        );
        let mut rwtxn = env.write_txn()?;
        for (block_hash, prev_block_hash, height, work) in [
            (new_main5, main4, 5, 6_u8),
            (new_main6, new_main5, 6, 7),
            (new_main7, new_main6, 7, 8),
        ] {
            archive.put_main_header_info(
                &mut rwtxn,
                &mainchain::BlockHeaderInfo {
                    block_hash,
                    prev_block_hash,
                    height,
                    work: bitcoin::Work::from_le_bytes([work; 32]),
                },
            )?;
        }
        archive.put_main_block_info(
            &mut rwtxn,
            new_main5,
            &mainchain::BlockInfo {
                bmm_commitment: None,
                events: vec![mainchain::BlockEvent::WithdrawalBundle(
                    WithdrawalBundleEvent {
                        m6id,
                        status: WithdrawalBundleEventStatus::Submitted,
                    },
                )],
            },
        )?;
        archive.put_main_block_info(
            &mut rwtxn,
            new_main6,
            &mainchain::BlockInfo {
                bmm_commitment: None,
                events: vec![
                    mainchain::BlockEvent::WithdrawalBundle(
                        WithdrawalBundleEvent {
                            m6id,
                            status: WithdrawalBundleEventStatus::Confirmed,
                        },
                    ),
                    mainchain::BlockEvent::Deposit(mainchain::Deposit {
                        tx_index: 1,
                        outpoint: later_deposit_outpoint,
                        output: later_deposit_output.clone(),
                    }),
                ],
            },
        )?;
        archive.put_main_block_info(
            &mut rwtxn,
            new_main7,
            &mainchain::BlockInfo {
                bmm_commitment: Some(replacement_tip),
                events: Vec::new(),
            },
        )?;
        archive.put_header(&mut rwtxn, &replacement_header)?;
        archive.put_body(&mut rwtxn, replacement_tip, &empty_body)?;
        rwtxn.commit()?;

        assert!(reorg_to_tip_on_mainchain(
            &env,
            &archive,
            &mempool,
            &state,
            #[cfg(feature = "zmq")]
            &zmq_pub_handler,
            Tip {
                block_hash: replacement_tip,
                main_block_hash: new_main7,
            },
            new_main7,
        )?);

        let rotxn = env.read_txn()?;
        let withdrawal_outpoint = OutPoint::Coinbase {
            merkle_root: genesis.merkle_root,
            vout: 0,
        };
        let utxos = state.get_utxos(&rotxn)?;
        assert_eq!(state.try_get_tip(&rotxn)?, Some(replacement_tip));
        assert!(!utxos.contains_key(&withdrawal_outpoint));
        assert_eq!(
            utxos.get(&OutPoint::Deposit(later_deposit_outpoint)),
            Some(&later_deposit_output)
        );
        assert!(state.try_get_pending_withdrawal_bundle(&rotxn)?.is_none());
        assert!(
            state
                .try_get_withdrawal_bundle_metadata(&rotxn, m6id)?
                .is_some()
        );

        let mut expected = Accumulator::default();
        let mut expected_diff = AccumulatorDiff::default();
        expected_diff.insert(utreexo_leaf_hash(
            &OutPoint::Deposit(later_deposit_outpoint),
            &later_deposit_output,
        ));
        expected.apply_diff(expected_diff)?;
        assert_eq!(
            state.utreexo_accumulator.lock().get_roots(),
            expected.get_roots()
        );
        assert_eq!(
            bincode::serialize(&*state.utreexo_accumulator.lock())?,
            bincode::serialize(
                &archive.accumulator_at(&rotxn, Some(replacement_tip))?
            )?
        );
        Ok(())
    }

    #[test]
    fn raw_mainchain_disconnect_rewinds_accumulator_and_reapplies_deposits()
    -> anyhow::Result<()> {
        let (_temp_dir, env) =
            temp_env("raw_mainchain_disconnect_rewinds_accumulator")?;
        let archive = Archive::new(&env)?;
        let state = State::new(&env, Accumulator::default())?;
        let mempool = MemPool::new(&env)?;
        #[cfg(feature = "zmq")]
        let zmq_runtime = tokio::runtime::Runtime::new()?;
        #[cfg(feature = "zmq")]
        let (zmq_tx, _zmq_rx) = futures::channel::mpsc::unbounded();
        #[cfg(feature = "zmq")]
        let zmq_pub_handler = ZmqPubHandler {
            tx: zmq_tx,
            _handle: zmq_runtime.spawn(async {}),
        };

        let main1 = bitcoin::BlockHash::from_byte_array([11; 32]);
        let main2a = bitcoin::BlockHash::from_byte_array([12; 32]);
        let main2b = bitcoin::BlockHash::from_byte_array([13; 32]);
        let main3a = bitcoin::BlockHash::from_byte_array([14; 32]);
        let main3b = bitcoin::BlockHash::from_byte_array([15; 32]);
        let abandoned_main3b = bitcoin::BlockHash::from_byte_array([16; 32]);
        let empty_body = Body {
            coinbase: Vec::new(),
            transactions: Vec::new(),
            authorizations: Vec::new(),
        };
        let genesis = Header {
            merkle_root: Body::compute_merkle_root(
                &empty_body.coinbase,
                &empty_body.transactions,
            ),
            prev_side_hash: None,
            prev_main_hash: bitcoin::BlockHash::all_zeros(),
        };
        let genesis_hash = genesis.hash();
        let branch_body = |marker| Body {
            coinbase: vec![Output {
                address: Address::ALL_ZEROS,
                content: OutputContent::Bitcoin(BitcoinOutputContent(
                    bitcoin::Amount::ZERO,
                )),
                memo: vec![marker],
            }],
            transactions: Vec::new(),
            authorizations: Vec::new(),
        };
        let body_a = branch_body(1);
        let body_b = branch_body(2);
        let header_a = Header {
            merkle_root: Body::compute_merkle_root(
                &body_a.coinbase,
                &body_a.transactions,
            ),
            prev_side_hash: Some(genesis_hash),
            prev_main_hash: main2a,
        };
        let header_b = Header {
            merkle_root: Body::compute_merkle_root(
                &body_b.coinbase,
                &body_b.transactions,
            ),
            prev_side_hash: Some(genesis_hash),
            prev_main_hash: main2b,
        };
        let hash_a = header_a.hash();
        let hash_b = header_b.hash();

        let deposit_outpoint = bitcoin::OutPoint {
            txid: bitcoin::Txid::from_byte_array([21; 32]),
            vout: 0,
        };
        let deposit_output = FilledOutput::new_bitcoin_value(
            Address::ALL_ZEROS,
            bitcoin::Amount::from_sat(50),
        );
        let deposit_event =
            mainchain::BlockEvent::Deposit(mainchain::Deposit {
                tx_index: 0,
                outpoint: deposit_outpoint,
                output: deposit_output.clone(),
            });
        let deposit_a_outpoint = bitcoin::OutPoint {
            txid: bitcoin::Txid::from_byte_array([22; 32]),
            vout: 0,
        };
        let deposit_a_output = FilledOutput::new_bitcoin_value(
            Address::ALL_ZEROS,
            bitcoin::Amount::from_sat(60),
        );
        let deposit_a_event =
            mainchain::BlockEvent::Deposit(mainchain::Deposit {
                tx_index: 0,
                outpoint: deposit_a_outpoint,
                output: deposit_a_output.clone(),
            });
        let deposit_b_outpoint = bitcoin::OutPoint {
            txid: bitcoin::Txid::from_byte_array([23; 32]),
            vout: 0,
        };
        let deposit_b_output = FilledOutput::new_bitcoin_value(
            Address::ALL_ZEROS,
            bitcoin::Amount::from_sat(70),
        );
        let deposit_b_event =
            mainchain::BlockEvent::Deposit(mainchain::Deposit {
                tx_index: 0,
                outpoint: deposit_b_outpoint,
                output: deposit_b_output.clone(),
            });

        let mut rwtxn = env.write_txn()?;
        let main_headers = [
            mainchain::BlockHeaderInfo {
                block_hash: main1,
                prev_block_hash: bitcoin::BlockHash::all_zeros(),
                height: 1,
                work: bitcoin::Work::from_le_bytes([1; 32]),
            },
            mainchain::BlockHeaderInfo {
                block_hash: main2a,
                prev_block_hash: main1,
                height: 2,
                work: bitcoin::Work::from_le_bytes([1; 32]),
            },
            mainchain::BlockHeaderInfo {
                block_hash: main2b,
                prev_block_hash: main1,
                height: 2,
                work: bitcoin::Work::from_le_bytes([2; 32]),
            },
            mainchain::BlockHeaderInfo {
                block_hash: main3a,
                prev_block_hash: main2a,
                height: 3,
                work: bitcoin::Work::from_le_bytes([1; 32]),
            },
            mainchain::BlockHeaderInfo {
                block_hash: main3b,
                prev_block_hash: main2b,
                height: 3,
                work: bitcoin::Work::from_le_bytes([2; 32]),
            },
            mainchain::BlockHeaderInfo {
                block_hash: abandoned_main3b,
                prev_block_hash: main2b,
                height: 3,
                work: bitcoin::Work::from_le_bytes([4; 32]),
            },
        ];
        for header_info in &main_headers {
            archive.put_main_header_info(&mut rwtxn, header_info)?;
        }
        archive.put_main_block_info(
            &mut rwtxn,
            main1,
            &mainchain::BlockInfo {
                bmm_commitment: Some(genesis_hash),
                events: vec![deposit_event.clone()],
            },
        )?;
        archive.put_main_block_info(
            &mut rwtxn,
            main2a,
            &mainchain::BlockInfo {
                bmm_commitment: None,
                events: vec![deposit_a_event.clone()],
            },
        )?;
        archive.put_main_block_info(
            &mut rwtxn,
            main2b,
            &mainchain::BlockInfo {
                bmm_commitment: None,
                events: vec![deposit_b_event.clone()],
            },
        )?;
        archive.put_main_block_info(
            &mut rwtxn,
            main3a,
            &mainchain::BlockInfo {
                bmm_commitment: Some(hash_a),
                events: Vec::new(),
            },
        )?;
        archive.put_main_block_info(
            &mut rwtxn,
            main3b,
            &mainchain::BlockInfo {
                bmm_commitment: Some(hash_b),
                events: Vec::new(),
            },
        )?;
        archive.put_main_block_info(
            &mut rwtxn,
            abandoned_main3b,
            &mainchain::BlockInfo {
                bmm_commitment: Some(hash_b),
                events: Vec::new(),
            },
        )?;

        let mut accumulator = Accumulator::default();
        connect_tip_(
            &mut rwtxn,
            &archive,
            &mempool,
            &state,
            &genesis,
            &empty_body,
            &mainchain::TwoWayPegData::default(),
            &mut accumulator,
        )?;
        let mut peg_data = mainchain::TwoWayPegData::default();
        peg_data.block_info.insert(
            main1,
            mainchain::BlockInfo {
                bmm_commitment: Some(genesis_hash),
                events: vec![deposit_event],
            },
        );
        peg_data.block_info.insert(
            main2a,
            mainchain::BlockInfo {
                bmm_commitment: None,
                events: vec![deposit_a_event],
            },
        );
        connect_tip_(
            &mut rwtxn,
            &archive,
            &mempool,
            &state,
            &header_a,
            &body_a,
            &peg_data,
            &mut accumulator,
        )?;
        let empty_child = Header {
            merkle_root: Body::compute_merkle_root(
                &empty_body.coinbase,
                &empty_body.transactions,
            ),
            prev_side_hash: Some(hash_a),
            prev_main_hash: main2a,
        };
        connect_tip_(
            &mut rwtxn,
            &archive,
            &mempool,
            &state,
            &empty_child,
            &empty_body,
            &mainchain::TwoWayPegData::default(),
            &mut accumulator,
        )?;
        archive.put_header(&mut rwtxn, &header_b)?;
        archive.put_body(&mut rwtxn, hash_b, &body_b)?;
        rwtxn.commit()?;

        // Disconnecting an eventless child must not infer and undo the
        // deposit interval already applied by its still-connected parent.
        let mut rwtxn = env.write_txn()?;
        disconnect_tip_(&mut rwtxn, &archive, &mempool, &state)?;
        rwtxn.commit()?;
        let rotxn = env.read_txn()?;
        assert_eq!(state.try_get_tip(&rotxn)?, Some(hash_a));
        assert!(
            state
                .get_utxos(&rotxn)?
                .contains_key(&OutPoint::Deposit(deposit_outpoint))
        );
        assert!(
            state
                .get_utxos(&rotxn)?
                .contains_key(&OutPoint::Deposit(deposit_a_outpoint))
        );
        *state.utreexo_accumulator.lock() =
            archive.accumulator_at(&rotxn, Some(hash_a))?;
        drop(rotxn);

        // Disconnecting the mainchain block that verified A immediately
        // rewinds to the newest side block whose verification is still on the
        // surviving mainchain. The operation commits the state and forest as
        // one unit; the abandoned branch's deposits disappear with A.
        assert!(disconnect_for_mainchain_tip(
            &env, &archive, &mempool, &state, main2a,
        )?);
        let rotxn = env.read_txn()?;
        assert_eq!(state.try_get_tip(&rotxn)?, Some(genesis_hash));
        assert_eq!(
            bincode::serialize(&*state.utreexo_accumulator.lock())?,
            bincode::serialize(
                &archive.accumulator_at(&rotxn, Some(genesis_hash))?
            )?
        );
        assert!(
            !state
                .get_utxos(&rotxn)?
                .contains_key(&OutPoint::Deposit(deposit_outpoint))
        );
        assert!(
            !state
                .get_utxos(&rotxn)?
                .contains_key(&OutPoint::Deposit(deposit_a_outpoint))
        );

        // A connect event carrying B's commitment can now discover the fully
        // archived candidate and apply it automatically on the new L1 branch.
        let candidate = ready_mainchain_candidate(
            &archive, &state, &rotxn, hash_b, main3b,
        )?
        .unwrap();
        assert_eq!(
            archive.get_best_main_verification(&rotxn, hash_b)?,
            abandoned_main3b
        );
        assert_eq!(candidate.main_block_hash, main3b);
        drop(rotxn);
        let applied = reorg_to_tip_on_mainchain(
            &env,
            &archive,
            &mempool,
            &state,
            #[cfg(feature = "zmq")]
            &zmq_pub_handler,
            candidate,
            main3b,
        )?;
        assert!(applied);

        let rotxn = env.read_txn()?;
        assert_eq!(state.try_get_tip(&rotxn)?, Some(hash_b));
        assert_eq!(
            state
                .get_utxos(&rotxn)?
                .get(&OutPoint::Deposit(deposit_outpoint)),
            Some(&deposit_output)
        );
        assert_eq!(
            state
                .get_utxos(&rotxn)?
                .get(&OutPoint::Deposit(deposit_a_outpoint)),
            None
        );
        assert_eq!(
            state
                .get_utxos(&rotxn)?
                .get(&OutPoint::Deposit(deposit_b_outpoint)),
            Some(&deposit_b_output)
        );

        let mut expected = Accumulator::default();
        let coinbase_outpoint = OutPoint::Coinbase {
            merkle_root: header_b.merkle_root,
            vout: 0,
        };
        let coinbase_output = FilledOutput {
            address: body_b.coinbase[0].address,
            content: crate::types::FilledOutputContent::Bitcoin(
                BitcoinOutputContent(bitcoin::Amount::ZERO),
            ),
            memo: body_b.coinbase[0].memo.clone(),
        };
        let mut expected_diff = AccumulatorDiff::default();
        expected_diff
            .insert(utreexo_leaf_hash(&coinbase_outpoint, &coinbase_output));
        expected_diff.insert(utreexo_leaf_hash(
            &OutPoint::Deposit(deposit_outpoint),
            &deposit_output,
        ));
        expected_diff.insert(utreexo_leaf_hash(
            &OutPoint::Deposit(deposit_b_outpoint),
            &deposit_b_output,
        ));
        expected.apply_diff(expected_diff)?;
        let expected_bytes = bincode::serialize(&expected)?;
        assert_eq!(
            bincode::serialize(&*state.utreexo_accumulator.lock())?,
            expected_bytes.clone()
        );

        // Missing withdrawal metadata is a recoverable sync block. Even when
        // the current sidechain tip has no verification on the replacement L1
        // lineage, neither its database state nor its accumulator is replaced
        // until the complete candidate succeeds.
        drop(rotxn);
        let main2d = bitcoin::BlockHash::from_byte_array([41; 32]);
        let main3d = bitcoin::BlockHash::from_byte_array([42; 32]);
        let body_d = branch_body(6);
        let header_d = Header {
            merkle_root: Body::compute_merkle_root(
                &body_d.coinbase,
                &body_d.transactions,
            ),
            prev_side_hash: Some(genesis_hash),
            prev_main_hash: main2d,
        };
        let hash_d = header_d.hash();
        let missing_m6id = M6id(bitcoin::Txid::from_byte_array([43; 32]));
        let mut rwtxn = env.write_txn()?;
        for header_info in [
            mainchain::BlockHeaderInfo {
                block_hash: main2d,
                prev_block_hash: main1,
                height: 2,
                work: bitcoin::Work::from_le_bytes([5; 32]),
            },
            mainchain::BlockHeaderInfo {
                block_hash: main3d,
                prev_block_hash: main2d,
                height: 3,
                work: bitcoin::Work::from_le_bytes([5; 32]),
            },
        ] {
            archive.put_main_header_info(&mut rwtxn, &header_info)?;
        }
        archive.put_main_block_info(
            &mut rwtxn,
            main2d,
            &mainchain::BlockInfo {
                bmm_commitment: None,
                events: vec![mainchain::BlockEvent::WithdrawalBundle(
                    WithdrawalBundleEvent {
                        m6id: missing_m6id,
                        status: WithdrawalBundleEventStatus::Submitted,
                    },
                )],
            },
        )?;
        archive.put_main_block_info(
            &mut rwtxn,
            main3d,
            &mainchain::BlockInfo {
                bmm_commitment: Some(hash_d),
                events: Vec::new(),
            },
        )?;
        archive.put_header(&mut rwtxn, &header_d)?;
        archive.put_body(&mut rwtxn, hash_d, &body_d)?;
        rwtxn.commit()?;

        let err = reorg_to_tip_on_mainchain(
            &env,
            &archive,
            &mempool,
            &state,
            #[cfg(feature = "zmq")]
            &zmq_pub_handler,
            Tip {
                block_hash: hash_d,
                main_block_hash: main3d,
            },
            main3d,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            Error::MissingWithdrawalBundleMetadata { m6id, .. }
                if m6id == missing_m6id
        ));
        let rotxn = env.read_txn()?;
        assert_eq!(state.try_get_tip(&rotxn)?, Some(hash_b));
        assert_eq!(
            bincode::serialize(&*state.utreexo_accumulator.lock())?,
            expected_bytes
        );

        // A later invalid block rolls back the entire replacement branch, not
        // just the invalid block. The in-memory forest is restored to the last
        // committed tip, and the poisoned body is removed from the archive.
        drop(rotxn);
        let main2c = bitcoin::BlockHash::from_byte_array([31; 32]);
        let main3c = bitcoin::BlockHash::from_byte_array([32; 32]);
        let main4c = bitcoin::BlockHash::from_byte_array([33; 32]);
        let body_c1 = branch_body(3);
        let header_c1 = Header {
            merkle_root: Body::compute_merkle_root(
                &body_c1.coinbase,
                &body_c1.transactions,
            ),
            prev_side_hash: Some(genesis_hash),
            prev_main_hash: main2c,
        };
        let hash_c1 = header_c1.hash();
        let header_body_c2 = branch_body(4);
        let invalid_body_c2 = branch_body(5);
        let header_c2 = Header {
            merkle_root: Body::compute_merkle_root(
                &header_body_c2.coinbase,
                &header_body_c2.transactions,
            ),
            prev_side_hash: Some(hash_c1),
            prev_main_hash: main3c,
        };
        let hash_c2 = header_c2.hash();
        let mut rwtxn = env.write_txn()?;
        for header_info in [
            mainchain::BlockHeaderInfo {
                block_hash: main2c,
                prev_block_hash: main1,
                height: 2,
                work: bitcoin::Work::from_le_bytes([3; 32]),
            },
            mainchain::BlockHeaderInfo {
                block_hash: main3c,
                prev_block_hash: main2c,
                height: 3,
                work: bitcoin::Work::from_le_bytes([3; 32]),
            },
            mainchain::BlockHeaderInfo {
                block_hash: main4c,
                prev_block_hash: main3c,
                height: 4,
                work: bitcoin::Work::from_le_bytes([3; 32]),
            },
        ] {
            archive.put_main_header_info(&mut rwtxn, &header_info)?;
        }
        archive.put_main_block_info(
            &mut rwtxn,
            main2c,
            &mainchain::BlockInfo::default(),
        )?;
        archive.put_main_block_info(
            &mut rwtxn,
            main3c,
            &mainchain::BlockInfo {
                bmm_commitment: Some(hash_c1),
                events: Vec::new(),
            },
        )?;
        archive.put_main_block_info(
            &mut rwtxn,
            main4c,
            &mainchain::BlockInfo {
                bmm_commitment: Some(hash_c2),
                events: Vec::new(),
            },
        )?;
        archive.put_header(&mut rwtxn, &header_c1)?;
        archive.put_body(&mut rwtxn, hash_c1, &body_c1)?;
        archive.put_header(&mut rwtxn, &header_c2)?;
        archive.put_body(&mut rwtxn, hash_c2, &invalid_body_c2)?;
        rwtxn.commit()?;

        let err = reorg_to_tip_on_mainchain(
            &env,
            &archive,
            &mempool,
            &state,
            #[cfg(feature = "zmq")]
            &zmq_pub_handler,
            Tip {
                block_hash: hash_c2,
                main_block_hash: main4c,
            },
            main4c,
        )
        .unwrap_err();
        assert!(matches!(err, Error::InvalidBlock(_)));
        let rotxn = env.read_txn()?;
        assert_eq!(state.try_get_tip(&rotxn)?, Some(hash_b));
        assert_eq!(
            bincode::serialize(&*state.utreexo_accumulator.lock())?,
            expected_bytes
        );
        assert!(archive.try_get_accumulator_diff(&rotxn, hash_c1)?.is_none());
        assert!(archive.try_get_body(&rotxn, hash_c1)?.is_some());
        assert!(archive.try_get_body(&rotxn, hash_c2)?.is_none());

        // A current tip without verification on the canonical mainchain cannot
        // silently keep running when the required replacement crosses the
        // retained accumulator floor.
        drop(rotxn);
        let mut rwtxn = env.write_txn()?;
        archive.advance_accumulator_reorg_floor(&mut rwtxn, 1)?;
        rwtxn.commit()?;
        let err = reorg_to_tip_on_mainchain(
            &env,
            &archive,
            &mempool,
            &state,
            #[cfg(feature = "zmq")]
            &zmq_pub_handler,
            Tip {
                block_hash: hash_c2,
                main_block_hash: main4c,
            },
            main4c,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            Error::MainchainDisconnectBelowAccumulatorFloor {
                target_height: Some(0),
                retained_floor: 1,
            }
        ));
        let rotxn = env.read_txn()?;
        assert_eq!(state.try_get_tip(&rotxn)?, Some(hash_b));
        assert_eq!(
            bincode::serialize(&*state.utreexo_accumulator.lock())?,
            expected_bytes
        );
        Ok(())
    }

    // a peer's invalid block (value out > value in) must not be fatal
    #[test]
    fn invalid_peer_block_is_not_fatal() {
        let err = Error::InvalidBlock(Box::new(state::Error::NotEnoughFees));
        assert!(!is_fatal_reorg_error(&err));
        assert_eq!(reorg_error_class(&err), ReorgErrorClass::InvalidBlock);
    }

    #[test]
    fn semantic_state_error_rejects_tip_without_stopping_net_task() {
        let err = Error::State(Box::new(state::Error::NotEnoughFees));
        assert!(!is_fatal_reorg_error(&err));
        assert_eq!(reorg_error_class(&err), ReorgErrorClass::Rejected);
    }

    #[test]
    fn mainchain_reconciliation_state_error_is_fatal() {
        let err = classify_mainchain_reconciliation_error(Error::State(
            Box::new(state::Error::NotEnoughFees),
        ));
        assert!(matches!(err, Error::MainchainReconciliationState(_)));
        assert!(is_fatal_reorg_error(&err));
        assert_eq!(reorg_error_class(&err), ReorgErrorClass::Fatal);
    }

    #[test]
    fn missing_metadata_blocks_without_stopping_net_task() {
        let err = Error::MissingWithdrawalBundleMetadata {
            event_block_hash: bitcoin::BlockHash::from_byte_array([1; 32]),
            m6id: crate::types::M6id(bitcoin::Txid::from_byte_array([2; 32])),
        };
        assert!(!is_fatal_reorg_error(&err));
        assert_eq!(reorg_error_class(&err), ReorgErrorClass::Blocked);
    }

    #[test]
    fn infrastructure_error_is_fatal() {
        assert!(is_fatal_reorg_error(&Error::PeerInfoRxClosed));
        assert_eq!(
            reorg_error_class(&Error::PeerInfoRxClosed),
            ReorgErrorClass::Fatal
        );
    }
}
