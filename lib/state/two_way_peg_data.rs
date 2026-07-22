//! Connect and disconnect two-way peg data

use std::collections::{BTreeMap, HashMap};

use fallible_iterator::FallibleIterator;
use sneed::{RoTxn, RwTxn};

use crate::{
    state::{
        Error, State, WITHDRAWAL_BUNDLE_FAILURE_GAP, WithdrawalBundleInfo,
        error,
        rollback::{HeightStamped, RollBack},
    },
    types::{
        AccumulatorDiff, AddressOutPointKey, AggregatedWithdrawal,
        AmountOverflowError, FilledOutput, FilledOutputContent, InPoint, M6id,
        OutPoint, OutPointKey, SpentOutput, WithdrawalBundle,
        WithdrawalBundleEvent, WithdrawalBundleEventStatus,
        WithdrawalBundleStatus, WithdrawalOutputContent,
        proto::mainchain::{BlockEvent, BlockInfo, TwoWayPegData},
        utreexo_leaf_hash,
    },
};

fn diff_add(
    diff: &mut AccumulatorDiff,
    outpoint: &OutPoint,
    output: &FilledOutput,
) {
    diff.insert(utreexo_leaf_hash(outpoint, output));
}

fn diff_remove(
    diff: &mut AccumulatorDiff,
    outpoint: &OutPoint,
    output: &FilledOutput,
) {
    diff.remove(utreexo_leaf_hash(outpoint, output));
}

pub(crate) struct ConnectContext {
    block_height: u32,
    latest_deposit_block_hash: Option<bitcoin::BlockHash>,
    latest_withdrawal_bundle_event_block_hash: Option<bitcoin::BlockHash>,
    accumulator_diff: AccumulatorDiff,
}

fn collect_withdrawal_bundle(
    state: &State,
    txn: &RoTxn,
    block_height: u32,
) -> Result<Option<WithdrawalBundle>, Error> {
    collect_withdrawal_bundle_with_max_inputs(
        state,
        txn,
        block_height,
        WithdrawalBundle::MAX_INPUTS,
    )
}

fn collect_withdrawal_bundle_with_max_inputs(
    state: &State,
    txn: &RoTxn,
    block_height: u32,
    max_inputs: usize,
) -> Result<Option<WithdrawalBundle>, Error> {
    let max_inputs = max_inputs.min(WithdrawalBundle::MAX_INPUTS);
    // Select a deterministic, bounded prefix before aggregating by
    // destination. Once a bundle is submitted those UTXOs leave the set, so
    // later bundles naturally continue with the remaining withdrawals.
    // destination -> (value, mainchain fee, spent_utxos)
    let mut address_to_aggregated_withdrawal = HashMap::<
        bitcoin::Address<bitcoin::address::NetworkUnchecked>,
        AggregatedWithdrawal,
    >::new();
    let mut utxos = state.utxos.iter(txn)?.map_err(Error::from);
    let mut selected_inputs = 0;
    while selected_inputs < max_inputs {
        let Some((outpoint, output)) = utxos.next()? else {
            break;
        };
        if let FilledOutputContent::BitcoinWithdrawal(
            WithdrawalOutputContent {
                value,
                ref main_address,
                main_fee,
            },
        ) = output.content
        {
            let aggregated = address_to_aggregated_withdrawal
                .entry(main_address.clone())
                .or_insert(AggregatedWithdrawal {
                    spend_utxos: BTreeMap::new(),
                    main_address: main_address.clone(),
                    value: bitcoin::Amount::ZERO,
                    main_fee: bitcoin::Amount::ZERO,
                });
            // Add up all values.
            aggregated.value = aggregated
                .value
                .checked_add(value)
                .ok_or(AmountOverflowError)?;
            aggregated.main_fee = aggregated
                .main_fee
                .checked_add(main_fee)
                .ok_or(AmountOverflowError)?;
            aggregated
                .spend_utxos
                .insert(outpoint.to_outpoint(), output);
            selected_inputs += 1;
        }
    }
    if address_to_aggregated_withdrawal.is_empty() {
        return Ok(None);
    }
    let mut aggregated_withdrawals: Vec<_> =
        address_to_aggregated_withdrawal.into_values().collect();
    aggregated_withdrawals.sort_by(|lhs, rhs| rhs.cmp(lhs));
    let mut fee = bitcoin::Amount::ZERO;
    let mut spend_utxos = BTreeMap::<OutPoint, FilledOutput>::new();
    let mut bundle_outputs = Vec::new();
    let mut bundle_txouts_size: u32 = 0;
    for aggregated in aggregated_withdrawals {
        let remaining_inputs = max_inputs.saturating_sub(spend_utxos.len());
        if remaining_inputs == 0 {
            break;
        }
        let script_pubkey =
            aggregated.main_address.assume_checked_ref().script_pubkey();
        let Ok(n_outputs) = u32::try_from(bundle_outputs.len() + 1) else {
            break;
        };
        let Ok(spk_size) = u32::try_from(script_pubkey.len()) else {
            // This SPK is invalid, but others might be ok
            continue;
        };
        let Some(txout_size) = WithdrawalBundle::txout_size(spk_size) else {
            // This SPK is invalid, but others might be ok
            continue;
        };
        if let Some(sum_txout_sizes) =
            bundle_txouts_size.checked_add(txout_size)
        {
            bundle_txouts_size = sum_txout_sizes;
        } else {
            break;
        };
        if WithdrawalBundle::predict_weight(n_outputs, bundle_txouts_size)
            .is_none()
        {
            break;
        }
        let selected_spend_utxos: BTreeMap<_, _> = aggregated
            .spend_utxos
            .into_iter()
            .take(remaining_inputs)
            .collect();
        let mut selected_value = bitcoin::Amount::ZERO;
        let mut selected_fee = bitcoin::Amount::ZERO;
        for output in selected_spend_utxos.values() {
            let FilledOutputContent::BitcoinWithdrawal(withdrawal) =
                &output.content
            else {
                unreachable!(
                    "aggregated withdrawal contains non-withdrawal output"
                )
            };
            selected_value = selected_value
                .checked_add(withdrawal.value)
                .ok_or(AmountOverflowError)?;
            selected_fee = selected_fee
                .checked_add(withdrawal.main_fee)
                .ok_or(AmountOverflowError)?;
        }
        let bundle_output = bitcoin::TxOut {
            value: selected_value,
            script_pubkey,
        };
        spend_utxos.extend(selected_spend_utxos);
        bundle_outputs.push(bundle_output);
        fee += selected_fee;
    }
    let bundle =
        WithdrawalBundle::new(block_height, fee, spend_utxos, bundle_outputs)?;
    if bundle.tx().weight().to_wu()
        > bitcoin::policy::MAX_STANDARD_TX_WEIGHT as u64
    {
        Err(Error::BundleTooHeavy {
            weight: bundle.tx().weight().to_wu(),
            max_weight: bitcoin::policy::MAX_STANDARD_TX_WEIGHT as u64,
        })?;
    }
    Ok(Some(bundle))
}

fn connect_withdrawal_bundle_submitted(
    state: &State,
    rwtxn: &mut RwTxn,
    block_height: u32,
    event_block_hash: &bitcoin::BlockHash,
    m6id: M6id,
    accumulator_diff: &mut AccumulatorDiff,
) -> Result<(), error::ConnectWithdrawalBundleSubmitted> {
    if let Some(bundle_m6id) =
        state.pending_withdrawal_bundle.try_get(rwtxn, &())?
        && bundle_m6id == m6id
    {
        tracing::debug!(
            %block_height,
            %m6id,
            "Pending withdrawal bundle submission confirmed"
        );
        let (bundle, mut bundle_status) = state
            .withdrawal_bundles
            .try_get(rwtxn, &m6id)?
            .ok_or(error::PendingWithdrawalBundleUnknown(m6id))?;
        let bundle = match bundle {
            WithdrawalBundleInfo::Known(bundle) => bundle,
            WithdrawalBundleInfo::Unknown
            | WithdrawalBundleInfo::UnknownConfirmed { spend_utxos: _ } => {
                let err = error::PendingWithdrawalBundleUnknown(m6id);
                return Err(err.into());
            }
        };
        state.withdrawal_bundle_archive.put(
            rwtxn,
            &m6id,
            &bundle.metadata(),
        )?;
        for (outpoint, output) in bundle.spend_utxos() {
            if !spend_withdrawal_utxo(state, rwtxn, m6id, outpoint, output)? {
                return Err(error::NoUtxo {
                    outpoint: *outpoint,
                }
                .into());
            }
            diff_remove(accumulator_diff, outpoint, output);
        }
        assert_eq!(
            bundle_status.latest().value,
            WithdrawalBundleStatus::Pending
        );
        bundle_status
            .push(WithdrawalBundleStatus::Submitted, block_height)
            .expect("push submitted status should be valid");
        state.withdrawal_bundles.put(
            rwtxn,
            &m6id,
            &(WithdrawalBundleInfo::Known(bundle), bundle_status),
        )?;
        state.pending_withdrawal_bundle.delete(rwtxn, &())?;
    } else if let Some((bundle, mut bundle_status)) =
        state.withdrawal_bundles.try_get(rwtxn, &m6id)?
    {
        match (&bundle, bundle_status.latest().value) {
            (_, WithdrawalBundleStatus::Confirmed) => {
                let err = error::ConnectWithdrawalBundleSubmitted::ConfirmedResubmitted {
                    event_block_hash: *event_block_hash,
                    m6id
                };
                return Err(err);
            }
            (
                _,
                WithdrawalBundleStatus::Submitted
                | WithdrawalBundleStatus::SubmittedUnexpected,
            ) => {
                let err =
                    error::ConnectWithdrawalBundleSubmitted::Resubmitted {
                        event_block_hash: *event_block_hash,
                        m6id,
                        submitted_block_height: bundle_status.latest().height,
                    };
                return Err(err);
            }
            (
                WithdrawalBundleInfo::Known(_),
                WithdrawalBundleStatus::Dropped,
            ) => {
                tracing::warn!(%event_block_hash, %m6id, "dropped bundle submitted");
            }
            (
                WithdrawalBundleInfo::Unknown
                | WithdrawalBundleInfo::UnknownConfirmed { spend_utxos: _ },
                WithdrawalBundleStatus::Dropped,
            ) => {
                let err =
                    error::ConnectWithdrawalBundleSubmitted::UnknownDropped {
                        m6id,
                        dropped_block_height: bundle_status.latest().height,
                    };
                return Err(err);
            }
            (
                WithdrawalBundleInfo::Known(_),
                WithdrawalBundleStatus::Pending,
            ) => {
                let err =
                    error::ConnectWithdrawalBundleSubmitted::DroppedPending(
                        m6id,
                    );
                return Err(err);
            }
            (
                WithdrawalBundleInfo::Unknown
                | WithdrawalBundleInfo::UnknownConfirmed { spend_utxos: _ },
                WithdrawalBundleStatus::Pending,
            ) => {
                let err =
                    error::ConnectWithdrawalBundleSubmitted::UnknownPending {
                        m6id,
                        pending_block_height: bundle_status.latest().height,
                    };
                return Err(err);
            }
            (
                WithdrawalBundleInfo::Known(_) | WithdrawalBundleInfo::Unknown,
                WithdrawalBundleStatus::Failed,
            ) => {
                tracing::warn!(%event_block_hash, %m6id, "failed bundle resubmitted");
            }
            (
                WithdrawalBundleInfo::UnknownConfirmed { spend_utxos: _ },
                WithdrawalBundleStatus::Failed,
            ) => {
                let err = error::ConnectWithdrawalBundleSubmitted::UnknownConfirmedFailed {
                    m6id,
                    failed_block_height: bundle_status.latest().height,
                };
                return Err(err);
            }
        }
        bundle_status
            .push(WithdrawalBundleStatus::SubmittedUnexpected, block_height)
            .expect("push submitted unexpected status should be valid");
        state
            .withdrawal_bundles
            .put(rwtxn, &m6id, &(bundle, bundle_status))?
    } else if let Some(archived_metadata) =
        state.withdrawal_bundle_archive.try_get(rwtxn, &m6id)?
    {
        if !archived_metadata.has_valid_inputs_commitment()
            || !archived_metadata.is_within_size_limit()
        {
            return Err(
                error::ConnectWithdrawalBundleSubmitted::MissingWithdrawalBundleMetadata {
                    event_block_hash: *event_block_hash,
                    m6id,
                },
            );
        }
        let mut spend_utxos = BTreeMap::new();
        for outpoint in archived_metadata.spend_outpoints() {
            let outpoint_key = OutPointKey::from_outpoint(outpoint);
            let output = state.utxos.try_get(rwtxn, &outpoint_key)?.ok_or(
                error::NoUtxo {
                    outpoint: *outpoint,
                },
            )?;
            spend_utxos.insert(*outpoint, output);
        }
        let bundle = archived_metadata
            .with_spend_utxos(spend_utxos)
            .expect("recovered spend UTXOs preserve archived outpoint keys");
        tracing::info!(
            %event_block_hash,
            %m6id,
            "Recovered withdrawal bundle metadata for L1 submission"
        );
        for (outpoint, output) in bundle.spend_utxos() {
            if !spend_withdrawal_utxo(state, rwtxn, m6id, outpoint, output)? {
                return Err(error::NoUtxo {
                    outpoint: *outpoint,
                }
                .into());
            }
            diff_remove(accumulator_diff, outpoint, output);
        }
        state.withdrawal_bundles.put(
            rwtxn,
            &m6id,
            &(
                WithdrawalBundleInfo::Known(bundle),
                RollBack::<HeightStamped<_>>::new(
                    WithdrawalBundleStatus::Submitted,
                    block_height,
                ),
            ),
        )?;
    } else {
        return Err(
            error::ConnectWithdrawalBundleSubmitted::MissingWithdrawalBundleMetadata {
                event_block_hash: *event_block_hash,
                m6id,
            },
        );
    };
    Ok(())
}

fn connect_withdrawal_bundle_confirmed(
    state: &State,
    rwtxn: &mut RwTxn,
    block_height: u32,
    event_block_hash: &bitcoin::BlockHash,
    m6id: M6id,
    accumulator_diff: &mut AccumulatorDiff,
) -> Result<(), Error> {
    let Some((bundle, mut bundle_status)) =
        state.withdrawal_bundles.try_get(rwtxn, &m6id)?
    else {
        return Err(Error::MissingWithdrawalBundleMetadata {
            event_block_hash: *event_block_hash,
            m6id,
        });
    };
    if bundle_status.latest().value == WithdrawalBundleStatus::Confirmed {
        // Already applied
        return Ok(());
    }
    assert!(matches!(
        bundle_status.latest().value,
        WithdrawalBundleStatus::Submitted
            | WithdrawalBundleStatus::SubmittedUnexpected
    ));
    match &bundle {
        WithdrawalBundleInfo::UnknownConfirmed { spend_utxos: _ } => {
            return Err(Error::UnknownWithdrawalBundleReconfirmed {
                event_block_hash: *event_block_hash,
                m6id,
            });
        }
        WithdrawalBundleInfo::Unknown => {
            return Err(Error::MissingWithdrawalBundleMetadata {
                event_block_hash: *event_block_hash,
                m6id,
            });
        }
        WithdrawalBundleInfo::Known(bundle) => {
            if matches!(
                bundle_status.latest().value,
                WithdrawalBundleStatus::SubmittedUnexpected
            ) {
                // If a previously dropped or failed bundle is confirmed,
                // then unless all of the bundle UTXOs can be spent,
                // the chain is insolvent, and cannot continue.
                tracing::warn!(
                    %event_block_hash,
                    %m6id,
                    "Unexpected withdrawal bundle confirmed, marking bundle UTXOs as spent"
                );
                for (outpoint, output) in bundle.spend_utxos() {
                    if !spend_withdrawal_utxo(
                        state, rwtxn, m6id, outpoint, output,
                    )? {
                        return Err(
                            Error::UnexpectedWithdrawalBundleInsolvency {
                                event_block_hash: *event_block_hash,
                                m6id,
                                outpoint: *outpoint,
                            },
                        );
                    }
                    diff_remove(accumulator_diff, outpoint, output);
                }
            }
        }
    }
    bundle_status
        .push(WithdrawalBundleStatus::Confirmed, block_height)
        .expect("Push confirmed status should be valid");
    state
        .withdrawal_bundles
        .put(rwtxn, &m6id, &(bundle, bundle_status))?;
    Ok(())
}

fn connect_withdrawal_bundle_failed(
    state: &State,
    rwtxn: &mut RwTxn,
    block_height: u32,
    m6id: M6id,
    accumulator_diff: &mut AccumulatorDiff,
) -> Result<(), Error> {
    tracing::debug!(
        %block_height,
        %m6id,
        "Handling failed withdrawal bundle");
    let (bundle, mut bundle_status) = state
        .withdrawal_bundles
        .try_get(rwtxn, &m6id)?
        .ok_or_else(|| Error::UnknownWithdrawalBundle { m6id })?;
    if bundle_status.latest().value == WithdrawalBundleStatus::Failed {
        // Already applied
        return Ok(());
    }
    assert!(matches!(
        bundle_status.latest().value,
        WithdrawalBundleStatus::Submitted
            | WithdrawalBundleStatus::SubmittedUnexpected
    ));
    match &bundle {
        WithdrawalBundleInfo::Unknown
        | WithdrawalBundleInfo::UnknownConfirmed { .. } => (),
        WithdrawalBundleInfo::Known(bundle) => 'known: {
            if matches!(
                bundle_status.latest().value,
                WithdrawalBundleStatus::SubmittedUnexpected
            ) {
                break 'known;
            }
            for (outpoint, output) in bundle.spend_utxos() {
                let outpoint_key = OutPointKey::from_outpoint(outpoint);
                if !state.unspend_utxo(rwtxn, &outpoint_key)? {
                    return Err(Error::NoStxo {
                        outpoint: *outpoint,
                    });
                };
                diff_add(accumulator_diff, outpoint, output);
            }
            let latest_failed_m6id = if let Some(mut latest_failed_m6id) =
                state.latest_failed_withdrawal_bundle.try_get(rwtxn, &())?
            {
                latest_failed_m6id
                    .push(m6id, block_height)
                    .expect("Push latest failed m6id should be valid");
                latest_failed_m6id
            } else {
                RollBack::<HeightStamped<_>>::new(m6id, block_height)
            };
            state.latest_failed_withdrawal_bundle.put(
                rwtxn,
                &(),
                &latest_failed_m6id,
            )?;
        }
    }
    bundle_status
        .push(WithdrawalBundleStatus::Failed, block_height)
        .expect("Push failed status should be valid");
    state
        .withdrawal_bundles
        .put(rwtxn, &m6id, &(bundle, bundle_status))?;
    Ok(())
}

fn connect_withdrawal_bundle_event(
    state: &State,
    rwtxn: &mut RwTxn,
    block_height: u32,
    event_block_hash: &bitcoin::BlockHash,
    event: &WithdrawalBundleEvent,
    accumulator_diff: &mut AccumulatorDiff,
) -> Result<(), Error> {
    match event.status {
        WithdrawalBundleEventStatus::Submitted => {
            connect_withdrawal_bundle_submitted(
                state,
                rwtxn,
                block_height,
                event_block_hash,
                event.m6id,
                accumulator_diff,
            )
            .map_err(Error::ConnectWithdrawalBundleSubmitted)
        }
        WithdrawalBundleEventStatus::Confirmed => {
            connect_withdrawal_bundle_confirmed(
                state,
                rwtxn,
                block_height,
                event_block_hash,
                event.m6id,
                accumulator_diff,
            )
        }
        WithdrawalBundleEventStatus::Failed => {
            connect_withdrawal_bundle_failed(
                state,
                rwtxn,
                block_height,
                event.m6id,
                accumulator_diff,
            )
        }
    }
}

fn connect_2wpd_event(
    state: &State,
    rwtxn: &mut RwTxn,
    block_height: u32,
    event_block_hash: bitcoin::BlockHash,
    event: &BlockEvent,
    context: &mut ConnectContext,
) -> Result<(), Error> {
    match event {
        BlockEvent::Deposit(deposit) => {
            let outpoint = OutPoint::Deposit(deposit.outpoint);
            let output = deposit.output.clone();
            let outpoint_key = OutPointKey::from_outpoint(&outpoint);
            if state.utxos.try_get(rwtxn, &outpoint_key)?.is_some()
                || state.stxos.try_get(rwtxn, &outpoint_key)?.is_some()
            {
                return Err(Error::DuplicateDeposit { outpoint });
            }
            state.put_utxo(rwtxn, outpoint_key, output, block_height)?;
            diff_add(&mut context.accumulator_diff, &outpoint, &deposit.output);
            context.latest_deposit_block_hash = Some(event_block_hash);
        }
        BlockEvent::WithdrawalBundle(withdrawal_bundle_event) => {
            let () = connect_withdrawal_bundle_event(
                state,
                rwtxn,
                block_height,
                &event_block_hash,
                withdrawal_bundle_event,
                &mut context.accumulator_diff,
            )?;
            context.latest_withdrawal_bundle_event_block_hash =
                Some(event_block_hash);
        }
    }
    Ok(())
}

pub(crate) fn begin_connect(
    state: &State,
    rwtxn: &mut RwTxn,
) -> Result<ConnectContext, Error> {
    let block_height = state.try_get_height(rwtxn)?.ok_or(Error::NoTip)?;
    tracing::trace!(%block_height, "Connecting 2WPD...");
    Ok(ConnectContext {
        block_height,
        latest_deposit_block_hash: None,
        latest_withdrawal_bundle_event_block_hash: None,
        accumulator_diff: AccumulatorDiff::default(),
    })
}

pub(crate) fn connect_block_info(
    state: &State,
    rwtxn: &mut RwTxn,
    event_block_hash: bitcoin::BlockHash,
    block_info: &BlockInfo,
    context: &mut ConnectContext,
) -> Result<(), Error> {
    for event in &block_info.events {
        connect_2wpd_event(
            state,
            rwtxn,
            context.block_height,
            event_block_hash,
            event,
            context,
        )?;
    }
    Ok(())
}

pub(crate) fn finish_connect(
    state: &State,
    rwtxn: &mut RwTxn,
    context: ConnectContext,
) -> Result<AccumulatorDiff, Error> {
    let block_height = context.block_height;
    // Handle deposits.
    if let Some(latest_deposit_block_hash) = context.latest_deposit_block_hash {
        let deposit_block_seq_idx = state
            .deposit_blocks
            .last(rwtxn)?
            .map_or(0, |(seq_idx, _)| seq_idx + 1);
        state.deposit_blocks.put(
            rwtxn,
            &deposit_block_seq_idx,
            &(latest_deposit_block_hash, block_height),
        )?;
    }
    // Handle withdrawals
    if let Some(latest_withdrawal_bundle_event_block_hash) =
        context.latest_withdrawal_bundle_event_block_hash
    {
        let withdrawal_bundle_event_block_seq_idx = state
            .withdrawal_bundle_event_blocks
            .last(rwtxn)?
            .map_or(0, |(seq_idx, _)| seq_idx + 1);
        state.withdrawal_bundle_event_blocks.put(
            rwtxn,
            &withdrawal_bundle_event_block_seq_idx,
            &(latest_withdrawal_bundle_event_block_hash, block_height),
        )?;
    }
    let last_withdrawal_bundle_failure_height = state
        .get_latest_failed_withdrawal_bundle(rwtxn)?
        .map(|(height, _bundle)| height)
        .unwrap_or_default();
    if block_height - last_withdrawal_bundle_failure_height
        >= WITHDRAWAL_BUNDLE_FAILURE_GAP
        && state
            .pending_withdrawal_bundle
            .try_get(rwtxn, &())?
            .is_none()
        && let Some(bundle) =
            collect_withdrawal_bundle(state, rwtxn, block_height)?
    {
        let m6id = bundle.compute_m6id();
        state.pending_withdrawal_bundle.put(rwtxn, &(), &m6id)?;
        let bundle_status = if let Some((_bundle, mut bundle_status)) =
            state.withdrawal_bundles.try_get(rwtxn, &m6id)?
        {
            bundle_status
                .push(WithdrawalBundleStatus::Pending, block_height)
                .expect("push pending status should be valid");
            bundle_status
        } else {
            RollBack::<HeightStamped<_>>::new(
                WithdrawalBundleStatus::Pending,
                block_height,
            )
        };
        state.put_withdrawal_bundle_metadata(rwtxn, &bundle.metadata())?;
        state.withdrawal_bundles.put(
            rwtxn,
            &m6id,
            &(WithdrawalBundleInfo::Known(bundle), bundle_status),
        )?;
        tracing::trace!(
            %block_height,
            %m6id,
            "Stored pending withdrawal bundle"
        );
    }
    Ok(context.accumulator_diff)
}

pub fn connect(
    state: &State,
    rwtxn: &mut RwTxn,
    two_way_peg_data: &TwoWayPegData,
) -> Result<AccumulatorDiff, Error> {
    let mut context = begin_connect(state, rwtxn)?;
    for (event_block_hash, event_block_info) in &two_way_peg_data.block_info {
        connect_block_info(
            state,
            rwtxn,
            *event_block_hash,
            event_block_info,
            &mut context,
        )?;
    }
    finish_connect(state, rwtxn, context)
}

fn disconnect_withdrawal_bundle_submitted(
    state: &State,
    rwtxn: &mut RwTxn,
    block_height: u32,
    m6id: M6id,
) -> Result<(), Error> {
    let Some((bundle, bundle_status)) =
        state.withdrawal_bundles.try_get(rwtxn, &m6id)?
    else {
        if let Some(pending_bundle_m6id) =
            state.pending_withdrawal_bundle.try_get(rwtxn, &())?
            && pending_bundle_m6id == m6id
        {
            // Already applied
            return Ok(());
        } else {
            return Err(Error::UnknownWithdrawalBundle { m6id });
        }
    };
    let (bundle_status, latest_bundle_status) = bundle_status.pop();
    assert!(matches!(
        latest_bundle_status.value,
        WithdrawalBundleStatus::Submitted
            | WithdrawalBundleStatus::SubmittedUnexpected
    ));
    assert_eq!(latest_bundle_status.height, block_height);
    match &bundle {
        WithdrawalBundleInfo::Unknown
        | WithdrawalBundleInfo::UnknownConfirmed { .. } => (),
        WithdrawalBundleInfo::Known(bundle) => {
            let submitted_from_pending =
                bundle_status.as_ref().is_some_and(|bundle_status| {
                    bundle_status.latest().value
                        == WithdrawalBundleStatus::Pending
                });
            let submitted_from_archive = bundle_status.is_none()
                && latest_bundle_status.value
                    == WithdrawalBundleStatus::Submitted;
            if submitted_from_pending || submitted_from_archive {
                for outpoint in bundle.spend_utxos().keys().rev() {
                    let outpoint_key = OutPointKey::from_outpoint(outpoint);
                    if !state.unspend_utxo(rwtxn, &outpoint_key)? {
                        return Err(Error::NoStxo {
                            outpoint: *outpoint,
                        });
                    }
                }
            }
            if submitted_from_pending {
                state.pending_withdrawal_bundle.put(rwtxn, &(), &m6id)?;
            }
        }
    }
    if let Some(bundle_status) = bundle_status {
        state
            .withdrawal_bundles
            .put(rwtxn, &m6id, &(bundle, bundle_status))?;
    } else {
        state.withdrawal_bundles.delete(rwtxn, &m6id)?;
    }
    Ok(())
}

fn disconnect_withdrawal_bundle_confirmed(
    state: &State,
    rwtxn: &mut RwTxn,
    block_height: u32,
    m6id: M6id,
) -> Result<(), Error> {
    let (mut bundle, bundle_status) = state
        .withdrawal_bundles
        .try_get(rwtxn, &m6id)?
        .ok_or_else(|| Error::UnknownWithdrawalBundle { m6id })?;
    let (prev_bundle_status, latest_bundle_status) = bundle_status.pop();
    if matches!(
        latest_bundle_status.value,
        WithdrawalBundleStatus::Submitted
            | WithdrawalBundleStatus::SubmittedUnexpected
    ) {
        // Already applied
        return Ok(());
    }
    assert_eq!(
        latest_bundle_status.value,
        WithdrawalBundleStatus::Confirmed
    );
    assert_eq!(latest_bundle_status.height, block_height);
    let prev_bundle_status = prev_bundle_status
        .expect("Pop confirmed bundle status should be valid");
    assert!(matches!(
        prev_bundle_status.latest().value,
        WithdrawalBundleStatus::Submitted
            | WithdrawalBundleStatus::SubmittedUnexpected
    ));
    match &bundle {
        WithdrawalBundleInfo::Known(bundle) => {
            if matches!(
                prev_bundle_status.latest().value,
                WithdrawalBundleStatus::SubmittedUnexpected
            ) {
                for outpoint in bundle.spend_utxos().keys() {
                    let outpoint_key = OutPointKey::from(outpoint);
                    if !state.unspend_utxo(rwtxn, &outpoint_key)? {
                        return Err(Error::NoStxo {
                            outpoint: *outpoint,
                        });
                    }
                }
            }
        }
        WithdrawalBundleInfo::UnknownConfirmed { spend_utxos } => {
            for outpoint in spend_utxos.keys() {
                let outpoint_key = OutPointKey::from_outpoint(outpoint);
                if !state.unspend_utxo(rwtxn, &outpoint_key)? {
                    return Err(Error::NoStxo {
                        outpoint: *outpoint,
                    });
                }
            }
            bundle = WithdrawalBundleInfo::Unknown;
        }
        WithdrawalBundleInfo::Unknown => (),
    }
    state.withdrawal_bundles.put(
        rwtxn,
        &m6id,
        &(bundle, prev_bundle_status),
    )?;
    Ok(())
}

fn disconnect_withdrawal_bundle_failed(
    state: &State,
    rwtxn: &mut RwTxn,
    block_height: u32,
    m6id: M6id,
) -> Result<(), Error> {
    let (bundle, bundle_status) = state
        .withdrawal_bundles
        .try_get(rwtxn, &m6id)?
        .ok_or_else(|| Error::UnknownWithdrawalBundle { m6id })?;
    let (prev_bundle_status, latest_bundle_status) = bundle_status.pop();
    if matches!(
        latest_bundle_status.value,
        WithdrawalBundleStatus::Submitted
            | WithdrawalBundleStatus::SubmittedUnexpected
    ) {
        // Already applied
        return Ok(());
    } else {
        assert_eq!(latest_bundle_status.value, WithdrawalBundleStatus::Failed);
    }
    assert_eq!(latest_bundle_status.height, block_height);
    let prev_bundle_status =
        prev_bundle_status.expect("Pop failed bundle status should be valid");
    assert!(matches!(
        prev_bundle_status.latest().value,
        WithdrawalBundleStatus::Submitted
            | WithdrawalBundleStatus::SubmittedUnexpected
    ));
    match &bundle {
        WithdrawalBundleInfo::Unknown
        | WithdrawalBundleInfo::UnknownConfirmed { .. } => (),
        WithdrawalBundleInfo::Known(bundle) => 'known: {
            if matches!(
                prev_bundle_status.latest().value,
                WithdrawalBundleStatus::SubmittedUnexpected
            ) {
                break 'known;
            }
            for (outpoint, output) in bundle.spend_utxos().iter().rev() {
                if !spend_withdrawal_utxo(state, rwtxn, m6id, outpoint, output)?
                {
                    return Err(error::NoUtxo {
                        outpoint: *outpoint,
                    }
                    .into());
                }
            }
            let (prev_latest_failed_m6id, latest_failed_m6id) = state
                .latest_failed_withdrawal_bundle
                .try_get(rwtxn, &())?
                .expect("latest failed withdrawal bundle should exist")
                .pop();
            assert_eq!(latest_failed_m6id.value, m6id);
            assert_eq!(latest_failed_m6id.height, block_height);
            if let Some(prev_latest_failed_m6id) = prev_latest_failed_m6id {
                state.latest_failed_withdrawal_bundle.put(
                    rwtxn,
                    &(),
                    &prev_latest_failed_m6id,
                )?;
            } else {
                state.latest_failed_withdrawal_bundle.delete(rwtxn, &())?;
            }
        }
    }
    state.withdrawal_bundles.put(
        rwtxn,
        &m6id,
        &(bundle, prev_bundle_status),
    )?;
    Ok(())
}

fn spend_withdrawal_utxo(
    state: &State,
    rwtxn: &mut RwTxn,
    m6id: M6id,
    outpoint: &OutPoint,
    output: &FilledOutput,
) -> Result<bool, sneed::db::Error> {
    let inpoint = InPoint::Withdrawal { m6id };
    let outpoint_key = OutPointKey::from_outpoint(outpoint);
    let spent_output = SpentOutput {
        output: output.clone(),
        inpoint,
    };
    state.spend_utxo(rwtxn, outpoint_key, spent_output)
}

fn disconnect_withdrawal_bundle_event(
    state: &State,
    rwtxn: &mut RwTxn,
    block_height: u32,
    event: &WithdrawalBundleEvent,
) -> Result<(), Error> {
    match event.status {
        WithdrawalBundleEventStatus::Submitted => {
            disconnect_withdrawal_bundle_submitted(
                state,
                rwtxn,
                block_height,
                event.m6id,
            )
        }
        WithdrawalBundleEventStatus::Confirmed => {
            disconnect_withdrawal_bundle_confirmed(
                state,
                rwtxn,
                block_height,
                event.m6id,
            )
        }
        WithdrawalBundleEventStatus::Failed => {
            disconnect_withdrawal_bundle_failed(
                state,
                rwtxn,
                block_height,
                event.m6id,
            )
        }
    }
}

fn disconnect_event(
    state: &State,
    rwtxn: &mut RwTxn,
    block_height: u32,
    latest_deposit_block_hash: &mut Option<bitcoin::BlockHash>,
    latest_withdrawal_bundle_event_block_hash: &mut Option<bitcoin::BlockHash>,
    event_block_hash: bitcoin::BlockHash,
    event: &BlockEvent,
) -> Result<(), Error> {
    match event {
        BlockEvent::Deposit(deposit) => {
            let outpoint = OutPoint::Deposit(deposit.outpoint);
            let outpoint_key = OutPointKey::from_outpoint(&outpoint);
            let address_key =
                AddressOutPointKey::new(deposit.output.address, outpoint_key);
            if !state.delete_utxo(rwtxn, &address_key)? {
                return Err(error::NoUtxo { outpoint }.into());
            }
            // Blocks are iterated in reverse here, so the first event block
            // hash seen is the latest. Keep it to match what `connect` stored.
            if latest_deposit_block_hash.is_none() {
                *latest_deposit_block_hash = Some(event_block_hash);
            }
        }
        BlockEvent::WithdrawalBundle(withdrawal_bundle_event) => {
            let () = disconnect_withdrawal_bundle_event(
                state,
                rwtxn,
                block_height,
                withdrawal_bundle_event,
            )?;
            // Blocks are iterated in reverse here, so the first event block
            // hash seen is the latest. Keep it to match what `connect` stored.
            if latest_withdrawal_bundle_event_block_hash.is_none() {
                *latest_withdrawal_bundle_event_block_hash =
                    Some(event_block_hash);
            }
        }
    }
    Ok(())
}

pub(crate) struct DisconnectContext {
    block_height: u32,
    latest_deposit_block_hash: Option<bitcoin::BlockHash>,
    latest_withdrawal_bundle_event_block_hash: Option<bitcoin::BlockHash>,
}

pub(crate) fn begin_disconnect(
    state: &State,
    rwtxn: &mut RwTxn,
) -> Result<DisconnectContext, Error> {
    let block_height = state
        .try_get_height(rwtxn)?
        .expect("Height should not be None");
    // `connect` creates a new pending bundle after applying L1 events, so undo
    // that bundle before reversing events which may restore the prior pending
    // bundle.
    let last_withdrawal_bundle_failure_height = state
        .get_latest_failed_withdrawal_bundle(rwtxn)?
        .map(|(height, _bundle)| height)
        .unwrap_or_default();
    if block_height - last_withdrawal_bundle_failure_height
        >= WITHDRAWAL_BUNDLE_FAILURE_GAP
        && let Some(bundle_m6id) =
            state.pending_withdrawal_bundle.try_get(rwtxn, &())?
        && let (bundle, bundle_status) = state
            .withdrawal_bundles
            .try_get(rwtxn, &bundle_m6id)?
            .ok_or(error::PendingWithdrawalBundleUnknown(bundle_m6id))?
        && bundle_status.latest().height == block_height
    {
        state.pending_withdrawal_bundle.delete(rwtxn, &())?;
        if let (Some(bundle_status), _latest_bundle_status) =
            bundle_status.pop()
        {
            state.withdrawal_bundles.put(
                rwtxn,
                &bundle_m6id,
                &(bundle, bundle_status),
            )?;
        } else {
            state.withdrawal_bundles.delete(rwtxn, &bundle_m6id)?;
        }
    }
    Ok(DisconnectContext {
        block_height,
        latest_deposit_block_hash: None,
        latest_withdrawal_bundle_event_block_hash: None,
    })
}

pub(crate) fn disconnect_block_info(
    state: &State,
    rwtxn: &mut RwTxn,
    event_block_hash: bitcoin::BlockHash,
    block_info: &BlockInfo,
    context: &mut DisconnectContext,
) -> Result<(), Error> {
    for event in block_info.events.iter().rev() {
        disconnect_event(
            state,
            rwtxn,
            context.block_height,
            &mut context.latest_deposit_block_hash,
            &mut context.latest_withdrawal_bundle_event_block_hash,
            event_block_hash,
            event,
        )?;
    }
    Ok(())
}

pub(crate) fn finish_disconnect(
    state: &State,
    rwtxn: &mut RwTxn,
    context: DisconnectContext,
) -> Result<(), Error> {
    let block_height = context.block_height;
    // Handle withdrawals
    if let Some(latest_withdrawal_bundle_event_block_hash) =
        context.latest_withdrawal_bundle_event_block_hash
    {
        let (
            last_withdrawal_bundle_event_block_seq_idx,
            (
                last_withdrawal_bundle_event_block_hash,
                last_withdrawal_bundle_event_block_height,
            ),
        ) = state
            .withdrawal_bundle_event_blocks
            .last(rwtxn)?
            .ok_or(Error::NoWithdrawalBundleEventBlock)?;
        assert_eq!(
            latest_withdrawal_bundle_event_block_hash,
            last_withdrawal_bundle_event_block_hash
        );
        assert_eq!(block_height, last_withdrawal_bundle_event_block_height);
        if !state
            .withdrawal_bundle_event_blocks
            .delete(rwtxn, &last_withdrawal_bundle_event_block_seq_idx)?
        {
            return Err(Error::NoWithdrawalBundleEventBlock);
        };
    }
    // Handle deposits
    if let Some(latest_deposit_block_hash) = context.latest_deposit_block_hash {
        let (
            last_deposit_block_seq_idx,
            (last_deposit_block_hash, last_deposit_block_height),
        ) = state
            .deposit_blocks
            .last(rwtxn)?
            .ok_or(Error::NoDepositBlock)?;
        assert_eq!(latest_deposit_block_hash, last_deposit_block_hash);
        assert_eq!(block_height, last_deposit_block_height);
        if !state
            .deposit_blocks
            .delete(rwtxn, &last_deposit_block_seq_idx)?
        {
            return Err(Error::NoDepositBlock);
        };
    }
    Ok(())
}

pub fn disconnect(
    state: &State,
    rwtxn: &mut RwTxn,
    two_way_peg_data: &TwoWayPegData,
) -> Result<(), Error> {
    let mut context = begin_disconnect(state, rwtxn)?;
    // Restore pending withdrawal bundle. Disconnect L1 blocks and their
    // events newest-to-oldest, matching the previous whole-batch traversal.
    for (event_block_hash, event_block_info) in
        two_way_peg_data.block_info.iter().rev()
    {
        disconnect_block_info(
            state,
            rwtxn,
            *event_block_hash,
            event_block_info,
            &mut context,
        )?;
    }
    finish_disconnect(state, rwtxn, context)
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use bitcoin::{
        Network,
        hashes::Hash as _,
        secp256k1::{Secp256k1, SecretKey},
    };
    use hashlink::LinkedHashMap;

    use crate::{
        state::{
            Error, HeightStamped, RollBack, State, WithdrawalBundleInfo,
            test::{bitcoin_filled_output, fresh_state},
            two_way_peg_data::{
                collect_withdrawal_bundle,
                collect_withdrawal_bundle_with_max_inputs, connect,
                connect_withdrawal_bundle_failed,
                connect_withdrawal_bundle_submitted, disconnect,
                disconnect_withdrawal_bundle_failed,
                disconnect_withdrawal_bundle_submitted,
            },
        },
        types::{
            AccumulatorDiff, Address, FilledOutput, FilledOutputContent,
            InPoint, M6id, OutPoint, OutPointKey, SpentOutput, Txid,
            WithdrawalBundle, WithdrawalBundleEvent,
            WithdrawalBundleEventStatus, WithdrawalBundleStatus,
            WithdrawalOutputContent,
            proto::mainchain::{BlockEvent, BlockInfo, Deposit, TwoWayPegData},
        },
    };

    #[test]
    fn unknown_submission_blocks_without_mutating_state() -> anyhow::Result<()>
    {
        let (_temp_dir, env, state) =
            fresh_state("unknown_submission_blocks_without_mutating_state")?;
        let event_block_hash = bitcoin::BlockHash::from_byte_array([2; 32]);
        let m6id = M6id(bitcoin::Txid::from_byte_array([3; 32]));
        let mut rwtxn = env.write_txn()?;
        let mut accumulator_diff = AccumulatorDiff::default();

        let err = connect_withdrawal_bundle_submitted(
            &state,
            &mut rwtxn,
            1,
            &event_block_hash,
            m6id,
            &mut accumulator_diff,
        )
        .unwrap_err();

        assert!(matches!(
            err,
            crate::state::error::ConnectWithdrawalBundleSubmitted::MissingWithdrawalBundleMetadata {
                event_block_hash: actual_event_block_hash,
                m6id: actual_m6id,
            } if actual_event_block_hash == event_block_hash && actual_m6id == m6id
        ));
        assert!(accumulator_diff.is_empty());
        assert!(state.withdrawal_bundles.try_get(&rwtxn, &m6id)?.is_none());
        Ok(())
    }

    #[test]
    fn archived_submission_metadata_survives_disconnect_and_reapplies()
    -> anyhow::Result<()> {
        let (_temp_dir, env, state) = fresh_state(
            "archived_submission_metadata_survives_disconnect_and_reapplies",
        )?;
        let outpoint = OutPoint::Regular {
            txid: Txid::from([1; 32]),
            vout: 0,
        };
        let outpoint_key = OutPointKey::from(&outpoint);
        let output = bitcoin_filled_output(Address::ALL_ZEROS, 1_000);
        let mut spend_utxos = BTreeMap::new();
        spend_utxos.insert(outpoint, output.clone());
        let bundle = WithdrawalBundle::new(
            7,
            bitcoin::Amount::ZERO,
            spend_utxos,
            Vec::new(),
        )?;
        let m6id = bundle.compute_m6id();
        let event_block_hash = bitcoin::BlockHash::from_byte_array([2; 32]);

        let mut rwtxn = env.write_txn()?;
        state.put_utxo(&mut rwtxn, outpoint_key, output.clone(), 0)?;
        state.put_withdrawal_bundle_metadata(&mut rwtxn, &bundle.metadata())?;
        rwtxn.commit()?;

        let mut rwtxn = env.write_txn()?;
        let mut accumulator_diff = AccumulatorDiff::default();
        connect_withdrawal_bundle_submitted(
            &state,
            &mut rwtxn,
            1,
            &event_block_hash,
            m6id,
            &mut accumulator_diff,
        )?;
        assert_eq!(accumulator_diff.counts(), (0, 1));
        assert!(state.utxos.try_get(&rwtxn, &outpoint_key)?.is_none());
        assert_eq!(
            state.stxos.get(&rwtxn, &outpoint_key)?.output.inpoint,
            InPoint::Withdrawal { m6id }
        );
        rwtxn.commit()?;

        let mut rwtxn = env.write_txn()?;
        disconnect_withdrawal_bundle_submitted(&state, &mut rwtxn, 1, m6id)?;
        assert!(state.utxos.try_get(&rwtxn, &outpoint_key)?.is_some());
        assert!(state.withdrawal_bundles.try_get(&rwtxn, &m6id)?.is_none());
        assert!(
            state
                .try_get_withdrawal_bundle_metadata(&rwtxn, m6id)?
                .is_some()
        );
        rwtxn.commit()?;

        let mut rwtxn = env.write_txn()?;
        let mut reapplied_diff = AccumulatorDiff::default();
        connect_withdrawal_bundle_submitted(
            &state,
            &mut rwtxn,
            1,
            &event_block_hash,
            m6id,
            &mut reapplied_diff,
        )?;
        assert_eq!(reapplied_diff.counts(), (0, 1));
        Ok(())
    }

    #[test]
    fn submitted_then_failed_bundle_has_no_net_accumulator_diff()
    -> anyhow::Result<()> {
        let (_temp_dir, env, state) = fresh_state(
            "submitted_then_failed_bundle_has_no_net_accumulator_diff",
        )?;
        let outpoint = OutPoint::Regular {
            txid: Txid::from([1; 32]),
            vout: 0,
        };
        let output = bitcoin_filled_output(Address::ALL_ZEROS, 1_000);
        let mut spend_utxos = BTreeMap::new();
        spend_utxos.insert(outpoint, output.clone());
        let bundle = WithdrawalBundle::new(
            1,
            bitcoin::Amount::ZERO,
            spend_utxos,
            Vec::new(),
        )?;
        let m6id = bundle.compute_m6id();
        let event_block_hash = bitcoin::BlockHash::from_byte_array([2; 32]);

        let mut rwtxn = env.write_txn()?;
        state.put_utxo(&mut rwtxn, OutPointKey::from(&outpoint), output, 0)?;
        state.withdrawal_bundles.put(
            &mut rwtxn,
            &m6id,
            &(
                WithdrawalBundleInfo::Known(bundle),
                RollBack::<HeightStamped<_>>::new(
                    WithdrawalBundleStatus::Pending,
                    0,
                ),
            ),
        )?;
        state
            .pending_withdrawal_bundle
            .put(&mut rwtxn, &(), &m6id)?;

        let mut accumulator_diff = AccumulatorDiff::default();
        connect_withdrawal_bundle_submitted(
            &state,
            &mut rwtxn,
            1,
            &event_block_hash,
            m6id,
            &mut accumulator_diff,
        )?;
        anyhow::ensure!(accumulator_diff.counts() == (0, 1));
        connect_withdrawal_bundle_failed(
            &state,
            &mut rwtxn,
            2,
            m6id,
            &mut accumulator_diff,
        )?;

        anyhow::ensure!(accumulator_diff.is_empty());
        anyhow::ensure!(
            state
                .utxos
                .try_get(&rwtxn, &OutPointKey::from(&outpoint))?
                .is_some()
        );
        Ok(())
    }

    // a failed known bundle reinstates its utxos as spendable, so disconnecting
    // the failure must spend them again
    #[test]
    fn disconnect_failed_bundle_spends_reinstated_utxo() -> anyhow::Result<()> {
        let (_temp_dir, env, state) =
            fresh_state("disconnect_failed_bundle_spends_reinstated_utxo")?;
        let outpoint = OutPoint::Regular {
            txid: Txid::from([1; 32]),
            vout: 0,
        };
        let output = bitcoin_filled_output(Address::ALL_ZEROS, 1000);
        let key = OutPointKey::from(&outpoint);

        let m6id = {
            let mut spend_utxos = BTreeMap::new();
            spend_utxos.insert(outpoint, output.clone());
            let bundle = WithdrawalBundle::new(
                1,
                bitcoin::Amount::ZERO,
                spend_utxos,
                Vec::new(),
            )?;
            let m6id = bundle.compute_m6id();
            let mut bundle_status = RollBack::<HeightStamped<_>>::new(
                WithdrawalBundleStatus::Submitted,
                0,
            );
            bundle_status
                .push(WithdrawalBundleStatus::Failed, 1)
                .unwrap();
            let mut rwtxn = env.write_txn()?;
            state.withdrawal_bundles.put(
                &mut rwtxn,
                &m6id,
                &(WithdrawalBundleInfo::Known(bundle), bundle_status),
            )?;
            state.latest_failed_withdrawal_bundle.put(
                &mut rwtxn,
                &(),
                &RollBack::<HeightStamped<_>>::new(m6id, 1),
            )?;
            // the failure reinstated the utxo
            state.put_utxo(&mut rwtxn, key, output, 1)?;
            rwtxn.commit()?;
            m6id
        };

        let mut rwtxn = env.write_txn()?;
        disconnect_withdrawal_bundle_failed(&state, &mut rwtxn, 1, m6id)?;
        anyhow::ensure!(state.utxos.try_get(&rwtxn, &key)?.is_none());
        let stxo = state.stxos.get(&rwtxn, &key)?;
        anyhow::ensure!(stxo.output.inpoint == InPoint::Withdrawal { m6id });
        Ok(())
    }

    #[test]
    fn duplicate_failed_event_after_unexpected_submission_disconnects_cleanly()
    -> anyhow::Result<()> {
        let (_temp_dir, env, state) = fresh_state(
            "duplicate_failed_event_after_unexpected_submission_disconnects_cleanly",
        )?;
        let m6id = M6id(bitcoin::Txid::from_byte_array([8; 32]));
        let mut status = RollBack::<HeightStamped<_>>::new(
            WithdrawalBundleStatus::SubmittedUnexpected,
            1,
        );
        status
            .push(WithdrawalBundleStatus::Failed, 2)
            .expect("submitted-unexpected to failed is valid");
        let mut rwtxn = env.write_txn()?;
        state.withdrawal_bundles.put(
            &mut rwtxn,
            &m6id,
            &(WithdrawalBundleInfo::Unknown, status),
        )?;

        // The first reverse applies the failure; a duplicate older failure is
        // already undone once SubmittedUnexpected is visible again.
        disconnect_withdrawal_bundle_failed(&state, &mut rwtxn, 2, m6id)?;
        disconnect_withdrawal_bundle_failed(&state, &mut rwtxn, 2, m6id)?;

        let (_, status) = state.withdrawal_bundles.get(&rwtxn, &m6id)?;
        assert_eq!(
            status.latest().value,
            WithdrawalBundleStatus::SubmittedUnexpected
        );
        assert_eq!(status.latest().height, 1);
        Ok(())
    }

    // disconnecting a withdrawal bundle event must remove its
    // withdrawal_bundle_event_blocks record, not a deposit_blocks record that
    // happens to share the same sequence index
    #[test]
    fn disconnect_withdrawal_event_block_uses_correct_db() -> anyhow::Result<()>
    {
        let (_temp_dir, env, state) =
            fresh_state("disconnect_withdrawal_event_block_uses_correct_db")?;

        let block_height = 5u32;
        let m6id = M6id(bitcoin::Txid::from_byte_array([7; 32]));
        let event_block_hash = bitcoin::BlockHash::from_byte_array([9; 32]);
        let deposit_block_hash = bitcoin::BlockHash::from_byte_array([3; 32]);

        let mut rwtxn = env.write_txn()?;
        state.height.put(&mut rwtxn, &(), &block_height)?;
        state.withdrawal_bundles.put(
            &mut rwtxn,
            &m6id,
            &(
                WithdrawalBundleInfo::Unknown,
                RollBack::<HeightStamped<_>>::new(
                    WithdrawalBundleStatus::Submitted,
                    block_height,
                ),
            ),
        )?;
        state.withdrawal_bundle_event_blocks.put(
            &mut rwtxn,
            &0,
            &(event_block_hash, block_height),
        )?;
        // a deposit record at the same sequence index that must survive
        state.deposit_blocks.put(
            &mut rwtxn,
            &0,
            &(deposit_block_hash, block_height),
        )?;
        rwtxn.commit()?;

        let two_way_peg_data = {
            let mut block_info = LinkedHashMap::new();
            block_info.insert(
                event_block_hash,
                BlockInfo {
                    bmm_commitment: None,
                    events: vec![BlockEvent::WithdrawalBundle(
                        WithdrawalBundleEvent {
                            m6id,
                            status: WithdrawalBundleEventStatus::Submitted,
                        },
                    )],
                },
            );
            TwoWayPegData { block_info }
        };

        let mut rwtxn = env.write_txn()?;
        disconnect(&state, &mut rwtxn, &two_way_peg_data)?;
        anyhow::ensure!(
            state
                .withdrawal_bundle_event_blocks
                .try_get(&rwtxn, &0)?
                .is_none()
        );
        anyhow::ensure!(state.deposit_blocks.try_get(&rwtxn, &0)?.is_some());
        rwtxn.commit()?;
        Ok(())
    }

    fn seeded_public_key(idx: u32) -> bitcoin::CompressedPublicKey {
        let secp = Secp256k1::new();
        let mut key_bytes = [0_u8; 32];
        key_bytes[28..].copy_from_slice(&idx.to_be_bytes());
        let secret_key = SecretKey::from_slice(&key_bytes)
            .expect("small non-zero integers are valid secret keys");
        let public_key =
            bitcoin::secp256k1::PublicKey::from_secret_key(&secp, &secret_key);
        bitcoin::CompressedPublicKey(public_key)
    }

    fn regtest_p2wpkh_address(
        idx: u32,
    ) -> bitcoin::Address<bitcoin::address::NetworkUnchecked> {
        let public_key = seeded_public_key(idx);
        bitcoin::Address::p2wpkh(&public_key, Network::Regtest).into_unchecked()
    }

    fn with_state_with_withdrawals<R>(
        test_name: &str,
        count: u32,
        main_address: fn(
            u32,
        ) -> bitcoin::Address<
            bitcoin::address::NetworkUnchecked,
        >,
        f: impl FnOnce(&State, &mut sneed::RwTxn<'_>) -> R,
    ) -> anyhow::Result<R> {
        let (_temp_dir, env, state) = fresh_state(test_name)?;
        let res = {
            let mut rwtxn = env.write_txn()?;
            state.height.put(
                &mut rwtxn,
                &(),
                &crate::state::WITHDRAWAL_BUNDLE_FAILURE_GAP,
            )?;

            for idx in 1..=count {
                let mut txid_bytes = [0_u8; 32];
                txid_bytes[28..].copy_from_slice(&idx.to_be_bytes());
                let outpoint = OutPoint::Regular {
                    txid: txid_bytes.into(),
                    vout: 0,
                };
                let output = FilledOutput {
                    address: {
                        let mut addr = [0u8; 20];
                        let idx = idx.to_be_bytes();
                        addr[..idx.len()].copy_from_slice(&idx);
                        Address::from(addr)
                    },
                    content: FilledOutputContent::BitcoinWithdrawal(
                        WithdrawalOutputContent {
                            value: bitcoin::Amount::from_sat(1_000),
                            main_fee: bitcoin::Amount::ZERO,
                            main_address: main_address(idx),
                        },
                    ),
                    memo: Vec::new(),
                };
                state.put_utxo(
                    &mut rwtxn,
                    OutPointKey::from(&outpoint),
                    output,
                    crate::state::WITHDRAWAL_BUNDLE_FAILURE_GAP,
                )?;
            }
            f(&state, &mut rwtxn)
        };
        Ok(res)
    }

    #[test]
    fn pending_bundle_metadata_outlives_its_l2_branch() -> anyhow::Result<()> {
        with_state_with_withdrawals(
            "pending_bundle_metadata_outlives_its_l2_branch",
            1,
            regtest_p2wpkh_address,
            |state, rwtxn| -> anyhow::Result<()> {
                let two_way_peg_data = TwoWayPegData::default();
                connect(state, rwtxn, &two_way_peg_data)?;
                let m6id = state.pending_withdrawal_bundle.get(rwtxn, &())?;
                assert!(
                    state
                        .try_get_withdrawal_bundle_metadata(rwtxn, m6id)?
                        .is_some()
                );

                disconnect(state, rwtxn, &two_way_peg_data)?;
                assert!(
                    state
                        .pending_withdrawal_bundle
                        .try_get(rwtxn, &())?
                        .is_none()
                );
                assert!(
                    state.withdrawal_bundles.try_get(rwtxn, &m6id)?.is_none()
                );
                assert!(
                    state
                        .try_get_withdrawal_bundle_metadata(rwtxn, m6id)?
                        .is_some()
                );
                Ok(())
            },
        )??;
        Ok(())
    }

    #[test]
    fn disconnect_removes_new_pending_before_restoring_submitted_pending()
    -> anyhow::Result<()> {
        with_state_with_withdrawals(
            "disconnect_removes_new_pending_before_restoring_submitted_pending",
            1,
            regtest_p2wpkh_address,
            |state, rwtxn| -> anyhow::Result<()> {
                connect(state, rwtxn, &TwoWayPegData::default())?;
                let submitted_m6id =
                    state.pending_withdrawal_bundle.get(rwtxn, &())?;

                let block_height =
                    crate::state::WITHDRAWAL_BUNDLE_FAILURE_GAP + 1;
                state.height.put(rwtxn, &(), &block_height)?;
                let next_outpoint = OutPoint::Regular {
                    txid: Txid::from([2; 32]),
                    vout: 0,
                };
                let next_output = FilledOutput {
                    address: Address::ALL_ZEROS,
                    content: FilledOutputContent::BitcoinWithdrawal(
                        WithdrawalOutputContent {
                            value: bitcoin::Amount::from_sat(1_000),
                            main_fee: bitcoin::Amount::ZERO,
                            main_address: regtest_p2wpkh_address(2),
                        },
                    ),
                    memo: Vec::new(),
                };
                state.put_utxo(
                    rwtxn,
                    OutPointKey::from(&next_outpoint),
                    next_output,
                    block_height,
                )?;

                let event_block_hash =
                    bitcoin::BlockHash::from_byte_array([3; 32]);
                let mut two_way_peg_data = TwoWayPegData::default();
                two_way_peg_data.block_info.insert(
                    event_block_hash,
                    BlockInfo {
                        bmm_commitment: None,
                        events: vec![BlockEvent::WithdrawalBundle(
                            WithdrawalBundleEvent {
                                m6id: submitted_m6id,
                                status: WithdrawalBundleEventStatus::Submitted,
                            },
                        )],
                    },
                );
                connect(state, rwtxn, &two_way_peg_data)?;
                let new_pending_m6id =
                    state.pending_withdrawal_bundle.get(rwtxn, &())?;
                anyhow::ensure!(new_pending_m6id != submitted_m6id);

                disconnect(state, rwtxn, &two_way_peg_data)?;

                anyhow::ensure!(
                    state.pending_withdrawal_bundle.get(rwtxn, &())?
                        == submitted_m6id
                );
                let (_bundle, submitted_status) =
                    state.withdrawal_bundles.get(rwtxn, &submitted_m6id)?;
                anyhow::ensure!(
                    submitted_status.latest().value
                        == WithdrawalBundleStatus::Pending
                );
                anyhow::ensure!(
                    submitted_status.latest().height
                        == crate::state::WITHDRAWAL_BUNDLE_FAILURE_GAP
                );
                anyhow::ensure!(
                    state
                        .withdrawal_bundles
                        .try_get(rwtxn, &new_pending_m6id)?
                        .is_none()
                );
                anyhow::ensure!(
                    state
                        .try_get_withdrawal_bundle_metadata(
                            rwtxn,
                            new_pending_m6id,
                        )?
                        .is_some()
                );
                Ok(())
            },
        )??;
        Ok(())
    }

    #[test]
    fn withdrawal_bundle_input_count_is_bounded() -> anyhow::Result<()> {
        let bundle = with_state_with_withdrawals(
            "withdrawal_bundle_input_count_is_bounded",
            3,
            regtest_p2wpkh_address,
            |state, rwtxn| {
                collect_withdrawal_bundle_with_max_inputs(state, rwtxn, 42, 2)
            },
        )??
        .ok_or_else(|| anyhow::anyhow!("expected a withdrawal bundle"))?;

        assert_eq!(bundle.spend_utxos().len(), 2);
        for idx in 1_u32..=2 {
            let mut txid = [0_u8; 32];
            txid[28..].copy_from_slice(&idx.to_be_bytes());
            assert!(bundle.spend_utxos().contains_key(&OutPoint::Regular {
                txid: txid.into(),
                vout: 0,
            }));
        }
        let mut omitted_txid = [0_u8; 32];
        omitted_txid[28..].copy_from_slice(&3_u32.to_be_bytes());
        assert!(!bundle.spend_utxos().contains_key(&OutPoint::Regular {
            txid: omitted_txid.into(),
            vout: 0,
        }));
        assert!(bundle.has_valid_inputs_commitment());
        assert!(bundle.metadata().is_within_size_limit());
        Ok(())
    }

    #[test]
    fn withdrawal_bundle_limit_is_applied_before_aggregation()
    -> anyhow::Result<()> {
        let (_temp_dir, env, state) = fresh_state(
            "withdrawal_bundle_limit_is_applied_before_aggregation",
        )?;
        let main_address = regtest_p2wpkh_address(1);
        let mut rwtxn = env.write_txn()?;
        for (marker, value) in [(1_u8, 1_u64), (2, u64::MAX)] {
            let outpoint = OutPoint::Regular {
                txid: Txid::from([marker; 32]),
                vout: 0,
            };
            let output = FilledOutput {
                address: Address::ALL_ZEROS,
                content: FilledOutputContent::BitcoinWithdrawal(
                    WithdrawalOutputContent {
                        value: bitcoin::Amount::from_sat(value),
                        main_fee: bitcoin::Amount::ZERO,
                        main_address: main_address.clone(),
                    },
                ),
                memo: Vec::new(),
            };
            state.put_utxo(
                &mut rwtxn,
                OutPointKey::from(&outpoint),
                output,
                0,
            )?;
        }

        let bundle = collect_withdrawal_bundle_with_max_inputs(
            &state, &rwtxn, 42, 1,
        )?
        .ok_or_else(|| anyhow::anyhow!("expected a withdrawal bundle"))?;
        assert_eq!(bundle.spend_utxos().len(), 1);
        assert_eq!(bundle.tx().output[2].value.to_sat(), 1);
        Ok(())
    }

    #[test]
    fn collect_withdrawal_bundle_p2wpkh_off_by_one_does_not_exceed_weight()
    -> anyhow::Result<()> {
        const CLAIMED_MAX_BUNDLE_OUTPUTS: u32 = 3_222;

        let bundle = with_state_with_withdrawals(
            "collect_withdrawal_bundle_p2wpkh_off_by_one",
            CLAIMED_MAX_BUNDLE_OUTPUTS + 1,
            regtest_p2wpkh_address,
            |state, rwtxn| collect_withdrawal_bundle(state, rwtxn, 42),
        )?;
        let bundle = match bundle {
            Ok(Some(bundle)) => bundle,
            Ok(None) => anyhow::bail!("expected a withdrawal bundle"),
            Err(err) => anyhow::bail!("unexpected collection error: {err:?}"),
        };
        let output_count = bundle.tx().output.len();
        let weight = bundle.tx().weight().to_wu();

        anyhow::ensure!(
            output_count == (CLAIMED_MAX_BUNDLE_OUTPUTS as usize + 2),
            "expected {} tx outputs including metadata, got {output_count}",
            CLAIMED_MAX_BUNDLE_OUTPUTS as usize + 2,
        );
        anyhow::ensure!(
            weight <= bitcoin::policy::MAX_STANDARD_TX_WEIGHT as u64,
            "unexpected overweight P2WPKH bundle: {weight} wu"
        );
        Ok(())
    }

    fn deposit_two_way_peg_data(
        event_block_hash: bitcoin::BlockHash,
        outpoint: bitcoin::OutPoint,
        output: FilledOutput,
    ) -> TwoWayPegData {
        let mut block_info = LinkedHashMap::new();
        block_info.insert(
            event_block_hash,
            BlockInfo {
                bmm_commitment: None,
                events: vec![BlockEvent::Deposit(Deposit {
                    tx_index: 0,
                    outpoint,
                    output,
                })],
            },
        );
        TwoWayPegData { block_info }
    }

    #[test]
    fn duplicate_deposit_cannot_overwrite_utxo() -> anyhow::Result<()> {
        let (_temp_dir, env, state) =
            fresh_state("duplicate_deposit_cannot_overwrite_utxo")?;
        let main_outpoint = bitcoin::OutPoint {
            txid: bitcoin::Txid::from_byte_array([1; 32]),
            vout: 0,
        };
        let outpoint = OutPoint::Deposit(main_outpoint);
        let key = OutPointKey::from(&outpoint);
        let existing_output = bitcoin_filled_output(Address([1; 20]), 1_000);
        let deposit_data = deposit_two_way_peg_data(
            bitcoin::BlockHash::from_byte_array([2; 32]),
            main_outpoint,
            bitcoin_filled_output(Address([2; 20]), 2_000),
        );

        let mut rwtxn = env.write_txn()?;
        state.height.put(&mut rwtxn, &(), &1)?;
        state.put_utxo(&mut rwtxn, key, existing_output.clone(), 0)?;

        let err = connect(&state, &mut rwtxn, &deposit_data).unwrap_err();
        anyhow::ensure!(matches!(
            err,
            Error::DuplicateDeposit {
                outpoint: duplicate
            } if duplicate == outpoint
        ));
        anyhow::ensure!(state.utxos.get(&rwtxn, &key)? == existing_output);
        anyhow::ensure!(state.stxos.try_get(&rwtxn, &key)?.is_none());
        anyhow::ensure!(state.deposit_blocks.last(&rwtxn)?.is_none());
        Ok(())
    }

    #[test]
    fn duplicate_deposit_cannot_resurrect_stxo() -> anyhow::Result<()> {
        let (_temp_dir, env, state) =
            fresh_state("duplicate_deposit_cannot_resurrect_stxo")?;
        let main_outpoint = bitcoin::OutPoint {
            txid: bitcoin::Txid::from_byte_array([3; 32]),
            vout: 1,
        };
        let outpoint = OutPoint::Deposit(main_outpoint);
        let key = OutPointKey::from(&outpoint);
        let existing_output = bitcoin_filled_output(Address([3; 20]), 3_000);
        let spent_output = SpentOutput {
            output: existing_output.clone(),
            inpoint: InPoint::Regular {
                txid: Txid::from([4; 32]),
                vin: 0,
            },
        };
        let deposit_data = deposit_two_way_peg_data(
            bitcoin::BlockHash::from_byte_array([5; 32]),
            main_outpoint,
            bitcoin_filled_output(Address([5; 20]), 5_000),
        );

        let mut rwtxn = env.write_txn()?;
        state.height.put(&mut rwtxn, &(), &1)?;
        state.put_utxo(&mut rwtxn, key, existing_output, 0)?;
        anyhow::ensure!(state.spend_utxo(&mut rwtxn, key, spent_output)?);
        let existing_stxo = state.stxos.get(&rwtxn, &key)?;

        let err = connect(&state, &mut rwtxn, &deposit_data).unwrap_err();
        anyhow::ensure!(matches!(
            err,
            Error::DuplicateDeposit {
                outpoint: duplicate
            } if duplicate == outpoint
        ));
        anyhow::ensure!(state.utxos.try_get(&rwtxn, &key)?.is_none());
        anyhow::ensure!(state.stxos.get(&rwtxn, &key)? == existing_stxo);
        anyhow::ensure!(state.deposit_blocks.last(&rwtxn)?.is_none());
        Ok(())
    }

    // connecting a deposit then disconnecting it on a reorg must round-trip
    #[test]
    fn deposit_reorg_round_trips() -> anyhow::Result<()> {
        use crate::types::{
            Body, Header, Transaction, proto::mainchain::Deposit,
        };

        let (_temp_dir, env, state) = fresh_state("deposit_reorg_round_trips")?;
        let empty_body = Body {
            coinbase: Vec::new(),
            transactions: Vec::new(),
            authorizations: Vec::new(),
        };
        let no_txs: &[Transaction] = &[];
        let merkle_root = Body::compute_merkle_root(&[], no_txs);
        let main0 = bitcoin::BlockHash::from_byte_array([10; 32]);
        let main1 = bitcoin::BlockHash::from_byte_array([11; 32]);

        let genesis = Header {
            merkle_root,
            prev_side_hash: None,
            prev_main_hash: main0,
        };
        {
            let mut rwtxn = env.write_txn()?;
            state.apply_block(&mut rwtxn, &genesis, &empty_body)?;
            state.connect_two_way_peg_data(
                &mut rwtxn,
                &TwoWayPegData::default(),
            )?;
            rwtxn.commit()?;
        }

        let block1 = Header {
            merkle_root,
            prev_side_hash: Some(genesis.hash()),
            prev_main_hash: main1,
        };
        let deposit_outpoint = bitcoin::OutPoint {
            txid: bitcoin::Txid::from_byte_array([2; 32]),
            vout: 0,
        };
        let deposit_key =
            OutPointKey::from(&OutPoint::Deposit(deposit_outpoint));
        let deposit_twpd = {
            let mut block_info = LinkedHashMap::new();
            block_info.insert(
                main1,
                BlockInfo {
                    bmm_commitment: None,
                    events: vec![BlockEvent::Deposit(Deposit {
                        tx_index: 0,
                        outpoint: deposit_outpoint,
                        output: bitcoin_filled_output(Address::ALL_ZEROS, 1000),
                    })],
                },
            );
            TwoWayPegData { block_info }
        };
        {
            let mut rwtxn = env.write_txn()?;
            state.apply_block(&mut rwtxn, &block1, &empty_body)?;
            state.connect_two_way_peg_data(&mut rwtxn, &deposit_twpd)?;
            anyhow::ensure!(
                state.utxos.try_get(&rwtxn, &deposit_key)?.is_some()
            );
            anyhow::ensure!(state.deposit_blocks.last(&rwtxn)?.is_some());
            rwtxn.commit()?;
        }

        {
            let mut rwtxn = env.write_txn()?;
            state.disconnect_two_way_peg_data(&mut rwtxn, &deposit_twpd)?;
            anyhow::ensure!(
                state.utxos.try_get(&rwtxn, &deposit_key)?.is_none()
            );
            anyhow::ensure!(state.deposit_blocks.last(&rwtxn)?.is_none());
            rwtxn.commit()?;
        }

        Ok(())
    }

    // A single two-way-peg batch can span multiple mainchain blocks. Connecting
    // deposits from two distinct blocks then disconnecting the batch must
    // restore the prior state. Before the fix, disconnect recomputed the latest
    // deposit block hash by reverse iteration (yielding the oldest block) and
    // panicked on the consistency assert against the newest hash connect stored.
    #[test]
    fn disconnect_two_deposit_blocks_restores_state() -> anyhow::Result<()> {
        fn deposit_block(salt: u8) -> (bitcoin::BlockHash, BlockInfo) {
            let dep = Deposit {
                tx_index: 0,
                outpoint: bitcoin::OutPoint {
                    txid: bitcoin::Txid::from_byte_array([salt; 32]),
                    vout: 0,
                },
                output: FilledOutput {
                    address: Address([salt; 20]),
                    content: FilledOutputContent::new_bitcoin_value(
                        bitcoin::Amount::from_sat(1000),
                    ),
                    memo: Vec::new(),
                },
            };
            (
                bitcoin::BlockHash::from_byte_array([salt; 32]),
                BlockInfo {
                    bmm_commitment: None,
                    events: vec![BlockEvent::Deposit(dep)],
                },
            )
        }
        let (_temp_dir, env, state) =
            fresh_state("disconnect_two_deposit_blocks_restores_state")?;
        let mut rwtxn = env.write_txn()?;
        state.height.put(&mut rwtxn, &(), &10)?;

        let mut block_info = LinkedHashMap::new();
        let (h1, b1) = deposit_block(1);
        let (h2, b2) = deposit_block(2);
        block_info.insert(h1, b1);
        block_info.insert(h2, b2);
        let tdp = TwoWayPegData { block_info };

        let accumulator_diff = connect(&state, &mut rwtxn, &tdp)?;
        anyhow::ensure!(accumulator_diff.counts() == (2, 0));
        anyhow::ensure!(state.utxos.len(&rwtxn)? == 2);
        disconnect(&state, &mut rwtxn, &tdp)?;

        anyhow::ensure!(state.utxos.len(&rwtxn)? == 0);
        anyhow::ensure!(state.deposit_blocks.len(&rwtxn)? == 0);
        Ok(())
    }
}
