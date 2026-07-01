//! Connect and disconnect blocks

use rayon::prelude::*;
use sneed::{RoTxn, RwTxn};

use crate::{
    state::{Error, PrevalidatedBlock, State, amm, dutch_auction, error},
    types::{
        AmountOverflowError, Authorization, BitAssetId, Body, FilledOutput,
        FilledOutputContent, GetAddress as _, GetBitcoinValue as _, Header,
        InPoint, OutPoint, OutPointKey, OutputContent, SpentOutput, TxData,
        Verify as _,
    },
};

/// Calculate total number of inputs across all transactions in a block body
fn calculate_total_inputs(body: &Body) -> usize {
    body.transactions.iter().map(|t| t.inputs.len()).sum()
}

/// Validate a block, returning fees
pub fn validate(
    state: &State,
    rotxn: &RoTxn,
    header: &Header,
    body: &Body,
) -> Result<bitcoin::Amount, Error> {
    let body_size =
        borsh::object_length(&body).map_err(Error::BorshSerialize)?;
    if body_size > Body::MAX_SIZE {
        return Err(Error::BodyTooLarge);
    }

    let tip_hash = state.try_get_tip(rotxn)?;
    if header.prev_side_hash != tip_hash {
        let err = error::InvalidHeader::PrevSideHash {
            expected: tip_hash,
            received: header.prev_side_hash,
        };
        return Err(Error::InvalidHeader(err));
    };
    let merkle_root =
        Body::compute_merkle_root(&body.coinbase, &body.transactions);
    if merkle_root != header.merkle_root {
        let err = Error::InvalidBody {
            expected: header.merkle_root,
            computed: merkle_root,
        };
        return Err(err);
    }
    let mut coinbase_value = bitcoin::Amount::ZERO;
    for output in &body.coinbase {
        coinbase_value = coinbase_value
            .checked_add(output.get_bitcoin_value())
            .ok_or(AmountOverflowError)?;
    }
    let mut total_fees = bitcoin::Amount::ZERO;
    let filled_txs: Vec<_> = body
        .transactions
        .iter()
        .map(|t| state.fill_transaction(rotxn, t))
        .collect::<Result<_, _>>()?;

    let total_inputs = calculate_total_inputs(body);

    // Collect all inputs as fixed-width keys for efficient double-spend detection via sort-and-scan
    let mut all_input_keys = Vec::with_capacity(total_inputs);
    for filled_transaction in &filled_txs {
        for input in &filled_transaction.transaction.inputs {
            all_input_keys.push(OutPointKey::from_outpoint(input));
        }
    }

    // Sort and check for duplicate outpoints (double-spend detection)
    all_input_keys.par_sort_unstable();
    if all_input_keys.windows(2).any(|w| w[0] == w[1]) {
        return Err(Error::UtxoDoubleSpent);
    }

    for filled_tx in &filled_txs {
        total_fees = total_fees
            .checked_add(state.validate_filled_transaction(rotxn, filled_tx)?)
            .ok_or(AmountOverflowError)?;
    }
    if coinbase_value > total_fees {
        return Err(Error::NotEnoughFees);
    }
    let spent_utxos = filled_txs.iter().flat_map(|t| t.spent_utxos.iter());
    for (authorization, spent_utxo) in
        body.authorizations.iter().zip(spent_utxos)
    {
        if authorization.get_address() != spent_utxo.address {
            return Err(Error::WrongPubKeyForAddress);
        }
    }
    let () = Authorization::verify_body(body).map_err(Error::Authorization)?;
    Ok(total_fees)
}

pub fn prevalidate(
    state: &State,
    rotxn: &RoTxn,
    header: &Header,
    body: &Body,
) -> Result<PrevalidatedBlock, Error> {
    let body_size =
        borsh::object_length(&body).map_err(Error::BorshSerialize)?;
    if body_size > Body::MAX_SIZE {
        return Err(Error::BodyTooLarge);
    }

    let tip_hash = state.try_get_tip(rotxn)?;
    if header.prev_side_hash != tip_hash {
        let err = error::InvalidHeader::PrevSideHash {
            expected: tip_hash,
            received: header.prev_side_hash,
        };
        return Err(Error::InvalidHeader(err));
    };

    let computed_merkle_root =
        Body::compute_merkle_root(&body.coinbase, &body.transactions);
    if computed_merkle_root != header.merkle_root {
        let err = Error::InvalidBody {
            expected: header.merkle_root,
            computed: computed_merkle_root,
        };
        return Err(err);
    }

    let mut coinbase_value = bitcoin::Amount::ZERO;
    for output in &body.coinbase {
        coinbase_value = coinbase_value
            .checked_add(output.get_bitcoin_value())
            .ok_or(AmountOverflowError)?;
    }

    let mut total_fees = bitcoin::Amount::ZERO;
    let filled_transactions: Vec<_> = body
        .transactions
        .iter()
        .map(|t| state.fill_transaction(rotxn, t))
        .collect::<Result<_, _>>()?;

    let total_inputs = calculate_total_inputs(body);

    // Collect all inputs as fixed-width keys for efficient double-spend detection via sort-and-scan
    let mut all_input_keys = Vec::with_capacity(total_inputs);
    for filled_transaction in &filled_transactions {
        for input in &filled_transaction.transaction.inputs {
            all_input_keys.push(OutPointKey::from_outpoint(input));
        }
    }

    // Sort and check for duplicate outpoints (double-spend detection)
    all_input_keys.par_sort_unstable();
    if all_input_keys.windows(2).any(|w| w[0] == w[1]) {
        return Err(Error::UtxoDoubleSpent);
    }

    for filled_tx in &filled_transactions {
        total_fees = total_fees
            .checked_add(state.validate_filled_transaction(rotxn, filled_tx)?)
            .ok_or(AmountOverflowError)?;
    }

    if coinbase_value > total_fees {
        return Err(Error::NotEnoughFees);
    }

    let spent_utxos_iter = filled_transactions
        .iter()
        .flat_map(|t| t.spent_utxos.iter());
    for (authorization, spent_utxo) in
        body.authorizations.iter().zip(spent_utxos_iter)
    {
        if authorization.get_address() != spent_utxo.address {
            return Err(Error::WrongPubKeyForAddress);
        }
    }

    let () = Authorization::verify_body(body).map_err(Error::Authorization)?;

    let height = state.try_get_height(rotxn)?.map_or(0, |height| height + 1);

    Ok(PrevalidatedBlock {
        filled_transactions,
        computed_merkle_root,
        total_fees,
        coinbase_value,
        next_height: height,
    })
}

pub fn connect_prevalidated(
    state: &State,
    rwtxn: &mut RwTxn,
    header: &Header,
    body: &Body,
    prevalidated: PrevalidatedBlock,
) -> Result<(), Error> {
    // Skip validation - already done in prevalidate
    // Use precomputed values to avoid redundant DB reads

    // Calculate precise capacities for optimal Vec performance
    let total_inputs: usize = prevalidated
        .filled_transactions
        .iter()
        .map(|tx| tx.transaction.inputs.len())
        .sum();
    let total_outputs: usize = prevalidated
        .filled_transactions
        .iter()
        .map(|tx| tx.transaction.outputs.len())
        .sum::<usize>()
        + body.coinbase.len();

    // Use Vec + sort_unstable instead of individual DB operations for better performance
    let mut utxo_deletes: Vec<OutPointKey> = Vec::with_capacity(total_inputs);
    let mut stxo_puts: Vec<(OutPointKey, SpentOutput)> =
        Vec::with_capacity(total_inputs);
    let mut utxo_puts: Vec<(OutPointKey, FilledOutput)> =
        Vec::with_capacity(total_outputs);

    // Collect coinbase outputs
    for (vout, output) in body.coinbase.iter().enumerate() {
        let outpoint = OutPoint::Coinbase {
            merkle_root: header.merkle_root,
            vout: vout as u32,
        };
        let filled_content = match output.content.clone() {
            OutputContent::Bitcoin(value) => {
                FilledOutputContent::Bitcoin(value)
            }
            OutputContent::Withdrawal(withdrawal) => {
                FilledOutputContent::BitcoinWithdrawal(withdrawal)
            }
            OutputContent::AmmLpToken(_)
            | OutputContent::BitAsset(_)
            | OutputContent::BitAssetControl
            | OutputContent::BitAssetReservation
            | OutputContent::DutchAuctionReceipt => {
                return Err(Error::BadCoinbaseOutputContent);
            }
        };
        let filled_output = FilledOutput {
            address: output.address,
            content: filled_content,
            memo: output.memo.clone(),
        };
        utxo_puts.push((OutPointKey::from_outpoint(&outpoint), filled_output));
    }

    // Collect transaction changes and apply transaction data effects
    for (transaction, filled_tx) in body
        .transactions
        .iter()
        .zip(&prevalidated.filled_transactions)
    {
        let txid = filled_tx.txid();

        // Process inputs
        for (vin, input) in filled_tx.inputs().iter().enumerate() {
            let key = OutPointKey::from_outpoint(input);
            // Reuse prevalidated spent UTXO instead of reading from DB
            let prev_utxo = filled_tx.spent_utxos[vin].clone();
            let spent_output = SpentOutput {
                output: prev_utxo,
                inpoint: InPoint::Regular {
                    txid,
                    vin: vin as u32,
                },
            };
            utxo_deletes.push(key);
            stxo_puts.push((key, spent_output));
        }

        // Process outputs
        let Some(filled_outputs) = filled_tx.filled_outputs() else {
            let err = error::FillTxOutputContents(Box::new(filled_tx.clone()));
            return Err(err.into());
        };
        for (vout, filled_output) in filled_outputs.iter().enumerate() {
            let outpoint = OutPoint::Regular {
                txid,
                vout: vout as u32,
            };
            let key = OutPointKey::from_outpoint(&outpoint);
            utxo_puts.push((key, filled_output.clone()));
        }

        // Apply transaction data effects
        match &transaction.data {
            None => (),
            Some(TxData::AmmBurn { .. }) => {
                let () = amm::apply_burn(&state.amm_pools, rwtxn, filled_tx)?;
            }
            Some(TxData::AmmMint { .. }) => {
                let () = amm::apply_mint(&state.amm_pools, rwtxn, filled_tx)?;
            }
            Some(TxData::AmmSwap { .. }) => {
                let () = amm::apply_swap(&state.amm_pools, rwtxn, filled_tx)?;
            }
            Some(TxData::BitAssetReservation { commitment }) => {
                let () = state
                    .bitassets
                    .put_reservation(rwtxn, &txid, commitment)?;
            }
            Some(TxData::BitAssetRegistration {
                name_hash,
                revealed_nonce: _,
                bitasset_data,
                initial_supply,
            }) => {
                let () = state.bitassets.apply_registration(
                    rwtxn,
                    filled_tx,
                    *name_hash,
                    bitasset_data,
                    *initial_supply,
                    prevalidated.next_height,
                )?;
            }
            Some(TxData::BitAssetMint(mint_amount)) => {
                let () = state.bitassets.apply_mint(
                    rwtxn,
                    filled_tx,
                    *mint_amount,
                    prevalidated.next_height,
                )?;
            }
            Some(TxData::BitAssetUpdate(bitasset_updates)) => {
                let () = state.bitassets.apply_updates(
                    rwtxn,
                    filled_tx,
                    (**bitasset_updates).clone(),
                    prevalidated.next_height,
                )?;
            }
            Some(TxData::DutchAuctionBid { .. }) => {
                let () = dutch_auction::apply_bid(
                    &state.dutch_auctions,
                    rwtxn,
                    filled_tx,
                    prevalidated.next_height,
                )?;
            }
            Some(TxData::DutchAuctionCreate(dutch_auction_params)) => {
                let () = dutch_auction::apply_create(
                    &state.dutch_auctions,
                    rwtxn,
                    filled_tx,
                    *dutch_auction_params,
                    prevalidated.next_height,
                )?;
            }
            Some(TxData::DutchAuctionCollect { .. }) => {
                let () = dutch_auction::apply_collect(
                    &state.dutch_auctions,
                    rwtxn,
                    filled_tx,
                    prevalidated.next_height,
                )?;
            }
        }
    }

    // Sort all vectors in parallel for optimal cursor access
    utxo_deletes.par_sort_unstable();
    stxo_puts.par_sort_unstable_by_key(|(key, _)| *key);
    utxo_puts.par_sort_unstable_by_key(|(key, _)| *key);

    // Apply all database operations using pre-sorted keys for optimal B-tree access
    for key in &utxo_deletes {
        state.utxos.delete(rwtxn, key)?;
    }

    for (key, spent_output) in &stxo_puts {
        state.stxos.put(rwtxn, key, spent_output)?;
    }

    for (key, filled_output) in &utxo_puts {
        state.utxos.put(rwtxn, key, filled_output)?;
    }

    // Update tip and height using precomputed values
    let block_hash = header.hash();
    state.tip.put(rwtxn, &(), &block_hash)?;
    state.height.put(rwtxn, &(), &prevalidated.next_height)?;

    Ok(())
}

pub fn connect(
    state: &State,
    rwtxn: &mut RwTxn,
    header: &Header,
    body: &Body,
) -> Result<(), Error> {
    let height = state.try_get_height(rwtxn)?.map_or(0, |height| height + 1);
    let tip_hash = state.try_get_tip(rwtxn)?;
    if tip_hash != header.prev_side_hash {
        let err = error::InvalidHeader::PrevSideHash {
            expected: tip_hash,
            received: header.prev_side_hash,
        };
        return Err(Error::InvalidHeader(err));
    }
    let merkle_root =
        Body::compute_merkle_root(&body.coinbase, &body.transactions);
    if merkle_root != header.merkle_root {
        let err = Error::InvalidBody {
            expected: merkle_root,
            computed: header.merkle_root,
        };
        return Err(err);
    }
    for (vout, output) in body.coinbase.iter().enumerate() {
        let outpoint = OutPoint::Coinbase {
            merkle_root: header.merkle_root,
            vout: vout as u32,
        };
        let outpoint_key = OutPointKey::from_outpoint(&outpoint);
        let filled_content = match output.content.clone() {
            OutputContent::Bitcoin(value) => {
                FilledOutputContent::Bitcoin(value)
            }
            OutputContent::Withdrawal(withdrawal) => {
                FilledOutputContent::BitcoinWithdrawal(withdrawal)
            }
            OutputContent::AmmLpToken(_)
            | OutputContent::BitAsset(_)
            | OutputContent::BitAssetControl
            | OutputContent::BitAssetReservation
            | OutputContent::DutchAuctionReceipt => {
                return Err(Error::BadCoinbaseOutputContent);
            }
        };
        let filled_output = FilledOutput {
            address: output.address,
            content: filled_content,
            memo: output.memo.clone(),
        };
        state.utxos.put(rwtxn, &outpoint_key, &filled_output)?;
    }
    for transaction in &body.transactions {
        let filled_tx = state.fill_transaction(rwtxn, transaction)?;
        let txid = filled_tx.txid();
        for (vin, input) in filled_tx.inputs().iter().enumerate() {
            let input_key = OutPointKey::from_outpoint(input);
            let spent_output = state
                .utxos
                .try_get(rwtxn, &input_key)?
                .ok_or(error::NoUtxo { outpoint: *input })?;
            let spent_output = SpentOutput {
                output: spent_output,
                inpoint: InPoint::Regular {
                    txid,
                    vin: vin as u32,
                },
            };
            state.utxos.delete(rwtxn, &input_key)?;
            state.stxos.put(rwtxn, &input_key, &spent_output)?;
        }
        let Some(filled_outputs) = filled_tx.filled_outputs() else {
            let err = error::FillTxOutputContents(Box::new(filled_tx));
            return Err(err.into());
        };
        for (vout, filled_output) in filled_outputs.iter().enumerate() {
            let outpoint = OutPoint::Regular {
                txid,
                vout: vout as u32,
            };
            let outpoint_key = OutPointKey::from_outpoint(&outpoint);
            state.utxos.put(rwtxn, &outpoint_key, filled_output)?;
        }
        match &transaction.data {
            None => (),
            Some(TxData::AmmBurn { .. }) => {
                let () = amm::apply_burn(&state.amm_pools, rwtxn, &filled_tx)?;
            }
            Some(TxData::AmmMint { .. }) => {
                let () = amm::apply_mint(&state.amm_pools, rwtxn, &filled_tx)?;
            }
            Some(TxData::AmmSwap { .. }) => {
                let () = amm::apply_swap(&state.amm_pools, rwtxn, &filled_tx)?;
            }
            Some(TxData::BitAssetReservation { commitment }) => {
                let () = state
                    .bitassets
                    .put_reservation(rwtxn, &txid, commitment)?;
            }
            Some(TxData::BitAssetRegistration {
                name_hash,
                revealed_nonce: _,
                bitasset_data,
                initial_supply,
            }) => {
                let () = state.bitassets.apply_registration(
                    rwtxn,
                    &filled_tx,
                    *name_hash,
                    bitasset_data,
                    *initial_supply,
                    height,
                )?;
            }
            Some(TxData::BitAssetMint(mint_amount)) => {
                let () = state.bitassets.apply_mint(
                    rwtxn,
                    &filled_tx,
                    *mint_amount,
                    height,
                )?;
            }
            Some(TxData::BitAssetUpdate(bitasset_updates)) => {
                let () = state.bitassets.apply_updates(
                    rwtxn,
                    &filled_tx,
                    (**bitasset_updates).clone(),
                    height,
                )?;
            }
            Some(TxData::DutchAuctionBid { .. }) => {
                let () = dutch_auction::apply_bid(
                    &state.dutch_auctions,
                    rwtxn,
                    &filled_tx,
                    height,
                )?;
            }
            Some(TxData::DutchAuctionCreate(dutch_auction_params)) => {
                let () = dutch_auction::apply_create(
                    &state.dutch_auctions,
                    rwtxn,
                    &filled_tx,
                    *dutch_auction_params,
                    height,
                )?;
            }
            Some(TxData::DutchAuctionCollect { .. }) => {
                let () = dutch_auction::apply_collect(
                    &state.dutch_auctions,
                    rwtxn,
                    &filled_tx,
                    height,
                )?;
            }
        }
    }
    let block_hash = header.hash();
    state.tip.put(rwtxn, &(), &block_hash)?;
    state.height.put(rwtxn, &(), &height)?;
    Ok(())
}

pub fn disconnect_tip(
    state: &State,
    rwtxn: &mut RwTxn,
    header: &Header,
    body: &Body,
) -> Result<(), Error> {
    let tip_hash = state.tip.try_get(rwtxn, &())?.ok_or(Error::NoTip)?;
    if tip_hash != header.hash() {
        let err = error::InvalidHeader::BlockHash {
            expected: tip_hash,
            computed: header.hash(),
        };
        return Err(Error::InvalidHeader(err));
    }
    let merkle_root =
        Body::compute_merkle_root(&body.coinbase, &body.transactions);
    if merkle_root != header.merkle_root {
        let err = Error::InvalidBody {
            expected: header.merkle_root,
            computed: merkle_root,
        };
        return Err(err);
    }
    let height = state
        .try_get_height(rwtxn)?
        .expect("Height should not be None");
    // revert txs, last-to-first
    body.transactions.iter().rev().try_for_each(|tx| {
        let txid = tx.txid();
        let filled_tx = state.fill_transaction_from_stxos(rwtxn, tx.clone())?;
        // revert transaction effects
        match &tx.data {
            None => (),
            Some(TxData::AmmBurn { .. }) => {
                let () = amm::revert_burn(&state.amm_pools, rwtxn, &filled_tx)?;
            }
            Some(TxData::AmmMint { .. }) => {
                let () = amm::revert_mint(&state.amm_pools, rwtxn, &filled_tx)?;
            }
            Some(TxData::AmmSwap { .. }) => {
                let () = amm::revert_swap(&state.amm_pools, rwtxn, &filled_tx)?;
            }
            Some(TxData::BitAssetMint(mint_amount)) => {
                let () = state.bitassets.revert_mint(
                    rwtxn,
                    &filled_tx,
                    *mint_amount,
                )?;
            }
            Some(TxData::BitAssetRegistration {
                name_hash,
                revealed_nonce: _,
                bitasset_data: _,
                initial_supply: _,
            }) => {
                let () = state.bitassets.revert_registration(
                    rwtxn,
                    &filled_tx,
                    BitAssetId(*name_hash),
                )?;
            }
            #[allow(clippy::collapsible_match)]
            Some(TxData::BitAssetReservation { commitment: _ }) => {
                if !state.bitassets.delete_reservation(rwtxn, &txid)? {
                    let err = error::BitAsset::MissingReservation { txid };
                    return Err(err.into());
                }
            }
            Some(TxData::BitAssetUpdate(bitasset_updates)) => {
                let () = state.bitassets.revert_updates(
                    rwtxn,
                    &filled_tx,
                    (**bitasset_updates).clone(),
                    height,
                )?;
            }
            Some(TxData::DutchAuctionBid { .. }) => {
                let () = dutch_auction::revert_bid(
                    &state.dutch_auctions,
                    rwtxn,
                    &filled_tx,
                )?;
            }
            Some(TxData::DutchAuctionCollect { .. }) => {
                let () = dutch_auction::revert_collect(
                    &state.dutch_auctions,
                    rwtxn,
                    &filled_tx,
                )?;
            }
            Some(TxData::DutchAuctionCreate(_auction_params)) => {
                let () = dutch_auction::revert_create(
                    &state.dutch_auctions,
                    rwtxn,
                    &filled_tx,
                )?;
            }
        }
        // delete UTXOs, last-to-first
        tx.outputs.iter().enumerate().rev().try_for_each(
            |(vout, _output)| {
                let outpoint = OutPoint::Regular {
                    txid,
                    vout: vout as u32,
                };
                let outpoint_key = OutPointKey::from_outpoint(&outpoint);
                if state.utxos.delete(rwtxn, &outpoint_key)? {
                    Ok::<_, Error>(())
                } else {
                    Err(error::NoUtxo { outpoint }.into())
                }
            },
        )?;
        // unspend STXOs, last-to-first
        tx.inputs.iter().rev().try_for_each(|outpoint| {
            let outpoint_key = OutPointKey::from_outpoint(outpoint);
            if let Some(spent_output) =
                state.stxos.try_get(rwtxn, &outpoint_key)?
            {
                state.stxos.delete(rwtxn, &outpoint_key)?;
                state
                    .utxos
                    .put(rwtxn, &outpoint_key, &spent_output.output)?;
                Ok(())
            } else {
                Err(Error::NoStxo {
                    outpoint: *outpoint,
                })
            }
        })
    })?;
    // delete coinbase UTXOs, last-to-first
    body.coinbase.iter().enumerate().rev().try_for_each(
        |(vout, _output)| {
            let outpoint = OutPoint::Coinbase {
                merkle_root: header.merkle_root,
                vout: vout as u32,
            };
            let outpoint_key = OutPointKey::from_outpoint(&outpoint);
            if state.utxos.delete(rwtxn, &outpoint_key)? {
                Ok::<_, Error>(())
            } else {
                Err(error::NoUtxo { outpoint }.into())
            }
        },
    )?;
    match (header.prev_side_hash, height) {
        (None, 0) => {
            state.tip.delete(rwtxn, &())?;
            state.height.delete(rwtxn, &())?;
        }
        (None, _) | (_, 0) => return Err(Error::NoTip),
        (Some(prev_side_hash), height) => {
            state.tip.put(rwtxn, &(), &prev_side_hash)?;
            state.height.put(rwtxn, &(), &(height - 1))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use bitcoin::hashes::Hash as _;

    use crate::{
        authorization::{self, SigningKey},
        state::{
            BitAssetSeqId,
            block::{connect, disconnect_tip, validate},
            test::fresh_state,
        },
        types::{
            BitAssetData, BitAssetDataUpdates, BitAssetId, BlockHash, Body,
            FilledOutput, FilledOutputContent, Hash, Header, OutPoint,
            OutPointKey, Output, OutputContent, Transaction, TxData, Update,
        },
    };

    fn header(prev_side_hash: Option<BlockHash>, body: &Body) -> Header {
        Header {
            merkle_root: Body::compute_merkle_root(
                &body.coinbase,
                &body.transactions,
            ),
            prev_side_hash,
            prev_main_hash: bitcoin::BlockHash::from_byte_array([0; 32]),
        }
    }

    fn all_retained_updates() -> BitAssetDataUpdates {
        BitAssetDataUpdates {
            commitment: Update::Retain,
            socket_addr_v4: Update::Retain,
            socket_addr_v6: Update::Retain,
            encryption_pubkey: Update::Retain,
            signing_pubkey: Update::Retain,
        }
    }

    #[test]
    fn disconnect_bitasset_data_update() -> anyhow::Result<()> {
        let (env, state) = fresh_state("disconnect_bitasset_data_update")?;
        let signing_key = SigningKey::from_bytes(&[7; 32]);
        let verifying_key = signing_key.verifying_key().into();
        let address = authorization::get_address(&verifying_key);

        let name_hash: Hash = [1; 32];
        let revealed_nonce: Hash = [2; 32];
        let commitment: Hash =
            blake3::keyed_hash(&revealed_nonce, &name_hash).into();
        let reservation_txid = [3; 32].into();
        let bitasset_id = BitAssetId(name_hash);
        let bitasset_outpoint = OutPoint::Regular {
            txid: [5; 32].into(),
            vout: 0,
        };
        let control_outpoint = OutPoint::Regular {
            txid: [4; 32].into(),
            vout: 0,
        };

        {
            let mut rwtxn = env.write_txn()?;
            state.bitassets.put_reservation(
                &mut rwtxn,
                &reservation_txid,
                &commitment,
            )?;
            let registration_tx = Transaction {
                inputs: vec![OutPoint::Regular {
                    txid: reservation_txid,
                    vout: 0,
                }],
                outputs: Vec::new(),
                memo: Vec::new(),
                data: Some(TxData::BitAssetRegistration {
                    name_hash,
                    revealed_nonce,
                    bitasset_data: Box::new(BitAssetData::default()),
                    initial_supply: 5,
                }),
            };
            let registration_filled = crate::types::FilledTransaction {
                transaction: registration_tx,
                spent_utxos: vec![FilledOutput {
                    address,
                    content: FilledOutputContent::BitAssetReservation(
                        reservation_txid,
                        commitment,
                    ),
                    memo: Vec::new(),
                }],
            };
            state.bitassets.apply_registration(
                &mut rwtxn,
                &registration_filled,
                name_hash,
                &BitAssetData::default(),
                5u64,
                0u32,
            )?;
            let seq = state.bitassets.next_seq(&rwtxn)?;
            anyhow::ensure!(seq == BitAssetSeqId(1));
            state.utxos.put(
                &mut rwtxn,
                &OutPointKey::from_outpoint(&bitasset_outpoint),
                &FilledOutput {
                    address,
                    content: FilledOutputContent::BitAsset(bitasset_id, 5),
                    memo: Vec::new(),
                },
            )?;
            state.utxos.put(
                &mut rwtxn,
                &OutPointKey::from_outpoint(&control_outpoint),
                &FilledOutput {
                    address,
                    content: FilledOutputContent::BitAssetControl(bitasset_id),
                    memo: Vec::new(),
                },
            )?;
            rwtxn.commit()?;
        }

        let genesis_body = Body {
            coinbase: Vec::new(),
            transactions: Vec::new(),
            authorizations: Vec::new(),
        };
        let genesis_header = header(None, &genesis_body);
        {
            let mut rwtxn = env.write_txn()?;
            connect(&state, &mut rwtxn, &genesis_header, &genesis_body)?;
            rwtxn.commit()?;
        }

        let mut updates = all_retained_updates();
        updates.commitment = Update::Set([9; 32]);
        let update_tx = Transaction {
            inputs: vec![bitasset_outpoint, control_outpoint],
            outputs: vec![
                Output::new(address, OutputContent::BitAsset(5)),
                Output::new(address, OutputContent::BitAssetControl),
            ],
            memo: Vec::new(),
            data: Some(TxData::BitAssetUpdate(Box::new(updates))),
        };
        let authorized_update = authorization::authorize(
            &[(address, &signing_key), (address, &signing_key)],
            update_tx,
        )?;
        let update_body = Body::new(vec![authorized_update], Vec::new());
        let update_header = header(Some(genesis_header.hash()), &update_body);

        {
            let rotxn = env.read_txn()?;
            validate(&state, &rotxn, &update_header, &update_body)?;
        }
        {
            let mut rwtxn = env.write_txn()?;
            connect(&state, &mut rwtxn, &update_header, &update_body)?;
            rwtxn.commit()?;
        }

        let mut rwtxn = env.write_txn()?;
        let () =
            disconnect_tip(&state, &mut rwtxn, &update_header, &update_body)?;
        Ok(())
    }
}
