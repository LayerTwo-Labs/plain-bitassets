use std::collections::{BTreeMap, HashMap, HashSet};

use fallible_iterator::FallibleIterator as _;
use futures::Stream;
use heed::{types::SerdeBincode, Database, RoTxn, RwTxn};
use itertools::Itertools;

use crate::{
    authorization::Authorization,
    types::{
        proto::mainchain::{BlockEvent, TwoWayPegData},
        Address, AggregatedWithdrawal, AmountOverflowError, Authorized,
        AuthorizedTransaction, BitAssetId, BlockHash, Body, FilledOutput,
        FilledOutputContent, FilledTransaction, GetAddress as _,
        GetBitcoinValue as _, Header, InPoint, M6id, OutPoint, OutputContent,
        SpentOutput, Transaction, TxData, Verify as _, WithdrawalBundle,
        WithdrawalBundleEvent, WithdrawalBundleStatus,
    },
    util::{EnvExt, UnitKey, Watchable, WatchableDb},
};

mod amm;
pub mod bitassets;
mod dutch_auction;
pub mod error;
mod rollback;

pub use amm::{AmmPair, PoolState as AmmPoolState};
pub use bitassets::SeqId as BitAssetSeqId;
pub use dutch_auction::DutchAuctionState;
pub use error::Error;
use rollback::{HeightStamped, RollBack};

pub const WITHDRAWAL_BUNDLE_FAILURE_GAP: u32 = 4;

type WithdrawalBundlesDb = Database<
    SerdeBincode<M6id>,
    SerdeBincode<(
        Option<WithdrawalBundle>,
        RollBack<HeightStamped<WithdrawalBundleStatus>>,
    )>,
>;

#[derive(Clone)]
pub struct State {
    /// Current tip
    tip: WatchableDb<SerdeBincode<UnitKey>, SerdeBincode<BlockHash>>,
    /// Current height
    height: Database<SerdeBincode<UnitKey>, SerdeBincode<u32>>,
    /// Associates ordered pairs of BitAssets to their AMM pool states
    pub amm_pools: amm::PoolsDb,
    pub bitassets: bitassets::Dbs,
    /// Associates Dutch auction sequence numbers with auction state
    pub dutch_auctions: dutch_auction::Db,
    pub utxos: Database<SerdeBincode<OutPoint>, SerdeBincode<FilledOutput>>,
    pub stxos: Database<SerdeBincode<OutPoint>, SerdeBincode<SpentOutput>>,
    /// Pending withdrawal bundle and block height
    pub pending_withdrawal_bundle:
        Database<SerdeBincode<UnitKey>, SerdeBincode<(WithdrawalBundle, u32)>>,
    /// Latest failed (known) withdrawal bundle
    latest_failed_withdrawal_bundle: Database<
        SerdeBincode<UnitKey>,
        SerdeBincode<RollBack<HeightStamped<M6id>>>,
    >,
    /// Withdrawal bundles and their status.
    /// Some withdrawal bundles may be unknown.
    /// in which case they are `None`.
    withdrawal_bundles: WithdrawalBundlesDb,
    /// Deposit blocks and the height at which they were applied, keyed sequentially
    pub deposit_blocks:
        Database<SerdeBincode<u32>, SerdeBincode<(bitcoin::BlockHash, u32)>>,
    /// Withdrawal bundle event blocks and the height at which they were applied, keyed sequentially
    pub withdrawal_bundle_event_blocks:
        Database<SerdeBincode<u32>, SerdeBincode<(bitcoin::BlockHash, u32)>>,
}

impl State {
    pub const NUM_DBS: u32 = bitassets::Dbs::NUM_DBS + 11;

    pub fn new(env: &heed::Env) -> Result<Self, Error> {
        let mut rwtxn = env.write_txn()?;
        let tip = env.create_watchable_db(&mut rwtxn, "tip")?;
        let height = env.create_database(&mut rwtxn, Some("height"))?;
        let amm_pools = env.create_database(&mut rwtxn, Some("amm_pools"))?;
        let bitassets = bitassets::Dbs::new(env, &mut rwtxn)?;
        let dutch_auctions =
            env.create_database(&mut rwtxn, Some("dutch_auctions"))?;
        let utxos = env.create_database(&mut rwtxn, Some("utxos"))?;
        let stxos = env.create_database(&mut rwtxn, Some("stxos"))?;
        let pending_withdrawal_bundle =
            env.create_database(&mut rwtxn, Some("pending_withdrawal_bundle"))?;
        let latest_failed_withdrawal_bundle = env.create_database(
            &mut rwtxn,
            Some("latest_failed_withdrawal_bundle"),
        )?;
        let withdrawal_bundles =
            env.create_database(&mut rwtxn, Some("withdrawal_bundles"))?;
        let deposit_blocks =
            env.create_database(&mut rwtxn, Some("deposit_blocks"))?;
        let withdrawal_bundle_event_blocks = env.create_database(
            &mut rwtxn,
            Some("withdrawal_bundle_event_blocks"),
        )?;
        rwtxn.commit()?;
        Ok(Self {
            tip,
            height,
            amm_pools,
            bitassets,
            dutch_auctions,
            utxos,
            stxos,
            pending_withdrawal_bundle,
            latest_failed_withdrawal_bundle,
            withdrawal_bundles,
            withdrawal_bundle_event_blocks,
            deposit_blocks,
        })
    }

    pub fn try_get_tip(
        &self,
        rotxn: &RoTxn,
    ) -> Result<Option<BlockHash>, Error> {
        let tip = self.tip.try_get(rotxn, &UnitKey)?;
        Ok(tip)
    }

    pub fn try_get_height(&self, rotxn: &RoTxn) -> Result<Option<u32>, Error> {
        let height = self.height.get(rotxn, &UnitKey)?;
        Ok(height)
    }

    pub fn get_utxos(
        &self,
        txn: &RoTxn,
    ) -> Result<HashMap<OutPoint, FilledOutput>, Error> {
        let mut utxos = HashMap::new();
        for item in self.utxos.iter(txn)? {
            let (outpoint, output) = item?;
            utxos.insert(outpoint, output);
        }
        Ok(utxos)
    }

    pub fn get_utxos_by_addresses(
        &self,
        txn: &RoTxn,
        addresses: &HashSet<Address>,
    ) -> Result<HashMap<OutPoint, FilledOutput>, Error> {
        let mut utxos = HashMap::new();
        for item in self.utxos.iter(txn)? {
            let (outpoint, output) = item?;
            if addresses.contains(&output.address) {
                utxos.insert(outpoint, output);
            }
        }
        Ok(utxos)
    }

    /// Get the latest failed withdrawal bundle, and the height at which it failed
    pub fn get_latest_failed_withdrawal_bundle(
        &self,
        rotxn: &RoTxn,
    ) -> Result<Option<(u32, M6id)>, Error> {
        let Some(latest_failed_m6id) =
            self.latest_failed_withdrawal_bundle.get(rotxn, &UnitKey)?
        else {
            return Ok(None);
        };
        let latest_failed_m6id = latest_failed_m6id.latest().value;
        let (_bundle, bundle_status) = self.withdrawal_bundles.get(rotxn, &latest_failed_m6id)?
            .expect("Inconsistent DBs: latest failed m6id should exist in withdrawal_bundles");
        let bundle_status = bundle_status.latest();
        assert_eq!(bundle_status.value, WithdrawalBundleStatus::Failed);
        Ok(Some((bundle_status.height, latest_failed_m6id)))
    }

    pub fn fill_transaction(
        &self,
        rotxn: &RoTxn,
        transaction: &Transaction,
    ) -> Result<FilledTransaction, Error> {
        let mut spent_utxos = vec![];
        for input in &transaction.inputs {
            let utxo = self
                .utxos
                .get(rotxn, input)?
                .ok_or(Error::NoUtxo { outpoint: *input })?;
            spent_utxos.push(utxo);
        }
        Ok(FilledTransaction {
            spent_utxos,
            transaction: transaction.clone(),
        })
    }

    /// Fill a transaction that has already been applied
    pub fn fill_transaction_from_stxos(
        &self,
        rotxn: &RoTxn,
        tx: Transaction,
    ) -> Result<FilledTransaction, Error> {
        let txid = tx.txid();
        let mut spent_utxos = vec![];
        // fill inputs last-to-first
        for (vin, input) in tx.inputs.iter().enumerate().rev() {
            let stxo = self
                .stxos
                .get(rotxn, input)?
                .ok_or(Error::NoStxo { outpoint: *input })?;
            assert_eq!(
                stxo.inpoint,
                InPoint::Regular {
                    txid,
                    vin: vin as u32
                }
            );
            spent_utxos.push(stxo.output);
        }
        spent_utxos.reverse();
        Ok(FilledTransaction {
            spent_utxos,
            transaction: tx,
        })
    }

    pub fn fill_authorized_transaction(
        &self,
        rotxn: &RoTxn,
        transaction: AuthorizedTransaction,
    ) -> Result<Authorized<FilledTransaction>, Error> {
        let filled_tx =
            self.fill_transaction(rotxn, &transaction.transaction)?;
        let authorizations = transaction.authorizations;
        Ok(Authorized {
            transaction: filled_tx,
            authorizations,
        })
    }

    fn collect_withdrawal_bundle(
        &self,
        txn: &RoTxn,
        block_height: u32,
    ) -> Result<Option<WithdrawalBundle>, Error> {
        // Weight of a bundle with 0 outputs.
        const BUNDLE_0_WEIGHT: u64 = 504;
        // Weight of a single output.
        const OUTPUT_WEIGHT: u64 = 128;
        // Turns out to be 3121.
        const MAX_BUNDLE_OUTPUTS: usize =
            ((bitcoin::policy::MAX_STANDARD_TX_WEIGHT as u64 - BUNDLE_0_WEIGHT)
                / OUTPUT_WEIGHT) as usize;

        // Aggregate all outputs by destination.
        // destination -> (value, mainchain fee, spent_utxos)
        let mut address_to_aggregated_withdrawal = HashMap::<
            bitcoin::Address<bitcoin::address::NetworkUnchecked>,
            AggregatedWithdrawal,
        >::new();
        for item in self.utxos.iter(txn)? {
            let (outpoint, output) = item?;
            if let FilledOutputContent::BitcoinWithdrawal(ref withdrawal) =
                output.content
            {
                let aggregated = address_to_aggregated_withdrawal
                    .entry(withdrawal.main_address.clone())
                    .or_insert(AggregatedWithdrawal {
                        spend_utxos: HashMap::new(),
                        main_address: withdrawal.main_address.clone(),
                        value: bitcoin::Amount::ZERO,
                        main_fee: bitcoin::Amount::ZERO,
                    });
                // Add up all values.
                aggregated.value = aggregated
                    .value
                    .checked_add(withdrawal.value)
                    .ok_or(AmountOverflowError)?;
                aggregated.main_fee = aggregated
                    .main_fee
                    .checked_add(withdrawal.main_fee)
                    .ok_or(AmountOverflowError)?;
                aggregated.spend_utxos.insert(outpoint, output);
            }
        }
        if address_to_aggregated_withdrawal.is_empty() {
            return Ok(None);
        }
        let mut aggregated_withdrawals: Vec<_> =
            address_to_aggregated_withdrawal.into_values().collect();
        aggregated_withdrawals.sort_by_key(|a| std::cmp::Reverse(a.clone()));
        let mut fee = bitcoin::Amount::ZERO;
        let mut spend_utxos = BTreeMap::<OutPoint, FilledOutput>::new();
        let mut bundle_outputs = vec![];
        for aggregated in &aggregated_withdrawals {
            if bundle_outputs.len() > MAX_BUNDLE_OUTPUTS {
                break;
            }
            let bundle_output = bitcoin::TxOut {
                value: aggregated.value,
                script_pubkey: aggregated
                    .main_address
                    .assume_checked_ref()
                    .script_pubkey(),
            };
            spend_utxos.extend(aggregated.spend_utxos.clone());
            bundle_outputs.push(bundle_output);
            fee += aggregated.main_fee;
        }
        let bundle = WithdrawalBundle::new(
            block_height,
            fee,
            spend_utxos,
            bundle_outputs,
        )?;
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

    /// Get pending withdrawal bundle and block height
    pub fn get_pending_withdrawal_bundle(
        &self,
        txn: &RoTxn,
    ) -> Result<Option<(WithdrawalBundle, u32)>, Error> {
        Ok(self.pending_withdrawal_bundle.get(txn, &UnitKey)?)
    }

    /// Check that
    /// * If the tx is a BitAsset reservation, then the number of bitasset
    ///   reservations in the outputs is exactly one more than the number of
    ///   bitasset reservations in the inputs.
    /// * If the tx is a BitAsset
    ///   registration, then the number of bitasset reservations in the outputs
    ///   is exactly one less than the number of bitasset reservations in the
    ///   inputs.
    /// * Otherwise, the number of bitasset reservations in the outputs
    ///   is exactly equal to the number of bitasset reservations in the inputs.
    pub fn validate_reservations(
        &self,
        tx: &FilledTransaction,
    ) -> Result<(), Error> {
        let n_reservation_inputs: usize = tx.spent_reservations().count();
        let n_reservation_outputs: usize = tx.reservation_outputs().count();
        if tx.is_reservation() {
            if n_reservation_outputs == n_reservation_inputs + 1 {
                return Ok(());
            }
        } else if tx.is_registration() {
            if n_reservation_inputs == n_reservation_outputs + 1 {
                return Ok(());
            }
        } else if n_reservation_inputs == n_reservation_outputs {
            return Ok(());
        }
        Err(Error::UnbalancedReservations {
            n_reservation_inputs,
            n_reservation_outputs,
        })
    }

    /** Check that
     *  * If the tx is a BitAsset registration, then
     *    * The number of BitAsset control coins in the outputs is exactly
     *      one more than the number of BitAsset control coins in the
     *      inputs
     *    * The number of BitAsset outputs is at least
     *      * The number of unique BitAsset inputs,
     *        if the initial supply is zero
     *      * One more than the number of unique BitAsset inputs,
     *        if the initial supply is nonzero.
     *    * The newly registered BitAsset must have been unregistered,
     *      prior to the registration tx.
     *    * The last output must be a BitAsset control coin
     *    * If the initial supply is nonzero,
     *      the second-to-last output must be a BitAsset output
     *    * Otherwise,
     *      * The number of BitAsset control coin outputs is exactly the number
     *        of BitAsset control coin inputs
     *      * The number of BitAsset outputs is at least
     *        the number of unique BitAssets in the inputs.
     *  * If the tx is a BitAsset update, then there must be at least one
     *    BitAsset control coin input and output.
     *  * If the tx is an AMM Burn, then
     *    * There must be at least two unique BitAsset outputs
     *    * The number of unique BitAsset outputs must be at most two more than
     *      the number of unique BitAsset inputs
     *    * The number of unique BitAsset inputs must be at most equal to the
     *      number of unique BitAsset outputs
     *  * If the tx is an AMM Mint, then
     *    * There must be at least two BitAsset inputs
     *    * The number of unique BitAsset outputs must be at most equal to the
     *      number of unique BitAsset inputs
     *    * The number of unique BitAsset inputs must be at most two more than
     *      the number of unique BitAsset outputs.
     *  * If the tx is an AMM Swap, then
     *    * There must be at least one BitAsset input
     *    * The number of unique BitAsset outputs must be one less than,
     *      one greater than, or equal to, the number of unique BitAsset inputs.
     *  * If the tx is a Dutch auction create, then
     *    * There must be at least one unique BitAsset input
     *    * The number of unique BitAsset outputs must be at most equal to the
     *      number of unique BitAsset inputs
     *    * The number of unique BitAsset inputs must be at most one more than
     *      the number of unique BitAsset outputs.
     *  * If the tx is a Dutch auction bid, then
     *    * There must be at least one BitAsset input
     *    * The number of unique BitAsset outputs must be one less than,
     *      one greater than, or equal to, the number of unique BitAsset inputs.
     *  * If the tx is a Dutch auction collect, then
     *    * There must be at least one unique BitAsset output
     *    * The number of unique BitAsset outputs must be at most two more than
     *      the number of unique BitAsset inputs
     *    * The number of unique BitAsset inputs must be at most equal to the
     *      number of unique BitAsset outputs
     * */
    pub fn validate_bitassets(
        &self,
        rotxn: &RoTxn,
        tx: &FilledTransaction,
    ) -> Result<(), Error> {
        // number of unique bitassets in the inputs
        let n_unique_bitasset_inputs: usize = tx
            .spent_bitassets()
            .filter_map(|(_, output)| output.bitasset())
            .unique()
            .count();
        let n_bitasset_control_inputs: usize =
            tx.spent_bitasset_controls().count();
        let n_bitasset_outputs: usize = tx.bitasset_outputs().count();
        let n_unique_bitasset_outputs: usize =
            tx.unique_spent_bitassets().len();
        let n_bitasset_control_outputs: usize =
            tx.bitasset_control_outputs().count();
        if tx.is_update()
            && (n_bitasset_control_inputs < 1 || n_bitasset_control_outputs < 1)
        {
            return Err(error::BitAsset::NoBitAssetsToUpdate.into());
        };
        if tx.is_amm_burn()
            && (n_unique_bitasset_outputs < 2
                || n_unique_bitasset_inputs > n_unique_bitasset_outputs
                || n_unique_bitasset_outputs > n_unique_bitasset_inputs + 2)
        {
            return Err(error::Amm::InvalidBurn.into());
        };
        if tx.is_amm_mint()
            && (n_unique_bitasset_inputs < 2
                || n_unique_bitasset_outputs > n_unique_bitasset_inputs
                || n_unique_bitasset_inputs > n_unique_bitasset_outputs + 2)
        {
            return Err(error::Amm::TooFewBitAssetsToMint.into());
        };
        if (tx.is_amm_swap() || tx.is_dutch_auction_bid())
            && (n_unique_bitasset_inputs < 1
                || !{
                    let min_unique_bitasset_outputs =
                        n_unique_bitasset_inputs.saturating_sub(1);
                    let max_unique_bitasset_outputs =
                        n_unique_bitasset_inputs + 1;
                    (min_unique_bitasset_outputs..=max_unique_bitasset_outputs)
                        .contains(&n_unique_bitasset_outputs)
                })
        {
            let err = error::dutch_auction::Bid::Invalid;
            return Err(Error::DutchAuction(err.into()));
        };
        if tx.is_dutch_auction_create()
            && (n_unique_bitasset_inputs < 1
                || n_unique_bitasset_outputs > n_unique_bitasset_inputs
                || n_unique_bitasset_inputs > n_unique_bitasset_outputs + 1)
        {
            return Err(error::DutchAuction::TooFewBitAssetsToCreate.into());
        };
        if tx.is_dutch_auction_collect()
            && (n_unique_bitasset_outputs < 1
                || n_unique_bitasset_inputs > n_unique_bitasset_outputs
                || n_unique_bitasset_outputs > n_unique_bitasset_inputs + 2)
        {
            let err = error::dutch_auction::Collect::Invalid;
            return Err(Error::DutchAuction(err.into()));
        };
        if let Some(TxData::BitAssetRegistration {
            name_hash,
            initial_supply,
            ..
        }) = tx.data()
        {
            if n_bitasset_control_outputs != n_bitasset_control_inputs + 1 {
                return Err(Error::UnbalancedBitAssetControls {
                    n_bitasset_control_inputs,
                    n_bitasset_control_outputs,
                });
            };
            if !tx
                .outputs()
                .last()
                .is_some_and(|last_output| last_output.is_bitasset_control())
            {
                return Err(Error::LastOutputNotControlCoin);
            }
            if *initial_supply == 0 {
                if n_bitasset_outputs < n_unique_bitasset_inputs {
                    return Err(Error::UnbalancedBitAssets {
                        n_unique_bitasset_inputs,
                        n_bitasset_outputs,
                    });
                }
            } else {
                if n_bitasset_outputs < n_unique_bitasset_inputs + 1 {
                    return Err(Error::UnbalancedBitAssets {
                        n_unique_bitasset_inputs,
                        n_bitasset_outputs,
                    });
                }
                let outputs = tx.outputs();
                let second_to_last_output = outputs.get(outputs.len() - 2);
                if !second_to_last_output
                    .is_some_and(|s2l_output| s2l_output.is_bitasset())
                {
                    return Err(Error::SecondLastOutputNotBitAsset);
                }
            }
            if self
                .bitassets
                .try_get_bitasset(rotxn, &BitAssetId(*name_hash))?
                .is_some()
            {
                return Err(Error::BitAssetAlreadyRegistered {
                    name_hash: *name_hash,
                });
            };
            Ok(())
        } else {
            if n_bitasset_control_outputs != n_bitasset_control_inputs {
                return Err(Error::UnbalancedBitAssetControls {
                    n_bitasset_control_inputs,
                    n_bitasset_control_outputs,
                });
            };
            if n_bitasset_outputs < n_unique_bitasset_inputs {
                return Err(Error::UnbalancedBitAssets {
                    n_unique_bitasset_inputs,
                    n_bitasset_outputs,
                });
            }
            Ok(())
        }
    }

    /// Validates a filled transaction, and returns the fee
    pub fn validate_filled_transaction(
        &self,
        rotxn: &RoTxn,
        tx: &FilledTransaction,
    ) -> Result<bitcoin::Amount, Error> {
        let () = self.validate_reservations(tx)?;
        let () = self.validate_bitassets(rotxn, tx)?;
        tx.bitcoin_fee()?.ok_or(Error::NotEnoughValueIn)
    }

    pub fn validate_transaction(
        &self,
        rotxn: &RoTxn,
        transaction: &AuthorizedTransaction,
    ) -> Result<bitcoin::Amount, Error> {
        let filled_transaction =
            self.fill_transaction(rotxn, &transaction.transaction)?;
        for (authorization, spent_utxo) in transaction
            .authorizations
            .iter()
            .zip(filled_transaction.spent_utxos.iter())
        {
            if authorization.get_address() != spent_utxo.address {
                return Err(Error::WrongPubKeyForAddress);
            }
        }
        if Authorization::verify_transaction(transaction).is_err() {
            return Err(Error::AuthorizationError);
        }
        let fee =
            self.validate_filled_transaction(rotxn, &filled_transaction)?;
        Ok(fee)
    }

    pub fn validate_block(
        &self,
        rotxn: &RoTxn,
        header: &Header,
        body: &Body,
    ) -> Result<bitcoin::Amount, Error> {
        let tip_hash = self.try_get_tip(rotxn)?;
        if header.prev_side_hash != tip_hash {
            let err = error::InvalidHeader::PrevSideHash {
                expected: tip_hash,
                received: header.prev_side_hash,
            };
            return Err(Error::InvalidHeader(err));
        };
        let merkle_root = body.compute_merkle_root();
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
        let mut spent_utxos = HashSet::new();
        let filled_transactions: Vec<_> = body
            .transactions
            .iter()
            .map(|t| self.fill_transaction(rotxn, t))
            .collect::<Result<_, _>>()?;
        for filled_tx in &filled_transactions {
            for input in &filled_tx.transaction.inputs {
                if spent_utxos.contains(input) {
                    return Err(Error::UtxoDoubleSpent);
                }
                spent_utxos.insert(*input);
            }
            total_fees = total_fees
                .checked_add(
                    self.validate_filled_transaction(rotxn, filled_tx)?,
                )
                .ok_or(AmountOverflowError)?;
        }
        if coinbase_value > total_fees {
            return Err(Error::NotEnoughFees);
        }
        let spent_utxos = filled_transactions
            .iter()
            .flat_map(|t| t.spent_utxos.iter());
        for (authorization, spent_utxo) in
            body.authorizations.iter().zip(spent_utxos)
        {
            if authorization.get_address() != spent_utxo.address {
                return Err(Error::WrongPubKeyForAddress);
            }
        }
        if Authorization::verify_body(body).is_err() {
            return Err(Error::AuthorizationError);
        }
        Ok(total_fees)
    }

    pub fn get_last_deposit_block_hash(
        &self,
        rotxn: &RoTxn,
    ) -> Result<Option<bitcoin::BlockHash>, Error> {
        let block_hash = self
            .deposit_blocks
            .last(rotxn)?
            .map(|(_, (block_hash, _))| block_hash);
        Ok(block_hash)
    }

    pub fn get_last_withdrawal_bundle_event_block_hash(
        &self,
        rotxn: &RoTxn,
    ) -> Result<Option<bitcoin::BlockHash>, Error> {
        let block_hash = self
            .withdrawal_bundle_event_blocks
            .last(rotxn)?
            .map(|(_, (block_hash, _))| block_hash);
        Ok(block_hash)
    }

    fn connect_2wpd_withdrawal_bundle_submitted(
        &self,
        rwtxn: &mut RwTxn,
        block_height: u32,
        event_block_hash: &bitcoin::BlockHash,
        m6id: M6id,
    ) -> Result<(), Error> {
        if let Some((bundle, bundle_block_height)) =
            self.pending_withdrawal_bundle.get(rwtxn, &UnitKey)?
            && bundle.compute_m6id() == m6id
        {
            assert_eq!(bundle_block_height, block_height - 1);
            tracing::debug!(
                %block_height,
                %m6id,
                "Withdrawal bundle successfully submitted"
            );
            for (outpoint, spend_output) in bundle.spend_utxos() {
                self.utxos.delete(rwtxn, outpoint)?;
                let spent_output = SpentOutput {
                    output: spend_output.clone(),
                    inpoint: InPoint::Withdrawal { m6id },
                };
                self.stxos.put(rwtxn, outpoint, &spent_output)?;
            }
            self.withdrawal_bundles.put(
                rwtxn,
                &m6id,
                &(
                    Some(bundle),
                    RollBack::<HeightStamped<_>>::new(
                        WithdrawalBundleStatus::Submitted,
                        block_height,
                    ),
                ),
            )?;
            self.pending_withdrawal_bundle.delete(rwtxn, &UnitKey)?;
        } else if let Some((_bundle, bundle_status)) =
            self.withdrawal_bundles.get(rwtxn, &m6id)?
        {
            // Already applied
            assert_eq!(
                bundle_status.earliest().value,
                WithdrawalBundleStatus::Submitted
            );
        } else {
            tracing::warn!(
                %event_block_hash,
                %m6id,
                "Unknown withdrawal bundle submitted"
            );
            self.withdrawal_bundles.put(
                rwtxn,
                &m6id,
                &(
                    None,
                    RollBack::<HeightStamped<_>>::new(
                        WithdrawalBundleStatus::Submitted,
                        block_height,
                    ),
                ),
            )?;
        };
        Ok(())
    }

    fn connect_2wpd_withdrawal_bundle_confirmed(
        &self,
        rwtxn: &mut RwTxn,
        block_height: u32,
        event_block_hash: &bitcoin::BlockHash,
        m6id: M6id,
    ) -> Result<(), Error> {
        let (bundle, mut bundle_status) = self
            .withdrawal_bundles
            .get(rwtxn, &m6id)?
            .ok_or(Error::UnknownWithdrawalBundle { m6id })?;
        if bundle_status.latest().value == WithdrawalBundleStatus::Confirmed {
            // Already applied
            return Ok(());
        }
        assert_eq!(
            bundle_status.latest().value,
            WithdrawalBundleStatus::Submitted
        );
        // If an unknown bundle is confirmed, all UTXOs older than the
        // bundle submission are potentially spent.
        // This is only accepted in the case that block height is 0,
        // and so no UTXOs could possibly have been double-spent yet.
        // In this case, ALL UTXOs are considered spent.
        if bundle.is_none() {
            if block_height == 0 {
                tracing::warn!(
                    %event_block_hash,
                    %m6id,
                    "Unknown withdrawal bundle confirmed, marking all UTXOs as spent"
                );
                let utxos: Vec<_> =
                    self.utxos.iter(rwtxn)?.collect::<Result<_, _>>()?;
                for (outpoint, output) in utxos {
                    let spent_output = SpentOutput {
                        output,
                        inpoint: InPoint::Withdrawal { m6id },
                    };
                    self.stxos.put(rwtxn, &outpoint, &spent_output)?;
                }
                self.utxos.clear(rwtxn)?;
            } else {
                return Err(Error::UnknownWithdrawalBundleConfirmed {
                    event_block_hash: *event_block_hash,
                    m6id,
                });
            }
        }
        bundle_status
            .push(WithdrawalBundleStatus::Confirmed, block_height)
            .expect("Push confirmed status should be valid");
        self.withdrawal_bundles
            .put(rwtxn, &m6id, &(bundle, bundle_status))?;
        Ok(())
    }

    fn connect_2wpd_withdrawal_bundle_failed(
        &self,
        rwtxn: &mut RwTxn,
        block_height: u32,
        m6id: M6id,
    ) -> Result<(), Error> {
        tracing::debug!(
            %block_height,
            %m6id,
            "Handling failed withdrawal bundle");
        let (bundle, mut bundle_status) = self
            .withdrawal_bundles
            .get(rwtxn, &m6id)?
            .ok_or_else(|| Error::UnknownWithdrawalBundle { m6id })?;
        if bundle_status.latest().value == WithdrawalBundleStatus::Failed {
            // Already applied
            return Ok(());
        }
        assert_eq!(
            bundle_status.latest().value,
            WithdrawalBundleStatus::Submitted
        );
        bundle_status
            .push(WithdrawalBundleStatus::Failed, block_height)
            .expect("Push failed status should be valid");
        if let Some(bundle) = &bundle {
            for (outpoint, output) in bundle.spend_utxos() {
                self.stxos.delete(rwtxn, outpoint)?;
                self.utxos.put(rwtxn, outpoint, output)?;
            }
            let latest_failed_m6id = if let Some(mut latest_failed_m6id) =
                self.latest_failed_withdrawal_bundle.get(rwtxn, &UnitKey)?
            {
                latest_failed_m6id
                    .push(m6id, block_height)
                    .expect("Push latest failed m6id should be valid");
                latest_failed_m6id
            } else {
                RollBack::<HeightStamped<_>>::new(m6id, block_height)
            };
            self.latest_failed_withdrawal_bundle.put(
                rwtxn,
                &UnitKey,
                &latest_failed_m6id,
            )?;
        }
        self.withdrawal_bundles
            .put(rwtxn, &m6id, &(bundle, bundle_status))?;
        Ok(())
    }

    fn connect_2wpd_withdrawal_bundle_event(
        &self,
        rwtxn: &mut RwTxn,
        block_height: u32,
        event_block_hash: &bitcoin::BlockHash,
        event: &WithdrawalBundleEvent,
    ) -> Result<(), Error> {
        match event.status {
            WithdrawalBundleStatus::Submitted => self
                .connect_2wpd_withdrawal_bundle_submitted(
                    rwtxn,
                    block_height,
                    event_block_hash,
                    event.m6id,
                ),
            WithdrawalBundleStatus::Confirmed => self
                .connect_2wpd_withdrawal_bundle_confirmed(
                    rwtxn,
                    block_height,
                    event_block_hash,
                    event.m6id,
                ),
            WithdrawalBundleStatus::Failed => self
                .connect_2wpd_withdrawal_bundle_failed(
                    rwtxn,
                    block_height,
                    event.m6id,
                ),
        }
    }

    fn connect_2wpd_event(
        &self,
        rwtxn: &mut RwTxn,
        block_height: u32,
        latest_deposit_block_hash: &mut Option<bitcoin::BlockHash>,
        latest_withdrawal_bundle_event_block_hash: &mut Option<
            bitcoin::BlockHash,
        >,
        event_block_hash: bitcoin::BlockHash,
        event: &BlockEvent,
    ) -> Result<(), Error> {
        match event {
            BlockEvent::Deposit(deposit) => {
                let outpoint = OutPoint::Deposit(deposit.outpoint);
                let output = deposit.output.clone();
                self.utxos.put(rwtxn, &outpoint, &output)?;
                *latest_deposit_block_hash = Some(event_block_hash);
            }
            BlockEvent::WithdrawalBundle(withdrawal_bundle_event) => {
                let () = self.connect_2wpd_withdrawal_bundle_event(
                    rwtxn,
                    block_height,
                    &event_block_hash,
                    withdrawal_bundle_event,
                )?;
                *latest_withdrawal_bundle_event_block_hash =
                    Some(event_block_hash);
            }
        }
        Ok(())
    }

    pub fn connect_two_way_peg_data(
        &self,
        rwtxn: &mut RwTxn,
        two_way_peg_data: &TwoWayPegData,
    ) -> Result<(), Error> {
        let block_height = self.try_get_height(rwtxn)?.ok_or(Error::NoTip)?;
        tracing::trace!(%block_height, "Connecting 2WPD...");
        // Handle deposits.
        let mut latest_deposit_block_hash = None;
        let mut latest_withdrawal_bundle_event_block_hash = None;
        for (event_block_hash, event_block_info) in &two_way_peg_data.block_info
        {
            for event in &event_block_info.events {
                let () = self.connect_2wpd_event(
                    rwtxn,
                    block_height,
                    &mut latest_deposit_block_hash,
                    &mut latest_withdrawal_bundle_event_block_hash,
                    *event_block_hash,
                    event,
                )?;
            }
        }
        // Handle deposits.
        if let Some(latest_deposit_block_hash) = latest_deposit_block_hash {
            let deposit_block_seq_idx = self
                .deposit_blocks
                .last(rwtxn)?
                .map_or(0, |(seq_idx, _)| seq_idx + 1);
            self.deposit_blocks.put(
                rwtxn,
                &deposit_block_seq_idx,
                &(latest_deposit_block_hash, block_height),
            )?;
        }
        // Handle withdrawals
        if let Some(latest_withdrawal_bundle_event_block_hash) =
            latest_withdrawal_bundle_event_block_hash
        {
            let withdrawal_bundle_event_block_seq_idx = self
                .withdrawal_bundle_event_blocks
                .last(rwtxn)?
                .map_or(0, |(seq_idx, _)| seq_idx + 1);
            self.withdrawal_bundle_event_blocks.put(
                rwtxn,
                &withdrawal_bundle_event_block_seq_idx,
                &(latest_withdrawal_bundle_event_block_hash, block_height),
            )?;
        }
        let last_withdrawal_bundle_failure_height = self
            .get_latest_failed_withdrawal_bundle(rwtxn)?
            .map(|(height, _bundle)| height)
            .unwrap_or_default();
        if block_height - last_withdrawal_bundle_failure_height
            >= WITHDRAWAL_BUNDLE_FAILURE_GAP
            && self
                .pending_withdrawal_bundle
                .get(rwtxn, &UnitKey)?
                .is_none()
        {
            if let Some(bundle) =
                self.collect_withdrawal_bundle(rwtxn, block_height)?
            {
                let m6id = bundle.compute_m6id();
                self.pending_withdrawal_bundle.put(
                    rwtxn,
                    &UnitKey,
                    &(bundle, block_height),
                )?;
                tracing::trace!(
                    %block_height,
                    %m6id,
                    "Stored pending withdrawal bundle"
                );
            }
        }
        Ok(())
    }

    pub fn disconnect_two_way_peg_data(
        &self,
        rwtxn: &mut RwTxn,
        two_way_peg_data: &TwoWayPegData,
    ) -> Result<(), Error> {
        let block_height = self
            .try_get_height(rwtxn)?
            .expect("Height should not be None");
        // Restore pending withdrawal bundle
        for (_, event) in two_way_peg_data.withdrawal_bundle_events().rev() {
            match event.status {
                WithdrawalBundleStatus::Submitted => {
                    let Some((bundle, bundle_status)) =
                        self.withdrawal_bundles.get(rwtxn, &event.m6id)?
                    else {
                        if let Some((bundle, _)) = self
                            .pending_withdrawal_bundle
                            .get(rwtxn, &UnitKey)?
                            && bundle.compute_m6id() == event.m6id
                        {
                            // Already applied
                            continue;
                        }
                        return Err(Error::UnknownWithdrawalBundle {
                            m6id: event.m6id,
                        });
                    };
                    let bundle_status = bundle_status.latest();
                    assert_eq!(
                        bundle_status.value,
                        WithdrawalBundleStatus::Submitted
                    );
                    assert_eq!(bundle_status.height, block_height);
                    for (outpoint, output) in bundle.spend_utxos().iter().rev()
                    {
                        if !self.stxos.delete(rwtxn, outpoint)? {
                            return Err(Error::NoStxo {
                                outpoint: *outpoint,
                            });
                        };
                        self.utxos.put(rwtxn, outpoint, output)?;
                    }
                    self.pending_withdrawal_bundle.put(
                        rwtxn,
                        &UnitKey,
                        &(bundle, bundle_status.height - 1),
                    )?;
                    self.withdrawal_bundles.delete(rwtxn, &event.m6id)?;
                }
                WithdrawalBundleStatus::Confirmed => {
                    let Some((bundle, bundle_status)) =
                        self.withdrawal_bundles.get(rwtxn, &event.m6id)?
                    else {
                        return Err(Error::UnknownWithdrawalBundle {
                            m6id: event.m6id,
                        });
                    };
                    let (prev_bundle_status, latest_bundle_status) =
                        bundle_status.pop();
                    if latest_bundle_status.value
                        == WithdrawalBundleStatus::Submitted
                    {
                        // Already applied
                        continue;
                    } else {
                        assert_eq!(
                            latest_bundle_status.value,
                            WithdrawalBundleStatus::Confirmed
                        );
                    }
                    assert_eq!(latest_bundle_status.height, block_height);
                    let prev_bundle_status = prev_bundle_status
                        .expect("Pop confirmed bundle status should be valid");
                    assert_eq!(
                        prev_bundle_status.latest().value,
                        WithdrawalBundleStatus::Submitted
                    );
                    self.withdrawal_bundles.put(
                        rwtxn,
                        &event.m6id,
                        &(bundle, prev_bundle_status),
                    )?;
                }
                WithdrawalBundleStatus::Failed => {
                    let Some((bundle, bundle_status)) =
                        self.withdrawal_bundles.get(rwtxn, &event.m6id)?
                    else {
                        return Err(Error::UnknownWithdrawalBundle {
                            m6id: event.m6id,
                        });
                    };
                    let (prev_bundle_status, latest_bundle_status) =
                        bundle_status.pop();
                    if latest_bundle_status.value
                        == WithdrawalBundleStatus::Submitted
                    {
                        // Already applied
                        continue;
                    } else {
                        assert_eq!(
                            latest_bundle_status.value,
                            WithdrawalBundleStatus::Failed
                        );
                    }
                    assert_eq!(latest_bundle_status.height, block_height);
                    let prev_bundle_status = prev_bundle_status
                        .expect("Pop failed bundle status should be valid");
                    assert_eq!(
                        prev_bundle_status.latest().value,
                        WithdrawalBundleStatus::Submitted
                    );
                    for (outpoint, output) in bundle.spend_utxos().iter().rev()
                    {
                        let spent_output = SpentOutput {
                            output: output.clone(),
                            inpoint: InPoint::Withdrawal { m6id: event.m6id },
                        };
                        self.stxos.put(rwtxn, outpoint, &spent_output)?;
                        if self.utxos.delete(rwtxn, outpoint)? {
                            return Err(Error::NoUtxo {
                                outpoint: *outpoint,
                            });
                        };
                    }
                    self.withdrawal_bundles.put(
                        rwtxn,
                        &event.m6id,
                        &(bundle, prev_bundle_status),
                    )?;
                    let (prev_latest_failed_m6id, latest_failed_m6id) = self
                        .latest_failed_withdrawal_bundle
                        .get(rwtxn, &UnitKey)?
                        .expect("latest failed withdrawal bundle should exist")
                        .pop();
                    assert_eq!(latest_failed_m6id.value, event.m6id);
                    assert_eq!(latest_failed_m6id.height, block_height);
                    if let Some(prev_latest_failed_m6id) =
                        prev_latest_failed_m6id
                    {
                        self.latest_failed_withdrawal_bundle.put(
                            rwtxn,
                            &UnitKey,
                            &prev_latest_failed_m6id,
                        )?;
                    } else {
                        self.latest_failed_withdrawal_bundle
                            .delete(rwtxn, &UnitKey)?;
                    }
                }
            }
        }
        // Handle withdrawals
        if let Some(latest_withdrawal_bundle_event_block_hash) =
            two_way_peg_data.latest_withdrawal_bundle_event_block_hash()
        {
            let (
                last_withdrawal_bundle_event_block_seq_idx,
                (
                    last_withdrawal_bundle_event_block_hash,
                    last_withdrawal_bundle_event_block_height,
                ),
            ) = self
                .withdrawal_bundle_event_blocks
                .last(rwtxn)?
                .ok_or(Error::NoWithdrawalBundleEventBlock)?;
            assert_eq!(
                *latest_withdrawal_bundle_event_block_hash,
                last_withdrawal_bundle_event_block_hash
            );
            assert_eq!(
                block_height - 1,
                last_withdrawal_bundle_event_block_height
            );
            if !self
                .deposit_blocks
                .delete(rwtxn, &last_withdrawal_bundle_event_block_seq_idx)?
            {
                return Err(Error::NoWithdrawalBundleEventBlock);
            };
        }
        let last_withdrawal_bundle_failure_height = self
            .get_latest_failed_withdrawal_bundle(rwtxn)?
            .map(|(height, _bundle)| height)
            .unwrap_or_default();
        if block_height - last_withdrawal_bundle_failure_height
            > WITHDRAWAL_BUNDLE_FAILURE_GAP
            && let Some((bundle, bundle_height)) =
                self.pending_withdrawal_bundle.get(rwtxn, &UnitKey)?
            && bundle_height == block_height - 1
        {
            self.pending_withdrawal_bundle.delete(rwtxn, &UnitKey)?;
            for (outpoint, output) in bundle.spend_utxos().iter().rev() {
                if !self.stxos.delete(rwtxn, outpoint)? {
                    return Err(Error::NoStxo {
                        outpoint: *outpoint,
                    });
                };
                self.utxos.put(rwtxn, outpoint, output)?;
            }
        }
        // Handle deposits
        if let Some(latest_deposit_block_hash) =
            two_way_peg_data.latest_deposit_block_hash()
        {
            let (
                last_deposit_block_seq_idx,
                (last_deposit_block_hash, last_deposit_block_height),
            ) = self
                .deposit_blocks
                .last(rwtxn)?
                .ok_or(Error::NoDepositBlock)?;
            assert_eq!(latest_deposit_block_hash, last_deposit_block_hash);
            assert_eq!(block_height - 1, last_deposit_block_height);
            if !self
                .deposit_blocks
                .delete(rwtxn, &last_deposit_block_seq_idx)?
            {
                return Err(Error::NoDepositBlock);
            };
        }
        for deposit in two_way_peg_data
            .deposits()
            .flat_map(|(_, deposits)| deposits)
            .rev()
        {
            let outpoint = OutPoint::Deposit(deposit.outpoint);
            if !self.utxos.delete(rwtxn, &outpoint)? {
                return Err(Error::NoUtxo { outpoint });
            }
        }
        Ok(())
    }

    pub fn connect_block(
        &self,
        rwtxn: &mut RwTxn,
        header: &Header,
        body: &Body,
    ) -> Result<(), Error> {
        let height = self.try_get_height(rwtxn)?.map_or(0, |height| height + 1);
        let tip_hash = self.try_get_tip(rwtxn)?;
        if tip_hash != header.prev_side_hash {
            let err = error::InvalidHeader::PrevSideHash {
                expected: tip_hash,
                received: header.prev_side_hash,
            };
            return Err(Error::InvalidHeader(err));
        }
        let merkle_root = body.compute_merkle_root();
        if merkle_root != header.merkle_root {
            let err = Error::InvalidBody {
                expected: merkle_root,
                computed: header.merkle_root,
            };
            return Err(err);
        }
        for (vout, output) in body.coinbase.iter().enumerate() {
            let outpoint = OutPoint::Coinbase {
                merkle_root,
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
            self.utxos.put(rwtxn, &outpoint, &filled_output)?;
        }
        for transaction in &body.transactions {
            let filled_tx = self.fill_transaction(rwtxn, transaction)?;
            let txid = filled_tx.txid();
            for (vin, input) in filled_tx.inputs().iter().enumerate() {
                let spent_output = self
                    .utxos
                    .get(rwtxn, input)?
                    .ok_or(Error::NoUtxo { outpoint: *input })?;
                let spent_output = SpentOutput {
                    output: spent_output,
                    inpoint: InPoint::Regular {
                        txid,
                        vin: vin as u32,
                    },
                };
                self.utxos.delete(rwtxn, input)?;
                self.stxos.put(rwtxn, input, &spent_output)?;
            }
            let filled_outputs = filled_tx
                .filled_outputs()
                .ok_or(Error::FillTxOutputContentsFailed)?;
            for (vout, filled_output) in filled_outputs.iter().enumerate() {
                let outpoint = OutPoint::Regular {
                    txid,
                    vout: vout as u32,
                };
                self.utxos.put(rwtxn, &outpoint, filled_output)?;
            }
            match &transaction.data {
                None => (),
                Some(TxData::AmmBurn { .. }) => {
                    let () =
                        amm::apply_burn(&self.amm_pools, rwtxn, &filled_tx)?;
                }
                Some(TxData::AmmMint { .. }) => {
                    let () =
                        amm::apply_mint(&self.amm_pools, rwtxn, &filled_tx)?;
                }
                Some(TxData::AmmSwap { .. }) => {
                    let () =
                        amm::apply_swap(&self.amm_pools, rwtxn, &filled_tx)?;
                }
                Some(TxData::BitAssetReservation { commitment }) => {
                    let () = self
                        .bitassets
                        .put_reservation(rwtxn, &txid, commitment)?;
                }
                Some(TxData::BitAssetRegistration {
                    name_hash,
                    revealed_nonce: _,
                    bitasset_data,
                    initial_supply,
                }) => {
                    let () = self.bitassets.apply_registration(
                        rwtxn,
                        &filled_tx,
                        *name_hash,
                        bitasset_data,
                        *initial_supply,
                        height,
                    )?;
                }
                Some(TxData::BitAssetMint(mint_amount)) => {
                    let () = self.bitassets.apply_mint(
                        rwtxn,
                        &filled_tx,
                        *mint_amount,
                        height,
                    )?;
                }
                Some(TxData::BitAssetUpdate(bitasset_updates)) => {
                    let () = self.bitassets.apply_updates(
                        rwtxn,
                        &filled_tx,
                        (**bitasset_updates).clone(),
                        height,
                    )?;
                }
                Some(TxData::DutchAuctionBid { .. }) => {
                    let () = dutch_auction::apply_bid(
                        &self.dutch_auctions,
                        rwtxn,
                        &filled_tx,
                        height,
                    )?;
                }
                Some(TxData::DutchAuctionCreate(dutch_auction_params)) => {
                    let () = dutch_auction::apply_create(
                        &self.dutch_auctions,
                        rwtxn,
                        &filled_tx,
                        *dutch_auction_params,
                        height,
                    )?;
                }
                Some(TxData::DutchAuctionCollect { .. }) => {
                    let () = dutch_auction::apply_collect(
                        &self.dutch_auctions,
                        rwtxn,
                        &filled_tx,
                        height,
                    )?;
                }
            }
        }
        let block_hash = header.hash();
        self.tip.put(rwtxn, &UnitKey, &block_hash)?;
        self.height.put(rwtxn, &UnitKey, &height)?;
        Ok(())
    }

    pub fn disconnect_tip(
        &self,
        rwtxn: &mut RwTxn,
        header: &Header,
        body: &Body,
    ) -> Result<(), Error> {
        let tip_hash =
            self.tip.try_get(rwtxn, &UnitKey)?.ok_or(Error::NoTip)?;
        if tip_hash != header.hash() {
            let err = error::InvalidHeader::BlockHash {
                expected: tip_hash,
                computed: header.hash(),
            };
            return Err(Error::InvalidHeader(err));
        }
        let merkle_root = body.compute_merkle_root();
        if merkle_root != header.merkle_root {
            let err = Error::InvalidBody {
                expected: header.merkle_root,
                computed: merkle_root,
            };
            return Err(err);
        }
        let height = self
            .try_get_height(rwtxn)?
            .expect("Height should not be None");
        // revert txs, last-to-first
        let mut filled_txs: Vec<FilledTransaction> = Vec::new();
        body.transactions.iter().rev().try_for_each(|tx| {
            let txid = tx.txid();
            let filled_tx =
                self.fill_transaction_from_stxos(rwtxn, tx.clone())?;
            // revert transaction effects
            match &tx.data {
                None => (),
                Some(TxData::AmmBurn { .. }) => {
                    let () =
                        amm::revert_burn(&self.amm_pools, rwtxn, &filled_tx)?;
                }
                Some(TxData::AmmMint { .. }) => {
                    let () =
                        amm::revert_mint(&self.amm_pools, rwtxn, &filled_tx)?;
                }
                Some(TxData::AmmSwap { .. }) => {
                    let () =
                        amm::revert_swap(&self.amm_pools, rwtxn, &filled_tx)?;
                }
                Some(TxData::BitAssetMint(mint_amount)) => {
                    let () = self.bitassets.revert_mint(
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
                    let () = self.bitassets.revert_registration(
                        rwtxn,
                        &filled_tx,
                        BitAssetId(*name_hash),
                    )?;
                }
                Some(TxData::BitAssetReservation { commitment: _ }) => {
                    if !self.bitassets.delete_reservation(rwtxn, &txid)? {
                        let err = error::BitAsset::MissingReservation { txid };
                        return Err(err.into());
                    }
                }
                Some(TxData::BitAssetUpdate(bitasset_updates)) => {
                    let () = self.bitassets.revert_updates(
                        rwtxn,
                        &filled_tx,
                        (**bitasset_updates).clone(),
                        height - 1,
                    )?;
                }
                Some(TxData::DutchAuctionBid { .. }) => {
                    let () = dutch_auction::revert_bid(
                        &self.dutch_auctions,
                        rwtxn,
                        &filled_tx,
                    )?;
                }
                Some(TxData::DutchAuctionCollect { .. }) => {
                    let () = dutch_auction::revert_collect(
                        &self.dutch_auctions,
                        rwtxn,
                        &filled_tx,
                    )?;
                }
                Some(TxData::DutchAuctionCreate(_auction_params)) => {
                    let () = dutch_auction::revert_create(
                        &self.dutch_auctions,
                        rwtxn,
                        &filled_tx,
                    )?;
                }
            }
            filled_txs.push(filled_tx);
            // delete UTXOs, last-to-first
            tx.outputs.iter().enumerate().rev().try_for_each(
                |(vout, _output)| {
                    let outpoint = OutPoint::Regular {
                        txid,
                        vout: vout as u32,
                    };
                    if self.utxos.delete(rwtxn, &outpoint)? {
                        Ok(())
                    } else {
                        Err(Error::NoUtxo { outpoint })
                    }
                },
            )?;
            // unspend STXOs, last-to-first
            tx.inputs.iter().rev().try_for_each(|outpoint| {
                if let Some(spent_output) = self.stxos.get(rwtxn, outpoint)? {
                    self.stxos.delete(rwtxn, outpoint)?;
                    self.utxos.put(rwtxn, outpoint, &spent_output.output)?;
                    Ok(())
                } else {
                    Err(Error::NoStxo {
                        outpoint: *outpoint,
                    })
                }
            })
        })?;
        filled_txs.reverse();
        // delete coinbase UTXOs, last-to-first
        body.coinbase.iter().enumerate().rev().try_for_each(
            |(vout, _output)| {
                let outpoint = OutPoint::Coinbase {
                    merkle_root: header.merkle_root,
                    vout: vout as u32,
                };
                if self.utxos.delete(rwtxn, &outpoint)? {
                    Ok(())
                } else {
                    Err(Error::NoUtxo { outpoint })
                }
            },
        )?;
        match (header.prev_side_hash, height) {
            (None, 0) => {
                self.tip.delete(rwtxn, &UnitKey)?;
                self.height.delete(rwtxn, &UnitKey)?;
            }
            (None, _) | (_, 0) => return Err(Error::NoTip),
            (Some(prev_side_hash), height) => {
                self.tip.put(rwtxn, &UnitKey, &prev_side_hash)?;
                self.height.put(rwtxn, &UnitKey, &(height - 1))?;
            }
        }
        Ok(())
    }

    /// Get total sidechain wealth in Bitcoin
    pub fn sidechain_wealth(
        &self,
        rotxn: &RoTxn,
    ) -> Result<bitcoin::Amount, Error> {
        let mut total_deposit_utxo_value = bitcoin::Amount::ZERO;
        self.utxos.iter(rotxn)?.try_for_each(|utxo| {
            let (outpoint, output) = utxo?;
            if let OutPoint::Deposit(_) = outpoint {
                total_deposit_utxo_value = total_deposit_utxo_value
                    .checked_add(output.get_bitcoin_value())
                    .ok_or(AmountOverflowError)?;
            }
            Ok::<_, Error>(())
        })?;
        let mut total_deposit_stxo_value = bitcoin::Amount::ZERO;
        let mut total_withdrawal_stxo_value = bitcoin::Amount::ZERO;
        self.stxos.iter(rotxn)?.try_for_each(|stxo| {
            let (outpoint, spent_output) = stxo?;
            if let OutPoint::Deposit(_) = outpoint {
                total_deposit_stxo_value = total_deposit_stxo_value
                    .checked_add(spent_output.output.get_bitcoin_value())
                    .ok_or(AmountOverflowError)?;
            }
            if let InPoint::Withdrawal { .. } = spent_output.inpoint {
                total_withdrawal_stxo_value = total_deposit_stxo_value
                    .checked_add(spent_output.output.get_bitcoin_value())
                    .ok_or(AmountOverflowError)?;
            }
            Ok::<_, Error>(())
        })?;

        let total_wealth: bitcoin::Amount = total_deposit_utxo_value
            .checked_add(total_deposit_stxo_value)
            .ok_or(AmountOverflowError)?
            .checked_sub(total_withdrawal_stxo_value)
            .ok_or(AmountOverflowError)?;
        Ok(total_wealth)
    }
}

impl Watchable<()> for State {
    type WatchStream = impl Stream<Item = ()>;

    /// Get a signal that notifies whenever the tip changes
    fn watch(&self) -> Self::WatchStream {
        tokio_stream::wrappers::WatchStream::new(self.tip.watch())
    }
}
