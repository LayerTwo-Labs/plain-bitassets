use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use fallible_iterator::FallibleIterator as _;
use futures::Stream;
use heed::types::SerdeBincode;
use itertools::Itertools;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sneed::{DatabaseUnique, RoDatabaseUnique, RoTxn, RwTxn, UnitKey};

use crate::{
    authorization::Authorization,
    types::{
        Accumulator, Address, AddressOutPointKey, AmountOverflowError,
        Authorized, AuthorizedTransaction, BitAssetId, BlockHash, Body,
        FilledOutput, FilledTransaction, GetAddress as _, GetBitcoinValue as _,
        Header, InPoint, M6id, OutPoint, OutPointKey, SpentOutput, Transaction,
        TxData, VERSION, Verify as _, Version, WithdrawalBundle,
        WithdrawalBundleStatus, proto::mainchain::TwoWayPegData,
    },
    util::Watchable,
};

mod amm;
pub mod bitassets;
mod block;
mod dutch_auction;
pub mod error;
mod rollback;
mod two_way_peg_data;

pub use amm::{AmmPair, PoolState as AmmPoolState};
pub use bitassets::SeqId as BitAssetSeqId;
pub use dutch_auction::DutchAuctionState;
pub use error::Error;
use rollback::{HeightStamped, RollBack};

pub const WITHDRAWAL_BUNDLE_FAILURE_GAP: u32 = 4;

/// Prevalidated block data containing computed values from validation
/// to avoid redundant computation during connection
pub struct PrevalidatedBlock {
    pub filled_transactions: Vec<FilledTransaction>,
    pub computed_merkle_root: crate::types::MerkleRoot,
    pub total_fees: bitcoin::Amount,
    pub coinbase_value: bitcoin::Amount,
    pub next_height: u32, // Precomputed next height to avoid DB read in write txn
    pub accumulator_diff: crate::types::AccumulatorDiff,
}

/// Information we have regarding a withdrawal bundle
#[derive(Debug, Deserialize, Serialize)]
enum WithdrawalBundleInfo {
    /// Withdrawal bundle is known
    Known(WithdrawalBundle),
    /// Withdrawal bundle is unknown but unconfirmed / failed
    Unknown,
    /// If an unknown withdrawal bundle is confirmed, ALL UTXOs are
    /// considered spent.
    UnknownConfirmed {
        spend_utxos: BTreeMap<OutPoint, FilledOutput>,
    },
}

type WithdrawalBundlesDb = DatabaseUnique<
    SerdeBincode<M6id>,
    SerdeBincode<(
        WithdrawalBundleInfo,
        RollBack<HeightStamped<WithdrawalBundleStatus>>,
    )>,
>;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SpentUtxoEntry {
    pub created_height: u32,
    pub output: SpentOutput,
}

#[derive(Clone)]
pub struct State {
    /// Current tip
    tip: DatabaseUnique<UnitKey, SerdeBincode<BlockHash>>,
    /// Current height
    height: DatabaseUnique<UnitKey, SerdeBincode<u32>>,
    /// Associates ordered pairs of BitAssets to their AMM pool states
    amm_pools: amm::PoolsDb,
    bitassets: bitassets::Dbs,
    /// Associates Dutch auction sequence numbers with auction state
    dutch_auctions: dutch_auction::Db,
    utxos: DatabaseUnique<OutPointKey, SerdeBincode<FilledOutput>>,
    utxo_heights_by_address:
        DatabaseUnique<AddressOutPointKey, SerdeBincode<u32>>,
    stxos: DatabaseUnique<OutPointKey, SerdeBincode<SpentUtxoEntry>>,
    /// Pending withdrawal bundle. MUST exist in withdrawal_bundles
    pending_withdrawal_bundle: DatabaseUnique<UnitKey, SerdeBincode<M6id>>,
    /// Latest failed (known) withdrawal bundle
    latest_failed_withdrawal_bundle:
        DatabaseUnique<UnitKey, SerdeBincode<RollBack<HeightStamped<M6id>>>>,
    /// Withdrawal bundles and their status.
    /// Some withdrawal bundles may be unknown.
    /// in which case they are `None`.
    withdrawal_bundles: WithdrawalBundlesDb,
    /// Deposit blocks and the height at which they were applied, keyed sequentially
    deposit_blocks: DatabaseUnique<
        SerdeBincode<u32>,
        SerdeBincode<(bitcoin::BlockHash, u32)>,
    >,
    /// Withdrawal bundle event blocks and the height at which they were applied, keyed sequentially
    withdrawal_bundle_event_blocks: DatabaseUnique<
        SerdeBincode<u32>,
        SerdeBincode<(bitcoin::BlockHash, u32)>,
    >,
    pub utreexo_accumulator: Arc<Mutex<Accumulator>>,
    _version: DatabaseUnique<UnitKey, SerdeBincode<Version>>,
}

impl State {
    pub const NUM_DBS: u32 = bitassets::Dbs::NUM_DBS + 12 + 1;

    pub fn new(
        env: &sneed::Env,
        utreexo_accumulator: Accumulator,
    ) -> Result<Self, Error> {
        let mut rwtxn = env.write_txn()?;
        let tip = DatabaseUnique::create(env, &mut rwtxn, "tip")?;
        let height = DatabaseUnique::create(env, &mut rwtxn, "height")?;
        let amm_pools = DatabaseUnique::create(env, &mut rwtxn, "amm_pools")?;
        let bitassets = bitassets::Dbs::new(env, &mut rwtxn)?;
        let dutch_auctions =
            DatabaseUnique::create(env, &mut rwtxn, "dutch_auctions")?;
        let utxos = DatabaseUnique::create(env, &mut rwtxn, "utxos")?;
        let utxo_heights_by_address =
            DatabaseUnique::create(env, &mut rwtxn, "utxo_heights_by_address")?;
        let stxos = DatabaseUnique::create(env, &mut rwtxn, "stxos")?;
        let pending_withdrawal_bundle = DatabaseUnique::create(
            env,
            &mut rwtxn,
            "pending_withdrawal_bundle",
        )?;
        let latest_failed_withdrawal_bundle = DatabaseUnique::create(
            env,
            &mut rwtxn,
            "latest_failed_withdrawal_bundle",
        )?;
        let withdrawal_bundles =
            DatabaseUnique::create(env, &mut rwtxn, "withdrawal_bundles")?;
        let deposit_blocks =
            DatabaseUnique::create(env, &mut rwtxn, "deposit_blocks")?;
        let withdrawal_bundle_event_blocks = DatabaseUnique::create(
            env,
            &mut rwtxn,
            "withdrawal_bundle_event_blocks",
        )?;
        let version = DatabaseUnique::create(env, &mut rwtxn, "state_version")?;
        if version.try_get(&rwtxn, &())?.is_none() {
            version.put(&mut rwtxn, &(), &*VERSION)?;
        }
        rwtxn.commit()?;
        Ok(Self {
            tip,
            height,
            amm_pools,
            bitassets,
            dutch_auctions,
            utxos,
            utxo_heights_by_address,
            stxos,
            pending_withdrawal_bundle,
            latest_failed_withdrawal_bundle,
            withdrawal_bundles,
            withdrawal_bundle_event_blocks,
            deposit_blocks,
            utreexo_accumulator: Arc::new(Mutex::new(utreexo_accumulator)),
            _version: version,
        })
    }

    pub fn amm_pools(&self) -> &amm::RoPoolsDb {
        &self.amm_pools
    }

    pub fn bitassets(&self) -> &bitassets::Dbs {
        &self.bitassets
    }

    pub fn deposit_blocks(
        &self,
    ) -> &RoDatabaseUnique<
        SerdeBincode<u32>,
        SerdeBincode<(bitcoin::BlockHash, u32)>,
    > {
        &self.deposit_blocks
    }

    pub fn dutch_auctions(&self) -> &dutch_auction::RoDb {
        &self.dutch_auctions
    }

    pub fn stxos(
        &self,
    ) -> &RoDatabaseUnique<OutPointKey, SerdeBincode<SpentUtxoEntry>> {
        &self.stxos
    }

    fn put_utxo(
        &self,
        rwtxn: &mut RwTxn,
        outpoint_key: OutPointKey,
        output: FilledOutput,
        created_height: u32,
    ) -> Result<AddressOutPointKey, sneed::db::Error> {
        let address_key = AddressOutPointKey::new(output.address, outpoint_key);
        self.utxo_heights_by_address.put(
            rwtxn,
            &address_key,
            &created_height,
        )?;
        self.utxos.put(rwtxn, &outpoint_key, &output)?;
        Ok(address_key)
    }

    fn delete_utxo(
        &self,
        rwtxn: &mut RwTxn,
        address_key: &AddressOutPointKey,
    ) -> Result<bool, sneed::db::Error> {
        self.utxo_heights_by_address.delete(rwtxn, &address_key)?;
        Ok(self.utxos.delete(rwtxn, &address_key.outpoint_key())?)
    }

    fn spend_utxo(
        &self,
        rwtxn: &mut RwTxn,
        outpoint_key: OutPointKey,
        spent_output: SpentOutput,
    ) -> Result<bool, sneed::db::Error> {
        let address_key =
            AddressOutPointKey::new(spent_output.output.address, outpoint_key);
        let Some(created_height) =
            self.utxo_heights_by_address.try_get(rwtxn, &address_key)?
        else {
            return Ok(false);
        };

        let entry = SpentUtxoEntry {
            created_height,
            output: spent_output,
        };

        self.stxos.put(rwtxn, &outpoint_key, &entry)?;
        self.delete_utxo(rwtxn, &address_key)
    }

    fn unspend_utxo(
        &self,
        rwtxn: &mut RwTxn,
        outpoint_key: &OutPointKey,
    ) -> Result<bool, sneed::db::Error> {
        let Some(entry) = self.stxos.try_get(rwtxn, outpoint_key)? else {
            return Ok(false);
        };

        self.put_utxo(
            rwtxn,
            *outpoint_key,
            entry.output.output,
            entry.created_height,
        )?;

        Ok(self.stxos.delete(rwtxn, outpoint_key)?)
    }

    pub fn withdrawal_bundle_event_blocks(
        &self,
    ) -> &RoDatabaseUnique<
        SerdeBincode<u32>,
        SerdeBincode<(bitcoin::BlockHash, u32)>,
    > {
        &self.withdrawal_bundle_event_blocks
    }

    pub fn try_get_tip(
        &self,
        rotxn: &RoTxn,
    ) -> Result<Option<BlockHash>, Error> {
        let tip = self.tip.try_get(rotxn, &())?;
        Ok(tip)
    }

    pub fn try_get_height(&self, rotxn: &RoTxn) -> Result<Option<u32>, Error> {
        let height = self.height.try_get(rotxn, &())?;
        Ok(height)
    }

    pub fn get_utxos(
        &self,
        rotxn: &RoTxn,
    ) -> Result<HashMap<OutPoint, FilledOutput>, Error> {
        let utxos: HashMap<OutPoint, FilledOutput> = self
            .utxos
            .iter(rotxn)?
            .map(|(key, output)| Ok((key.to_outpoint(), output)))
            .collect()?;
        Ok(utxos)
    }

    pub fn get_utxos_by_addresses(
        &self,
        rotxn: &RoTxn,
        addresses: &HashSet<Address>,
        height_threshold: u32,
    ) -> Result<HashMap<OutPoint, FilledOutput>, Error> {
        let mut utxos = HashMap::new();
        for address in addresses {
            let start = AddressOutPointKey::start(*address);
            let end = AddressOutPointKey::end(*address);
            let mut iter = self
                .utxo_heights_by_address
                .range(rotxn, &(start..=end))
                .map_err(sneed::db::Error::from)?;
            while let Some((key, created_height)) = iter.next()? {
                if created_height >= height_threshold {
                    let outpoint_key = key.outpoint_key();
                    let outpoint = outpoint_key.to_outpoint();
                    let output = self.utxos.get(rotxn, &outpoint_key)?;
                    utxos.insert(outpoint, output);
                }
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
            self.latest_failed_withdrawal_bundle.try_get(rotxn, &())?
        else {
            return Ok(None);
        };
        let latest_failed_m6id = latest_failed_m6id.latest().value;
        let (_bundle, bundle_status) = self.withdrawal_bundles.try_get(rotxn, &latest_failed_m6id)?
            .unwrap_or_else(||
                panic!("Inconsistent DBs: latest failed m6id {latest_failed_m6id} should exist in withdrawal_bundles")
            );
        let failed_height = bundle_status
            .iter()
            .rev()
            .find_map(|status| match status.value {
                WithdrawalBundleStatus::Failed => Some(status.height),
                WithdrawalBundleStatus::Confirmed
                | WithdrawalBundleStatus::Dropped
                | WithdrawalBundleStatus::Pending
                | WithdrawalBundleStatus::Submitted
                | WithdrawalBundleStatus::SubmittedUnexpected => None,
            })
            .unwrap_or_else(|| {
                panic!("missing failure status for {latest_failed_m6id}")
            });
        Ok(Some((failed_height, latest_failed_m6id)))
    }

    pub fn fill_transaction(
        &self,
        rotxn: &RoTxn,
        transaction: &Transaction,
    ) -> Result<FilledTransaction, Error> {
        let mut spent_utxos = Vec::with_capacity(transaction.inputs.len());
        for input in &transaction.inputs {
            let key = OutPointKey::from_outpoint(input);
            let utxo = self
                .utxos
                .try_get(rotxn, &key)?
                .ok_or(error::NoUtxo { outpoint: *input })?;
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
        let mut spent_utxos = Vec::with_capacity(tx.inputs.len());
        // fill inputs last-to-first
        for (vin, input) in tx.inputs.iter().enumerate().rev() {
            let key = OutPointKey::from_outpoint(input);
            let stxo = self
                .stxos
                .try_get(rotxn, &key)?
                .ok_or(Error::NoStxo { outpoint: *input })?;
            assert_eq!(
                stxo.output.inpoint,
                InPoint::Regular {
                    txid,
                    vin: vin as u32
                }
            );
            spent_utxos.push(stxo.output.output);
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

    /// Get pending withdrawal bundle and block height
    pub fn try_get_pending_withdrawal_bundle(
        &self,
        rotxn: &RoTxn,
    ) -> Result<Option<(WithdrawalBundle, u32)>, Error> {
        let Some(m6id) = self.pending_withdrawal_bundle.try_get(rotxn, &())?
        else {
            return Ok(None);
        };
        let (bundle_info, bundle_status) =
            self.withdrawal_bundles.get(rotxn, &m6id)?;
        let bundle = match bundle_info {
            WithdrawalBundleInfo::Known(bundle) => bundle,
            WithdrawalBundleInfo::Unknown
            | WithdrawalBundleInfo::UnknownConfirmed { spend_utxos: _ } => {
                return Err(error::PendingWithdrawalBundleUnknown(m6id).into());
            }
        };
        let height = bundle_status.latest().height;
        Ok(Some((bundle, height)))
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
        }
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
            revealed_nonce,
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
            let bitasset_id = BitAssetId(*name_hash);
            // A registration must burn the reservation that commits to it,
            // i.e. a spent reservation whose commitment equals
            // keyed_hash(revealed_nonce, name_hash). Without this check,
            // `apply_registration` would later fail to find the reservation
            // to burn.
            {
                let implied_commitment =
                    blake3::keyed_hash(revealed_nonce, name_hash).into();
                let burns_matching_reservation =
                    tx.spent_reservations().any(|(_, filled_output)| {
                        filled_output.reservation_commitment()
                            == Some(&implied_commitment)
                    });
                if !burns_matching_reservation {
                    return Err(
                        error::BitAsset::NoReservationForRegistration {
                            bitasset: bitasset_id,
                        }
                        .into(),
                    );
                }
            }
            if self
                .bitassets
                .try_get_bitasset(rotxn, &bitasset_id)?
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
            if n_unique_bitasset_inputs == 0 && n_bitasset_outputs != 0 {
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
        let fee = tx.bitcoin_fee()?;
        for (outpoint, output) in tx.spent_inputs() {
            // a withdrawal output is committed to a bundle and can only be
            // spent by the bundle, never by a transaction
            if output.content.is_withdrawal() {
                return Err(Error::SpendWithdrawalOutput {
                    outpoint: *outpoint,
                });
            }
        }
        Ok(fee)
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
        let () = Authorization::verify_transaction(transaction)
            .map_err(Error::Authorization)?;
        let fee =
            self.validate_filled_transaction(rotxn, &filled_transaction)?;
        Ok(fee)
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

    /// Get total sidechain wealth in Bitcoin
    pub fn sidechain_wealth(
        &self,
        rotxn: &RoTxn,
    ) -> Result<bitcoin::Amount, Error> {
        let mut total_deposit_utxo_value = bitcoin::Amount::ZERO;
        self.utxos.iter(rotxn)?.map_err(Error::from).for_each(
            |(outpoint_key, output)| {
                let outpoint = outpoint_key.to_outpoint();
                if let OutPoint::Deposit(_) = outpoint {
                    total_deposit_utxo_value = total_deposit_utxo_value
                        .checked_add(output.get_bitcoin_value())
                        .ok_or(AmountOverflowError)?;
                }
                Ok::<_, Error>(())
            },
        )?;
        let mut total_deposit_stxo_value = bitcoin::Amount::ZERO;
        let mut total_withdrawal_stxo_value = bitcoin::Amount::ZERO;
        self.stxos.iter(rotxn)?.map_err(Error::from).for_each(
            |(outpoint_key, spent_entry)| {
                let spent_output = spent_entry.output;
                let outpoint = outpoint_key.to_outpoint();
                if let OutPoint::Deposit(_) = outpoint {
                    total_deposit_stxo_value = total_deposit_stxo_value
                        .checked_add(spent_output.output.get_bitcoin_value())
                        .ok_or(AmountOverflowError)?;
                }
                if let InPoint::Withdrawal { .. } = spent_output.inpoint {
                    total_withdrawal_stxo_value = total_withdrawal_stxo_value
                        .checked_add(spent_output.output.get_bitcoin_value())
                        .ok_or(AmountOverflowError)?;
                }
                Ok::<_, Error>(())
            },
        )?;
        let total_wealth: bitcoin::Amount = total_deposit_utxo_value
            .checked_add(total_deposit_stxo_value)
            .ok_or(AmountOverflowError)?
            .checked_sub(total_withdrawal_stxo_value)
            .ok_or(AmountOverflowError)?;
        Ok(total_wealth)
    }

    pub fn validate_block(
        &self,
        rotxn: &RoTxn,
        header: &Header,
        body: &Body,
    ) -> Result<bitcoin::Amount, Error> {
        block::validate(self, rotxn, header, body)
    }

    pub fn connect_block(
        &self,
        rwtxn: &mut RwTxn,
        header: &Header,
        body: &Body,
    ) -> Result<(), Error> {
        block::connect(self, rwtxn, header, body)
    }

    pub fn disconnect_tip(
        &self,
        rwtxn: &mut RwTxn,
        header: &Header,
        body: &Body,
    ) -> Result<(), Error> {
        block::disconnect_tip(self, rwtxn, header, body)
    }

    pub fn connect_two_way_peg_data(
        &self,
        rwtxn: &mut RwTxn,
        two_way_peg_data: &TwoWayPegData,
    ) -> Result<crate::types::AccumulatorDiff, Error> {
        two_way_peg_data::connect(self, rwtxn, two_way_peg_data)
    }

    pub fn disconnect_two_way_peg_data(
        &self,
        rwtxn: &mut RwTxn,
        two_way_peg_data: &TwoWayPegData,
    ) -> Result<(), Error> {
        two_way_peg_data::disconnect(self, rwtxn, two_way_peg_data)
    }

    pub fn prevalidate_block(
        &self,
        rotxn: &RoTxn,
        header: &Header,
        body: &Body,
    ) -> Result<PrevalidatedBlock, Error> {
        block::prevalidate(self, rotxn, header, body)
    }

    pub fn connect_prevalidated_block(
        &self,
        rwtxn: &mut RwTxn,
        header: &Header,
        body: &Body,
        prevalidated: &PrevalidatedBlock,
    ) -> Result<(), Error> {
        block::connect_prevalidated(self, rwtxn, header, body, prevalidated)
    }

    pub fn apply_block(
        &self,
        rwtxn: &mut RwTxn,
        header: &Header,
        body: &Body,
    ) -> Result<(), Error> {
        let prevalidated = self.prevalidate_block(rwtxn, header, body)?;
        self.connect_prevalidated_block(rwtxn, header, body, &prevalidated)?;
        Ok(())
    }
}

impl Watchable<()> for State {
    type WatchStream = impl Stream<Item = ()>;

    /// Get a signal that notifies whenever the tip changes
    fn watch(&self) -> Self::WatchStream {
        tokio_stream::wrappers::WatchStream::new(self.tip.watch().clone())
    }
}

#[cfg(test)]
mod test {
    use bitcoin::hashes::Hash as _;
    use ed25519_dalek::SigningKey;

    use crate::{
        authorization,
        state::{Error, State, error},
        types::{
            Accumulator, Address, AuthorizedTransaction, BitAssetData,
            BitAssetId, FilledOutput, FilledOutputContent, FilledTransaction,
            Hash, InPoint, OutPoint, OutPointKey, Output, OutputContent,
            SpentOutput, Transaction, TxData, Txid, VerifyingKey,
            WithdrawalOutputContent,
        },
    };

    fn temp_dir(test_name: &str) -> anyhow::Result<temp_dir::TempDir> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let res = temp_dir::TempDir::with_prefix(format!(
            "bitassets-{test_name}-{}-{nanos}",
            std::process::id()
        ))?;
        Ok(res)
    }

    // open a fresh state-backed env in a unique temp dir
    pub fn temp_env(
        test_name: &str,
    ) -> anyhow::Result<(temp_dir::TempDir, sneed::Env)> {
        let temp_dir = temp_dir(test_name)?;
        let mut opts = heed::EnvOpenOptions::new();
        opts.map_size(64 * 1024 * 1024).max_dbs(State::NUM_DBS);
        let env = unsafe { sneed::Env::open(&opts, temp_dir.path()) }?;
        Ok((temp_dir, env))
    }

    pub fn fresh_state(
        test_name: &str,
    ) -> anyhow::Result<(temp_dir::TempDir, sneed::Env, State)> {
        let (temp_dir, env) = temp_env(test_name)?;
        let state = State::new(&env, Accumulator::default())?;
        Ok((temp_dir, env, state))
    }

    /// Create a bitcoin filled output
    pub fn bitcoin_filled_output(address: Address, sats: u64) -> FilledOutput {
        FilledOutput::new_bitcoin_value(
            address,
            bitcoin::Amount::from_sat(sats),
        )
    }

    /// Fund `address` with a single bitcoin UTXO of `value` sats, returning its
    /// outpoint.
    fn fund(
        env: &sneed::Env,
        state: &State,
        address: Address,
        value_sats: u64,
    ) -> OutPoint {
        let outpoint = OutPoint::Regular {
            txid: Default::default(),
            vout: 0,
        };
        let output = bitcoin_filled_output(address, value_sats);
        let mut rwtxn = env.write_txn().unwrap();
        state
            .utxos
            .put(&mut rwtxn, &OutPointKey::from(&outpoint), &output)
            .unwrap();
        rwtxn.commit().unwrap();
        outpoint
    }

    /// Build a BitAsset registration that registers `bitasset_id` with
    /// `revealed_nonce`, while spending a single reservation that commits to
    /// `reservation_commitment`.
    fn registration_tx(
        bitasset_id: BitAssetId,
        revealed_nonce: Hash,
        reservation_commitment: Hash,
        initial_supply: u64,
        bitasset_data: BitAssetData,
    ) -> FilledTransaction {
        let address = Address([0; 20]);
        let mut transaction = Transaction::new(
            vec![OutPoint::Regular {
                txid: Txid([0; 32]),
                vout: 0,
            }],
            vec![
                Output::new(address, OutputContent::BitAsset(initial_supply)),
                Output::new(address, OutputContent::BitAssetControl),
            ],
        );
        transaction.data = Some(TxData::BitAssetRegistration {
            name_hash: bitasset_id.0,
            revealed_nonce,
            bitasset_data: Box::new(bitasset_data),
            initial_supply,
        });
        let reservation = FilledOutput::new(
            address,
            FilledOutputContent::BitAssetReservation(
                Txid([0; 32]),
                reservation_commitment,
            ),
        );
        FilledTransaction {
            transaction,
            spent_utxos: vec![reservation],
        }
    }

    /// A transaction that spends an input without supplying an authorization
    /// for it must be rejected. Otherwise the `zip` of authorizations and
    /// spent UTXOs silently skips the unauthorized input, allowing any UTXO to
    /// be spent without a signature.
    #[test]
    fn validate_transaction_rejects_missing_authorization() -> anyhow::Result<()>
    {
        let (_temp_dir, env, state) = fresh_state("auth_count")?;
        let signing_key = SigningKey::from_bytes(&[1u8; 32]);
        let verifying_key: VerifyingKey = signing_key.verifying_key().into();
        let address = authorization::get_address(&verifying_key);
        let outpoint = fund(&env, &state, address, 1000);

        let transaction = Transaction::new(
            vec![outpoint],
            vec![bitcoin_filled_output(address, 900).into()],
        );

        // The attack: spend the input while providing no authorization for it.
        let unauthorized = AuthorizedTransaction {
            transaction: transaction.clone(),
            authorizations: Vec::new(),
        };
        let rotxn = env.read_txn()?;
        let err = state
            .validate_transaction(&rotxn, &unauthorized)
            .expect_err("tx with no authorizations must be rejected");
        anyhow::ensure!(
            matches!(
                err,
                Error::Authorization(
                    crate::authorization::Error::NotEnoughAuthorizations
                )
            ),
            "unexpected error: {err:?}"
        );

        // The same transaction with a valid authorization is accepted.
        let authorized =
            authorization::authorize(&[(address, &signing_key)], transaction)?;
        state
            .validate_transaction(&rotxn, &authorized)
            .expect("correctly authorized tx should validate");
        Ok(())
    }

    /// A registration whose spent reservation does not commit to the
    /// registered name must be rejected. Otherwise it passes validation and
    /// later panics in `apply_registration`, which fails to find the
    /// reservation to burn.
    #[test]
    fn validate_bitassets_rejects_registration_without_matching_reservation()
    -> anyhow::Result<()> {
        let (_temp_dir, env, state) = fresh_state("registration")?;
        let rotxn = env.read_txn()?;
        let name_hash = [7; 32];
        let bitasset_id = BitAssetId(name_hash);
        let revealed_nonce: Hash = [3; 32];
        let initial_supply = 123;
        let bitasset_data = || BitAssetData::default();
        let implied_commitment: Hash =
            blake3::keyed_hash(&revealed_nonce, &name_hash).into();

        // The reservation commits to something other than the registered name.
        let mismatched_commitment: Hash = [0; 32];
        assert_ne!(mismatched_commitment, implied_commitment);
        let tx = registration_tx(
            bitasset_id,
            revealed_nonce,
            mismatched_commitment,
            initial_supply,
            bitasset_data(),
        );
        let err = state.validate_bitassets(&rotxn, &tx).expect_err(
            "registration without a matching reservation must be rejected",
        );
        anyhow::ensure!(
            matches!(
                err,
                Error::BitAsset(
                    error::BitAsset::NoReservationForRegistration { bitasset }
                ) if bitasset == bitasset_id
            ),
            "unexpected error: {err:?}"
        );

        // The same registration burning the matching reservation is accepted.
        let tx = registration_tx(
            bitasset_id,
            revealed_nonce,
            implied_commitment,
            initial_supply,
            bitasset_data(),
        );
        state.validate_bitassets(&rotxn, &tx).expect(
            "registration burning the matching reservation should validate",
        );
        Ok(())
    }

    #[test]
    fn cannot_spend_withdrawal_output() -> anyhow::Result<()> {
        let (_temp_dir, env, state) =
            fresh_state("cannot-spend-withdrawal-output")?;
        let main_address = {
            let pkh = bitcoin::PubkeyHash::hash(b"test pubkey");
            bitcoin::Address::p2pkh(pkh, bitcoin::NetworkKind::Test)
                .into_unchecked()
        };
        let withdrawal = FilledOutput {
            address: Address::ALL_ZEROS,
            content: FilledOutputContent::BitcoinWithdrawal(
                WithdrawalOutputContent {
                    value: bitcoin::Amount::from_sat(1000),
                    main_fee: bitcoin::Amount::from_sat(300),
                    main_address,
                },
            ),
            memo: Vec::new(),
        };
        let outpoint = OutPoint::Regular {
            txid: [1; 32].into(),
            vout: 0,
        };
        let tx = FilledTransaction {
            transaction: Transaction {
                inputs: vec![outpoint],
                outputs: vec![
                    bitcoin_filled_output(Address::ALL_ZEROS, 1300).into(),
                ],
                ..Default::default()
            },
            spent_utxos: vec![withdrawal],
        };
        let rotxn = env.read_txn()?;
        assert!(matches!(
            state.validate_filled_transaction(&rotxn, &tx),
            Err(crate::state::Error::SpendWithdrawalOutput { .. })
        ));
        Ok(())
    }

    #[test]
    fn sidechain_wealth() -> anyhow::Result<()> {
        use std::str::FromStr;

        use bitcoin::hashes::Hash as _;

        let (_temp_dir, env, state) = fresh_state("sidechain-wealth")?;
        {
            let mut rwtxn = env.write_txn()?;

            // One unspent DEPOSIT UTXO: 50 sats.
            let deposit_utxo_op = OutPoint::Deposit(bitcoin::OutPoint {
                txid: bitcoin::Txid::from_str(
                    "0000000000000000000000000000000000000000000000000000000000000001",
                )?,
                vout: 0,
            });
            state.utxos.put(
                &mut rwtxn,
                &OutPointKey::from(&deposit_utxo_op),
                &bitcoin_filled_output(Address::ALL_ZEROS, 50),
            )?;

            // Two spent DEPOSIT STXOs: 100 + 100 sats.
            for (i, sats) in [(2u8, 100u64), (3u8, 100u64)] {
                let op = OutPoint::Deposit(bitcoin::OutPoint {
                    txid: bitcoin::Txid::from_byte_array([i; 32]),
                    vout: 0,
                });
                let stxo = super::SpentUtxoEntry {
                    created_height: 0,
                    output: SpentOutput {
                        output: bitcoin_filled_output(Address::ALL_ZEROS, sats),
                        inpoint: InPoint::Regular {
                            txid: [i; 32].into(),
                            vin: 0,
                        },
                    },
                };
                state
                    .stxos
                    .put(&mut rwtxn, &OutPointKey::from(&op), &stxo)?;
            }

            // Two WITHDRAWAL STXOs: 10 + 10 sats
            for (i, sats) in [(4u8, 10u64), (5u8, 10u64)] {
                let op = OutPoint::Regular {
                    txid: [i; 32].into(),
                    vout: 0,
                };
                let stxo = super::SpentUtxoEntry {
                    created_height: 0,
                    output: SpentOutput {
                        output: bitcoin_filled_output(Address::ALL_ZEROS, sats),
                        inpoint: InPoint::Withdrawal {
                            m6id: crate::types::M6id(
                                bitcoin::Txid::from_byte_array([i; 32]),
                            ),
                        },
                    },
                };
                state
                    .stxos
                    .put(&mut rwtxn, &OutPointKey::from(&op), &stxo)?;
            }

            rwtxn.commit()?;
        }

        let rotxn = env.read_txn()?;
        let sidechain_wealth = state.sidechain_wealth(&rotxn)?;

        // Correct value: deposit UTXO 50 + deposit STXOs 200 - withdrawal
        // STXOs 20 = 230 sats.
        let expected_sidechain_wealth = bitcoin::Amount::from_sat(230);
        anyhow::ensure!(
            sidechain_wealth == expected_sidechain_wealth,
            "Expected sidechain wealth ({}), but computed ({})",
            expected_sidechain_wealth,
            sidechain_wealth,
        );
        Ok(())
    }

    #[test]
    fn address_utxo_index_stores_height_and_restores_on_unspend()
    -> anyhow::Result<()> {
        use std::collections::HashSet;

        let (_, env, state) = fresh_state(
            "address_utxo_index_stores_height_and_restores_on_unspend",
        )?;
        let address = Address([1; 20]);
        let outpoint = OutPoint::Regular {
            txid: Txid([2; 32]),
            vout: 0,
        };
        let key = OutPointKey::from_outpoint(&outpoint);
        let output = bitcoin_filled_output(address, 42);
        let created_height = 7;
        let addresses = HashSet::from([address]);

        {
            let mut rwtxn = env.write_txn()?;
            let address_key = state.put_utxo(
                &mut rwtxn,
                key,
                output.clone(),
                created_height,
            )?;
            anyhow::ensure!(
                state.utxo_heights_by_address.get(&rwtxn, &address_key)?
                    == created_height
            );
            rwtxn.commit()?;
        }

        {
            let rotxn = env.read_txn()?;
            let indexed =
                state.get_utxos_by_addresses(&rotxn, &addresses, 0)?;
            anyhow::ensure!(indexed.get(&outpoint) == Some(&output));
            let above_height = state.get_utxos_by_addresses(
                &rotxn,
                &addresses,
                created_height + 1,
            )?;
            anyhow::ensure!(above_height.is_empty());
        }

        {
            let mut rwtxn = env.write_txn()?;
            let spent_output = SpentOutput {
                output: output.clone(),
                inpoint: InPoint::Regular {
                    txid: Txid([3; 32]),
                    vin: 0,
                },
            };
            anyhow::ensure!(state.spend_utxo(&mut rwtxn, key, spent_output)?);
            anyhow::ensure!(state.unspend_utxo(&mut rwtxn, &key)?);
            rwtxn.commit()?;
        }

        let rotxn = env.read_txn()?;
        let restored =
            state.get_utxos_by_addresses(&rotxn, &addresses, created_height)?;
        anyhow::ensure!(restored.get(&outpoint) == Some(&output));
        let above_height = state.get_utxos_by_addresses(
            &rotxn,
            &addresses,
            created_height + 1,
        )?;
        anyhow::ensure!(above_height.is_empty());
        Ok(())
    }
}
