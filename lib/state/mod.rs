use std::collections::{BTreeMap, HashMap, HashSet};

use fallible_iterator::FallibleIterator;
use futures::Stream;
use heed::types::SerdeBincode;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sneed::{DatabaseUnique, RoDatabaseUnique, RoTxn, RwTxn, UnitKey};

use crate::{
    authorization::Authorization,
    types::{
        Address, AmountOverflowError, Authorized, AuthorizedTransaction,
        BitAssetId, BlockHash, Body, FilledOutput, FilledTransaction,
        GetAddress as _, GetBitcoinValue as _, Header, InPoint, M6id, OutPoint,
        SpentOutput, Transaction, TxData, VERSION, Verify as _, Version,
        WithdrawalBundle, WithdrawalBundleStatus,
        proto::mainchain::TwoWayPegData,
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

impl WithdrawalBundleInfo {
    fn is_known(&self) -> bool {
        match self {
            Self::Known(_) => true,
            Self::Unknown | Self::UnknownConfirmed { .. } => false,
        }
    }
}

type WithdrawalBundlesDb = DatabaseUnique<
    SerdeBincode<M6id>,
    SerdeBincode<(
        WithdrawalBundleInfo,
        RollBack<HeightStamped<WithdrawalBundleStatus>>,
    )>,
>;

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
    utxos: DatabaseUnique<SerdeBincode<OutPoint>, SerdeBincode<FilledOutput>>,
    stxos: DatabaseUnique<SerdeBincode<OutPoint>, SerdeBincode<SpentOutput>>,
    /// Pending withdrawal bundle and block height
    pending_withdrawal_bundle:
        DatabaseUnique<UnitKey, SerdeBincode<(WithdrawalBundle, u32)>>,
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
    _version: DatabaseUnique<UnitKey, SerdeBincode<Version>>,
}

impl State {
    pub const NUM_DBS: u32 = bitassets::Dbs::NUM_DBS + 12;

    pub fn new(env: &sneed::Env) -> Result<Self, Error> {
        let mut rwtxn = env.write_txn()?;
        let tip = DatabaseUnique::create(env, &mut rwtxn, "tip")?;
        let height = DatabaseUnique::create(env, &mut rwtxn, "height")?;
        let amm_pools = DatabaseUnique::create(env, &mut rwtxn, "amm_pools")?;
        let bitassets = bitassets::Dbs::new(env, &mut rwtxn)?;
        let dutch_auctions =
            DatabaseUnique::create(env, &mut rwtxn, "dutch_auctions")?;
        let utxos = DatabaseUnique::create(env, &mut rwtxn, "utxos")?;
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
            stxos,
            pending_withdrawal_bundle,
            latest_failed_withdrawal_bundle,
            withdrawal_bundles,
            withdrawal_bundle_event_blocks,
            deposit_blocks,
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
    ) -> &RoDatabaseUnique<SerdeBincode<OutPoint>, SerdeBincode<SpentOutput>>
    {
        &self.stxos
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
        let utxos = self.utxos.iter(rotxn)?.collect()?;
        Ok(utxos)
    }

    pub fn get_utxos_by_addresses(
        &self,
        rotxn: &RoTxn,
        addresses: &HashSet<Address>,
    ) -> Result<HashMap<OutPoint, FilledOutput>, Error> {
        let utxos = self
            .utxos
            .iter(rotxn)?
            .filter(|(_, output)| Ok(addresses.contains(&output.address)))
            .collect()?;
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
                .try_get(rotxn, input)?
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
                .try_get(rotxn, input)?
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

    /// Get pending withdrawal bundle and block height
    pub fn get_pending_withdrawal_bundle(
        &self,
        txn: &RoTxn,
    ) -> Result<Option<(WithdrawalBundle, u32)>, Error> {
        Ok(self.pending_withdrawal_bundle.try_get(txn, &())?)
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
            |(outpoint, output)| {
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
            |(outpoint, spent_output)| {
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
    ) -> Result<(), Error> {
        two_way_peg_data::connect(self, rwtxn, two_way_peg_data)
    }

    pub fn disconnect_two_way_peg_data(
        &self,
        rwtxn: &mut RwTxn,
        two_way_peg_data: &TwoWayPegData,
    ) -> Result<(), Error> {
        two_way_peg_data::disconnect(self, rwtxn, two_way_peg_data)
    }
}

impl Watchable<()> for State {
    type WatchStream = impl Stream<Item = ()>;

    /// Get a signal that notifies whenever the tip changes
    fn watch(&self) -> Self::WatchStream {
        tokio_stream::wrappers::WatchStream::new(self.tip.watch().clone())
    }
}
