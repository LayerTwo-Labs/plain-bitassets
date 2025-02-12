//! Functions and types related to BitAssets

use std::net::{SocketAddrV4, SocketAddrV6};

use heed::{types::SerdeBincode, Database, RoTxn, RwTxn};
use serde::{Deserialize, Serialize};

use crate::{
    state::{
        error::BitAsset as Error,
        rollback::{RollBack, TxidStamped},
    },
    types::{
        BitAssetDataUpdates, BitAssetId, EncryptionPubKey, FilledTransaction,
        Hash, Txid, Update, VerifyingKey,
    },
};

/// Representation of BitAsset data that supports rollbacks.
/// The most recent datum is the element at the back of the vector.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BitAssetData {
    /// Commitment to arbitrary data
    pub(in crate::state) commitment: RollBack<TxidStamped<Option<Hash>>>,
    /// Optional ipv4 addr
    pub(in crate::state) socket_addr_v4:
        RollBack<TxidStamped<Option<SocketAddrV4>>>,
    /// Optional ipv6 addr
    pub(in crate::state) socket_addr_v6:
        RollBack<TxidStamped<Option<SocketAddrV6>>>,
    /// Optional pubkey used for encryption
    pub(in crate::state) encryption_pubkey:
        RollBack<TxidStamped<Option<EncryptionPubKey>>>,
    /// Optional pubkey used for signing messages
    pub(in crate::state) signing_pubkey:
        RollBack<TxidStamped<Option<VerifyingKey>>>,
    /// Total supply
    pub(in crate::state) total_supply: RollBack<TxidStamped<u64>>,
}

impl BitAssetData {
    // initialize from BitAsset data provided during a registration
    pub(in crate::state) fn init(
        bitasset_data: crate::types::BitAssetData,
        initial_supply: u64,
        txid: Txid,
        height: u32,
    ) -> Self {
        Self {
            commitment: RollBack::<TxidStamped<_>>::new(
                bitasset_data.commitment,
                txid,
                height,
            ),
            socket_addr_v4: RollBack::<TxidStamped<_>>::new(
                bitasset_data.socket_addr_v4,
                txid,
                height,
            ),
            socket_addr_v6: RollBack::<TxidStamped<_>>::new(
                bitasset_data.socket_addr_v6,
                txid,
                height,
            ),
            encryption_pubkey: RollBack::<TxidStamped<_>>::new(
                bitasset_data.encryption_pubkey,
                txid,
                height,
            ),
            signing_pubkey: RollBack::<TxidStamped<_>>::new(
                bitasset_data.signing_pubkey,
                txid,
                height,
            ),
            total_supply: RollBack::<TxidStamped<_>>::new(
                initial_supply,
                txid,
                height,
            ),
        }
    }

    // apply bitasset data updates
    pub(in crate::state) fn apply_updates(
        &mut self,
        updates: BitAssetDataUpdates,
        txid: Txid,
        height: u32,
    ) {
        let Self {
            ref mut commitment,
            ref mut socket_addr_v4,
            ref mut socket_addr_v6,
            ref mut encryption_pubkey,
            ref mut signing_pubkey,
            total_supply: _,
        } = self;

        // apply an update to a single data field
        fn apply_field_update<T>(
            data_field: &mut RollBack<TxidStamped<Option<T>>>,
            update: Update<T>,
            txid: Txid,
            height: u32,
        ) {
            match update {
                Update::Delete => data_field.push(None, txid, height),
                Update::Retain => (),
                Update::Set(value) => {
                    data_field.push(Some(value), txid, height)
                }
            }
        }
        apply_field_update(commitment, updates.commitment, txid, height);
        apply_field_update(
            socket_addr_v4,
            updates.socket_addr_v4,
            txid,
            height,
        );
        apply_field_update(
            socket_addr_v6,
            updates.socket_addr_v6,
            txid,
            height,
        );
        apply_field_update(
            encryption_pubkey,
            updates.encryption_pubkey,
            txid,
            height,
        );
        apply_field_update(
            signing_pubkey,
            updates.signing_pubkey,
            txid,
            height,
        );
    }

    // revert BitAsset data updates
    pub(in crate::state) fn revert_updates(
        &mut self,
        updates: BitAssetDataUpdates,
        txid: Txid,
        height: u32,
    ) {
        // apply an update to a single data field
        fn revert_field_update<T>(
            data_field: &mut RollBack<TxidStamped<Option<T>>>,
            update: Update<T>,
            txid: Txid,
            height: u32,
        ) where
            T: std::fmt::Debug + Eq,
        {
            match update {
                Update::Delete => {
                    let popped = data_field.pop();
                    assert!(popped.is_some());
                    let popped = popped.unwrap();
                    assert!(popped.data.is_none());
                    assert_eq!(popped.txid, txid);
                    assert_eq!(popped.height, height)
                }
                Update::Retain => (),
                Update::Set(value) => {
                    let popped = data_field.pop();
                    assert!(popped.is_some());
                    let popped = popped.unwrap();
                    assert!(popped.data.is_some());
                    assert_eq!(popped.data.unwrap(), value);
                    assert_eq!(popped.txid, txid);
                    assert_eq!(popped.height, height)
                }
            }
        }

        let Self {
            ref mut commitment,
            ref mut socket_addr_v4,
            ref mut socket_addr_v6,
            ref mut encryption_pubkey,
            ref mut signing_pubkey,
            total_supply: _,
        } = self;
        revert_field_update(
            signing_pubkey,
            updates.signing_pubkey,
            txid,
            height,
        );
        revert_field_update(
            encryption_pubkey,
            updates.encryption_pubkey,
            txid,
            height,
        );
        revert_field_update(
            socket_addr_v6,
            updates.socket_addr_v6,
            txid,
            height,
        );
        revert_field_update(
            socket_addr_v4,
            updates.socket_addr_v4,
            txid,
            height,
        );
        revert_field_update(commitment, updates.commitment, txid, height);
    }

    /** Returns the Bitasset data as it was, at the specified block height.
     *  If a value was updated several times in the block, returns the
     *  last value seen in the block.
     *  Returns `None` if the data did not exist at the specified block
     *  height. */
    pub fn at_block_height(
        &self,
        height: u32,
    ) -> Option<crate::types::BitAssetData> {
        Some(crate::types::BitAssetData {
            commitment: self.commitment.at_block_height(height)?.data,
            socket_addr_v4: self.socket_addr_v4.at_block_height(height)?.data,
            socket_addr_v6: self.socket_addr_v6.at_block_height(height)?.data,
            encryption_pubkey: self
                .encryption_pubkey
                .at_block_height(height)?
                .data,
            signing_pubkey: self.signing_pubkey.at_block_height(height)?.data,
        })
    }

    /// get the current bitasset data
    pub fn current(&self) -> crate::types::BitAssetData {
        crate::types::BitAssetData {
            commitment: self.commitment.latest().data,
            socket_addr_v4: self.socket_addr_v4.latest().data,
            socket_addr_v6: self.socket_addr_v6.latest().data,
            encryption_pubkey: self.encryption_pubkey.latest().data,
            signing_pubkey: self.signing_pubkey.latest().data,
        }
    }
}

/// BitAsset sequence ID
#[derive(
    utoipa::ToSchema,
    Clone,
    Copy,
    Debug,
    Deserialize,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct SeqId(pub u32);

/// BitAsset databases
#[derive(Clone)]
pub struct Dbs {
    /// Associates BitAsset IDs (name hashes) with BitAsset sequence numbers
    bitasset_to_seq: Database<SerdeBincode<BitAssetId>, SerdeBincode<SeqId>>,
    /// Associates BitAsset IDs (name hashes) with BitAsset data
    // TODO: make this read-only
    pub(crate) bitassets:
        Database<SerdeBincode<BitAssetId>, SerdeBincode<BitAssetData>>,
    /// Associates tx hashes with BitAsset reservation commitments
    reservations: Database<SerdeBincode<Txid>, SerdeBincode<Hash>>,
    /// Associates BitAsset sequence numbers with BitAsset IDs (name hashes)
    // TODO: make this read-only
    pub(crate) seq_to_bitasset:
        Database<SerdeBincode<SeqId>, SerdeBincode<BitAssetId>>,
}

impl Dbs {
    pub const NUM_DBS: u32 = 4;

    /// Create / Open DBs. Does not commit the RwTxn.
    pub(in crate::state) fn new(
        env: &heed::Env,
        rwtxn: &mut RwTxn,
    ) -> Result<Self, Error> {
        let bitasset_to_seq =
            env.create_database(rwtxn, Some("bitasset_to_bitasset_seq"))?;
        let bitassets = env.create_database(rwtxn, Some("bitassets"))?;
        let reservations =
            env.create_database(rwtxn, Some("bitasset_reservations"))?;
        let seq_to_bitasset =
            env.create_database(rwtxn, Some("bitasset_seq_to_bitasset"))?;
        Ok(Self {
            reservations,
            seq_to_bitasset,
            bitasset_to_seq,
            bitassets,
        })
    }

    /// The sequence number of the last registered BitAsset.
    /// Returns `None` if no BitAssets have been registered.
    pub(in crate::state) fn last_seq(
        &self,
        rotxn: &RoTxn,
    ) -> Result<Option<SeqId>, Error> {
        match self.seq_to_bitasset.last(rotxn)? {
            Some((seq, _)) => Ok(Some(seq)),
            None => Ok(None),
        }
    }

    /// The sequence number that the next registered BitAsset will take.
    pub(in crate::state) fn next_seq(
        &self,
        rotxn: &RoTxn,
    ) -> Result<SeqId, Error> {
        match self.last_seq(rotxn)? {
            Some(SeqId(seq)) => Ok(SeqId(seq + 1)),
            None => Ok(SeqId(0)),
        }
    }

    /// Return the Bitasset data, if it exists
    pub fn try_get_bitasset(
        &self,
        rotxn: &RoTxn,
        bitasset: &BitAssetId,
    ) -> Result<Option<BitAssetData>, heed::Error> {
        self.bitassets.get(rotxn, bitasset)
    }

    /// Return the Bitasset data. Returns an error if it does not exist.
    pub fn get_bitasset(
        &self,
        rotxn: &RoTxn,
        bitasset: &BitAssetId,
    ) -> Result<BitAssetData, Error> {
        self.try_get_bitasset(rotxn, bitasset)?
            .ok_or(Error::Missing {
                bitasset: *bitasset,
            })
    }

    /// Resolve bitasset data at the specified block height, if it exists.
    pub fn try_get_bitasset_data_at_block_height(
        &self,
        rotxn: &RoTxn,
        bitasset: &BitAssetId,
        height: u32,
    ) -> Result<Option<crate::types::BitAssetData>, heed::Error> {
        let res = self
            .bitassets
            .get(rotxn, bitasset)?
            .and_then(|bitasset_data| bitasset_data.at_block_height(height));
        Ok(res)
    }

    /** Resolve bitasset data at the specified block height.
     * Returns an error if it does not exist. */
    pub fn get_bitasset_data_at_block_height(
        &self,
        rotxn: &RoTxn,
        bitasset: &BitAssetId,
        height: u32,
    ) -> Result<crate::types::BitAssetData, Error> {
        self.get_bitasset(rotxn, bitasset)?
            .at_block_height(height)
            .ok_or(Error::MissingData {
                name_hash: bitasset.0,
                block_height: height,
            })
    }

    /// resolve current bitasset data, if it exists
    pub fn try_get_current_bitasset_data(
        &self,
        rotxn: &RoTxn,
        bitasset: &BitAssetId,
    ) -> Result<Option<crate::types::BitAssetData>, Error> {
        let res = self
            .bitassets
            .get(rotxn, bitasset)?
            .map(|bitasset_data| bitasset_data.current());
        Ok(res)
    }

    /// Resolve current bitasset data. Returns an error if it does not exist.
    pub fn get_current_bitasset_data(
        &self,
        rotxn: &RoTxn,
        bitasset: &BitAssetId,
    ) -> Result<crate::types::BitAssetData, Error> {
        self.try_get_current_bitasset_data(rotxn, bitasset)?.ok_or(
            Error::Missing {
                bitasset: *bitasset,
            },
        )
    }

    /// Delete a BitAsset reservation.
    /// Returns `true` if a BitAsset reservation was deleted.
    pub(in crate::state) fn delete_reservation(
        &self,
        rwtxn: &mut RwTxn,
        txid: &Txid,
    ) -> Result<bool, Error> {
        self.reservations.delete(rwtxn, txid).map_err(Error::Heed)
    }

    /// Store a BitAsset reservation
    pub(in crate::state) fn put_reservation(
        &self,
        rwtxn: &mut RwTxn,
        txid: &Txid,
        commitment: &Hash,
    ) -> Result<(), Error> {
        self.reservations
            .put(rwtxn, txid, commitment)
            .map_err(Error::Heed)
    }

    /// Apply BitAsset updates
    pub(in crate::state) fn apply_updates(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
        bitasset_updates: BitAssetDataUpdates,
        height: u32,
    ) -> Result<(), Error> {
        /* The updated BitAsset is the BitAsset that corresponds to the last
         * bitasset output, or equivalently, the BitAsset corresponding to the
         * last BitAsset input */
        let updated_bitasset = filled_tx
            .spent_bitassets()
            .next_back()
            .ok_or(Error::NoBitAssetsToUpdate)?
            .1
            .bitasset()
            .expect("should only contain BitAsset outputs");
        let mut bitasset_data = self
            .bitassets
            .get(rwtxn, updated_bitasset)?
            .ok_or(Error::Missing {
                bitasset: *updated_bitasset,
            })?;
        bitasset_data.apply_updates(bitasset_updates, filled_tx.txid(), height);
        self.bitassets
            .put(rwtxn, updated_bitasset, &bitasset_data)?;
        Ok(())
    }

    /// Revert BitAsset updates
    pub(in crate::state) fn revert_updates(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
        bitasset_updates: BitAssetDataUpdates,
        height: u32,
    ) -> Result<(), Error> {
        /* The updated BitAsset is the BitAsset that corresponds to the last
         * bitasset output, or equivalently, the BitAsset corresponding to the
         * last BitAsset input */
        let updated_bitasset = filled_tx
            .spent_bitassets()
            .next_back()
            .ok_or(Error::NoBitAssetsToUpdate)?
            .1
            .bitasset()
            .expect("should only contain BitAsset outputs");
        let mut bitasset_data = self
            .bitassets
            .get(rwtxn, updated_bitasset)?
            .ok_or(Error::Missing {
                bitasset: *updated_bitasset,
            })?;
        bitasset_data.revert_updates(
            bitasset_updates,
            filled_tx.txid(),
            height,
        );
        self.bitassets
            .put(rwtxn, updated_bitasset, &bitasset_data)?;
        Ok(())
    }

    /// Apply BitAsset registration
    pub(in crate::state) fn apply_registration(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
        name_hash: Hash,
        bitasset_data: &crate::types::BitAssetData,
        initial_supply: u64,
        height: u32,
    ) -> Result<(), Error> {
        // Find the reservation to burn
        let implied_commitment =
            filled_tx.implied_reservation_commitment().expect(
                "A BitAsset registration tx should have an implied commitment",
            );
        let burned_reservation_txid =
            filled_tx.spent_reservations().find_map(|(_, filled_output)| {
                let (txid, commitment) = filled_output.reservation_data()
                    .expect("A spent reservation should correspond to a commitment");
                if *commitment == implied_commitment {
                    Some(txid)
                } else {
                    None
                }
            }).expect("A BitAsset registration tx should correspond to a burned reservation");
        if !self.reservations.delete(rwtxn, burned_reservation_txid)? {
            return Err(Error::MissingReservation {
                txid: *burned_reservation_txid,
            });
        }
        let bitasset_id = BitAssetId(name_hash);
        // Assign a sequence number
        {
            let seq = self.next_seq(rwtxn)?;
            self.seq_to_bitasset.put(rwtxn, &seq, &bitasset_id)?;
            self.bitasset_to_seq.put(rwtxn, &bitasset_id, &seq)?;
        }
        let bitasset_data = BitAssetData::init(
            bitasset_data.clone(),
            initial_supply,
            filled_tx.txid(),
            height,
        );
        self.bitassets.put(rwtxn, &bitasset_id, &bitasset_data)?;
        Ok(())
    }

    /// Revert BitAsset registration
    pub(in crate::state) fn revert_registration(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
        bitasset: BitAssetId,
    ) -> Result<(), Error> {
        let Some(seq) = self.bitasset_to_seq.get(rwtxn, &bitasset)? else {
            return Err(Error::Missing { bitasset });
        };
        self.bitasset_to_seq.delete(rwtxn, &bitasset)?;
        if !self.seq_to_bitasset.delete(rwtxn, &seq)? {
            return Err(Error::Missing { bitasset });
        }
        if !self.bitassets.delete(rwtxn, &bitasset)? {
            return Err(Error::Missing { bitasset });
        }
        // Find the reservation to restore
        let implied_commitment =
            filled_tx.implied_reservation_commitment().expect(
                "A BitAsset registration tx should have an implied commitment",
            );
        let burned_reservation_txid =
            filled_tx.spent_reservations().find_map(|(_, filled_output)| {
                let (txid, commitment) = filled_output.reservation_data()
                    .expect("A spent reservation should correspond to a commitment");
                if *commitment == implied_commitment {
                    Some(txid)
                } else {
                    None
                }
            }).expect("A BitAsset registration tx should correspond to a burned reservation");
        self.reservations.put(
            rwtxn,
            burned_reservation_txid,
            &implied_commitment,
        )?;
        Ok(())
    }

    /// Apply BitAsset mint
    pub(in crate::state) fn apply_mint(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
        mint_amount: u64,
        height: u32,
    ) -> Result<(), Error> {
        /* The updated BitAsset is the BitAsset that corresponds to the last
         * BitAsset control coin output, or equivalently, the BitAsset corresponding to the
         * last BitAsset control coin input */
        let minted_bitasset = filled_tx
            .spent_bitasset_controls()
            .next_back()
            .ok_or(Error::NoBitAssetsToMint)?
            .1
            .get_bitasset()
            .expect("should only contain BitAsset outputs");
        let mut bitasset_data = self
            .bitassets
            .get(rwtxn, &minted_bitasset)?
            .ok_or(Error::Missing {
                bitasset: minted_bitasset,
            })?;
        let new_total_supply = bitasset_data
            .total_supply
            .0
            .first()
            .data
            .checked_add(mint_amount)
            .ok_or(Error::TotalSupplyOverflow)?;
        bitasset_data.total_supply.push(
            new_total_supply,
            filled_tx.txid(),
            height,
        );
        self.bitassets
            .put(rwtxn, &minted_bitasset, &bitasset_data)?;
        Ok(())
    }

    /// Revert BitAsset mint
    pub(in crate::state) fn revert_mint(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
        mint_amount: u64,
    ) -> Result<(), Error> {
        /* The updated BitAsset is the BitAsset that corresponds to the last
         * BitAsset control coin output, or equivalently, the BitAsset corresponding to the
         * last BitAsset control coin input */
        let minted_bitasset = filled_tx
            .spent_bitasset_controls()
            .next_back()
            .ok_or(Error::NoBitAssetsToMint)?
            .1
            .get_bitasset()
            .expect("should only contain BitAsset outputs");
        let mut bitasset_data = self
            .bitassets
            .get(rwtxn, &minted_bitasset)?
            .ok_or(Error::Missing {
                bitasset: minted_bitasset,
            })?;
        let total_supply = bitasset_data.total_supply.0.first().data;
        let _ = bitasset_data.total_supply.pop();
        let new_total_supply = bitasset_data.total_supply.0.first().data;
        assert_eq!(
            new_total_supply,
            total_supply
                .checked_sub(mint_amount)
                .ok_or(Error::TotalSupplyUnderflow)?
        );
        self.bitassets
            .put(rwtxn, &minted_bitasset, &bitasset_data)?;
        Ok(())
    }
}
