use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::{Ipv4Addr, Ipv6Addr},
};

use futures::Stream;
use heed::{types::SerdeBincode, Database, RoTxn, RwTxn};
use itertools::Itertools;
use nonempty::NonEmpty;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    authorization::{Authorization, VerifyingKey},
    types::{
        self, proto::mainchain::TwoWayPegData, Address, AggregatedWithdrawal,
        AmmBurn, AmmMint, AmmSwap, AmountOverflowError, AmountUnderflowError,
        AssetId, Authorized, AuthorizedTransaction, BitAssetDataUpdates,
        BitAssetId, BlockHash, Body, DutchAuctionBid, DutchAuctionCollect,
        DutchAuctionId, DutchAuctionParams, EncryptionPubKey, FilledOutput,
        FilledOutputContent, FilledTransaction, GetAddress as _,
        GetBitcoinValue as _, Hash, Header, InPoint, M6id, MerkleRoot,
        OutPoint, OutputContent, SpentOutput, Transaction, TxData, Txid,
        Update, Verify as _, WithdrawalBundle, WithdrawalBundleError,
        WithdrawalBundleStatus,
    },
    util::{EnvExt, UnitKey, Watchable, WatchableDb},
};

/// Data of type `T` paired with block height at which it was last updated
#[derive(Clone, Debug, Deserialize, Serialize)]
struct HeightStamped<T> {
    value: T,
    height: u32,
}

/// Data of type `T` paired with
//  * the txid at which it was last updated
//  * block height at which it was last updated
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TxidStamped<T> {
    pub data: T,
    pub txid: Txid,
    pub height: u32,
}

/// Wrapper struct for fields that support rollbacks
#[derive(Clone, Debug, Deserialize, Serialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct RollBack<T>(NonEmpty<T>);

impl<T> RollBack<HeightStamped<T>> {
    fn new(value: T, height: u32) -> Self {
        let height_stamped = HeightStamped { value, height };
        Self(NonEmpty::new(height_stamped))
    }

    /// Pop the most recent value
    fn pop(mut self) -> (Option<Self>, HeightStamped<T>) {
        if let Some(value) = self.0.pop() {
            (Some(self), value)
        } else {
            (None, self.0.head)
        }
    }

    /// Attempt to push a value as the new most recent.
    /// Returns the value if the operation fails.
    fn push(&mut self, value: T, height: u32) -> Result<(), T> {
        if self.0.last().height >= height {
            return Err(value);
        }
        let height_stamped = HeightStamped { value, height };
        self.0.push(height_stamped);
        Ok(())
    }

    /// Returns the earliest value
    fn earliest(&self) -> &HeightStamped<T> {
        self.0.first()
    }

    /// Returns the most recent value
    fn latest(&self) -> &HeightStamped<T> {
        self.0.last()
    }
}

impl<T> RollBack<TxidStamped<T>> {
    fn new(value: T, txid: Txid, height: u32) -> Self {
        let txid_stamped = TxidStamped {
            data: value,
            txid,
            height,
        };
        Self(NonEmpty::new(txid_stamped))
    }

    /// pop the most recent value
    fn pop(&mut self) -> Option<TxidStamped<T>> {
        self.0.pop()
    }

    /// push a value as the new most recent
    fn push(&mut self, value: T, txid: Txid, height: u32) {
        let txid_stamped = TxidStamped {
            data: value,
            txid,
            height,
        };
        self.0.push(txid_stamped)
    }

    /** Returns the value as it was, at the specified block height.
     *  If a value was updated several times in the block, returns the
     *  last value seen in the block. */
    fn at_block_height(&self, height: u32) -> Option<&TxidStamped<T>> {
        self.0
            .iter()
            .rev()
            .find(|txid_stamped| txid_stamped.height <= height)
    }

    /// returns the most recent value, along with it's txid
    pub fn latest(&self) -> &TxidStamped<T> {
        self.0.last()
    }
}

#[derive(
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
    ToSchema,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct BitAssetSeqId(pub u32);

/// Representation of BitAsset data that supports rollbacks.
/// The most recent datum is the element at the back of the vector.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BitAssetData {
    /// Commitment to arbitrary data
    commitment: RollBack<TxidStamped<Option<Hash>>>,
    /// Optional ipv4 addr
    ipv4_addr: RollBack<TxidStamped<Option<Ipv4Addr>>>,
    /// Optional ipv6 addr
    ipv6_addr: RollBack<TxidStamped<Option<Ipv6Addr>>>,
    /// Optional pubkey used for encryption
    encryption_pubkey: RollBack<TxidStamped<Option<EncryptionPubKey>>>,
    /// Optional pubkey used for signing messages
    signing_pubkey: RollBack<TxidStamped<Option<VerifyingKey>>>,
    /// Total supply
    total_supply: RollBack<TxidStamped<u64>>,
}

impl BitAssetData {
    // initialize from BitAsset data provided during a registration
    fn init(
        bitasset_data: types::BitAssetData,
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
            ipv4_addr: RollBack::<TxidStamped<_>>::new(
                bitasset_data.ipv4_addr,
                txid,
                height,
            ),
            ipv6_addr: RollBack::<TxidStamped<_>>::new(
                bitasset_data.ipv6_addr,
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
    fn apply_updates(
        &mut self,
        updates: BitAssetDataUpdates,
        txid: Txid,
        height: u32,
    ) {
        let Self {
            ref mut commitment,
            ref mut ipv4_addr,
            ref mut ipv6_addr,
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
        apply_field_update(ipv4_addr, updates.ipv4_addr, txid, height);
        apply_field_update(ipv6_addr, updates.ipv6_addr, txid, height);
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
    fn revert_updates(
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
            ref mut ipv4_addr,
            ref mut ipv6_addr,
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
        revert_field_update(ipv6_addr, updates.ipv6_addr, txid, height);
        revert_field_update(ipv4_addr, updates.ipv4_addr, txid, height);
        revert_field_update(commitment, updates.commitment, txid, height);
    }

    /** Returns the Bitasset data as it was, at the specified block height.
     *  If a value was updated several times in the block, returns the
     *  last value seen in the block.
     *  Returns `None` if the data did not exist at the specified block
     *  height. */
    pub fn at_block_height(&self, height: u32) -> Option<types::BitAssetData> {
        Some(types::BitAssetData {
            commitment: self.commitment.at_block_height(height)?.data,
            ipv4_addr: self.ipv4_addr.at_block_height(height)?.data,
            ipv6_addr: self.ipv6_addr.at_block_height(height)?.data,
            encryption_pubkey: self
                .encryption_pubkey
                .at_block_height(height)?
                .data,
            signing_pubkey: self.signing_pubkey.at_block_height(height)?.data,
        })
    }

    /// get the current bitasset data
    pub fn current(&self) -> types::BitAssetData {
        types::BitAssetData {
            commitment: self.commitment.latest().data,
            ipv4_addr: self.ipv4_addr.latest().data,
            ipv6_addr: self.ipv6_addr.latest().data,
            encryption_pubkey: self.encryption_pubkey.latest().data,
            signing_pubkey: self.signing_pubkey.latest().data,
        }
    }
}

/// Errors when bidding on a Dutch auction
#[derive(Debug, thiserror::Error)]
pub enum DutchAuctionBidError {
    #[error("Auction has already ended")]
    AuctionEnded,
    #[error("Auction has not started yet")]
    AuctionNotStarted,
    #[error("Incorrect receive asset specified")]
    IncorrectReceiveAsset,
    #[error("Incorrect spend asset")]
    IncorrectSpendAsset,
    #[error("Tx can only be applied at the specified price")]
    InvalidPrice,
    #[error("Invalid TxData")]
    InvalidTxData,
    #[error("Auction not found")]
    MissingAuction,
    #[error("Bid quantity is more than is offered in the auction")]
    QuantityTooLarge,
}

/// Errors when creating a Dutch auction
#[derive(Debug, thiserror::Error)]
pub enum DutchAuctionCreateError {
    #[error("Tx expired; Auction start block already exists")]
    Expired,
    #[error("Invalid tx; Final price cannot be greater than initial price")]
    FinalPrice,
    #[error(
        "Invalid tx; For a single-block auction, 
             final price must be exactly equal to initial price"
    )]
    PriceMismatch,
    #[error("Invalid tx; Auction duration cannot be `0` blocks")]
    ZeroDuration,
}

/// Errors when collecting the proceeds from a Dutch auction
#[derive(Debug, thiserror::Error)]
pub enum DutchAuctionCollectError {
    #[error("Auction has not ended yet")]
    AuctionNotFinished,
    #[error("Incorrect offered asset")]
    IncorrectOfferedAsset,
    #[error(
        "Offered asset amount must be exactly equal to the amount remaining"
    )]
    IncorrectOfferedAssetAmount,
    #[error("Incorrect receive asset specified")]
    IncorrectReceiveAsset,
    #[error(
        "Receive asset amount must be exactly equal to the amount received"
    )]
    IncorrectReceiveAssetAmount,
    #[error("Invalid TxData")]
    InvalidTxData,
    #[error("Auction not found")]
    MissingAuction,
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidHeaderError {
    #[error("expected block hash {expected}, but computed {computed}")]
    BlockHash {
        expected: BlockHash,
        computed: BlockHash,
    },
    #[error("expected previous sidechain block hash {expected}, but received {received}")]
    PrevSideHash {
        expected: BlockHash,
        received: BlockHash,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("AMM burn overflow")]
    AmmBurnOverflow,
    #[error("AMM burn underflow")]
    AmmBurnUnderflow,
    #[error("AMM LP token overflow")]
    AmmLpTokenOverflow,
    #[error("AMM LP token underflow")]
    AmmLpTokenUnderflow,
    #[error("AMM pool invariant")]
    AmmPoolInvariant,
    #[error(transparent)]
    AmountOverflow(#[from] AmountOverflowError),
    #[error(transparent)]
    AmountUnderflow(#[from] AmountUnderflowError),
    #[error("failed to verify authorization")]
    AuthorizationError,
    #[error("bad coinbase output content")]
    BadCoinbaseOutputContent,
    #[error("bitasset {name_hash:?} already registered")]
    BitAssetAlreadyRegistered { name_hash: Hash },
    #[error("bundle too heavy {weight} > {max_weight}")]
    BundleTooHeavy { weight: u64, max_weight: u64 },
    #[error(transparent)]
    BorshSerialize(borsh::io::Error),
    #[error(transparent)]
    DutchAuctionBid(#[from] DutchAuctionBidError),
    #[error(transparent)]
    DutchAuctionCreate(#[from] DutchAuctionCreateError),
    #[error(transparent)]
    DutchAuctionCollect(#[from] DutchAuctionCollectError),
    #[error("failed to fill tx output contents: invalid transaction")]
    FillTxOutputContentsFailed,
    #[error("heed error")]
    Heed(#[from] heed::Error),
    #[error("Insufficient liquidity")]
    InsufficientLiquidity,
    #[error("Invalid AMM burn")]
    InvalidAmmBurn,
    #[error("Invalid AMM mint")]
    InvalidAmmMint,
    #[error("Invalid AMM swap")]
    InvalidAmmSwap,
    #[error("invalid body: expected merkle root {expected}, but computed {computed}")]
    InvalidBody {
        expected: MerkleRoot,
        computed: MerkleRoot,
    },
    #[error("Invalid Dutch auction bid")]
    InvalidDutchAuctionBid,
    #[error("Invalid Dutch auction collect")]
    InvalidDutchAuctionCollect,
    #[error("invalid header: {0}")]
    InvalidHeader(InvalidHeaderError),
    #[error(
        "The last output in a BitAsset registration tx must be a control coin"
    )]
    LastOutputNotControlCoin,
    #[error("missing AMM pool state for {asset0}-{asset1}")]
    MissingAmmPoolState { asset0: AssetId, asset1: AssetId },
    #[error("missing BitAsset {bitasset:?}")]
    MissingBitAsset { bitasset: BitAssetId },
    #[error(
        "Missing BitAsset data for {name_hash:?} at block height {block_height}"
    )]
    MissingBitAssetData { name_hash: Hash, block_height: u32 },
    #[error("missing BitAsset input {name_hash:?}")]
    MissingBitAssetInput { name_hash: Hash },
    #[error("missing Dutch auction {0}")]
    MissingDutchAuction(DutchAuctionId),
    #[error("missing BitAsset reservation {txid}")]
    MissingReservation { txid: Txid },
    #[error("no BitAssets to mint")]
    NoBitAssetsToMint,
    #[error("no BitAssets to update")]
    NoBitAssetsToUpdate,
    #[error("deposit block doesn't exist")]
    NoDepositBlock,
    #[error("total fees less than coinbase value")]
    NotEnoughFees,
    #[error("value in is less than value out")]
    NotEnoughValueIn,
    #[error("stxo {outpoint} doesn't exist")]
    NoStxo { outpoint: OutPoint },
    #[error("utxo {outpoint} doesn't exist")]
    NoUtxo { outpoint: OutPoint },
    #[error("Withdrawal bundle event block doesn't exist")]
    NoWithdrawalBundleEventBlock,
    #[error("Failed to revert AMM mint")]
    RevertAmmMint,
    #[error("Failed to revert AMM swap")]
    RevertAmmSwap,
    #[error("Failed to revert Dutch Auction collect")]
    RevertDutchAuctionCollect,
    #[error(
        "The second-last output in a BitAsset registration tx \
             must be the BitAsset mint, \
             if the initial supply is nonzero"
    )]
    SecondLastOutputNotBitAsset,
    #[error(transparent)]
    SignatureError(#[from] ed25519_dalek::SignatureError),
    #[error("Too few BitAssets to mint an AMM position")]
    TooFewBitAssetsToAmmMint,
    #[error("Too few BitAssets to create a Dutch auction")]
    TooFewBitAssetsToDutchAuctionCreate,
    #[error("Too few BitAsset control coin outputs")]
    TooFewBitAssetControlOutputs,
    #[error("Mint would cause total supply to overflow")]
    TotalSupplyOverflow,
    #[error("Reverting Mint would cause total supply to underflow")]
    TotalSupplyUnderflow,
    #[error(
        "unbalanced BitAsset control coins: \
         {n_bitasset_control_inputs} BitAsset control coin inputs, \
         {n_bitasset_control_outputs} BitAsset control coin outputs"
    )]
    UnbalancedBitAssetControls {
        n_bitasset_control_inputs: usize,
        n_bitasset_control_outputs: usize,
    },
    #[error("unbalanced BitAssets: {n_unique_bitasset_inputs} unique BitAsset inputs, {n_bitasset_outputs} BitAsset outputs")]
    UnbalancedBitAssets {
        n_unique_bitasset_inputs: usize,
        n_bitasset_outputs: usize,
    },
    #[error("unbalanced reservations: {n_reservation_inputs} reservation inputs, {n_reservation_outputs} reservation outputs")]
    UnbalancedReservations {
        n_reservation_inputs: usize,
        n_reservation_outputs: usize,
    },
    #[error("Unknown withdrawal bundle: {m6id}")]
    UnknownWithdrawalBundle { m6id: M6id },
    #[error("utxo double spent")]
    UtxoDoubleSpent,
    #[error(transparent)]
    WithdrawalBundle(#[from] WithdrawalBundleError),
    #[error("wrong public key for address")]
    WrongPubKeyForAddress,
}

/// Ordered pair of [`AssetId`]s
#[derive(Clone, Copy, Debug, Serialize)]
pub struct AmmPair(AssetId, AssetId);

impl AmmPair {
    pub fn new(asset0: AssetId, asset1: AssetId) -> Self {
        if asset0 <= asset1 {
            Self(asset0, asset1)
        } else {
            Self(asset1, asset0)
        }
    }

    /// Returns the lower [`AssetId`]
    pub fn asset0(&self) -> AssetId {
        self.0
    }

    /// Returns the greater [`AssetId`]
    pub fn asset1(&self) -> AssetId {
        self.1
    }
}

/// Current state of an AMM pool
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema,
)]
pub struct AmmPoolState {
    /// Reserve of the first asset
    pub reserve0: u64,
    /// Reserve of the second asset
    pub reserve1: u64,
    /// Total amount of outstanding LP tokens
    pub outstanding_lp_tokens: u64,
    /// tx that created the pool
    creation_txid: Txid,
}

impl AmmPoolState {
    fn new(txid: Txid) -> Self {
        Self {
            reserve0: 0,
            reserve1: 0,
            outstanding_lp_tokens: 0,
            creation_txid: txid,
        }
    }

    /// Returns the new pool state after minting a position
    pub fn mint(&self, amount0: u64, amount1: u64) -> Result<Self, Error> {
        // Geometric mean of two [`u64`]s
        fn geometric_mean(x: u64, y: u64) -> u64 {
            num::integer::sqrt(x as u128 * y as u128)
            // u64 truncation of u128 square root is always safe
            as u64
        }
        let AmmPoolState {
            reserve0,
            reserve1,
            outstanding_lp_tokens,
            creation_txid,
        } = self;
        let new_reserve0 =
            reserve0.checked_add(amount0).ok_or(Error::InvalidAmmMint)?;
        let new_reserve1 =
            reserve1.checked_add(amount1).ok_or(Error::InvalidAmmMint)?;
        if *reserve0 == 0 || *reserve1 == 0 || *outstanding_lp_tokens == 0 {
            let lp_tokens_minted = geometric_mean(new_reserve0, new_reserve1);
            let new_outstanding_lp_tokens =
                outstanding_lp_tokens + lp_tokens_minted;
            Ok(AmmPoolState {
                reserve0: new_reserve0,
                reserve1: new_reserve1,
                outstanding_lp_tokens: new_outstanding_lp_tokens,
                creation_txid: *creation_txid,
            })
        } else {
            // LP tokens minted based on asset 0
            let lp_tokens_minted_0: u128 = (*outstanding_lp_tokens as u128
                * amount0 as u128)
                / *reserve0 as u128;
            // LP tokens minted based on asset 1
            let lp_tokens_minted_1: u128 = (*outstanding_lp_tokens as u128
                * amount1 as u128)
                / *reserve1 as u128;
            // LP tokens minted is the minimum of the two calculations
            let lp_tokens_minted: u64 =
                u128::min(lp_tokens_minted_0, lp_tokens_minted_1)
                    .try_into()
                    .map_err(|_| Error::AmmLpTokenOverflow)?;
            let new_outstanding_lp_tokens = outstanding_lp_tokens
                .checked_add(lp_tokens_minted)
                .ok_or(Error::AmmLpTokenOverflow)?;
            Ok(AmmPoolState {
                reserve0: new_reserve0,
                reserve1: new_reserve1,
                outstanding_lp_tokens: new_outstanding_lp_tokens,
                creation_txid: *creation_txid,
            })
        }
    }

    /// Returns the new pool state after reverting a mint.
    fn revert_mint(
        &self,
        amount0: u64,
        amount1: u64,
        lp_tokens_minted: u64,
    ) -> Result<Self, Error> {
        let new_reserve0 = self
            .reserve0
            .checked_sub(amount0)
            .ok_or(Error::InvalidAmmMint)?;
        let new_reserve1 = self
            .reserve1
            .checked_sub(amount1)
            .ok_or(Error::InvalidAmmMint)?;
        let new_outstanding_lp_tokens = self
            .outstanding_lp_tokens
            .checked_sub(lp_tokens_minted)
            .ok_or(Error::AmmLpTokenUnderflow)?;
        let new_state = Self {
            reserve0: new_reserve0,
            reserve1: new_reserve1,
            outstanding_lp_tokens: new_outstanding_lp_tokens,
            creation_txid: self.creation_txid,
        };
        if *self == new_state.mint(amount0, amount1)? {
            Ok(new_state)
        } else {
            Err(Error::RevertAmmMint)
        }
    }

    /// Returns the new pool state after burning a position
    pub fn burn(&self, lp_token_burn: u64) -> Result<Self, Error> {
        let AmmPoolState {
            reserve0,
            reserve1,
            outstanding_lp_tokens,
            creation_txid,
        } = self;
        if *outstanding_lp_tokens == 0 {
            do yeet Error::InvalidAmmBurn
        };
        // compute payout based on either asset
        let payout = |reserve: u64| -> Result<u64, Error> {
            let payout: u128 = (reserve as u128 * lp_token_burn as u128)
                / (*outstanding_lp_tokens as u128);
            payout.try_into().map_err(|_| Error::AmmBurnOverflow)
        };
        // payout in asset 0
        let payout_0 = payout(*reserve0)?;
        // payout in asset 1
        let payout_1 = payout(*reserve1)?;
        let new_reserve0 = reserve0
            .checked_sub(payout_0)
            .ok_or(Error::AmmBurnUnderflow)?;
        let new_reserve1 = reserve1
            .checked_sub(payout_1)
            .ok_or(Error::AmmBurnUnderflow)?;
        let new_outstanding_lp_tokens = outstanding_lp_tokens
            .checked_sub(lp_token_burn)
            .ok_or(Error::AmmBurnUnderflow)?;
        Ok(AmmPoolState {
            reserve0: new_reserve0,
            reserve1: new_reserve1,
            outstanding_lp_tokens: new_outstanding_lp_tokens,
            creation_txid: *creation_txid,
        })
    }

    /// Returns the new pool state after reverting a burn
    pub fn unburn(
        &self,
        lp_token_unburn: u64,
        payout_0: u64,
        payout_1: u64,
    ) -> Result<Self, Error> {
        let AmmPoolState {
            reserve0,
            reserve1,
            outstanding_lp_tokens,
            creation_txid,
        } = self;
        let new_outstanding_lp_tokens = outstanding_lp_tokens
            .checked_add(lp_token_unburn)
            .ok_or(Error::AmmBurnOverflow)?;
        let new_reserve0 = reserve0
            .checked_add(payout_0)
            .ok_or(Error::AmmBurnOverflow)?;
        let new_reserve1 = reserve1
            .checked_add(payout_1)
            .ok_or(Error::AmmBurnOverflow)?;
        Ok(AmmPoolState {
            reserve0: new_reserve0,
            reserve1: new_reserve1,
            outstanding_lp_tokens: new_outstanding_lp_tokens,
            creation_txid: *creation_txid,
        })
    }

    /// Returns the new pool state after a swap
    pub fn swap_asset0_for_asset1(
        &self,
        amount_spend: u64,
    ) -> Result<Self, Error> {
        let AmmPoolState {
            reserve0,
            reserve1,
            outstanding_lp_tokens,
            creation_txid,
        } = self;
        let reserve_product: u128 = *reserve0 as u128 * *reserve1 as u128;
        let spend_after_fee = ((amount_spend as u128 * 997) / 1000) as u64;
        let _spend_fee = amount_spend
            .checked_sub(spend_after_fee)
            .ok_or(Error::InvalidAmmSwap)?;

        // used for computing product for swap price
        let effective_spend_asset_reserve = reserve0 + spend_after_fee;
        let new_receive_asset_reserve_before_fee: u64 = reserve_product
            .div_ceil(effective_spend_asset_reserve as u128)
            .try_into()
            .map_err(|_| Error::InvalidAmmSwap)?;
        let amount_receive_before_fee: u64 = reserve1
            .checked_sub(new_receive_asset_reserve_before_fee)
            .ok_or(Error::InvalidAmmSwap)?;
        let amount_receive_after_fee =
            ((amount_receive_before_fee as u128 * 997) / 1000) as u64;
        let _receive_fee = amount_receive_before_fee
            .checked_sub(amount_receive_before_fee)
            .ok_or(Error::InvalidAmmSwap)?;
        let (new_reserve0, new_reserve1) = {
            let new_reserve1 = reserve1
                .checked_sub(amount_receive_after_fee)
                .ok_or(Error::InsufficientLiquidity)?;
            (reserve0 + amount_spend, new_reserve1)
        };
        Ok(AmmPoolState {
            reserve0: new_reserve0,
            reserve1: new_reserve1,
            outstanding_lp_tokens: *outstanding_lp_tokens,
            creation_txid: *creation_txid,
        })
    }

    /// Returns the new pool state after a swap
    pub fn swap_asset1_for_asset0(
        &self,
        amount_spend: u64,
    ) -> Result<Self, Error> {
        let AmmPoolState {
            reserve0,
            reserve1,
            outstanding_lp_tokens,
            creation_txid,
        } = self;
        let reserve_product: u128 = *reserve0 as u128 * *reserve1 as u128;
        let spend_after_fee = ((amount_spend as u128 * 997) / 1000) as u64;
        let _spend_fee = amount_spend
            .checked_sub(spend_after_fee)
            .ok_or(Error::InvalidAmmSwap)?;
        // used for computing product for swap price
        let effective_spend_asset_reserve = reserve1 + spend_after_fee;
        let new_receive_asset_reserve_before_fee: u64 = reserve_product
            .div_ceil(effective_spend_asset_reserve as u128)
            .try_into()
            .map_err(|_| Error::InvalidAmmSwap)?;
        let amount_receive_before_fee: u64 = reserve0
            .checked_sub(new_receive_asset_reserve_before_fee)
            .ok_or(Error::InvalidAmmSwap)?;
        let amount_receive_after_fee =
            ((amount_receive_before_fee as u128 * 997) / 1000) as u64;
        let _receive_fee = amount_receive_before_fee
            .checked_sub(amount_receive_before_fee)
            .ok_or(Error::InvalidAmmSwap)?;
        let (new_reserve0, new_reserve1) = {
            let new_reserve0 = reserve0
                .checked_sub(amount_receive_after_fee)
                .ok_or(Error::InsufficientLiquidity)?;
            (new_reserve0, reserve1 + amount_spend)
        };
        Ok(AmmPoolState {
            reserve0: new_reserve0,
            reserve1: new_reserve1,
            outstanding_lp_tokens: *outstanding_lp_tokens,
            creation_txid: *creation_txid,
        })
    }

    /// Returns the pool state after reverting a swap
    fn revert_swap(&self, swap: AmmSwap) -> Result<Self, Error> {
        let amm_pair = AmmPair::new(swap.asset_receive, swap.asset_spend);
        let new_reserve0;
        let new_reserve1;
        if swap.asset_spend == amm_pair.asset1() {
            new_reserve0 = self.reserve0 + swap.amount_receive;
            new_reserve1 = self.reserve1 - swap.amount_spend;
        } else {
            new_reserve0 = self.reserve0 - swap.amount_spend;
            new_reserve1 = self.reserve1 + swap.amount_receive;
        }
        let new_state = Self {
            reserve0: new_reserve0,
            reserve1: new_reserve1,
            outstanding_lp_tokens: self.outstanding_lp_tokens,
            creation_txid: self.creation_txid,
        };
        // apply the swap again to see if the reverted state is correct
        let check_state = if swap.asset_spend == amm_pair.asset0() {
            new_state.swap_asset0_for_asset1(swap.amount_spend)?
        } else {
            new_state.swap_asset1_for_asset0(swap.amount_spend)?
        };
        if check_state == *self {
            Ok(new_state)
        } else {
            Err(Error::RevertAmmSwap)
        }
    }
}

/// Parameters of a Dutch Auction
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DutchAuctionState {
    /// Block height at which the auction starts
    pub start_block: u32,
    /// Block height at the most recent bid
    pub most_recent_bid_block: RollBack<TxidStamped<u32>>,
    /// Auction duration, in blocks
    pub duration: u32,
    /// The asset to be auctioned
    pub base_asset: AssetId,
    /// The initial amount of base asset to be auctioned
    pub initial_base_amount: u64,
    /// The remaining amount of the base asset to be auctioned
    pub base_amount_remaining: RollBack<TxidStamped<u64>>,
    /// The asset in which the auction is to be quoted
    pub quote_asset: AssetId,
    /// The amount of the quote asset that has been received
    pub quote_amount: RollBack<TxidStamped<u64>>,
    /// Initial price
    pub initial_price: u64,
    /// Price immediately after the most recent bid
    pub price_after_most_recent_bid: RollBack<TxidStamped<u64>>,
    /// End price as initially specified
    pub initial_end_price: u64,
    /// End price after the most recent bid
    pub end_price_after_most_recent_bid: RollBack<TxidStamped<u64>>,
}

impl DutchAuctionState {
    /// Returns the new auction state after a bid
    pub fn bid(
        &self,
        txid: Txid,
        bid_amount: u64,
        height: u32,
    ) -> Result<Self, Error> {
        let DutchAuctionState {
            start_block,
            most_recent_bid_block,
            duration,
            base_asset: _,
            initial_base_amount: _,
            base_amount_remaining,
            quote_asset: _,
            quote_amount,
            initial_price: _,
            price_after_most_recent_bid,
            initial_end_price: _,
            end_price_after_most_recent_bid,
        } = self;
        if height < *start_block {
            do yeet DutchAuctionBidError::AuctionNotStarted
        };
        // Blocks elapsed since last bid
        let elapsed_blocks = height - most_recent_bid_block.latest().data;
        let end_block = start_block.saturating_add(*duration - 1);
        if height > end_block {
            do yeet DutchAuctionBidError::AuctionEnded
        };
        let remaining_duration_at_most_recent_bid =
            end_block - most_recent_bid_block.latest().data;
        // Calculate current price
        let price = if remaining_duration_at_most_recent_bid == 0 {
            price_after_most_recent_bid.latest().data
        } else {
            let price_decrease: u128 = {
                /* ((price_after_most_recent_bid
                 * - end_price_after_most_recent_bid)
                 * / (remaining_duration_at_most_recent_bid)) * elapsed
                 * == ((price_after_most_recent_bid
                 * - end_price_after_most_recent_bid) * elapsed)
                 * / (remaining_duration_at_most_recent_bid) */
                let max_price_decrease =
                    (price_after_most_recent_bid.latest().data
                        - end_price_after_most_recent_bid.latest().data)
                        as u128;
                (max_price_decrease * elapsed_blocks as u128)
                    / (remaining_duration_at_most_recent_bid as u128)
            };
            // This is safe, as `(elapsed_blocks / remaining_duration_at_most_recent_bid) < 1`
            let price_decrease = {
                assert!(
                    elapsed_blocks / remaining_duration_at_most_recent_bid < 1
                );
                price_decrease as u64
            };
            price_after_most_recent_bid.latest().data - price_decrease
        };
        if price == 0 {
            do yeet DutchAuctionBidError::InvalidPrice
        };
        // Calculate order quantity for this bid, in terms of the base
        let order_quantity: u128 = {
            /* bid_amount / (price / base_amount_remaining)
             * == (bid_amount * base_amount_remaining) / price */
            (bid_amount as u128 * base_amount_remaining.latest().data as u128)
                .div_ceil(price as u128)
        };
        let order_quantity: u64 =
            if order_quantity <= base_amount_remaining.latest().data as u128 {
                order_quantity as u64
            } else {
                do yeet DutchAuctionBidError::QuantityTooLarge
            };
        let new_base_amount_remaining =
            base_amount_remaining.latest().data - order_quantity;
        let end_price = {
            /* Truncation to `u64` here is safe as
            `(new_base_amount_remaining / base_amount_remaining) < 1` */
            (end_price_after_most_recent_bid.latest().data as u128
                * new_base_amount_remaining as u128)
                .div_ceil(base_amount_remaining.latest().data as u128)
                as u64
        };
        let mut most_recent_bid_block = most_recent_bid_block.clone();
        most_recent_bid_block.push(height, txid, height);
        let mut base_amount_remaining = base_amount_remaining.clone();
        base_amount_remaining.push(new_base_amount_remaining, txid, height);
        let mut quote_amount = quote_amount.clone();
        quote_amount.push(
            quote_amount.latest().data + bid_amount,
            txid,
            height,
        );
        let mut price_after_most_recent_bid =
            price_after_most_recent_bid.clone();
        price_after_most_recent_bid.push(price - bid_amount, txid, height);
        let mut end_price_after_most_recent_bid =
            end_price_after_most_recent_bid.clone();
        end_price_after_most_recent_bid.push(end_price, txid, height);
        Ok(Self {
            most_recent_bid_block,
            base_amount_remaining,
            quote_amount,
            price_after_most_recent_bid,
            end_price_after_most_recent_bid,
            ..*self
        })
    }

    /// Returns the dutch auction state after reverting a bid
    fn revert_bid(&self, txid: Txid) -> Result<Self, Error> {
        let mut most_recent_bid_block = self.most_recent_bid_block.clone();
        assert!(most_recent_bid_block
            .pop()
            .is_some_and(|ts| ts.txid == txid));
        let mut base_amount_remaining = self.base_amount_remaining.clone();
        assert!(base_amount_remaining
            .pop()
            .is_some_and(|ts| ts.txid == txid));
        let mut quote_amount = self.quote_amount.clone();
        assert!(quote_amount.pop().is_some_and(|ts| ts.txid == txid));
        let mut price_after_most_recent_bid =
            self.price_after_most_recent_bid.clone();
        assert!(price_after_most_recent_bid
            .pop()
            .is_some_and(|ts| ts.txid == txid));
        let mut end_price_after_most_recent_bid =
            self.end_price_after_most_recent_bid.clone();
        assert!(end_price_after_most_recent_bid
            .pop()
            .is_some_and(|ts| ts.txid == txid));
        Ok(Self {
            most_recent_bid_block,
            base_amount_remaining,
            quote_amount,
            price_after_most_recent_bid,
            end_price_after_most_recent_bid,
            ..*self
        })
    }
}

type WithdrawalBundlesDb = Database<
    SerdeBincode<M6id>,
    SerdeBincode<(
        WithdrawalBundle,
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
    pub amm_pools: Database<SerdeBincode<AmmPair>, SerdeBincode<AmmPoolState>>,
    /// Associates tx hashes with BitAsset reservation commitments
    pub bitasset_reservations: Database<SerdeBincode<Txid>, SerdeBincode<Hash>>,
    /// Associates BitAsset sequence numbers with BitAsset IDs (name hashes)
    pub bitasset_seq_to_bitasset:
        Database<SerdeBincode<BitAssetSeqId>, SerdeBincode<BitAssetId>>,
    /// Associates BitAsset IDs (name hashes) with BitAsset sequence numbers
    pub bitasset_to_bitasset_seq:
        Database<SerdeBincode<BitAssetId>, SerdeBincode<BitAssetSeqId>>,
    /// Associates BitAsset IDs (name hashes) with BitAsset data
    pub bitassets:
        Database<SerdeBincode<BitAssetId>, SerdeBincode<BitAssetData>>,
    /// Associates Dutch auction sequence numbers with auction state
    pub dutch_auctions:
        Database<SerdeBincode<DutchAuctionId>, SerdeBincode<DutchAuctionState>>,
    pub utxos: Database<SerdeBincode<OutPoint>, SerdeBincode<FilledOutput>>,
    pub stxos: Database<SerdeBincode<OutPoint>, SerdeBincode<SpentOutput>>,
    /// Pending withdrawal bundle and block height
    pub pending_withdrawal_bundle:
        Database<SerdeBincode<UnitKey>, SerdeBincode<(WithdrawalBundle, u32)>>,
    latest_failed_withdrawal_bundle: Database<
        SerdeBincode<UnitKey>,
        SerdeBincode<RollBack<HeightStamped<M6id>>>,
    >,
    /// Mapping from block height to withdrawal bundle and status
    withdrawal_bundles: WithdrawalBundlesDb,
    /// deposit blocks and the height at which they were applied, keyed sequentially
    pub deposit_blocks:
        Database<SerdeBincode<u32>, SerdeBincode<(bitcoin::BlockHash, u32)>>,
    /// withdrawal bundle event blocks and the height at which they were applied, keyed sequentially
    pub withdrawal_bundle_event_blocks:
        Database<SerdeBincode<u32>, SerdeBincode<(bitcoin::BlockHash, u32)>>,
}

impl State {
    pub const NUM_DBS: u32 = 15;
    pub const WITHDRAWAL_BUNDLE_FAILURE_GAP: u32 = 5;

    pub fn new(env: &heed::Env) -> Result<Self, Error> {
        let mut rwtxn = env.write_txn()?;
        let tip = env.create_watchable_db(&mut rwtxn, "tip")?;
        let height = env.create_database(&mut rwtxn, Some("height"))?;
        let amm_pools = env.create_database(&mut rwtxn, Some("amm_pools"))?;
        let bitasset_reservations =
            env.create_database(&mut rwtxn, Some("bitasset_reservations"))?;
        let bitasset_seq_to_bitasset =
            env.create_database(&mut rwtxn, Some("bitasset_seq_to_bitasset"))?;
        let bitasset_to_bitasset_seq =
            env.create_database(&mut rwtxn, Some("bitasset_to_bitasset_seq"))?;
        let bitassets = env.create_database(&mut rwtxn, Some("bitassets"))?;
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
            bitasset_reservations,
            bitasset_seq_to_bitasset,
            bitasset_to_bitasset_seq,
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

    pub fn get_tip(&self, rotxn: &RoTxn) -> Result<BlockHash, Error> {
        let tip = self.tip.try_get(rotxn, &UnitKey)?.unwrap_or_default();
        Ok(tip)
    }

    pub fn get_height(&self, rotxn: &RoTxn) -> Result<u32, Error> {
        let height = self.height.get(rotxn, &UnitKey)?.unwrap_or_default();
        Ok(height)
    }

    /** The sequence number of the last registered BitAsset.
     * Returns `None` if no BitAssets have been registered. */
    pub fn last_bitasset_seq(
        &self,
        txn: &RoTxn,
    ) -> Result<Option<BitAssetSeqId>, Error> {
        match self.bitasset_seq_to_bitasset.last(txn)? {
            Some((seq, _)) => Ok(Some(seq)),
            None => Ok(None),
        }
    }

    /// The sequence number that the next registered BitAsset will take.
    pub fn next_bitasset_seq(
        &self,
        txn: &RoTxn,
    ) -> Result<BitAssetSeqId, Error> {
        match self.last_bitasset_seq(txn)? {
            Some(BitAssetSeqId(seq)) => Ok(BitAssetSeqId(seq + 1)),
            None => Ok(BitAssetSeqId(0)),
        }
    }

    /// Return the Bitasset data. Returns an error if it does not exist.
    fn get_bitasset(
        &self,
        txn: &RoTxn,
        bitasset: &BitAssetId,
    ) -> Result<BitAssetData, Error> {
        self.bitassets
            .get(txn, bitasset)?
            .ok_or(Error::MissingBitAsset {
                bitasset: *bitasset,
            })
    }

    /// Resolve bitasset data at the specified block height, if it exists.
    pub fn try_get_bitasset_data_at_block_height(
        &self,
        txn: &RoTxn,
        bitasset: &BitAssetId,
        height: u32,
    ) -> Result<Option<types::BitAssetData>, heed::Error> {
        let res = self
            .bitassets
            .get(txn, bitasset)?
            .and_then(|bitasset_data| bitasset_data.at_block_height(height));
        Ok(res)
    }

    /** Resolve bitasset data at the specified block height.
     * Returns an error if it does not exist. */
    pub fn get_bitasset_data_at_block_height(
        &self,
        txn: &RoTxn,
        bitasset: &BitAssetId,
        height: u32,
    ) -> Result<types::BitAssetData, Error> {
        self.get_bitasset(txn, bitasset)?
            .at_block_height(height)
            .ok_or(Error::MissingBitAssetData {
                name_hash: bitasset.0,
                block_height: height,
            })
    }

    /// resolve current bitasset data, if it exists
    pub fn try_get_current_bitasset_data(
        &self,
        txn: &RoTxn,
        bitasset: &BitAssetId,
    ) -> Result<Option<types::BitAssetData>, heed::Error> {
        let res = self
            .bitassets
            .get(txn, bitasset)?
            .map(|bitasset_data| bitasset_data.current());
        Ok(res)
    }

    /// Resolve current bitasset data. Returns an error if it does not exist.
    pub fn get_current_bitasset_data(
        &self,
        txn: &RoTxn,
        bitasset: &BitAssetId,
    ) -> Result<types::BitAssetData, Error> {
        self.try_get_current_bitasset_data(txn, bitasset)?.ok_or(
            Error::MissingBitAsset {
                bitasset: *bitasset,
            },
        )
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
    fn get_latest_failed_withdrawal_bundle(
        &self,
        rotxn: &RoTxn,
    ) -> Result<Option<(u32, WithdrawalBundle)>, Error> {
        let Some(latest_failed_m6id) =
            self.latest_failed_withdrawal_bundle.get(rotxn, &UnitKey)?
        else {
            return Ok(None);
        };
        let latest_failed_m6id = latest_failed_m6id.latest().value;
        let (bundle, bundle_status) = self.withdrawal_bundles.get(rotxn, &latest_failed_m6id)?
            .expect("Inconsistent DBs: latest failed m6id should exist in withdrawal_bundles");
        let bundle_status = bundle_status.latest();
        assert_eq!(bundle_status.value, WithdrawalBundleStatus::Failed);
        Ok(Some((bundle_status.height, bundle)))
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
                // Set maximum mainchain fee.
                if withdrawal.main_fee > aggregated.main_fee {
                    aggregated.main_fee = withdrawal.main_fee;
                }
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
            return Err(Error::NoBitAssetsToUpdate);
        };
        if tx.is_amm_burn()
            && (n_unique_bitasset_outputs < 2
                || n_unique_bitasset_inputs > n_unique_bitasset_outputs
                || n_unique_bitasset_outputs > n_unique_bitasset_inputs + 2)
        {
            return Err(Error::InvalidAmmBurn);
        };
        if tx.is_amm_mint()
            && (n_unique_bitasset_inputs < 2
                || n_unique_bitasset_outputs > n_unique_bitasset_inputs
                || n_unique_bitasset_inputs > n_unique_bitasset_outputs + 2)
        {
            return Err(Error::TooFewBitAssetsToAmmMint);
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
            return Err(Error::InvalidDutchAuctionBid);
        };
        if tx.is_dutch_auction_create()
            && (n_unique_bitasset_inputs < 1
                || n_unique_bitasset_outputs > n_unique_bitasset_inputs
                || n_unique_bitasset_inputs > n_unique_bitasset_outputs + 1)
        {
            return Err(Error::TooFewBitAssetsToDutchAuctionCreate);
        };
        if tx.is_dutch_auction_collect()
            && (n_unique_bitasset_outputs < 1
                || n_unique_bitasset_inputs > n_unique_bitasset_outputs
                || n_unique_bitasset_outputs > n_unique_bitasset_inputs + 2)
        {
            return Err(Error::InvalidDutchAuctionCollect);
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
                .get(rotxn, &BitAssetId(*name_hash))?
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
        let tip_hash = self.get_tip(rotxn)?;
        if header.prev_side_hash != tip_hash {
            let err = InvalidHeaderError::PrevSideHash {
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

    pub fn connect_two_way_peg_data(
        &self,
        rwtxn: &mut RwTxn,
        two_way_peg_data: &TwoWayPegData,
    ) -> Result<(), Error> {
        let block_height = self.get_height(rwtxn)?;
        tracing::trace!(%block_height, "Connecting 2WPD...");
        // Handle deposits.
        if let Some(latest_deposit_block_hash) =
            two_way_peg_data.latest_deposit_block_hash()
        {
            let deposit_block_seq_idx = self
                .deposit_blocks
                .last(rwtxn)?
                .map_or(0, |(seq_idx, _)| seq_idx + 1);
            self.deposit_blocks.put(
                rwtxn,
                &deposit_block_seq_idx,
                &(latest_deposit_block_hash, block_height - 1),
            )?;
        }
        for deposit in two_way_peg_data
            .deposits()
            .flat_map(|(_, deposits)| deposits)
        {
            let outpoint = OutPoint::Deposit(deposit.outpoint);
            let output = deposit.output.clone();
            self.utxos.put(rwtxn, &outpoint, &output)?;
        }

        // Handle withdrawals
        if let Some(latest_withdrawal_bundle_event_block_hash) =
            two_way_peg_data.latest_withdrawal_bundle_event_block_hash()
        {
            let withdrawal_bundle_event_block_seq_idx = self
                .withdrawal_bundle_event_blocks
                .last(rwtxn)?
                .map_or(0, |(seq_idx, _)| seq_idx + 1);
            self.withdrawal_bundle_event_blocks.put(
                rwtxn,
                &withdrawal_bundle_event_block_seq_idx,
                &(*latest_withdrawal_bundle_event_block_hash, block_height - 1),
            )?;
        }
        let last_withdrawal_bundle_failure_height = self
            .get_latest_failed_withdrawal_bundle(rwtxn)?
            .map(|(height, _bundle)| height)
            .unwrap_or_default();
        if block_height - last_withdrawal_bundle_failure_height
            > Self::WITHDRAWAL_BUNDLE_FAILURE_GAP
            && self
                .pending_withdrawal_bundle
                .get(rwtxn, &UnitKey)?
                .is_none()
        {
            if let Some(bundle) =
                self.collect_withdrawal_bundle(rwtxn, block_height)?
            {
                let m6id = bundle.compute_m6id();
                for (outpoint, spend_output) in bundle.spend_utxos() {
                    self.utxos.delete(rwtxn, outpoint)?;
                    let spent_output = SpentOutput {
                        output: spend_output.clone(),
                        inpoint: InPoint::Withdrawal { m6id },
                    };
                    self.stxos.put(rwtxn, outpoint, &spent_output)?;
                }
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
        for (_, event) in two_way_peg_data.withdrawal_bundle_events() {
            match event.status {
                WithdrawalBundleStatus::Submitted => {
                    let Some((bundle, bundle_block_height)) =
                        self.pending_withdrawal_bundle.get(rwtxn, &UnitKey)?
                    else {
                        if let Some((_bundle, bundle_status)) =
                            self.withdrawal_bundles.get(rwtxn, &event.m6id)?
                        {
                            // Already applied
                            assert_eq!(
                                bundle_status.earliest().value,
                                WithdrawalBundleStatus::Submitted
                            );
                            continue;
                        }
                        return Err(Error::UnknownWithdrawalBundle {
                            m6id: event.m6id,
                        });
                    };
                    assert_eq!(bundle_block_height, block_height - 2);
                    if bundle.compute_m6id() != event.m6id {
                        return Err(Error::UnknownWithdrawalBundle {
                            m6id: event.m6id,
                        });
                    }
                    tracing::debug!(
                        m6id = %event.m6id,
                        "Withdrawal bundle successfully submitted"
                    );
                    self.withdrawal_bundles.put(
                        rwtxn,
                        &event.m6id,
                        &(
                            bundle,
                            RollBack::<HeightStamped<_>>::new(
                                WithdrawalBundleStatus::Submitted,
                                block_height,
                            ),
                        ),
                    )?;
                    self.pending_withdrawal_bundle.delete(rwtxn, &UnitKey)?;
                }
                WithdrawalBundleStatus::Confirmed => {
                    let Some((bundle, mut bundle_status)) =
                        self.withdrawal_bundles.get(rwtxn, &event.m6id)?
                    else {
                        return Err(Error::UnknownWithdrawalBundle {
                            m6id: event.m6id,
                        });
                    };
                    if bundle_status.latest().value
                        == WithdrawalBundleStatus::Confirmed
                    {
                        // Already applied
                        continue;
                    } else {
                        assert_eq!(
                            bundle_status.latest().value,
                            WithdrawalBundleStatus::Submitted
                        );
                    }
                    bundle_status
                        .push(WithdrawalBundleStatus::Confirmed, block_height)
                        .expect("Push confirmed status should be valid");
                    self.withdrawal_bundles.put(
                        rwtxn,
                        &event.m6id,
                        &(bundle, bundle_status),
                    )?;
                }
                WithdrawalBundleStatus::Failed => {
                    let Some((bundle, mut bundle_status)) =
                        self.withdrawal_bundles.get(rwtxn, &event.m6id)?
                    else {
                        return Err(Error::UnknownWithdrawalBundle {
                            m6id: event.m6id,
                        });
                    };
                    if bundle_status.latest().value
                        == WithdrawalBundleStatus::Failed
                    {
                        // Already applied
                        continue;
                    } else {
                        assert_eq!(
                            bundle_status.latest().value,
                            WithdrawalBundleStatus::Submitted
                        );
                    }
                    bundle_status
                        .push(WithdrawalBundleStatus::Failed, block_height)
                        .expect("Push failed status should be valid");
                    for (outpoint, output) in bundle.spend_utxos() {
                        self.stxos.delete(rwtxn, outpoint)?;
                        self.utxos.put(rwtxn, outpoint, output)?;
                    }
                    let latest_failed_m6id =
                        if let Some(mut latest_failed_m6id) = self
                            .latest_failed_withdrawal_bundle
                            .get(rwtxn, &UnitKey)?
                        {
                            latest_failed_m6id
                                .push(event.m6id, block_height)
                                .expect(
                                    "Push latest failed m6id should be valid",
                                );
                            latest_failed_m6id
                        } else {
                            RollBack::<HeightStamped<_>>::new(
                                event.m6id,
                                block_height,
                            )
                        };
                    self.latest_failed_withdrawal_bundle.put(
                        rwtxn,
                        &UnitKey,
                        &latest_failed_m6id,
                    )?;
                    self.withdrawal_bundles.put(
                        rwtxn,
                        &event.m6id,
                        &(bundle, bundle_status),
                    )?;
                }
            }
        }
        Ok(())
    }

    pub fn disconnect_two_way_peg_data(
        &self,
        rwtxn: &mut RwTxn,
        two_way_peg_data: &TwoWayPegData,
    ) -> Result<(), Error> {
        let block_height = self.get_height(rwtxn)?;
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
                    self.pending_withdrawal_bundle.put(
                        rwtxn,
                        &UnitKey,
                        &(bundle, bundle_status.height - 2),
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
            > Self::WITHDRAWAL_BUNDLE_FAILURE_GAP
            && let Some((bundle, bundle_height)) =
                self.pending_withdrawal_bundle.get(rwtxn, &UnitKey)?
            && bundle_height == block_height - 2
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

    // Apply AMM burn
    fn apply_amm_burn(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
    ) -> Result<(), Error> {
        let AmmBurn {
            asset0,
            asset1,
            lp_token_burn,
            amount0,
            amount1,
        } = filled_tx.amm_burn().ok_or(Error::InvalidAmmBurn)?;
        let amm_pair = AmmPair::new(asset0, asset1);
        let amm_pool_state =
            self.amm_pools.get(rwtxn, &amm_pair)?.ok_or_else(|| {
                Error::MissingAmmPoolState {
                    asset0: amm_pair.asset0(),
                    asset1: amm_pair.asset1(),
                }
            })?;
        let new_amm_pool_state = amm_pool_state.burn(lp_token_burn)?;
        // payout in asset 0
        let payout0 = amm_pool_state.reserve0 - new_amm_pool_state.reserve0;
        if payout0 != amount0 {
            return Err(Error::InvalidAmmBurn);
        }
        // payout in asset 1
        let payout1 = amm_pool_state.reserve1 - new_amm_pool_state.reserve1;
        if payout1 != amount1 {
            return Err(Error::InvalidAmmBurn);
        }
        self.amm_pools.put(rwtxn, &amm_pair, &new_amm_pool_state)?;
        Ok(())
    }

    fn revert_amm_burn(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
    ) -> Result<(), Error> {
        let AmmBurn {
            asset0,
            asset1,
            lp_token_burn,
            amount0,
            amount1,
        } = filled_tx.amm_burn().ok_or(Error::InvalidAmmBurn)?;
        let amm_pair = AmmPair::new(asset0, asset1);
        let amm_pool_state =
            self.amm_pools.get(rwtxn, &amm_pair)?.ok_or_else(|| {
                Error::MissingAmmPoolState {
                    asset0: amm_pair.asset0(),
                    asset1: amm_pair.asset1(),
                }
            })?;
        let prev_amm_pool_state =
            amm_pool_state.unburn(lp_token_burn, amount0, amount1)?;
        self.amm_pools.put(rwtxn, &amm_pair, &prev_amm_pool_state)?;
        Ok(())
    }

    // Apply AMM mint
    fn apply_amm_mint(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
    ) -> Result<(), Error> {
        let AmmMint {
            asset0,
            asset1,
            amount0,
            amount1,
            lp_token_mint,
        } = filled_tx.amm_mint().ok_or(Error::InvalidAmmMint)?;
        if asset0 == asset1 {
            return Err(Error::InvalidAmmMint);
        }
        let amm_pair = AmmPair::new(asset0, asset1);
        let amm_pool_state = self
            .amm_pools
            .get(rwtxn, &amm_pair)?
            .unwrap_or_else(|| AmmPoolState::new(filled_tx.txid()));
        let new_amm_pool_state = amm_pool_state.mint(amount0, amount1)?;
        let lp_tokens_minted = new_amm_pool_state
            .outstanding_lp_tokens
            .checked_sub(lp_token_mint)
            .ok_or(Error::InvalidAmmMint)?;
        if lp_tokens_minted != lp_token_mint {
            do yeet Error::InvalidAmmMint;
        }
        self.amm_pools.put(rwtxn, &amm_pair, &new_amm_pool_state)?;
        Ok(())
    }

    // Revert AMM mint
    fn revert_amm_mint(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
    ) -> Result<(), Error> {
        let AmmMint {
            asset0,
            asset1,
            amount0,
            amount1,
            lp_token_mint,
        } = filled_tx.amm_mint().ok_or(Error::InvalidAmmMint)?;
        if asset0 == asset1 {
            return Err(Error::InvalidAmmMint);
        }
        let amm_pair = AmmPair::new(asset0, asset1);
        let amm_pool_state =
            self.amm_pools.get(rwtxn, &amm_pair)?.ok_or_else(|| {
                Error::MissingAmmPoolState {
                    asset0: amm_pair.asset0(),
                    asset1: amm_pair.asset1(),
                }
            })?;
        if amm_pool_state.creation_txid == filled_tx.txid() {
            self.amm_pools.delete(rwtxn, &amm_pair)?;
        } else {
            let new_amm_pool_state =
                amm_pool_state.revert_mint(amount0, amount1, lp_token_mint)?;
            self.amm_pools.put(rwtxn, &amm_pair, &new_amm_pool_state)?;
        }
        Ok(())
    }

    // Apply AMM swap
    fn apply_amm_swap(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
    ) -> Result<(), Error> {
        let AmmSwap {
            asset_spend,
            asset_receive,
            amount_spend,
            amount_receive,
        } = filled_tx.amm_swap().ok_or(Error::InvalidAmmSwap)?;
        let amm_pair = AmmPair::new(asset_spend, asset_receive);
        let amm_pool_state =
            self.amm_pools.get(rwtxn, &amm_pair)?.ok_or_else(|| {
                Error::MissingAmmPoolState {
                    asset0: amm_pair.asset0(),
                    asset1: amm_pair.asset1(),
                }
            })?;
        let new_amm_pool_state;
        let amount_receive_after_fee;
        if asset_spend < asset_receive {
            new_amm_pool_state =
                amm_pool_state.swap_asset0_for_asset1(amount_spend)?;
            amount_receive_after_fee =
                amm_pool_state.reserve1 - new_amm_pool_state.reserve1;
        } else {
            new_amm_pool_state =
                amm_pool_state.swap_asset1_for_asset0(amount_spend)?;
            amount_receive_after_fee =
                amm_pool_state.reserve0 - new_amm_pool_state.reserve0;
        };
        if amount_receive != amount_receive_after_fee {
            return Err(Error::InvalidAmmSwap);
        }
        self.amm_pools.put(rwtxn, &amm_pair, &new_amm_pool_state)?;
        Ok(())
    }

    // Revert AMM swap
    fn revert_amm_swap(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
    ) -> Result<(), Error> {
        let amm_swap @ AmmSwap {
            asset_spend,
            asset_receive,
            amount_spend: _,
            amount_receive: _,
        } = filled_tx.amm_swap().ok_or(Error::InvalidAmmSwap)?;
        let amm_pair = AmmPair::new(asset_spend, asset_receive);
        let amm_pool_state =
            self.amm_pools.get(rwtxn, &amm_pair)?.ok_or_else(|| {
                Error::MissingAmmPoolState {
                    asset0: amm_pair.asset0(),
                    asset1: amm_pair.asset1(),
                }
            })?;
        let new_amm_pool_state = amm_pool_state.revert_swap(amm_swap)?;
        self.amm_pools.put(rwtxn, &amm_pair, &new_amm_pool_state)?;
        Ok(())
    }

    // Apply BitAsset registration
    fn apply_bitasset_registration(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
        name_hash: Hash,
        bitasset_data: &types::BitAssetData,
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
        if !self
            .bitasset_reservations
            .delete(rwtxn, burned_reservation_txid)?
        {
            return Err(Error::MissingReservation {
                txid: *burned_reservation_txid,
            });
        }
        let bitasset_id = BitAssetId(name_hash);
        // Assign a sequence number
        {
            let seq = self.next_bitasset_seq(rwtxn)?;
            self.bitasset_seq_to_bitasset
                .put(rwtxn, &seq, &bitasset_id)?;
            self.bitasset_to_bitasset_seq
                .put(rwtxn, &bitasset_id, &seq)?;
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

    fn revert_bitasset_registration(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
        bitasset: BitAssetId,
    ) -> Result<(), Error> {
        let Some(seq) = self.bitasset_to_bitasset_seq.get(rwtxn, &bitasset)?
        else {
            return Err(Error::MissingBitAsset { bitasset });
        };
        self.bitasset_to_bitasset_seq.delete(rwtxn, &bitasset)?;
        if !self.bitasset_seq_to_bitasset.delete(rwtxn, &seq)? {
            return Err(Error::MissingBitAsset { bitasset });
        }
        if !self.bitassets.delete(rwtxn, &bitasset)? {
            return Err(Error::MissingBitAsset { bitasset });
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
        self.bitasset_reservations.put(
            rwtxn,
            burned_reservation_txid,
            &implied_commitment,
        )?;
        Ok(())
    }

    // Apply BitAsset mint
    fn apply_bitasset_mint(
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
            .ok_or(Error::MissingBitAsset {
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

    // Revert BitAsset mint
    fn revert_bitasset_mint(
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
            .ok_or(Error::MissingBitAsset {
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

    // Apply BitAsset updates
    fn apply_bitasset_updates(
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
            .ok_or(Error::MissingBitAsset {
                bitasset: *updated_bitasset,
            })?;
        bitasset_data.apply_updates(bitasset_updates, filled_tx.txid(), height);
        self.bitassets
            .put(rwtxn, updated_bitasset, &bitasset_data)?;
        Ok(())
    }

    // Revert BitAsset updates
    fn revert_bitasset_updates(
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
            .ok_or(Error::MissingBitAsset {
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

    // Apply Dutch auction bid
    fn apply_dutch_auction_bid(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
        height: u32,
    ) -> Result<(), Error> {
        let DutchAuctionBid {
            auction_id,
            asset_spend,
            asset_receive,
            amount_spend,
            amount_receive,
        } = filled_tx
            .dutch_auction_bid()
            .ok_or(DutchAuctionBidError::InvalidTxData)?;
        let dutch_auction_state = self
            .dutch_auctions
            .get(rwtxn, &auction_id)?
            .ok_or(DutchAuctionBidError::MissingAuction)?;
        if asset_receive != dutch_auction_state.base_asset {
            do yeet DutchAuctionBidError::IncorrectReceiveAsset
        }
        if asset_spend != dutch_auction_state.quote_asset {
            do yeet DutchAuctionBidError::IncorrectSpendAsset
        }
        if amount_receive
            > dutch_auction_state.base_amount_remaining.latest().data
        {
            do yeet DutchAuctionBidError::QuantityTooLarge
        };
        let new_dutch_auction_state =
            dutch_auction_state.bid(filled_tx.txid(), amount_spend, height)?;
        let order_quantity =
            dutch_auction_state.base_amount_remaining.latest().data
                - new_dutch_auction_state.base_amount_remaining.latest().data;
        if amount_receive != order_quantity {
            do yeet DutchAuctionBidError::InvalidPrice
        };
        self.dutch_auctions.put(
            rwtxn,
            &auction_id,
            &new_dutch_auction_state,
        )?;
        Ok(())
    }

    // Revert Dutch auction bid
    fn revert_dutch_auction_bid(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
    ) -> Result<(), Error> {
        let DutchAuctionBid {
            auction_id,
            asset_spend,
            asset_receive,
            amount_spend: _,
            amount_receive: _,
        } = filled_tx
            .dutch_auction_bid()
            .ok_or(DutchAuctionBidError::InvalidTxData)?;
        let dutch_auction_state = self
            .dutch_auctions
            .get(rwtxn, &auction_id)?
            .ok_or(DutchAuctionBidError::MissingAuction)?;
        if asset_receive != dutch_auction_state.base_asset {
            do yeet DutchAuctionBidError::IncorrectReceiveAsset
        }
        if asset_spend != dutch_auction_state.quote_asset {
            do yeet DutchAuctionBidError::IncorrectSpendAsset
        }
        let new_dutch_auction_state =
            dutch_auction_state.revert_bid(filled_tx.txid())?;
        self.dutch_auctions.put(
            rwtxn,
            &auction_id,
            &new_dutch_auction_state,
        )?;
        Ok(())
    }

    // Apply Dutch auction create
    fn apply_dutch_auction_create(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
        dutch_auction_params: DutchAuctionParams,
        height: u32,
    ) -> Result<(), Error> {
        let DutchAuctionParams {
            start_block,
            duration,
            base_asset,
            base_amount,
            quote_asset,
            initial_price,
            final_price,
        } = dutch_auction_params;
        if height >= start_block {
            do yeet DutchAuctionCreateError::Expired;
        };
        if final_price > initial_price {
            do yeet DutchAuctionCreateError::FinalPrice;
        };
        match duration {
            0 => do yeet DutchAuctionCreateError::ZeroDuration,
            1 => {
                if final_price != initial_price {
                    do yeet DutchAuctionCreateError::PriceMismatch
                }
            }
            _ => (),
        };
        let txid = filled_tx.txid();
        let dutch_auction_id = DutchAuctionId(txid);
        let dutch_auction_state =
            DutchAuctionState {
                start_block,
                most_recent_bid_block: RollBack::<TxidStamped<_>>::new(
                    start_block,
                    txid,
                    height,
                ),
                duration,
                base_asset,
                initial_base_amount: base_amount,
                base_amount_remaining: RollBack::<TxidStamped<_>>::new(
                    base_amount,
                    txid,
                    height,
                ),
                quote_asset,
                quote_amount: RollBack::<TxidStamped<_>>::new(0, txid, height),
                initial_price,
                price_after_most_recent_bid: RollBack::<TxidStamped<_>>::new(
                    initial_price,
                    txid,
                    height,
                ),
                initial_end_price: final_price,
                end_price_after_most_recent_bid:
                    RollBack::<TxidStamped<_>>::new(final_price, txid, height),
            };
        self.dutch_auctions.put(
            rwtxn,
            &dutch_auction_id,
            &dutch_auction_state,
        )?;
        Ok(())
    }

    // Revert Dutch auction create
    fn revert_dutch_auction_create(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
    ) -> Result<(), Error> {
        let dutch_auction_id = DutchAuctionId(filled_tx.txid());
        if !self.dutch_auctions.delete(rwtxn, &dutch_auction_id)? {
            return Err(Error::MissingDutchAuction(dutch_auction_id));
        };
        Ok(())
    }

    // Apply Dutch auction collect
    fn apply_dutch_auction_collect(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
        height: u32,
    ) -> Result<(), Error> {
        let DutchAuctionCollect {
            auction_id,
            asset_offered,
            asset_receive,
            amount_offered_remaining,
            amount_received,
        } = filled_tx
            .dutch_auction_collect()
            .ok_or(DutchAuctionCollectError::InvalidTxData)?;
        let mut auction_state = self
            .dutch_auctions
            .get(rwtxn, &auction_id)?
            .ok_or(DutchAuctionCollectError::MissingAuction)?;
        if auction_state.base_asset != asset_offered {
            do yeet DutchAuctionCollectError::IncorrectOfferedAsset
        }
        if auction_state.quote_asset != asset_receive {
            do yeet DutchAuctionCollectError::IncorrectReceiveAsset
        }
        if height
            < auction_state
                .start_block
                .saturating_add(auction_state.duration)
        {
            do yeet DutchAuctionCollectError::AuctionNotFinished
        }
        if amount_offered_remaining
            != auction_state.base_amount_remaining.latest().data
        {
            do yeet DutchAuctionCollectError::IncorrectOfferedAssetAmount
        }
        if amount_received != auction_state.quote_amount.latest().data {
            do yeet DutchAuctionCollectError::IncorrectReceiveAssetAmount
        }
        let txid = filled_tx.txid();
        auction_state.base_amount_remaining.push(0, txid, height);
        auction_state.quote_amount.push(0, txid, height);
        self.dutch_auctions
            .put(rwtxn, &auction_id, &auction_state)?;
        Ok(())
    }

    // Revert Dutch auction collect
    fn revert_dutch_auction_collect(
        &self,
        rwtxn: &mut RwTxn,
        filled_tx: &FilledTransaction,
    ) -> Result<(), Error> {
        let DutchAuctionCollect {
            auction_id,
            asset_offered: _,
            asset_receive: _,
            amount_offered_remaining,
            amount_received,
        } = filled_tx
            .dutch_auction_collect()
            .ok_or(DutchAuctionCollectError::InvalidTxData)?;
        let txid = filled_tx.txid();
        let mut auction_state = self
            .dutch_auctions
            .get(rwtxn, &auction_id)?
            .ok_or(Error::RevertDutchAuctionCollect)?;
        assert!(auction_state
            .base_amount_remaining
            .pop()
            .is_some_and(|ts| { ts.txid == txid && ts.data == 0 }));
        assert_eq!(
            auction_state.base_amount_remaining.latest().data,
            amount_offered_remaining
        );
        assert!(auction_state
            .quote_amount
            .pop()
            .is_some_and(|ts| { ts.txid == txid && ts.data == 0 }));
        assert_eq!(auction_state.quote_amount.latest().data, amount_received);
        self.dutch_auctions
            .put(rwtxn, &auction_id, &auction_state)?;
        Ok(())
    }

    pub fn connect_block(
        &self,
        rwtxn: &mut RwTxn,
        header: &Header,
        body: &Body,
    ) -> Result<(), Error> {
        let height = self.get_height(rwtxn)?;
        let tip_hash = self.get_tip(rwtxn)?;
        if tip_hash != header.prev_side_hash {
            let err = InvalidHeaderError::PrevSideHash {
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
                    self.apply_amm_burn(rwtxn, &filled_tx)?;
                }
                Some(TxData::AmmMint { .. }) => {
                    self.apply_amm_mint(rwtxn, &filled_tx)?;
                }
                Some(TxData::AmmSwap { .. }) => {
                    self.apply_amm_swap(rwtxn, &filled_tx)?;
                }
                Some(TxData::BitAssetReservation { commitment }) => {
                    self.bitasset_reservations.put(rwtxn, &txid, commitment)?;
                }
                Some(TxData::BitAssetRegistration {
                    name_hash,
                    revealed_nonce: _,
                    bitasset_data,
                    initial_supply,
                }) => {
                    let () = self.apply_bitasset_registration(
                        rwtxn,
                        &filled_tx,
                        *name_hash,
                        bitasset_data,
                        *initial_supply,
                        height,
                    )?;
                }
                Some(TxData::BitAssetMint(mint_amount)) => {
                    let () = self.apply_bitasset_mint(
                        rwtxn,
                        &filled_tx,
                        *mint_amount,
                        height,
                    )?;
                }
                Some(TxData::BitAssetUpdate(bitasset_updates)) => {
                    let () = self.apply_bitasset_updates(
                        rwtxn,
                        &filled_tx,
                        (**bitasset_updates).clone(),
                        height,
                    )?;
                }
                Some(TxData::DutchAuctionBid { .. }) => {
                    let () = self
                        .apply_dutch_auction_bid(rwtxn, &filled_tx, height)?;
                }
                Some(TxData::DutchAuctionCreate(dutch_auction_params)) => {
                    let () = self.apply_dutch_auction_create(
                        rwtxn,
                        &filled_tx,
                        *dutch_auction_params,
                        height,
                    )?;
                }
                Some(TxData::DutchAuctionCollect { .. }) => {
                    let () = self.apply_dutch_auction_collect(
                        rwtxn, &filled_tx, height,
                    )?;
                }
            }
        }
        let block_hash = header.hash();
        self.tip.put(rwtxn, &UnitKey, &block_hash)?;
        self.height.put(rwtxn, &UnitKey, &(height + 1))?;
        Ok(())
    }

    pub fn disconnect_tip(
        &self,
        rwtxn: &mut RwTxn,
        header: &Header,
        body: &Body,
    ) -> Result<(), Error> {
        let tip_hash = self.get_tip(rwtxn)?;
        if tip_hash != header.hash() {
            let err = InvalidHeaderError::BlockHash {
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
        let height = self.get_height(rwtxn)?;
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
                    let () = self.revert_amm_burn(rwtxn, &filled_tx)?;
                }
                Some(TxData::AmmMint { .. }) => {
                    let () = self.revert_amm_mint(rwtxn, &filled_tx)?;
                }
                Some(TxData::AmmSwap { .. }) => {
                    let () = self.revert_amm_swap(rwtxn, &filled_tx)?;
                }
                Some(TxData::BitAssetMint(mint_amount)) => {
                    let () = self.revert_bitasset_mint(
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
                    let () = self.revert_bitasset_registration(
                        rwtxn,
                        &filled_tx,
                        BitAssetId(*name_hash),
                    )?;
                }
                Some(TxData::BitAssetReservation { commitment: _ }) => {
                    if !self.bitasset_reservations.delete(rwtxn, &txid)? {
                        return Err(Error::MissingReservation { txid });
                    }
                }
                Some(TxData::BitAssetUpdate(bitasset_updates)) => {
                    let () = self.revert_bitasset_updates(
                        rwtxn,
                        &filled_tx,
                        (**bitasset_updates).clone(),
                        height - 1,
                    )?;
                }
                Some(TxData::DutchAuctionBid { .. }) => {
                    let () =
                        self.revert_dutch_auction_bid(rwtxn, &filled_tx)?;
                }
                Some(TxData::DutchAuctionCollect { .. }) => {
                    let () =
                        self.revert_dutch_auction_collect(rwtxn, &filled_tx)?;
                }
                Some(TxData::DutchAuctionCreate(_auction_params)) => {
                    let () =
                        self.revert_dutch_auction_create(rwtxn, &filled_tx)?;
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
        self.tip.put(rwtxn, &UnitKey, &header.prev_side_hash)?;
        self.height.put(rwtxn, &UnitKey, &(height - 1))?;
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
