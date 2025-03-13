//! State errors

use sneed::{db::error as db, env::error as env, rwtxn::error as rwtxn};
use thiserror::Error;
use transitive::Transitive;

use crate::types::{
    AmountOverflowError, AmountUnderflowError, AssetId, BitAssetId, BlockHash,
    Hash, M6id, MerkleRoot, OutPoint, Txid, WithdrawalBundleError,
};

/// Errors related to an AMM pool
#[derive(Debug, Error, Transitive)]
#[transitive(from(db::Delete, db::Error))]
#[transitive(from(db::Error, sneed::Error))]
#[transitive(from(db::Put, db::Error))]
#[transitive(from(db::TryGet, db::Error))]
pub enum Amm {
    #[error("AMM burn overflow")]
    BurnOverflow,
    #[error("AMM burn underflow")]
    BurnUnderflow,
    #[error(transparent)]
    Db(#[from] sneed::Error),
    #[error("Insufficient liquidity")]
    InsufficientLiquidity,
    #[error("Invalid AMM burn")]
    InvalidBurn,
    #[error("Invalid AMM mint")]
    InvalidMint,
    #[error("Invalid AMM swap")]
    InvalidSwap,
    #[error("AMM LP token overflow")]
    LpTokenOverflow,
    #[error("AMM LP token underflow")]
    LpTokenUnderflow,
    #[error("missing AMM pool state for {asset0}-{asset1}")]
    MissingPoolState { asset0: AssetId, asset1: AssetId },
    #[error("AMM pool invariant")]
    PoolInvariant,
    #[error("Failed to revert AMM mint")]
    RevertMint,
    #[error("Failed to revert AMM swap")]
    RevertSwap,
    #[error("Too few BitAssets to mint an AMM position")]
    TooFewBitAssetsToMint,
}

/// Errors related to BitAssets
#[derive(Debug, Error, Transitive)]
#[transitive(from(db::Delete, db::Error))]
#[transitive(from(db::Last, db::Error))]
#[transitive(from(db::Put, db::Error))]
#[transitive(from(db::TryGet, db::Error))]
pub enum BitAsset {
    #[error(transparent)]
    Db(#[from] db::Error),
    #[error("missing BitAsset {bitasset:?}")]
    Missing { bitasset: BitAssetId },
    #[error(
        "Missing BitAsset data for {name_hash:?} at block height {block_height}"
    )]
    MissingData { name_hash: Hash, block_height: u32 },
    #[error("missing BitAsset reservation {txid}")]
    MissingReservation { txid: Txid },
    #[error("no BitAssets to mint")]
    NoBitAssetsToMint,
    #[error("no BitAssets to update")]
    NoBitAssetsToUpdate,
    #[error("Mint would cause total supply to overflow")]
    TotalSupplyOverflow,
    #[error("Reverting Mint would cause total supply to underflow")]
    TotalSupplyUnderflow,
}

/// Errors related to Dutch auctions
pub mod dutch_auction {
    use sneed::db::error as db;
    use thiserror::Error;
    use transitive::Transitive;

    use crate::types::DutchAuctionId;

    /// Errors when bidding on a Dutch auction
    #[derive(Debug, Error)]
    pub enum Bid {
        #[error("Auction has already ended")]
        AuctionEnded,
        #[error("Auction has not started yet")]
        AuctionNotStarted,
        #[error("Incorrect receive asset specified")]
        IncorrectReceiveAsset,
        #[error("Incorrect spend asset")]
        IncorrectSpendAsset,
        #[error("Invalid Dutch auction bid")]
        Invalid,
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
    #[derive(Debug, Error)]
    pub enum Create {
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
    #[derive(Debug, Error)]
    pub enum Collect {
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
        #[error("Invalid Dutch auction collect")]
        Invalid,
        #[error("Invalid TxData")]
        InvalidTxData,
        #[error("Auction not found")]
        MissingAuction,
        #[error("Failed to revert Dutch Auction collect")]
        Revert,
    }

    /// Errors related to Dutch auctions
    #[derive(Debug, Error, Transitive)]
    #[transitive(from(db::Delete, db::Error))]
    #[transitive(from(db::Error, sneed::Error))]
    #[transitive(from(db::Put, db::Error))]
    #[transitive(from(db::TryGet, db::Error))]
    pub enum Error {
        #[error(transparent)]
        Bid(#[from] Bid),
        #[error(transparent)]
        Collect(#[from] Collect),
        #[error(transparent)]
        Create(#[from] Create),
        #[error(transparent)]
        Db(#[from] sneed::Error),
        #[error("missing Dutch auction {0}")]
        Missing(DutchAuctionId),
        #[error("Too few BitAssets to create a Dutch auction")]
        TooFewBitAssetsToCreate,
    }
}
pub use dutch_auction::Error as DutchAuction;

#[derive(Debug, Error)]
pub enum InvalidHeader {
    #[error("expected block hash {expected}, but computed {computed}")]
    BlockHash {
        expected: BlockHash,
        computed: BlockHash,
    },
    #[error(
        "expected previous sidechain block hash {expected:?}, but received {received:?}"
    )]
    PrevSideHash {
        expected: Option<BlockHash>,
        received: Option<BlockHash>,
    },
}

#[derive(Debug, Error, Transitive)]
#[transitive(from(db::Clear, db::Error))]
#[transitive(from(db::Delete, db::Error))]
#[transitive(from(db::Error, sneed::Error))]
#[transitive(from(db::IterInit, db::Error))]
#[transitive(from(db::IterItem, db::Error))]
#[transitive(from(db::Last, db::Error))]
#[transitive(from(db::Put, db::Error))]
#[transitive(from(db::TryGet, db::Error))]
#[transitive(from(env::CreateDb, env::Error))]
#[transitive(from(env::Error, sneed::Error))]
#[transitive(from(env::WriteTxn, env::Error))]
#[transitive(from(rwtxn::Commit, rwtxn::Error))]
#[transitive(from(rwtxn::Error, sneed::Error))]
pub enum Error {
    #[error(transparent)]
    Amm(#[from] Amm),
    #[error(transparent)]
    AmountOverflow(#[from] AmountOverflowError),
    #[error(transparent)]
    AmountUnderflow(#[from] AmountUnderflowError),
    #[error("failed to verify authorization")]
    AuthorizationError,
    #[error("bad coinbase output content")]
    BadCoinbaseOutputContent,
    #[error(transparent)]
    BitAsset(#[from] BitAsset),
    #[error("bitasset {name_hash:?} already registered")]
    BitAssetAlreadyRegistered { name_hash: Hash },
    #[error("bundle too heavy {weight} > {max_weight}")]
    BundleTooHeavy { weight: u64, max_weight: u64 },
    #[error(transparent)]
    BorshSerialize(borsh::io::Error),
    #[error(transparent)]
    Db(#[from] sneed::Error),
    #[error(transparent)]
    DutchAuction(#[from] DutchAuction),
    #[error("failed to fill tx output contents: invalid transaction")]
    FillTxOutputContentsFailed,
    #[error(
        "invalid body: expected merkle root {expected}, but computed {computed}"
    )]
    InvalidBody {
        expected: MerkleRoot,
        computed: MerkleRoot,
    },
    #[error("invalid header: {0}")]
    InvalidHeader(InvalidHeader),
    #[error(
        "The last output in a BitAsset registration tx must be a control coin"
    )]
    LastOutputNotControlCoin,
    #[error("missing BitAsset input {name_hash:?}")]
    MissingBitAssetInput { name_hash: Hash },
    #[error("deposit block doesn't exist")]
    NoDepositBlock,
    #[error("total fees less than coinbase value")]
    NotEnoughFees,
    #[error("value in is less than value out")]
    NotEnoughValueIn,
    #[error("no tip")]
    NoTip,
    #[error("stxo {outpoint} doesn't exist")]
    NoStxo { outpoint: OutPoint },
    #[error("utxo {outpoint} doesn't exist")]
    NoUtxo { outpoint: OutPoint },
    #[error("Withdrawal bundle event block doesn't exist")]
    NoWithdrawalBundleEventBlock,
    #[error(
        "The second-last output in a BitAsset registration tx \
             must be the BitAsset mint, \
             if the initial supply is nonzero"
    )]
    SecondLastOutputNotBitAsset,
    #[error(transparent)]
    SignatureError(#[from] ed25519_dalek::SignatureError),
    #[error("Too few BitAsset control coin outputs")]
    TooFewBitAssetControlOutputs,
    #[error(
        "unbalanced BitAsset control coins: \
         {n_bitasset_control_inputs} BitAsset control coin inputs, \
         {n_bitasset_control_outputs} BitAsset control coin outputs"
    )]
    UnbalancedBitAssetControls {
        n_bitasset_control_inputs: usize,
        n_bitasset_control_outputs: usize,
    },
    #[error(
        "unbalanced BitAssets: {n_unique_bitasset_inputs} unique BitAsset inputs, {n_bitasset_outputs} BitAsset outputs"
    )]
    UnbalancedBitAssets {
        n_unique_bitasset_inputs: usize,
        n_bitasset_outputs: usize,
    },
    #[error(
        "unbalanced reservations: {n_reservation_inputs} reservation inputs, {n_reservation_outputs} reservation outputs"
    )]
    UnbalancedReservations {
        n_reservation_inputs: usize,
        n_reservation_outputs: usize,
    },
    #[error("Unknown withdrawal bundle: {m6id}")]
    UnknownWithdrawalBundle { m6id: M6id },
    #[error(
        "Unknown withdrawal bundle confirmed in {event_block_hash}: {m6id}"
    )]
    UnknownWithdrawalBundleConfirmed {
        event_block_hash: bitcoin::BlockHash,
        m6id: M6id,
    },
    #[error("utxo double spent")]
    UtxoDoubleSpent,
    #[error(transparent)]
    WithdrawalBundle(#[from] WithdrawalBundleError),
    #[error("wrong public key for address")]
    WrongPubKeyForAddress,
}
