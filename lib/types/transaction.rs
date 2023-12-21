use std::{
    cmp::Ordering,
    collections::HashMap,
    hash::Hasher,
    net::{Ipv4Addr, Ipv6Addr},
};

use educe::Educe;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use bip300301::bitcoin;

use super::{
    address::Address,
    hashes::{self, Hash, MerkleRoot, Txid},
    serde_display_fromstr_human_readable, serde_hexstr_human_readable,
    EncryptionPubKey, GetBitcoinValue,
};
use crate::authorization::{Authorization, PublicKey};

#[derive(Hash, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutPoint {
    // Created by transactions.
    Regular { txid: Txid, vout: u32 },
    // Created by block bodies.
    Coinbase { merkle_root: MerkleRoot, vout: u32 },
    // Created by mainchain deposits.
    Deposit(bitcoin::OutPoint),
}

/// Reference to a tx input.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum InPoint {
    /// Transaction input
    Regular {
        txid: Txid,
        // index of the spend in the inputs to spend_tx
        vin: u32,
    },
    // Created by mainchain withdrawals
    Withdrawal {
        txid: bitcoin::Txid,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Content {
    AmmLpToken(u64),
    BitAsset(u64),
    BitAssetControl,
    BitAssetReservation,
    /// Receipt used to redeem the proceeds of an auction
    DutchAuctionReceipt,
    Value(u64),
    Withdrawal {
        value: u64,
        main_fee: u64,
        main_address: bitcoin::Address<bitcoin::address::NetworkUnchecked>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Output {
    #[serde(with = "serde_display_fromstr_human_readable")]
    pub address: Address,
    pub content: Content,
    #[serde(with = "serde_hexstr_human_readable")]
    pub memo: Vec<u8>,
}

pub type TxInputs = Vec<OutPoint>;

pub type TxOutputs = Vec<Output>;

fn hash_option_public_key<H>(pk: &Option<PublicKey>, state: &mut H)
where
    H: Hasher,
{
    use std::hash::Hash;
    pk.map(|pk| pk.to_bytes()).hash(state)
}

#[derive(
    Clone, Debug, Default, Educe, Eq, PartialEq, Serialize, Deserialize,
)]
#[educe(Hash)]
pub struct BitAssetData {
    /// Commitment to arbitrary data
    pub commitment: Option<Hash>,
    /// Optional ipv4 addr
    pub ipv4_addr: Option<Ipv4Addr>,
    /// Optional ipv6 addr
    pub ipv6_addr: Option<Ipv6Addr>,
    /// Optional pubkey used for encryption
    pub encryption_pubkey: Option<EncryptionPubKey>,
    /// Optional pubkey used for signing messages
    #[educe(Hash(method = "hash_option_public_key"))]
    pub signing_pubkey: Option<PublicKey>,
}

/// Delete, retain, or set a value
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Update<T> {
    Delete,
    Retain,
    Set(T),
}

/// Updates to the data associated with a BitAsset
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BitAssetDataUpdates {
    /// Commitment to arbitrary data
    pub commitment: Update<Hash>,
    /// Optional ipv4 addr
    pub ipv4_addr: Update<Ipv4Addr>,
    /// Optional ipv6 addr
    pub ipv6_addr: Update<Ipv6Addr>,
    /// Optional pubkey used for encryption
    pub encryption_pubkey: Update<EncryptionPubKey>,
    /// Optional pubkey used for signing messages
    pub signing_pubkey: Update<PublicKey>,
}

/// Parameters of a Dutch Auction
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct DutchAuctionParams {
    /// Block height at which the auction starts
    pub start_block: u32,
    /// Auction duration, in blocks
    pub duration: u32,
    /// The asset to be auctioned
    pub base_asset: Hash,
    /// The amount of the base asset to be auctioned
    pub base_amount: u64,
    /// The asset in which the auction is to be quoted
    pub quote_asset: Hash,
    /// Initial price
    pub initial_price: u64,
    /// Final price
    pub final_price: u64,
}

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TransactionData {
    /// Burn an AMM position
    AmmBurn {
        /// Amount of the lexicographically ordered first BitAsset to receive
        amount0: u64,
        /// Amount of the lexicographically ordered second BitAsset to receive
        amount1: u64,
        /// Amount of the LP token to burn
        lp_token_burn: u64,
    },
    /// Mint an AMM position
    AmmMint {
        /// Amount of the lexicographically ordered first BitAsset to deposit
        amount0: u64,
        /// Amount of the lexicographically ordered second BitAsset to deposit
        amount1: u64,
        /// Amount of the LP token to receive
        lp_token_mint: u64,
    },
    /// AMM swap
    AmmSwap {
        /// Amount to spend
        amount_spent: u64,
        /// Amount to receive
        amount_receive: u64,
        /// Pair asset to swap for
        pair_asset: Hash,
    },
    BitAssetReservation {
        /// Commitment to the BitAsset that will be registered
        #[serde(with = "serde_hexstr_human_readable")]
        commitment: Hash,
    },
    BitAssetRegistration {
        /// Reveal of the name hash
        #[serde(with = "serde_hexstr_human_readable")]
        name_hash: Hash,
        /// Reveal of the nonce used for the BitAsset reservation commitment
        #[serde(with = "serde_hexstr_human_readable")]
        revealed_nonce: Hash,
        /// Initial BitAsset data
        bitasset_data: Box<BitAssetData>,
        /// Amount to mint
        initial_supply: u64,
    },
    /// Mint more of a BitAsset
    BitAssetMint(u64),
    BitAssetUpdate(Box<BitAssetDataUpdates>),
    DutchAuctionCreate(DutchAuctionParams),
    DutchAuctionBid {
        auction_id: DutchAuctionId,
        /// Asset to receive in the auction
        receive_asset: Hash,
        /// Quantity to purchase in the auction
        quantity: u64,
        /// Total bid size, in terms of the quote asset
        bid_size: u64,
    },
    DutchAuctionCollect {
        /// Base asset
        asset_offered: Hash,
        /// Quote asset
        asset_receive: Hash,
        /// Amount of the offered base asset
        amount_offered_remaining: u64,
        /// Amount of the received quote asset
        amount_received: u64,
    },
}

pub type TxData = TransactionData;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Transaction {
    pub inputs: TxInputs,
    pub outputs: TxOutputs,
    #[serde(with = "serde_hexstr_human_readable")]
    pub memo: Vec<u8>,
    pub data: Option<TransactionData>,
}

/// Unique identifier for each Dutch auction
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DutchAuctionId(pub Txid);

/** Representation of Output Content that includes asset type and/or
 *  reservation commitment */
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FilledContent {
    AmmLpToken {
        asset0: Hash,
        asset1: Hash,
        amount: u64,
    },
    Bitcoin(u64),
    BitcoinWithdrawal {
        value: u64,
        main_fee: u64,
        main_address: bitcoin::Address<bitcoin::address::NetworkUnchecked>,
    },
    /// BitAsset ID and coin value
    BitAsset(Hash, u64),
    BitAssetControl(Hash),
    /// Reservation txid and commitment
    BitAssetReservation(Txid, Hash),
    /// Auction ID
    DutchAuctionReceipt(DutchAuctionId),
}

/// Representation of output that includes asset type
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FilledOutput {
    #[serde(with = "serde_display_fromstr_human_readable")]
    pub address: Address,
    pub content: FilledContent,
    #[serde(with = "serde_hexstr_human_readable")]
    pub memo: Vec<u8>,
}

/// Representation of a spent output
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SpentOutput {
    pub output: FilledOutput,
    pub inpoint: InPoint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilledTransaction {
    pub transaction: Transaction,
    pub spent_utxos: Vec<FilledOutput>,
}

/// Struct describing an AMM burn
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AmmBurn {
    pub asset0: Hash,
    pub asset1: Hash,
    /// Amount of asset 0 received
    pub amount0: u64,
    /// Amount of asset 1 received
    pub amount1: u64,
    /// Amount of LP token burned
    pub lp_token_burn: u64,
}

/// Struct describing an AMM mint
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AmmMint {
    pub asset0: Hash,
    pub asset1: Hash,
    /// Amount of asset 0 deposited
    pub amount0: u64,
    /// Amount of asset 1 deposited
    pub amount1: u64,
    /// Amount of LP token received
    pub lp_token_mint: u64,
}

/// Struct describing an AMM swap
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AmmSwap {
    pub asset_spend: Hash,
    pub asset_receive: Hash,
    /// Amount of spend asset spent
    pub amount_spend: u64,
    //// Amount of receive asset received
    pub amount_receive: u64,
}

/// Struct describing a Dutch auction bid
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DutchAuctionBid {
    pub auction_id: DutchAuctionId,
    pub asset_spend: Hash,
    pub asset_receive: Hash,
    /// Amount of spend asset spent
    pub amount_spend: u64,
    //// Amount of receive asset received
    pub amount_receive: u64,
}

/// Struct describing a Dutch auction collect
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DutchAuctionCollect {
    pub auction_id: DutchAuctionId,
    pub asset_offered: Hash,
    pub asset_receive: Hash,
    /// Amount of offered asset remaining
    pub amount_offered_remaining: u64,
    //// Amount of receive asset received
    pub amount_received: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizedTransaction {
    pub transaction: Transaction,
    /// Authorization is called witness in Bitcoin.
    pub authorizations: Vec<Authorization>,
}

impl std::fmt::Display for OutPoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Regular { txid, vout } => write!(f, "regular {txid} {vout}"),
            Self::Coinbase { merkle_root, vout } => {
                write!(f, "coinbase {merkle_root} {vout}")
            }
            Self::Deposit(bitcoin::OutPoint { txid, vout }) => {
                write!(f, "deposit {txid} {vout}")
            }
        }
    }
}

impl Content {
    /// `true` if the output content corresponds to a BitAsset
    pub fn is_bitasset(&self) -> bool {
        matches!(self, Self::BitAsset(_))
    }

    /// `true` if the output content corresponds to a BitAsset control coin
    pub fn is_bitasset_control(&self) -> bool {
        matches!(self, Self::BitAssetControl)
    }

    /// `true`` if the output content corresponds to a reservation
    pub fn is_reservation(&self) -> bool {
        matches!(self, Self::BitAssetReservation)
    }

    pub fn is_bitcoin(&self) -> bool {
        matches!(self, Self::Value(_))
    }
    pub fn is_withdrawal(&self) -> bool {
        matches!(self, Self::Withdrawal { .. })
    }
}

impl GetBitcoinValue for Content {
    #[inline(always)]
    fn get_bitcoin_value(&self) -> u64 {
        match self {
            Self::AmmLpToken(_)
            | Self::BitAsset(_)
            | Self::BitAssetControl
            | Self::BitAssetReservation
            | Self::DutchAuctionReceipt => 0,
            Self::Value(value) => *value,
            Self::Withdrawal { value, .. } => *value,
        }
    }
}

impl Output {
    pub fn new(address: Address, content: Content) -> Self {
        Self {
            address,
            content,
            memo: Vec::new(),
        }
    }

    /// `true` if the output content corresponds to a BitAsset
    pub fn is_bitasset(&self) -> bool {
        self.content.is_bitasset()
    }

    /// `true` if the output content corresponds to a BitAsset control coin
    pub fn is_bitasset_control(&self) -> bool {
        self.content.is_bitasset_control()
    }

    /// `true` if the output content corresponds to a reservation
    pub fn is_reservation(&self) -> bool {
        self.content.is_reservation()
    }
}

impl GetBitcoinValue for Output {
    #[inline(always)]
    fn get_bitcoin_value(&self) -> u64 {
        self.content.get_bitcoin_value()
    }
}

impl TxData {
    /// `true` if the tx data corresponds to an AMM burn
    pub fn is_amm_burn(&self) -> bool {
        matches!(self, Self::AmmBurn { .. })
    }

    /// `true` if the tx data corresponds to an AMM mint
    pub fn is_amm_mint(&self) -> bool {
        matches!(self, Self::AmmMint { .. })
    }

    /// `true` if the tx data corresponds to an AMM swap
    pub fn is_amm_swap(&self) -> bool {
        matches!(self, Self::AmmSwap { .. })
    }

    /// `true` if the tx data corresponds to a Dutch auction bid
    pub fn is_dutch_auction_bid(&self) -> bool {
        matches!(self, Self::DutchAuctionBid { .. })
    }

    /// `true` if the tx data corresponds to a Dutch auction creation
    pub fn is_dutch_auction_create(&self) -> bool {
        matches!(self, Self::DutchAuctionCreate(_))
    }

    /// `true` if the tx data corresponds to a Dutch auction collect
    pub fn is_dutch_auction_collect(&self) -> bool {
        matches!(self, Self::DutchAuctionCollect { .. })
    }

    /// `true` if the tx data corresponds to a reservation
    pub fn is_registration(&self) -> bool {
        matches!(self, Self::BitAssetRegistration { .. })
    }

    /// `true` if the tx data corresponds to a reservation
    pub fn is_reservation(&self) -> bool {
        matches!(self, Self::BitAssetReservation { .. })
    }

    /// `true` if the tx data corresponds to an update
    pub fn is_update(&self) -> bool {
        matches!(self, Self::BitAssetUpdate(_))
    }
}

impl Transaction {
    pub fn new(inputs: TxInputs, outputs: TxOutputs) -> Self {
        Self {
            inputs,
            outputs,
            memo: Vec::new(),
            data: None,
        }
    }

    /// Return an iterator over Bitcoin outputs with index
    pub fn indexed_bitcoin_value_outputs(
        &self,
    ) -> impl Iterator<Item = (usize, &Output)> {
        self.outputs
            .iter()
            .enumerate()
            .filter(|(_, output)| output.get_bitcoin_value() != 0)
    }

    /// Return an iterator over BitAsset outputs
    pub fn bitasset_outputs(&self) -> impl Iterator<Item = &Output> {
        self.outputs.iter().filter(|output| output.is_bitasset())
    }

    /// Return an iterator over BitAsset control outputs
    pub fn bitasset_control_outputs(&self) -> impl Iterator<Item = &Output> {
        self.outputs
            .iter()
            .filter(|output| output.is_bitasset_control())
    }

    /// `true` if the tx data corresponds to an AMM burn
    pub fn is_amm_burn(&self) -> bool {
        match &self.data {
            Some(tx_data) => tx_data.is_amm_burn(),
            None => false,
        }
    }

    /// `true` if the tx data corresponds to an AMM mint
    pub fn is_amm_mint(&self) -> bool {
        match &self.data {
            Some(tx_data) => tx_data.is_amm_mint(),
            None => false,
        }
    }

    /// `true` if the tx data corresponds to an AMM swap
    pub fn is_amm_swap(&self) -> bool {
        match &self.data {
            Some(tx_data) => tx_data.is_amm_swap(),
            None => false,
        }
    }

    /// `true` if the tx data corresponds to a Dutch auction bid
    pub fn is_dutch_auction_bid(&self) -> bool {
        match &self.data {
            Some(tx_data) => tx_data.is_dutch_auction_bid(),
            None => false,
        }
    }

    /// `true` if the tx data corresponds to a Dutch auction creation
    pub fn is_dutch_auction_create(&self) -> bool {
        match &self.data {
            Some(tx_data) => tx_data.is_dutch_auction_create(),
            None => false,
        }
    }

    /// `true` if the tx data corresponds to a Dutch auction collect
    pub fn is_dutch_auction_collect(&self) -> bool {
        match &self.data {
            Some(tx_data) => tx_data.is_dutch_auction_collect(),
            None => false,
        }
    }

    /// `true` if the tx data corresponds to a BitAsset registration
    pub fn is_registration(&self) -> bool {
        match &self.data {
            Some(tx_data) => tx_data.is_registration(),
            None => false,
        }
    }

    /// `true` if the tx data corresponds to a regular tx
    pub fn is_regular(&self) -> bool {
        self.data.is_none()
    }

    /// `true` if the tx data corresponds to a reservation
    pub fn is_reservation(&self) -> bool {
        match &self.data {
            Some(tx_data) => tx_data.is_reservation(),
            None => false,
        }
    }

    /// `true` if the tx data corresponds to an update
    pub fn is_update(&self) -> bool {
        match &self.data {
            Some(tx_data) => tx_data.is_update(),
            None => false,
        }
    }

    /// If the tx is a bitasset registration, returns the registered name hash
    pub fn registration_name_hash(&self) -> Option<Hash> {
        match self.data {
            Some(TxData::BitAssetRegistration { name_hash, .. }) => {
                Some(name_hash)
            }
            _ => None,
        }
    }

    /** If the tx is a bitasset registration, returns the implied reservation
     * commitment */
    pub fn implied_reservation_commitment(&self) -> Option<Hash> {
        match self.data {
            Some(TxData::BitAssetRegistration {
                name_hash,
                revealed_nonce,
                ..
            }) => {
                let implied_commitment =
                    blake3::keyed_hash(&revealed_nonce, &name_hash).into();
                Some(implied_commitment)
            }
            _ => None,
        }
    }

    /// Return an iterator over reservation outputs
    pub fn reservation_outputs(
        &self,
    ) -> impl DoubleEndedIterator<Item = &Output> {
        self.outputs.iter().filter(|output| output.is_reservation())
    }

    pub fn txid(&self) -> Txid {
        hashes::hash(self).into()
    }

    /// If the tx is a bitasset reservation, returns the reservation commitment
    pub fn reservation_commitment(&self) -> Option<Hash> {
        match self.data {
            Some(TxData::BitAssetReservation { commitment }) => {
                Some(commitment)
            }
            _ => None,
        }
    }
}

impl FilledContent {
    /** Returns the BitAsset ID (name hash) and if the filled
     * output content corresponds to a BitAsset. */
    pub fn bitasset(&self) -> Option<&Hash> {
        match self {
            Self::BitAsset(name_hash, _) => Some(name_hash),
            _ => None,
        }
    }

    /** Returns the BitAsset ID (name hash) and if the filled
     * output content corresponds to a BitAsset or BitAsset control coin. */
    pub fn get_bitasset(&self) -> Option<Hash> {
        match self {
            Self::BitAsset(name_hash, _) | Self::BitAssetControl(name_hash) => {
                Some(*name_hash)
            }
            _ => None,
        }
    }

    /** Returns the BitAsset ID (name hash) and coin value, if the filled
     *  output content corresponds to a BitAsset output. */
    pub fn bitasset_value(&self) -> Option<(Hash, u64)> {
        match self {
            Self::BitAsset(name_hash, value) => Some((*name_hash, *value)),
            _ => None,
        }
    }

    /** Returns the Dutch auction ID, if the filled output content corresponds
     *  to a Dutch auction receipt output. */
    pub fn dutch_auction_receipt(&self) -> Option<DutchAuctionId> {
        match self {
            Self::DutchAuctionReceipt(auction_id) => Some(*auction_id),
            _ => None,
        }
    }

    /** Returns the LP token's corresponding asset pair and amount,
     *  if the filled output content corresponds to an LP token output. */
    pub fn lp_token_amount(&self) -> Option<(Hash, Hash, u64)> {
        match self {
            Self::AmmLpToken {
                asset0,
                asset1,
                amount,
            } => Some((*asset0, *asset1, *amount)),
            _ => None,
        }
    }

    /// `true` if the output content corresponds to a BitAsset
    pub fn is_bitasset(&self) -> bool {
        matches!(self, Self::BitAsset(_, _))
    }

    /// `true` if the output content corresponds to a BitAsset control coin
    pub fn is_bitasset_control(&self) -> bool {
        matches!(self, Self::BitAssetControl(_))
    }

    /// `true` if the output content corresponds to a Bitcoin
    pub fn is_bitcoin(&self) -> bool {
        matches!(self, Self::Bitcoin(_))
    }

    /// `true` if the output content corresponds to a Dutch auction receipt
    pub fn is_dutch_auction_receipt(&self) -> bool {
        matches!(self, Self::DutchAuctionReceipt(_))
    }

    /// `true` if the output content corresponds to an LP token
    pub fn is_lp_token(&self) -> bool {
        matches!(self, Self::AmmLpToken { .. })
    }

    /// `true` if the output content corresponds to a reservation
    pub fn is_reservation(&self) -> bool {
        matches!(self, Self::BitAssetReservation { .. })
    }

    /// `true` if the output content corresponds to a withdrawal
    pub fn is_withdrawal(&self) -> bool {
        matches!(self, Self::BitcoinWithdrawal { .. })
    }

    /** Returns the reservation txid and commitment if the filled output
     * content corresponds to a BitAsset reservation output. */
    pub fn reservation_data(&self) -> Option<(&Txid, &Hash)> {
        match self {
            Self::BitAssetReservation(txid, commitment) => {
                Some((txid, commitment))
            }
            _ => None,
        }
    }

    /** Returns the reservation commitment if the filled output content
     *  corresponds to a BitAsset reservation output. */
    pub fn reservation_commitment(&self) -> Option<&Hash> {
        self.reservation_data().map(|(_, commitment)| commitment)
    }
}

impl From<FilledContent> for Content {
    fn from(filled: FilledContent) -> Self {
        match filled {
            FilledContent::AmmLpToken {
                asset0: _,
                asset1: _,
                amount,
            } => Content::AmmLpToken(amount),
            FilledContent::Bitcoin(value) => Content::Value(value),
            FilledContent::BitcoinWithdrawal {
                value,
                main_fee,
                main_address,
            } => Content::Withdrawal {
                value,
                main_fee,
                main_address,
            },
            FilledContent::BitAsset(_, value) => Content::BitAsset(value),
            FilledContent::BitAssetControl(_) => Content::BitAssetControl,
            FilledContent::BitAssetReservation { .. } => {
                Content::BitAssetReservation
            }
            FilledContent::DutchAuctionReceipt(_) => {
                Content::DutchAuctionReceipt
            }
        }
    }
}

impl GetBitcoinValue for FilledContent {
    fn get_bitcoin_value(&self) -> u64 {
        Content::from(self.clone()).get_bitcoin_value()
    }
}

impl FilledOutput {
    /// Construct a new filled output
    pub fn new(address: Address, content: FilledContent) -> Self {
        Self {
            address,
            content,
            memo: Vec::new(),
        }
    }

    /** Returns the BitAsset ID (name hash) if the filled output content
     * corresponds to a BitAsset */
    pub fn bitasset(&self) -> Option<&Hash> {
        self.content.bitasset()
    }

    /** Returns the BitAsset ID (name hash) if the filled output content
     * corresponds to a BitAsset or BitAsset control coin. */
    pub fn get_bitasset(&self) -> Option<Hash> {
        self.content.get_bitasset()
    }

    /** Returns the BitAsset ID (name hash) and coin value
     * if the filled output content corresponds to a BitAsset output. */
    pub fn bitasset_value(&self) -> Option<(Hash, u64)> {
        self.content.bitasset_value()
    }

    /** Returns the Dutch auction ID, if the filled output content corresponds
     *  to a Dutch auction receipt output. */
    pub fn dutch_auction_receipt(&self) -> Option<DutchAuctionId> {
        self.content.dutch_auction_receipt()
    }

    /** Returns the LP token's corresponding asset pair and amount,
     *  if the filled output content corresponds to an LP token output. */
    pub fn lp_token_amount(&self) -> Option<(Hash, Hash, u64)> {
        self.content.lp_token_amount()
    }

    /// Accessor for content
    pub fn content(&self) -> &FilledContent {
        &self.content
    }

    /// `true` if the output content corresponds to a BitAsset
    pub fn is_bitasset(&self) -> bool {
        self.content.is_bitasset()
    }

    /// `true` if the output content corresponds to a BitAsset control coin
    pub fn is_bitasset_control(&self) -> bool {
        self.content.is_bitasset_control()
    }

    /// `true` if the output content corresponds to a Bitcoin
    pub fn is_bitcoin(&self) -> bool {
        self.content.is_bitcoin()
    }

    /// `true` if the output content corresponds to a Dutch auction receipt
    pub fn is_dutch_auction_receipt(&self) -> bool {
        self.content.is_dutch_auction_receipt()
    }

    /// `true` if the output content corresponds to an LP token
    pub fn is_lp_token(&self) -> bool {
        self.content.is_lp_token()
    }

    /// True if the output content corresponds to a reservation
    pub fn is_reservation(&self) -> bool {
        self.content.is_reservation()
    }

    /** Returns the reservation txid and commitment if the filled output
     *  content corresponds to a BitAsset reservation output. */
    pub fn reservation_data(&self) -> Option<(&Txid, &Hash)> {
        self.content.reservation_data()
    }

    /** Returns the reservation commitment if the filled output content
     *  corresponds to a BitAsset reservation output. */
    pub fn reservation_commitment(&self) -> Option<&Hash> {
        self.content.reservation_commitment()
    }
}

impl From<FilledOutput> for Output {
    fn from(filled: FilledOutput) -> Self {
        Self {
            address: filled.address,
            content: filled.content.into(),
            memo: filled.memo,
        }
    }
}

impl GetBitcoinValue for FilledOutput {
    fn get_bitcoin_value(&self) -> u64 {
        self.content.get_bitcoin_value()
    }
}

impl FilledTransaction {
    // Return an iterator over BitAsset outputs
    pub fn bitasset_outputs(&self) -> impl Iterator<Item = &Output> {
        self.transaction.bitasset_outputs()
    }

    // Return an iterator over BitAsset control outputs
    pub fn bitasset_control_outputs(&self) -> impl Iterator<Item = &Output> {
        self.transaction.bitasset_control_outputs()
    }

    /// Accessor for tx data
    pub fn data(&self) -> &Option<TxData> {
        &self.transaction.data
    }

    /** If the tx is a bitasset registration, returns the implied reservation
     * commitment */
    pub fn implied_reservation_commitment(&self) -> Option<Hash> {
        self.transaction.implied_reservation_commitment()
    }

    /// Accessor for tx outputs
    pub fn inputs(&self) -> &TxInputs {
        &self.transaction.inputs
    }

    /// `true` if the tx data corresponds to an AMM burn
    pub fn is_amm_burn(&self) -> bool {
        self.transaction.is_amm_burn()
    }

    /// `true` if the tx data corresponds to an AMM mint
    pub fn is_amm_mint(&self) -> bool {
        self.transaction.is_amm_mint()
    }

    /// `true` if the tx data corresponds to an AMM swap
    pub fn is_amm_swap(&self) -> bool {
        self.transaction.is_amm_swap()
    }

    /// `true` if the tx data corresponds to a Dutch auction bid
    pub fn is_dutch_auction_bid(&self) -> bool {
        self.transaction.is_dutch_auction_bid()
    }

    /// `true` if the tx data corresponds to a Dutch auction creation
    pub fn is_dutch_auction_create(&self) -> bool {
        self.transaction.is_dutch_auction_create()
    }

    /// `true` if the tx data corresponds to a Dutch auction collect
    pub fn is_dutch_auction_collect(&self) -> bool {
        self.transaction.is_dutch_auction_collect()
    }

    /// `true` if the tx data corresponds to a BitAsset registration
    pub fn is_registration(&self) -> bool {
        self.transaction.is_registration()
    }

    /// `true` if the tx data corresponds to a regular tx
    pub fn is_regular(&self) -> bool {
        self.transaction.is_regular()
    }

    /// `true` if the tx data corresponds to a BitAsset reservation
    pub fn is_reservation(&self) -> bool {
        self.transaction.is_reservation()
    }

    /// `true` if the tx data corresponds to a BitAsset update
    pub fn is_update(&self) -> bool {
        self.transaction.is_update()
    }

    /// Accessor for tx outputs
    pub fn outputs(&self) -> &TxOutputs {
        &self.transaction.outputs
    }

    /** If the tx is an AMM burn, returns the LP token's
     *  corresponding [`AmmBurn`]. */
    pub fn amm_burn(&self) -> Option<AmmBurn> {
        match self.transaction.data {
            Some(TransactionData::AmmBurn {
                amount0,
                amount1,
                lp_token_burn,
            }) => {
                let unique_spent_lp_tokens = self.unique_spent_lp_tokens();
                let (asset0, asset1, _) = unique_spent_lp_tokens.first()?;
                Some(AmmBurn {
                    asset0: *asset0,
                    asset1: *asset1,
                    amount0,
                    amount1,
                    lp_token_burn,
                })
            }
            _ => None,
        }
    }

    /// If the tx is an AMM mint, returns the corresponding [`AmmMint`].
    pub fn amm_mint(&self) -> Option<AmmMint> {
        match self.transaction.data {
            Some(TransactionData::AmmMint {
                amount0,
                amount1,
                lp_token_mint,
            }) => match self.unique_spent_bitassets().get(0..=1) {
                Some([(first_asset, _), (second_asset, _)]) => {
                    let mut assets = [first_asset, second_asset];
                    assets.sort();
                    let [asset0, asset1] = assets;
                    Some(AmmMint {
                        asset0: *asset0,
                        asset1: *asset1,
                        amount0,
                        amount1,
                        lp_token_mint,
                    })
                }
                _ => None,
            },
            _ => None,
        }
    }

    /// If the tx is an AMM swap, returns the corresponding [`AmmSwap`].
    pub fn amm_swap(&self) -> Option<AmmSwap> {
        match self.transaction.data {
            Some(TransactionData::AmmSwap {
                amount_spent,
                amount_receive,
                pair_asset,
            }) => {
                let (spent_bitasset, _) =
                    *self.unique_spent_bitassets().first()?;
                Some(AmmSwap {
                    asset_spend: spent_bitasset,
                    asset_receive: pair_asset,
                    amount_spend: amount_spent,
                    amount_receive,
                })
            }
            _ => None,
        }
    }

    /** If the tx is a valid BitAsset mint,
     *  returns the BitAsset ID and mint amount */
    pub fn bitasset_mint(&self) -> Option<(Hash, u64)> {
        match self.transaction.data {
            Some(TransactionData::BitAssetMint(amount)) => {
                let (_, control_output) =
                    self.spent_bitasset_controls().next_back()?;
                let bitasset = control_output.get_bitasset()?;
                Some((bitasset, amount))
            }
            _ => None,
        }
    }

    /** If the tx is a Dutch auction bid,
     *  returns the corresponding [`DutchAuctionBid`]. */
    pub fn dutch_auction_bid(&self) -> Option<DutchAuctionBid> {
        match self.transaction.data {
            Some(TransactionData::DutchAuctionBid {
                auction_id,
                receive_asset,
                quantity,
                bid_size,
            }) => {
                let unique_spent_bitassets = self.unique_spent_bitassets();
                let (asset_spend, _) = unique_spent_bitassets.first()?;
                Some(DutchAuctionBid {
                    auction_id,
                    asset_spend: *asset_spend,
                    asset_receive: receive_asset,
                    amount_spend: bid_size,
                    amount_receive: quantity,
                })
            }
            _ => None,
        }
    }

    /** If the tx is a Dutch auction creation,
     *  returns the corresponding [`DutchAuctionParams`]. */
    pub fn dutch_auction_create(&self) -> Option<DutchAuctionParams> {
        match self.transaction.data {
            Some(TransactionData::DutchAuctionCreate(dutch_auction_params)) => {
                Some(dutch_auction_params)
            }
            _ => None,
        }
    }

    /** If the tx is a Dutch auction collect,
     *  returns the corresponding [`DutchAuctionCollect`]. */
    pub fn dutch_auction_collect(&self) -> Option<DutchAuctionCollect> {
        match self.transaction.data {
            Some(TransactionData::DutchAuctionCollect {
                asset_offered,
                asset_receive,
                amount_offered_remaining,
                amount_received,
            }) => {
                let mut spent_dutch_auction_receipts =
                    self.spent_dutch_auction_receipts();
                let auction_id = spent_dutch_auction_receipts
                    .next()?
                    .1
                    .dutch_auction_receipt()?;
                Some(DutchAuctionCollect {
                    auction_id,
                    asset_offered,
                    asset_receive,
                    amount_offered_remaining,
                    amount_received,
                })
            }
            _ => None,
        }
    }

    /// If the tx is a BitAsset registration, returns the registered name hash
    pub fn registration_name_hash(&self) -> Option<Hash> {
        self.transaction.registration_name_hash()
    }

    /// Return an iterator over BitAsset reservation outputs
    pub fn reservation_outputs(&self) -> impl Iterator<Item = &Output> {
        self.transaction.reservation_outputs()
    }

    /// If the tx is a BitAsset reservation, returns the reservation commitment
    pub fn reservation_commitment(&self) -> Option<Hash> {
        self.transaction.reservation_commitment()
    }

    /// Rccessor for txid
    pub fn txid(&self) -> Txid {
        self.transaction.txid()
    }

    /// Return an iterator over spent outpoints/outputs
    pub fn spent_inputs(
        &self,
    ) -> impl DoubleEndedIterator<Item = (&OutPoint, &FilledOutput)> {
        self.inputs().iter().zip(self.spent_utxos.iter())
    }

    /// Returns the total Bitcoin value spent
    pub fn spent_bitcoin_value(&self) -> u64 {
        self.spent_utxos
            .iter()
            .map(GetBitcoinValue::get_bitcoin_value)
            .sum()
    }

    /// Returns the total Bitcoin value in the outputs
    pub fn bitcoin_value_out(&self) -> u64 {
        self.outputs()
            .iter()
            .map(GetBitcoinValue::get_bitcoin_value)
            .sum()
    }

    /** Returns the difference between the value spent and value out, if it is
     * non-negative. */
    pub fn bitcoin_fee(&self) -> Option<u64> {
        let spent_value = self.spent_bitcoin_value();
        let value_out = self.bitcoin_value_out();
        if spent_value < value_out {
            None
        } else {
            Some(spent_value - value_out)
        }
    }
    /// Return an iterator over spent reservations
    pub fn spent_reservations(
        &self,
    ) -> impl Iterator<Item = (&OutPoint, &FilledOutput)> {
        self.spent_inputs()
            .filter(|(_, filled_output)| filled_output.is_reservation())
    }

    /// Return an iterator over spent BitAssets
    pub fn spent_bitassets(
        &self,
    ) -> impl DoubleEndedIterator<Item = (&OutPoint, &FilledOutput)> {
        self.spent_inputs()
            .filter(|(_, filled_output)| filled_output.is_bitasset())
    }

    /** Return a vector of pairs consisting of a BitAsset and the combined
     *  input value for that BitAsset.
     *  The vector is ordered such that BitAssets occur in the same order
     *  as they first occur in the inputs. */
    pub fn unique_spent_bitassets(&self) -> Vec<(Hash, u64)> {
        // Combined value for each BitAsset
        let mut combined_value = HashMap::<Hash, u64>::new();
        let spent_bitasset_values = || {
            self.spent_bitassets()
                .filter_map(|(_, output)| output.bitasset_value())
        };
        // Increment combined value for the BitAsset
        spent_bitasset_values().for_each(|(bitasset, value)| {
            *combined_value.entry(bitasset).or_default() += value;
        });
        spent_bitasset_values()
            .unique_by(|(bitasset, _)| *bitasset)
            .map(|(bitasset, _)| (bitasset, combined_value[&bitasset]))
            .collect()
    }

    /// Return an iterator over spent BitAsset control coins
    pub fn spent_bitasset_controls(
        &self,
    ) -> impl DoubleEndedIterator<Item = (&OutPoint, &FilledOutput)> {
        self.spent_inputs()
            .filter(|(_, filled_output)| filled_output.is_bitasset_control())
    }

    /// Return an iterator over spent Dutch auction receipts
    pub fn spent_dutch_auction_receipts(
        &self,
    ) -> impl DoubleEndedIterator<Item = (&OutPoint, &FilledOutput)> {
        self.spent_inputs().filter(|(_, filled_output)| {
            filled_output.is_dutch_auction_receipt()
        })
    }

    /// Return an iterator over spent AMM LP tokens
    pub fn spent_lp_tokens(
        &self,
    ) -> impl DoubleEndedIterator<Item = (&OutPoint, &FilledOutput)> {
        self.spent_inputs()
            .filter(|(_, filled_output)| filled_output.is_lp_token())
    }

    /** Return a vector of pairs consisting of an LP token's corresponding
     *  asset pair and the combined input amount for that LP token.
     *  The vector is ordered such that LP tokens occur in the same order
     *  as they first occur in the inputs. */
    pub fn unique_spent_lp_tokens(&self) -> Vec<(Hash, Hash, u64)> {
        // Combined amount for each LP token
        let mut combined_amounts = HashMap::<(Hash, Hash), u64>::new();
        let spent_lp_token_amounts = || {
            self.spent_lp_tokens()
                .filter_map(|(_, output)| output.lp_token_amount())
        };
        // Increment combined amount for the BitAsset
        spent_lp_token_amounts().for_each(|(asset0, asset1, amount)| {
            *combined_amounts.entry((asset0, asset1)).or_default() += amount;
        });
        spent_lp_token_amounts()
            .unique_by(|(asset0, asset1, _)| (*asset0, *asset1))
            .map(|(asset0, asset1, _)| {
                (asset0, asset1, combined_amounts[&(asset0, asset1)])
            })
            .collect()
    }

    /** Returns an iterator over total value for each BitAsset that must
     *  appear in the outputs, in order.
     *  The total output value can possibly over/underflow in a transaction,
     *  so the total output values are [`Option<u64>`],
     *  where `None` indicates over/underflow. */
    fn output_bitasset_total_values(
        &self,
    ) -> impl Iterator<Item = (Hash, Option<u64>)> + '_ {
        /* If this tx is a BitAsset registration, this is the BitAsset ID and
         * value of the output corresponding to the newly created BitAsset,
         * which must be the second-to-last registration output.
         * ie. If there are `n >= 2` outputs `0..(n-1)`,
         * output `(n-1)` is the BitAsset control coin,
         * and output `(n-2)` is the BitAsset mint.
         * Note that there may be no BitAsset mint,
         * in the case that the initial supply is zero. */
        let new_bitasset_value: Option<(Hash, u64)> =
            match self.transaction.data {
                Some(TransactionData::BitAssetRegistration {
                    name_hash,
                    initial_supply,
                    ..
                }) if initial_supply != 0 => Some((name_hash, initial_supply)),
                _ => None,
            };
        let bitasset_mint = self.bitasset_mint();
        let (mut amm_burn0, mut amm_burn1) = match self.amm_burn() {
            Some(AmmBurn {
                asset0,
                asset1,
                amount0,
                amount1,
                lp_token_burn: _,
            }) => (Some((asset0, amount0)), Some((asset1, amount1))),
            None => (None, None),
        };
        let (mut amm_mint0, mut amm_mint1) = match self.amm_mint() {
            Some(AmmMint {
                asset0,
                asset1,
                amount0,
                amount1,
                lp_token_mint: _,
            }) => (Some((asset0, amount0)), Some((asset1, amount1))),
            None => (None, None),
        };
        let (mut amm_swap_spend, mut amm_swap_receive) = match self.amm_swap() {
            Some(AmmSwap {
                asset_spend,
                asset_receive,
                amount_spend,
                amount_receive,
            }) => (
                Some((asset_spend, amount_spend)),
                Some((asset_receive, amount_receive)),
            ),
            None => (None, None),
        };
        let (mut dutch_auction_bid_spend, mut dutch_auction_bid_receive) =
            match self.dutch_auction_bid() {
                Some(DutchAuctionBid {
                    auction_id: _,
                    asset_spend,
                    asset_receive,
                    amount_spend,
                    amount_receive,
                }) => (
                    Some((asset_spend, amount_spend)),
                    Some((asset_receive, amount_receive)),
                ),
                None => (None, None),
            };
        let mut dutch_auction_create_spend =
            self.dutch_auction_create().map(|auction_params| {
                (auction_params.base_asset, auction_params.base_amount)
            });
        let (mut dutch_auction_collect0, mut dutch_auction_collect1) =
            match self.dutch_auction_collect() {
                Some(DutchAuctionCollect {
                    auction_id: _,
                    asset_offered,
                    asset_receive,
                    amount_offered_remaining,
                    amount_received,
                }) => (
                    Some((asset_offered, amount_offered_remaining)),
                    Some((asset_receive, amount_received)),
                ),
                None => (None, None),
            };
        self.unique_spent_bitassets()
            .into_iter()
            .map(move |(bitasset, total_value)| {
                let total_value = if let Some((mint_bitasset, mint_amount)) =
                    bitasset_mint
                    && mint_bitasset == bitasset
                {
                    total_value.checked_add(mint_amount)
                } else if let Some((burn_asset, burn_amount)) = amm_burn0
                    && burn_asset == bitasset
                {
                    amm_burn0 = None;
                    total_value.checked_add(burn_amount)
                } else if let Some((burn_asset, burn_amount)) = amm_burn1
                    && burn_asset == bitasset
                {
                    amm_burn1 = None;
                    total_value.checked_add(burn_amount)
                } else if let Some((mint_asset, mint_amount)) = amm_mint0
                    && mint_asset == bitasset
                {
                    amm_mint0 = None;
                    total_value.checked_sub(mint_amount)
                } else if let Some((mint_asset, mint_amount)) = amm_mint1
                    && mint_asset == bitasset
                {
                    amm_mint1 = None;
                    total_value.checked_sub(mint_amount)
                } else if let Some((swap_spend_asset, swap_spend_amount)) =
                    amm_swap_spend
                    && swap_spend_asset == bitasset
                {
                    amm_swap_spend = None;
                    total_value.checked_sub(swap_spend_amount)
                } else if let Some((swap_receive_asset, swap_receive_amount)) =
                    amm_swap_receive
                    && swap_receive_asset == bitasset
                {
                    amm_swap_receive = None;
                    total_value.checked_add(swap_receive_amount)
                } else if let Some((receive_asset, receive_amount)) =
                    dutch_auction_bid_receive
                    && receive_asset == bitasset
                {
                    dutch_auction_bid_receive = None;
                    total_value.checked_add(receive_amount)
                } else if let Some((spend_asset, spend_amount)) =
                    dutch_auction_bid_spend
                    && spend_asset == bitasset
                {
                    dutch_auction_bid_spend = None;
                    total_value.checked_sub(spend_amount)
                } else if let Some((spend_asset, spend_amount)) =
                    dutch_auction_create_spend
                    && spend_asset == bitasset
                {
                    dutch_auction_create_spend = None;
                    total_value.checked_sub(spend_amount)
                } else if let Some((receive_asset, receive_amount)) =
                    dutch_auction_collect0
                    && receive_asset == bitasset
                {
                    dutch_auction_collect0 = None;
                    total_value.checked_add(receive_amount)
                } else if let Some((receive_asset, receive_amount)) =
                    dutch_auction_collect1
                    && receive_asset == bitasset
                {
                    dutch_auction_collect1 = None;
                    total_value.checked_add(receive_amount)
                } else {
                    Some(total_value)
                };
                (bitasset, total_value)
            })
            .filter(|(_, amount)| *amount != Some(0))
            .chain(amm_burn0.map(|(burn_asset, burn_amount)| {
                (burn_asset, Some(burn_amount))
            }))
            .chain(amm_burn1.map(|(burn_asset, burn_amount)| {
                (burn_asset, Some(burn_amount))
            }))
            .chain(amm_mint0.map(|(mint_asset, _)|
                    /* If the BitAssets are not already accounted for,
                    * indicate an underflow */
                    (mint_asset, None)))
            .chain(amm_mint1.map(|(mint_asset, _)|
                    /* If the BitAssets are not already accounted for,
                    * indicate an underflow */
                    (mint_asset, None)))
            .chain(amm_swap_spend.map(|(spend_asset, _)|
                    /* If the BitAssets are not already accounted for,
                    * indicate an underflow */
                    (spend_asset, None)))
            .chain(amm_swap_receive.map(|(receive_asset, receive_amount)| {
                (receive_asset, Some(receive_amount))
            }))
            .chain(dutch_auction_bid_receive.map(
                |(receive_asset, receive_amount)| {
                    (receive_asset, Some(receive_amount))
                },
            ))
            .chain(dutch_auction_bid_spend.map(|(spend_asset, _)|
                    /* If the BitAssets are not already accounted for,
                    * indicate an underflow */
                    (spend_asset, None)))
            .chain(dutch_auction_create_spend.map(|(spend_asset, _)|
                    /* If the BitAssets are not already accounted for,
                    * indicate an underflow */
                    (spend_asset, None)))
            .chain(dutch_auction_collect0.map(
                |(receive_asset, receive_amount)| {
                    (receive_asset, Some(receive_amount))
                },
            ))
            .chain(dutch_auction_collect1.map(
                |(receive_asset, receive_amount)| {
                    (receive_asset, Some(receive_amount))
                },
            ))
            .chain(
                new_bitasset_value.map(|(bitasset, total_value)| {
                    (bitasset, Some(total_value))
                }),
            )
    }

    /** Returns an iterator over total amount for each LP token that must
     *  appear in the outputs, in order.
     *  The total output value can possibly over/underflow,
     *  so the total output values are [`Option<u64>`],
     *  where `None` indicates over/underflow. */
    fn output_lp_token_total_amounts(
        &self,
    ) -> impl Iterator<Item = (Hash, Hash, Option<u64>)> + '_ {
        /* If this tx is an AMM burn, this is the corresponding BitAsset IDs
        and token amount of the output corresponding to the newly created
        AMM LP position. */
        let mut amm_burn: Option<AmmBurn> = self.amm_burn();
        /* If this tx is an AMM mint, this is the corresponding BitAsset IDs
        and token amount of the output corresponding to the newly created
        AMM LP position. */
        let mut amm_mint: Option<AmmMint> = self.amm_mint();
        self.unique_spent_lp_tokens()
            .into_iter()
            .map(move |(asset0, asset1, total_amount)| {
                let total_value = if let Some(AmmBurn {
                    asset0: burn_asset0,
                    asset1: burn_asset1,
                    amount0: _,
                    amount1: _,
                    lp_token_burn,
                }) = amm_burn
                    && (burn_asset0, burn_asset1) == (asset0, asset1)
                {
                    amm_burn = None;
                    total_amount.checked_sub(lp_token_burn)
                } else if let Some(AmmMint {
                    asset0: mint_asset0,
                    asset1: mint_asset1,
                    amount0: _,
                    amount1: _,
                    lp_token_mint,
                }) = amm_mint
                    && (mint_asset0, mint_asset1) == (asset0, asset1)
                {
                    amm_mint = None;
                    total_amount.checked_add(lp_token_mint)
                } else {
                    Some(total_amount)
                };
                (asset0, asset1, total_value)
            })
            .chain(amm_burn.map(|amm_burn| {
                /* If the LP tokens are not already accounted for,
                 * indicate an underflow */
                (amm_burn.asset0, amm_burn.asset1, None)
            }))
            .chain(amm_mint.map(|amm_mint| {
                (
                    amm_mint.asset0,
                    amm_mint.asset1,
                    Some(amm_mint.lp_token_mint),
                )
            }))
    }

    /** Compute the filled content for BitAsset reservation outputs.
     *  WARNING: do not expose DoubleEndedIterator. */
    fn filled_bitasset_control_output_content(
        &self,
    ) -> impl Iterator<Item = FilledContent> + '_ {
        /* If this tx is a BitAsset registration, this is the content of the
         * output corresponding to the newly created BitAsset control,
         * which must be the last registration output.
         * ie. If there are `n` outputs `0..(n-1)`, then output `(n-1)`
         * is the BitAsset control coin. */
        let new_bitasset_control_content: Option<FilledContent> = self
            .registration_name_hash()
            .map(FilledContent::BitAssetControl);
        self.spent_bitasset_controls()
            .map(|(_, filled_output)| filled_output.content())
            .cloned()
            .chain(new_bitasset_control_content)
    }

    /// Compute the filled content for Dutch auction receipt outputs.
    // WARNING: do not expose DoubleEndedIterator.
    fn filled_dutch_auction_receipts(
        &self,
    ) -> impl Iterator<Item = FilledContent> + '_ {
        /* If this tx is a Dutch auction creation, this is the content of the
         * output corresponding to the newly created Dutch auction receipt,
         * which is the last Dutch auction receipt output. */
        let new_dutch_auction_receipt_content =
            if self.is_dutch_auction_create() {
                let auction_id = DutchAuctionId(self.txid());
                Some(FilledContent::DutchAuctionReceipt(auction_id))
            } else {
                None
            };
        let mut spent_dutch_auction_receipts =
            self.spent_dutch_auction_receipts();
        /* If this tx is a Dutch auction collect,
        the first auction receipt is burned */
        if self.is_dutch_auction_collect() {
            let _ = spent_dutch_auction_receipts.next();
        }
        spent_dutch_auction_receipts
            .map(|(_, filled_output)| filled_output.content())
            .cloned()
            .chain(new_dutch_auction_receipt_content)
    }

    /// compute the filled content for BitAsset reservation outputs
    /// WARNING: do not expose DoubleEndedIterator.
    fn filled_reservation_output_content(
        &self,
    ) -> impl Iterator<Item = FilledContent> + '_ {
        // If this tx is a BitAsset reservation, this is the content of the
        // output corresponding to the newly created BitAsset reservation,
        // which must be the final reservation output.
        let new_reservation_content: Option<FilledContent> =
            self.reservation_commitment().map(|commitment| {
                FilledContent::BitAssetReservation(self.txid(), commitment)
            });
        // used to track if the reservation that should be burned as part
        // of a registration tx
        let mut reservation_to_burn: Option<Hash> =
            self.implied_reservation_commitment();
        self.spent_reservations()
            .map(|(_, filled_output)| filled_output.content())
            // In the event of a registration, the first corresponding
            // reservation does not occur in the output
            .filter(move |content| {
                if let Some(implied_commitment) = reservation_to_burn {
                    if matches!(
                        content,
                        FilledContent::BitAssetReservation(_, commitment)
                            if *commitment == implied_commitment)
                    {
                        reservation_to_burn = None;
                        false
                    } else {
                        true
                    }
                } else {
                    true
                }
            })
            .cloned()
            .chain(new_reservation_content)
    }

    /// compute the filled outputs.
    /// returns None if the outputs cannot be filled because the tx is invalid
    // FIXME: Invalidate tx if any iterator is incomplete
    pub fn filled_outputs(&self) -> Option<Vec<FilledOutput>> {
        let mut output_bitasset_total_values =
            self.output_bitasset_total_values().peekable();
        let mut output_lp_token_total_amounts =
            self.output_lp_token_total_amounts().peekable();
        let mut filled_bitasset_control_output_content =
            self.filled_bitasset_control_output_content();
        let mut filled_dutch_auction_receipts =
            self.filled_dutch_auction_receipts();
        let mut filled_reservation_output_content =
            self.filled_reservation_output_content();
        self.outputs()
            .iter()
            .map(|output| {
                let content = match output.content.clone() {
                    Content::AmmLpToken(amount) => {
                        let (asset0, asset1, remaining_amount) =
                            output_lp_token_total_amounts.peek_mut()?;
                        let remaining_amount = remaining_amount.as_mut()?;
                        let filled_content = FilledContent::AmmLpToken {
                            asset0: *asset0,
                            asset1: *asset1,
                            amount,
                        };
                        match amount.cmp(remaining_amount) {
                            Ordering::Greater => {
                                // Invalid tx, return `None`
                                return None;
                            }
                            Ordering::Equal => {
                                // Advance the iterator to the next LP token
                                let _ = output_lp_token_total_amounts.next()?;
                            }
                            Ordering::Less => {
                                // Decrement the remaining value for the current LP token
                                *remaining_amount -= amount;
                            }
                        }
                        filled_content
                    }
                    Content::BitAsset(value) => {
                        let (bitasset, remaining_value) =
                            output_bitasset_total_values.peek_mut()?;
                        let remaining_value = remaining_value.as_mut()?;
                        let filled_content =
                            FilledContent::BitAsset(*bitasset, value);
                        match value.cmp(remaining_value) {
                            Ordering::Greater => {
                                // Invalid tx, return `None`
                                return None;
                            }
                            Ordering::Equal => {
                                // Advance the iterator to the next BitAsset
                                let _ = output_bitasset_total_values.next()?;
                            }
                            Ordering::Less => {
                                // Decrement the remaining value for the current BitAsset
                                *remaining_value -= value;
                            }
                        }
                        filled_content
                    }
                    Content::BitAssetControl => {
                        filled_bitasset_control_output_content.next()?.clone()
                    }
                    Content::BitAssetReservation => {
                        filled_reservation_output_content.next()?.clone()
                    }
                    Content::DutchAuctionReceipt => {
                        filled_dutch_auction_receipts.next()?
                    }
                    Content::Value(value) => FilledContent::Bitcoin(value),
                    Content::Withdrawal {
                        value,
                        main_fee,
                        main_address,
                    } => FilledContent::BitcoinWithdrawal {
                        value,
                        main_fee,
                        main_address,
                    },
                };
                Some(FilledOutput {
                    address: output.address,
                    content,
                    memo: output.memo.clone(),
                })
            })
            .collect()
    }
}
