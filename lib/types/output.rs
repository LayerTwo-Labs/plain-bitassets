use bip300301::bitcoin;
use borsh::BorshSerialize;
use serde::{Deserialize, Serialize};

use super::{
    serde_display_fromstr_human_readable, serde_hexstr_human_readable, Address,
    AssetId, BitAssetId, DutchAuctionId, GetBitcoinValue, Hash, InPoint, Txid,
};

#[derive(
    BorshSerialize, Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct BitcoinOutputContent(pub u64);

// The subset of output contents that correspond to assets
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AssetOutputContent {
    BitAsset(u64),
    BitAssetControl,
    Value(BitcoinOutputContent),
    Withdrawal {
        value: u64,
        main_fee: u64,
        main_address: bitcoin::Address<bitcoin::address::NetworkUnchecked>,
    },
}

impl From<BitcoinOutputContent> for AssetOutputContent {
    fn from(content: BitcoinOutputContent) -> Self {
        Self::Value(content)
    }
}

fn borsh_serialize_bitcoin_address<V, W>(
    bitcoin_address: &bitcoin::Address<V>,
    writer: &mut W,
) -> borsh::io::Result<()>
where
    V: bitcoin::address::NetworkValidation,
    W: borsh::io::Write,
{
    let spk = bitcoin_address
        .as_unchecked()
        .assume_checked_ref()
        .script_pubkey();
    borsh::BorshSerialize::serialize(spk.as_bytes(), writer)
}

#[derive(
    BorshSerialize, Clone, Debug, Deserialize, Eq, PartialEq, Serialize,
)]
pub enum OutputContent {
    AmmLpToken(u64),
    BitAsset(u64),
    BitAssetControl,
    BitAssetReservation,
    /// Receipt used to redeem the proceeds of an auction
    DutchAuctionReceipt,
    Value(BitcoinOutputContent),
    Withdrawal {
        value: u64,
        main_fee: u64,
        #[borsh(serialize_with = "borsh_serialize_bitcoin_address")]
        main_address: bitcoin::Address<bitcoin::address::NetworkUnchecked>,
    },
}

impl OutputContent {
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

    /// `true` if the output corresponds to an asset output
    pub fn is_asset(&self) -> bool {
        matches!(
            self,
            Self::BitAsset(_)
                | Self::BitAssetControl
                | Self::Value(_)
                | Self::Withdrawal { .. }
        )
    }
}

impl From<BitcoinOutputContent> for OutputContent {
    fn from(content: BitcoinOutputContent) -> Self {
        Self::Value(content)
    }
}

impl From<AssetOutputContent> for OutputContent {
    fn from(content: AssetOutputContent) -> Self {
        match content {
            AssetOutputContent::BitAsset(value) => Self::BitAsset(value),
            AssetOutputContent::BitAssetControl => Self::BitAssetControl,
            AssetOutputContent::Value(value) => Self::Value(value),
            AssetOutputContent::Withdrawal {
                value,
                main_fee,
                main_address,
            } => Self::Withdrawal {
                value,
                main_fee,
                main_address,
            },
        }
    }
}

impl From<OutputContent> for Option<BitcoinOutputContent> {
    fn from(content: OutputContent) -> Option<BitcoinOutputContent> {
        match content {
            OutputContent::Value(value) => Some(value),
            _ => None,
        }
    }
}

impl From<OutputContent> for Option<AssetOutputContent> {
    fn from(content: OutputContent) -> Option<AssetOutputContent> {
        match content {
            OutputContent::BitAsset(value) => {
                Some(AssetOutputContent::BitAsset(value))
            }
            OutputContent::BitAssetControl => {
                Some(AssetOutputContent::BitAssetControl)
            }
            OutputContent::Value(value) => {
                Some(AssetOutputContent::Value(value))
            }
            OutputContent::Withdrawal {
                value,
                main_fee,
                main_address,
            } => Some(AssetOutputContent::Withdrawal {
                value,
                main_fee,
                main_address,
            }),
            _ => None,
        }
    }
}

impl GetBitcoinValue for OutputContent {
    #[inline(always)]
    fn get_bitcoin_value(&self) -> u64 {
        match self {
            Self::AmmLpToken(_)
            | Self::BitAsset(_)
            | Self::BitAssetControl
            | Self::BitAssetReservation
            | Self::DutchAuctionReceipt => 0,
            Self::Value(value) => value.0,
            Self::Withdrawal { value, .. } => *value,
        }
    }
}

/** Representation of Output Content that includes asset type and/or
 *  reservation commitment */
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FilledContent {
    AmmLpToken {
        asset0: AssetId,
        asset1: AssetId,
        amount: u64,
    },
    Bitcoin(BitcoinOutputContent),
    BitcoinWithdrawal {
        value: u64,
        main_fee: u64,
        main_address: bitcoin::Address<bitcoin::address::NetworkUnchecked>,
    },
    /// BitAsset ID and coin value
    BitAsset(BitAssetId, u64),
    BitAssetControl(BitAssetId),
    /// Reservation txid and commitment
    BitAssetReservation(Txid, Hash),
    /// Auction ID
    DutchAuctionReceipt(DutchAuctionId),
}

impl FilledContent {
    /** Returns the BitAsset ID, if the filled
     * output content corresponds to a BitAsset. */
    pub fn bitasset(&self) -> Option<&BitAssetId> {
        match self {
            Self::BitAsset(bitasset_id, _) => Some(bitasset_id),
            _ => None,
        }
    }

    /** Returns the BitAsset ID (name hash) and if the filled
     * output content corresponds to a BitAsset or BitAsset control coin. */
    pub fn get_bitasset(&self) -> Option<BitAssetId> {
        match self {
            Self::BitAsset(bitasset_id, _)
            | Self::BitAssetControl(bitasset_id) => Some(*bitasset_id),
            _ => None,
        }
    }

    /** Returns the BitAsset ID and coin value, if the filled
     *  output content corresponds to a BitAsset output. */
    pub fn bitasset_value(&self) -> Option<(BitAssetId, u64)> {
        match self {
            Self::BitAsset(bitasset_id, value) => Some((*bitasset_id, *value)),
            _ => None,
        }
    }

    /** Returns the [`AssetId`] and coin value, if the filled
     *  output content corresponds to an asset output. */
    pub fn asset_value(&self) -> Option<(AssetId, u64)> {
        match self {
            Self::BitAsset(bitasset_id, value) => {
                Some((AssetId::BitAsset(*bitasset_id), *value))
            }
            Self::BitAssetControl(bitasset_id) => {
                Some((AssetId::BitAssetControl(*bitasset_id), 1))
            }
            Self::Bitcoin(value) => Some((AssetId::Bitcoin, value.0)),
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
    pub fn lp_token_amount(&self) -> Option<(AssetId, AssetId, u64)> {
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

impl From<FilledContent> for OutputContent {
    fn from(filled: FilledContent) -> Self {
        match filled {
            FilledContent::AmmLpToken {
                asset0: _,
                asset1: _,
                amount,
            } => OutputContent::AmmLpToken(amount),
            FilledContent::Bitcoin(value) => OutputContent::Value(value),
            FilledContent::BitcoinWithdrawal {
                value,
                main_fee,
                main_address,
            } => OutputContent::Withdrawal {
                value,
                main_fee,
                main_address,
            },
            FilledContent::BitAsset(_, value) => OutputContent::BitAsset(value),
            FilledContent::BitAssetControl(_) => OutputContent::BitAssetControl,
            FilledContent::BitAssetReservation { .. } => {
                OutputContent::BitAssetReservation
            }
            FilledContent::DutchAuctionReceipt(_) => {
                OutputContent::DutchAuctionReceipt
            }
        }
    }
}

impl GetBitcoinValue for FilledContent {
    fn get_bitcoin_value(&self) -> u64 {
        OutputContent::from(self.clone()).get_bitcoin_value()
    }
}

#[derive(
    BorshSerialize, Clone, Debug, Deserialize, Eq, PartialEq, Serialize,
)]
pub struct Output<Content = OutputContent> {
    #[serde(with = "serde_display_fromstr_human_readable")]
    pub address: Address,
    pub content: Content,
    #[serde(with = "serde_hexstr_human_readable")]
    pub memo: Vec<u8>,
}

pub type TxOutput = Output;

pub type BitcoinOutput = Output<BitcoinOutputContent>;

pub type AssetOutput = Output<AssetOutputContent>;

pub type FilledOutput = Output<FilledContent>;

impl<Content> Output<Content> {
    pub fn new(address: Address, content: Content) -> Self {
        Self {
            address,
            content,
            memo: Vec::new(),
        }
    }

    pub fn map_content<C, F>(self, f: F) -> Output<C>
    where
        F: FnOnce(Content) -> C,
    {
        Output {
            address: self.address,
            content: f(self.content),
            memo: self.memo,
        }
    }

    pub fn map_content_opt<C, F>(self, f: F) -> Option<Output<C>>
    where
        F: FnOnce(Content) -> Option<C>,
    {
        Some(Output {
            address: self.address,
            content: f(self.content)?,
            memo: self.memo,
        })
    }
}

impl TxOutput {
    /// `true` if the output content corresponds to a Bitcoin Value
    pub fn is_bitcoin(&self) -> bool {
        self.content.is_bitcoin()
    }

    /// `true` if the output content corresponds to a Bitcoin Withdrawal
    pub fn is_withdrawal(&self) -> bool {
        self.content.is_withdrawal()
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

    /// `true` if the output corresponds to an asset output
    pub fn is_asset(&self) -> bool {
        self.content.is_asset()
    }
}

impl From<TxOutput> for Option<BitcoinOutput> {
    fn from(output: Output) -> Option<BitcoinOutput> {
        output.map_content_opt(OutputContent::into)
    }
}

impl From<TxOutput> for Option<AssetOutput> {
    fn from(output: Output) -> Option<AssetOutput> {
        output.map_content_opt(OutputContent::into)
    }
}

impl GetBitcoinValue for TxOutput {
    #[inline(always)]
    fn get_bitcoin_value(&self) -> u64 {
        self.content.get_bitcoin_value()
    }
}

impl FilledOutput {
    /** Returns the BitAsset ID if the filled output content
     * corresponds to a BitAsset */
    pub fn bitasset(&self) -> Option<&BitAssetId> {
        self.content.bitasset()
    }

    /** Returns the BitAsset ID if the filled output content
     * corresponds to a BitAsset or BitAsset control coin. */
    pub fn get_bitasset(&self) -> Option<BitAssetId> {
        self.content.get_bitasset()
    }

    /** Returns the BitAsset ID and coin value
     * if the filled output content corresponds to a BitAsset output. */
    pub fn bitasset_value(&self) -> Option<(BitAssetId, u64)> {
        self.content.bitasset_value()
    }

    /** Returns the [`AssetId`] and coin value
     * if the filled output content corresponds to an asset output. */
    pub fn asset_value(&self) -> Option<(AssetId, u64)> {
        self.content.asset_value()
    }

    /** Returns the Dutch auction ID, if the filled output content corresponds
     *  to a Dutch auction receipt output. */
    pub fn dutch_auction_receipt(&self) -> Option<DutchAuctionId> {
        self.content.dutch_auction_receipt()
    }

    /** Returns the LP token's corresponding asset pair and amount,
     *  if the filled output content corresponds to an LP token output. */
    pub fn lp_token_amount(&self) -> Option<(AssetId, AssetId, u64)> {
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

/// Representation of a spent output
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SpentOutput<Content = FilledContent> {
    pub output: Output<Content>,
    pub inpoint: InPoint,
}
