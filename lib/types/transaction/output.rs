use borsh::BorshSerialize;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DeserializeAs, IfIsHumanReadable, SerializeAs};
use utoipa::ToSchema;

use crate::types::{
    serde_display_fromstr_human_readable, serde_hexstr_human_readable, Address,
    AssetId, BitAssetId, DutchAuctionId, GetBitcoinValue, Hash, InPoint,
    OutPoint, Txid,
};

/// Serialize [`bitcoin::Amount`] as sats
struct BitcoinAmountSats;

impl<'de> DeserializeAs<'de, bitcoin::Amount> for BitcoinAmountSats {
    fn deserialize_as<D>(deserializer: D) -> Result<bitcoin::Amount, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        bitcoin::amount::serde::as_sat::deserialize(deserializer)
    }
}

impl SerializeAs<bitcoin::Amount> for BitcoinAmountSats {
    fn serialize_as<S>(
        source: &bitcoin::Amount,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        bitcoin::amount::serde::as_sat::serialize(source, serializer)
    }
}

fn borsh_serialize_bitcoin_amount<W>(
    bitcoin_amount: &bitcoin::Amount,
    writer: &mut W,
) -> borsh::io::Result<()>
where
    W: borsh::io::Write,
{
    borsh::BorshSerialize::serialize(&bitcoin_amount.to_sat(), writer)
}

#[serde_as]
#[derive(
    BorshSerialize,
    Clone,
    Copy,
    Debug,
    Deserialize,
    Eq,
    PartialEq,
    Serialize,
    ToSchema,
)]
#[repr(transparent)]
#[schema(value_type = u64)]
#[serde(transparent)]
pub struct BitcoinContent(
    #[borsh(serialize_with = "borsh_serialize_bitcoin_amount")]
    #[serde_as(as = "IfIsHumanReadable<BitcoinAmountSats>")]
    pub bitcoin::Amount,
);

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

mod withdrawal_content {
    use serde::{Deserialize, Serialize};

    /// Defines a WithdrawalContent struct with the specified visibility, name,
    /// derives, and attributes for each field
    macro_rules! WithdrawalContent {
        (   $vis:vis $struct_name:ident
            $(, attrs: [$($attr:meta),* $(,)?])?
            $(, value_attrs: [$($value_attr:meta),* $(,)?])?
            $(, main_fee_attrs: [$($main_fee_attr:meta),* $(,)?])?
            $(, main_address_attrs: [$($main_address_attr:meta),* $(,)?])?
            $(,)?
        ) => {
            // Generate attributes if they were provided
            $(
                $(#[$attr])*
            )?
            $vis struct $struct_name {
                // Generate attributes if they were provided
                $(
                    $(#[$value_attr])*
                )?
                pub value: bitcoin::Amount,
                // Generate attributes if they were provided
                $(
                    $(#[$main_fee_attr])*
                )?
                pub main_fee: bitcoin::Amount,
                // Generate attributes if they were provided
                $(
                    $(#[$main_address_attr])*
                )?
                pub main_address: bitcoin::Address<
                    bitcoin::address::NetworkUnchecked
                >,
            }
        }
    }

    WithdrawalContent!(DefaultRepr, attrs: [derive(Deserialize, Serialize)]);

    WithdrawalContent!(
        HumanReadableRepr,
        attrs: [
            derive(utoipa::ToSchema, Deserialize, Serialize),
            schema(as = WithdrawalOutputContent)
        ],
        value_attrs: [
            schema(value_type = u64),
            serde(rename = "value_sats"),
            serde(with = "bitcoin::amount::serde::as_sat")
        ],
        main_fee_attrs: [
            schema(value_type = u64),
            serde(rename = "main_fee_sats"),
            serde(with = "bitcoin::amount::serde::as_sat")
        ],
        main_address_attrs: [
            schema(value_type = crate::types::schema::BitcoinAddr),
        ],
    );

    type SerdeRepr = serde_with::IfIsHumanReadable<
        serde_with::FromInto<HumanReadableRepr>,
        serde_with::FromInto<DefaultRepr>,
    >;

    WithdrawalContent!(
        pub WithdrawalContent,
        attrs: [derive(
            borsh::BorshSerialize,
            Clone,
            Debug,
            Eq,
            PartialEq
        )],
        value_attrs: [
            borsh(serialize_with = "super::borsh_serialize_bitcoin_amount"),
        ],
        main_fee_attrs: [
            borsh(serialize_with = "super::borsh_serialize_bitcoin_amount"),
        ],
        main_address_attrs: [
            borsh(serialize_with = "super::borsh_serialize_bitcoin_address"),
        ],
    );

    impl From<WithdrawalContent> for DefaultRepr {
        fn from(withdrawal_content: WithdrawalContent) -> Self {
            Self {
                value: withdrawal_content.value,
                main_fee: withdrawal_content.main_fee,
                main_address: withdrawal_content.main_address,
            }
        }
    }

    impl From<WithdrawalContent> for HumanReadableRepr {
        fn from(withdrawal_content: WithdrawalContent) -> Self {
            Self {
                value: withdrawal_content.value,
                main_fee: withdrawal_content.main_fee,
                main_address: withdrawal_content.main_address,
            }
        }
    }

    impl From<DefaultRepr> for WithdrawalContent {
        fn from(repr: DefaultRepr) -> Self {
            Self {
                value: repr.value,
                main_fee: repr.main_fee,
                main_address: repr.main_address,
            }
        }
    }

    impl From<HumanReadableRepr> for WithdrawalContent {
        fn from(repr: HumanReadableRepr) -> Self {
            Self {
                value: repr.value,
                main_fee: repr.main_fee,
                main_address: repr.main_address,
            }
        }
    }

    impl<'de> Deserialize<'de> for WithdrawalContent {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            <SerdeRepr as serde_with::DeserializeAs<'de, _>>::deserialize_as(
                deserializer,
            )
        }
    }

    impl Serialize for WithdrawalContent {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            <SerdeRepr as serde_with::SerializeAs<_>>::serialize_as(
                self, serializer,
            )
        }
    }

    impl utoipa::PartialSchema for WithdrawalContent {
        fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
            <HumanReadableRepr as utoipa::PartialSchema>::schema()
        }
    }

    impl utoipa::ToSchema for WithdrawalContent {
        fn name() -> std::borrow::Cow<'static, str> {
            <HumanReadableRepr as utoipa::ToSchema>::name()
        }
    }

    impl crate::types::GetBitcoinValue for WithdrawalContent {
        fn get_bitcoin_value(&self) -> bitcoin::Amount {
            self.value
        }
    }
}
pub use withdrawal_content::WithdrawalContent;

// The subset of output contents that correspond to assets
#[derive(Clone, Debug, Eq, PartialEq, ToSchema)]
pub enum AssetContent {
    BitAsset(u64),
    BitAssetControl,
    Bitcoin(BitcoinContent),
    Withdrawal(WithdrawalContent),
}

impl From<BitcoinContent> for AssetContent {
    fn from(content: BitcoinContent) -> Self {
        Self::Bitcoin(content)
    }
}

mod content {
    use serde::{Deserialize, Serialize};

    /// Defines a Content enum with the specified visibility, name,
    /// derives, and attributes for each variant
    macro_rules! Content {
        (   $vis:vis $enum_name:ident
            $(, attrs: [$($attr:meta),* $(,)?])?
            $(, bitcoin_attrs: [$($bitcoin_attr:meta),* $(,)?])?
            $(,)?
        ) => {
            // Generate attributes if they were provided
            $(
                $(#[$attr])*
            )?
            $vis enum $enum_name {
                AmmLpToken(u64),
                // Generate attributes if they were provided
                $(
                    $(#[$bitcoin_attr])*
                )?
                Bitcoin(super::BitcoinContent),
                BitAsset(u64),
                BitAssetControl,
                BitAssetReservation,
                /// Receipt used to redeem the proceeds of an auction
                DutchAuctionReceipt,
                Withdrawal(super::WithdrawalContent),
            }
        }
    }

    Content!(DefaultRepr, attrs: [derive(Deserialize, Serialize)]);

    Content!(
        HumanReadableRepr,
        attrs: [
            derive(utoipa::ToSchema, Deserialize, Serialize),
            schema(as = OutputContent)
        ],
        bitcoin_attrs: [
            serde(rename = "BitcoinSats")
        ],
    );

    type SerdeRepr = serde_with::IfIsHumanReadable<
        serde_with::FromInto<HumanReadableRepr>,
        serde_with::FromInto<DefaultRepr>,
    >;

    Content!(
        pub Content,
        attrs: [derive(
            borsh::BorshSerialize,
            Clone,
            Debug,
            Eq,
            PartialEq,
        )],
    );

    impl Content {
        /// `true` if the output content corresponds to a BitAsset
        pub fn is_bitasset(&self) -> bool {
            matches!(self, Self::BitAsset(_))
        }

        /// `true` if the output content corresponds to a BitAsset control coin
        pub fn is_bitasset_control(&self) -> bool {
            matches!(self, Self::BitAssetControl)
        }

        /// `true` if the output content corresponds to a reservation
        pub fn is_reservation(&self) -> bool {
            matches!(self, Self::BitAssetReservation)
        }

        pub fn is_bitcoin(&self) -> bool {
            matches!(self, Self::Bitcoin(_))
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
                    | Self::Bitcoin(_)
                    | Self::Withdrawal { .. }
            )
        }

        pub fn as_bitcoin(self) -> Option<super::BitcoinContent> {
            match self {
                Self::Bitcoin(value) => Some(value),
                _ => None,
            }
        }

        pub fn as_asset(self) -> Option<super::AssetContent> {
            match self {
                Self::BitAsset(value) => {
                    Some(super::AssetContent::BitAsset(value))
                }
                Self::BitAssetControl => {
                    Some(super::AssetContent::BitAssetControl)
                }
                Self::Bitcoin(value) => {
                    Some(super::AssetContent::Bitcoin(value))
                }
                Self::Withdrawal(withdrawal) => {
                    Some(super::AssetContent::Withdrawal(withdrawal))
                }
                Self::AmmLpToken(_)
                | Self::BitAssetReservation
                | Self::DutchAuctionReceipt => None,
            }
        }
    }

    impl From<super::BitcoinContent> for Content {
        fn from(content: super::BitcoinContent) -> Self {
            Self::Bitcoin(content)
        }
    }

    impl From<super::AssetContent> for Content {
        fn from(content: super::AssetContent) -> Self {
            match content {
                super::AssetContent::BitAsset(value) => Self::BitAsset(value),
                super::AssetContent::BitAssetControl => Self::BitAssetControl,
                super::AssetContent::Bitcoin(value) => Self::Bitcoin(value),
                super::AssetContent::Withdrawal(withdrawal) => {
                    Self::Withdrawal(withdrawal)
                }
            }
        }
    }

    impl From<DefaultRepr> for Content {
        fn from(repr: DefaultRepr) -> Self {
            match repr {
                DefaultRepr::AmmLpToken(value) => Self::AmmLpToken(value),
                DefaultRepr::BitAsset(value) => Self::BitAsset(value),
                DefaultRepr::BitAssetControl => Self::BitAssetControl,
                DefaultRepr::BitAssetReservation => Self::BitAssetReservation,
                DefaultRepr::Bitcoin(value) => Self::Bitcoin(value),
                DefaultRepr::DutchAuctionReceipt => Self::DutchAuctionReceipt,
                DefaultRepr::Withdrawal(withdrawal) => {
                    Self::Withdrawal(withdrawal)
                }
            }
        }
    }

    impl From<HumanReadableRepr> for Content {
        fn from(repr: HumanReadableRepr) -> Self {
            match repr {
                HumanReadableRepr::AmmLpToken(value) => Self::AmmLpToken(value),
                HumanReadableRepr::BitAsset(value) => Self::BitAsset(value),
                HumanReadableRepr::BitAssetControl => Self::BitAssetControl,
                HumanReadableRepr::BitAssetReservation => {
                    Self::BitAssetReservation
                }
                HumanReadableRepr::Bitcoin(value) => Self::Bitcoin(value),
                HumanReadableRepr::DutchAuctionReceipt => {
                    Self::DutchAuctionReceipt
                }
                HumanReadableRepr::Withdrawal(withdrawal) => {
                    Self::Withdrawal(withdrawal)
                }
            }
        }
    }

    impl From<Content> for DefaultRepr {
        fn from(content: Content) -> Self {
            match content {
                Content::AmmLpToken(value) => Self::AmmLpToken(value),
                Content::BitAsset(value) => Self::BitAsset(value),
                Content::BitAssetControl => Self::BitAssetControl,
                Content::BitAssetReservation => Self::BitAssetReservation,
                Content::Bitcoin(value) => Self::Bitcoin(value),
                Content::DutchAuctionReceipt => Self::DutchAuctionReceipt,
                Content::Withdrawal(withdrawal) => Self::Withdrawal(withdrawal),
            }
        }
    }

    impl From<Content> for HumanReadableRepr {
        fn from(content: Content) -> Self {
            match content {
                Content::AmmLpToken(value) => Self::AmmLpToken(value),
                Content::BitAsset(value) => Self::BitAsset(value),
                Content::BitAssetControl => Self::BitAssetControl,
                Content::BitAssetReservation => Self::BitAssetReservation,
                Content::Bitcoin(value) => Self::Bitcoin(value),
                Content::DutchAuctionReceipt => Self::DutchAuctionReceipt,
                Content::Withdrawal(withdrawal) => Self::Withdrawal(withdrawal),
            }
        }
    }

    impl<'de> Deserialize<'de> for Content {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            <SerdeRepr as serde_with::DeserializeAs<'de, _>>::deserialize_as(
                deserializer,
            )
        }
    }

    impl Serialize for Content {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            <SerdeRepr as serde_with::SerializeAs<_>>::serialize_as(
                self, serializer,
            )
        }
    }

    impl utoipa::PartialSchema for Content {
        fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
            <HumanReadableRepr as utoipa::PartialSchema>::schema()
        }
    }

    impl utoipa::ToSchema for Content {
        fn name() -> std::borrow::Cow<'static, str> {
            <HumanReadableRepr as utoipa::ToSchema>::name()
        }
    }

    impl crate::types::GetBitcoinValue for Content {
        #[inline(always)]
        fn get_bitcoin_value(&self) -> bitcoin::Amount {
            match self {
                Self::AmmLpToken(_)
                | Self::BitAsset(_)
                | Self::BitAssetControl
                | Self::BitAssetReservation
                | Self::DutchAuctionReceipt => bitcoin::Amount::ZERO,
                Self::Bitcoin(value) => value.0,
                Self::Withdrawal(withdrawal) => withdrawal.get_bitcoin_value(),
            }
        }
    }
}
pub use content::Content;

mod filled_content {
    use serde::{Deserialize, Serialize};

    use crate::types::{AssetId, BitAssetId, DutchAuctionId, Hash, Txid};

    /// Defines a FilledContent enum with the specified visibility, name,
    /// derives, and attributes for each variant
    macro_rules! FilledContent {
        (   $vis:vis $enum_name:ident
            $(, attrs: [$($attr:meta),* $(,)?])?
            $(, bitcoin_attrs: [$($bitcoin_attr:meta),* $(,)?])?
            $(, bitasset_reservation_commitment_attrs:
                [$($bitasset_reservation_commitment_attr:meta),* $(,)?]
            )?
            $(,)?
        ) => {
            /// Representation of Output Content that includes asset type and/or
            /// reservation commitment
            // Generate attributes if they were provided
            $(
                $(#[$attr])*
            )?
            $vis enum $enum_name {
                AmmLpToken {
                    asset0: AssetId,
                    asset1: AssetId,
                    amount: u64,
                },
                // Generate attributes if they were provided
                $(
                    $(#[$bitcoin_attr])*
                )?
                Bitcoin(super::BitcoinContent),
                BitcoinWithdrawal(super::WithdrawalContent),
                /// BitAsset ID and coin value
                BitAsset(BitAssetId, u64),
                BitAssetControl(BitAssetId),
                /// Reservation txid and commitment
                BitAssetReservation(
                    crate::types::Txid,
                    $(
                        $(#[$bitasset_reservation_commitment_attr])*
                    )?
                    crate::types::Hash
                ),
                /// Auction ID
                DutchAuctionReceipt(DutchAuctionId),
            }
        }
    }

    FilledContent!(DefaultRepr, attrs: [derive(Deserialize, Serialize)]);

    FilledContent!(
        HumanReadableRepr,
        attrs: [
            derive(utoipa::ToSchema, Deserialize, Serialize),
            schema(as = FilledOutputContent)
        ],
        bitcoin_attrs: [
            serde(rename = "BitcoinSats")
        ],
        bitasset_reservation_commitment_attrs: [
            serde(with = "hex::serde")
        ]
    );

    type SerdeRepr = serde_with::IfIsHumanReadable<
        serde_with::FromInto<HumanReadableRepr>,
        serde_with::FromInto<DefaultRepr>,
    >;

    FilledContent!(
        pub FilledContent,
        attrs: [derive(
            Clone,
            Debug,
            Eq,
            PartialEq,
        )],
    );

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
                Self::BitAsset(bitasset_id, value) => {
                    Some((*bitasset_id, *value))
                }
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
                Self::Bitcoin(value) => {
                    Some((AssetId::Bitcoin, value.0.to_sat()))
                }
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

    impl From<FilledContent> for super::Content {
        fn from(filled: FilledContent) -> Self {
            match filled {
                FilledContent::AmmLpToken {
                    asset0: _,
                    asset1: _,
                    amount,
                } => super::Content::AmmLpToken(amount),
                FilledContent::Bitcoin(value) => super::Content::Bitcoin(value),
                FilledContent::BitcoinWithdrawal(withdrawal) => {
                    super::Content::Withdrawal(withdrawal)
                }
                FilledContent::BitAsset(_, value) => {
                    super::Content::BitAsset(value)
                }
                FilledContent::BitAssetControl(_) => {
                    super::Content::BitAssetControl
                }
                FilledContent::BitAssetReservation { .. } => {
                    super::Content::BitAssetReservation
                }
                FilledContent::DutchAuctionReceipt(_) => {
                    super::Content::DutchAuctionReceipt
                }
            }
        }
    }

    impl From<DefaultRepr> for FilledContent {
        fn from(repr: DefaultRepr) -> Self {
            match repr {
                DefaultRepr::AmmLpToken {
                    asset0,
                    asset1,
                    amount,
                } => Self::AmmLpToken {
                    asset0,
                    asset1,
                    amount,
                },
                DefaultRepr::BitAsset(asset_id, value) => {
                    Self::BitAsset(asset_id, value)
                }
                DefaultRepr::BitAssetControl(bitasset_id) => {
                    Self::BitAssetControl(bitasset_id)
                }
                DefaultRepr::BitAssetReservation(txid, commitment) => {
                    Self::BitAssetReservation(txid, commitment)
                }
                DefaultRepr::Bitcoin(value) => Self::Bitcoin(value),
                DefaultRepr::BitcoinWithdrawal(withdrawal) => {
                    Self::BitcoinWithdrawal(withdrawal)
                }
                DefaultRepr::DutchAuctionReceipt(auction_id) => {
                    Self::DutchAuctionReceipt(auction_id)
                }
            }
        }
    }

    impl From<HumanReadableRepr> for FilledContent {
        fn from(repr: HumanReadableRepr) -> Self {
            match repr {
                HumanReadableRepr::AmmLpToken {
                    asset0,
                    asset1,
                    amount,
                } => Self::AmmLpToken {
                    asset0,
                    asset1,
                    amount,
                },
                HumanReadableRepr::BitAsset(asset_id, value) => {
                    Self::BitAsset(asset_id, value)
                }
                HumanReadableRepr::BitAssetControl(bitasset_id) => {
                    Self::BitAssetControl(bitasset_id)
                }
                HumanReadableRepr::BitAssetReservation(txid, commitment) => {
                    Self::BitAssetReservation(txid, commitment)
                }
                HumanReadableRepr::Bitcoin(value) => Self::Bitcoin(value),
                HumanReadableRepr::BitcoinWithdrawal(withdrawal) => {
                    Self::BitcoinWithdrawal(withdrawal)
                }
                HumanReadableRepr::DutchAuctionReceipt(auction_id) => {
                    Self::DutchAuctionReceipt(auction_id)
                }
            }
        }
    }

    impl From<FilledContent> for DefaultRepr {
        fn from(content: FilledContent) -> Self {
            match content {
                FilledContent::AmmLpToken {
                    asset0,
                    asset1,
                    amount,
                } => Self::AmmLpToken {
                    asset0,
                    asset1,
                    amount,
                },
                FilledContent::BitAsset(asset_id, value) => {
                    Self::BitAsset(asset_id, value)
                }
                FilledContent::BitAssetControl(bitasset_id) => {
                    Self::BitAssetControl(bitasset_id)
                }
                FilledContent::BitAssetReservation(txid, commitment) => {
                    Self::BitAssetReservation(txid, commitment)
                }
                FilledContent::Bitcoin(value) => Self::Bitcoin(value),
                FilledContent::BitcoinWithdrawal(withdrawal) => {
                    Self::BitcoinWithdrawal(withdrawal)
                }
                FilledContent::DutchAuctionReceipt(auction_id) => {
                    Self::DutchAuctionReceipt(auction_id)
                }
            }
        }
    }

    impl From<FilledContent> for HumanReadableRepr {
        fn from(content: FilledContent) -> Self {
            match content {
                FilledContent::AmmLpToken {
                    asset0,
                    asset1,
                    amount,
                } => Self::AmmLpToken {
                    asset0,
                    asset1,
                    amount,
                },
                FilledContent::BitAsset(asset_id, value) => {
                    Self::BitAsset(asset_id, value)
                }
                FilledContent::BitAssetControl(bitasset_id) => {
                    Self::BitAssetControl(bitasset_id)
                }
                FilledContent::BitAssetReservation(txid, commitment) => {
                    Self::BitAssetReservation(txid, commitment)
                }
                FilledContent::Bitcoin(value) => Self::Bitcoin(value),
                FilledContent::BitcoinWithdrawal(withdrawal) => {
                    Self::BitcoinWithdrawal(withdrawal)
                }
                FilledContent::DutchAuctionReceipt(auction_id) => {
                    Self::DutchAuctionReceipt(auction_id)
                }
            }
        }
    }

    impl<'de> Deserialize<'de> for FilledContent {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            <SerdeRepr as serde_with::DeserializeAs<'de, _>>::deserialize_as(
                deserializer,
            )
        }
    }

    impl Serialize for FilledContent {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            <SerdeRepr as serde_with::SerializeAs<_>>::serialize_as(
                self, serializer,
            )
        }
    }

    impl utoipa::PartialSchema for FilledContent {
        fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
            <HumanReadableRepr as utoipa::PartialSchema>::schema()
        }
    }

    impl utoipa::ToSchema for FilledContent {
        fn name() -> std::borrow::Cow<'static, str> {
            <HumanReadableRepr as utoipa::ToSchema>::name()
        }
    }

    impl crate::types::GetBitcoinValue for FilledContent {
        fn get_bitcoin_value(&self) -> bitcoin::Amount {
            super::Content::from(self.clone()).get_bitcoin_value()
        }
    }
}
pub use filled_content::FilledContent;

#[derive(
    BorshSerialize,
    Clone,
    Debug,
    Deserialize,
    Eq,
    PartialEq,
    Serialize,
    ToSchema,
)]
pub struct Output<OutputContent = Content> {
    #[serde(with = "serde_display_fromstr_human_readable")]
    pub address: Address,
    pub content: OutputContent,
    #[serde(with = "serde_hexstr_human_readable")]
    pub memo: Vec<u8>,
}

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

pub type TxOutput = Output;

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

impl GetBitcoinValue for TxOutput {
    #[inline(always)]
    fn get_bitcoin_value(&self) -> bitcoin::Amount {
        self.content.get_bitcoin_value()
    }
}

pub type BitcoinOutput = Output<BitcoinContent>;

impl From<TxOutput> for Option<BitcoinOutput> {
    fn from(output: Output) -> Option<BitcoinOutput> {
        output.map_content_opt(Content::as_bitcoin)
    }
}

pub type AssetOutput = Output<AssetContent>;

impl From<TxOutput> for Option<AssetOutput> {
    fn from(output: Output) -> Option<AssetOutput> {
        output.map_content_opt(Content::as_asset)
    }
}

pub type FilledOutput = Output<FilledContent>;

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
    fn get_bitcoin_value(&self) -> bitcoin::Amount {
        self.content.get_bitcoin_value()
    }
}

/// Representation of a spent output
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SpentOutput<OutputContent = FilledContent> {
    pub output: Output<OutputContent>,
    pub inpoint: InPoint,
}

#[derive(
    BorshSerialize,
    Clone,
    Debug,
    Deserialize,
    Eq,
    PartialEq,
    Serialize,
    ToSchema,
)]
pub struct Pointed<OutputContent = Content> {
    pub outpoint: OutPoint,
    #[schema(inline)]
    pub output: Output<OutputContent>,
}
