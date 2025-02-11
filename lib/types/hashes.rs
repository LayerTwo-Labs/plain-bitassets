use std::str::FromStr;

use bitcoin::hashes::Hash as _;
use borsh::{BorshDeserialize, BorshSerialize};
use hex::FromHex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::serde_hexstr_human_readable;

pub type Hash = [u8; blake3::OUT_LEN];

pub fn hash<T: serde::Serialize>(data: &T) -> Hash {
    let data_serialized = bincode::serialize(data)
        .expect("failed to serialize a type to compute a hash");
    blake3::hash(&data_serialized).into()
}

pub fn update<T: serde::Serialize>(hasher: &mut blake3::Hasher, data: &T) {
    let data_serialized = bincode::serialize(data)
        .expect("failed to serialize a type to compute a hash");
    let _hasher = hasher.update(&data_serialized);
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Clone,
    Copy,
    Default,
    Deserialize,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
pub struct BlockHash(#[serde(with = "serde_hexstr_human_readable")] pub Hash);

impl From<Hash> for BlockHash {
    fn from(other: Hash) -> Self {
        Self(other)
    }
}

impl From<BlockHash> for Hash {
    fn from(other: BlockHash) -> Self {
        other.0
    }
}

impl From<BlockHash> for Vec<u8> {
    fn from(other: BlockHash) -> Self {
        other.0.into()
    }
}

impl From<BlockHash> for bitcoin::BlockHash {
    fn from(other: BlockHash) -> Self {
        let inner: [u8; 32] = other.into();
        Self::from_byte_array(inner)
    }
}

impl FromHex for BlockHash {
    type Error = <Hash as FromHex>::Error;

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        Hash::from_hex(hex).map(Self)
    }
}

impl std::fmt::Display for BlockHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl std::fmt::Debug for BlockHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl FromStr for BlockHash {
    type Err = <Self as FromHex>::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

impl utoipa::PartialSchema for BlockHash {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        let obj =
            utoipa::openapi::Object::with_type(utoipa::openapi::Type::String);
        utoipa::openapi::RefOr::T(utoipa::openapi::Schema::Object(obj))
    }
}

impl utoipa::ToSchema for BlockHash {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("BlockHash")
    }
}

#[derive(
    BorshSerialize,
    Clone,
    Copy,
    Default,
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
pub struct MerkleRoot(#[serde(with = "serde_hexstr_human_readable")] Hash);

impl From<Hash> for MerkleRoot {
    fn from(other: Hash) -> Self {
        Self(other)
    }
}

impl From<MerkleRoot> for Hash {
    fn from(other: MerkleRoot) -> Self {
        other.0
    }
}

impl std::fmt::Display for MerkleRoot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl std::fmt::Debug for MerkleRoot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl utoipa::PartialSchema for MerkleRoot {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        let obj =
            utoipa::openapi::Object::with_type(utoipa::openapi::Type::String);
        utoipa::openapi::RefOr::T(utoipa::openapi::Schema::Object(obj))
    }
}

impl utoipa::ToSchema for MerkleRoot {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("MerkleRoot")
    }
}

#[derive(
    BorshDeserialize,
    BorshSerialize,
    Clone,
    Copy,
    Default,
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
pub struct Txid(#[serde(with = "serde_hexstr_human_readable")] pub Hash);

impl Txid {
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl From<Hash> for Txid {
    fn from(other: Hash) -> Self {
        Self(other)
    }
}

impl From<Txid> for Hash {
    fn from(other: Txid) -> Self {
        other.0
    }
}

impl<'a> From<&'a Txid> for &'a Hash {
    fn from(other: &'a Txid) -> Self {
        &other.0
    }
}

impl std::fmt::Display for Txid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl std::fmt::Debug for Txid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl FromHex for Txid {
    type Error = <Hash as FromHex>::Error;

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        Hash::from_hex(hex).map(Self)
    }
}

impl FromStr for Txid {
    type Err = <Self as FromHex>::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

impl utoipa::PartialSchema for Txid {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        let obj =
            utoipa::openapi::Object::with_type(utoipa::openapi::Type::String);
        utoipa::openapi::RefOr::T(utoipa::openapi::Schema::Object(obj))
    }
}

impl utoipa::ToSchema for Txid {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("Txid")
    }
}

/// Identifier for a BitAsset
#[derive(
    BorshDeserialize,
    BorshSerialize,
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
pub struct BitAssetId(#[serde(with = "serde_hexstr_human_readable")] pub Hash);

impl FromHex for BitAssetId {
    type Error = <Hash as FromHex>::Error;

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        Hash::from_hex(hex).map(Self)
    }
}

impl utoipa::PartialSchema for BitAssetId {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        let obj =
            utoipa::openapi::Object::with_type(utoipa::openapi::Type::String);
        utoipa::openapi::RefOr::T(utoipa::openapi::Schema::Object(obj))
    }
}

impl utoipa::ToSchema for BitAssetId {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("BitAssetId")
    }
}

#[derive(Debug, Error)]
pub enum ParseAssetIdError {
    #[error(transparent)]
    Borsh(#[from] borsh::io::Error),
    #[error(transparent)]
    FromHex(#[from] hex::FromHexError),
}

/// Identifier for an arbitrary asset (Bitcoin, BitAsset, or BitAsset control)
#[derive(
    Clone,
    Copy,
    Debug,
    BorshDeserialize,
    BorshSerialize,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
)]
pub enum AssetId {
    Bitcoin,
    BitAsset(BitAssetId),
    BitAssetControl(BitAssetId),
}

impl<'de> Deserialize<'de> for AssetId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: Vec<u8> =
            serde_hexstr_human_readable::deserialize(deserializer)?;
        borsh::from_slice(&bytes).map_err(serde::de::Error::custom)
    }
}

impl Serialize for AssetId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes = borsh::to_vec(self).map_err(serde::ser::Error::custom)?;
        serde_hexstr_human_readable::serialize(bytes, serializer)
    }
}

impl std::fmt::Display for AssetId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bytes = borsh::to_vec(self).unwrap();
        hex::encode(bytes).fmt(f)
    }
}

impl FromStr for AssetId {
    type Err = ParseAssetIdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes: Vec<u8> = hex::decode(s)?;
        borsh::from_slice(&bytes).map_err(Self::Err::from)
    }
}

impl utoipa::PartialSchema for AssetId {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        let obj =
            utoipa::openapi::Object::with_type(utoipa::openapi::Type::String);
        utoipa::openapi::RefOr::T(utoipa::openapi::Schema::Object(obj))
    }
}

impl utoipa::ToSchema for AssetId {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("AssetId")
    }
}

/// Unique identifier for each Dutch auction
#[derive(
    BorshDeserialize,
    BorshSerialize,
    Clone,
    Copy,
    Debug,
    Deserialize,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct DutchAuctionId(pub Txid);

impl std::fmt::Display for DutchAuctionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl FromHex for DutchAuctionId {
    type Error = <Hash as FromHex>::Error;

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        Txid::from_hex(hex).map(Self)
    }
}

impl FromStr for DutchAuctionId {
    type Err = <Self as FromHex>::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

impl utoipa::PartialSchema for DutchAuctionId {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        let obj =
            utoipa::openapi::Object::with_type(utoipa::openapi::Type::String);
        utoipa::openapi::RefOr::T(utoipa::openapi::Schema::Object(obj))
    }
}

impl utoipa::ToSchema for DutchAuctionId {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("DutchAuctionId")
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct M6id(pub bitcoin::Txid);

impl std::fmt::Display for M6id {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
