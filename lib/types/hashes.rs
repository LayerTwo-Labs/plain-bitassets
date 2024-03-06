use bip300301::bitcoin;
use bitcoin::hashes::Hash as _;
use borsh::{BorshDeserialize, BorshSerialize};
use heed::zerocopy::{self, AsBytes, FromBytes};
use hex::FromHex;
use serde::{Deserialize, Serialize};

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

#[derive(Default, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
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

#[derive(Default, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
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

#[derive(
    AsBytes,
    BorshDeserialize,
    BorshSerialize,
    Clone,
    Copy,
    Default,
    Deserialize,
    Eq,
    FromBytes,
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

/// Unique identifier for each Dutch auction
#[derive(
    BorshDeserialize,
    BorshSerialize,
    Clone,
    Copy,
    Debug,
    Deserialize,
    Eq,
    PartialEq,
    Serialize,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct DutchAuctionId(pub Txid);
