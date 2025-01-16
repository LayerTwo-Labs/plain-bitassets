use std::net::{Ipv4Addr, Ipv6Addr};

use borsh::BorshSerialize;
use serde::{Deserialize, Serialize};
use utoipa::{
    openapi::{RefOr, Schema},
    PartialSchema, ToSchema,
};

use crate::types::{EncryptionPubKey, Hash, VerifyingKey};

#[derive(
    BorshSerialize,
    Clone,
    Debug,
    Default,
    Deserialize,
    Eq,
    Hash,
    PartialEq,
    Serialize,
    ToSchema,
)]
pub struct BitAssetData {
    /// Commitment to arbitrary data
    #[schema(value_type = Option<String>)]
    pub commitment: Option<Hash>,
    /// Optional ipv4 addr
    #[schema(value_type = Option<String>)]
    pub ipv4_addr: Option<Ipv4Addr>,
    /// Optional ipv6 addr
    #[schema(value_type = Option<String>)]
    pub ipv6_addr: Option<Ipv6Addr>,
    /// Optional pubkey used for encryption
    pub encryption_pubkey: Option<EncryptionPubKey>,
    /// Optional pubkey used for signing messages
    pub signing_pubkey: Option<VerifyingKey>,
}

/// Delete, retain, or set a value
#[derive(BorshSerialize, Clone, Debug, Deserialize, Serialize)]
pub enum Update<T> {
    Delete,
    Retain,
    Set(T),
}

impl<T> Update<T> {
    /// Create a schema from a schema for `T`.
    fn schema(schema_t: RefOr<Schema>) -> RefOr<Schema> {
        let schema_delete = utoipa::openapi::ObjectBuilder::new()
            .schema_type(utoipa::openapi::Type::String)
            .enum_values(Some(["Delete"]));
        let schema_retain = utoipa::openapi::ObjectBuilder::new()
            .schema_type(utoipa::openapi::Type::String)
            .enum_values(Some(["Retain"]));
        let schema_set = utoipa::openapi::ObjectBuilder::new()
            .property("Set", schema_t)
            .required("Set");
        let schema = utoipa::openapi::OneOfBuilder::new()
            .item(schema_delete)
            .item(schema_retain)
            .item(schema_set)
            .build()
            .into();
        RefOr::T(schema)
    }
}

impl PartialSchema for Update<Hash> {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        Self::schema(<String as PartialSchema>::schema())
    }
}

impl ToSchema for Update<Hash> {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("UpdateHash")
    }
}

impl PartialSchema for Update<Ipv4Addr> {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        Self::schema(<String as PartialSchema>::schema())
    }
}

impl ToSchema for Update<Ipv4Addr> {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("UpdateIpv4Addr")
    }
}

impl PartialSchema for Update<Ipv6Addr> {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        Self::schema(<String as PartialSchema>::schema())
    }
}

impl ToSchema for Update<Ipv6Addr> {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("UpdateIpv6Addr")
    }
}

impl PartialSchema for Update<EncryptionPubKey> {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        Self::schema(<String as PartialSchema>::schema())
    }
}

impl ToSchema for Update<EncryptionPubKey> {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("UpdateEncryptionPubKey")
    }
}

impl PartialSchema for Update<VerifyingKey> {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        Self::schema(<String as PartialSchema>::schema())
    }
}

impl ToSchema for Update<VerifyingKey> {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("UpdateVerifyingKey")
    }
}

impl PartialSchema for Update<u64> {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        Self::schema(<u64 as PartialSchema>::schema())
    }
}

impl ToSchema for Update<u64> {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("UpdateU64")
    }
}

/// Updates to the data associated with a BitAsset
#[derive(BorshSerialize, Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct BitAssetDataUpdates {
    /// Commitment to arbitrary data
    #[schema(schema_with = <Update<Hash> as PartialSchema>::schema)]
    pub commitment: Update<Hash>,
    /// Optional ipv4 addr
    #[schema(schema_with = <Update<Ipv4Addr> as PartialSchema>::schema)]
    pub ipv4_addr: Update<Ipv4Addr>,
    /// Optional ipv6 addr
    #[schema(schema_with = <Update<Ipv6Addr> as PartialSchema>::schema)]
    pub ipv6_addr: Update<Ipv6Addr>,
    /// Optional pubkey used for encryption
    #[schema(schema_with = <Update<EncryptionPubKey> as PartialSchema>::schema)]
    pub encryption_pubkey: Update<EncryptionPubKey>,
    /// Optional pubkey used for signing messages
    #[schema(schema_with = <Update<VerifyingKey> as PartialSchema>::schema)]
    pub signing_pubkey: Update<VerifyingKey>,
}
