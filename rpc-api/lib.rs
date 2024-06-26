//! RPC API

use std::{marker::PhantomData, net::SocketAddr};

use bip300301::bitcoin;
use fraction::Fraction;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use l2l_openapi::open_api;

use plain_bitassets::{
    state::{AmmPoolState, BitAssetSeqId, DutchAuctionState},
    types::{
        open_api_schemas, Address, AssetId, Authorization, BitAssetData,
        BitAssetDataUpdates, BitAssetId, Block, BlockHash, Body,
        DutchAuctionId, DutchAuctionParams, FilledOutputContent, Header,
        MerkleRoot, OutPoint, Output, PointedOutput, Transaction, TxData, TxIn,
        Txid,
    },
};
use serde::{Deserialize, Serialize};
use utoipa::{
    openapi::{RefOr, Schema, SchemaType},
    PartialSchema, ToSchema,
};

struct BitcoinAddrSchema;

impl PartialSchema for BitcoinAddrSchema {
    fn schema() -> RefOr<Schema> {
        let obj = utoipa::openapi::Object::with_type(SchemaType::String);
        RefOr::T(Schema::Object(obj))
    }
}

impl ToSchema<'static> for BitcoinAddrSchema {
    fn schema() -> (&'static str, RefOr<Schema>) {
        ("bitcoin.Address", <Self as PartialSchema>::schema())
    }
}

struct BitcoinAmountSchema;

impl PartialSchema for BitcoinAmountSchema {
    fn schema() -> RefOr<Schema> {
        let obj = utoipa::openapi::Object::with_type(SchemaType::String);
        RefOr::T(Schema::Object(obj))
    }
}

impl ToSchema<'static> for BitcoinAmountSchema {
    fn schema() -> (&'static str, RefOr<Schema>) {
        ("bitcoin.Amount", <Self as PartialSchema>::schema())
    }
}

struct BitcoinBlockHashSchema;

impl PartialSchema for BitcoinBlockHashSchema {
    fn schema() -> RefOr<Schema> {
        let obj = utoipa::openapi::Object::with_type(SchemaType::String);
        RefOr::T(Schema::Object(obj))
    }
}

impl ToSchema<'static> for BitcoinBlockHashSchema {
    fn schema() -> (&'static str, RefOr<Schema>) {
        ("bitcoin.BlockHash", <Self as PartialSchema>::schema())
    }
}

struct BitcoinOutPointSchema;

impl PartialSchema for BitcoinOutPointSchema {
    fn schema() -> RefOr<Schema> {
        let obj = utoipa::openapi::Object::new();
        RefOr::T(Schema::Object(obj))
    }
}

impl ToSchema<'static> for BitcoinOutPointSchema {
    fn schema() -> (&'static str, RefOr<Schema>) {
        ("bitcoin.OutPoint", <Self as PartialSchema>::schema())
    }
}

struct EncryptionPubKeySchema;

impl PartialSchema for EncryptionPubKeySchema {
    fn schema() -> RefOr<Schema> {
        let obj = utoipa::openapi::Object::with_type(SchemaType::String);
        RefOr::T(Schema::Object(obj))
    }
}

impl ToSchema<'static> for EncryptionPubKeySchema {
    fn schema() -> (&'static str, RefOr<Schema>) {
        ("EncryptionPubKey", <Self as PartialSchema>::schema())
    }
}

struct FractionSchema;

impl PartialSchema for FractionSchema {
    fn schema() -> RefOr<Schema> {
        let obj = utoipa::openapi::Object::new();
        RefOr::T(Schema::Object(obj))
    }
}

impl ToSchema<'static> for FractionSchema {
    fn schema() -> (&'static str, RefOr<Schema>) {
        ("Fraction", <Self as PartialSchema>::schema())
    }
}

struct HashSchema;

impl PartialSchema for HashSchema {
    fn schema() -> RefOr<Schema> {
        let obj = utoipa::openapi::Object::new();
        RefOr::T(Schema::Object(obj))
    }
}

impl ToSchema<'static> for HashSchema {
    fn schema() -> (&'static str, RefOr<Schema>) {
        ("Hash", <Self as PartialSchema>::schema())
    }
}

struct Ipv4AddrSchema;

impl PartialSchema for Ipv4AddrSchema {
    fn schema() -> RefOr<Schema> {
        let obj = utoipa::openapi::Object::with_type(SchemaType::String);
        RefOr::T(Schema::Object(obj))
    }
}

impl ToSchema<'static> for Ipv4AddrSchema {
    fn schema() -> (&'static str, RefOr<Schema>) {
        ("Ipv4Addr", <Self as PartialSchema>::schema())
    }
}

struct Ipv6AddrSchema;

impl PartialSchema for Ipv6AddrSchema {
    fn schema() -> RefOr<Schema> {
        let obj = utoipa::openapi::Object::with_type(SchemaType::String);
        RefOr::T(Schema::Object(obj))
    }
}

impl ToSchema<'static> for Ipv6AddrSchema {
    fn schema() -> (&'static str, RefOr<Schema>) {
        ("Ipv6Addr", <Self as PartialSchema>::schema())
    }
}

struct OpenApiSchema;

impl PartialSchema for OpenApiSchema {
    fn schema() -> RefOr<Schema> {
        let obj = utoipa::openapi::Object::new();
        RefOr::T(Schema::Object(obj))
    }
}

impl ToSchema<'static> for OpenApiSchema {
    fn schema() -> (&'static str, RefOr<Schema>) {
        ("OpenApiSchema", <Self as PartialSchema>::schema())
    }
}

struct SocketAddrSchema;

impl PartialSchema for SocketAddrSchema {
    fn schema() -> RefOr<Schema> {
        let obj = utoipa::openapi::Object::with_type(SchemaType::String);
        RefOr::T(Schema::Object(obj))
    }
}

impl ToSchema<'static> for SocketAddrSchema {
    fn schema() -> (&'static str, RefOr<utoipa::openapi::schema::Schema>) {
        ("SocketAddr", <Self as PartialSchema>::schema())
    }
}

/// Utoipa does not support tuples at all, so these are represented as an
/// arbitrary json value
#[derive(Default)]
struct TupleSchema<A, B>(PhantomData<A>, PhantomData<B>);

impl<A, B> PartialSchema for TupleSchema<A, B> {
    fn schema() -> RefOr<Schema> {
        let obj = utoipa::openapi::Object::with_type(SchemaType::Value);
        RefOr::T(Schema::Object(obj))
    }
}

impl<'a, A, B> ToSchema<'a> for TupleSchema<A, B> {
    fn schema() -> (&'a str, RefOr<Schema>) {
        ("Tuple", <Self as PartialSchema>::schema())
    }
}

/// Utoipa does not support tuples at all, so these are represented as an
/// arbitrary json value
#[derive(Default)]
struct Tuple3Schema<A, B, C>(PhantomData<A>, PhantomData<B>, PhantomData<C>);

impl<A, B, C> PartialSchema for Tuple3Schema<A, B, C> {
    fn schema() -> RefOr<Schema> {
        let obj = utoipa::openapi::Object::with_type(SchemaType::Value);
        RefOr::T(Schema::Object(obj))
    }
}

impl<'a, A, B, C> ToSchema<'a> for Tuple3Schema<A, B, C> {
    fn schema() -> (&'a str, RefOr<Schema>) {
        ("Tuple3", <Self as PartialSchema>::schema())
    }
}

struct VerifyingKeySchema;

impl PartialSchema for VerifyingKeySchema {
    fn schema() -> RefOr<Schema> {
        let obj = utoipa::openapi::Object::with_type(SchemaType::String);
        RefOr::T(Schema::Object(obj))
    }
}

impl ToSchema<'static> for VerifyingKeySchema {
    fn schema() -> (&'static str, RefOr<Schema>) {
        ("VerifyingKey", <Self as PartialSchema>::schema())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct TxInfo {
    pub confirmations: Option<u32>,
    pub fee_sats: u64,
    pub txin: Option<TxIn>,
}

#[open_api(ref_schemas[
    open_api_schemas::UpdateHash,
    open_api_schemas::UpdateIpv4Addr, open_api_schemas::UpdateIpv6Addr,
    open_api_schemas::UpdateEncryptionPubKey,
    open_api_schemas::UpdateVerifyingKey,
    Address, AssetId, Authorization, BitAssetData, BitAssetDataUpdates,
    BitcoinBlockHashSchema, BitcoinOutPointSchema,
    BlockHash, Body, DutchAuctionId, DutchAuctionParams, EncryptionPubKeySchema,
    HashSchema, Header, Ipv4AddrSchema, Ipv6AddrSchema, MerkleRoot, OutPoint,
    Output, Transaction, TxData, Txid, TxIn,
    VerifyingKeySchema
])]
#[rpc(client, server)]
pub trait Rpc {
    /// Burn an AMM position
    #[method(name = "amm_burn")]
    async fn amm_burn(
        &self,
        asset0: AssetId,
        asset1: AssetId,
        lp_token_amount: u64,
    ) -> RpcResult<Txid>;

    /// Mint an AMM position
    #[method(name = "amm_mint")]
    async fn amm_mint(
        &self,
        asset0: AssetId,
        asset1: AssetId,
        amount0: u64,
        amount1: u64,
    ) -> RpcResult<Txid>;

    /// Returns the amount of `asset_receive` to receive
    #[method(name = "amm_swap")]
    async fn amm_swap(
        &self,
        asset_spend: AssetId,
        asset_receive: AssetId,
        amount_spend: u64,
    ) -> RpcResult<u64>;

    /// List all BitAssets
    #[open_api_method(output_schema(
        PartialSchema = "Vec<Tuple3Schema<BitAssetSeqId, BitAssetId, BitAssetData>>"
    ))]
    #[method(name = "bitassets")]
    async fn bitassets(
        &self,
    ) -> RpcResult<Vec<(BitAssetSeqId, BitAssetId, BitAssetData)>>;

    /// Balance in sats
    #[method(name = "bitcoin_balance")]
    async fn bitcoin_balance(&self) -> RpcResult<u64>;

    /// Connect to a peer
    #[open_api_method(output_schema(ToSchema))]
    #[method(name = "connect_peer")]
    async fn connect_peer(
        &self,
        #[open_api_method_arg(schema(ToSchema = "SocketAddrSchema"))]
        addr: SocketAddr,
    ) -> RpcResult<()>;

    /// Returns the amount of the base asset to receive
    #[method(name = "dutch_auction_bid")]
    async fn dutch_auction_bid(
        &self,
        dutch_auction_id: DutchAuctionId,
        bid_size: u64,
    ) -> RpcResult<u64>;

    /// Create a dutch auction
    #[method(name = "dutch_auction_create")]
    async fn dutch_auction_create(
        &self,
        #[open_api_method_arg(schema(ToSchema))]
        dutch_auction_params: DutchAuctionParams,
    ) -> RpcResult<Txid>;

    /// Returns the amount of the base asset and quote asset to receive
    #[open_api_method(output_schema(ToSchema = "TupleSchema<u64, u64>"))]
    #[method(name = "dutch_auction_collect")]
    async fn dutch_auction_collect(
        &self,
        dutch_auction_id: DutchAuctionId,
    ) -> RpcResult<(u64, u64)>;

    /// List all Dutch auctions
    #[open_api_method(output_schema(
        ToSchema = "TupleSchema<DutchAuctionId, DutchAuctionState>"
    ))]
    #[method(name = "dutch_auctions")]
    async fn dutch_auctions(
        &self,
    ) -> RpcResult<Vec<(DutchAuctionId, DutchAuctionState)>>;

    /// Format a deposit address
    #[method(name = "format_deposit_address")]
    async fn format_deposit_address(
        &self,
        address: Address,
    ) -> RpcResult<String>;

    /// Generate a mnemonic seed phrase
    #[method(name = "generate_mnemonic")]
    async fn generate_mnemonic(&self) -> RpcResult<String>;

    /// Get the state of the specified AMM pool
    #[open_api_method(output_schema(ToSchema))]
    #[method(name = "get_amm_pool_state")]
    async fn get_amm_pool_state(
        &self,
        asset0: AssetId,
        asset1: AssetId,
    ) -> RpcResult<AmmPoolState>;

    /// Get the current price for the specified pair
    #[open_api_method(output_schema(PartialSchema = "Option<FractionSchema>"))]
    #[method(name = "get_amm_price")]
    async fn get_amm_price(
        &self,
        base: AssetId,
        quote: AssetId,
    ) -> RpcResult<Option<Fraction>>;

    /// Get block data
    #[open_api_method(output_schema(ToSchema))]
    #[method(name = "get_block")]
    async fn get_block(&self, block_hash: BlockHash) -> RpcResult<Block>;

    /// Get a new address
    #[method(name = "get_new_address")]
    async fn get_new_address(&self) -> RpcResult<Address>;

    /// Get transaction by txid
    #[method(name = "get_transaction")]
    async fn get_transaction(
        &self,
        txid: Txid,
    ) -> RpcResult<Option<Transaction>>;

    /// Get information about a transaction in the current chain
    #[method(name = "get_transaction_info")]
    async fn get_transaction_info(
        &self,
        txid: Txid,
    ) -> RpcResult<Option<TxInfo>>;

    /// Get wallet addresses, sorted by base58 encoding
    #[method(name = "get_wallet_addresses")]
    async fn get_wallet_addresses(&self) -> RpcResult<Vec<Address>>;

    /// Get wallet UTXOs
    #[method(name = "get_wallet_utxos")]
    async fn get_wallet_utxos(
        &self,
    ) -> RpcResult<Vec<PointedOutput<FilledOutputContent>>>;

    /// Get the current block count
    #[method(name = "getblockcount")]
    async fn getblockcount(&self) -> RpcResult<u32>;

    /// List all UTXOs
    #[open_api_method(output_schema(
        PartialSchema = "Vec<open_api_schemas::PointedFilledOutput>"
    ))]
    #[method(name = "list_utxos")]
    async fn list_utxos(
        &self,
    ) -> RpcResult<Vec<PointedOutput<FilledOutputContent>>>;

    /// Attempt to mine a sidechain block
    #[open_api_method(output_schema(ToSchema))]
    #[method(name = "mine")]
    async fn mine(&self, fee: Option<u64>) -> RpcResult<()>;

    /*
    #[method(name = "my_unconfirmed_stxos")]
    async fn my_unconfirmed_stxos(&self) -> RpcResult<Vec<InPoint>>;
    */

    /// List unconfirmed owned UTXOs
    #[method(name = "my_unconfirmed_utxos")]
    async fn my_unconfirmed_utxos(&self) -> RpcResult<Vec<PointedOutput>>;

    /// List owned UTXOs
    #[method(name = "my_utxos")]
    async fn my_utxos(
        &self,
    ) -> RpcResult<Vec<PointedOutput<FilledOutputContent>>>;

    /// Get OpenRPC schema
    #[open_api_method(output_schema(PartialSchema = "OpenApiSchema"))]
    #[method(name = "openapi_schema")]
    async fn openapi_schema(&self) -> RpcResult<utoipa::openapi::OpenApi>;

    /// Reserve a BitAsset
    #[method(name = "reserve_bitasset")]
    async fn reserve_bitasset(&self, plain_name: String) -> RpcResult<Txid>;

    /// Set the wallet seed from a mnemonic seed phrase
    #[open_api_method(output_schema(ToSchema))]
    #[method(name = "set_seed_from_mnemonic")]
    async fn set_seed_from_mnemonic(&self, mnemonic: String) -> RpcResult<()>;

    /// Get total sidechain wealth
    #[open_api_method(output_schema(ToSchema = "BitcoinAmountSchema"))]
    #[method(name = "sidechain_wealth")]
    async fn sidechain_wealth(&self) -> RpcResult<bitcoin::Amount>;

    /// Stop the node
    #[method(name = "stop")]
    async fn stop(&self);

    /// Transfer funds to the specified address
    #[method(name = "transfer")]
    async fn transfer(
        &self,
        dest: Address,
        value: u64,
        fee: u64,
        memo: Option<String>,
    ) -> RpcResult<Txid>;

    /// Initiate a withdrawal to the specified mainchain address
    #[method(name = "withdraw")]
    async fn withdraw(
        &self,
        #[open_api_method_arg(schema(PartialSchema = "BitcoinAddrSchema"))]
        mainchain_address: bitcoin::Address<
            bitcoin::address::NetworkUnchecked,
        >,
        amount_sats: u64,
        fee_sats: u64,
        mainchain_fee_sats: u64,
    ) -> RpcResult<Txid>;
}
