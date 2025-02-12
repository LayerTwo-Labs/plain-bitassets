//! RPC API

use std::net::SocketAddr;

use fraction::Fraction;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use l2l_openapi::open_api;

use plain_bitassets::{
    state::{AmmPoolState, BitAssetSeqId, DutchAuctionState},
    types::{
        schema as bitassets_schema, Address, AssetId, Authorization,
        BitAssetData, BitAssetDataUpdates, BitAssetId, BitcoinOutputContent,
        Block, BlockHash, Body, DutchAuctionId, DutchAuctionParams,
        EncryptionPubKey, FilledOutputContent, Header, MerkleRoot, OutPoint,
        Output, OutputContent, PointedOutput, Transaction, TxData, TxIn, Txid,
        VerifyingKey, WithdrawalOutputContent,
    },
    wallet::Balance,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

mod schema;
#[cfg(test)]
mod test;

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct TxInfo {
    pub confirmations: Option<u32>,
    pub fee_sats: u64,
    pub txin: Option<TxIn>,
}

#[open_api(ref_schemas[
    bitassets_schema::BitcoinAddr, bitassets_schema::BitcoinBlockHash,
    bitassets_schema::BitcoinOutPoint, Address, AssetId, Authorization,
    BitAssetData, BitAssetDataUpdates, BitAssetId, BitcoinOutputContent,
    BlockHash, Body, DutchAuctionId, DutchAuctionParams, EncryptionPubKey,
    Header, MerkleRoot, OutPoint, Output, OutputContent, Transaction, TxData,
    Txid, TxIn, WithdrawalOutputContent, VerifyingKey,
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

    /// Retrieve data for a single BitAsset
    #[method(name = "bitasset_data")]
    async fn bitasset_data(
        &self,
        bitasset_id: BitAssetId,
    ) -> RpcResult<BitAssetData>;

    /// List all BitAssets
    #[open_api_method(output_schema(PartialSchema = "schema::Array<
            schema::ArrayTuple3<BitAssetSeqId, BitAssetId, BitAssetData>
        >"))]
    #[method(name = "bitassets")]
    async fn bitassets(
        &self,
    ) -> RpcResult<Vec<(BitAssetSeqId, BitAssetId, BitAssetData)>>;

    /// Balance in sats
    #[open_api_method(output_schema(ToSchema))]
    #[method(name = "bitcoin_balance")]
    async fn bitcoin_balance(&self) -> RpcResult<Balance>;

    /// Deposit to address
    #[open_api_method(output_schema(PartialSchema = "schema::BitcoinTxid"))]
    #[method(name = "create_deposit")]
    async fn create_deposit(
        &self,
        address: Address,
        value_sats: u64,
        fee_sats: u64,
    ) -> RpcResult<bitcoin::Txid>;

    /// Connect to a peer
    #[open_api_method(output_schema(ToSchema))]
    #[method(name = "connect_peer")]
    async fn connect_peer(
        &self,
        #[open_api_method_arg(schema(ToSchema = "schema::SocketAddr"))]
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
    #[open_api_method(output_schema(
        PartialSchema = "schema::ArrayTuple<u64, u64>"
    ))]
    #[method(name = "dutch_auction_collect")]
    async fn dutch_auction_collect(
        &self,
        dutch_auction_id: DutchAuctionId,
    ) -> RpcResult<(u64, u64)>;

    /// List all Dutch auctions
    #[open_api_method(output_schema(
        PartialSchema = "schema::Array<schema::ArrayTuple<DutchAuctionId, serde_json::Value>>"
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
    #[open_api_method(output_schema(
        PartialSchema = "schema::Optional<schema::Fraction>"
    ))]
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

    /// Get new encryption key
    #[method(name = "get_new_encryption_key")]
    async fn get_new_encryption_key(&self) -> RpcResult<EncryptionPubKey>;

    /// Get new verifying/signing key
    #[method(name = "get_new_verifying_key")]
    async fn get_new_verifying_key(&self) -> RpcResult<VerifyingKey>;

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
        ToSchema = "Vec<PointedOutput<FilledOutputContent>>"
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
    #[open_api_method(output_schema(ToSchema = "schema::OpenApi"))]
    #[method(name = "openapi_schema")]
    async fn openapi_schema(&self) -> RpcResult<utoipa::openapi::OpenApi>;

    /// Reserve a BitAsset
    #[method(name = "reserve_bitasset")]
    async fn reserve_bitasset(&self, plain_name: String) -> RpcResult<Txid>;

    /// Set the wallet seed from a mnemonic seed phrase
    #[open_api_method(output_schema(ToSchema))]
    #[method(name = "set_seed_from_mnemonic")]
    async fn set_seed_from_mnemonic(&self, mnemonic: String) -> RpcResult<()>;

    /// Get total sidechain wealth in sats
    #[method(name = "sidechain_wealth")]
    async fn sidechain_wealth_sats(&self) -> RpcResult<u64>;

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
        #[open_api_method_arg(schema(
            PartialSchema = "bitassets_schema::BitcoinAddr"
        ))]
        mainchain_address: bitcoin::Address<
            bitcoin::address::NetworkUnchecked,
        >,
        amount_sats: u64,
        fee_sats: u64,
        mainchain_fee_sats: u64,
    ) -> RpcResult<Txid>;
}
