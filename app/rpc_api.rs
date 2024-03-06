//! RPC API

use fraction::Fraction;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};

use plain_bitassets::{
    state::{AmmPoolState, BitAssetSeqId, DutchAuctionState},
    types::{
        Address, AssetId, BitAssetData, BitAssetId, Block, BlockHash,
        DutchAuctionId, DutchAuctionParams, FilledOutput, OutPoint, Output,
    },
};

#[rpc(client, server)]
pub trait Rpc {
    #[method(name = "stop")]
    async fn stop(&self);

    /// Balance in sats
    #[method(name = "bitcoin_balance")]
    async fn bitcoin_balance(&self) -> RpcResult<u64>;

    #[method(name = "format_deposit_address")]
    async fn format_deposit_address(
        &self,
        address: Address,
    ) -> RpcResult<String>;

    #[method(name = "getblockcount")]
    async fn getblockcount(&self) -> RpcResult<u32>;

    #[method(name = "get_amm_price")]
    async fn get_amm_price(
        &self,
        base: AssetId,
        quote: AssetId,
    ) -> RpcResult<Option<Fraction>>;

    #[method(name = "get_amm_pool_state")]
    async fn get_amm_pool_state(
        &self,
        asset0: AssetId,
        asset1: AssetId,
    ) -> RpcResult<AmmPoolState>;

    #[method(name = "amm_mint")]
    async fn amm_mint(
        &self,
        asset0: AssetId,
        asset1: AssetId,
        amount0: u64,
        amount1: u64,
    ) -> RpcResult<()>;

    #[method(name = "amm_burn")]
    async fn amm_burn(
        &self,
        asset0: AssetId,
        asset1: AssetId,
        lp_token_amount: u64,
    ) -> RpcResult<()>;

    /// Returns the amount of `asset_receive` to receive
    #[method(name = "amm_swap")]
    async fn amm_swap(
        &self,
        asset_spend: AssetId,
        asset_receive: AssetId,
        amount_spend: u64,
    ) -> RpcResult<u64>;

    /// List all BitAssets
    #[method(name = "bitassets")]
    async fn bitassets(
        &self,
    ) -> RpcResult<Vec<(BitAssetSeqId, BitAssetId, BitAssetData)>>;

    /// List all Dutch auctions
    #[method(name = "dutch_auctions")]
    async fn dutch_auctions(
        &self,
    ) -> RpcResult<Vec<(DutchAuctionId, DutchAuctionState)>>;

    #[method(name = "dutch_auction_create")]
    async fn dutch_auction_create(
        &self,
        dutch_auction_params: DutchAuctionParams,
    ) -> RpcResult<()>;

    /// Returns the amount of the base asset to receive
    #[method(name = "dutch_auction_bid")]
    async fn dutch_auction_bid(
        &self,
        dutch_auction_id: DutchAuctionId,
        bid_size: u64,
    ) -> RpcResult<u64>;

    /// Returns the amount of the base asset and quote asset to receive
    #[method(name = "dutch_auction_collect")]
    async fn dutch_auction_collect(
        &self,
        dutch_auction_id: DutchAuctionId,
    ) -> RpcResult<(u64, u64)>;

    #[method(name = "get_block_hash")]
    async fn get_block_hash(&self, height: u32) -> RpcResult<BlockHash>;

    #[method(name = "get_block")]
    async fn get_block(&self, block_hash: BlockHash) -> RpcResult<Block>;

    #[method(name = "mine")]
    async fn mine(&self, fee: Option<u64>) -> RpcResult<()>;

    /*
    #[method(name = "my_unconfirmed_stxos")]
    async fn my_unconfirmed_stxos(&self) -> RpcResult<Vec<InPoint>>;
    */

    #[method(name = "my_unconfirmed_utxos")]
    async fn my_unconfirmed_utxos(&self) -> RpcResult<Vec<(OutPoint, Output)>>;

    #[method(name = "my_utxos")]
    async fn my_utxos(&self) -> RpcResult<Vec<FilledOutput>>;

    #[method(name = "get_new_address")]
    async fn get_new_address(&self) -> RpcResult<Address>;

    #[method(name = "generate_mnemonic")]
    async fn generate_mnemonic(&self) -> RpcResult<String>;

    #[method(name = "set_seed_from_mnemonic")]
    async fn set_seed_from_mnemonic(&self, mnemonic: String) -> RpcResult<()>;

    #[method(name = "transfer")]
    async fn transfer(
        &self,
        dest: Address,
        value: u64,
        fee: u64,
        memo: Option<String>,
    ) -> RpcResult<()>;

    #[method(name = "reserve_bitasset")]
    async fn reserve_bitasset(&self, plain_name: String) -> RpcResult<()>;
}
