//! RPC API

use std::net::{IpAddr, SocketAddr};

use fraction::Fraction;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use l2l_openapi::open_api;

use liquid_simplicity::{
    authorization::{Dst, Signature},
    net::{Peer, PeerConnectionStatus},
    state::{AmmPoolState, BitAssetSeqId, DutchAuctionState},
    types::{
        Address, AssetId, Authorization, BitAssetData, BitAssetDataUpdates,
        BitAssetId, BitcoinOutputContent, Block, BlockHash, Body,
        DutchAuctionId, DutchAuctionParams, EncryptionPubKey,
        FilledOutputContent, Header, MerkleRoot, OutPoint, Output,
        OutputContent, PointedOutput, Transaction, TxData, TxIn, Txid,
        VerifyingKey, WithdrawalBundle, WithdrawalOutputContent,
        schema as bitassets_schema,
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

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct TxProof {
    pub txid: Txid,
    pub transaction: Transaction,
    pub txin: Option<TxIn>,
    pub block: Option<Block>,
    pub sidechain_block_height: Option<u32>,
    pub bmm_inclusions: Vec<String>,
    pub best_main_verification: Option<String>,
    pub confirmations: Option<u32>,
    pub fee_sats: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct LiteWalletProofRef {
    pub txid: Txid,
    pub block_hash: Option<String>,
    pub sidechain_block_height: Option<u32>,
    pub bmm_inclusions: Vec<String>,
    pub best_main_verification: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct LiteWalletUtreexoProof {
    pub outpoint: OutPoint,
    pub leaf_hash: String,
    pub targets: Vec<u64>,
    pub hashes: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct LiteWalletUpdate {
    pub tip_hash: Option<String>,
    pub tip_height: Option<u32>,
    pub utreexo_leaf_count: u64,
    pub utreexo_roots: Vec<String>,
    pub created_utxos: Vec<PointedOutput<FilledOutputContent>>,
    pub spent_outpoints: Vec<OutPoint>,
    pub mempool_created_utxos: Vec<PointedOutput>,
    pub mempool_spent_outpoints: Vec<OutPoint>,
    pub transactions: Vec<Transaction>,
    pub proof_refs: Vec<LiteWalletProofRef>,
    pub utreexo_proofs: Vec<LiteWalletUtreexoProof>,
}

/// Private-signet Floresta-compatible service flags for peers that can serve
/// Utreexo proofs.
///
/// This is `NODE_NETWORK_LIMITED | NODE_WITNESS | UTREEXO`.
pub const FLORESTA_UTREEXO_ANCHOR_SERVICES: u64 = 1024 | 8 | (1 << 12);

/// Floresta's `anchors.json` serializes peer IPs as externally tagged enum
/// variants, for example `{ "V4": "127.0.0.1" }`.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub enum FlorestaAnchorAddress {
    V4(String),
    V6(String),
}

/// Floresta treats anchors as tried peers keyed by the Unix timestamp of the
/// last successful connection.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub enum FlorestaAnchorState {
    Tried(u64),
}

/// A Floresta-compatible private-signet Utreexo peer anchor entry.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct FlorestaUtreexoAnchor {
    pub address: FlorestaAnchorAddress,
    pub last_connected: u64,
    pub state: FlorestaAnchorState,
    pub services: u64,
    pub port: u16,
    pub id: Option<usize>,
}

impl FlorestaUtreexoAnchor {
    pub fn from_socket_addr(addr: SocketAddr, last_connected: u64) -> Self {
        let address = match addr.ip() {
            IpAddr::V4(ip) => FlorestaAnchorAddress::V4(ip.to_string()),
            IpAddr::V6(ip) => FlorestaAnchorAddress::V6(ip.to_string()),
        };

        Self {
            address,
            last_connected,
            state: FlorestaAnchorState::Tried(last_connected),
            services: FLORESTA_UTREEXO_ANCHOR_SERVICES,
            port: addr.port(),
            id: None,
        }
    }
}

/// Self-advertisement that lets Floresta discover this private-signet
/// BitAssets endpoint as a Utreexo-capable peer/source.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct FlorestaUtreexoPeerSource {
    pub network: String,
    pub anchor: FlorestaUtreexoAnchor,
    pub services: u64,
    pub service_names: Vec<String>,
    pub bitassets_rpc_url: String,
    pub bitassets_p2p_addr: String,
    pub lite_wallet_quic_addr: String,
}

#[open_api(ref_schemas[
    bitassets_schema::BitcoinAddr, bitassets_schema::BitcoinBlockHash,
    bitassets_schema::BitcoinTransaction, bitassets_schema::BitcoinOutPoint,
    bitassets_schema::SocketAddr, Address, AssetId, Authorization,
    BitAssetData, BitAssetDataUpdates, BitAssetId, BitcoinOutputContent, Block,
    BlockHash, Body, DutchAuctionId, DutchAuctionParams, EncryptionPubKey,
    FilledOutputContent, FlorestaAnchorAddress, FlorestaAnchorState,
    FlorestaUtreexoPeerSource, Header, MerkleRoot, OutPoint, Output,
    OutputContent, PointedOutput, PointedOutput<FilledOutputContent>,
    LiteWalletProofRef, LiteWalletUpdate, LiteWalletUtreexoProof,
    PeerConnectionStatus, Signature, Transaction, TxData, Txid, TxIn,
    TxProof, WithdrawalOutputContent, VerifyingKey,
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
        #[open_api_method_arg(schema(
            ToSchema = "bitassets_schema::SocketAddr"
        ))]
        addr: SocketAddr,
    ) -> RpcResult<()>;

    /// Decrypt a message with the specified encryption key corresponding to
    /// the specified encryption pubkey.
    /// Returns a decrypted hex string.
    #[method(name = "decrypt_msg")]
    async fn decrypt_msg(
        &self,
        encryption_pubkey: EncryptionPubKey,
        ciphertext: String,
    ) -> RpcResult<String>;

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

    /// Encrypt a message to the specified encryption pubkey
    /// Returns the ciphertext as a hex string.
    #[method(name = "encrypt_msg")]
    async fn encrypt_msg(
        &self,
        encryption_pubkey: EncryptionPubKey,
        msg: String,
    ) -> RpcResult<String>;

    /// Delete peer from known_peers DB.
    /// Connections to the peer are not terminated.
    #[method(name = "forget_peer")]
    async fn forget_peer(
        &self,
        #[open_api_method_arg(schema(
            PartialSchema = "bitassets_schema::SocketAddr"
        ))]
        addr: SocketAddr,
    ) -> RpcResult<()>;

    /// Format a deposit address
    #[method(name = "format_deposit_address")]
    async fn format_deposit_address(
        &self,
        address: Address,
    ) -> RpcResult<String>;

    /// Convert explicit private-signet Bitcoin P2P peer addresses to
    /// Floresta-compatible Utreexo anchor entries.
    #[open_api_method(output_schema(
        PartialSchema = "schema::Array<FlorestaUtreexoAnchor>"
    ))]
    #[method(name = "private_signet_utreexo_anchors")]
    async fn private_signet_utreexo_anchors(
        &self,
        #[open_api_method_arg(schema(
            PartialSchema = "schema::Array<bitassets_schema::SocketAddr>"
        ))]
        peers: Vec<SocketAddr>,
    ) -> RpcResult<Vec<FlorestaUtreexoAnchor>>;

    /// Export active private-signet BitAssets peers as Floresta-compatible
    /// Utreexo anchors.
    ///
    /// This is useful for Luke's private signet when a local Utreexo test node
    /// deliberately reuses the same peer address for BitAssets and Bitcoin P2P. Prefer
    /// `private_signet_utreexo_anchors` when the Bitcoin Utreexo peer addresses are
    /// known explicitly.
    #[open_api_method(output_schema(
        PartialSchema = "schema::Array<FlorestaUtreexoAnchor>"
    ))]
    #[method(name = "private_signet_active_utreexo_anchors")]
    async fn private_signet_active_utreexo_anchors(
        &self,
    ) -> RpcResult<Vec<FlorestaUtreexoAnchor>>;

    /// Advertise this private-signet BitAssets node as a Floresta Utreexo
    /// peer/source using an explicit peer address.
    #[open_api_method(output_schema(ToSchema))]
    #[method(name = "private_signet_utreexo_peer_source")]
    async fn private_signet_utreexo_peer_source(
        &self,
        #[open_api_method_arg(schema(
            ToSchema = "bitassets_schema::SocketAddr"
        ))]
        peer: SocketAddr,
    ) -> RpcResult<FlorestaUtreexoPeerSource>;

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

    /// Get mainchain blocks that commit to a specified block hash
    #[open_api_method(output_schema(
        PartialSchema = "bitassets_schema::BitcoinBlockHash"
    ))]
    #[method(name = "get_bmm_inclusions")]
    async fn get_bmm_inclusions(
        &self,
        block_hash: liquid_simplicity::types::BlockHash,
    ) -> RpcResult<Vec<bitcoin::BlockHash>>;

    /// Get the best mainchain block hash known by Thunder
    #[open_api_method(output_schema(
        PartialSchema = "schema::Optional<bitassets_schema::BitcoinBlockHash>"
    ))]
    #[method(name = "get_best_mainchain_block_hash")]
    async fn get_best_mainchain_block_hash(
        &self,
    ) -> RpcResult<Option<bitcoin::BlockHash>>;

    /// Get the best sidechain block hash known by BitAssets
    #[open_api_method(output_schema(
        PartialSchema = "schema::Optional<BlockHash>"
    ))]
    #[method(name = "get_best_sidechain_block_hash")]
    async fn get_best_sidechain_block_hash(
        &self,
    ) -> RpcResult<Option<BlockHash>>;

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

    /// Get proof-oriented archive data for a transaction in the current chain
    #[method(name = "get_transaction_proof")]
    async fn get_transaction_proof(
        &self,
        txid: Txid,
    ) -> RpcResult<Option<TxProof>>;

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

    /// Get the height of the latest failed withdrawal bundle
    #[method(name = "latest_failed_withdrawal_bundle_height")]
    async fn latest_failed_withdrawal_bundle_height(
        &self,
    ) -> RpcResult<Option<u32>>;

    /// List peers
    #[method(name = "list_peers")]
    async fn list_peers(&self) -> RpcResult<Vec<Peer>>;

    /// List all UTXOs
    #[open_api_method(output_schema(
        ToSchema = "Vec<PointedOutput<FilledOutputContent>>"
    ))]
    #[method(name = "list_utxos")]
    async fn list_utxos(
        &self,
    ) -> RpcResult<Vec<PointedOutput<FilledOutputContent>>>;

    /// Get address-scoped lite-wallet state updates.
    #[open_api_method(output_schema(ToSchema))]
    #[method(name = "get_lite_wallet_update")]
    async fn get_lite_wallet_update(
        &self,
        script_hashes: Vec<String>,
        from_block_hash: Option<String>,
    ) -> RpcResult<LiteWalletUpdate>;

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

    /// Get pending withdrawal bundle
    #[open_api_method(output_schema(ToSchema))]
    #[method(name = "pending_withdrawal_bundle")]
    async fn pending_withdrawal_bundle(
        &self,
    ) -> RpcResult<Option<WithdrawalBundle>>;

    /// Get OpenRPC schema
    #[open_api_method(output_schema(ToSchema = "schema::OpenApi"))]
    #[method(name = "openapi_schema")]
    async fn openapi_schema(&self) -> RpcResult<utoipa::openapi::OpenApi>;

    /// Register a BitAsset
    #[method(name = "register_bitasset")]
    async fn register_bitasset(
        &self,
        plain_name: String,
        initial_supply: u64,
        bitasset_data: Option<BitAssetData>,
    ) -> RpcResult<Txid>;

    /// Remove a tx from the mempool
    #[open_api_method(output_schema(ToSchema))]
    #[method(name = "remove_from_mempool")]
    async fn remove_from_mempool(&self, txid: Txid) -> RpcResult<()>;

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

    /// Sign an arbitrary message with the specified verifying key
    #[method(name = "sign_arbitrary_msg")]
    async fn sign_arbitrary_msg(
        &self,
        verifying_key: VerifyingKey,
        msg: String,
    ) -> RpcResult<Signature>;

    /// Sign an arbitrary message with the secret key for the specified address
    #[method(name = "sign_arbitrary_msg_as_addr")]
    async fn sign_arbitrary_msg_as_addr(
        &self,
        address: Address,
        msg: String,
    ) -> RpcResult<Authorization>;

    /// Submit an already authorized transaction encoded as canonical Borsh hex
    #[method(name = "submit_authorized_transaction")]
    async fn submit_authorized_transaction(
        &self,
        hex_borsh_authorized_tx: String,
    ) -> RpcResult<Txid>;

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

    /// Transfer bitassets to the specified address
    #[method(name = "transfer_bitasset")]
    async fn transfer_bitasset(
        &self,
        dest: Address,
        asset_id: BitAssetId,
        amount: u64,
        fee_sats: u64,
        memo: Option<String>,
    ) -> RpcResult<Txid>;

    /// Verify a signature on a message against the specified verifying key.
    /// Returns `true` if the signature is valid
    #[method(name = "verify_signature")]
    async fn verify_signature(
        &self,
        signature: Signature,
        verifying_key: VerifyingKey,
        dst: Dst,
        msg: String,
    ) -> RpcResult<bool>;

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
