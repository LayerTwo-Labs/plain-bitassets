use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::LazyLock,
    time::Duration,
};

use clap::{Parser, Subcommand};
use http::HeaderMap;
use jsonrpsee::{core::client::ClientT, http_client::HttpClientBuilder};
use plain_bitassets::{
    authorization::{Dst, Signature},
    types::{
        Address, AssetId, BitAssetData, BitAssetId, BlockHash, DutchAuctionId,
        DutchAuctionParams, VerifyingKey, THIS_SIDECHAIN,
    },
};
use plain_bitassets_app_rpc_api::RpcClient;
use tracing_subscriber::layer::SubscriberExt as _;
use url::Url;

#[derive(Clone, Debug, Subcommand)]
#[command(arg_required_else_help(true))]
pub enum Command {
    /// Burn an AMM position
    AmmBurn {
        #[arg(long)]
        asset0: AssetId,
        #[arg(long)]
        asset1: AssetId,
        #[arg(long)]
        lp_token_amount: u64,
    },
    /// Mint an AMM position
    AmmMint {
        #[arg(long)]
        asset0: AssetId,
        #[arg(long)]
        asset1: AssetId,
        #[arg(long)]
        amount0: u64,
        #[arg(long)]
        amount1: u64,
    },
    /// Returns the amount of `asset_receive` to receive
    AmmSwap {
        #[arg(long)]
        asset_spend: AssetId,
        #[arg(long)]
        asset_receive: AssetId,
        #[arg(long)]
        amount_spend: u64,
    },
    /// Retrieve data for a single BitAsset
    #[command(name = "bitasset-data")]
    BitAssetData { bitasset_id: BitAssetId },
    /// List all BitAssets
    Bitassets,
    /// Get Bitcoin balance in sats
    BitcoinBalance,
    /// Connect to a peer
    ConnectPeer { addr: SocketAddr },
    /// Deposit to address
    CreateDeposit {
        address: Address,
        #[arg(long)]
        value_sats: u64,
        #[arg(long)]
        fee_sats: u64,
    },
    /// Returns the amount of the base asset to receive
    DutchAuctionBid {
        #[arg(long)]
        auction_id: DutchAuctionId,
        #[arg(long)]
        bid_size: u64,
    },
    /// Create a dutch auction
    DutchAuctionCreate {
        #[command(flatten)]
        params: DutchAuctionParams,
    },
    /// Returns the amount of the base asset and quote asset to receive
    DutchAuctionCollect { auction_id: DutchAuctionId },
    /// List all Dutch auctions
    DutchAuctions,
    /// Format a deposit address
    FormatDepositAddress { address: Address },
    /// Generate a mnemonic seed phrase
    GenerateMnemonic,
    /// Get the state of the specified AMM pool
    GetAmmPoolState {
        #[arg(long)]
        asset0: AssetId,
        #[arg(long)]
        asset1: AssetId,
    },
    /// Get the current price for the specified pair
    GetAmmPrice {
        #[arg(long)]
        base: AssetId,
        #[arg(long)]
        quote: AssetId,
    },
    /// Get the best mainchain block hash
    GetBestMainchainBlockHash,
    /// Get the best sidechain block hash
    GetBestSidechainBlockHash,
    /// Get block data
    GetBlock { block_hash: BlockHash },
    /// Get the current block count
    GetBlockcount,
    /// Get mainchain blocks that commit to a specified block hash
    GetBmmInclusions {
        block_hash: plain_bitassets::types::BlockHash,
    },
    /// Get a new address
    GetNewAddress,
    /// Get a new encryption pubkey
    GetNewEncryptionKey,
    /// Get a new verifying key
    GetNewVerifyingKey,
    /// Get wallet addresses, sorted by base58 encoding
    GetWalletAddresses,
    /// Get wallet UTXOs
    GetWalletUtxos,
    /// Get the height of the latest failed withdrawal bundle
    LatestFailedWithdrawalBundleHeight,
    /// List peers
    ListPeers,
    /// List all UTXOs
    ListUtxos,
    /// Attempt to mine a sidechain block
    Mine {
        #[arg(long)]
        fee_sats: Option<u64>,
    },
    /// List unconfirmed owned UTXOs
    MyUnconfirmedUtxos,
    /// List owned UTXOs
    MyUtxos,
    /// Show OpenAPI schema
    #[command(name = "openapi-schema")]
    OpenApiSchema,
    /// Get pending withdrawal bundle
    PendingWithdrawalBundle,
    /// Register a BitAsset
    RegisterBitasset {
        plaintext_name: String,
        #[arg(long)]
        initial_supply: u64,
        #[command(flatten)]
        bitasset_data: Box<BitAssetData>,
    },
    /// Reserve a BitAsset
    ReserveBitasset { plaintext_name: String },
    /// Set the wallet seed from a mnemonic seed phrase
    SetSeedFromMnemonic { mnemonic: String },
    /// Get total sidechain wealth
    SidechainWealth,
    /// Sign an arbitrary message with the specified verifying key
    SignArbitraryMsg {
        #[arg(long)]
        verifying_key: VerifyingKey,
        #[arg(long)]
        msg: String,
    },
    /// Sign an arbitrary message with the secret key for the specified address
    SignArbitraryMsgAsAddr {
        #[arg(long)]
        address: Address,
        #[arg(long)]
        msg: String,
    },
    /// Stop the node
    Stop,
    /// Transfer funds to the specified address
    Transfer {
        dest: Address,
        #[arg(long)]
        value_sats: u64,
        #[arg(long)]
        fee_sats: u64,
    },
    /// Transfer bitassets to the specified address
    TransferBitasset {
        dest: Address,
        #[arg(long)]
        asset_id: BitAssetId,
        #[arg(long)]
        amount: u64,
        #[arg(long)]
        fee_sats: u64,
    },
    /// Verify a signature on a message against the specified verifying key.
    /// Returns `true` if the signature is valid
    VerifySignature {
        #[arg(long)]
        signature: Signature,
        #[arg(long)]
        verifying_key: VerifyingKey,
        #[arg(long)]
        dst: Dst,
        #[arg(long)]
        msg: String,
    },
    /// Initiate a withdrawal to the specified mainchain address
    Withdraw {
        mainchain_address: bitcoin::Address<bitcoin::address::NetworkUnchecked>,
        #[arg(long)]
        amount_sats: u64,
        #[arg(long)]
        fee_sats: u64,
        #[arg(long)]
        mainchain_fee_sats: u64,
    },
}

const DEFAULT_RPC_ADDR: SocketAddr = SocketAddr::new(
    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
    6000 + THIS_SIDECHAIN as u16,
);

static DEFAULT_RPC_URL: LazyLock<Url> = LazyLock::new(|| {
    Url::parse(&format!("http://{DEFAULT_RPC_ADDR}")).unwrap()
});

const DEFAULT_TIMEOUT_SECS: u64 = 60;

#[derive(Clone, Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
    /// Base URL used for requests to the RPC server.
    #[arg(default_value_t = DEFAULT_RPC_URL.clone(), long)]
    pub rpc_url: Url,
    /// Timeout for RPC requests in seconds.
    #[arg(default_value_t = DEFAULT_TIMEOUT_SECS, long = "timeout")]
    timeout_secs: u64,
    #[arg(short, long, help = "Enable verbose HTTP output")]
    pub verbose: bool,
}

impl Cli {
    pub fn new(
        command: Command,
        rpc_url: Url,
        timeout_secs: Option<u64>,
        verbose: Option<bool>,
    ) -> Self {
        Self {
            command,
            rpc_url,
            timeout_secs: timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS),
            verbose: verbose.unwrap_or(false),
        }
    }
}
/// Handle a command, returning CLI output
async fn handle_command<RpcClient>(
    rpc_client: &RpcClient,
    command: Command,
) -> anyhow::Result<String>
where
    RpcClient: ClientT + Sync,
{
    Ok(match command {
        Command::AmmBurn {
            asset0,
            asset1,
            lp_token_amount,
        } => {
            let txid =
                rpc_client.amm_burn(asset0, asset1, lp_token_amount).await?;
            format!("{txid}")
        }
        Command::AmmMint {
            asset0,
            asset1,
            amount0,
            amount1,
        } => {
            let txid = rpc_client
                .amm_mint(asset0, asset1, amount0, amount1)
                .await?;
            format!("{txid}")
        }
        Command::AmmSwap {
            asset_spend,
            asset_receive,
            amount_spend,
        } => {
            let amount = rpc_client
                .amm_swap(asset_spend, asset_receive, amount_spend)
                .await?;
            format!("{amount}")
        }
        Command::BitAssetData { bitasset_id } => {
            let bitasset_data = rpc_client.bitasset_data(bitasset_id).await?;
            serde_json::to_string_pretty(&bitasset_data)?
        }
        Command::Bitassets => {
            let bitassets = rpc_client.bitassets().await?;
            serde_json::to_string_pretty(&bitassets)?
        }
        Command::BitcoinBalance => {
            let balance = rpc_client.bitcoin_balance().await?;
            serde_json::to_string_pretty(&balance)?
        }
        Command::ConnectPeer { addr } => {
            let () = rpc_client.connect_peer(addr).await?;
            String::default()
        }
        Command::CreateDeposit {
            address,
            value_sats,
            fee_sats,
        } => {
            let txid = rpc_client
                .create_deposit(address, value_sats, fee_sats)
                .await?;
            format!("{txid}")
        }
        Command::DutchAuctionBid {
            auction_id,
            bid_size,
        } => {
            let amount =
                rpc_client.dutch_auction_bid(auction_id, bid_size).await?;
            format!("{amount}")
        }
        Command::DutchAuctionCreate { params } => {
            let txid = rpc_client.dutch_auction_create(params).await?;
            format!("{txid}")
        }
        Command::DutchAuctionCollect { auction_id } => {
            let (amount0, amount1) =
                rpc_client.dutch_auction_collect(auction_id).await?;
            let resp = serde_json::json!({
                "amount0": amount0,
                "amount1": amount1
            });
            serde_json::to_string_pretty(&resp)?
        }
        Command::DutchAuctions => {
            let auctions = rpc_client.dutch_auctions().await?;
            serde_json::to_string_pretty(&auctions)?
        }
        Command::FormatDepositAddress { address } => {
            rpc_client.format_deposit_address(address).await?
        }
        Command::GenerateMnemonic => rpc_client.generate_mnemonic().await?,
        Command::GetAmmPoolState { asset0, asset1 } => {
            let state = rpc_client.get_amm_pool_state(asset0, asset1).await?;
            serde_json::to_string_pretty(&state)?
        }
        Command::GetAmmPrice { base, quote } => {
            let price = rpc_client.get_amm_price(base, quote).await?;
            serde_json::to_string_pretty(&price)?
        }
        Command::GetBlock { block_hash } => {
            let block = rpc_client.get_block(block_hash).await?;
            serde_json::to_string_pretty(&block)?
        }
        Command::GetBlockcount => {
            let blockcount = rpc_client.getblockcount().await?;
            format!("{blockcount}")
        }
        Command::GetBestMainchainBlockHash => {
            let block_hash = rpc_client.get_best_mainchain_block_hash().await?;
            serde_json::to_string_pretty(&block_hash)?
        }
        Command::GetBestSidechainBlockHash => {
            let block_hash = rpc_client.get_best_sidechain_block_hash().await?;
            serde_json::to_string_pretty(&block_hash)?
        }
        Command::GetBmmInclusions { block_hash } => {
            let bmm_inclusions =
                rpc_client.get_bmm_inclusions(block_hash).await?;
            serde_json::to_string_pretty(&bmm_inclusions)?
        }
        Command::GetNewAddress => {
            let address = rpc_client.get_new_address().await?;
            format!("{address}")
        }
        Command::GetNewEncryptionKey => {
            let epk = rpc_client.get_new_encryption_key().await?;
            format!("{epk}")
        }
        Command::GetNewVerifyingKey => {
            let vk = rpc_client.get_new_verifying_key().await?;
            format!("{vk}")
        }
        Command::GetWalletAddresses => {
            let addresses = rpc_client.get_wallet_addresses().await?;
            serde_json::to_string_pretty(&addresses)?
        }
        Command::GetWalletUtxos => {
            let utxos = rpc_client.get_wallet_utxos().await?;
            serde_json::to_string_pretty(&utxos)?
        }
        Command::LatestFailedWithdrawalBundleHeight => {
            let height =
                rpc_client.latest_failed_withdrawal_bundle_height().await?;
            serde_json::to_string_pretty(&height)?
        }
        Command::ListPeers => {
            let peers = rpc_client.list_peers().await?;
            serde_json::to_string_pretty(&peers)?
        }
        Command::ListUtxos => {
            let utxos = rpc_client.list_utxos().await?;
            serde_json::to_string_pretty(&utxos)?
        }
        Command::Mine { fee_sats } => {
            let () = rpc_client.mine(fee_sats).await?;
            String::default()
        }
        Command::MyUnconfirmedUtxos => {
            let utxos = rpc_client.my_unconfirmed_utxos().await?;
            serde_json::to_string_pretty(&utxos)?
        }
        Command::MyUtxos => {
            let utxos = rpc_client.my_utxos().await?;
            serde_json::to_string_pretty(&utxos)?
        }
        Command::OpenApiSchema => {
            let openapi =
                <plain_bitassets_app_rpc_api::RpcDoc as utoipa::OpenApi>::openapi();
            openapi.to_pretty_json()?
        }
        Command::PendingWithdrawalBundle => {
            let withdrawal_bundle =
                rpc_client.pending_withdrawal_bundle().await?;
            serde_json::to_string_pretty(&withdrawal_bundle)?
        }
        Command::RegisterBitasset {
            plaintext_name,
            initial_supply,
            bitasset_data,
        } => {
            let txid = rpc_client
                .register_bitasset(
                    plaintext_name,
                    initial_supply,
                    Some(*bitasset_data),
                )
                .await?;
            format!("{txid}")
        }
        Command::ReserveBitasset { plaintext_name } => {
            let txid = rpc_client.reserve_bitasset(plaintext_name).await?;
            format!("{txid}")
        }
        Command::SetSeedFromMnemonic { mnemonic } => {
            let () = rpc_client.set_seed_from_mnemonic(mnemonic).await?;
            String::default()
        }
        Command::SidechainWealth => {
            let sidechain_wealth = rpc_client.sidechain_wealth_sats().await?;
            format!("{sidechain_wealth}")
        }
        Command::SignArbitraryMsg { verifying_key, msg } => {
            let sig = rpc_client.sign_arbitrary_msg(verifying_key, msg).await?;
            format!("{sig}")
        }
        Command::SignArbitraryMsgAsAddr { address, msg } => {
            let authorization =
                rpc_client.sign_arbitrary_msg_as_addr(address, msg).await?;
            serde_json::to_string_pretty(&authorization)?
        }
        Command::Stop => {
            let () = rpc_client.stop().await?;
            String::default()
        }
        Command::Transfer {
            dest,
            value_sats,
            fee_sats,
        } => {
            let txid = rpc_client
                .transfer(dest, value_sats, fee_sats, None)
                .await?;
            format!("{txid}")
        }
        Command::TransferBitasset {
            dest,
            asset_id,
            amount,
            fee_sats,
        } => {
            let txid = rpc_client
                .transfer_bitasset(dest, asset_id, amount, fee_sats, None)
                .await?;
            format!("{txid}")
        }
        Command::VerifySignature {
            signature,
            verifying_key,
            dst,
            msg,
        } => {
            let res = rpc_client
                .verify_signature(signature, verifying_key, dst, msg)
                .await?;
            format!("{res}")
        }
        Command::Withdraw {
            mainchain_address,
            amount_sats,
            fee_sats,
            mainchain_fee_sats,
        } => {
            let txid = rpc_client
                .withdraw(
                    mainchain_address,
                    amount_sats,
                    fee_sats,
                    mainchain_fee_sats,
                )
                .await?;
            format!("{txid}")
        }
    })
}

fn set_tracing_subscriber() -> anyhow::Result<()> {
    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stdout()))
        .with_file(true)
        .with_line_number(true);

    let subscriber = tracing_subscriber::registry().with(stdout_layer);
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

impl Cli {
    pub async fn run(self) -> anyhow::Result<String> {
        if self.verbose {
            set_tracing_subscriber()?;
        }
        let request_id = uuid::Uuid::new_v4().as_simple().to_string();
        tracing::info!(%request_id);
        let builder = HttpClientBuilder::default()
            .request_timeout(Duration::from_secs(self.timeout_secs))
            .set_max_logging_length(1024)
            .set_headers(HeaderMap::from_iter([(
                http::header::HeaderName::from_static("x-request-id"),
                http::header::HeaderValue::from_str(&request_id)?,
            )]));
        let client = builder.build(self.rpc_url)?;
        let result = handle_command(&client, self.command).await?;
        Ok(result)
    }
}
