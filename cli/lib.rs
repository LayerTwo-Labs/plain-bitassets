use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use bip300301::bitcoin;
use clap::{Parser, Subcommand};
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use plain_bitassets::types::{
    Address, AssetId, BlockHash, DutchAuctionId, DutchAuctionParams,
};
use plain_bitassets_app_rpc_api::RpcClient;

#[derive(Clone, Debug, Subcommand)]
#[command(arg_required_else_help(true))]
pub enum Command {
    /// Burn an AMM position
    AmmBurn {
        asset0: AssetId,
        asset1: AssetId,
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
    /// List all BitAssets
    Bitassets,
    /// Get Bitcoin balance in sats
    BitcoinBalance,
    /// Connect to a peer
    ConnectPeer { addr: SocketAddr },
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
    GetAmmPoolState { asset0: AssetId, asset1: AssetId },
    /// Get the current price for the specified pair
    GetAmmPrice { base: AssetId, quote: AssetId },
    /// Get block data
    GetBlock { block_hash: BlockHash },
    /// Get the hash of the block at the specified height
    GetBlockHash { height: u32 },
    /// Get the current block count
    GetBlockcount,
    /// Get a new address
    GetNewAddress,
    /// Attempt to mine a sidechain block
    Mine {
        #[arg(long)]
        fee_sats: Option<u64>,
    },
    /// List unconfirmed owned UTXOs
    MyUnconfirmedUtxos,
    /// List owned UTXOs
    MyUtxos,
    /// Reserve a BitAsset
    ReserveBitasset { plaintext_name: String },
    /// Set the wallet seed from a mnemonic seed phrase
    SetSeedFromMnemonic { mnemonic: String },
    /// Get total sidechain wealth
    SidechainWealth,
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

const DEFAULT_RPC_ADDR: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2020);

#[derive(Clone, Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// address for use by the RPC server
    #[arg(default_value_t = DEFAULT_RPC_ADDR, long)]
    pub rpc_addr: SocketAddr,

    #[command(subcommand)]
    pub command: Command,
}

impl Cli {
    pub async fn run(self) -> anyhow::Result<String> {
        let rpc_client: HttpClient = HttpClientBuilder::default()
            .build(format!("http://{}", self.rpc_addr))?;
        let res = match self.command {
            Command::AmmBurn {
                asset0,
                asset1,
                lp_token_amount,
            } => {
                let txid = rpc_client
                    .amm_burn(asset0, asset1, lp_token_amount)
                    .await?;
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
            Command::Bitassets => {
                let bitassets = rpc_client.bitassets().await?;
                serde_json::to_string_pretty(&bitassets)?
            }
            Command::BitcoinBalance => {
                let balance = rpc_client.bitcoin_balance().await?;
                format!("{balance}")
            }
            Command::ConnectPeer { addr } => {
                let () = rpc_client.connect_peer(addr).await?;
                String::default()
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
                let state =
                    rpc_client.get_amm_pool_state(asset0, asset1).await?;
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
            Command::GetBlockHash { height } => {
                let block_hash = rpc_client.get_block_hash(height).await?;
                format!("{block_hash}")
            }
            Command::GetBlockcount => {
                let blockcount = rpc_client.getblockcount().await?;
                format!("{blockcount}")
            }
            Command::GetNewAddress => {
                let address = rpc_client.get_new_address().await?;
                format!("{address}")
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
            Command::ReserveBitasset { plaintext_name } => {
                let txid = rpc_client.reserve_bitasset(plaintext_name).await?;
                format!("{txid}")
            }
            Command::SetSeedFromMnemonic { mnemonic } => {
                let () = rpc_client.set_seed_from_mnemonic(mnemonic).await?;
                String::default()
            }
            Command::SidechainWealth => {
                let sidechain_wealth = rpc_client.sidechain_wealth().await?;
                format!("{sidechain_wealth}")
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
        };
        Ok(res)
    }
}
