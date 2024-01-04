use std::{borrow::Cow, cmp::Ordering, net::SocketAddr};

use fraction::Fraction;
use jsonrpsee::{
    core::{async_trait, RpcResult},
    proc_macros::rpc,
    server::Server,
    types::{ErrorObject, ResponsePayload},
};

use plain_bitassets::{
    node,
    state::{self, AmmPoolState},
    types::{Address, AssetId, Block, BlockHash, Transaction},
    wallet,
};

use crate::app::{self, App};

#[rpc(server)]
pub trait Rpc {
    #[method(name = "stop")]
    async fn stop(&self);

    #[method(name = "getblockcount")]
    async fn getblockcount(&self) -> u32;

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

    #[method(name = "get_block_hash")]
    async fn get_block_hash(&self, height: u32) -> RpcResult<BlockHash>;

    #[method(name = "get_block")]
    async fn get_block(&self, block_hash: BlockHash) -> RpcResult<Block>;

    #[method(name = "mine")]
    async fn mine(&self) -> RpcResult<()>;

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
    async fn reserve_bitasset(
        &self,
        plain_name: String,
    ) -> ResponsePayload<'static, ()>;
}

pub struct RpcServerImpl {
    app: App,
}

fn custom_err(err_msg: impl Into<String>) -> ErrorObject<'static> {
    ErrorObject::owned(-1, err_msg.into(), Option::<()>::None)
}

fn convert_app_err(err: app::Error) -> ErrorObject<'static> {
    custom_err(err.to_string())
}

fn convert_node_err(err: node::Error) -> ErrorObject<'static> {
    custom_err(err.to_string())
}

fn convert_wallet_err(err: wallet::Error) -> ErrorObject<'static> {
    custom_err(err.to_string())
}

#[async_trait]
impl RpcServer for RpcServerImpl {
    async fn stop(&self) {
        std::process::exit(0);
    }

    async fn getblockcount(&self) -> u32 {
        self.app.node.get_height().unwrap_or(0)
    }

    async fn get_amm_price(
        &self,
        base: AssetId,
        quote: AssetId,
    ) -> RpcResult<Option<Fraction>> {
        self.app
            .node
            .try_get_amm_price(base, quote)
            .map_err(convert_node_err)
    }

    async fn get_amm_pool_state(
        &self,
        asset0: AssetId,
        asset1: AssetId,
    ) -> RpcResult<AmmPoolState> {
        self.app
            .node
            .get_amm_pool_state(asset0, asset1)
            .map_err(convert_node_err)
    }

    async fn amm_mint(
        &self,
        asset0: AssetId,
        asset1: AssetId,
        amount0: u64,
        amount1: u64,
    ) -> RpcResult<()> {
        let amm_pool_state = self.get_amm_pool_state(asset0, asset1).await?;
        let next_amm_pool_state = amm_pool_state
            .mint(amount0, amount1)
            .map_err(|err| convert_node_err(err.into()))?;
        let lp_token_mint = next_amm_pool_state.outstanding_lp_tokens
            - amm_pool_state.outstanding_lp_tokens;
        let mut tx = Transaction::default();
        let () = self
            .app
            .wallet
            .amm_mint(&mut tx, asset0, asset1, amount0, amount1, lp_token_mint)
            .map_err(convert_wallet_err)?;
        let authorized_tx =
            self.app.wallet.authorize(tx).map_err(convert_wallet_err)?;
        self.app
            .node
            .submit_transaction(&authorized_tx)
            .await
            .map_err(convert_node_err)
    }

    async fn amm_burn(
        &self,
        asset0: AssetId,
        asset1: AssetId,
        lp_token_amount: u64,
    ) -> RpcResult<()> {
        let amm_pool_state = self.get_amm_pool_state(asset0, asset1).await?;
        let next_amm_pool_state = amm_pool_state
            .burn(lp_token_amount)
            .map_err(|err| convert_node_err(err.into()))?;
        let amount0 = amm_pool_state.reserve0 - next_amm_pool_state.reserve0;
        let amount1 = amm_pool_state.reserve1 - next_amm_pool_state.reserve1;
        let mut tx = Transaction::default();
        let () = self
            .app
            .wallet
            .amm_burn(
                &mut tx,
                asset0,
                asset1,
                amount0,
                amount1,
                lp_token_amount,
            )
            .map_err(convert_wallet_err)?;
        let authorized_tx =
            self.app.wallet.authorize(tx).map_err(convert_wallet_err)?;
        self.app
            .node
            .submit_transaction(&authorized_tx)
            .await
            .map_err(convert_node_err)
    }

    async fn amm_swap(
        &self,
        asset_spend: AssetId,
        asset_receive: AssetId,
        amount_spend: u64,
    ) -> RpcResult<u64> {
        let pair = match asset_spend.cmp(&asset_receive) {
            Ordering::Less => (asset_spend, asset_receive),
            Ordering::Equal => {
                let err = node::Error::State(state::Error::InvalidAmmSwap);
                return Err(convert_node_err(err));
            }
            Ordering::Greater => (asset_receive, asset_spend),
        };
        let amm_pool_state = self.get_amm_pool_state(pair.0, pair.1).await?;
        let amount_receive = (if asset_spend < asset_receive {
            amm_pool_state.swap_asset0_for_asset1(amount_spend).map(
                |new_amm_pool_state| {
                    new_amm_pool_state.reserve1 - amm_pool_state.reserve1
                },
            )
        } else {
            amm_pool_state.swap_asset1_for_asset0(amount_spend).map(
                |new_amm_pool_state| {
                    new_amm_pool_state.reserve0 - amm_pool_state.reserve0
                },
            )
        })
        .map_err(|err| convert_node_err(err.into()))?;
        let mut tx = Transaction::default();
        let () = self
            .app
            .wallet
            .amm_swap(
                &mut tx,
                asset_spend,
                asset_receive,
                amount_spend,
                amount_receive,
            )
            .map_err(convert_wallet_err)?;
        let authorized_tx =
            self.app.wallet.authorize(tx).map_err(convert_wallet_err)?;
        self.app
            .node
            .submit_transaction(&authorized_tx)
            .await
            .map_err(convert_node_err)?;
        Ok(amount_receive)
    }

    async fn get_block_hash(&self, height: u32) -> RpcResult<BlockHash> {
        let block_hash = self
            .app
            .node
            .get_header(height)
            .map_err(convert_node_err)?
            .ok_or_else(|| custom_err("block not found"))?
            .hash();
        Ok(block_hash)
    }

    async fn get_block(&self, block_hash: BlockHash) -> RpcResult<Block> {
        let block = self
            .app
            .node
            .get_block(block_hash)
            .expect("This error should have been handled properly.");
        Ok(block)
    }

    async fn mine(&self) -> RpcResult<()> {
        self.app.mine().await.map_err(convert_app_err)
    }

    async fn get_new_address(&self) -> RpcResult<Address> {
        self.app
            .wallet
            .get_new_address()
            .map_err(convert_wallet_err)
    }

    async fn generate_mnemonic(&self) -> RpcResult<String> {
        let mnemonic = bip39::Mnemonic::new(
            bip39::MnemonicType::Words12,
            bip39::Language::English,
        );
        Ok(mnemonic.to_string())
    }

    async fn set_seed_from_mnemonic(&self, mnemonic: String) -> RpcResult<()> {
        let mnemonic =
            bip39::Mnemonic::from_phrase(&mnemonic, bip39::Language::English)
                .map_err(|err| custom_err(err.to_string()))?;
        let seed = bip39::Seed::new(&mnemonic, "");
        let seed_bytes: [u8; 64] = seed.as_bytes().try_into().map_err(
            |err: <[u8; 64] as TryFrom<&[u8]>>::Error| {
                custom_err(err.to_string())
            },
        )?;
        self.app
            .wallet
            .set_seed(&seed_bytes)
            .map_err(convert_wallet_err)
    }

    async fn transfer(
        &self,
        dest: Address,
        value: u64,
        fee: u64,
        memo: Option<String>,
    ) -> RpcResult<()> {
        let memo = match memo {
            None => None,
            Some(memo) => {
                let hex = hex::decode(memo)
                    .map_err(|err| custom_err(err.to_string()))?;
                Some(hex)
            }
        };
        let tx = self
            .app
            .wallet
            .create_regular_transaction(dest, value, fee, memo)
            .map_err(convert_wallet_err)?;
        let authorized_tx =
            self.app.wallet.authorize(tx).map_err(convert_wallet_err)?;
        self.app
            .node
            .submit_transaction(&authorized_tx)
            .await
            .map_err(convert_node_err)
    }

    async fn reserve_bitasset(
        &self,
        plain_name: String,
    ) -> ResponsePayload<'static, ()> {
        let mut tx = Transaction::default();
        let () = match self.app.wallet.reserve_bitasset(&mut tx, &plain_name) {
            Ok(()) => (),
            Err(err) => return ResponsePayload::Error(convert_wallet_err(err)),
        };
        let authorized_tx = match self.app.wallet.authorize(tx) {
            Ok(tx) => tx,
            Err(err) => return ResponsePayload::Error(convert_wallet_err(err)),
        };
        match self.app.node.submit_transaction(&authorized_tx).await {
            Ok(()) => ResponsePayload::Result(Cow::Owned(())),
            Err(err) => ResponsePayload::Error(convert_node_err(err)),
        }
    }
}

pub async fn run_server(
    app: App,
    rpc_addr: SocketAddr,
) -> anyhow::Result<SocketAddr> {
    let server = Server::builder().build(rpc_addr).await?;

    let addr = server.local_addr()?;
    let handle = server.start(RpcServerImpl { app }.into_rpc());

    // In this example we don't care about doing shutdown so let's it run forever.
    // You may use the `ServerHandle` to shut it down or manage it yourself.
    tokio::spawn(handle.stopped());

    Ok(addr)
}
