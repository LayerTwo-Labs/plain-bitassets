use std::{cmp::Ordering, net::SocketAddr};

use bip300301::bitcoin;
use fraction::Fraction;
use jsonrpsee::{
    core::{async_trait, RpcResult},
    server::Server,
    types::ErrorObject,
};

use plain_bitassets::{
    node,
    state::{self, AmmPair, AmmPoolState, BitAssetSeqId, DutchAuctionState},
    types::{
        Address, AssetId, BitAssetData, BitAssetId, Block, BlockHash,
        DutchAuctionId, DutchAuctionParams, FilledOutput, OutPoint, Output,
        Transaction, Txid,
    },
    wallet,
};
use plain_bitassets_app_rpc_api::RpcServer;

use crate::app::{self, App};

pub struct RpcServerImpl {
    app: App,
}

fn custom_err(err_msg: impl Into<String>) -> ErrorObject<'static> {
    ErrorObject::owned(-1, err_msg.into(), Option::<()>::None)
}

fn convert_app_err(err: app::Error) -> ErrorObject<'static> {
    tracing::error!("{err}");
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
    async fn amm_burn(
        &self,
        asset0: AssetId,
        asset1: AssetId,
        lp_token_amount: u64,
    ) -> RpcResult<Txid> {
        let amm_pair = AmmPair::new(asset0, asset1);
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
                amm_pair.asset0(),
                amm_pair.asset1(),
                amount0,
                amount1,
                lp_token_amount,
            )
            .map_err(convert_wallet_err)?;
        let txid = tx.txid();
        let () = self.app.sign_and_send(tx).map_err(convert_app_err)?;
        Ok(txid)
    }

    async fn amm_mint(
        &self,
        asset0: AssetId,
        asset1: AssetId,
        amount0: u64,
        amount1: u64,
    ) -> RpcResult<Txid> {
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
        let txid = tx.txid();
        let () = self.app.sign_and_send(tx).map_err(convert_app_err)?;
        Ok(txid)
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
            .submit_transaction(authorized_tx)
            .map_err(convert_node_err)?;
        Ok(amount_receive)
    }

    async fn bitassets(
        &self,
    ) -> RpcResult<Vec<(BitAssetSeqId, BitAssetId, BitAssetData)>> {
        self.app.node.bitassets().map_err(convert_node_err)
    }

    async fn bitcoin_balance(&self) -> RpcResult<u64> {
        self.app
            .wallet
            .get_bitcoin_balance()
            .map_err(convert_wallet_err)
    }

    async fn connect_peer(&self, addr: SocketAddr) -> RpcResult<()> {
        self.app.node.connect_peer(addr).map_err(convert_node_err)
    }

    async fn dutch_auction_bid(
        &self,
        auction_id: DutchAuctionId,
        bid_size: u64,
    ) -> RpcResult<u64> {
        let height = self.getblockcount().await?;
        let auction_state = self
            .app
            .node
            .get_dutch_auction_state(auction_id)
            .map_err(convert_node_err)?;
        let next_auction_state = auction_state
            .bid(Txid::default(), bid_size, height)
            .map_err(|err| convert_node_err(err.into()))?;
        let receive_quantity =
            auction_state.base_amount_remaining.latest().data
                - next_auction_state.base_amount_remaining.latest().data;
        let mut tx = Transaction::default();
        let () = self
            .app
            .wallet
            .dutch_auction_bid(
                &mut tx,
                auction_id,
                auction_state.base_asset,
                auction_state.quote_asset,
                bid_size,
                receive_quantity,
            )
            .map_err(convert_wallet_err)?;
        let authorized_tx =
            self.app.wallet.authorize(tx).map_err(convert_wallet_err)?;
        self.app
            .node
            .submit_transaction(authorized_tx)
            .map_err(convert_node_err)?;
        Ok(receive_quantity)
    }

    async fn dutch_auction_collect(
        &self,
        auction_id: DutchAuctionId,
    ) -> RpcResult<(u64, u64)> {
        let height = self.getblockcount().await?;
        let auction_state = self
            .app
            .node
            .get_dutch_auction_state(auction_id)
            .map_err(convert_node_err)?;
        if height <= auction_state.start_block + auction_state.duration {
            let err = state::DutchAuctionCollectError::AuctionNotFinished;
            let err = node::Error::State(err.into());
            return Err(convert_node_err(err));
        }
        let mut tx = Transaction::default();
        let () = self
            .app
            .wallet
            .dutch_auction_collect(
                &mut tx,
                auction_id,
                auction_state.base_asset,
                auction_state.quote_asset,
                auction_state.base_amount_remaining.latest().data,
                auction_state.quote_amount.latest().data,
            )
            .map_err(convert_wallet_err)?;
        let authorized_tx =
            self.app.wallet.authorize(tx).map_err(convert_wallet_err)?;
        self.app
            .node
            .submit_transaction(authorized_tx)
            .map_err(convert_node_err)?;
        Ok((
            auction_state.base_amount_remaining.latest().data,
            auction_state.quote_amount.latest().data,
        ))
    }

    async fn dutch_auction_create(
        &self,
        dutch_auction_params: DutchAuctionParams,
    ) -> RpcResult<Txid> {
        let mut tx = Transaction::default();
        let () = self
            .app
            .wallet
            .dutch_auction_create(&mut tx, dutch_auction_params)
            .map_err(convert_wallet_err)?;
        let txid = tx.txid();
        let () = self.app.sign_and_send(tx).map_err(convert_app_err)?;
        Ok(txid)
    }

    async fn dutch_auctions(
        &self,
    ) -> RpcResult<Vec<(DutchAuctionId, DutchAuctionState)>> {
        self.app.node.dutch_auctions().map_err(convert_node_err)
    }

    async fn format_deposit_address(
        &self,
        address: Address,
    ) -> RpcResult<String> {
        let deposit_address = plain_bitassets::format_deposit_address(
            node::THIS_SIDECHAIN,
            &address.to_string(),
        );
        Ok(deposit_address)
    }

    async fn generate_mnemonic(&self) -> RpcResult<String> {
        let mnemonic = bip39::Mnemonic::new(
            bip39::MnemonicType::Words12,
            bip39::Language::English,
        );
        Ok(mnemonic.to_string())
    }

    async fn get_amm_pool_state(
        &self,
        asset0: AssetId,
        asset1: AssetId,
    ) -> RpcResult<AmmPoolState> {
        let amm_pair = AmmPair::new(asset0, asset1);
        self.app
            .node
            .get_amm_pool_state(amm_pair)
            .map_err(convert_node_err)
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

    async fn get_block(&self, block_hash: BlockHash) -> RpcResult<Block> {
        let block = self
            .app
            .node
            .get_block(block_hash)
            .expect("This error should have been handled properly.");
        Ok(block)
    }

    async fn get_new_address(&self) -> RpcResult<Address> {
        self.app
            .wallet
            .get_new_address()
            .map_err(convert_wallet_err)
    }

    async fn getblockcount(&self) -> RpcResult<u32> {
        self.app.node.get_tip_height().map_err(convert_node_err)
    }

    async fn mine(&self, fee: Option<u64>) -> RpcResult<()> {
        let fee = fee.map(bip300301::bitcoin::Amount::from_sat);
        self.app.local_pool.spawn_pinned({
            let app = self.app.clone();
            move || async move { app.mine(fee).await.map_err(convert_app_err) }
        }).await.unwrap()
    }

    async fn my_unconfirmed_utxos(&self) -> RpcResult<Vec<(OutPoint, Output)>> {
        let addresses = self
            .app
            .wallet
            .get_addresses()
            .map_err(convert_wallet_err)?;
        let utxos = self
            .app
            .node
            .get_unconfirmed_utxos_by_addresses(&addresses)
            .map_err(convert_node_err)?;
        Ok(Vec::from_iter(utxos))
    }

    async fn my_utxos(&self) -> RpcResult<Vec<FilledOutput>> {
        let utxos = self
            .app
            .wallet
            .get_utxos()
            .map_err(convert_wallet_err)?
            .into_values()
            .collect();
        Ok(utxos)
    }

    async fn reserve_bitasset(&self, plain_name: String) -> RpcResult<Txid> {
        let mut tx = Transaction::default();
        let () = match self.app.wallet.reserve_bitasset(&mut tx, &plain_name) {
            Ok(()) => (),
            Err(err) => return Err(convert_wallet_err(err)),
        };
        let txid = tx.txid();
        let () = self.app.sign_and_send(tx).map_err(convert_app_err)?;
        Ok(txid)
    }

    async fn set_seed_from_mnemonic(&self, mnemonic: String) -> RpcResult<()> {
        self.app
            .wallet
            .set_seed_from_mnemonic(mnemonic.as_str())
            .map_err(convert_wallet_err)
    }

    async fn sidechain_wealth(&self) -> RpcResult<bitcoin::Amount> {
        self.app
            .node
            .get_sidechain_wealth()
            .map_err(convert_node_err)
    }

    async fn stop(&self) {
        std::process::exit(0);
    }

    async fn transfer(
        &self,
        dest: Address,
        value: u64,
        fee: u64,
        memo: Option<String>,
    ) -> RpcResult<Txid> {
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
            .create_transfer(dest, value, fee, memo)
            .map_err(convert_wallet_err)?;
        let txid = tx.txid();
        let () = self.app.sign_and_send(tx).map_err(convert_app_err)?;
        Ok(txid)
    }

    async fn withdraw(
        &self,
        mainchain_address: bitcoin::Address<bitcoin::address::NetworkUnchecked>,
        amount_sats: u64,
        fee_sats: u64,
        mainchain_fee_sats: u64,
    ) -> RpcResult<Txid> {
        let tx = self
            .app
            .wallet
            .create_withdrawal(
                mainchain_address,
                amount_sats,
                mainchain_fee_sats,
                fee_sats,
            )
            .map_err(convert_wallet_err)?;
        let txid = tx.txid();
        self.app.sign_and_send(tx).map_err(convert_app_err)?;
        Ok(txid)
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
