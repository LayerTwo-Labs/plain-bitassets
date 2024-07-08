use std::{collections::HashMap, sync::Arc};

use futures::{StreamExt, TryFutureExt};
use parking_lot::RwLock;
use plain_bitassets::{
    bip300301::{bitcoin, MainClient},
    format_deposit_address,
    miner::{self, Miner},
    node::{self, Node, THIS_SIDECHAIN},
    types::{
        self, BitcoinOutputContent, Body, FilledOutput, OutPoint, Output,
        Transaction,
    },
    wallet::{self, Wallet},
};
use tokio::{spawn, sync::RwLock as TokioRwLock, task::JoinHandle};
use tokio_util::task::LocalPoolHandle;

use crate::cli::Config;

fn update_wallet(node: &Node, wallet: &Wallet) -> Result<(), Error> {
    let addresses = wallet.get_addresses()?;
    let unconfirmed_utxos =
        node.get_unconfirmed_utxos_by_addresses(&addresses)?;
    let utxos = node.get_utxos_by_addresses(&addresses)?;
    let confirmed_outpoints: Vec<_> = wallet.get_utxos()?.into_keys().collect();
    let confirmed_spent = node
        .get_spent_utxos(&confirmed_outpoints)?
        .into_iter()
        .map(|(outpoint, spent_output)| (outpoint, spent_output.inpoint));
    let unconfirmed_outpoints: Vec<_> =
        wallet.get_unconfirmed_utxos()?.into_keys().collect();
    let unconfirmed_spent = node
        .get_unconfirmed_spent_utxos(
            confirmed_outpoints.iter().chain(&unconfirmed_outpoints),
        )?
        .into_iter();
    let spent: Vec<_> = confirmed_spent.chain(unconfirmed_spent).collect();
    wallet.put_utxos(&utxos)?;
    wallet.put_unconfirmed_utxos(&unconfirmed_utxos)?;
    wallet.spend_utxos(&spent)?;
    Ok(())
}

/// Update (unconfirmed) utxos & wallet
fn update(
    node: &Node,
    utxos: &mut HashMap<OutPoint, FilledOutput>,
    unconfirmed_utxos: &mut HashMap<OutPoint, Output>,
    wallet: &Wallet,
) -> Result<(), Error> {
    let () = update_wallet(node, wallet)?;
    *utxos = wallet.get_utxos()?;
    *unconfirmed_utxos = wallet.get_unconfirmed_utxos()?;
    Ok(())
}

#[derive(Clone)]
pub struct App {
    pub node: Arc<Node>,
    pub wallet: Wallet,
    pub miner: Arc<TokioRwLock<Miner>>,
    pub utxos: Arc<RwLock<HashMap<OutPoint, FilledOutput>>>,
    pub unconfirmed_utxos: Arc<RwLock<HashMap<OutPoint, Output>>>,
    pub runtime: Arc<tokio::runtime::Runtime>,
    task: Arc<JoinHandle<()>>,
    pub local_pool: LocalPoolHandle,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("drivechain error")]
    Drivechain(#[from] bip300301::Error),
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("jsonrpsee error")]
    Jsonrpsee(#[from] jsonrpsee::core::Error),
    #[error("miner error: {0}")]
    Miner(#[from] miner::Error),
    #[error("node error")]
    Node(#[from] node::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error("wallet error")]
    Wallet(#[from] wallet::Error),
}

impl App {
    async fn task(
        node: Arc<Node>,
        utxos: Arc<RwLock<HashMap<OutPoint, FilledOutput>>>,
        unconfirmed_utxos: Arc<RwLock<HashMap<OutPoint, Output>>>,
        wallet: Wallet,
    ) -> Result<(), Error> {
        let mut state_changes = node.watch_state();
        while let Some(()) = state_changes.next().await {
            let () = update(
                &node,
                &mut utxos.write(),
                &mut unconfirmed_utxos.write(),
                &wallet,
            )?;
        }
        Ok(())
    }

    fn spawn_task(
        node: Arc<Node>,
        utxos: Arc<RwLock<HashMap<OutPoint, FilledOutput>>>,
        unconfirmed_utxos: Arc<RwLock<HashMap<OutPoint, Output>>>,
        wallet: Wallet,
    ) -> JoinHandle<()> {
        spawn(
            Self::task(node, utxos, unconfirmed_utxos, wallet).unwrap_or_else(
                |err| {
                    let err = anyhow::Error::from(err);
                    tracing::error!("{err:#}")
                },
            ),
        )
    }

    pub fn new(config: &Config) -> Result<Self, Error> {
        // Node launches some tokio tasks for p2p networking, that is why we need a tokio runtime
        // here.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let wallet = Wallet::new(&config.datadir.join("wallet.mdb"))?;
        if let Some(seed_phrase_path) = &config.mnemonic_seed_phrase_path {
            let mnemonic = std::fs::read_to_string(seed_phrase_path)?;
            let () = wallet.set_seed_from_mnemonic(mnemonic.as_str())?;
        }
        let miner = Miner::new(
            THIS_SIDECHAIN,
            config.main_addr,
            &config.main_user,
            &config.main_password,
        )?;
        let rt_guard = runtime.enter();
        let local_pool = LocalPoolHandle::new(1);
        let node = Node::new(
            config.net_addr,
            &config.datadir,
            config.main_addr,
            config.network,
            &config.main_password,
            &config.main_user,
            local_pool.clone(),
            #[cfg(all(not(target_os = "windows"), feature = "zmq"))]
            config.zmq_addr,
        )?;
        let (unconfirmed_utxos, utxos) = {
            let mut utxos = wallet.get_utxos()?;
            let mut unconfirmed_utxos = wallet.get_unconfirmed_utxos()?;
            let transactions = node.get_all_transactions()?;
            for transaction in &transactions {
                for input in &transaction.transaction.inputs {
                    utxos.remove(input);
                    unconfirmed_utxos.remove(input);
                }
            }
            let unconfirmed_utxos = Arc::new(RwLock::new(unconfirmed_utxos));
            let utxos = Arc::new(RwLock::new(utxos));
            (unconfirmed_utxos, utxos)
        };
        let node = Arc::new(node);
        let task = Self::spawn_task(
            node.clone(),
            utxos.clone(),
            unconfirmed_utxos.clone(),
            wallet.clone(),
        );
        drop(rt_guard);
        Ok(Self {
            node,
            wallet,
            miner: Arc::new(TokioRwLock::new(miner)),
            unconfirmed_utxos,
            utxos,
            runtime: Arc::new(runtime),
            task: Arc::new(task),
            local_pool,
        })
    }

    /// Update utxos & wallet
    fn update(&self) -> Result<(), Error> {
        update(
            self.node.as_ref(),
            &mut self.utxos.write(),
            &mut self.unconfirmed_utxos.write(),
            &self.wallet,
        )
    }

    pub fn sign_and_send(&self, tx: Transaction) -> Result<(), Error> {
        let authorized_transaction = self.wallet.authorize(tx)?;
        self.node.submit_transaction(authorized_transaction)?;
        let () = self.update()?;
        Ok(())
    }

    pub fn get_new_main_address(
        &self,
    ) -> Result<bitcoin::Address<bitcoin::address::NetworkChecked>, Error> {
        let address = self.runtime.block_on({
            let miner = self.miner.clone();
            async move {
                let miner_read = miner.read().await;
                let drivechain_client = &miner_read.drivechain.client;
                let mainchain_info =
                    drivechain_client.get_blockchain_info().await?;
                let res = drivechain_client
                    .getnewaddress("", "legacy")
                    .await?
                    .require_network(mainchain_info.chain)
                    .unwrap();
                Result::<_, Error>::Ok(res)
            }
        })?;
        Ok(address)
    }

    const EMPTY_BLOCK_BMM_BRIBE: bip300301::bitcoin::Amount =
        bip300301::bitcoin::Amount::from_sat(1000);

    pub async fn mine(
        &self,
        fee: Option<bip300301::bitcoin::Amount>,
    ) -> Result<(), Error> {
        const NUM_TRANSACTIONS: usize = 1000;
        let (txs, tx_fees) = self.node.get_transactions(NUM_TRANSACTIONS)?;
        let coinbase = match tx_fees {
            0 => vec![],
            _ => vec![types::Output::new(
                self.wallet.get_new_address()?,
                types::OutputContent::Value(BitcoinOutputContent(tx_fees)),
            )],
        };
        let body = {
            let txs = txs.into_iter().map(|tx| tx.into()).collect();
            Body::new(txs, coinbase)
        };
        let prev_side_hash = self.node.get_tip()?;
        let prev_main_hash = self
            .miner
            .read()
            .await
            .drivechain
            .get_mainchain_tip()
            .await?;
        let header = types::Header {
            merkle_root: body.compute_merkle_root(),
            prev_side_hash,
            prev_main_hash,
        };
        let bribe = fee.unwrap_or_else(|| {
            if tx_fees > 0 {
                bip300301::bitcoin::Amount::from_sat(tx_fees)
            } else {
                Self::EMPTY_BLOCK_BMM_BRIBE
            }
        });
        let mut miner_write = self.miner.write().await;
        miner_write
            .attempt_bmm(bribe.to_sat(), 0, header, body)
            .await?;
        tracing::trace!("confirming bmm...");
        if let Some((main_hash, header, body)) =
            miner_write.confirm_bmm().await?
        {
            tracing::trace!(
                "confirmed bmm, submitting block {}",
                header.hash()
            );
            self.node.submit_block(main_hash, &header, &body).await?;
        }
        let () = self.update()?;
        Ok(())
    }

    pub fn deposit(
        &mut self,
        amount: bitcoin::Amount,
        fee: bitcoin::Amount,
    ) -> Result<(), Error> {
        self.runtime.block_on(async {
            let address = self.wallet.get_new_address()?;
            let address =
                format_deposit_address(THIS_SIDECHAIN, &format!("{address}"));
            self.miner
                .read()
                .await
                .drivechain
                .client
                .createsidechaindeposit(
                    THIS_SIDECHAIN,
                    &address,
                    amount.into(),
                    fee.into(),
                )
                .await?;
            Ok(())
        })
    }
}

impl Drop for App {
    fn drop(&mut self) {
        self.task.abort()
    }
}
