use std::{
    collections::{HashMap, HashSet},
    ops::Range,
    sync::Arc,
};

use parking_lot::RwLock;
use tokio::sync::RwLock as TokioRwLock;

use plain_bitassets::{
    bip300301::{self, bitcoin, jsonrpsee, MainClient},
    format_deposit_address,
    miner::{self, Miner},
    node::{self, Node, THIS_SIDECHAIN},
    types::{
        self, FilledOutput, GetValue, Hash, InPoint, OutPoint, Transaction,
    },
    wallet::{self, Wallet},
};

use crate::cli::Config;

#[derive(Clone)]
pub struct App {
    pub node: Node,
    pub wallet: Wallet,
    pub miner: Arc<TokioRwLock<Miner>>,
    pub utxos: Arc<RwLock<HashMap<OutPoint, FilledOutput>>>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("drivechain error")]
    Drivechain(#[from] bip300301::Error),
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("jsonrpsee error")]
    Jsonrpsee(#[from] jsonrpsee::core::Error),
    #[error("miner error")]
    Miner(#[from] miner::Error),
    #[error("node error")]
    Node(#[from] node::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error("wallet error")]
    Wallet(#[from] wallet::Error),
}

impl App {
    pub fn new(config: &Config) -> Result<Self, Error> {
        // Node launches some tokio tasks for p2p networking, that is why we need a tokio runtime
        // here.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let wallet = Wallet::new(&config.datadir.join("wallet.mdb"))?;
        let miner = Miner::new(
            THIS_SIDECHAIN,
            config.main_addr,
            &config.main_user,
            &config.main_password,
        )?;
        let node = runtime.block_on(async {
            let node = match Node::new(
                config.net_addr,
                &config.datadir,
                config.main_addr,
                &config.main_password,
                &config.main_user,
                #[cfg(not(target_os = "windows"))]
                config.zmq_addr,
            ) {
                Ok(node) => node,
                Err(err) => return Err(err),
            };
            Ok(node)
        })?;
        let utxos = {
            let mut utxos = wallet.get_utxos()?;
            let transactions = node.get_all_transactions()?;
            for transaction in &transactions {
                for input in &transaction.transaction.inputs {
                    utxos.remove(input);
                }
            }
            Arc::new(RwLock::new(utxos))
        };
        Ok(Self {
            node,
            wallet,
            miner: Arc::new(TokioRwLock::new(miner)),
            utxos,
            runtime: Arc::new(runtime),
        })
    }

    pub fn sign_and_send(&mut self, tx: Transaction) -> Result<(), Error> {
        let authorized_transaction = self.wallet.authorize(tx)?;
        self.runtime
            .block_on(self.node.submit_transaction(&authorized_transaction))?;
        self.update_utxos()?;
        Ok(())
    }

    pub fn get_new_main_address(
        &self,
    ) -> Result<bitcoin::Address<bitcoin::address::NetworkChecked>, Error> {
        let address = self.runtime.block_on({
            let miner = self.miner.clone();
            async move {
                let miner_read = miner.read().await;
                miner_read
                    .drivechain
                    .client
                    .getnewaddress("", "legacy")
                    .await
            }
        })?;
        let address: bitcoin::Address<bitcoin::address::NetworkChecked> =
            address.require_network(bitcoin::Network::Regtest).unwrap();
        Ok(address)
    }

    /** Get all paymail.
     *  If `inbox_whitelist` is `Some`,
     * only the specified bitassets will be used as inboxes. */
    pub fn get_paymail(
        &self,
        inbox_whitelist: Option<&HashSet<Hash>>,
    ) -> Result<HashMap<OutPoint, FilledOutput>, Error> {
        let mut utxos = self.wallet.get_utxos()?;
        let mut bitasset_utxos = self.wallet.get_bitassets()?;
        let mut bitasset_stxos = self.wallet.get_spent_bitassets()?;
        if let Some(inbox_whitelist) = inbox_whitelist {
            bitasset_utxos.retain(|_, output| {
                let Some(bitasset) = output.bitasset() else {
                    return false;
                };
                inbox_whitelist.contains(bitasset)
            });
            bitasset_stxos.retain(|_, output| {
                let Some(bitasset) = output.output.bitasset() else {
                    return false;
                };
                inbox_whitelist.contains(bitasset)
            })
        };
        let outpoints_to_block_heights: HashMap<_, _> = utxos
            .iter()
            .map(|(&outpoint, _)| outpoint)
            .chain(bitasset_stxos.iter().map(|(&outpoint, _)| outpoint))
            .filter_map(|outpoint| match outpoint {
                OutPoint::Regular { txid, vout: _ } => Some((outpoint, txid)),
                _ => None,
            })
            .map(|(outpoint, txid)| {
                let height = self.node.get_tx_height(txid)?;
                Ok((outpoint, height))
            })
            .collect::<Result<_, node::Error>>()?;
        let inpoints_to_block_heights: HashMap<_, _> =
            bitasset_stxos.values()
                .map(|spent_output| {
                let txid = match spent_output.inpoint {
                    InPoint::Regular { txid, vin:_ } => txid,
                    _ => panic!(
                        "Impossible: bitasset inpoint can only refer to regular tx"
                    )
                };
                let height = self.node.get_tx_height(txid)?;
                Ok((spent_output.inpoint, height))
            }).collect::<Result<_, node::Error>>()?;
        /* associate to each address, a set of pairs of bitasset data and
        ownership period for the bitasset. */
        let mut addrs_to_bitassets_ownership: HashMap<_, HashSet<_>> =
            HashMap::new();
        // populate with owned bitassets
        for (outpoint, output) in bitasset_utxos {
            let Some(bitasset) = output.bitasset() else {
                continue;
            };
            let bitasset_data =
                self.node.get_current_bitasset_data(bitasset)?;
            let height = outpoints_to_block_heights[&outpoint];
            let owned = Range {
                start: height,
                end: u32::MAX,
            };
            addrs_to_bitassets_ownership
                .entry(output.address)
                .or_default()
                .insert((bitasset_data, owned));
        }
        // populate with spent bitassets
        for (outpoint, output) in bitasset_stxos {
            let Some(bitasset) = output.output.bitasset() else {
                continue;
            };
            let acquired_height = outpoints_to_block_heights[&outpoint];
            let spent_height = inpoints_to_block_heights[&output.inpoint];
            let bitasset_data = self
                .node
                .get_bitasset_data_at_block_height(bitasset, acquired_height)?;
            let owned = Range {
                start: acquired_height,
                end: spent_height,
            };
            addrs_to_bitassets_ownership
                .entry(output.output.address)
                .or_default()
                .insert((bitasset_data, owned));
        }
        // retain if memo exists, and output value >= paymail fee
        utxos.retain(|outpoint, output| {
            if output.memo.is_empty() {
                return false;
            }
            let Some(bitasset_data_ownership) =
                addrs_to_bitassets_ownership.get(&output.address)
            else {
                return false;
            };
            let height = outpoints_to_block_heights[outpoint];
            let min_fee = bitasset_data_ownership
                .iter()
                .filter_map(|(bitasset_data, ownership)| {
                    if !ownership.contains(&height) {
                        return None;
                    };
                    bitasset_data.paymail_fee
                })
                .min();
            let Some(min_fee) = min_fee else {
                return false;
            };
            output.get_value() >= min_fee
        });
        Ok(utxos)
    }

    const EMPTY_BLOCK_BMM_BRIBE: u64 = 1000;
    pub async fn mine(&self) -> Result<(), Error> {
        const NUM_TRANSACTIONS: usize = 1000;
        let (transactions, fee) =
            self.node.get_transactions(NUM_TRANSACTIONS)?;
        let coinbase = match fee {
            0 => vec![],
            _ => vec![types::Output::new(
                self.wallet.get_new_address()?,
                types::OutputContent::Value(fee),
            )],
        };
        let body = types::Body::new(transactions, coinbase);
        let prev_side_hash = self.node.get_best_hash()?;
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
        let bribe = if fee > 0 {
            fee
        } else {
            Self::EMPTY_BLOCK_BMM_BRIBE
        };
        let bribe = bitcoin::Amount::from_sat(bribe);
        let mut miner_write = self.miner.write().await;
        miner_write
            .attempt_bmm(bribe.to_sat(), 0, header, body)
            .await?;
        miner_write.generate().await?;
        if let Ok(Some((header, body))) = miner_write.confirm_bmm().await {
            self.node.submit_block(&header, &body).await?;
        }
        self.update_wallet()?;
        self.update_utxos()?;
        Ok(())
    }

    fn update_wallet(&self) -> Result<(), Error> {
        let addresses = self.wallet.get_addresses()?;
        let utxos = self.node.get_utxos_by_addresses(&addresses)?;
        let outpoints: Vec<_> = self.wallet.get_utxos()?.into_keys().collect();
        let spent: Vec<_> = self
            .node
            .get_spent_utxos(&outpoints)?
            .into_iter()
            .map(|(outpoint, spent_output)| (outpoint, spent_output.inpoint))
            .collect();
        self.wallet.put_utxos(&utxos)?;
        self.wallet.spend_utxos(&spent)?;
        Ok(())
    }

    fn update_utxos(&self) -> Result<(), Error> {
        let mut utxos = self.wallet.get_utxos()?;
        let transactions = self.node.get_all_transactions()?;
        for transaction in &transactions {
            for input in &transaction.transaction.inputs {
                utxos.remove(input);
            }
        }
        *self.utxos.write() = utxos;
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
