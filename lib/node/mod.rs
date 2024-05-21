#[cfg(all(not(target_os = "windows"), feature = "zmq"))]
use std::sync::Arc;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt::Debug,
    net::SocketAddr,
    path::Path,
};

use bip300301::{bitcoin, DepositInfo};
use fallible_iterator::FallibleIterator;
use fraction::Fraction;
use tokio_util::task::LocalPoolHandle;

use crate::{
    archive::{self, Archive},
    mempool::{self, MemPool},
    net::{self, Net},
    state::{
        self, AmmPair, AmmPoolState, BitAssetSeqId, DutchAuctionBidError,
        DutchAuctionState, State,
    },
    types::{
        Address, AssetId, Authorized, AuthorizedTransaction, BitAssetData,
        BitAssetId, Block, BlockHash, Body, DutchAuctionId, FilledOutput,
        FilledTransaction, GetBitcoinValue, Header, InPoint, OutPoint, Output,
        SpentOutput, Transaction, TxIn, Txid, WithdrawalBundle,
    },
};

mod mainchain_task;
mod net_task;

use mainchain_task::MainchainTaskHandle;
use net_task::NetTaskHandle;
#[cfg(all(not(target_os = "windows"), feature = "zmq"))]
use net_task::ZmqPubHandler;

pub const THIS_SIDECHAIN: u8 = 4;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("address parse error")]
    AddrParse(#[from] std::net::AddrParseError),
    #[error("archive error")]
    Archive(#[from] archive::Error),
    #[error("bincode error")]
    Bincode(#[from] bincode::Error),
    #[error("drivechain error")]
    Drivechain(#[from] bip300301::Error),
    #[error("heed error")]
    Heed(#[from] heed::Error),
    #[error("quinn error")]
    Io(#[from] std::io::Error),
    #[error("mempool error")]
    MemPool(#[from] mempool::Error),
    #[error("net error")]
    Net(#[from] net::Error),
    #[error("net task error")]
    NetTask(#[from] net_task::Error),
    #[error("peer info stream closed")]
    PeerInfoRxClosed,
    #[error("Receive mainchain task response cancelled")]
    ReceiveMainchainTaskResponse,
    #[error("Send mainchain task request failed")]
    SendMainchainTaskRequest,
    #[error("state error")]
    State(#[from] state::Error),
    #[error("Utreexo error: {0}")]
    Utreexo(String),
}

/// Request any missing two way peg data up to the specified block hash.
/// All ancestor headers must exist in the archive.
// TODO: deposits only for now
#[allow(dead_code)]
async fn request_two_way_peg_data(
    env: &heed::Env,
    archive: &Archive,
    drivechain: &bip300301::Drivechain,
    block_hash: bitcoin::BlockHash,
) -> Result<(), Error> {
    // last block for which deposit info is known
    let last_known_deposit_info = {
        let rotxn = env.read_txn()?;
        #[allow(clippy::let_and_return)]
        let last_known_deposit_info = archive
            .main_ancestors(&rotxn, block_hash)
            .find(|block_hash| {
                let deposits = archive.try_get_deposits(&rotxn, *block_hash)?;
                Ok(deposits.is_some())
            })?;
        last_known_deposit_info
    };
    if last_known_deposit_info == Some(block_hash) {
        return Ok(());
    }
    let two_way_peg_data = drivechain
        .get_two_way_peg_data(block_hash, last_known_deposit_info)
        .await?;
    let mut rwtxn = env.write_txn()?;
    // Deposits by block, first-to-last within each block
    let deposits_by_block: HashMap<bitcoin::BlockHash, Vec<DepositInfo>> = {
        let mut deposits = HashMap::<_, Vec<_>>::new();
        two_way_peg_data.deposits.into_iter().for_each(|deposit| {
            deposits
                .entry(deposit.block_hash)
                .or_default()
                .push(deposit)
        });
        let () = archive
            .main_ancestors(&rwtxn, block_hash)
            .take_while(|block_hash| {
                Ok(last_known_deposit_info != Some(*block_hash))
            })
            .for_each(|block_hash| {
                let _ = deposits.entry(block_hash).or_default();
                Ok(())
            })?;
        deposits
    };
    deposits_by_block
        .into_iter()
        .try_for_each(|(block_hash, deposits)| {
            archive.put_deposits(&mut rwtxn, block_hash, deposits)
        })?;
    rwtxn.commit()?;
    Ok(())
}

pub type FilledTransactionWithPosition =
    (Authorized<FilledTransaction>, Option<TxIn>);

#[derive(Clone)]
pub struct Node {
    archive: Archive,
    drivechain: bip300301::Drivechain,
    env: heed::Env,
    _local_pool: LocalPoolHandle,
    mainchain_task: MainchainTaskHandle,
    mempool: MemPool,
    net: Net,
    net_task: NetTaskHandle,
    state: State,
    #[cfg(all(not(target_os = "windows"), feature = "zmq"))]
    zmq_pub_handler: Arc<ZmqPubHandler>,
}

impl Node {
    pub fn new(
        bind_addr: SocketAddr,
        datadir: &Path,
        main_addr: SocketAddr,
        password: &str,
        user: &str,
        local_pool: LocalPoolHandle,
        #[cfg(all(not(target_os = "windows"), feature = "zmq"))]
        zmq_addr: SocketAddr,
    ) -> Result<Self, Error> {
        let env_path = datadir.join("data.mdb");
        // let _ = std::fs::remove_dir_all(&env_path);
        std::fs::create_dir_all(&env_path)?;
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(1024 * 1024 * 1024) // 1GB
                .max_dbs(
                    State::NUM_DBS
                        + Archive::NUM_DBS
                        + MemPool::NUM_DBS
                        + Net::NUM_DBS,
                )
                .open(env_path)?
        };
        let state = State::new(&env)?;
        #[cfg(all(not(target_os = "windows"), feature = "zmq"))]
        let zmq_pub_handler = Arc::new(ZmqPubHandler::new(zmq_addr));
        let archive = Archive::new(&env)?;
        let mempool = MemPool::new(&env)?;
        let drivechain = bip300301::Drivechain::new(
            THIS_SIDECHAIN,
            main_addr,
            user,
            password,
        )?;
        let (mainchain_task, mainchain_task_response_rx) =
            MainchainTaskHandle::new(
                env.clone(),
                archive.clone(),
                drivechain.clone(),
            );
        let (net, peer_info_rx) =
            Net::new(&env, archive.clone(), state.clone(), bind_addr)?;

        let net_task = NetTaskHandle::new(
            local_pool.clone(),
            env.clone(),
            archive.clone(),
            drivechain.clone(),
            mainchain_task.clone(),
            mainchain_task_response_rx,
            mempool.clone(),
            net.clone(),
            peer_info_rx,
            state.clone(),
            #[cfg(all(not(target_os = "windows"), feature = "zmq"))]
            zmq_pub_handler.clone(),
        );
        Ok(Self {
            archive,
            drivechain,
            env,
            _local_pool: local_pool,
            mainchain_task,
            mempool,
            net,
            net_task,
            state,
            #[cfg(all(not(target_os = "windows"), feature = "zmq"))]
            zmq_pub_handler: zmq_pub_handler.clone(),
        })
    }

    pub fn drivechain(&self) -> &bip300301::Drivechain {
        &self.drivechain
    }

    pub async fn get_best_parentchain_hash(
        &self,
    ) -> Result<bitcoin::BlockHash, Error> {
        use bip300301::MainClient;
        let res = self
            .drivechain
            .client
            .getbestblockhash()
            .await
            .map_err(bip300301::Error::Jsonrpsee)?;
        Ok(res)
    }

    pub fn get_tip_height(&self) -> Result<u32, Error> {
        let rotxn = self.env.read_txn()?;
        Ok(self.state.get_height(&rotxn)?)
    }

    pub fn get_tip(&self) -> Result<BlockHash, Error> {
        let rotxn = self.env.read_txn()?;
        Ok(self.state.get_tip(&rotxn)?)
    }

    pub fn try_get_amm_price(
        &self,
        base: AssetId,
        quote: AssetId,
    ) -> Result<Option<Fraction>, Error> {
        let txn = self.env.read_txn()?;
        let amm_pair = AmmPair::new(base, quote);
        let Some(AmmPoolState {
            reserve0,
            reserve1,
            outstanding_lp_tokens: _,
            ..
        }) = self.state.amm_pools.get(&txn, &amm_pair)?
        else {
            return Ok(None);
        };
        if reserve0 == 0 || reserve1 == 0 {
            return Ok(None);
        }
        if base < quote {
            Ok(Some(Fraction::new(reserve1, reserve0)))
        } else {
            Ok(Some(Fraction::new(reserve0, reserve1)))
        }
    }

    pub fn try_get_amm_pool_state(
        &self,
        pair: AmmPair,
    ) -> Result<Option<AmmPoolState>, Error> {
        let txn = self.env.read_txn()?;
        let res = self.state.amm_pools.get(&txn, &pair)?;
        Ok(res)
    }

    pub fn get_amm_pool_state(
        &self,
        pair: AmmPair,
    ) -> Result<AmmPoolState, Error> {
        let res = self.try_get_amm_pool_state(pair)?.ok_or_else(|| {
            state::Error::MissingAmmPoolState {
                asset0: pair.asset0(),
                asset1: pair.asset1(),
            }
        })?;
        Ok(res)
    }

    /// List all BitAssets and their current data
    pub fn bitassets(
        &self,
    ) -> Result<Vec<(BitAssetSeqId, BitAssetId, BitAssetData)>, Error> {
        let txn = self.env.read_txn()?;
        let bitasset_ids_to_data: HashMap<_, _> = self
            .state
            .bitassets
            .iter(&txn)?
            .map(|res| {
                res.map(|(bitasset_id, bitasset_data)| {
                    (bitasset_id, bitasset_data.current())
                })
            })
            .collect::<Result<_, _>>()?;
        let res = self
            .state
            .bitasset_seq_to_bitasset
            .iter(&txn)?
            .map(|res| {
                res.map_err(Error::Heed).and_then(
                    |(bitasset_seq_id, bitasset_id)| {
                        let bitasset_data =
                            bitasset_ids_to_data.get(&bitasset_id).ok_or(
                                Error::State(state::Error::MissingBitAsset {
                                    bitasset: bitasset_id,
                                }),
                            )?;
                        Ok((
                            bitasset_seq_id,
                            bitasset_id,
                            bitasset_data.clone(),
                        ))
                    },
                )
            })
            .collect::<Result<_, _>>()?;
        Ok(res)
    }

    /// List all dutch auctions and their current state
    pub fn dutch_auctions(
        &self,
    ) -> Result<Vec<(DutchAuctionId, DutchAuctionState)>, Error> {
        let txn = self.env.read_txn()?;
        let res = self
            .state
            .dutch_auctions
            .iter(&txn)?
            .collect::<Result<_, _>>()?;
        Ok(res)
    }

    pub fn try_get_dutch_auction_state(
        &self,
        auction_id: DutchAuctionId,
    ) -> Result<Option<DutchAuctionState>, Error> {
        let txn = self.env.read_txn()?;
        let res = self.state.dutch_auctions.get(&txn, &auction_id)?;
        Ok(res)
    }

    pub fn get_dutch_auction_state(
        &self,
        auction_id: DutchAuctionId,
    ) -> Result<DutchAuctionState, Error> {
        self.try_get_dutch_auction_state(auction_id).and_then(
            |dutch_auction_state| {
                dutch_auction_state.ok_or_else(|| {
                    Error::State(DutchAuctionBidError::MissingAuction.into())
                })
            },
        )
    }

    pub fn try_get_height(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<u32>, Error> {
        let rotxn = self.env.read_txn()?;
        Ok(self.archive.try_get_height(&rotxn, block_hash)?)
    }

    pub fn get_height(&self, block_hash: BlockHash) -> Result<u32, Error> {
        let rotxn = self.env.read_txn()?;
        Ok(self.archive.get_height(&rotxn, block_hash)?)
    }

    /// Get blocks in which a tx was included, and tx index within those blocks
    pub fn get_tx_inclusions(
        &self,
        txid: Txid,
    ) -> Result<BTreeMap<BlockHash, u32>, Error> {
        let rotxn = self.env.read_txn()?;
        Ok(self.archive.get_tx_inclusions(&rotxn, txid)?)
    }

    /// Returns true if the second specified block is a descendant of the first
    /// specified block
    /// Returns an error if either of the specified block headers do not exist
    /// in the archive.
    pub fn is_descendant(
        &self,
        ancestor: BlockHash,
        descendant: BlockHash,
    ) -> Result<bool, Error> {
        let rotxn = self.env.read_txn()?;
        Ok(self.archive.is_descendant(&rotxn, ancestor, descendant)?)
    }

    /** Resolve bitasset data at the specified block height.
     * Returns an error if it does not exist.rror if it does not exist. */
    pub fn get_bitasset_data_at_block_height(
        &self,
        bitasset: &BitAssetId,
        height: u32,
    ) -> Result<BitAssetData, Error> {
        let txn = self.env.read_txn()?;
        Ok(self
            .state
            .get_bitasset_data_at_block_height(&txn, bitasset, height)?)
    }

    /// resolve current bitasset data, if it exists
    pub fn try_get_current_bitasset_data(
        &self,
        bitasset: &BitAssetId,
    ) -> Result<Option<BitAssetData>, Error> {
        let txn = self.env.read_txn()?;
        Ok(self.state.try_get_current_bitasset_data(&txn, bitasset)?)
    }

    /// Resolve current bitasset data. Returns an error if it does not exist.
    pub fn get_current_bitasset_data(
        &self,
        bitasset: &BitAssetId,
    ) -> Result<BitAssetData, Error> {
        let txn = self.env.read_txn()?;
        Ok(self.state.get_current_bitasset_data(&txn, bitasset)?)
    }

    pub fn submit_transaction(
        &self,
        transaction: AuthorizedTransaction,
    ) -> Result<(), Error> {
        {
            let mut txn = self.env.write_txn()?;
            self.state.validate_transaction(&txn, &transaction)?;
            self.mempool.put(&mut txn, &transaction)?;
            txn.commit()?;
        }
        self.net.push_tx(Default::default(), transaction);
        Ok(())
    }

    pub fn get_spent_utxos(
        &self,
        outpoints: &[OutPoint],
    ) -> Result<Vec<(OutPoint, SpentOutput)>, Error> {
        let txn = self.env.read_txn()?;
        let mut spent = vec![];
        for outpoint in outpoints {
            if let Some(output) = self.state.stxos.get(&txn, outpoint)? {
                spent.push((*outpoint, output));
            }
        }
        Ok(spent)
    }

    pub fn get_unconfirmed_spent_utxos<'a, OutPoints>(
        &self,
        outpoints: OutPoints,
    ) -> Result<Vec<(OutPoint, InPoint)>, Error>
    where
        OutPoints: IntoIterator<Item = &'a OutPoint>,
    {
        let txn = self.env.read_txn()?;
        let mut spent = vec![];
        for outpoint in outpoints {
            if let Some(inpoint) =
                self.mempool.spent_utxos.get(&txn, outpoint)?
            {
                spent.push((*outpoint, inpoint));
            }
        }
        Ok(spent)
    }

    pub fn get_unconfirmed_utxos_by_addresses(
        &self,
        addresses: &HashSet<Address>,
    ) -> Result<HashMap<OutPoint, Output>, Error> {
        let txn = self.env.read_txn()?;
        let mut res = HashMap::new();
        let () = addresses.iter().try_for_each(|addr| {
            let utxos = self.mempool.get_unconfirmed_utxos(&txn, addr)?;
            res.extend(utxos);
            Result::<(), Error>::Ok(())
        })?;
        Ok(res)
    }

    pub fn get_utxos_by_addresses(
        &self,
        addresses: &HashSet<Address>,
    ) -> Result<HashMap<OutPoint, FilledOutput>, Error> {
        let txn = self.env.read_txn()?;
        let utxos = self.state.get_utxos_by_addresses(&txn, addresses)?;
        Ok(utxos)
    }

    pub fn try_get_header(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<Header>, Error> {
        let txn = self.env.read_txn()?;
        Ok(self.archive.try_get_header(&txn, block_hash)?)
    }

    pub fn get_header(&self, block_hash: BlockHash) -> Result<Header, Error> {
        let txn = self.env.read_txn()?;
        Ok(self.archive.get_header(&txn, block_hash)?)
    }

    /// Get the block hash at the specified height in the current chain,
    /// if it exists
    pub fn try_get_block_hash(
        &self,
        height: u32,
    ) -> Result<Option<BlockHash>, Error> {
        let rotxn = self.env.read_txn()?;
        let tip = self.state.get_tip(&rotxn)?;
        let tip_height = self.state.get_height(&rotxn)?;
        if tip_height >= height {
            self.archive
                .ancestors(&rotxn, tip)
                .nth((tip_height - height) as usize)
                .map_err(Error::from)
        } else {
            Ok(None)
        }
    }

    pub fn try_get_body(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<Body>, Error> {
        let txn = self.env.read_txn()?;
        Ok(self.archive.try_get_body(&txn, block_hash)?)
    }

    pub fn get_body(&self, block_hash: BlockHash) -> Result<Body, Error> {
        let txn = self.env.read_txn()?;
        Ok(self.archive.get_body(&txn, block_hash)?)
    }

    pub fn get_block(&self, block_hash: BlockHash) -> Result<Block, Error> {
        let rotxn = self.env.read_txn()?;
        Ok(self.archive.get_block(&rotxn, block_hash)?)
    }

    pub fn get_all_transactions(
        &self,
    ) -> Result<Vec<AuthorizedTransaction>, Error> {
        let txn = self.env.read_txn()?;
        let transactions = self.mempool.take_all(&txn)?;
        Ok(transactions)
    }

    /// Get total sidechain wealth in Bitcoin
    pub fn get_sidechain_wealth(&self) -> Result<bitcoin::Amount, Error> {
        let txn = self.env.read_txn()?;
        Ok(self.state.sidechain_wealth(&txn)?)
    }

    pub fn get_transactions(
        &self,
        number: usize,
    ) -> Result<(Vec<Authorized<FilledTransaction>>, u64), Error> {
        let mut rwtxn = self.env.write_txn()?;
        let transactions = self.mempool.take(&rwtxn, number)?;
        let mut fee: u64 = 0;
        let mut returned_transactions = vec![];
        let mut spent_utxos = HashSet::new();
        for transaction in transactions {
            let inputs: HashSet<_> =
                transaction.transaction.inputs.iter().copied().collect();
            if !spent_utxos.is_disjoint(&inputs) {
                println!("UTXO double spent");
                self.mempool
                    .delete(&mut rwtxn, transaction.transaction.txid())?;
                continue;
            }
            if self
                .state
                .validate_transaction(&rwtxn, &transaction)
                .is_err()
            {
                self.mempool
                    .delete(&mut rwtxn, transaction.transaction.txid())?;
                continue;
            }
            let filled_transaction = self
                .state
                .fill_authorized_transaction(&rwtxn, transaction)?;
            let value_in: u64 = filled_transaction
                .transaction
                .spent_utxos
                .iter()
                .map(GetBitcoinValue::get_bitcoin_value)
                .sum();
            let value_out: u64 = filled_transaction
                .transaction
                .transaction
                .outputs
                .iter()
                .map(GetBitcoinValue::get_bitcoin_value)
                .sum();
            fee += value_in - value_out;
            spent_utxos.extend(filled_transaction.transaction.inputs());
            returned_transactions.push(filled_transaction);
        }
        rwtxn.commit()?;
        Ok((returned_transactions, fee))
    }

    /// get a transaction from the archive or mempool, if it exists
    pub fn try_get_transaction(
        &self,
        txid: Txid,
    ) -> Result<Option<Transaction>, Error> {
        let rotxn = self.env.read_txn()?;
        if let Some((block_hash, txin)) = self
            .archive
            .get_tx_inclusions(&rotxn, txid)?
            .first_key_value()
        {
            let body = self.archive.get_body(&rotxn, *block_hash)?;
            let tx = body.transactions.into_iter().nth(*txin as usize).unwrap();
            Ok(Some(tx))
        } else if let Some(auth_tx) =
            self.mempool.transactions.get(&rotxn, &txid)?
        {
            Ok(Some(auth_tx.transaction))
        } else {
            Ok(None)
        }
    }

    /// get a filled transaction from the archive/state or mempool,
    /// and the tx index, if the transaction exists
    /// and can be filled with the current state.
    /// a tx index of `None` indicates that the tx is in the mempool.
    pub fn try_get_filled_transaction(
        &self,
        txid: Txid,
    ) -> Result<Option<FilledTransactionWithPosition>, Error> {
        let rotxn = self.env.read_txn()?;
        let tip = self.state.get_tip(&rotxn)?;
        let inclusions = self.archive.get_tx_inclusions(&rotxn, txid)?;
        if let Some((block_hash, idx)) =
            inclusions.into_iter().try_find(|(block_hash, _)| {
                self.archive.is_descendant(&rotxn, *block_hash, tip)
            })?
        {
            let body = self.archive.get_body(&rotxn, block_hash)?;
            let auth_txs = body.authorized_transactions();
            let auth_tx = auth_txs.into_iter().nth(idx as usize).unwrap();
            let filled_tx = self
                .state
                .fill_transaction_from_stxos(&rotxn, auth_tx.transaction)?;
            let auth_tx = Authorized {
                transaction: filled_tx,
                authorizations: auth_tx.authorizations,
            };
            let txin = TxIn { block_hash, idx };
            let res = (auth_tx, Some(txin));
            return Ok(Some(res));
        }
        if let Some(auth_tx) = self.mempool.transactions.get(&rotxn, &txid)? {
            match self.state.fill_authorized_transaction(&rotxn, auth_tx) {
                Ok(filled_tx) => {
                    let res = (filled_tx, None);
                    Ok(Some(res))
                }
                Err(state::Error::NoUtxo { .. }) => Ok(None),
                Err(err) => Err(err.into()),
            }
        } else {
            Ok(None)
        }
    }

    pub fn get_pending_withdrawal_bundle(
        &self,
    ) -> Result<Option<WithdrawalBundle>, Error> {
        let txn = self.env.read_txn()?;
        let bundle = self
            .state
            .get_pending_withdrawal_bundle(&txn)?
            .map(|(bundle, _)| bundle);
        Ok(bundle)
    }

    pub fn remove_from_mempool(&self, txid: Txid) -> Result<(), Error> {
        let mut rwtxn = self.env.write_txn()?;
        let () = self.mempool.delete(&mut rwtxn, txid)?;
        rwtxn.commit()?;
        Ok(())
    }

    pub fn connect_peer(&self, addr: SocketAddr) -> Result<(), Error> {
        self.net
            .connect_peer(self.env.clone(), addr)
            .map_err(Error::from)
    }

    /// Attempt to submit a block.
    /// Returns `Ok(true)` if the block was accepted successfully as the new tip.
    /// Returns `Ok(false)` if the block could not be submitted for some reason,
    /// or was rejected as the new tip.
    pub async fn submit_block(
        &self,
        header: &Header,
        body: &Body,
    ) -> Result<bool, Error> {
        let block_hash = header.hash();
        // Store the header, if ancestors exist
        match self.try_get_header(header.prev_side_hash)? {
            None => {
                tracing::error!(
                    "Rejecting block {block_hash} due to missing ancestor headers",
                );
                return Ok(false);
            }
            Some(_) => {
                let mut rwtxn = self.env.write_txn()?;
                let () = self.archive.put_header(&mut rwtxn, header)?;
                rwtxn.commit()?;
            }
        }
        // Request mainchain headers if they do not exist
        let _: mainchain_task::Response = self
            .mainchain_task
            .request_oneshot(mainchain_task::Request::AncestorHeaders(
                header.hash(),
            ))
            .map_err(|_| Error::SendMainchainTaskRequest)?
            .await
            .map_err(|_| Error::ReceiveMainchainTaskResponse)?;
        // Verify BMM
        let _: mainchain_task::Response = self
            .mainchain_task
            .request_oneshot(mainchain_task::Request::VerifyBmm(block_hash))
            .map_err(|_| Error::SendMainchainTaskRequest)?
            .await
            .map_err(|_| Error::ReceiveMainchainTaskResponse)?;
        {
            let rotxn = self.env.read_txn()?;
            if !self.archive.get_bmm_verification(&rotxn, block_hash)? {
                tracing::error!(
                    "Rejecting block {block_hash} due to failing BMM verification",
                );
                return Ok(false);
            }
            rotxn.commit()?;
        }
        // Check that ancestor bodies exist, and store body
        {
            let rotxn = self.env.read_txn()?;
            let tip = self.state.get_tip(&rotxn)?;
            let common_ancestor =
                self.archive.last_common_ancestor(&rotxn, tip, block_hash)?;
            let missing_bodies = self.archive.get_missing_bodies(
                &rotxn,
                block_hash,
                common_ancestor,
            )?;
            if !(missing_bodies.is_empty()
                || missing_bodies == vec![block_hash])
            {
                tracing::error!(
                    "Rejecting block {block_hash} due to missing ancestor bodies",
                );
                return Ok(false);
            }
            rotxn.commit()?;
            if missing_bodies == vec![block_hash] {
                let mut rwtxn = self.env.write_txn()?;
                let () = self.archive.put_body(&mut rwtxn, block_hash, body)?;
                rwtxn.commit()?;
            }
        }
        // Submit new tip
        if !self.net_task.new_tip_ready_confirm(header.hash()).await? {
            return Ok(false);
        };
        let rotxn = self.env.read_txn()?;
        let bundle = self.state.get_pending_withdrawal_bundle(&rotxn)?;
        #[cfg(all(not(target_os = "windows"), feature = "zmq"))]
        {
            let height = self.state.get_height(&rotxn)?;
            let block_hash = header.hash();
            let zmq_msgs = vec![
                "hashblock".into(),
                block_hash.0[..].into(),
                height.to_le_bytes()[..].into(),
            ];
            self.zmq_pub_handler.tx.unbounded_send(zmq_msgs).unwrap();
        }
        if let Some((bundle, _)) = bundle {
            let () = self
                .drivechain
                .broadcast_withdrawal_bundle(bundle.transaction)
                .await?;
        }
        Ok(true)
    }
}
