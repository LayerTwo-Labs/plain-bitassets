use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    net::SocketAddr,
    path::Path,
    sync::Arc,
};

#[cfg(not(target_os = "windows"))]
use async_zmq::SinkExt;
use fraction::Fraction;
use heed::RoTxn;
use tokio::sync::RwLock;
#[cfg(not(target_os = "windows"))]
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    authorization::Authorization,
    net::{PeerState, Request, Response},
    state::{AmmPoolState, DutchAuctionBidError, DutchAuctionState},
    types::*,
};

pub const THIS_SIDECHAIN: u8 = 2;

#[derive(Debug, thiserror::Error)]
pub enum GetAmmPoolStateError {
    #[error(
        "`asset0` and `asset1` must be distinct and lexicographically ordered"
    )]
    InvalidOrdering,
}

#[derive(Debug, thiserror::Error)]
pub enum GetAmmMintError {
    #[error(
        "`asset0` and `asset1` must be distinct and lexicographically ordered"
    )]
    InvalidOrdering,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("address parse error")]
    AddrParse(#[from] std::net::AddrParseError),
    #[error("AMM mint error")]
    AmmMint(#[from] GetAmmMintError),
    #[error("Get AMM pool state error")]
    AmmPoolState(#[from] GetAmmPoolStateError),
    #[error("archive error")]
    Archive(#[from] crate::archive::Error),
    #[error("bincode error")]
    Bincode(#[from] bincode::Error),
    #[error("drivechain error")]
    Drivechain(#[from] bip300301::Error),
    #[error("heed error")]
    Heed(#[from] heed::Error),
    #[error("quinn error")]
    Io(#[from] std::io::Error),
    #[error("mempool error")]
    MemPool(#[from] crate::mempool::Error),
    #[error("net error")]
    Net(#[from] crate::net::Error),
    #[error("state error")]
    State(#[from] crate::state::Error),
}

#[cfg(not(target_os = "windows"))]
#[derive(Debug)]
struct ZmqPubHandler {
    tx: mpsc::UnboundedSender<Vec<async_zmq::Message>>,
    _handle: JoinHandle<()>,
}

#[derive(Clone)]
pub struct Node {
    archive: crate::archive::Archive,
    drivechain: bip300301::Drivechain,
    env: heed::Env,
    mempool: crate::mempool::MemPool,
    net: crate::net::Net,
    state: crate::state::State,
    #[cfg(not(target_os = "windows"))]
    zmq_pub_handler: Arc<ZmqPubHandler>,
}

#[cfg(not(target_os = "windows"))]
impl ZmqPubHandler {
    // run the handler, obtaining a sender sink and the handler task
    fn new(socket_addr: SocketAddr) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<async_zmq::Message>>();
        let handle = tokio::task::spawn(async move {
            let mut zmq_pub =
                async_zmq::publish(&format!("tcp://{socket_addr}"))
                    .unwrap()
                    .bind()
                    .unwrap();

            while let Some(msgs) = rx.recv().await {
                let () = zmq_pub.send(msgs.into()).await.unwrap();
            }
        });
        Self {
            tx,
            _handle: handle,
        }
    }
}

impl Node {
    pub fn new(
        bind_addr: SocketAddr,
        datadir: &Path,
        main_addr: SocketAddr,
        password: &str,
        user: &str,
        #[cfg(not(target_os = "windows"))] zmq_addr: SocketAddr,
    ) -> Result<Self, Error> {
        let env_path = datadir.join("data.mdb");
        // let _ = std::fs::remove_dir_all(&env_path);
        std::fs::create_dir_all(&env_path)?;
        let env = heed::EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(
                crate::state::State::NUM_DBS
                    + crate::archive::Archive::NUM_DBS
                    + crate::mempool::MemPool::NUM_DBS,
            )
            .open(env_path)?;
        let archive = crate::archive::Archive::new(&env)?;
        let drivechain = bip300301::Drivechain::new(
            THIS_SIDECHAIN,
            main_addr,
            user,
            password,
        )?;
        let mempool = crate::mempool::MemPool::new(&env)?;
        let net = crate::net::Net::new(bind_addr)?;
        let state = crate::state::State::new(&env)?;
        #[cfg(not(target_os = "windows"))]
        let zmq_pub_handler = Arc::new(ZmqPubHandler::new(zmq_addr));
        Ok(Self {
            archive,
            drivechain,
            env,
            mempool,
            net,
            state,
            #[cfg(not(target_os = "windows"))]
            zmq_pub_handler,
        })
    }

    pub fn get_height(&self) -> Result<u32, Error> {
        let txn = self.env.read_txn()?;
        Ok(self.archive.get_height(&txn)?)
    }

    pub fn try_get_amm_price(
        &self,
        base: AssetId,
        quote: AssetId,
    ) -> Result<Option<Fraction>, Error> {
        let txn = self.env.read_txn()?;
        let AmmPoolState {
            reserve0,
            reserve1,
            outstanding_lp_tokens: _,
        } = self
            .state
            .amm_pools
            .get(&txn, &(base, quote))?
            .unwrap_or_default();
        if reserve0 == 0 || reserve1 == 0 {
            return Ok(None);
        }
        if base < quote {
            Ok(Some(Fraction::new(reserve1, reserve0)))
        } else {
            Ok(Some(Fraction::new(reserve0, reserve1)))
        }
    }

    pub fn get_amm_pool_state(
        &self,
        asset0: AssetId,
        asset1: AssetId,
    ) -> Result<AmmPoolState, Error> {
        if asset0 >= asset1 {
            do yeet GetAmmPoolStateError::InvalidOrdering
        }
        let txn = self.env.read_txn()?;
        let res = self
            .state
            .amm_pools
            .get(&txn, &(asset0, asset0))?
            .unwrap_or_default();
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

    pub fn get_best_hash(&self) -> Result<BlockHash, Error> {
        let txn = self.env.read_txn()?;
        Ok(self.archive.get_best_hash(&txn)?)
    }

    /** Returns the height of the block in which the tx was included.
     *  Returns an error if the tx does not exist in any known block. */
    pub fn get_tx_height(&self, txid: Txid) -> Result<u32, Error> {
        let txn = self.env.read_txn()?;
        Ok(self.archive.get_tx_height(&txn, txid)?)
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

    pub fn validate_transaction(
        &self,
        rotxn: &RoTxn,
        transaction: &AuthorizedTransaction,
    ) -> Result<u64, Error> {
        let filled_transaction = self
            .state
            .fill_transaction(rotxn, &transaction.transaction)?;
        for (authorization, spent_utxo) in transaction
            .authorizations
            .iter()
            .zip(filled_transaction.spent_utxos.iter())
        {
            if authorization.get_address() != spent_utxo.address {
                return Err(crate::state::Error::WrongPubKeyForAddress.into());
            }
        }
        if Authorization::verify_transaction(transaction).is_err() {
            return Err(crate::state::Error::AuthorizationError.into());
        }
        let fee = self
            .state
            .validate_filled_transaction(rotxn, &filled_transaction)?;
        Ok(fee)
    }

    pub async fn submit_transaction(
        &self,
        transaction: &AuthorizedTransaction,
    ) -> Result<(), Error> {
        {
            let mut txn = self.env.write_txn()?;
            self.validate_transaction(&txn, transaction)?;
            self.mempool.put(&mut txn, transaction)?;
            txn.commit()?;
        }
        for peer in self.net.peers.read().await.values() {
            peer.request(&Request::PushTransaction {
                transaction: Box::new(transaction.clone()),
            })
            .await?;
        }
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

    pub fn get_utxos_by_addresses(
        &self,
        addresses: &HashSet<Address>,
    ) -> Result<HashMap<OutPoint, FilledOutput>, Error> {
        let txn = self.env.read_txn()?;
        let utxos = self.state.get_utxos_by_addresses(&txn, addresses)?;
        Ok(utxos)
    }

    pub fn get_header(&self, height: u32) -> Result<Option<Header>, Error> {
        let txn = self.env.read_txn()?;
        Ok(self.archive.get_header(&txn, height)?)
    }

    pub fn get_body(&self, height: u32) -> Result<Option<Body>, Error> {
        let txn = self.env.read_txn()?;
        Ok(self.archive.get_body(&txn, height)?)
    }

    /// Get the block corresponding to the provided blockhash, if it exists
    pub fn get_block(&self, block_hash: BlockHash) -> Result<Block, Error> {
        let txn = self.env.read_txn()?;
        Ok(self.archive.get_block(&txn, block_hash)?)
    }

    pub fn get_all_transactions(
        &self,
    ) -> Result<Vec<AuthorizedTransaction>, Error> {
        let txn = self.env.read_txn()?;
        let transactions = self.mempool.take_all(&txn)?;
        Ok(transactions)
    }

    pub fn get_transactions(
        &self,
        number: usize,
    ) -> Result<(Vec<AuthorizedTransaction>, u64), Error> {
        let mut txn = self.env.write_txn()?;
        let transactions = self.mempool.take(&txn, number)?;
        let mut fee: u64 = 0;
        let mut returned_transactions = vec![];
        let mut spent_utxos = HashSet::new();
        for transaction in &transactions {
            let inputs: HashSet<_> =
                transaction.transaction.inputs.iter().copied().collect();
            if !spent_utxos.is_disjoint(&inputs) {
                println!("UTXO double spent");
                self.mempool
                    .delete(&mut txn, &transaction.transaction.txid())?;
                continue;
            }
            if self.validate_transaction(&txn, transaction).is_err() {
                self.mempool
                    .delete(&mut txn, &transaction.transaction.txid())?;
                continue;
            }
            let filled_transaction = self
                .state
                .fill_transaction(&txn, &transaction.transaction)?;
            let bitcoin_value_in: u64 = filled_transaction
                .spent_utxos
                .iter()
                .map(GetBitcoinValue::get_bitcoin_value)
                .sum();
            let bitcoin_value_out: u64 = filled_transaction
                .transaction
                .outputs
                .iter()
                .map(GetBitcoinValue::get_bitcoin_value)
                .sum();
            fee += bitcoin_value_in - bitcoin_value_out;
            returned_transactions.push(transaction.clone());
            spent_utxos.extend(transaction.transaction.inputs.clone());
        }
        txn.commit()?;
        Ok((returned_transactions, fee))
    }

    pub fn get_pending_withdrawal_bundle(
        &self,
    ) -> Result<Option<WithdrawalBundle>, Error> {
        let txn = self.env.read_txn()?;
        Ok(self.state.get_pending_withdrawal_bundle(&txn)?)
    }

    pub async fn submit_block(
        &self,
        header: &Header,
        body: &Body,
    ) -> Result<(), Error> {
        let last_deposit_block_hash = {
            let txn = self.env.read_txn()?;
            self.state.get_last_deposit_block_hash(&txn)?
        };
        let bundle = {
            let two_way_peg_data = self
                .drivechain
                .get_two_way_peg_data(
                    header.prev_main_hash,
                    last_deposit_block_hash,
                )
                .await?;
            let mut txn = self.env.write_txn()?;
            self.state.validate_body(&txn, body)?;
            let height = self.archive.get_height(&txn)?;
            self.state.connect_body(&mut txn, body, height)?;
            self.state.connect_two_way_peg_data(
                &mut txn,
                &two_way_peg_data,
                height,
            )?;
            let bundle = self.state.get_pending_withdrawal_bundle(&txn)?;
            self.archive.append_header(&mut txn, header)?;
            self.archive.put_body(&mut txn, header, body)?;
            for transaction in &body.transactions {
                self.mempool.delete(&mut txn, &transaction.txid())?;
            }
            txn.commit()?;
            #[cfg(not(target_os = "windows"))]
            {
                let block_hash = header.hash();
                let zmq_msgs = vec![
                    "hashblock".into(),
                    block_hash.0[..].into(),
                    height.to_le_bytes()[..].into(),
                ];
                self.zmq_pub_handler.tx.send(zmq_msgs).unwrap();
            }
            bundle
        };
        if let Some(bundle) = bundle {
            let () = self
                .drivechain
                .broadcast_withdrawal_bundle(bundle.transaction)
                .await?;
        }
        Ok(())
    }

    pub async fn connect(&self, addr: SocketAddr) -> Result<(), Error> {
        let peer = self.net.connect(addr).await?;
        let peer0 = peer.clone();
        let node0 = self.clone();
        tokio::spawn(async move {
            loop {
                match node0.peer_listen(&peer0).await {
                    Ok(_) => {}
                    Err(err) => {
                        println!("{:?}", err);
                        break;
                    }
                }
            }
        });
        let peer0 = peer.clone();
        let node0 = self.clone();
        tokio::spawn(async move {
            loop {
                match node0.heart_beat_listen(&peer0).await {
                    Ok(_) => {}
                    Err(err) => {
                        println!("{:?}", err);
                        break;
                    }
                }
            }
        });
        Ok(())
    }

    pub async fn heart_beat_listen(
        &self,
        peer: &crate::net::Peer,
    ) -> Result<(), Error> {
        let message = match peer.connection.read_datagram().await {
            Ok(message) => message,
            Err(err) => {
                self.net
                    .peers
                    .write()
                    .await
                    .remove(&peer.connection.stable_id());
                let addr = peer.connection.stable_id();
                println!("connection {addr} closed");
                return Err(crate::net::Error::from(err).into());
            }
        };
        let state: PeerState = bincode::deserialize(&message)?;
        *peer.state.write().await = Some(state);
        Ok(())
    }

    pub async fn peer_listen(
        &self,
        peer: &crate::net::Peer,
    ) -> Result<(), Error> {
        let (mut send, mut recv) = peer
            .connection
            .accept_bi()
            .await
            .map_err(crate::net::Error::from)?;
        let data = recv
            .read_to_end(crate::net::READ_LIMIT)
            .await
            .map_err(crate::net::Error::from)?;
        let message: Request = bincode::deserialize(&data)?;
        match message {
            Request::GetBlock { height } => {
                let (header, body) = {
                    let txn = self.env.read_txn()?;
                    (
                        self.archive.get_header(&txn, height)?,
                        self.archive.get_body(&txn, height)?,
                    )
                };
                let response = match (header, body) {
                    (Some(header), Some(body)) => {
                        Response::Block { header, body }
                    }
                    (_, _) => Response::NoBlock,
                };
                let response = bincode::serialize(&response)?;
                send.write_all(&response)
                    .await
                    .map_err(crate::net::Error::from)?;
                send.finish().await.map_err(crate::net::Error::from)?;
            }
            Request::PushTransaction { transaction } => {
                let valid = {
                    let txn = self.env.read_txn()?;
                    self.validate_transaction(&txn, &transaction)
                };
                match valid {
                    Err(err) => {
                        let response = Response::TransactionRejected;
                        let response = bincode::serialize(&response)?;
                        send.write_all(&response)
                            .await
                            .map_err(crate::net::Error::from)?;
                        return Err(err);
                    }
                    Ok(_) => {
                        {
                            let mut txn = self.env.write_txn()?;
                            println!(
                                "adding transaction to mempool: {:?}",
                                &transaction
                            );
                            self.mempool.put(&mut txn, &transaction)?;
                            txn.commit()?;
                        }
                        for peer0 in self.net.peers.read().await.values() {
                            if peer0.connection.stable_id()
                                == peer.connection.stable_id()
                            {
                                continue;
                            }
                            peer0
                                .request(&Request::PushTransaction {
                                    transaction: transaction.clone(),
                                })
                                .await?;
                        }
                        let response = Response::TransactionAccepted;
                        let response = bincode::serialize(&response)?;
                        send.write_all(&response)
                            .await
                            .map_err(crate::net::Error::from)?;
                        return Ok(());
                    }
                }
            }
        };
        Ok(())
    }

    pub fn run(self: Arc<Self>) -> Result<(), Error> {
        // Listening to connections.
        let node = self.clone();
        tokio::spawn(async move {
            loop {
                let incoming_conn = node.net.server.accept().await.unwrap();
                let connection = incoming_conn.await.unwrap();
                for peer in node.net.peers.read().await.values() {
                    if peer.connection.remote_address()
                        == connection.remote_address()
                    {
                        println!(
                            "already connected to {} refusing duplicate connection",
                            connection.remote_address()
                        );
                        connection.close(
                            crate::net::quinn::VarInt::from_u32(1),
                            b"already connected",
                        );
                    }
                }
                if connection.close_reason().is_some() {
                    continue;
                }
                println!(
                    "[server] connection accepted: addr={} id={}",
                    connection.remote_address(),
                    connection.stable_id(),
                );
                let peer = crate::net::Peer {
                    state: Arc::new(RwLock::new(None)),
                    connection,
                };
                let node0 = node.clone();
                let peer0 = peer.clone();
                tokio::spawn(async move {
                    loop {
                        match node0.peer_listen(&peer0).await {
                            Ok(_) => {}
                            Err(err) => {
                                println!("{:?}", err);
                                break;
                            }
                        }
                    }
                });
                let node0 = node.clone();
                let peer0 = peer.clone();
                tokio::spawn(async move {
                    loop {
                        match node0.heart_beat_listen(&peer0).await {
                            Ok(_) => {}
                            Err(err) => {
                                println!("{:?}", err);
                                break;
                            }
                        }
                    }
                });
                node.net
                    .peers
                    .write()
                    .await
                    .insert(peer.connection.stable_id(), peer);
            }
        });

        // Heart beat.
        let node = self.clone();
        tokio::spawn(async move {
            loop {
                for peer in node.net.peers.read().await.values() {
                    let block_height = {
                        let txn = node.env.read_txn().unwrap();
                        node.archive.get_height(&txn).unwrap()
                    };
                    let state = PeerState { block_height };
                    peer.heart_beat(&state).unwrap();
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });

        // Request missing headers.
        let node = self.clone();
        tokio::spawn({
            let node = node.clone();
            async move {
                loop {
                    for peer in node.net.peers.read().await.values() {
                        if let Some(state) = &peer.state.read().await.as_ref() {
                            let height = {
                                let txn = node.env.read_txn().unwrap();
                                node.archive.get_height(&txn).unwrap()
                            };
                            if state.block_height > height {
                                let response = peer
                                    .request(&Request::GetBlock {
                                        height: height + 1,
                                    })
                                    .await
                                    .unwrap();
                                match response {
                                    Response::Block { header, body } => {
                                        println!(
                                            "got new header {:?}",
                                            &header
                                        );
                                        node.submit_block(&header, &body)
                                            .await
                                            .unwrap();
                                    }
                                    Response::NoBlock => {}
                                    Response::TransactionAccepted => {}
                                    Response::TransactionRejected => {}
                                };
                            }
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        });
        Ok(())
    }
}
