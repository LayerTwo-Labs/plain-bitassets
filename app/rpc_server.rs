use std::{
    borrow::Cow, cmp::Ordering, collections::HashSet, net::SocketAddr,
    time::Duration,
};

use bitcoin::Amount;
use fraction::Fraction;
use futures::StreamExt as _;
use jsonrpsee::{
    core::{RpcResult, async_trait, middleware::RpcServiceBuilder},
    server::Server,
    types::ErrorObject,
};
use serde::{Deserialize, Serialize};

use plain_bitassets::{
    authorization::{self, Dst, Signature},
    net::{self, Peer},
    state::{self, AmmPair, AmmPoolState, BitAssetSeqId, DutchAuctionState},
    types::{
        Address, AssetId, Authorization, AuthorizedTransaction, BitAssetData,
        BitAssetId, Block, BlockHash, DutchAuctionId, DutchAuctionParams,
        EncryptionPubKey, FilledOutput, FilledOutputContent, OutPoint,
        PointedOutput, Transaction, Txid, VerifyingKey, WithdrawalBundle,
        keys::Ecies,
    },
    wallet::Balance,
};
use plain_bitassets_app_rpc_api::{
    LiteWalletProofRef, LiteWalletUpdate, LiteWalletUtreexoProof, RpcServer,
    TxInfo, TxProof,
};
use rustreexo::{
    node_hash::BitcoinNodeHash,
    pollard::{Pollard, PollardAddition},
    proof::Proof,
    stump::Stump,
};
use tower_http::{
    request_id::{
        MakeRequestId, PropagateRequestIdLayer, RequestId, SetRequestIdLayer,
    },
    trace::{DefaultOnFailure, DefaultOnResponse, TraceLayer},
};

use crate::app::App;

fn custom_err_msg(err_msg: impl Into<String>) -> ErrorObject<'static> {
    ErrorObject::owned(-1, err_msg.into(), Option::<()>::None)
}

fn custom_err<Error>(error: Error) -> ErrorObject<'static>
where
    anyhow::Error: From<Error>,
{
    let error = anyhow::Error::from(error);
    custom_err_msg(format!("{error:#}"))
}

pub struct RpcServerImpl {
    app: App,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LiteWalletQuicRequest {
    Subscribe {
        script_hashes: Vec<String>,
        from_block_hash: Option<String>,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LiteWalletQuicResponse {
    Snapshot { update: LiteWalletUpdate },
    Mempool { update: LiteWalletUpdate },
    Confirmed { update: LiteWalletUpdate },
    Error { message: String },
}

const LITE_WALLET_QUIC_MAX_REQUEST_BYTES: usize = 64 * 1024;
const LITE_WALLET_QUIC_MEMPOOL_POLL_INTERVAL: Duration = Duration::from_secs(2);
const LITE_WALLET_MAX_SCRIPT_HASHES: usize = 256;

fn normalize_lite_wallet_script_hashes(
    script_hashes: Vec<String>,
) -> RpcResult<HashSet<String>> {
    if script_hashes.is_empty() {
        return Err(custom_err_msg(
            "get_lite_wallet_update requires at least one script hash",
        ));
    }
    if script_hashes.len() > LITE_WALLET_MAX_SCRIPT_HASHES {
        return Err(custom_err_msg(format!(
            "get_lite_wallet_update accepts at most {LITE_WALLET_MAX_SCRIPT_HASHES} script hashes"
        )));
    }

    let mut watched = HashSet::with_capacity(script_hashes.len());
    for script_hash in script_hashes {
        let script_hash = script_hash.to_ascii_lowercase();
        let decoded = hex::decode(&script_hash).map_err(|err| {
            custom_err_msg(format!(
                "script hash {script_hash} is not valid hex: {err}"
            ))
        })?;
        if decoded.len() != 32 {
            return Err(custom_err_msg(format!(
                "script hash {script_hash} must be 32 bytes, got {} bytes",
                decoded.len()
            )));
        }
        watched.insert(script_hash);
    }
    Ok(watched)
}

fn ensure_lite_wallet_cursor_on_active_chain(
    from_block_hash: BlockHash,
    active_hash_at_height: Option<BlockHash>,
) -> RpcResult<()> {
    match active_hash_at_height {
        Some(active_hash_at_height)
            if active_hash_at_height == from_block_hash =>
        {
            Ok(())
        }
        Some(active_hash_at_height) => Err(custom_err_msg(format!(
            "from_block_hash {from_block_hash} is no longer on the active sidechain at its height; active hash is {active_hash_at_height}; resync from snapshot"
        ))),
        None => Err(custom_err_msg(format!(
            "from_block_hash {from_block_hash} height is no longer available on the active sidechain; resync from snapshot"
        ))),
    }
}

impl RpcServerImpl {
    fn script_hash(address: &Address) -> String {
        hex::encode(blake3::hash(&address.0).as_bytes())
    }

    fn lite_wallet_leaf_hash(
        outpoint: &OutPoint,
        output: &FilledOutput,
        proof_ref: &LiteWalletProofRef,
    ) -> BitcoinNodeHash {
        let content = match &output.content {
            FilledOutputContent::BitAsset(bitasset_id, amount) => {
                format!("bitasset:{}:{amount}", hex::encode(bitasset_id.0))
            }
            FilledOutputContent::BitAssetControl(bitasset_id) => {
                format!("bitasset-control:{}", hex::encode(bitasset_id.0))
            }
            FilledOutputContent::AmmLpToken {
                asset0,
                asset1,
                amount,
            } => {
                format!("amm-lp:{asset0}:{asset1}:{amount}")
            }
            FilledOutputContent::Bitcoin(value) => {
                format!("bitcoin:{}", value.0.to_sat())
            }
            FilledOutputContent::BitcoinWithdrawal(withdrawal) => {
                format!("withdrawal:{withdrawal:?}")
            }
            FilledOutputContent::BitAssetReservation(txid, commitment) => {
                format!("reservation:{txid}:{}", hex::encode(commitment))
            }
            FilledOutputContent::DutchAuctionReceipt(auction_id) => {
                format!("dutch-auction:{auction_id}")
            }
        };
        let payload = borsh::to_vec(&(
            "plain-bitassets:lite-wallet-leaf:v1",
            outpoint.to_string(),
            output.address.0,
            content,
            output.memo.clone(),
            proof_ref.sidechain_block_height.unwrap_or_default(),
            proof_ref.block_hash.clone().unwrap_or_default(),
        ))
        .expect("lite-wallet leaf payload is always borsh-serializable");
        BitcoinNodeHash::from(*blake3::hash(&payload).as_bytes())
    }

    fn utreexo_view(
        &self,
        utxos: &std::collections::HashMap<OutPoint, FilledOutput>,
    ) -> RpcResult<(
        u64,
        Vec<String>,
        std::collections::HashMap<OutPoint, (String, Proof)>,
    )> {
        let mut pollard = Pollard::new();
        let mut stump = Stump::new();
        let mut leaves = Vec::new();
        let mut leaf_by_outpoint = std::collections::HashMap::new();
        for (outpoint, output) in utxos {
            let txid = match outpoint {
                OutPoint::Regular { txid, vout: _ } => Some(*txid),
                OutPoint::Coinbase { .. } | OutPoint::Deposit(_) => None,
            };
            let proof_ref = match txid {
                Some(txid) => self.lite_wallet_proof_ref(txid)?,
                None => LiteWalletProofRef {
                    txid: Txid([0; 32]),
                    block_hash: None,
                    sidechain_block_height: None,
                    bmm_inclusions: Vec::new(),
                    best_main_verification: None,
                },
            };
            let leaf_hash =
                Self::lite_wallet_leaf_hash(outpoint, output, &proof_ref);
            leaves.push(PollardAddition {
                hash: leaf_hash,
                remember: true,
            });
            leaf_by_outpoint.insert(*outpoint, leaf_hash);
        }
        pollard
            .modify(&leaves, &[], Proof::default())
            .map_err(|err| {
                custom_err_msg(format!("utreexo pollard modify: {err:?}"))
            })?;
        let add_hashes: Vec<_> = leaves.iter().map(|leaf| leaf.hash).collect();
        stump = stump
            .modify(&add_hashes, &[], &Proof::default())
            .map_err(|err| {
                custom_err_msg(format!("utreexo stump modify: {err:?}"))
            })?
            .0;
        let mut proofs = std::collections::HashMap::new();
        for (outpoint, leaf_hash) in leaf_by_outpoint {
            let proof = pollard.batch_proof(&[leaf_hash]).map_err(|err| {
                custom_err_msg(format!("utreexo proof: {err:?}"))
            })?;
            proofs.insert(outpoint, (leaf_hash.to_string(), proof));
        }
        Ok((
            stump.leaves,
            stump.roots.iter().map(ToString::to_string).collect(),
            proofs,
        ))
    }

    fn lite_wallet_proof_ref(
        &self,
        txid: Txid,
    ) -> RpcResult<LiteWalletProofRef> {
        let Some((_, txin)) = self
            .app
            .node
            .try_get_filled_transaction(txid)
            .map_err(custom_err)?
        else {
            return Ok(LiteWalletProofRef {
                txid,
                block_hash: None,
                sidechain_block_height: None,
                bmm_inclusions: Vec::new(),
                best_main_verification: None,
            });
        };
        let Some(txin) = txin else {
            return Ok(LiteWalletProofRef {
                txid,
                block_hash: None,
                sidechain_block_height: None,
                bmm_inclusions: Vec::new(),
                best_main_verification: None,
            });
        };
        let sidechain_block_height = self
            .app
            .node
            .get_height(txin.block_hash)
            .map_err(custom_err)?;
        let bmm_inclusions = self
            .app
            .node
            .get_bmm_inclusions(txin.block_hash)
            .map_err(custom_err)?
            .into_iter()
            .map(|block_hash| block_hash.to_string())
            .collect();
        let best_main_verification = self
            .app
            .node
            .get_best_main_verification(txin.block_hash)
            .map_err(custom_err)?
            .to_string();
        Ok(LiteWalletProofRef {
            txid,
            block_hash: Some(txin.block_hash.to_string()),
            sidechain_block_height: Some(sidechain_block_height),
            bmm_inclusions,
            best_main_verification: Some(best_main_verification),
        })
    }

    fn lite_wallet_update(
        &self,
        script_hashes: Vec<String>,
        from_block_hash: Option<String>,
    ) -> RpcResult<LiteWalletUpdate> {
        let watched = normalize_lite_wallet_script_hashes(script_hashes)?;
        let tip_hash = self
            .app
            .node
            .try_get_tip()
            .map_err(custom_err)?
            .map(|hash| hash.to_string());
        let tip_height =
            self.app.node.try_get_tip_height().map_err(custom_err)?;

        let mut created_utxos = Vec::new();
        let mut spent_outpoints = Vec::new();
        let mut transactions = Vec::new();
        let mut proof_refs = Vec::new();
        let all_confirmed_utxos =
            self.app.node.get_all_utxos().map_err(custom_err)?;
        let confirmed_watched_utxos: std::collections::HashMap<_, _> =
            all_confirmed_utxos
                .iter()
                .filter(|(_, output)| {
                    watched.contains(&Self::script_hash(&output.address))
                })
                .map(|(outpoint, output)| (*outpoint, output.clone()))
                .collect();
        let (utreexo_leaf_count, utreexo_roots, utreexo_proof_map) =
            self.utreexo_view(&all_confirmed_utxos)?;

        match (from_block_hash, tip_height) {
            (None, _) => {
                created_utxos = confirmed_watched_utxos
                    .iter()
                    .map(|(outpoint, output)| PointedOutput {
                        outpoint: *outpoint,
                        output: output.clone(),
                    })
                    .collect();
                for txid in confirmed_watched_utxos
                    .keys()
                    .filter_map(|outpoint| match outpoint {
                        plain_bitassets::types::OutPoint::Regular {
                            txid,
                            vout: _,
                        } => Some(*txid),
                        plain_bitassets::types::OutPoint::Coinbase {
                            ..
                        }
                        | plain_bitassets::types::OutPoint::Deposit(_) => None,
                    })
                    .collect::<HashSet<_>>()
                {
                    if let Some((filled_tx, _)) = self
                        .app
                        .node
                        .try_get_filled_transaction(txid)
                        .map_err(custom_err)?
                    {
                        transactions.push(filled_tx.transaction.transaction);
                    }
                    proof_refs.push(self.lite_wallet_proof_ref(txid)?);
                }
            }
            (Some(from_block_hash), Some(tip_height)) => {
                let from_block_hash: BlockHash =
                    from_block_hash.parse().map_err(custom_err)?;
                let from_height = self
                    .app
                    .node
                    .try_get_height(from_block_hash)
                    .map_err(custom_err)?
                    .ok_or_else(|| {
                        custom_err_msg(format!(
                            "from_block_hash {from_block_hash} is not known"
                        ))
                    })?;
                let active_hash_at_from_height = self
                    .app
                    .node
                    .try_get_block_hash(from_height)
                    .map_err(custom_err)?;
                ensure_lite_wallet_cursor_on_active_chain(
                    from_block_hash,
                    active_hash_at_from_height,
                )?;
                for height in from_height.saturating_add(1)..=tip_height {
                    let Some(block_hash) = self
                        .app
                        .node
                        .try_get_block_hash(height)
                        .map_err(custom_err)?
                    else {
                        continue;
                    };
                    let body = self
                        .app
                        .node
                        .get_body(block_hash)
                        .map_err(custom_err)?;
                    for tx in body.transactions {
                        let txid = tx.txid();
                        let filled_tx = self
                            .app
                            .node
                            .try_get_filled_transaction(txid)
                            .map_err(custom_err)?
                            .map(|(filled_tx, _)| filled_tx.transaction);
                        let Some(filled_tx) = filled_tx else {
                            continue;
                        };

                        let mut relevant = false;
                        for (outpoint, spent_output) in filled_tx
                            .inputs()
                            .iter()
                            .zip(filled_tx.spent_utxos.iter())
                        {
                            if watched.contains(&Self::script_hash(
                                &spent_output.address,
                            )) {
                                spent_outpoints.push(*outpoint);
                                relevant = true;
                            }
                        }
                        if let Some(filled_outputs) = filled_tx.filled_outputs()
                        {
                            for (vout, output) in
                                filled_outputs.into_iter().enumerate()
                            {
                                if watched.contains(&Self::script_hash(
                                    &output.address,
                                )) {
                                    created_utxos.push(PointedOutput {
                                        outpoint: plain_bitassets::types::OutPoint::Regular {
                                            txid,
                                            vout: vout as u32,
                                        },
                                        output,
                                    });
                                    relevant = true;
                                }
                            }
                        }
                        if relevant {
                            transactions.push(tx);
                            proof_refs.push(self.lite_wallet_proof_ref(txid)?);
                        }
                    }
                }
            }
            (Some(_), None) => (),
        }

        let mempool_transactions =
            self.app.node.get_all_transactions().map_err(custom_err)?;
        let mut mempool_created_utxos = Vec::new();
        for tx in &mempool_transactions {
            let txid = tx.transaction.txid();
            for (vout, output) in tx.transaction.outputs.iter().enumerate() {
                if watched.contains(&Self::script_hash(&output.address)) {
                    mempool_created_utxos.push(PointedOutput {
                        outpoint: OutPoint::Regular {
                            txid,
                            vout: vout as u32,
                        },
                        output: output.clone(),
                    });
                }
            }
        }

        let watched_unspent_outpoints: Vec<_> = confirmed_watched_utxos
            .keys()
            .chain(mempool_created_utxos.iter().map(|utxo| &utxo.outpoint))
            .collect();
        let mempool_spent_outpoints = self
            .app
            .node
            .get_unconfirmed_spent_utxos(watched_unspent_outpoints)
            .map_err(custom_err)?
            .into_iter()
            .map(|(outpoint, _)| outpoint)
            .collect();
        let utreexo_proofs = created_utxos
            .iter()
            .filter_map(|utxo| {
                let (leaf_hash, proof) =
                    utreexo_proof_map.get(&utxo.outpoint)?.clone();
                Some(LiteWalletUtreexoProof {
                    outpoint: utxo.outpoint,
                    leaf_hash,
                    targets: proof.targets,
                    hashes: proof
                        .hashes
                        .iter()
                        .map(ToString::to_string)
                        .collect(),
                })
            })
            .collect();

        Ok(LiteWalletUpdate {
            tip_hash,
            tip_height,
            utreexo_leaf_count,
            utreexo_roots,
            created_utxos,
            spent_outpoints,
            mempool_created_utxos,
            mempool_spent_outpoints,
            transactions,
            proof_refs,
            utreexo_proofs,
        })
    }
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
        let next_amm_pool_state =
            amm_pool_state.burn(lp_token_amount).map_err(custom_err)?;
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
            .map_err(custom_err)?;
        let txid = tx.txid();
        let () = self.app.sign_and_send(tx).map_err(custom_err)?;
        Ok(txid)
    }

    async fn amm_mint(
        &self,
        asset0: AssetId,
        asset1: AssetId,
        amount0: u64,
        amount1: u64,
    ) -> RpcResult<Txid> {
        let pair = AmmPair::new(asset0, asset1);
        let lp_token_mint = match self
            .app
            .node
            .try_get_amm_pool_state(pair)
            .map_err(custom_err)?
        {
            Some(amm_pool_state) => {
                let next_amm_pool_state = amm_pool_state
                    .mint(amount0, amount1)
                    .map_err(custom_err)?;
                next_amm_pool_state.outstanding_lp_tokens
                    - amm_pool_state.outstanding_lp_tokens
            }
            None => num::integer::sqrt(amount0 as u128 * amount1 as u128)
                .try_into()
                .map_err(custom_err)?,
        };
        let mut tx = Transaction::default();
        let () = self
            .app
            .wallet
            .amm_mint(&mut tx, asset0, asset1, amount0, amount1, lp_token_mint)
            .map_err(custom_err)?;
        let txid = tx.txid();
        let () = self.app.sign_and_send(tx).map_err(custom_err)?;
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
                let err = state::error::Amm::InvalidSwap;
                return Err(custom_err(err));
            }
            Ordering::Greater => (asset_receive, asset_spend),
        };
        let amm_pool_state = self.get_amm_pool_state(pair.0, pair.1).await?;
        let amount_receive = (if asset_spend < asset_receive {
            amm_pool_state.swap_asset0_for_asset1(amount_spend).map(
                |new_amm_pool_state| {
                    amm_pool_state.reserve1 - new_amm_pool_state.reserve1
                },
            )
        } else {
            amm_pool_state.swap_asset1_for_asset0(amount_spend).map(
                |new_amm_pool_state| {
                    amm_pool_state.reserve0 - new_amm_pool_state.reserve0
                },
            )
        })
        .map_err(custom_err)?;
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
            .map_err(custom_err)?;
        let authorized_tx =
            self.app.wallet.authorize(tx).map_err(custom_err)?;
        self.app
            .node
            .submit_transaction(authorized_tx)
            .map_err(custom_err)?;
        Ok(amount_receive)
    }

    async fn bitasset_data(
        &self,
        bitasset_id: BitAssetId,
    ) -> RpcResult<BitAssetData> {
        self.app
            .node
            .get_current_bitasset_data(&bitasset_id)
            .map_err(custom_err)
    }

    async fn bitassets(
        &self,
    ) -> RpcResult<Vec<(BitAssetSeqId, BitAssetId, BitAssetData)>> {
        self.app.node.bitassets().map_err(custom_err)
    }

    async fn bitcoin_balance(&self) -> RpcResult<Balance> {
        self.app.wallet.get_bitcoin_balance().map_err(custom_err)
    }

    async fn connect_peer(&self, addr: SocketAddr) -> RpcResult<()> {
        self.app.node.connect_peer(addr).map_err(custom_err)
    }

    async fn create_deposit(
        &self,
        address: Address,
        value_sats: u64,
        fee_sats: u64,
    ) -> RpcResult<bitcoin::Txid> {
        let app = self.app.clone();
        tokio::task::spawn_blocking(move || {
            app.deposit(
                address,
                bitcoin::Amount::from_sat(value_sats),
                bitcoin::Amount::from_sat(fee_sats),
            )
            .map_err(custom_err)
        })
        .await
        .unwrap()
    }

    async fn decrypt_msg(
        &self,
        encryption_pubkey: EncryptionPubKey,
        msg: String,
    ) -> RpcResult<String> {
        let ciphertext = hex::decode(msg).map_err(custom_err)?;
        self.app
            .wallet
            .decrypt_msg(&encryption_pubkey, &ciphertext)
            .map(hex::encode)
            .map_err(custom_err)
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
            .map_err(custom_err)?;
        let next_auction_state = auction_state
            .bid(Txid::default(), bid_size, height)
            .map_err(custom_err)?;
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
            .map_err(custom_err)?;
        let authorized_tx =
            self.app.wallet.authorize(tx).map_err(custom_err)?;
        self.app
            .node
            .submit_transaction(authorized_tx)
            .map_err(custom_err)?;
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
            .map_err(custom_err)?;
        if height <= auction_state.start_block + auction_state.duration {
            let err = state::error::dutch_auction::Collect::AuctionNotFinished;
            return Err(custom_err(err));
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
            .map_err(custom_err)?;
        let authorized_tx =
            self.app.wallet.authorize(tx).map_err(custom_err)?;
        self.app
            .node
            .submit_transaction(authorized_tx)
            .map_err(custom_err)?;
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
            .map_err(custom_err)?;
        let txid = tx.txid();
        let () = self.app.sign_and_send(tx).map_err(custom_err)?;
        Ok(txid)
    }

    async fn dutch_auctions(
        &self,
    ) -> RpcResult<Vec<(DutchAuctionId, DutchAuctionState)>> {
        self.app.node.dutch_auctions().map_err(custom_err)
    }

    async fn encrypt_msg(
        &self,
        encryption_pubkey: EncryptionPubKey,
        msg: String,
    ) -> RpcResult<String> {
        Ecies::new(encryption_pubkey.0)
            .encrypt(msg.as_bytes())
            .map(hex::encode)
            .map_err(|err| custom_err(anyhow::anyhow!("{err:?}")))
    }

    async fn forget_peer(&self, addr: SocketAddr) -> RpcResult<()> {
        match self.app.node.forget_peer(&addr) {
            Ok(_) => Ok(()),
            Err(err) => Err(custom_err(err)),
        }
    }

    async fn format_deposit_address(
        &self,
        address: Address,
    ) -> RpcResult<String> {
        let deposit_address = address.format_for_deposit();
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
            .map_err(custom_err)
    }

    async fn get_amm_price(
        &self,
        base: AssetId,
        quote: AssetId,
    ) -> RpcResult<Option<Fraction>> {
        self.app
            .node
            .try_get_amm_price(base, quote)
            .map_err(custom_err)
    }

    async fn get_block(&self, block_hash: BlockHash) -> RpcResult<Block> {
        let block = self
            .app
            .node
            .get_block(block_hash)
            .expect("This error should have been handled properly.");
        Ok(block)
    }

    async fn get_best_sidechain_block_hash(
        &self,
    ) -> RpcResult<Option<BlockHash>> {
        self.app.node.try_get_tip().map_err(custom_err)
    }

    async fn get_best_mainchain_block_hash(
        &self,
    ) -> RpcResult<Option<bitcoin::BlockHash>> {
        let Some(sidechain_hash) =
            self.app.node.try_get_tip().map_err(custom_err)?
        else {
            // No sidechain tip, so no best mainchain block hash.
            return Ok(None);
        };
        let block_hash = self
            .app
            .node
            .get_best_main_verification(sidechain_hash)
            .map_err(custom_err)?;
        Ok(Some(block_hash))
    }

    async fn get_bmm_inclusions(
        &self,
        block_hash: plain_bitassets::types::BlockHash,
    ) -> RpcResult<Vec<bitcoin::BlockHash>> {
        self.app
            .node
            .get_bmm_inclusions(block_hash)
            .map_err(custom_err)
    }

    async fn get_new_address(&self) -> RpcResult<Address> {
        self.app.wallet.get_new_address().map_err(custom_err)
    }

    async fn get_new_encryption_key(&self) -> RpcResult<EncryptionPubKey> {
        self.app.wallet.get_new_encryption_key().map_err(custom_err)
    }

    async fn get_new_verifying_key(&self) -> RpcResult<VerifyingKey> {
        self.app.wallet.get_new_verifying_key().map_err(custom_err)
    }

    async fn get_transaction(
        &self,
        txid: Txid,
    ) -> RpcResult<Option<Transaction>> {
        self.app.node.try_get_transaction(txid).map_err(custom_err)
    }

    async fn get_transaction_info(
        &self,
        txid: Txid,
    ) -> RpcResult<Option<TxInfo>> {
        let Some((filled_tx, txin)) = self
            .app
            .node
            .try_get_filled_transaction(txid)
            .map_err(custom_err)?
        else {
            return Ok(None);
        };
        let confirmations = match txin {
            Some(txin) => {
                let tip_height = self
                    .app
                    .node
                    .try_get_tip_height()
                    .map_err(custom_err)?
                    .expect("Height should exist for tip");
                let height = self
                    .app
                    .node
                    .get_height(txin.block_hash)
                    .map_err(custom_err)?;
                Some(tip_height - height)
            }
            None => None,
        };
        let fee_sats = filled_tx
            .transaction
            .bitcoin_fee()
            .map_err(custom_err)?
            .unwrap()
            .to_sat();
        let res = TxInfo {
            confirmations,
            fee_sats,
            txin,
        };
        Ok(Some(res))
    }

    async fn get_transaction_proof(
        &self,
        txid: Txid,
    ) -> RpcResult<Option<TxProof>> {
        let Some((filled_tx, txin)) = self
            .app
            .node
            .try_get_filled_transaction(txid)
            .map_err(custom_err)?
        else {
            return Ok(None);
        };

        let (
            confirmations,
            block,
            sidechain_block_height,
            bmm_inclusions,
            best_main_verification,
        ) = match txin {
            Some(txin) => {
                let tip_height = self
                    .app
                    .node
                    .try_get_tip_height()
                    .map_err(custom_err)?
                    .expect("Height should exist for tip");
                let height = self
                    .app
                    .node
                    .get_height(txin.block_hash)
                    .map_err(custom_err)?;
                let block = self
                    .app
                    .node
                    .get_block(txin.block_hash)
                    .map_err(custom_err)?;
                let bmm_inclusions = self
                    .app
                    .node
                    .get_bmm_inclusions(txin.block_hash)
                    .map_err(custom_err)?;
                let best_main_verification = self
                    .app
                    .node
                    .get_best_main_verification(txin.block_hash)
                    .map_err(custom_err)?;

                (
                    Some(tip_height - height),
                    Some(block),
                    Some(height),
                    bmm_inclusions
                        .into_iter()
                        .map(|block_hash| block_hash.to_string())
                        .collect(),
                    Some(best_main_verification.to_string()),
                )
            }
            None => (None, None, None, Vec::new(), None),
        };

        let fee_sats = filled_tx
            .transaction
            .bitcoin_fee()
            .map_err(custom_err)?
            .unwrap()
            .to_sat();

        Ok(Some(TxProof {
            txid,
            transaction: filled_tx.transaction.transaction,
            txin,
            block,
            sidechain_block_height,
            bmm_inclusions,
            best_main_verification,
            confirmations,
            fee_sats,
        }))
    }

    async fn get_wallet_addresses(&self) -> RpcResult<Vec<Address>> {
        let addrs = self.app.wallet.get_addresses().map_err(custom_err)?;
        let mut res: Vec<_> = addrs.into_iter().collect();
        res.sort_by_key(|addr| addr.as_base58());
        Ok(res)
    }

    async fn get_wallet_utxos(
        &self,
    ) -> RpcResult<Vec<PointedOutput<FilledOutputContent>>> {
        let utxos = self.app.wallet.get_utxos().map_err(custom_err)?;
        let utxos = utxos
            .into_iter()
            .map(|(outpoint, output)| PointedOutput { outpoint, output })
            .collect();
        Ok(utxos)
    }

    async fn getblockcount(&self) -> RpcResult<u32> {
        let height = self.app.node.try_get_tip_height().map_err(custom_err)?;
        let block_count = height.map_or(0, |height| height + 1);
        Ok(block_count)
    }

    async fn latest_failed_withdrawal_bundle_height(
        &self,
    ) -> RpcResult<Option<u32>> {
        let height = self
            .app
            .node
            .get_latest_failed_withdrawal_bundle_height()
            .map_err(custom_err)?;
        Ok(height)
    }

    async fn list_peers(&self) -> RpcResult<Vec<Peer>> {
        let peers = self.app.node.get_active_peers();
        Ok(peers)
    }

    async fn list_utxos(
        &self,
    ) -> RpcResult<Vec<PointedOutput<FilledOutputContent>>> {
        let utxos = self.app.node.get_all_utxos().map_err(custom_err)?;
        let res = utxos
            .into_iter()
            .map(|(outpoint, output)| PointedOutput { outpoint, output })
            .collect();
        Ok(res)
    }

    async fn get_lite_wallet_update(
        &self,
        script_hashes: Vec<String>,
        from_block_hash: Option<String>,
    ) -> RpcResult<LiteWalletUpdate> {
        self.lite_wallet_update(script_hashes, from_block_hash)
    }

    async fn mine(&self, fee: Option<u64>) -> RpcResult<()> {
        let fee = fee.map(bitcoin::Amount::from_sat);
        self.app
            .local_pool
            .spawn_pinned({
                let app = self.app.clone();
                move || async move { app.mine(fee).await.map_err(custom_err) }
            })
            .await
            .unwrap()
    }

    async fn my_unconfirmed_utxos(&self) -> RpcResult<Vec<PointedOutput>> {
        let addresses = self.app.wallet.get_addresses().map_err(custom_err)?;
        let utxos = self
            .app
            .node
            .get_unconfirmed_utxos_by_addresses(&addresses)
            .map_err(custom_err)?
            .into_iter()
            .map(|(outpoint, output)| PointedOutput { outpoint, output })
            .collect();
        Ok(utxos)
    }

    async fn my_utxos(
        &self,
    ) -> RpcResult<Vec<PointedOutput<FilledOutputContent>>> {
        let utxos = self
            .app
            .wallet
            .get_utxos()
            .map_err(custom_err)?
            .into_iter()
            .map(|(outpoint, output)| PointedOutput { outpoint, output })
            .collect();
        Ok(utxos)
    }

    async fn openapi_schema(&self) -> RpcResult<utoipa::openapi::OpenApi> {
        let res =
            <plain_bitassets_app_rpc_api::RpcDoc as utoipa::OpenApi>::openapi();
        Ok(res)
    }

    async fn pending_withdrawal_bundle(
        &self,
    ) -> RpcResult<Option<WithdrawalBundle>> {
        self.app
            .node
            .try_get_pending_withdrawal_bundle()
            .map_err(custom_err)
    }

    async fn register_bitasset(
        &self,
        plain_name: String,
        initial_supply: u64,
        bitasset_data: Option<BitAssetData>,
    ) -> RpcResult<Txid> {
        let mut tx = Transaction::default();
        let bitasset_data = Cow::Owned(bitasset_data.unwrap_or_default());
        let () = match self.app.wallet.register_bitasset(
            &mut tx,
            &plain_name,
            bitasset_data,
            initial_supply,
        ) {
            Ok(()) => (),
            Err(err) => return Err(custom_err(err)),
        };
        let txid = tx.txid();
        let () = self.app.sign_and_send(tx).map_err(custom_err)?;
        Ok(txid)
    }

    async fn remove_from_mempool(&self, txid: Txid) -> RpcResult<()> {
        self.app.node.remove_from_mempool(txid).map_err(custom_err)
    }

    async fn reserve_bitasset(&self, plain_name: String) -> RpcResult<Txid> {
        let mut tx = Transaction::default();
        let () = match self.app.wallet.reserve_bitasset(&mut tx, &plain_name) {
            Ok(()) => (),
            Err(err) => return Err(custom_err(err)),
        };
        let txid = tx.txid();
        let () = self.app.sign_and_send(tx).map_err(custom_err)?;
        Ok(txid)
    }

    async fn set_seed_from_mnemonic(&self, mnemonic: String) -> RpcResult<()> {
        self.app
            .wallet
            .set_seed_from_mnemonic(mnemonic.as_str())
            .map_err(custom_err)
    }

    async fn sidechain_wealth_sats(&self) -> RpcResult<u64> {
        let sidechain_wealth =
            self.app.node.get_sidechain_wealth().map_err(custom_err)?;
        Ok(sidechain_wealth.to_sat())
    }

    async fn sign_arbitrary_msg(
        &self,
        verifying_key: VerifyingKey,
        msg: String,
    ) -> RpcResult<Signature> {
        self.app
            .wallet
            .sign_arbitrary_msg(&verifying_key, &msg)
            .map_err(custom_err)
    }

    async fn sign_arbitrary_msg_as_addr(
        &self,
        address: Address,
        msg: String,
    ) -> RpcResult<Authorization> {
        self.app
            .wallet
            .sign_arbitrary_msg_as_addr(&address, &msg)
            .map_err(custom_err)
    }

    async fn submit_authorized_transaction(
        &self,
        hex_borsh_authorized_tx: String,
    ) -> RpcResult<Txid> {
        let bytes = hex::decode(hex_borsh_authorized_tx).map_err(custom_err)?;
        let authorized_tx: AuthorizedTransaction =
            borsh::from_slice(&bytes).map_err(custom_err)?;
        let txid = authorized_tx.transaction.txid();
        self.app
            .node
            .submit_transaction(authorized_tx)
            .map_err(custom_err)?;
        Ok(txid)
    }

    async fn stop(&self) {
        std::process::exit(0);
    }

    async fn transfer(
        &self,
        dest: Address,
        value_sats: u64,
        fee_sats: u64,
        memo: Option<String>,
    ) -> RpcResult<Txid> {
        let memo = match memo {
            None => None,
            Some(memo) => {
                let hex = hex::decode(memo).map_err(custom_err)?;
                Some(hex)
            }
        };
        let tx = self
            .app
            .wallet
            .create_transfer(
                dest,
                Amount::from_sat(value_sats),
                Amount::from_sat(fee_sats),
                memo,
            )
            .map_err(custom_err)?;
        let txid = tx.txid();
        let () = self.app.sign_and_send(tx).map_err(custom_err)?;
        Ok(txid)
    }

    async fn transfer_bitasset(
        &self,
        dest: Address,
        asset_id: BitAssetId,
        amount: u64,
        fee_sats: u64,
        memo: Option<String>,
    ) -> RpcResult<Txid> {
        let memo = match memo {
            None => None,
            Some(memo) => {
                let hex = hex::decode(memo).map_err(custom_err)?;
                Some(hex)
            }
        };
        let tx = self
            .app
            .wallet
            .create_bitasset_transfer(
                dest,
                asset_id,
                amount,
                Amount::from_sat(fee_sats),
                memo,
            )
            .map_err(custom_err)?;
        let txid = tx.txid();
        let () = self.app.sign_and_send(tx).map_err(custom_err)?;
        Ok(txid)
    }

    async fn verify_signature(
        &self,
        signature: Signature,
        verifying_key: VerifyingKey,
        dst: Dst,
        msg: String,
    ) -> RpcResult<bool> {
        let res = authorization::verify(
            signature,
            &verifying_key,
            dst,
            msg.as_bytes(),
        );
        Ok(res)
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
                Amount::from_sat(amount_sats),
                Amount::from_sat(mainchain_fee_sats),
                Amount::from_sat(fee_sats),
            )
            .map_err(custom_err)?;
        let txid = tx.txid();
        self.app.sign_and_send(tx).map_err(custom_err)?;
        Ok(txid)
    }
}

#[derive(Clone, Debug)]
struct RequestIdMaker;

impl MakeRequestId for RequestIdMaker {
    fn make_request_id<B>(
        &mut self,
        _: &http::Request<B>,
    ) -> Option<RequestId> {
        use uuid::Uuid;
        // the 'simple' format renders the UUID with no dashes, which
        // makes for easier copy/pasting.
        let id = Uuid::new_v4();
        let id = id.as_simple();
        let id = format!("req_{id}"); // prefix all IDs with "req_", to make them easier to identify

        let Ok(header_value) = http::HeaderValue::from_str(&id) else {
            return None;
        };

        Some(RequestId::new(header_value))
    }
}

pub async fn run_server(
    app: App,
    rpc_url: url::Url,
) -> anyhow::Result<SocketAddr> {
    const REQUEST_ID_HEADER: &str = "x-request-id";

    // Ordering here matters! Order here is from official docs on request IDs tracings
    // https://docs.rs/tower-http/latest/tower_http/request_id/index.html#using-trace
    let tracer = tower::ServiceBuilder::new()
        .layer(SetRequestIdLayer::new(
            http::HeaderName::from_static(REQUEST_ID_HEADER),
            RequestIdMaker,
        ))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(move |request: &http::Request<_>| {
                    let request_id = request
                        .headers()
                        .get(http::HeaderName::from_static(REQUEST_ID_HEADER))
                        .and_then(|h| h.to_str().ok())
                        .filter(|s| !s.is_empty());

                    tracing::span!(
                        tracing::Level::DEBUG,
                        "request",
                        method = %request.method(),
                        uri = %request.uri(),
                        request_id , // this is needed for the record call below to work
                    )
                })
                .on_request(())
                .on_eos(())
                .on_response(
                    DefaultOnResponse::new().level(tracing::Level::INFO),
                )
                .on_failure(
                    DefaultOnFailure::new().level(tracing::Level::ERROR),
                ),
        )
        .layer(PropagateRequestIdLayer::new(http::HeaderName::from_static(
            REQUEST_ID_HEADER,
        )))
        .into_inner();

    let http_middleware = tower::ServiceBuilder::new().layer(tracer);
    let rpc_middleware = RpcServiceBuilder::new().rpc_logger(1024);

    let server = Server::builder()
        .set_http_middleware(http_middleware)
        .set_rpc_middleware(rpc_middleware)
        .build(rpc_url.socket_addrs(|| None)?.as_slice())
        .await?;

    let addr = server.local_addr()?;
    let handle = server.start(RpcServerImpl { app }.into_rpc());

    // In this example we don't care about doing shutdown so let's it run forever.
    // You may use the `ServerHandle` to shut it down or manage it yourself.
    tokio::spawn(handle.stopped());

    Ok(addr)
}

pub async fn run_lite_wallet_quic_server(
    app: App,
    bind_addr: SocketAddr,
) -> anyhow::Result<()> {
    let (endpoint, _server_cert) = net::make_server_endpoint(bind_addr)?;
    while let Some(connecting) = endpoint.accept().await {
        let app = app.clone();
        tokio::spawn(async move {
            if let Err(err) =
                handle_lite_wallet_quic_connection(app, connecting).await
            {
                tracing::warn!("lite-wallet QUIC connection failed: {err:#}");
            }
        });
    }
    Ok(())
}

async fn handle_lite_wallet_quic_connection(
    app: App,
    connecting: quinn::Incoming,
) -> anyhow::Result<()> {
    let connection = connecting.await?;
    let (mut send, mut recv) = connection.accept_bi().await?;
    let request_bytes = match recv
        .read_to_end(LITE_WALLET_QUIC_MAX_REQUEST_BYTES)
        .await
    {
        Ok(request_bytes) => request_bytes,
        Err(err) => {
            write_lite_wallet_quic_response(
                &mut send,
                &LiteWalletQuicResponse::Error {
                    message: format!(
                        "lite-wallet request exceeds {LITE_WALLET_QUIC_MAX_REQUEST_BYTES} bytes or could not be read: {err}"
                    ),
                },
            )
            .await?;
            send.finish()?;
            return Ok(());
        }
    };
    let request =
        match serde_json::from_slice::<LiteWalletQuicRequest>(&request_bytes) {
            Ok(request) => request,
            Err(err) => {
                write_lite_wallet_quic_response(
                    &mut send,
                    &LiteWalletQuicResponse::Error {
                        message: format!("invalid lite-wallet request: {err}"),
                    },
                )
                .await?;
                send.finish()?;
                return Ok(());
            }
        };

    let LiteWalletQuicRequest::Subscribe {
        script_hashes,
        from_block_hash,
    } = request;
    let rpc = RpcServerImpl { app: app.clone() };
    let mut last_tip_hash = from_block_hash;
    match rpc.lite_wallet_update(script_hashes.clone(), last_tip_hash.clone()) {
        Ok(update) => {
            last_tip_hash = update.tip_hash.clone();
            write_lite_wallet_quic_response(
                &mut send,
                &LiteWalletQuicResponse::Snapshot { update },
            )
            .await?;
        }
        Err(err) => {
            write_lite_wallet_quic_response(
                &mut send,
                &LiteWalletQuicResponse::Error {
                    message: err.to_string(),
                },
            )
            .await?;
            send.finish()?;
            return Ok(());
        }
    }

    let mut state_changes = Box::pin(app.node.watch_state());
    let mut mempool_poll =
        tokio::time::interval(LITE_WALLET_QUIC_MEMPOOL_POLL_INTERVAL);
    loop {
        tokio::select! {
            Some(()) = state_changes.next() => {
                match rpc.lite_wallet_update(script_hashes.clone(), last_tip_hash.clone()) {
                    Ok(update) => {
                        last_tip_hash = update.tip_hash.clone();
                        write_lite_wallet_quic_response(
                            &mut send,
                            &LiteWalletQuicResponse::Confirmed { update },
                        )
                        .await?;
                    }
                    Err(err) => {
                        write_lite_wallet_quic_response(
                            &mut send,
                            &LiteWalletQuicResponse::Error {
                                message: err.to_string(),
                            },
                        )
                        .await?;
                    }
                }
            }
            _ = mempool_poll.tick() => {
                match rpc.lite_wallet_update(script_hashes.clone(), last_tip_hash.clone()) {
                    Ok(update)
                        if !update.mempool_created_utxos.is_empty()
                            || !update.mempool_spent_outpoints.is_empty() =>
                    {
                        write_lite_wallet_quic_response(
                            &mut send,
                            &LiteWalletQuicResponse::Mempool { update },
                        )
                        .await?;
                    }
                    Ok(_) => {}
                    Err(err) => {
                        write_lite_wallet_quic_response(
                            &mut send,
                            &LiteWalletQuicResponse::Error {
                                message: err.to_string(),
                            },
                        )
                        .await?;
                    }
                }
            }
            else => break,
        }
    }
    Ok(())
}

async fn write_lite_wallet_quic_response(
    send: &mut quinn::SendStream,
    response: &LiteWalletQuicResponse,
) -> anyhow::Result<()> {
    // Lite-wallet QUIC currently uses one JSON message per line on a
    // bidirectional stream. The live smoke expects this newline framing until
    // the protocol graduates to a compact binary envelope.
    let mut bytes = serde_json::to_vec(response)?;
    bytes.push(b'\n');
    send.write_all(&bytes).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_script_hash(byte: u8) -> String {
        hex::encode([byte; 32])
    }

    #[test]
    fn lite_wallet_watch_set_rejects_empty() {
        let err = normalize_lite_wallet_script_hashes(Vec::new())
            .expect_err("empty watch set must be rejected");
        assert!(err.to_string().contains("at least one script hash"));
    }

    #[test]
    fn lite_wallet_watch_set_rejects_oversized() {
        let hashes = (0..=LITE_WALLET_MAX_SCRIPT_HASHES)
            .map(|i| valid_script_hash(i as u8))
            .collect();
        let err = normalize_lite_wallet_script_hashes(hashes)
            .expect_err("oversized watch set must be rejected");
        assert!(err.to_string().contains("accepts at most"));
    }

    #[test]
    fn lite_wallet_watch_set_rejects_malformed_script_hash() {
        let err =
            normalize_lite_wallet_script_hashes(vec!["not-hex".to_string()])
                .expect_err("malformed script hash must be rejected");
        assert!(err.to_string().contains("not valid hex"));
    }

    #[test]
    fn lite_wallet_watch_set_rejects_wrong_length_script_hash() {
        let err =
            normalize_lite_wallet_script_hashes(vec![hex::encode([7; 31])])
                .expect_err("short script hash must be rejected");
        assert!(err.to_string().contains("must be 32 bytes"));
    }

    #[test]
    fn lite_wallet_watch_set_normalizes_and_deduplicates() {
        let upper = valid_script_hash(0xaa).to_ascii_uppercase();
        let lower = valid_script_hash(0xaa);
        let watched =
            normalize_lite_wallet_script_hashes(vec![upper, lower]).unwrap();

        assert_eq!(watched.len(), 1);
        assert!(watched.contains(&valid_script_hash(0xaa)));
    }

    #[test]
    fn lite_wallet_cursor_accepts_active_chain_hash() {
        let hash: BlockHash = hex::encode([1; 32]).parse().unwrap();

        ensure_lite_wallet_cursor_on_active_chain(hash, Some(hash)).unwrap();
    }

    #[test]
    fn lite_wallet_cursor_rejects_reorged_hash() {
        let stale_hash: BlockHash = hex::encode([1; 32]).parse().unwrap();
        let active_hash: BlockHash = hex::encode([2; 32]).parse().unwrap();

        let err = ensure_lite_wallet_cursor_on_active_chain(
            stale_hash,
            Some(active_hash),
        )
        .expect_err("stale cursor must force snapshot resync");

        assert!(err.to_string().contains("resync from snapshot"));
        assert!(
            err.to_string()
                .contains("no longer on the active sidechain")
        );
    }

    #[test]
    fn lite_wallet_cursor_rejects_unavailable_height() {
        let stale_hash: BlockHash = hex::encode([1; 32]).parse().unwrap();

        let err = ensure_lite_wallet_cursor_on_active_chain(stale_hash, None)
            .expect_err("unavailable cursor height must force snapshot resync");

        assert!(err.to_string().contains("resync from snapshot"));
        assert!(err.to_string().contains("height is no longer available"));
    }
}
