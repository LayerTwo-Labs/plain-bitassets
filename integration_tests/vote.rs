//! Test an unknown withdrawal event

use std::collections::HashMap;

use bip300301_enforcer_integration_tests::{
    integration_test::{
        activate_sidechain, deposit, fund_enforcer, propose_sidechain,
    },
    setup::{
        setup as setup_enforcer, Mode, Network, PostSetup as EnforcerPostSetup,
        Sidechain as _,
    },
    util::{AbortOnDrop, AsyncTrial},
};
use futures::{
    channel::mpsc, future::BoxFuture, FutureExt as _, StreamExt as _,
};
use plain_bitassets::{
    authorization::{Dst, Signature},
    types::{Address, BitAssetData, BitAssetId, GetAddress as _, Txid},
};
use plain_bitassets_app_rpc_api::RpcClient as _;
use tokio::time::sleep;
use tracing::Instrument as _;

use crate::{
    setup::{Init, PostSetup},
    util::BinPaths,
};

#[derive(Debug)]
struct BitAssetsNodes {
    /// Sidechain process that will be issuing a BitAsset
    issuer: PostSetup,
    /// Sidechain process that will be voting
    voter_0: PostSetup,
    /// Sidechain process that will be voting
    voter_1: PostSetup,
}

impl BitAssetsNodes {
    async fn setup(
        bin_paths: &BinPaths,
        res_tx: mpsc::UnboundedSender<anyhow::Result<()>>,
        enforcer_post_setup: &EnforcerPostSetup,
    ) -> anyhow::Result<Self> {
        // Initialize a single node
        let setup_single = |suffix: &str| {
            PostSetup::setup(
                Init {
                    bitassets_app: bin_paths.bitassets.clone(),
                    data_dir_suffix: Some(suffix.to_owned()),
                },
                enforcer_post_setup,
                res_tx.clone(),
            )
        };
        let res = Self {
            issuer: setup_single("issuer").await?,
            voter_0: setup_single("voter_0").await?,
            voter_1: setup_single("voter_1").await?,
        };
        tracing::debug!(
            issuer_addr = %res.issuer.net_addr(),
            voter_0_addr = %res.voter_0.net_addr(),
            "Connecting issuer to voter 0");
        let () = res
            .issuer
            .rpc_client
            .connect_peer(res.voter_0.net_addr().into())
            .await?;
        tracing::debug!(
            issuer_addr = %res.issuer.net_addr(),
            voter_1_addr = %res.voter_1.net_addr(),
            "Connecting issuer to voter 1");
        let () = res
            .issuer
            .rpc_client
            .connect_peer(res.voter_1.net_addr().into())
            .await?;
        Ok(res)
    }
}

const DEPOSIT_AMOUNT: bitcoin::Amount = bitcoin::Amount::from_sat(21_000_000);
const DEPOSIT_FEE: bitcoin::Amount = bitcoin::Amount::from_sat(1_000_000);

/// Initial setup for the test
async fn setup(
    bin_paths: &BinPaths,
    res_tx: mpsc::UnboundedSender<anyhow::Result<()>>,
) -> anyhow::Result<(EnforcerPostSetup, BitAssetsNodes)> {
    let mut enforcer_post_setup = setup_enforcer(
        &bin_paths.others,
        Network::Regtest,
        Mode::Mempool,
        res_tx.clone(),
    )
    .await?;
    let () = propose_sidechain::<PostSetup>(&mut enforcer_post_setup).await?;
    tracing::info!("Proposed sidechain successfully");
    let () = activate_sidechain::<PostSetup>(&mut enforcer_post_setup).await?;
    tracing::info!("Activated sidechain successfully");
    let () = fund_enforcer::<PostSetup>(&mut enforcer_post_setup).await?;
    let mut bitassets_nodes =
        BitAssetsNodes::setup(bin_paths, res_tx, &enforcer_post_setup).await?;
    let issuer_deposit_address =
        bitassets_nodes.issuer.get_deposit_address().await?;
    let () = deposit(
        &mut enforcer_post_setup,
        &mut bitassets_nodes.issuer,
        &issuer_deposit_address,
        DEPOSIT_AMOUNT,
        DEPOSIT_FEE,
    )
    .await?;
    tracing::info!("Deposited to sidechain successfully");
    Ok((enforcer_post_setup, bitassets_nodes))
}

const PLAINTEXT_NAME: &str = "test-bitasset";
const VOTE_CALL_MSG: &str = "test vote call";
const VOTE_YES_MSG: &str = "test vote call YES";
const VOTE_NO_MSG: &str = "test vote call NO";
const INITIAL_SUPPLY: u64 = 100;
/// BitAssets allocated to voter 0
const VOTER_ALLOCATION_0: u64 = 60;
/// BitAssets allocated to voter 1
const VOTER_ALLOCATION_1: u64 = 40;

async fn vote_task(
    bin_paths: BinPaths,
    res_tx: mpsc::UnboundedSender<anyhow::Result<()>>,
) -> anyhow::Result<()> {
    let (mut enforcer_post_setup, bitassets_nodes) =
        setup(&bin_paths, res_tx.clone()).await?;
    tracing::info!("Reserving BitAsset");
    let _: Txid = bitassets_nodes
        .issuer
        .rpc_client
        .reserve_bitasset(PLAINTEXT_NAME.to_owned())
        .await?;
    let bitasset_id =
        BitAssetId(blake3::hash(PLAINTEXT_NAME.as_bytes()).into());
    bitassets_nodes
        .issuer
        .bmm_single(&mut enforcer_post_setup)
        .await?;
    tracing::info!("Generating issuer verifying key");
    let issuer_vk = bitassets_nodes
        .issuer
        .rpc_client
        .get_new_verifying_key()
        .await?;
    tracing::info!("Registering BitAsset");
    let _: Txid = bitassets_nodes
        .issuer
        .rpc_client
        .register_bitasset(
            PLAINTEXT_NAME.to_owned(),
            INITIAL_SUPPLY,
            Some(BitAssetData {
                signing_pubkey: Some(issuer_vk),
                ..Default::default()
            }),
        )
        .await?;
    bitassets_nodes
        .issuer
        .bmm_single(&mut enforcer_post_setup)
        .await?;
    tracing::info!("Sending BitAsset to voters");
    let voter_addr_0 =
        bitassets_nodes.voter_0.rpc_client.get_new_address().await?;
    let voter_addr_1 =
        bitassets_nodes.voter_1.rpc_client.get_new_address().await?;
    let _: Txid = bitassets_nodes
        .issuer
        .rpc_client
        .transfer_bitasset(
            voter_addr_0,
            bitasset_id,
            VOTER_ALLOCATION_0,
            0,
            None,
        )
        .await?;
    bitassets_nodes
        .issuer
        .bmm_single(&mut enforcer_post_setup)
        .await?;
    let _: Txid = bitassets_nodes
        .issuer
        .rpc_client
        .transfer_bitasset(
            voter_addr_1,
            bitasset_id,
            VOTER_ALLOCATION_1,
            0,
            None,
        )
        .await?;
    bitassets_nodes
        .issuer
        .bmm_single(&mut enforcer_post_setup)
        .await?;
    tracing::info!("Signing vote call message");
    let vote_call_msg_sig: Signature = bitassets_nodes
        .issuer
        .rpc_client
        .sign_arbitrary_msg(issuer_vk, VOTE_CALL_MSG.to_owned())
        .await?;
    tracing::info!("Verifying vote call message signature");
    for voter in [&bitassets_nodes.voter_0, &bitassets_nodes.voter_1] {
        anyhow::ensure!(
            voter
                .rpc_client
                .verify_signature(
                    vote_call_msg_sig,
                    issuer_vk,
                    Dst::Arbitrary,
                    VOTE_CALL_MSG.to_owned()
                )
                .await?
        )
    }
    tracing::info!("Taking snapshot of BitAsset holders");
    let vote_weights: HashMap<Address, u64> = {
        let mut weights = HashMap::new();
        let utxos = bitassets_nodes.issuer.rpc_client.list_utxos().await?;
        for utxo in utxos {
            if let Some((asset_id, value)) = utxo.output.bitasset_value() {
                if asset_id == bitasset_id {
                    *weights.entry(utxo.output.address).or_default() += value;
                }
            }
        }
        weights
    };
    anyhow::ensure!(vote_weights.len() >= 2);
    tracing::info!("Signing votes");
    let vote_auth_0 = bitassets_nodes
        .voter_0
        .rpc_client
        .sign_arbitrary_msg_as_addr(voter_addr_0, VOTE_YES_MSG.to_owned())
        .await?;
    let vote_auth_1 = bitassets_nodes
        .voter_1
        .rpc_client
        .sign_arbitrary_msg_as_addr(voter_addr_1, VOTE_NO_MSG.to_owned())
        .await?;
    tracing::info!("Verifying votes");
    let (total_yes, total_no) = {
        let (mut total_yes, mut total_no) = (0, 0);
        let mut vote_weights = vote_weights;
        for vote_auth in [vote_auth_0, vote_auth_1] {
            let voter_addr = vote_auth.get_address();
            if bitassets_nodes
                .issuer
                .rpc_client
                .verify_signature(
                    vote_auth.signature,
                    vote_auth.verifying_key,
                    Dst::Arbitrary,
                    VOTE_YES_MSG.to_owned(),
                )
                .await?
            {
                if let Some(weight) = vote_weights.remove(&voter_addr) {
                    total_yes += weight;
                }
            } else if bitassets_nodes
                .issuer
                .rpc_client
                .verify_signature(
                    vote_auth.signature,
                    vote_auth.verifying_key,
                    Dst::Arbitrary,
                    VOTE_NO_MSG.to_owned(),
                )
                .await?
            {
                if let Some(weight) = vote_weights.remove(&voter_addr) {
                    total_no += weight;
                }
            }
        }
        (total_yes, total_no)
    };
    anyhow::ensure!(total_yes == VOTER_ALLOCATION_0);
    anyhow::ensure!(total_no == VOTER_ALLOCATION_1);
    // Cleanup
    {
        drop(bitassets_nodes);
        tracing::info!(
            "Removing {}",
            enforcer_post_setup.out_dir.path().display()
        );
        drop(enforcer_post_setup.tasks);
        // Wait for tasks to die
        sleep(std::time::Duration::from_secs(1)).await;
        enforcer_post_setup.out_dir.cleanup()?;
    }
    Ok(())
}

async fn vote(bin_paths: BinPaths) -> anyhow::Result<()> {
    let (res_tx, mut res_rx) = mpsc::unbounded();
    let _test_task: AbortOnDrop<()> = tokio::task::spawn({
        let res_tx = res_tx.clone();
        async move {
            let res = vote_task(bin_paths, res_tx.clone()).await;
            let _send_err: Result<(), _> = res_tx.unbounded_send(res);
        }
        .in_current_span()
    })
    .into();
    res_rx.next().await.ok_or_else(|| {
        anyhow::anyhow!("Unexpected end of test task result stream")
    })?
}

pub fn vote_trial(
    bin_paths: BinPaths,
) -> AsyncTrial<BoxFuture<'static, anyhow::Result<()>>> {
    AsyncTrial::new("vote", vote(bin_paths).boxed())
}
