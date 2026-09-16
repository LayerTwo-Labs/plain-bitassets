//! Regression tests: zero-input transactions and outpoint-key re-creation.
//!
//! ## The defect
//!
//! There was no input-count rule: a transaction with NO inputs passed
//! `State::validate_transaction` (0 authorizations == 0 inputs) and
//! `block::prevalidate` (the same-body double-spend check only looks at input
//! keys). Its txid is a pure function of its contents, so the identical tx
//! could be mined again later, and `block::connect_prevalidated` re-`put`
//! `utxos[Regular { txid, vout }]` without checking for an existing row, while
//! `block::disconnect_tip` deletes by key and errors `NoUtxo` on a missing
//! row. With Z = {inputs: [], outputs: [Bitcoin(0)]}: Z validated; Z connected
//! at h1 and again at h2; disconnecting h2 deleted the output still owned by
//! h1 (a node that saw h2 then diverges from one that did not on any later
//! spend of it); disconnecting h1 afterwards failed `NoUtxo`; a body carrying
//! Z twice connected but could never be disconnected; a spent (Z, 0) was
//! resurrected by re-mining Z. A zero-input tx carries no authorization, so
//! anyone can re-broadcast it.
//!
//! The same re-creation exists WITHOUT zero-input txs for COINBASE keys:
//! `Coinbase { merkle_root, vout }` with `merkle_root = H(coinbase, txs)` (no
//! parent commitment), so two blocks with no transactions and the same
//! zero-value coinbase output (coinbase value 0 <= fees 0) re-create the same
//! key (T4/T4b below).
//!
//! ## The fix (consensus change)
//!
//! - `State::validate_filled_transaction`: a transaction must have >= 1 input
//!   -> `Error::NoInputs` (T1, T2, T8).
//! - `State::validate_filled_transaction`: a tx output key may not already
//!   exist in `utxos` or `stxos` -> `Error::OutPointAlreadyExists`. This is
//!   independent of output content and tx data, so it covers BitAsset, AMM and
//!   Dutch auction outputs as well (T7).
//! - `block::prevalidate` (and `block::validate`): no txid twice in one body
//!   -> `Error::DuplicateTransaction`; coinbase output keys may not already
//!   exist in `utxos` or `stxos` -> `Error::OutPointAlreadyExists`.
//!
//! The output-key and body checks alone already reject every re-creation;
//! the input-count rule additionally rejects zero-input transactions outright.
//!
//! `validate_filled_transaction` is shared by the mempool path
//! (`validate_transaction`: `submit_transaction`, peer `handle_push_tx`, the
//! miner's `get_transactions`) and the block path (`apply_block` =
//! `prevalidate` + `connect_prevalidated`).
//!
//! Every test drives the live functions and asserts the exact reject reason.
//! Reasons are compared as strings so the suite also compiles against the
//! unfixed code (where it fails).

use std::collections::HashMap;

use bitcoin::hashes::Hash as _;
use ed25519_dalek::SigningKey;

use crate::{
    authorization,
    state::{
        Error, State,
        test::{bitcoin_filled_output, fresh_state},
    },
    types::{
        Address, AssetId, AuthorizedTransaction, BitAssetDataUpdates,
        BitAssetId, BlockHash, Body, DutchAuctionParams, FilledOutput, Header,
        InPoint, OutPoint, OutPointKey, Output, OutputContent, SpentOutput,
        Transaction, TxData, Txid, Update, VerifyingKey,
        WithdrawalOutputContent,
    },
};

const ADDR_A: Address = Address([0xAA; 20]);

// ---------------------------------------------------------------------------
// Exact reject reasons.
// ---------------------------------------------------------------------------

fn no_inputs_reason(txid: Txid) -> String {
    format!("transaction {txid} has no inputs")
}

fn outpoint_exists_reason(outpoint: OutPoint) -> String {
    format!("output {outpoint} already exists")
}

fn duplicate_tx_reason(txid: Txid) -> String {
    format!("block body contains transaction {txid} more than once")
}

/// Require `res` to be an error whose `Display` is exactly `expected`.
fn expect_reject<T: std::fmt::Debug>(
    ctx: &str,
    res: Result<T, Error>,
    expected: &str,
) -> anyhow::Result<()> {
    match res {
        Err(err) if err.to_string() == expected => Ok(()),
        Err(err) => anyhow::bail!(
            "{ctx}: wrong reject reason\n  got:      {err}\n  expected: {expected}"
        ),
        Ok(ok) => anyhow::bail!(
            "{ctx}: accepted ({ok:?}), expected rejection: {expected}"
        ),
    }
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

/// Z = {inputs: [], outputs: [Bitcoin(sats) -> A]}
fn zero_input_tx(sats: u64) -> Transaction {
    Transaction::new(
        Vec::new(),
        vec![bitcoin_filled_output(ADDR_A, sats).into()],
    )
}

fn unauthorized(tx: Transaction) -> AuthorizedTransaction {
    AuthorizedTransaction {
        transaction: tx,
        authorizations: Vec::new(),
    }
}

fn make_block(
    prev_side_hash: Option<BlockHash>,
    txs: Vec<AuthorizedTransaction>,
    coinbase: Vec<Output>,
) -> (Header, Body) {
    let body = Body::new(txs, coinbase);
    let header = Header {
        merkle_root: Body::compute_merkle_root(
            &body.coinbase,
            &body.transactions,
        ),
        prev_side_hash,
        prev_main_hash: bitcoin::BlockHash::from_byte_array([0; 32]),
    };
    (header, body)
}

/// Apply via the live path; commit only on success (a failed rwtxn is
/// dropped = aborted, exactly as the node does).
fn apply(
    env: &sneed::Env,
    state: &State,
    header: &Header,
    body: &Body,
) -> Result<(), Error> {
    let mut rwtxn = env.write_txn().expect("write txn");
    state.apply_block(&mut rwtxn, header, body)?;
    rwtxn.commit().expect("commit");
    Ok(())
}

fn disconnect(
    env: &sneed::Env,
    state: &State,
    header: &Header,
    body: &Body,
) -> Result<(), Error> {
    let mut rwtxn = env.write_txn().expect("write txn");
    state.disconnect_tip(&mut rwtxn, header, body)?;
    rwtxn.commit().expect("commit");
    Ok(())
}

#[derive(Debug, PartialEq)]
struct Snapshot {
    tip: Option<BlockHash>,
    height: Option<u32>,
    utxos: HashMap<OutPoint, FilledOutput>,
    stxos: HashMap<OutPoint, SpentOutput>,
}

fn snapshot(env: &sneed::Env, state: &State) -> Snapshot {
    use fallible_iterator::FallibleIterator as _;
    let rotxn = env.read_txn().expect("read txn");
    let stxos = state
        .stxos
        .iter(&rotxn)
        .expect("stxos iter")
        .map(|(key, spent): (OutPointKey, SpentOutput)| {
            Ok((key.to_outpoint(), spent))
        })
        .collect()
        .expect("stxos collect");
    Snapshot {
        tip: state.try_get_tip(&rotxn).expect("tip"),
        height: state.try_get_height(&rotxn).expect("height"),
        utxos: state.get_utxos(&rotxn).expect("utxos"),
        stxos,
    }
}

fn validate(
    env: &sneed::Env,
    state: &State,
    tx: &AuthorizedTransaction,
) -> Result<bitcoin::Amount, Error> {
    let rotxn = env.read_txn().expect("read txn");
    state.validate_transaction(&rotxn, tx)
}

/// A key we control and its address.
fn owner(seed: u8) -> (SigningKey, Address) {
    let signing_key = SigningKey::from_bytes(&[seed; 32]);
    let vk: VerifyingKey = signing_key.verifying_key().into();
    let address = authorization::get_address(&vk);
    (signing_key, address)
}

/// Fund `address` with a deposit-origin (money-path) Bitcoin UTXO, written
/// directly the way `two_way_peg_data::connect` does.
fn fund_deposit(
    env: &sneed::Env,
    state: &State,
    address: Address,
    tag: u8,
    sats: u64,
) -> OutPoint {
    let outpoint = OutPoint::Deposit(bitcoin::OutPoint {
        txid: bitcoin::Txid::from_byte_array([tag; 32]),
        vout: 0,
    });
    let mut rwtxn = env.write_txn().expect("write txn");
    state
        .utxos
        .put(
            &mut rwtxn,
            &OutPointKey::from_outpoint(&outpoint),
            &bitcoin_filled_output(address, sats),
        )
        .expect("put");
    rwtxn.commit().expect("commit");
    outpoint
}

// ---------------------------------------------------------------------------
// T1 — the input-count rule at mempool: every zero-input shape is rejected with NoInputs.
// ---------------------------------------------------------------------------

#[test]
fn zero_input_t1_mempool_rejects_zero_input_txs() -> anyhow::Result<()> {
    let (_tmp, env, state) = fresh_state("zero_input_t1")?;

    // (a) Bitcoin(0) output, no inputs, no authorizations.
    let z = zero_input_tx(0);
    expect_reject(
        "T1a zero-input Bitcoin(0): validate_transaction",
        validate(&env, &state, &unauthorized(z.clone())),
        &no_inputs_reason(z.txid()),
    )?;
    // The authorization layer still accepts 0 == 0: the rule lives in state,
    // on the shared mempool/connect choke point, not in authorization.
    let res =
        authorization::verify_authorized_transaction(&unauthorized(z.clone()));
    anyhow::ensure!(res.is_ok(), "authorization layer unchanged: {res:?}");

    // (a'') a memo makes a DIFFERENT txid: still rejected.
    let mut z_memo = zero_input_tx(0);
    z_memo.memo = b"anything".to_vec();
    expect_reject(
        "T1a'' zero-input Bitcoin(0)+memo",
        validate(&env, &state, &unauthorized(z_memo.clone())),
        &no_inputs_reason(z_memo.txid()),
    )?;

    // (b) non-zero output value: rejected before value conservation.
    let z1 = zero_input_tx(1);
    expect_reject(
        "T1b zero-input Bitcoin(1)",
        validate(&env, &state, &unauthorized(z1.clone())),
        &no_inputs_reason(z1.txid()),
    )?;

    // (c) zero-value WITHDRAWAL output from nothing.
    let main_address = {
        let pkh = bitcoin::PubkeyHash::hash(b"zero-input withdrawal");
        bitcoin::Address::p2pkh(pkh, bitcoin::NetworkKind::Test)
            .into_unchecked()
    };
    let zw = Transaction::new(
        Vec::new(),
        vec![Output::new(
            ADDR_A,
            OutputContent::Withdrawal(WithdrawalOutputContent {
                value: bitcoin::Amount::ZERO,
                main_fee: bitcoin::Amount::ZERO,
                main_address,
            }),
        )],
    );
    expect_reject(
        "T1c zero-input Withdrawal(0,0)",
        validate(&env, &state, &unauthorized(zw.clone())),
        &no_inputs_reason(zw.txid()),
    )?;

    // (d) zero-input, ZERO-output tx (creates no keys, so only the input-count
    // rule stops its unbounded free replay).
    let empty = Transaction::new(Vec::new(), Vec::new());
    expect_reject(
        "T1d zero-input zero-output",
        validate(&env, &state, &unauthorized(empty.clone())),
        &no_inputs_reason(empty.txid()),
    )?;

    // Positive control: a signed 1-input spend of a deposit validates.
    let (sk, addr) = owner(3);
    let dep = fund_deposit(&env, &state, addr, 1, 1000);
    let t = authorization::authorize(
        &[(addr, &sk)],
        Transaction::new(
            vec![dep],
            vec![bitcoin_filled_output(addr, 900).into()],
        ),
    )?;
    let res = validate(&env, &state, &t);
    anyhow::ensure!(
        matches!(res, Ok(fee) if fee == bitcoin::Amount::from_sat(100)),
        "money-path control must validate: {res:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// T2 — the input-count rule at block connect: a body carrying a zero-input tx is rejected, and
// nothing is written.
// ---------------------------------------------------------------------------

#[test]
fn zero_input_t2_block_with_zero_input_tx_rejected() -> anyhow::Result<()> {
    let (_tmp, env, state) = fresh_state("zero_input_t2")?;
    let z = zero_input_tx(0);

    let g = make_block(None, Vec::new(), Vec::new());
    apply(&env, &state, &g.0, &g.1)?;
    let pre = snapshot(&env, &state);

    // (a) Z alone.
    let h1 =
        make_block(Some(g.0.hash()), vec![unauthorized(z.clone())], vec![]);
    expect_reject(
        "T2a apply_block(body {Z})",
        apply(&env, &state, &h1.0, &h1.1),
        &no_inputs_reason(z.txid()),
    )?;
    anyhow::ensure!(
        snapshot(&env, &state) == pre,
        "rejected block wrote state"
    );

    // (b) Z after a valid signed spend in the same body.
    let (sk, addr) = owner(5);
    let dep = fund_deposit(&env, &state, addr, 2, 1000);
    let pre = snapshot(&env, &state);
    let t = authorization::authorize(
        &[(addr, &sk)],
        Transaction::new(
            vec![dep],
            vec![bitcoin_filled_output(addr, 1000).into()],
        ),
    )?;
    let h1b = make_block(
        Some(g.0.hash()),
        vec![t.clone(), unauthorized(z.clone())],
        vec![],
    );
    expect_reject(
        "T2b apply_block(body {T, Z})",
        apply(&env, &state, &h1b.0, &h1b.1),
        &no_inputs_reason(z.txid()),
    )?;
    anyhow::ensure!(
        snapshot(&env, &state) == pre,
        "rejected block wrote state"
    );

    // Control: the same body without Z connects.
    let h1c = make_block(Some(g.0.hash()), vec![t], vec![]);
    apply(&env, &state, &h1c.0, &h1c.1)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// T3 — the output-key check for a signed transaction. An honest transaction
// that spends inputs cannot create an existing key, so the pre-existing row is
// seeded directly.
// ---------------------------------------------------------------------------

type T3Setup = (
    temp_dir::TempDir,
    sneed::Env,
    State,
    AuthorizedTransaction,
    OutPoint,
);

fn t3_setup(name: &str) -> anyhow::Result<T3Setup> {
    let (tmp, env, state) = fresh_state(name)?;
    let g = make_block(None, Vec::new(), Vec::new());
    apply(&env, &state, &g.0, &g.1)?;
    let (sk, addr) = owner(9);
    let dep = fund_deposit(&env, &state, addr, 3, 1000);
    let t = authorization::authorize(
        &[(addr, &sk)],
        Transaction::new(
            vec![dep],
            vec![bitcoin_filled_output(addr, 1000).into()],
        ),
    )?;
    let created = OutPoint::Regular {
        txid: t.transaction.txid(),
        vout: 0,
    };
    Ok((tmp, env, state, t, created))
}

#[test]
fn zero_input_t3_tx_output_key_already_unspent_rejected() -> anyhow::Result<()>
{
    let (_tmp, env, state, t, created) = t3_setup("zero_input_t3")?;
    anyhow::ensure!(validate(&env, &state, &t).is_ok(), "control before seed");
    {
        let mut rwtxn = env.write_txn()?;
        state.utxos.put(
            &mut rwtxn,
            &OutPointKey::from_outpoint(&created),
            &bitcoin_filled_output(ADDR_A, 0),
        )?;
        rwtxn.commit()?;
    }
    let expected = outpoint_exists_reason(created);
    expect_reject(
        "T3 validate_transaction(T, (T,0) already in utxos)",
        validate(&env, &state, &t),
        &expected,
    )?;
    let pre = snapshot(&env, &state);
    let tip = pre.tip;
    let b = make_block(tip, vec![t], vec![]);
    expect_reject(
        "T3 apply_block({T}, (T,0) already in utxos)",
        apply(&env, &state, &b.0, &b.1),
        &expected,
    )?;
    anyhow::ensure!(
        snapshot(&env, &state) == pre,
        "rejected block wrote state"
    );
    Ok(())
}

#[test]
fn zero_input_t3b_tx_output_key_already_spent_rejected() -> anyhow::Result<()> {
    let (_tmp, env, state, t, created) = t3_setup("zero_input_t3b")?;
    {
        let mut rwtxn = env.write_txn()?;
        state.stxos.put(
            &mut rwtxn,
            &OutPointKey::from_outpoint(&created),
            &SpentOutput {
                output: bitcoin_filled_output(ADDR_A, 0),
                inpoint: InPoint::Regular {
                    txid: Txid([0x55; 32]),
                    vin: 0,
                },
            },
        )?;
        rwtxn.commit()?;
    }
    let expected = outpoint_exists_reason(created);
    expect_reject(
        "T3b validate_transaction(T, (T,0) already in stxos)",
        validate(&env, &state, &t),
        &expected,
    )?;
    let pre = snapshot(&env, &state);
    let b = make_block(pre.tip, vec![t], vec![]);
    expect_reject(
        "T3b apply_block({T}, (T,0) already in stxos)",
        apply(&env, &state, &b.0, &b.1),
        &expected,
    )?;
    anyhow::ensure!(
        snapshot(&env, &state) == pre,
        "rejected block wrote state"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// T4 — coinbase keys: re-creation without any zero-input transaction.
// ---------------------------------------------------------------------------

/// g = {coinbase: [Bitcoin(0) -> A], txs: []}; h1 = an identical body on top of
/// g has the same merkle root, hence the same `Coinbase{root,0}` key.
#[test]
fn zero_input_t4_coinbase_key_recreation_rejected() -> anyhow::Result<()> {
    let (_tmp, env, state) = fresh_state("zero_input_t4")?;
    let empty = snapshot(&env, &state);
    let coinbase = vec![Output::from(bitcoin_filled_output(ADDR_A, 0))];

    let g = make_block(None, Vec::new(), coinbase.clone());
    apply(&env, &state, &g.0, &g.1)?;
    let after_g = snapshot(&env, &state);
    let cb = OutPoint::Coinbase {
        merkle_root: g.0.merkle_root,
        vout: 0,
    };
    anyhow::ensure!(after_g.utxos.contains_key(&cb));

    let h1 = make_block(Some(g.0.hash()), Vec::new(), coinbase);
    anyhow::ensure!(h1.0.merkle_root == g.0.merkle_root);
    {
        let rotxn = env.read_txn()?;
        expect_reject(
            "T4 prevalidate_block(h1, identical coinbase-only body)",
            state
                .prevalidate_block(&rotxn, &h1.0, &h1.1)
                .map(|p| p.next_height),
            &outpoint_exists_reason(cb),
        )?;
    }
    expect_reject(
        "T4 apply_block(h1)",
        apply(&env, &state, &h1.0, &h1.1),
        &outpoint_exists_reason(cb),
    )?;
    anyhow::ensure!(snapshot(&env, &state) == after_g);

    // Undo stays exact: disconnecting g returns to the empty state.
    disconnect(&env, &state, &g.0, &g.1)?;
    anyhow::ensure!(snapshot(&env, &state) == empty, "undo not byte-exact");
    Ok(())
}

/// The spent variant (formerly: resurrection + spend replay): a spent
/// coinbase key (row in `stxos`) cannot be re-created either, and the
/// connect^3 / disconnect^2 round trip is byte-exact.
#[test]
fn zero_input_t4b_spent_coinbase_key_recreation_rejected() -> anyhow::Result<()>
{
    let (_tmp, env, state) = fresh_state("zero_input_t4b")?;
    let empty = snapshot(&env, &state);
    let (sk, addr) = owner(42);
    let coinbase = vec![Output::from(bitcoin_filled_output(addr, 0))];

    let g = make_block(None, Vec::new(), coinbase.clone());
    apply(&env, &state, &g.0, &g.1)?;
    let cb = OutPoint::Coinbase {
        merkle_root: g.0.merkle_root,
        vout: 0,
    };
    let s = authorization::authorize(
        &[(addr, &sk)],
        Transaction::new(vec![cb], vec![bitcoin_filled_output(addr, 0).into()]),
    )?;
    let h1 = make_block(Some(g.0.hash()), vec![s.clone()], Vec::new());
    apply(&env, &state, &h1.0, &h1.1)?;
    let spent = snapshot(&env, &state);
    anyhow::ensure!(
        !spent.utxos.contains_key(&cb) && spent.stxos.contains_key(&cb)
    );

    let h2 = make_block(Some(h1.0.hash()), Vec::new(), coinbase);
    anyhow::ensure!(h2.0.merkle_root == g.0.merkle_root);
    expect_reject(
        "T4b apply_block(h2 re-creates spent coinbase key)",
        apply(&env, &state, &h2.0, &h2.1),
        &outpoint_exists_reason(cb),
    )?;
    anyhow::ensure!(snapshot(&env, &state) == spent);

    // The spend S cannot replay either (its input is gone).
    let res = validate(&env, &state, &s);
    anyhow::ensure!(matches!(res, Err(Error::NoUtxo(_))), "{res:?}");

    disconnect(&env, &state, &h1.0, &h1.1)?;
    disconnect(&env, &state, &g.0, &g.1)?;
    anyhow::ensure!(snapshot(&env, &state) == empty, "undo not byte-exact");
    Ok(())
}

// ---------------------------------------------------------------------------
// T5 — body level: the same txid twice in ONE body.
// ---------------------------------------------------------------------------

#[test]
fn zero_input_t5_duplicate_txid_in_one_body_rejected() -> anyhow::Result<()> {
    let (_tmp, env, state) = fresh_state("zero_input_t5")?;
    let g = make_block(None, Vec::new(), Vec::new());
    apply(&env, &state, &g.0, &g.1)?;

    // (a) Z twice. Transaction checks run against the pre-block state, so
    // without the body rule both copies would pass the output-key check.
    // DuplicateTransaction is reported before NoInputs.
    let z = zero_input_tx(0);
    let pre = snapshot(&env, &state);
    let b = make_block(
        Some(g.0.hash()),
        vec![unauthorized(z.clone()), unauthorized(z.clone())],
        vec![],
    );
    {
        let rotxn = env.read_txn()?;
        expect_reject(
            "T5a prevalidate_block(body with Z twice)",
            state
                .prevalidate_block(&rotxn, &b.0, &b.1)
                .map(|p| p.next_height),
            &duplicate_tx_reason(z.txid()),
        )?;
    }
    expect_reject(
        "T5a apply_block(body with Z twice)",
        apply(&env, &state, &b.0, &b.1),
        &duplicate_tx_reason(z.txid()),
    )?;
    anyhow::ensure!(snapshot(&env, &state) == pre);

    // (b) a signed 1-input tx twice: DuplicateTransaction, not UtxoDoubleSpent.
    let (sk, addr) = owner(11);
    let dep = fund_deposit(&env, &state, addr, 4, 1000);
    let pre = snapshot(&env, &state);
    let t = authorization::authorize(
        &[(addr, &sk)],
        Transaction::new(
            vec![dep],
            vec![bitcoin_filled_output(addr, 1000).into()],
        ),
    )?;
    let b = make_block(Some(g.0.hash()), vec![t.clone(), t.clone()], vec![]);
    expect_reject(
        "T5b apply_block(body with T twice)",
        apply(&env, &state, &b.0, &b.1),
        &duplicate_tx_reason(t.transaction.txid()),
    )?;
    anyhow::ensure!(snapshot(&env, &state) == pre);
    Ok(())
}

// ---------------------------------------------------------------------------
// T6 — money-path control: deposit spend -> spend of its output -> re-mining
// the first tx is refused -> disconnect^2 is byte-exact (deposit restored).
// ---------------------------------------------------------------------------

#[test]
fn zero_input_t6_money_path_round_trip() -> anyhow::Result<()> {
    let (_tmp, env, state) = fresh_state("zero_input_t6")?;
    let (sk, addr) = owner(21);
    let dep = fund_deposit(&env, &state, addr, 6, 5000);
    let funded = snapshot(&env, &state);

    let t = authorization::authorize(
        &[(addr, &sk)],
        Transaction::new(
            vec![dep],
            vec![bitcoin_filled_output(addr, 5000).into()],
        ),
    )?;
    anyhow::ensure!(validate(&env, &state, &t).is_ok());
    let g = make_block(None, vec![t.clone()], Vec::new());
    apply(&env, &state, &g.0, &g.1)?;
    let tp = OutPoint::Regular {
        txid: t.transaction.txid(),
        vout: 0,
    };
    let s = authorization::authorize(
        &[(addr, &sk)],
        Transaction::new(
            vec![tp],
            vec![bitcoin_filled_output(addr, 4000).into()],
        ),
    )?;
    // coinbase claims S's 1000-sat fee
    let h1 = make_block(
        Some(g.0.hash()),
        vec![s],
        vec![Output::from(bitcoin_filled_output(addr, 1000))],
    );
    apply(&env, &state, &h1.0, &h1.1)?;

    // T cannot be re-mined: its deposit input is spent.
    let h2 = make_block(Some(h1.0.hash()), vec![t], Vec::new());
    let res = apply(&env, &state, &h2.0, &h2.1);
    anyhow::ensure!(matches!(res, Err(Error::NoUtxo(_))), "{res:?}");

    disconnect(&env, &state, &h1.0, &h1.1)?;
    disconnect(&env, &state, &g.0, &g.1)?;
    anyhow::ensure!(snapshot(&env, &state) == funded, "undo not byte-exact");
    Ok(())
}

// ---------------------------------------------------------------------------
// T7 — the output-key check covers every output content kind and every
// BitAsset / AMM / Dutch auction tx data variant that creates outputs. The
// check keys `Regular { txid, vout }` over `transaction.outputs`, independent
// of content and data, and runs before the content-specific checks, so each
// tx below only needs a valid, authorized input to reach it.
// ---------------------------------------------------------------------------

/// Where the pre-existing row for the created outpoint is seeded.
#[derive(Clone, Copy)]
enum Seed {
    Unspent,
    Spent,
}

/// Build {inputs: [signed deposit], outputs: [content -> owner], data}, seed
/// a row at its `Regular { txid, 0 }` key, and require the exact
/// `OutPointAlreadyExists` reason at mempool validation and at block connect,
/// with no state written.
fn assert_created_output_key_checked(
    name: &str,
    tag: u8,
    content: OutputContent,
    data: Option<TxData>,
    seed: Seed,
) -> anyhow::Result<()> {
    let (_tmp, env, state) = fresh_state(name)?;
    let (sk, addr) = owner(tag);
    let dep = fund_deposit(&env, &state, addr, tag, 1000);
    let tx = Transaction {
        inputs: vec![dep],
        outputs: vec![Output::new(addr, content)],
        memo: Vec::new(),
        data,
    };
    let t = authorization::authorize(&[(addr, &sk)], tx)?;
    let created = OutPoint::Regular {
        txid: t.transaction.txid(),
        vout: 0,
    };
    let expected = outpoint_exists_reason(created);

    // Control: without the seeded row the tx is not rejected for this reason.
    let res = validate(&env, &state, &t);
    anyhow::ensure!(
        !matches!(&res, Err(err) if err.to_string() == expected),
        "{name}: rejected as re-created before seeding: {res:?}"
    );

    {
        let mut rwtxn = env.write_txn()?;
        let key = OutPointKey::from_outpoint(&created);
        match seed {
            Seed::Unspent => state.utxos.put(
                &mut rwtxn,
                &key,
                &bitcoin_filled_output(ADDR_A, 0),
            )?,
            Seed::Spent => state.stxos.put(
                &mut rwtxn,
                &key,
                &SpentOutput {
                    output: bitcoin_filled_output(ADDR_A, 0),
                    inpoint: InPoint::Regular {
                        txid: Txid([0x66; 32]),
                        vin: 0,
                    },
                },
            )?,
        }
        rwtxn.commit()?;
    }

    expect_reject(
        &format!("{name}: validate_transaction"),
        validate(&env, &state, &t),
        &expected,
    )?;
    let pre = snapshot(&env, &state);
    let b = make_block(pre.tip, vec![t], vec![]);
    expect_reject(
        &format!("{name}: apply_block"),
        apply(&env, &state, &b.0, &b.1),
        &expected,
    )?;
    anyhow::ensure!(
        snapshot(&env, &state) == pre,
        "{name}: rejected block wrote state"
    );
    Ok(())
}

fn some_bitasset() -> BitAssetId {
    BitAssetId([0x42; 32])
}

#[test]
fn zero_input_t7a_bitasset_reservation_output_key_checked() -> anyhow::Result<()>
{
    assert_created_output_key_checked(
        "zero_input_t7a",
        71,
        OutputContent::BitAssetReservation,
        Some(TxData::BitAssetReservation {
            commitment: [0x07; 32],
        }),
        Seed::Unspent,
    )
}

#[test]
fn zero_input_t7b_bitasset_mint_output_key_checked() -> anyhow::Result<()> {
    assert_created_output_key_checked(
        "zero_input_t7b",
        72,
        OutputContent::BitAsset(5),
        Some(TxData::BitAssetMint(5)),
        Seed::Spent,
    )
}

#[test]
fn zero_input_t7c_bitasset_update_control_output_key_checked()
-> anyhow::Result<()> {
    let updates = BitAssetDataUpdates {
        commitment: Update::Retain,
        socket_addr_v4: Update::Retain,
        socket_addr_v6: Update::Retain,
        encryption_pubkey: Update::Retain,
        signing_pubkey: Update::Retain,
    };
    assert_created_output_key_checked(
        "zero_input_t7c",
        73,
        OutputContent::BitAssetControl,
        Some(TxData::BitAssetUpdate(Box::new(updates))),
        Seed::Unspent,
    )
}

#[test]
fn zero_input_t7d_amm_mint_lp_token_output_key_checked() -> anyhow::Result<()> {
    assert_created_output_key_checked(
        "zero_input_t7d",
        74,
        OutputContent::AmmLpToken(10),
        Some(TxData::AmmMint {
            amount0: 1,
            amount1: 1,
            lp_token_mint: 10,
        }),
        Seed::Spent,
    )
}

#[test]
fn zero_input_t7e_amm_burn_output_key_checked() -> anyhow::Result<()> {
    assert_created_output_key_checked(
        "zero_input_t7e",
        75,
        OutputContent::BitAsset(1),
        Some(TxData::AmmBurn {
            amount0: 1,
            amount1: 1,
            lp_token_burn: 10,
        }),
        Seed::Unspent,
    )
}

#[test]
fn zero_input_t7f_dutch_auction_create_receipt_output_key_checked()
-> anyhow::Result<()> {
    assert_created_output_key_checked(
        "zero_input_t7f",
        76,
        OutputContent::DutchAuctionReceipt,
        Some(TxData::DutchAuctionCreate(DutchAuctionParams {
            start_block: 1,
            duration: 10,
            base_asset: AssetId::BitAsset(some_bitasset()),
            base_amount: 5,
            quote_asset: AssetId::Bitcoin,
            initial_price: 100,
            final_price: 1,
        })),
        Seed::Spent,
    )
}

#[test]
fn zero_input_t7g_dutch_auction_collect_output_key_checked()
-> anyhow::Result<()> {
    assert_created_output_key_checked(
        "zero_input_t7g",
        77,
        OutputContent::BitAsset(5),
        Some(TxData::DutchAuctionCollect {
            asset_offered: AssetId::BitAsset(some_bitasset()),
            asset_receive: AssetId::Bitcoin,
            amount_offered_remaining: 5,
            amount_received: 0,
        }),
        Seed::Unspent,
    )
}

// ---------------------------------------------------------------------------
// T8 — the input-count rule for a zero-input BitAsset reservation, the shape the
// `reserve_bitasset` RPC builds from `Transaction::default()`: no inputs, one
// reservation output, zero fee, no authorizations. It validated before this
// rule; now it is rejected (the RPC now funds it with an input).
// ---------------------------------------------------------------------------

#[test]
fn zero_input_t8_zero_input_reservation_rejected() -> anyhow::Result<()> {
    let (_tmp, env, state) = fresh_state("zero_input_t8")?;
    let reservation = Transaction {
        inputs: Vec::new(),
        outputs: vec![Output::new(ADDR_A, OutputContent::BitAssetReservation)],
        memo: Vec::new(),
        data: Some(TxData::BitAssetReservation {
            commitment: [0x08; 32],
        }),
    };
    let expected = no_inputs_reason(reservation.txid());
    let r = unauthorized(reservation);
    expect_reject(
        "T8 validate_transaction(zero-input reservation)",
        validate(&env, &state, &r),
        &expected,
    )?;
    let pre = snapshot(&env, &state);
    let b = make_block(pre.tip, vec![r], vec![]);
    expect_reject(
        "T8 apply_block({zero-input reservation})",
        apply(&env, &state, &b.0, &b.1),
        &expected,
    )?;
    anyhow::ensure!(
        snapshot(&env, &state) == pre,
        "rejected block wrote state"
    );
    Ok(())
}
