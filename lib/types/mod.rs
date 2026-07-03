use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    sync::LazyLock,
};

use bitcoin::amount::CheckedSum as _;

use hashlink::{LinkedHashMap, linked_hash_map::Entry};
use rustreexo::accumulator::{
    mem_forest::MemForest, node_hash::AccumulatorHash, proof::Proof,
};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::io;
use thiserror::Error;
use utoipa::ToSchema;

pub use crate::authorization::Authorization;

mod address;
pub mod bitasset_data;
pub mod hashes;
pub mod keys;
pub mod proto;
pub mod schema;
pub mod transaction;

pub use address::{ADDRESS_SIZE, Address};
pub use bitasset_data::{BitAssetData, BitAssetDataUpdates, Update};
pub use hashes::{
    AssetId, BitAssetId, BlockHash, DutchAuctionId, Hash, M6id, MerkleProof,
    MerkleProofNode, MerkleRoot, Txid,
};
pub use keys::{EncryptionPubKey, VerifyingKey};
pub use transaction::{
    AddressOutPointKey, AddressTxidKey, AmmBurn, AmmMint, AmmSwap, AssetOutput,
    AssetOutputContent, Authorized, AuthorizedTransaction, BitcoinOutput,
    BitcoinOutputContent, DutchAuctionBid, DutchAuctionCollect,
    DutchAuctionParams, FilledOutput, FilledOutputContent, FilledTransaction,
    InPoint, OutPoint, OutPointKey, Output, OutputContent, PointedOutput,
    SpentOutput, Transaction, TxData, TxInputs, WithdrawalOutputContent,
};

pub const THIS_SIDECHAIN: u8 = 4;

#[derive(Debug, Error)]
#[error("Bitcoin amount overflow")]
pub struct AmountOverflowError;

#[derive(Debug, Error)]
#[error("Bitcoin amount underflow")]
pub struct AmountUnderflowError;

/// (de)serialize as Display/FromStr for human-readable forms like json,
/// and default serialization for non human-readable forms like bincode
mod serde_display_fromstr_human_readable {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use serde_with::{DeserializeAs, DisplayFromStr, SerializeAs};
    use std::{fmt::Display, str::FromStr};

    pub fn serialize<S, T>(data: T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize + Display,
    {
        if serializer.is_human_readable() {
            DisplayFromStr::serialize_as(&data, serializer)
        } else {
            data.serialize(serializer)
        }
    }

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de> + FromStr,
        <T as FromStr>::Err: Display,
    {
        if deserializer.is_human_readable() {
            DisplayFromStr::deserialize_as(deserializer)
        } else {
            T::deserialize(deserializer)
        }
    }
}

/// (de)serialize as hex strings for human-readable forms like json,
/// and default serialization for non human-readable formats like bincode
mod serde_hexstr_human_readable {
    use hex::{FromHex, ToHex};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S, T>(data: T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize + ToHex,
    {
        if serializer.is_human_readable() {
            hex::serde::serialize(data, serializer)
        } else {
            data.serialize(serializer)
        }
    }

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de> + FromHex,
        <T as FromHex>::Error: std::fmt::Display,
    {
        if deserializer.is_human_readable() {
            hex::serde::deserialize(deserializer)
        } else {
            T::deserialize(deserializer)
        }
    }
}

pub trait GetAddress {
    fn get_address(&self) -> Address;
}

pub trait GetBitcoinValue {
    /// Bitcoin value in sats
    fn get_bitcoin_value(&self) -> bitcoin::Amount;
}

#[derive(Debug, Error)]
pub enum Bech32mDecodeError {
    #[error(transparent)]
    Bech32m(#[from] bech32::DecodeError),
    #[error(
        "Wrong Bech32 HRP. Perhaps this key is being used somewhere it shouldn't be."
    )]
    WrongHrp,
    #[error("Wrong decoded byte length. Must decode to 32 bytes of data.")]
    WrongSize,
    #[error("Wrong Bech32 variant. Only Bech32m is accepted.")]
    WrongVariant,
}

fn borsh_serialize_bitcoin_block_hash<W>(
    block_hash: &bitcoin::BlockHash,
    writer: &mut W,
) -> borsh::io::Result<()>
where
    W: borsh::io::Write,
{
    let bytes: &[u8; 32] = block_hash.as_ref();
    borsh::BorshSerialize::serialize(bytes, writer)
}

#[derive(
    borsh::BorshSerialize,
    Clone,
    Debug,
    Deserialize,
    Eq,
    Hash,
    PartialEq,
    Serialize,
    ToSchema,
)]
pub struct Header {
    pub merkle_root: MerkleRoot,
    pub prev_side_hash: Option<BlockHash>,
    #[borsh(serialize_with = "borsh_serialize_bitcoin_block_hash")]
    #[schema(value_type = crate::types::schema::BitcoinBlockHash)]
    pub prev_main_hash: bitcoin::BlockHash,
}

impl Header {
    pub fn hash(&self) -> BlockHash {
        hashes::hash_with_scratch_buffer(self).into()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum WithdrawalBundleEventStatus {
    Confirmed,
    Failed,
    Submitted,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum WithdrawalBundleStatus {
    Confirmed,
    /// Formerly pending bundle
    Dropped,
    Failed,
    Pending,
    Submitted,
    /// Submitted, but unexpected due to previously being dropped or failing.
    /// It may not be possible to account for this withdrawal bundle, if it
    /// double-spends UTXOs.
    SubmittedUnexpected,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WithdrawalBundleEvent {
    pub m6id: M6id,
    pub status: WithdrawalBundleEventStatus,
}

pub static OP_DRIVECHAIN_SCRIPT: LazyLock<bitcoin::ScriptBuf> =
    LazyLock::new(|| {
        let mut script = bitcoin::ScriptBuf::new();
        script.push_opcode(bitcoin::opcodes::all::OP_RETURN);
        script.push_instruction(bitcoin::script::Instruction::PushBytes(
            &bitcoin::script::PushBytesBuf::from([THIS_SIDECHAIN]),
        ));
        script.push_opcode(bitcoin::opcodes::OP_TRUE);
        script
    });

#[derive(Debug, Error)]
enum WithdrawalBundleErrorInner {
    #[error("bundle too heavy: weight `{weight}` > max weight `{max_weight}`")]
    BundleTooHeavy { weight: u64, max_weight: u64 },
}

#[derive(Debug, Error)]
#[error("Withdrawal bundle error")]
pub struct WithdrawalBundleError(#[from] WithdrawalBundleErrorInner);

#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
pub struct WithdrawalBundle {
    #[schema(value_type = Vec<(
        transaction::OutPoint,
        transaction::FilledOutput)>
    )]
    #[serde_as(as = "serde_with::IfIsHumanReadable<serde_with::Seq<(_, _)>>")]
    spend_utxos: BTreeMap<OutPoint, FilledOutput>,
    #[schema(value_type = schema::BitcoinTransaction)]
    tx: bitcoin::Transaction,
}

impl WithdrawalBundle {
    /// Compute the size of a single txout
    pub const fn txout_size(spk_size: u32) -> Option<u32> {
        let Some(size) = (bitcoin::Amount::SIZE as u32)
            .checked_add(bitcoin::VarInt(spk_size as u64).size() as u32)
        else {
            return None;
        };
        size.checked_add(spk_size)
    }

    /// Predict the weight of a withdrawal bundle, based on the number of
    /// outputs (not including the commitment/treasury outputs) and the
    /// sum of sizes of txouts (not including the commitment/treasury outputs).
    /// Returns None if the predicted weight exceeds the maximum tx weight.
    pub const fn predict_weight(
        n_outputs: u32,
        sum_txout_sizes: u32,
    ) -> Option<bitcoin::Weight> {
        use bitcoin::{VarInt, Weight};
        const fn txin_base_size(script_sig_size: u32) -> Option<u32> {
            const OUTPOINT_SIZE: u8 = 36;
            const SEQUENCE_SIZE: u8 = 4;
            let script_sig_len_size: u8 =
                VarInt(script_sig_size as u64).size() as u8;
            let Some(res) = ((OUTPOINT_SIZE + script_sig_len_size) as u32)
                .checked_add(script_sig_size)
            else {
                return None;
            };
            res.checked_add(SEQUENCE_SIZE as u32)
        }
        const fn tx_base_size(
            n_inputs: u32,
            sum_txin_base_sizes: u32,
            n_outputs: u32,
            sum_txout_sizes: u32,
        ) -> Option<u32> {
            const VERSION_SIZE: u8 = 4;
            const fn vin_base_size(
                n_inputs: u32,
                sum_txin_base_sizes: u32,
            ) -> Option<u32> {
                let len_size = VarInt(n_inputs as u64).size() as u8;
                (len_size as u32).checked_add(sum_txin_base_sizes)
            }
            const fn vout_size(
                n_outputs: u32,
                sum_txout_sizes: u32,
            ) -> Option<u32> {
                let len_size = VarInt(n_outputs as u64).size() as u8;
                (len_size as u32).checked_add(sum_txout_sizes)
            }
            const LOCKTIME_SIZE: u8 = bitcoin::absolute::LockTime::SIZE as u8;
            let res = VERSION_SIZE as u32;
            let Some(vin_base_size) =
                vin_base_size(n_inputs, sum_txin_base_sizes)
            else {
                return None;
            };
            let Some(res) = res.checked_add(vin_base_size) else {
                return None;
            };
            let Some(vout_size) = vout_size(n_outputs, sum_txout_sizes) else {
                return None;
            };
            let Some(res) = res.checked_add(vout_size) else {
                return None;
            };
            res.checked_add(LOCKTIME_SIZE as u32)
        }
        const N_INPUTS: u32 = 1;
        const SUM_TXIN_BASE_SIZES: u32 = {
            const TREASURY_TXIN_BASE_SIZE: u32 = {
                const TREASURY_SCRIPT_SIG_SIZE: u32 = 0;
                txin_base_size(TREASURY_SCRIPT_SIG_SIZE).unwrap()
            };
            TREASURY_TXIN_BASE_SIZE
        };
        let Some(n_outputs) = n_outputs.checked_add(2) else {
            return None;
        };
        let Some(sum_txout_sizes) = ({
            const INPUTS_COMMITMENT_TXOUT_SIZE: u32 = {
                const INPUTS_COMMITMENT_OUTPUT_SPK_SIZE: u8 = 34;
                WithdrawalBundle::txout_size(
                    INPUTS_COMMITMENT_OUTPUT_SPK_SIZE as u32,
                )
                .unwrap()
            };
            const MAINCHAIN_FEE_COMMITMENT_TXOUT_SIZE: u32 = {
                const MAINCHAIN_FEE_COMMITMENT_OUTPUT_SPK_SIZE: u8 = 10;
                WithdrawalBundle::txout_size(
                    MAINCHAIN_FEE_COMMITMENT_OUTPUT_SPK_SIZE as u32,
                )
                .unwrap()
            };
            (INPUTS_COMMITMENT_TXOUT_SIZE + MAINCHAIN_FEE_COMMITMENT_TXOUT_SIZE)
                .checked_add(sum_txout_sizes)
        }) else {
            return None;
        };
        let Some(tx_base_size) = tx_base_size(
            N_INPUTS,
            SUM_TXIN_BASE_SIZES,
            n_outputs,
            sum_txout_sizes,
        ) else {
            return None;
        };
        let Some(tx_weight_wu) =
            (tx_base_size as u64).checked_mul(Weight::WITNESS_SCALE_FACTOR)
        else {
            return None;
        };
        if tx_weight_wu <= bitcoin::Transaction::MAX_STANDARD_WEIGHT.to_wu() {
            Some(Weight::from_wu(tx_weight_wu))
        } else {
            None
        }
    }

    pub fn new(
        block_height: u32,
        fee: bitcoin::Amount,
        spend_utxos: BTreeMap<OutPoint, FilledOutput>,
        bundle_outputs: Vec<bitcoin::TxOut>,
    ) -> Result<Self, WithdrawalBundleError> {
        let inputs_commitment_txout = {
            // Create inputs commitment.
            let inputs: Vec<OutPoint> = [
                // Commit to inputs.
                spend_utxos.keys().copied().collect(),
                // Commit to block height.
                vec![OutPoint::Regular {
                    txid: [0; 32].into(),
                    vout: block_height,
                }],
            ]
            .concat();
            let commitment = hashes::hash_with_scratch_buffer(&inputs);
            let script_pubkey = bitcoin::script::Builder::new()
                .push_opcode(bitcoin::opcodes::all::OP_RETURN)
                .push_slice(commitment)
                .into_script();
            bitcoin::TxOut {
                value: bitcoin::Amount::ZERO,
                script_pubkey,
            }
        };
        let mainchain_fee_txout = {
            let script_pubkey = bitcoin::script::Builder::new()
                .push_opcode(bitcoin::opcodes::all::OP_RETURN)
                .push_slice(fee.to_sat().to_be_bytes())
                .into_script();
            bitcoin::TxOut {
                value: bitcoin::Amount::ZERO,
                script_pubkey,
            }
        };
        let outputs = Vec::from_iter(
            [mainchain_fee_txout, inputs_commitment_txout]
                .into_iter()
                .chain(bundle_outputs),
        );
        let tx = bitcoin::Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::blockdata::locktime::absolute::LockTime::ZERO,
            input: Vec::new(),
            output: outputs,
        };
        if tx.weight().to_wu() > bitcoin::policy::MAX_STANDARD_TX_WEIGHT as u64
        {
            Err(WithdrawalBundleErrorInner::BundleTooHeavy {
                weight: tx.weight().to_wu(),
                max_weight: bitcoin::policy::MAX_STANDARD_TX_WEIGHT as u64,
            })?;
        }
        Ok(Self { spend_utxos, tx })
    }

    pub fn compute_m6id(&self) -> M6id {
        M6id(self.tx.compute_txid())
    }

    pub fn spend_utxos(&self) -> &BTreeMap<OutPoint, FilledOutput> {
        &self.spend_utxos
    }

    pub fn tx(&self) -> &bitcoin::Transaction {
        &self.tx
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TwoWayPegData {
    pub deposits: HashMap<OutPoint, Output>,
    pub deposit_block_hash: Option<bitcoin::BlockHash>,
    pub bundle_statuses: HashMap<M6id, WithdrawalBundleEvent>,
}

#[derive(
    borsh::BorshSerialize, Clone, Debug, Deserialize, Serialize, ToSchema,
)]
pub struct Body {
    pub coinbase: Vec<Output>,
    pub transactions: Vec<Transaction>,
    pub authorizations: Vec<Authorization>,
}

impl Body {
    pub fn new(
        authorized_transactions: Vec<AuthorizedTransaction>,
        coinbase: Vec<Output>,
    ) -> Self {
        let mut authorizations = Vec::with_capacity(
            authorized_transactions
                .iter()
                .map(|t| t.transaction.inputs.len())
                .sum(),
        );
        let mut transactions =
            Vec::with_capacity(authorized_transactions.len());
        for at in authorized_transactions.into_iter() {
            authorizations.extend(at.authorizations);
            transactions.push(at.transaction);
        }
        Self {
            coinbase,
            transactions,
            authorizations,
        }
    }

    /// Size limit in bytes
    pub const MAX_SIZE: usize = 8 * 1024 * 1024;

    pub fn authorized_transactions(&self) -> Vec<AuthorizedTransaction> {
        let mut authorizations_iter = self.authorizations.iter();
        self.transactions
            .iter()
            .map(|tx| {
                let mut authorizations = Vec::with_capacity(tx.inputs.len());
                for _ in 0..tx.inputs.len() {
                    let auth = authorizations_iter.next().unwrap();
                    authorizations.push(auth.clone());
                }
                AuthorizedTransaction {
                    transaction: tx.clone(),
                    authorizations,
                }
            })
            .collect()
    }

    pub fn compute_merkle_root(
        coinbase: &[Output],
        txs: &[Transaction],
    ) -> MerkleRoot {
        let mut leaves = Vec::with_capacity(txs.len() + 1);
        leaves.push(hashes::hash_with_scratch_buffer(&coinbase));
        leaves.extend(txs.iter().map(hashes::hash_with_scratch_buffer));
        Body::compute_cbmt_tree(&leaves)[0].into()
    }

    // https://github.com/nervosnetwork/merkle-tree/blob/5d1898263e7167560fdaa62f09e8d52991a1c712/README.md#tree-struct
    fn compute_cbmt_tree(leaves: &[Hash]) -> Vec<Hash> {
        let n = leaves.len();
        let mut nodes = vec![Hash::default(); 2 * n - 1];

        nodes[n - 1..].copy_from_slice(leaves);

        for idx in (0..n - 1).rev() {
            nodes[idx] = hashes::hash_with_scratch_buffer(&(
                nodes[2 * idx + 1],
                nodes[2 * idx + 2],
            ));
        }

        nodes
    }

    pub fn compute_tx_merkle_proof(
        coinbase: &[Output],
        txs: &[Transaction],
        tx_idx: usize,
    ) -> MerkleProof {
        let mut leaves = Vec::with_capacity(txs.len() + 1);
        leaves.push(hashes::hash_with_scratch_buffer(&coinbase));
        leaves.extend(txs.iter().map(hashes::hash_with_scratch_buffer));
        Body::compute_cbmt_proof(&leaves, tx_idx + 1)
    }

    fn compute_cbmt_proof(leaves: &[Hash], leaf_index: usize) -> MerkleProof {
        let n = leaves.len();
        let nodes = Body::compute_cbmt_tree(leaves);
        let mut idx = leaf_index + n - 1;
        let mut siblings = Vec::new();

        while idx > 0 {
            let sibling_idx = (idx + 1) ^ 1;
            let sibling_idx = sibling_idx - 1;

            siblings.push(MerkleProofNode {
                hash: nodes[sibling_idx],
                is_left: sibling_idx < idx,
            });

            idx = (idx - 1) / 2;
        }

        MerkleProof {
            leaf_index,
            siblings,
        }
    }

    pub fn get_inputs(&self) -> Vec<OutPoint> {
        self.transactions
            .iter()
            .flat_map(|tx| tx.inputs.iter())
            .copied()
            .collect()
    }

    pub fn get_outputs(&self) -> HashMap<OutPoint, Output> {
        let mut outputs = HashMap::new();
        let merkle_root =
            Body::compute_merkle_root(&self.coinbase, &self.transactions);
        for (vout, output) in self.coinbase.iter().enumerate() {
            let vout = vout as u32;
            let outpoint = OutPoint::Coinbase { merkle_root, vout };
            outputs.insert(outpoint, output.clone());
        }
        for transaction in &self.transactions {
            let txid = transaction.txid();
            for (vout, output) in transaction.outputs.iter().enumerate() {
                let vout = vout as u32;
                let outpoint = OutPoint::Regular { txid, vout };
                outputs.insert(outpoint, output.clone());
            }
        }
        outputs
    }

    pub fn get_coinbase_value(
        &self,
    ) -> Result<bitcoin::Amount, AmountOverflowError> {
        self.coinbase
            .iter()
            .map(|output| output.get_bitcoin_value())
            .checked_sum()
            .ok_or(AmountOverflowError)
    }
}

pub trait Verify {
    type Error;
    fn verify_transaction(
        transaction: &AuthorizedTransaction,
    ) -> Result<(), Self::Error>;
    fn verify_body(body: &Body) -> Result<(), Self::Error>;
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct Block {
    #[serde(flatten)]
    pub header: Header,
    #[serde(flatten)]
    pub body: Body,
    pub height: u32,
}

/*
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisconnectData {
    pub spent_utxos: HashMap<types::OutPoint, Output>,
    pub deposits: Vec<types::OutPoint>,
    pub pending_bundles: Vec<bitcoin::Txid>,
    pub spent_bundles: HashMap<bitcoin::Txid, Vec<types::OutPoint>>,
    pub spent_withdrawals: HashMap<types::OutPoint, Output>,
    pub failed_withdrawals: Vec<bitcoin::Txid>,
}
*/

#[derive(Eq, PartialEq, Clone, Debug)]
pub struct AggregatedWithdrawal {
    pub spend_utxos: HashMap<OutPoint, FilledOutput>,
    pub main_address: bitcoin::Address<bitcoin::address::NetworkUnchecked>,
    pub value: bitcoin::Amount,
    pub main_fee: bitcoin::Amount,
}

impl Ord for AggregatedWithdrawal {
    fn cmp(&self, other: &Self) -> Ordering {
        // A *total* order (lexicographic by main_fee, value, main_address). The
        // previous `OR of >` was not antisymmetric/transitive, so the
        // withdrawal-bundle output order (and hence compute_m6id) depended on
        // HashMap iteration order and could differ across nodes. A real total order makes
        // the sorted bundle canonical regardless of aggregation order.
        (self.main_fee, self.value, &self.main_address).cmp(&(
            other.main_fee,
            other.value,
            &other.main_address,
        ))
    }
}

impl PartialOrd for AggregatedWithdrawal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Hash type used by the utreexo accumulator.
///
/// `Blake3UtxoHash::Some` is a real 32-byte BLAKE3 digest.
/// `Placeholder` and `Empty` are rustreexo sentinel values used internally by
/// the accumulator. They are not real UTXO hashes and should not appear in
/// serialized accumulator diffs.
///
/// This type stores the concrete BLAKE3 digest as `[u8; 32]` instead of
/// `blake3::Hash` because rustreexo and the surrounding accumulator code need
/// ordering traits such as `Ord`, while `blake3::Hash` intentionally does not
/// implement `Ord`.
///
/// WARNING: the exact `parent_hash` algorithm is consensus-critical. Changing
/// it changes all accumulator roots and invalidates existing proofs.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Blake3UtxoHash {
    Some([u8; 32]),
    Placeholder,
    #[default]
    Empty,
}

impl Blake3UtxoHash {
    pub fn hash_bytes(bytes: &[u8]) -> Self {
        Self::Some(*blake3::hash(bytes).as_bytes())
    }

    pub fn as_bytes(&self) -> Option<[u8; 32]> {
        match self {
            Self::Some(bytes) => Some(*bytes),
            Self::Placeholder | Self::Empty => None,
        }
    }
}

impl From<blake3::Hash> for Blake3UtxoHash {
    fn from(hash: blake3::Hash) -> Self {
        Self::Some(*hash.as_bytes())
    }
}

impl From<[u8; 32]> for Blake3UtxoHash {
    fn from(bytes: [u8; 32]) -> Self {
        Self::Some(bytes)
    }
}

impl std::fmt::Display for Blake3UtxoHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Some(bytes) => write!(f, "{:?}", bytes),
            Self::Placeholder => write!(f, "Placeholder"),
            Self::Empty => write!(f, "Empty"),
        }
    }
}

impl AccumulatorHash for Blake3UtxoHash {
    fn placeholder() -> Self {
        Self::Placeholder
    }

    fn empty() -> Self {
        Self::Empty
    }

    fn is_placeholder(&self) -> bool {
        matches!(self, Self::Placeholder)
    }

    fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    fn write<W>(&self, writer: &mut W) -> io::Result<()>
    where
        W: io::Write,
    {
        match self {
            Self::Some(bytes) => writer.write_all(bytes),
            Self::Placeholder | Self::Empty => writer.write_all(&[0u8; 32]),
        }
    }

    fn read<R>(reader: &mut R) -> io::Result<Self>
    where
        R: io::Read,
    {
        let mut bytes = [0u8; 32];
        reader.read_exact(&mut bytes)?;

        if bytes == [0u8; 32] {
            Ok(Self::Placeholder)
        } else {
            Ok(Self::Some(bytes))
        }
    }

    fn parent_hash(left: &Self, right: &Self) -> Self {
        let (Self::Some(left), Self::Some(right)) = (left, right) else {
            unreachable!("parent_hash called with non-concrete hash");
        };

        let mut input = [0u8; 64];
        input[..32].copy_from_slice(left);
        input[32..].copy_from_slice(right);
        Self::Some(*blake3::hash(&input).as_bytes())
    }
}

/// Manage accumulator diffs.
/// Insertions and removals 'cancel out' exactly once.
/// Inserting twice will cause one insertion.
/// Removing twice will cause one deletion.
/// Inserting and then removing will have no overall effect,
/// but a second removal will still cause a deletion.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AccumulatorDiff {
    /// `true` indicates insertion, `false` indicates removal.
    diff: LinkedHashMap<[u8; 32], bool>,
    /// Total number of insertions still represented in `diff`.
    insertions: usize,
    /// Total number of deletions still represented in `diff`.
    deletions: usize,
}

impl AccumulatorDiff {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            diff: LinkedHashMap::with_capacity(capacity),
            insertions: 0,
            deletions: 0,
        }
    }

    pub fn insert(&mut self, utxo_hash: [u8; 32]) {
        match self.diff.entry(utxo_hash) {
            Entry::Occupied(entry) => {
                if !entry.get() {
                    entry.remove();
                    debug_assert!(self.deletions > 0);
                    self.deletions -= 1;
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(true);
                self.insertions += 1;
            }
        }
    }

    pub fn remove(&mut self, utxo_hash: [u8; 32]) {
        match self.diff.entry(utxo_hash) {
            Entry::Occupied(entry) => {
                if *entry.get() {
                    entry.remove();
                    debug_assert!(self.insertions > 0);
                    self.insertions -= 1;
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(false);
                self.deletions += 1;
            }
        }
    }

    pub fn counts(&self) -> (usize, usize) {
        (self.insertions, self.deletions)
    }

    pub fn is_empty(&self) -> bool {
        self.diff.is_empty()
    }
}

impl Serialize for AccumulatorDiff {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut insertions = Vec::with_capacity(self.insertions);
        let mut deletions = Vec::with_capacity(self.deletions);

        for (hash, insert) in &self.diff {
            if *insert {
                insertions.push(*hash);
            } else {
                deletions.push(*hash);
            }
        }

        (insertions, deletions).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AccumulatorDiff {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let (insertions, deletions): (Vec<[u8; 32]>, Vec<[u8; 32]>) =
            Deserialize::deserialize(deserializer)?;

        let mut diff =
            AccumulatorDiff::with_capacity(insertions.len() + deletions.len());

        for hash in insertions {
            diff.insert(hash);
        }

        for hash in deletions {
            diff.remove(hash);
        }

        Ok(diff)
    }
}

#[repr(transparent)]
#[derive(Debug, Error)]
#[error("utreexo error: {0}")]
pub struct UtreexoError(String);

#[derive(Default)]
#[repr(transparent)]
pub struct Accumulator(pub MemForest<Blake3UtxoHash>);

unsafe impl Send for Accumulator {}
unsafe impl Sync for Accumulator {}

impl Accumulator {
    pub fn apply_diff(
        &mut self,
        diff: AccumulatorDiff,
    ) -> Result<(), UtreexoError> {
        let mut insertions = Vec::with_capacity(diff.insertions);
        let mut deletions = Vec::with_capacity(diff.deletions);

        for (utxo_hash, insert) in diff.diff {
            let utxo_hash = Blake3UtxoHash::Some(utxo_hash);

            if insert {
                insertions.push(utxo_hash);
            } else {
                deletions.push(utxo_hash);
            }
        }

        self.0
            .modify(&insertions, &deletions)
            .map_err(UtreexoError)?;

        Ok(())
    }

    pub fn get_roots(&self) -> Vec<Blake3UtxoHash> {
        self.0
            .get_roots()
            .iter()
            .map(|node| node.get_data())
            .collect()
    }

    pub fn prove(
        &self,
        targets: &[Blake3UtxoHash],
    ) -> Result<Proof<Blake3UtxoHash>, UtreexoError> {
        self.0.prove(targets).map_err(UtreexoError)
    }

    pub fn verify(
        &self,
        proof: &Proof<Blake3UtxoHash>,
        del_hashes: &[Blake3UtxoHash],
    ) -> Result<bool, UtreexoError> {
        self.0.verify(proof, del_hashes).map_err(UtreexoError)
    }
}

impl<'de> Deserialize<'de> for Accumulator {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: Vec<u8> =
            <Vec<_> as Deserialize>::deserialize(deserializer)?;
        let mem_forest = MemForest::deserialize(&*bytes)
            .map_err(<D::Error as serde::de::Error>::custom)?;
        Ok(Self(mem_forest))
    }
}

impl Serialize for Accumulator {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut bytes = Vec::new();
        self.0
            .serialize(&mut bytes)
            .map_err(<S::Error as serde::ser::Error>::custom)?;
        <Vec<_> as Serialize>::serialize(&bytes, serializer)
    }
}

/// Transaction index
#[derive(Clone, Copy, Debug, Deserialize, Serialize, ToSchema)]
pub struct TxIn {
    pub block_hash: BlockHash,
    pub idx: u32,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum BmmResult {
    Verified,
    Failed,
}

/// A tip refers to both a sidechain block AND the mainchain block that commits
/// to it.
#[derive(
    borsh::BorshSerialize,
    Clone,
    Copy,
    Debug,
    Deserialize,
    Eq,
    Hash,
    PartialEq,
    Serialize,
)]
pub struct Tip {
    pub block_hash: BlockHash,
    #[borsh(serialize_with = "borsh_serialize_bitcoin_block_hash")]
    pub main_block_hash: bitcoin::BlockHash,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[cfg_attr(
    feature = "clap",
    derive(clap::ValueEnum, strum::Display),
    strum(serialize_all = "lowercase")
)]
pub enum Network {
    #[default]
    Signet,
    Regtest,
    Forknet,
}

/// Semver-compatible version
#[derive(
    borsh::BorshSerialize,
    Clone,
    Copy,
    Debug,
    Deserialize,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
pub struct Version {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl From<semver::Version> for Version {
    fn from(version: semver::Version) -> Self {
        let semver::Version {
            major,
            minor,
            patch,
            pre: _,
            build: _,
        } = version;
        Self {
            major,
            minor,
            patch,
        }
    }
}
// Do not make this public outside of this crate, as it could break semver
pub(crate) static VERSION: LazyLock<Version> = LazyLock::new(|| {
    const VERSION_STR: &str = env!("CARGO_PKG_VERSION");
    semver::Version::parse(VERSION_STR).unwrap().into()
});

#[cfg(test)]
mod merkle_tests {
    use super::*;

    fn tx(memo: &[u8]) -> Transaction {
        let mut tx = Transaction::default();
        tx.memo = memo.to_vec();
        tx
    }

    #[test]
    fn tx_merkle_proof_verifies_membership() {
        let coinbase = Vec::new();
        let txs = vec![tx(b"tx-0"), tx(b"tx-1"), tx(b"tx-2"), tx(b"tx-3")];

        let root = Body::compute_merkle_root(&coinbase, &txs);
        let proof = Body::compute_tx_merkle_proof(&coinbase, &txs, 2);
        let leaf = hashes::hash_with_scratch_buffer(&txs[2]);

        assert!(proof.verify(leaf, root));
    }

    #[test]
    fn tx_merkle_proof_rejects_non_member() {
        let coinbase = Vec::new();
        let txs = vec![tx(b"tx-0"), tx(b"tx-1"), tx(b"tx-2"), tx(b"tx-3")];
        let other = tx(b"other");

        let root = Body::compute_merkle_root(&coinbase, &txs);
        let proof = Body::compute_tx_merkle_proof(&coinbase, &txs, 2);
        let leaf = hashes::hash_with_scratch_buffer(&other);

        assert!(!proof.verify(leaf, root));
    }

    #[test]
    fn tx_merkle_proof_rejects_wrong_position() {
        let coinbase = Vec::new();
        let txs = vec![tx(b"tx-0"), tx(b"tx-1"), tx(b"tx-2"), tx(b"tx-3")];

        let root = Body::compute_merkle_root(&coinbase, &txs);
        let proof = Body::compute_tx_merkle_proof(&coinbase, &txs, 2);
        let leaf = hashes::hash_with_scratch_buffer(&txs[1]);

        assert!(!proof.verify(leaf, root));
    }
}

#[cfg(test)]
mod withdrawal_bundle_order_regression {
    use super::*;
    use std::collections::{BTreeMap, HashMap};

    use bitcoin::{Address, Amount, address::NetworkUnchecked};

    fn aw(value: u64, main_fee: u64) -> AggregatedWithdrawal {
        // value/main_fee drive the comparison; one address is enough to expose it.
        let addr: Address<NetworkUnchecked> =
            "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4"
                .parse()
                .unwrap();
        AggregatedWithdrawal {
            spend_utxos: HashMap::new(),
            main_address: addr,
            value: Amount::from_sat(value),
            main_fee: Amount::from_sat(main_fee),
        }
    }

    // Build the bundle m6id exactly as `collect_withdrawal_bundle` does, for a given
    // (HashMap-determined) input order.
    fn bundle_m6id(mut aggregated: Vec<AggregatedWithdrawal>) -> M6id {
        aggregated.sort_by_key(|a| std::cmp::Reverse(a.clone()));
        let outputs: Vec<bitcoin::TxOut> = aggregated
            .iter()
            .map(|a| bitcoin::TxOut {
                value: a.value,
                script_pubkey: a
                    .main_address
                    .assume_checked_ref()
                    .script_pubkey(),
            })
            .collect();
        WithdrawalBundle::new(0, Amount::ZERO, BTreeMap::new(), outputs)
            .unwrap()
            .compute_m6id()
    }

    // The withdrawal bundle's m6id must not depend on the order in which withdrawals
    // were aggregated (HashMap iteration order is randomized per process). Before the
    // total-order fix, the comparator was non-transitive and this failed.
    #[test]
    fn m6id_is_independent_of_aggregation_order() {
        let a = aw(1, 3);
        let b = aw(3, 2);
        let c = aw(2, 1);
        let m = bundle_m6id(vec![a.clone(), b.clone(), c.clone()]);
        for perm in [
            vec![c.clone(), b.clone(), a.clone()],
            vec![b.clone(), a.clone(), c.clone()],
            vec![a.clone(), c.clone(), b.clone()],
        ] {
            assert_eq!(
                m,
                bundle_m6id(perm),
                "m6id must not depend on aggregation order"
            );
        }
    }
}
