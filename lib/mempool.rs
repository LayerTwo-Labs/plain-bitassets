use std::collections::{HashMap, HashSet, VecDeque};

use fallible_iterator::FallibleIterator as _;
use heed::types::SerdeBincode;
use sneed::{
    DatabaseUnique, DbError, EnvError, RoTxn, RwTxn, RwTxnError, UnitKey, db,
    env, rwtxn,
};

use crate::types::{
    Address, AuthorizedTransaction, InPoint, OutPoint, Output, Txid, VERSION,
    Version,
};

#[allow(clippy::duplicated_attributes)]
#[derive(thiserror::Error, transitive::Transitive, Debug)]
#[transitive(from(db::error::Delete, DbError))]
#[transitive(from(db::error::Put, DbError))]
#[transitive(from(db::error::TryGet, DbError))]
#[transitive(from(env::error::CreateDb, EnvError))]
#[transitive(from(env::error::WriteTxn, EnvError))]
#[transitive(from(rwtxn::error::Commit, RwTxnError))]
pub enum Error {
    #[error(transparent)]
    Db(#[from] DbError),
    #[error("Database env error")]
    DbEnv(#[from] EnvError),
    #[error("Database write error")]
    DbWrite(#[from] RwTxnError),
    #[error("Missing transaction {0}")]
    MissingTransaction(Txid),
    #[error("can't add transaction, utxo double spent")]
    UtxoDoubleSpent,
}

#[derive(Clone)]
pub struct MemPool {
    pub transactions:
        DatabaseUnique<SerdeBincode<Txid>, SerdeBincode<AuthorizedTransaction>>,
    pub spent_utxos:
        DatabaseUnique<SerdeBincode<OutPoint>, SerdeBincode<InPoint>>,
    /// Associates relevant txs to each address
    address_to_txs:
        DatabaseUnique<SerdeBincode<Address>, SerdeBincode<HashSet<Txid>>>,
    _version: DatabaseUnique<UnitKey, SerdeBincode<Version>>,
}

impl MemPool {
    pub const NUM_DBS: u32 = 4;

    pub fn new(env: &sneed::Env) -> Result<Self, Error> {
        let mut rwtxn = env.write_txn()?;
        let transactions =
            DatabaseUnique::create(env, &mut rwtxn, "transactions")?;
        let spent_utxos =
            DatabaseUnique::create(env, &mut rwtxn, "spent_utxos")?;
        let address_to_txs =
            DatabaseUnique::create(env, &mut rwtxn, "address_to_txs")?;
        let version =
            DatabaseUnique::create(env, &mut rwtxn, "mempool_version")?;
        if version.try_get(&rwtxn, &())?.is_none() {
            version.put(&mut rwtxn, &(), &*VERSION)?;
        }
        rwtxn.commit()?;
        Ok(Self {
            transactions,
            spent_utxos,
            address_to_txs,
            _version: version,
        })
    }

    /// Stores STXOs, checking for double spends
    fn put_stxos<Iter>(
        &self,
        rwtxn: &mut RwTxn,
        stxos: Iter,
    ) -> Result<(), Error>
    where
        Iter: IntoIterator<Item = (OutPoint, InPoint)>,
    {
        stxos.into_iter().try_for_each(|(outpoint, inpoint)| {
            if self.spent_utxos.try_get(rwtxn, &outpoint)?.is_some() {
                Err(Error::UtxoDoubleSpent)
            } else {
                self.spent_utxos.put(rwtxn, &outpoint, &inpoint)?;
                Ok(())
            }
        })
    }

    /// Delete STXOs
    fn delete_stxos<'a, Iter>(
        &self,
        rwtxn: &mut RwTxn,
        stxos: Iter,
    ) -> Result<(), Error>
    where
        Iter: IntoIterator<Item = &'a OutPoint>,
    {
        stxos.into_iter().try_for_each(|stxo| {
            let _ = self.spent_utxos.delete(rwtxn, stxo)?;
            Ok(())
        })
    }

    /// Associates the [`Txid`] with the [`Address`],
    /// by inserting into `address_to_txs`.
    fn assoc_txid_with_address(
        &self,
        rwtxn: &mut RwTxn,
        txid: Txid,
        address: &Address,
    ) -> Result<(), Error> {
        let mut associated_txs = self
            .address_to_txs
            .try_get(rwtxn, address)?
            .unwrap_or_default();
        associated_txs.insert(txid);
        self.address_to_txs.put(rwtxn, address, &associated_txs)?;
        Ok(())
    }

    /// Associates the [`Transaction`]'s [`Txid`] with all relevant
    /// [`Address`]es, by inserting into `address_to_txs`.
    fn assoc_tx_with_relevant_addresses(
        &self,
        rwtxn: &mut RwTxn,
        tx: &AuthorizedTransaction,
    ) -> Result<(), Error> {
        let txid = tx.transaction.txid();
        tx.relevant_addresses().into_iter().try_for_each(|addr| {
            self.assoc_txid_with_address(rwtxn, txid, &addr)
        })
    }

    /// Unassociates the [`Txid`] with the [`Address`],
    /// by deleting from `address_to_txs`.
    fn unassoc_txid_with_address(
        &self,
        rwtxn: &mut RwTxn,
        txid: &Txid,
        address: &Address,
    ) -> Result<(), Error> {
        let Some(mut associated_txs) =
            self.address_to_txs.try_get(rwtxn, address)?
        else {
            return Ok(());
        };
        associated_txs.remove(txid);
        if !associated_txs.is_empty() {
            self.address_to_txs.put(rwtxn, address, &associated_txs)?;
        } else {
            let _ = self.address_to_txs.delete(rwtxn, address)?;
        }
        Ok(())
    }

    /// Unassociates the [`Transaction`]'s [`Txid`] with all relevant
    /// [`Address`]es, by deleting from `address_to_txs`.
    fn unassoc_tx_with_relevant_addresses(
        &self,
        rwtxn: &mut RwTxn,
        tx: &AuthorizedTransaction,
    ) -> Result<(), Error> {
        let txid = tx.transaction.txid();
        tx.relevant_addresses().into_iter().try_for_each(|addr| {
            self.unassoc_txid_with_address(rwtxn, &txid, &addr)
        })
    }

    pub fn put(
        &self,
        rwtxn: &mut RwTxn,
        transaction: &AuthorizedTransaction,
    ) -> Result<(), Error> {
        let txid = transaction.transaction.txid();
        tracing::debug!("adding transaction {txid} to mempool");
        let stxos = {
            let txid = transaction.transaction.txid();
            transaction.transaction.inputs.iter().enumerate().map(
                move |(vin, outpoint)| {
                    (
                        *outpoint,
                        InPoint::Regular {
                            txid,
                            vin: vin as u32,
                        },
                    )
                },
            )
        };
        let () = self.put_stxos(rwtxn, stxos)?;
        self.transactions.put(rwtxn, &txid, transaction)?;
        let () = self.assoc_tx_with_relevant_addresses(rwtxn, transaction)?;
        Ok(())
    }

    pub fn delete(&self, rwtxn: &mut RwTxn, txid: Txid) -> Result<(), Error> {
        let mut pending_deletes = VecDeque::from([txid]);
        while let Some(txid) = pending_deletes.pop_front() {
            if let Some(tx) = self.transactions.try_get(rwtxn, &txid)? {
                let () = self.delete_stxos(rwtxn, &tx.transaction.inputs)?;
                let () = self.unassoc_tx_with_relevant_addresses(rwtxn, &tx)?;
                self.transactions.delete(rwtxn, &txid)?;
                for vout in 0..tx.transaction.outputs.len() {
                    let outpoint = OutPoint::Regular {
                        txid,
                        vout: vout as u32,
                    };
                    if let Some(InPoint::Regular {
                        txid: child_txid, ..
                    }) = self.spent_utxos.try_get(rwtxn, &outpoint)?
                    {
                        pending_deletes.push_back(child_txid);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn take(
        &self,
        rotxn: &RoTxn,
        number: usize,
    ) -> Result<Vec<AuthorizedTransaction>, Error> {
        self.transactions
            .iter(rotxn)
            .map_err(DbError::from)?
            .take(number)
            .map(|(_, transaction)| Ok(transaction))
            .collect()
            .map_err(|err| DbError::from(err).into())
    }

    pub fn take_all(
        &self,
        rotxn: &RoTxn,
    ) -> Result<Vec<AuthorizedTransaction>, Error> {
        self.transactions
            .iter(rotxn)
            .map_err(DbError::from)?
            .map(|(_, transaction)| Ok(transaction))
            .collect()
            .map_err(|err| DbError::from(err).into())
    }

    /// Get [`Txid`]s relevant to a particular address
    fn get_txids_relevant_to_address(
        &self,
        rotxn: &RoTxn,
        addr: &Address,
    ) -> Result<HashSet<Txid>, Error> {
        let res = self
            .address_to_txs
            .try_get(rotxn, addr)?
            .unwrap_or_default();
        Ok(res)
    }

    /// Get [`Transaction`]s relevant to a particular address
    fn get_txs_relevant_to_address(
        &self,
        rotxn: &RoTxn,
        addr: &Address,
    ) -> Result<Vec<AuthorizedTransaction>, Error> {
        self.get_txids_relevant_to_address(rotxn, addr)?
            .into_iter()
            .map(|txid| {
                self.transactions
                    .try_get(rotxn, &txid)?
                    .ok_or(Error::MissingTransaction(txid))
            })
            .collect()
    }

    /// Get unconfirmed UTXOs relevant to a particular address
    pub fn get_unconfirmed_utxos(
        &self,
        rotxn: &RoTxn,
        addr: &Address,
    ) -> Result<HashMap<OutPoint, Output>, Error> {
        let relevant_txs = self.get_txs_relevant_to_address(rotxn, addr)?;
        let res = relevant_txs
            .into_iter()
            .flat_map(|tx| {
                let txid = tx.transaction.txid();
                tx.transaction.outputs.into_iter().enumerate().filter_map(
                    move |(vout, output)| {
                        if output.address == *addr {
                            Some((
                                OutPoint::Regular {
                                    txid,
                                    vout: vout as u32,
                                },
                                output,
                            ))
                        } else {
                            None
                        }
                    },
                )
            })
            .collect();
        Ok(res)
    }
}
