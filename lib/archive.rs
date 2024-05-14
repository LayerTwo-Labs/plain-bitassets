use std::{cmp::Ordering, collections::BTreeMap};

use bip300301::{
    bitcoin::{self, block::Header as BitcoinHeader, hashes::Hash},
    DepositInfo,
};
use fallible_iterator::FallibleIterator;
use heed::{types::SerdeBincode, Database, RoTxn, RwTxn};

use crate::types::{Block, BlockHash, Body, Header, Txid};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid mainchain block hash for deposit")]
    DepositInvalidMainBlockHash,
    #[error("heed error")]
    Heed(#[from] heed::Error),
    #[error("invalid previous side hash")]
    InvalidPrevSideHash,
    #[error("invalid merkle root")]
    InvalidMerkleRoot,
    #[error("no block with hash {0}")]
    NoBlock(BlockHash),
    #[error("no BMM verification result with for block {0}")]
    NoBmmVerification(BlockHash),
    #[error("no deposits info for block {0}")]
    NoDepositsInfo(bitcoin::BlockHash),
    #[error("no header with hash {0}")]
    NoHeader(BlockHash),
    #[error("no height info hash {0}")]
    NoHeight(BlockHash),
    #[error("no mainchain header with hash {0}")]
    NoMainHeader(bitcoin::BlockHash),
    #[error("no tx with txid {0}")]
    NoTx(Txid),
}

#[derive(Clone)]
pub struct Archive {
    block_hash_to_height: Database<SerdeBincode<BlockHash>, SerdeBincode<u32>>,
    /// BMM verification status for each header.
    /// A status of false indicates that verification failed.
    bmm_verifications: Database<SerdeBincode<BlockHash>, SerdeBincode<bool>>,
    bodies: Database<SerdeBincode<BlockHash>, SerdeBincode<Body>>,
    /// Deposits by mainchain block, sorted first-to-last in each block
    deposits: Database<
        SerdeBincode<bitcoin::BlockHash>,
        SerdeBincode<Vec<DepositInfo>>,
    >,
    /// Sidechain headers. All ancestors of any header should always be present.
    headers: Database<SerdeBincode<BlockHash>, SerdeBincode<Header>>,
    /// Mainchain headers. All ancestors of any header should always be present
    main_headers:
        Database<SerdeBincode<bitcoin::BlockHash>, SerdeBincode<BitcoinHeader>>,
    /// Total work for mainchain headers.
    /// All ancestors of any block should always be present
    total_work:
        Database<SerdeBincode<bitcoin::BlockHash>, SerdeBincode<bitcoin::Work>>,
    /// Blocks in which a tx has been included, and index within the block
    txid_to_inclusions:
        Database<SerdeBincode<Txid>, SerdeBincode<BTreeMap<BlockHash, u32>>>,
}

impl Archive {
    pub const NUM_DBS: u32 = 8;

    pub fn new(env: &heed::Env) -> Result<Self, Error> {
        let block_hash_to_height =
            env.create_database(Some("hash_to_height"))?;
        let bmm_verifications =
            env.create_database(Some("bmm_verifications"))?;
        let bodies = env.create_database(Some("bodies"))?;
        let deposits = env.create_database(Some("deposits"))?;
        let headers = env.create_database(Some("headers"))?;
        let main_headers = env.create_database(Some("main_headers"))?;
        let total_work = env.create_database(Some("total_work"))?;
        let txid_to_inclusions =
            env.create_database(Some("txid_to_inclusions"))?;
        Ok(Self {
            block_hash_to_height,
            bmm_verifications,
            bodies,
            deposits,
            headers,
            main_headers,
            total_work,
            txid_to_inclusions,
        })
    }

    /** Get the height of a block from it's hash.
     *  Returns [`None`] if no block with the specified hash exists. */
    pub fn try_get_height(
        &self,
        rotxn: &RoTxn,
        block_hash: BlockHash,
    ) -> Result<Option<u32>, Error> {
        if block_hash == BlockHash::default() {
            Ok(Some(0))
        } else {
            self.block_hash_to_height
                .get(rotxn, &block_hash)
                .map_err(Error::from)
        }
    }

    /** Get the height of a block from it's hash.
     *  Returns an error if no block with the specified hash exists. */
    pub fn get_height(
        &self,
        rotxn: &RoTxn,
        block_hash: BlockHash,
    ) -> Result<u32, Error> {
        self.try_get_height(rotxn, block_hash)?
            .ok_or(Error::NoHeight(block_hash))
    }

    pub fn try_get_bmm_verification(
        &self,
        rotxn: &RoTxn,
        block_hash: BlockHash,
    ) -> Result<Option<bool>, Error> {
        if block_hash == BlockHash::default() {
            Ok(Some(true))
        } else {
            self.bmm_verifications
                .get(rotxn, &block_hash)
                .map_err(Error::from)
        }
    }

    pub fn get_bmm_verification(
        &self,
        rotxn: &RoTxn,
        block_hash: BlockHash,
    ) -> Result<bool, Error> {
        self.try_get_bmm_verification(rotxn, block_hash)?
            .ok_or(Error::NoBmmVerification(block_hash))
    }

    pub fn try_get_body(
        &self,
        rotxn: &RoTxn,
        block_hash: BlockHash,
    ) -> Result<Option<Body>, Error> {
        let body = self.bodies.get(rotxn, &block_hash)?;
        Ok(body)
    }

    pub fn get_body(
        &self,
        rotxn: &RoTxn,
        block_hash: BlockHash,
    ) -> Result<Body, Error> {
        self.try_get_body(rotxn, block_hash)?
            .ok_or(Error::NoBlock(block_hash))
    }

    pub fn try_get_deposits(
        &self,
        rotxn: &RoTxn,
        block_hash: bitcoin::BlockHash,
    ) -> Result<Option<Vec<DepositInfo>>, Error> {
        let deposits = self.deposits.get(rotxn, &block_hash)?;
        Ok(deposits)
    }

    pub fn get_deposits(
        &self,
        rotxn: &RoTxn,
        block_hash: bitcoin::BlockHash,
    ) -> Result<Vec<DepositInfo>, Error> {
        self.try_get_deposits(rotxn, block_hash)?
            .ok_or(Error::NoDepositsInfo(block_hash))
    }

    pub fn try_get_header(
        &self,
        rotxn: &RoTxn,
        block_hash: BlockHash,
    ) -> Result<Option<Header>, Error> {
        let header = self.headers.get(rotxn, &block_hash)?;
        Ok(header)
    }

    pub fn get_header(
        &self,
        rotxn: &RoTxn,
        block_hash: BlockHash,
    ) -> Result<Header, Error> {
        self.try_get_header(rotxn, block_hash)?
            .ok_or(Error::NoHeader(block_hash))
    }

    pub fn try_get_block(
        &self,
        rotxn: &RoTxn,
        block_hash: BlockHash,
    ) -> Result<Option<Block>, Error> {
        let Some(body) = self.try_get_body(rotxn, block_hash)? else {
            return Ok(None);
        };
        let header = self.get_header(rotxn, block_hash)?;
        let height = self.get_height(rotxn, block_hash)?;
        let block = Block {
            header,
            body,
            height,
        };
        Ok(Some(block))
    }

    pub fn get_block(
        &self,
        rotxn: &RoTxn,
        block_hash: BlockHash,
    ) -> Result<Block, Error> {
        self.try_get_block(rotxn, block_hash)?
            .ok_or(Error::NoBlock(block_hash))
    }

    pub fn try_get_main_header(
        &self,
        rotxn: &RoTxn,
        block_hash: bitcoin::BlockHash,
    ) -> Result<Option<BitcoinHeader>, Error> {
        let header = self.main_headers.get(rotxn, &block_hash)?;
        Ok(header)
    }

    fn get_main_header(
        &self,
        rotxn: &RoTxn,
        block_hash: bitcoin::BlockHash,
    ) -> Result<BitcoinHeader, Error> {
        self.try_get_main_header(rotxn, block_hash)?
            .ok_or(Error::NoMainHeader(block_hash))
    }

    pub fn try_get_total_work(
        &self,
        rotxn: &RoTxn,
        block_hash: bitcoin::BlockHash,
    ) -> Result<Option<bitcoin::Work>, Error> {
        let total_work = self.total_work.get(rotxn, &block_hash)?;
        Ok(total_work)
    }

    pub fn get_total_work(
        &self,
        rotxn: &RoTxn,
        block_hash: bitcoin::BlockHash,
    ) -> Result<bitcoin::Work, Error> {
        self.try_get_total_work(rotxn, block_hash)?
            .ok_or(Error::NoMainHeader(block_hash))
    }

    /// Get blocks in which a tx was included
    pub fn get_tx_inclusions(
        &self,
        rotxn: &RoTxn,
        txid: Txid,
    ) -> Result<BTreeMap<BlockHash, u32>, Error> {
        let inclusions = self
            .txid_to_inclusions
            .get(rotxn, &txid)?
            .unwrap_or_default();
        Ok(inclusions)
    }

    /// Store a BMM verification result
    pub fn put_bmm_verification(
        &self,
        rwtxn: &mut RwTxn,
        block_hash: BlockHash,
        verification_result: bool,
    ) -> Result<(), Error> {
        self.bmm_verifications
            .put(rwtxn, &block_hash, &verification_result)?;
        Ok(())
    }

    /// Store a block body. The header must already exist.
    pub fn put_body(
        &self,
        rwtxn: &mut RwTxn,
        block_hash: BlockHash,
        body: &Body,
    ) -> Result<(), Error> {
        let _header = self.get_header(rwtxn, block_hash)?;
        self.bodies.put(rwtxn, &block_hash, body)?;
        body.transactions
            .iter()
            .enumerate()
            .try_for_each(|(txin, tx)| {
                let txid = tx.txid();
                let mut inclusions = self.get_tx_inclusions(rwtxn, txid)?;
                inclusions.insert(block_hash, txin as u32);
                self.txid_to_inclusions.put(rwtxn, &txid, &inclusions)?;
                Ok(())
            })
    }

    /// Store deposit info for a block
    pub fn put_deposits(
        &self,
        rwtxn: &mut RwTxn,
        block_hash: bitcoin::BlockHash,
        mut deposits: Vec<DepositInfo>,
    ) -> Result<(), Error> {
        deposits.sort_by_key(|deposit| deposit.tx_index);
        if !deposits
            .iter()
            .all(|deposit| deposit.block_hash == block_hash)
        {
            return Err(Error::DepositInvalidMainBlockHash);
        };
        self.deposits.put(rwtxn, &block_hash, &deposits)?;
        Ok(())
    }

    pub fn put_header(
        &self,
        rwtxn: &mut RwTxn,
        header: &Header,
    ) -> Result<(), Error> {
        let Some(prev_height) =
            self.try_get_height(rwtxn, header.prev_side_hash)?
        else {
            return Err(Error::InvalidPrevSideHash);
        };
        let height = prev_height + 1;
        let block_hash = header.hash();
        self.block_hash_to_height.put(rwtxn, &block_hash, &height)?;
        self.headers.put(rwtxn, &block_hash, header)?;
        Ok(())
    }

    pub fn put_main_header(
        &self,
        rwtxn: &mut RwTxn,
        header: &BitcoinHeader,
    ) -> Result<(), Error> {
        if self
            .try_get_main_header(rwtxn, header.prev_blockhash)?
            .is_none()
            && header.prev_blockhash != bitcoin::BlockHash::all_zeros()
        {
            return Err(Error::NoMainHeader(header.prev_blockhash));
        }
        let block_hash = header.block_hash();
        let total_work =
            if header.prev_blockhash != bitcoin::BlockHash::all_zeros() {
                let prev_work =
                    self.get_total_work(rwtxn, header.prev_blockhash)?;
                prev_work + header.work()
            } else {
                header.work()
            };
        self.main_headers.put(rwtxn, &block_hash, header)?;
        self.total_work.put(rwtxn, &block_hash, &total_work)?;
        Ok(())
    }

    /// Return a fallible iterator over ancestors of a block,
    /// starting with the specified block's header
    pub fn ancestors<'a>(
        &'a self,
        rotxn: &'a RoTxn,
        mut block_hash: BlockHash,
    ) -> impl FallibleIterator<Item = BlockHash, Error = Error> + 'a {
        fallible_iterator::from_fn(move || {
            if block_hash == BlockHash::default() {
                Ok(None)
            } else {
                let res = Some(block_hash);
                let header = self.get_header(rotxn, block_hash)?;
                block_hash = header.prev_side_hash;
                Ok(res)
            }
        })
    }

    /// Returns true if the second specified block is a descendant of the first
    /// specified block
    /// Returns an error if either of the specified block headers do not exist
    /// in the archive.
    pub fn is_descendant(
        &self,
        rotxn: &RoTxn,
        ancestor: BlockHash,
        descendant: BlockHash,
    ) -> Result<bool, Error> {
        if ancestor == descendant {
            return Ok(true);
        }
        let ancestor_height = self.get_height(rotxn, ancestor)?;
        let descendant_height = self.get_height(rotxn, descendant)?;
        if ancestor_height > descendant_height {
            return Ok(false);
        }
        self.ancestors(rotxn, descendant)
            .skip(1)
            .take((descendant_height - ancestor_height) as usize)
            .any(|block_hash| Ok(block_hash == ancestor))
    }

    /// Return a fallible iterator over ancestors of a mainchain block,
    /// starting with the specified block's header
    pub fn main_ancestors<'a>(
        &'a self,
        rotxn: &'a RoTxn,
        mut block_hash: bitcoin::BlockHash,
    ) -> impl FallibleIterator<Item = bitcoin::BlockHash, Error = Error> + 'a
    {
        fallible_iterator::from_fn(move || {
            if block_hash == bitcoin::BlockHash::all_zeros() {
                Ok(None)
            } else {
                let res = Some(block_hash);
                let header = self.get_main_header(rotxn, block_hash)?;
                block_hash = header.prev_blockhash;
                Ok(res)
            }
        })
    }

    /// Find the last common ancestor of two blocks, if headers for both exist
    pub fn last_common_ancestor(
        &self,
        rotxn: &RoTxn,
        mut block_hash0: BlockHash,
        mut block_hash1: BlockHash,
    ) -> Result<BlockHash, Error> {
        let mut height0 = self.get_height(rotxn, block_hash0)?;
        let mut height1 = self.get_height(rotxn, block_hash1)?;
        let mut header0 = self.try_get_header(rotxn, block_hash0)?;
        let mut header1 = self.try_get_header(rotxn, block_hash1)?;
        // Find respective ancestors of block_hash0 and block_hash1 with height
        // equal to min(height0, height1)
        loop {
            match height0.cmp(&height1) {
                Ordering::Less => {
                    block_hash1 = header1.unwrap().prev_side_hash;
                    header1 = self.try_get_header(rotxn, block_hash1)?;
                    height1 -= 1;
                }
                Ordering::Greater => {
                    block_hash0 = header0.unwrap().prev_side_hash;
                    header0 = self.try_get_header(rotxn, block_hash0)?;
                    height0 -= 1;
                }
                Ordering::Equal => {
                    if block_hash0 == block_hash1 {
                        return Ok(block_hash0);
                    } else {
                        block_hash0 = header0.unwrap().prev_side_hash;
                        block_hash1 = header1.unwrap().prev_side_hash;
                        header0 = self.try_get_header(rotxn, block_hash0)?;
                        header1 = self.try_get_header(rotxn, block_hash1)?;
                        height0 -= 1;
                        height1 -= 1;
                    }
                }
            };
        }
    }
}
