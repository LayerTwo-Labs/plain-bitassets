use heed::{
    byteorder::{BigEndian, ByteOrder},
    types::{OwnedType, SerdeBincode},
    Database, RoTxn, RwTxn,
};

use crate::types::{hashes, Block, BlockHash, Body, Header, Txid};

#[derive(Clone)]
pub struct Archive {
    headers: Database<OwnedType<[u8; 4]>, SerdeBincode<Header>>,
    bodies: Database<OwnedType<[u8; 4]>, SerdeBincode<Body>>,
    hash_to_height: Database<OwnedType<[u8; 32]>, OwnedType<[u8; 4]>>,
    txid_to_block_hash: Database<OwnedType<[u8; 32]>, OwnedType<[u8; 32]>>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("heed error")]
    Heed(#[from] heed::Error),
    #[error("invalid previous side hash")]
    InvalidPrevSideHash,
    #[error("invalid merkle root")]
    InvalidMerkleRoot,
    #[error("no header with hash {0}")]
    NoHeader(BlockHash),
    #[error("no tx with txid {0}")]
    NoTx(Txid),
}

impl Archive {
    pub const NUM_DBS: u32 = 4;

    pub fn new(env: &heed::Env) -> Result<Self, Error> {
        let headers = env.create_database(Some("headers"))?;
        let bodies = env.create_database(Some("bodies"))?;
        let hash_to_height = env.create_database(Some("hash_to_height"))?;
        let txid_to_block_hash =
            env.create_database(Some("txid_to_block_hash"))?;
        Ok(Self {
            headers,
            bodies,
            hash_to_height,
            txid_to_block_hash,
        })
    }

    pub fn get_header(
        &self,
        txn: &RoTxn,
        height: u32,
    ) -> Result<Option<Header>, Error> {
        let height = height.to_be_bytes();
        let header = self.headers.get(txn, &height)?;
        Ok(header)
    }

    pub fn get_body(
        &self,
        txn: &RoTxn,
        height: u32,
    ) -> Result<Option<Body>, Error> {
        let height = height.to_be_bytes();
        let header = self.bodies.get(txn, &height)?;
        Ok(header)
    }

    pub fn get_best_hash(&self, txn: &RoTxn) -> Result<BlockHash, Error> {
        let best_hash = match self.headers.last(txn)? {
            Some((_, header)) => hashes::hash(&header).into(),
            None => [0; 32].into(),
        };
        Ok(best_hash)
    }

    pub fn get_height(&self, txn: &RoTxn) -> Result<u32, Error> {
        let height = match self.headers.last(txn)? {
            Some((height, _)) => BigEndian::read_u32(&height),
            None => 0,
        };
        Ok(height)
    }

    /** Get the height of a block from it's hash.
     *  Returns [`None`] if no block with the specified hash exists. */
    pub fn try_get_block_height(
        &self,
        txn: &RoTxn,
        block_hash: BlockHash,
    ) -> Result<Option<u32>, Error> {
        let res = self
            .hash_to_height
            .get(txn, &block_hash.into())?
            .map(|height| BigEndian::read_u32(&height));
        Ok(res)
    }

    /** Get the height of a block from it's hash.
     *  Returns an error if no block with the specified hash exists. */
    pub fn get_block_height(
        &self,
        txn: &RoTxn,
        block_hash: BlockHash,
    ) -> Result<u32, Error> {
        if let Some(block_height) =
            self.try_get_block_height(txn, block_hash)?
        {
            Ok(block_height)
        } else {
            Err(Error::NoHeader(block_hash))
        }
    }

    pub fn get_block(
        &self,
        txn: &RoTxn,
        block_hash: BlockHash,
    ) -> Result<Block, Error> {
        let height = self.get_block_height(txn, block_hash)?;
        let header = self.get_header(txn, height)?.unwrap();
        let body = self.get_body(txn, height)?.unwrap();
        let block = Block {
            header,
            body,
            height,
        };
        Ok(block)
    }

    /** Returns the height of the block in which the tx was included,
     * if it was included */
    pub fn try_get_tx_height(
        &self,
        txn: &RoTxn,
        txid: Txid,
    ) -> Result<Option<u32>, Error> {
        let Some(block_hash) =
            self.txid_to_block_hash.get(txn, &txid.into())?
        else {
            return Ok(None);
        };
        let height = self.get_block_height(txn, block_hash.into())?;
        Ok(Some(height))
    }

    /** Returns the height of the block in which the tx was included.
     *  Returns an error if the tx does not exist in any known block. */
    pub fn get_tx_height(&self, txn: &RoTxn, txid: Txid) -> Result<u32, Error> {
        if let Some(height) = self.try_get_tx_height(txn, txid)? {
            Ok(height)
        } else {
            Err(Error::NoTx(txid))
        }
    }

    pub fn put_body(
        &self,
        txn: &mut RwTxn,
        header: &Header,
        body: &Body,
    ) -> Result<(), Error> {
        if header.merkle_root != body.compute_merkle_root() {
            return Err(Error::InvalidMerkleRoot);
        }
        let block_hash = header.hash();
        let height = self
            .hash_to_height
            .get(txn, &block_hash.into())?
            .ok_or(Error::NoHeader(block_hash))?;
        self.bodies.put(txn, &height, body)?;
        body.transactions.iter().try_for_each(|tx| {
            self.txid_to_block_hash.put(
                txn,
                &tx.txid().into(),
                &block_hash.into(),
            )
        })?;
        Ok(())
    }

    pub fn append_header(
        &self,
        txn: &mut RwTxn,
        header: &Header,
    ) -> Result<(), Error> {
        let height = self.get_height(txn)?;
        let best_hash = self.get_best_hash(txn)?;
        if header.prev_side_hash != best_hash {
            return Err(Error::InvalidPrevSideHash);
        }
        let new_height = (height + 1).to_be_bytes();
        self.headers.put(txn, &new_height, header)?;
        self.hash_to_height
            .put(txn, &header.hash().into(), &new_height)?;
        Ok(())
    }
}
