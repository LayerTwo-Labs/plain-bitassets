use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    path::Path,
};

use bip300301::bitcoin;
use byteorder::{BigEndian, ByteOrder};
use ed25519_dalek_bip32::{ChildIndex, DerivationPath, ExtendedSigningKey};
use futures::{Stream, StreamExt};
use heed::{
    types::{Bytes, SerdeBincode, Str, U8},
    RoTxn,
};
use tokio_stream::{wrappers::WatchStream, StreamMap};

use crate::{
    authorization::{self, get_address, Authorization},
    types::{
        Address, AssetId, AuthorizedTransaction, BitAssetData, BitAssetId,
        BitcoinOutputContent, DutchAuctionId, DutchAuctionParams, FilledOutput,
        GetBitcoinValue, Hash, InPoint, OutPoint, Output, OutputContent,
        SpentOutput, Transaction, TxData,
    },
    util::{EnvExt, Watchable, WatchableDb},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("address {address} does not exist")]
    AddressDoesNotExist { address: crate::types::Address },
    #[error("authorization error")]
    Authorization(#[from] crate::authorization::Error),
    #[error("bip32 error")]
    Bip32(#[from] ed25519_dalek_bip32::Error),
    #[error("heed error")]
    Heed(#[from] heed::Error),
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("no index for address {address}")]
    NoIndex { address: Address },
    #[error("wallet doesn't have a seed")]
    NoSeed,
    #[error("could not find bitasset reservation for `{plain_name}`")]
    NoBitassetReservation { plain_name: String },
    #[error("not enough funds")]
    NotEnoughFunds,
    #[error("utxo doesn't exist")]
    NoUtxo,
    #[error("failed to parse mnemonic seed phrase")]
    ParseMnemonic(#[source] anyhow::Error),
    #[error("seed has already been set")]
    SeedAlreadyExists,
}

#[derive(Clone)]
pub struct Wallet {
    env: heed::Env,
    // Seed is always [u8; 64], but due to serde not implementing serialize
    // for [T; 64], use heed's `Bytes`
    // TODO: Don't store the seed in plaintext.
    seed: WatchableDb<U8, Bytes>,
    /// Map each address to it's index
    address_to_index: WatchableDb<SerdeBincode<Address>, SerdeBincode<[u8; 4]>>,
    /// Map each address index to an address
    index_to_address: WatchableDb<SerdeBincode<[u8; 4]>, SerdeBincode<Address>>,
    unconfirmed_utxos:
        WatchableDb<SerdeBincode<OutPoint>, SerdeBincode<Output>>,
    utxos: WatchableDb<SerdeBincode<OutPoint>, SerdeBincode<FilledOutput>>,
    stxos: WatchableDb<SerdeBincode<OutPoint>, SerdeBincode<SpentOutput>>,
    spent_unconfirmed_utxos: WatchableDb<
        SerdeBincode<OutPoint>,
        SerdeBincode<SpentOutput<OutputContent>>,
    >,
    /// Associates reservation commitments with plaintext BitAsset names
    bitasset_reservations: WatchableDb<SerdeBincode<[u8; 32]>, Str>,
    /// Associates BitAssets with plaintext names
    known_bitassets: WatchableDb<SerdeBincode<BitAssetId>, Str>,
}

impl Wallet {
    pub const NUM_DBS: u32 = 9;

    pub fn new(path: &Path) -> Result<Self, Error> {
        std::fs::create_dir_all(path)?;
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .max_dbs(Self::NUM_DBS)
                .open(path)?
        };
        let mut rwtxn = env.write_txn()?;
        let seed_db = env.create_watchable_db(&mut rwtxn, "seed")?;
        let address_to_index =
            env.create_watchable_db(&mut rwtxn, "address_to_index")?;
        let index_to_address =
            env.create_watchable_db(&mut rwtxn, "index_to_address")?;
        let unconfirmed_utxos =
            env.create_watchable_db(&mut rwtxn, "unconfirmed_utxos")?;
        let utxos = env.create_watchable_db(&mut rwtxn, "utxos")?;
        let stxos = env.create_watchable_db(&mut rwtxn, "stxos")?;
        let spent_unconfirmed_utxos =
            env.create_watchable_db(&mut rwtxn, "spent_unconfirmed_utxos")?;
        let bitasset_reservations =
            env.create_watchable_db(&mut rwtxn, "bitasset_reservations")?;
        let known_bitassets =
            env.create_watchable_db(&mut rwtxn, "known_bitassets")?;
        rwtxn.commit()?;
        Ok(Self {
            env,
            seed: seed_db,
            address_to_index,
            index_to_address,
            unconfirmed_utxos,
            utxos,
            stxos,
            spent_unconfirmed_utxos,
            bitasset_reservations,
            known_bitassets,
        })
    }

    fn get_signing_key(
        &self,
        rotxn: &RoTxn,
        index: u32,
    ) -> Result<ed25519_dalek::SigningKey, Error> {
        let seed = self.seed.try_get(rotxn, &0)?.ok_or(Error::NoSeed)?;
        let xpriv = ExtendedSigningKey::from_seed(seed)?;
        let derivation_path = DerivationPath::new([
            ChildIndex::Hardened(1),
            ChildIndex::Hardened(0),
            ChildIndex::Hardened(0),
            ChildIndex::Hardened(index),
        ]);
        let xsigning_key = xpriv.derive(&derivation_path)?;
        Ok(xsigning_key.signing_key)
    }

    // get the signing key that corresponds to the provided address
    fn get_signing_key_for_addr(
        &self,
        rotxn: &RoTxn,
        address: &Address,
    ) -> Result<ed25519_dalek::SigningKey, Error> {
        let addr_idx = self
            .address_to_index
            .try_get(rotxn, address)?
            .ok_or(Error::AddressDoesNotExist { address: *address })?;
        let signing_key =
            self.get_signing_key(rotxn, u32::from_be_bytes(addr_idx))?;
        // sanity check that signing key corresponds to address
        assert_eq!(*address, get_address(&signing_key.verifying_key()));
        Ok(signing_key)
    }

    pub fn get_new_address(&self) -> Result<Address, Error> {
        let mut txn = self.env.write_txn()?;
        let (last_index, _) = self
            .index_to_address
            .last(&txn)?
            .unwrap_or(([0; 4], [0; 20].into()));
        let last_index = BigEndian::read_u32(&last_index);
        let index = last_index + 1;
        let signing_key = self.get_signing_key(&txn, index)?;
        let address = get_address(&signing_key.verifying_key());
        let index = index.to_be_bytes();
        self.index_to_address.put(&mut txn, &index, &address)?;
        self.address_to_index.put(&mut txn, &address, &index)?;
        txn.commit()?;
        Ok(address)
    }

    /// Overwrite the seed, or set it if it does not already exist.
    pub fn overwrite_seed(&self, seed: &[u8; 64]) -> Result<(), Error> {
        let mut rwtxn = self.env.write_txn()?;
        self.seed.put(&mut rwtxn, &0, seed)?;
        self.address_to_index.clear(&mut rwtxn)?;
        self.index_to_address.clear(&mut rwtxn)?;
        self.unconfirmed_utxos.clear(&mut rwtxn)?;
        self.utxos.clear(&mut rwtxn)?;
        self.stxos.clear(&mut rwtxn)?;
        self.spent_unconfirmed_utxos.clear(&mut rwtxn)?;
        self.bitasset_reservations.clear(&mut rwtxn)?;
        rwtxn.commit()?;
        Ok(())
    }

    pub fn has_seed(&self) -> Result<bool, Error> {
        let rotxn = self.env.read_txn()?;
        Ok(self.seed.try_get(&rotxn, &0)?.is_some())
    }

    /// Set the seed, if it does not already exist
    pub fn set_seed(&self, seed: &[u8; 64]) -> Result<(), Error> {
        if self.has_seed()? {
            Err(Error::SeedAlreadyExists)
        } else {
            self.overwrite_seed(seed)
        }
    }

    /// Set the seed from a mnemonic seed phrase,
    /// if the seed does not already exist
    pub fn set_seed_from_mnemonic(&self, mnemonic: &str) -> Result<(), Error> {
        let mnemonic =
            bip39::Mnemonic::from_phrase(mnemonic, bip39::Language::English)
                .map_err(Error::ParseMnemonic)?;
        let seed = bip39::Seed::new(&mnemonic, "");
        let seed_bytes: [u8; 64] = seed.as_bytes().try_into().unwrap();
        self.set_seed(&seed_bytes)
    }

    /// Create a transaction with a fee only.
    pub fn create_regular_transaction(
        &self,
        fee: u64,
    ) -> Result<Transaction, Error> {
        let (total, coins) = self.select_bitcoins(fee)?;
        let change = total - fee;
        let inputs = coins.into_keys().collect();
        let outputs = vec![Output::new(
            self.get_new_address()?,
            OutputContent::Value(BitcoinOutputContent(change)),
        )];
        Ok(Transaction::new(inputs, outputs))
    }

    pub fn create_withdrawal(
        &self,
        main_address: bitcoin::Address<bitcoin::address::NetworkUnchecked>,
        value: u64,
        main_fee: u64,
        fee: u64,
    ) -> Result<Transaction, Error> {
        let (total, coins) = self.select_bitcoins(value + fee + main_fee)?;
        let change = total - value - fee;
        let inputs = coins.into_keys().collect();
        let outputs = vec![
            Output::new(
                self.get_new_address()?,
                OutputContent::Withdrawal {
                    value,
                    main_fee,
                    main_address,
                },
            ),
            Output::new(
                self.get_new_address()?,
                OutputContent::Value(BitcoinOutputContent(change)),
            ),
        ];
        Ok(Transaction::new(inputs, outputs))
    }

    pub fn create_transfer(
        &self,
        address: Address,
        bitcoin_value: u64,
        fee: u64,
        memo: Option<Vec<u8>>,
    ) -> Result<Transaction, Error> {
        let (total, coins) = self.select_bitcoins(bitcoin_value + fee)?;
        let change = total - bitcoin_value - fee;
        let inputs = coins.into_keys().collect();
        let outputs = vec![
            Output {
                address,
                content: OutputContent::Value(BitcoinOutputContent(
                    bitcoin_value,
                )),
                memo: memo.unwrap_or_default(),
            },
            Output::new(
                self.get_new_address()?,
                OutputContent::Value(BitcoinOutputContent(change)),
            ),
        ];
        Ok(Transaction::new(inputs, outputs))
    }

    /// given a regular transaction, add a bitasset reservation.
    /// given a bitasset reservation tx, change the reserved name.
    /// panics if the tx is not regular or a bitasset reservation tx.
    pub fn reserve_bitasset(
        &self,
        tx: &mut Transaction,
        plain_name: &str,
    ) -> Result<(), Error> {
        assert!(
            tx.is_regular() || tx.is_reservation(),
            "this function only accepts a regular or bitasset reservation tx"
        );
        // address for the reservation output
        let reservation_addr =
            // if the tx is already bitasset reservation,
            // re-use the reservation address
            if tx.is_reservation() {
                tx.reservation_outputs().next_back()
                    .expect("A bitasset reservation tx must have at least one reservation output")
                    .address
            }
            // if the last output is owned by this wallet, then use
            // the address associated with the last output
            else if let Some(last_output) = tx.outputs.last() {
                let last_output_addr = last_output.address;
                let rotxn = self.env.read_txn()?;
                if self.address_to_index.try_get(&rotxn, &last_output_addr)?.is_some() {
                    last_output_addr
                } else {
                    self.get_new_address()?
                }
            } else {
                self.get_new_address()?
            };
        let rotxn = self.env.read_txn()?;
        let reservation_signing_key =
            self.get_signing_key_for_addr(&rotxn, &reservation_addr)?;
        let name_hash: Hash = blake3::hash(plain_name.as_bytes()).into();
        let bitasset_id = BitAssetId(name_hash);
        // hmac(secret, name_hash)
        let nonce =
            blake3::keyed_hash(reservation_signing_key.as_bytes(), &name_hash)
                .into();
        // hmac(nonce, name_hash)
        let commitment = blake3::keyed_hash(&nonce, &name_hash).into();
        // store reservation data
        let mut rwtxn = self.env.write_txn()?;
        self.bitasset_reservations
            .put(&mut rwtxn, &commitment, plain_name)?;
        self.known_bitassets
            .put(&mut rwtxn, &bitasset_id, plain_name)?;
        rwtxn.commit()?;
        // if the tx is regular, add a reservation output
        if tx.is_regular() {
            let reservation_output = Output::new(
                reservation_addr,
                OutputContent::BitAssetReservation,
            );
            tx.outputs.push(reservation_output);
        };
        tx.data = Some(TxData::BitAssetReservation { commitment });
        Ok(())
    }

    /// given a regular transaction, add a bitasset registration.
    /// panics if the tx is not regular.
    /// returns an error if there is no corresponding reservation utxo
    /// does not modify the tx if there is no corresponding reservation utxo.
    pub fn register_bitasset(
        &self,
        tx: &mut Transaction,
        plain_name: &str,
        bitasset_data: Cow<BitAssetData>,
        initial_supply: u64,
    ) -> Result<(), Error> {
        assert!(tx.is_regular(), "this function only accepts a regular tx");
        // address for the registration output
        let registration_addr =
            // if the last output is owned by this wallet, then use
            // the address associated with the last output
            if let Some(last_output) = tx.outputs.last() {
                let last_output_addr = last_output.address;
                let rotxn = self.env.read_txn()?;
                if self.address_to_index.try_get(&rotxn, &last_output_addr)?.is_some() {
                    last_output_addr
                } else {
                    self.get_new_address()?
                }
            } else {
                self.get_new_address()?
            };
        let name_hash: Hash = blake3::hash(plain_name.as_bytes()).into();
        let bitasset_id = BitAssetId(name_hash);
        /* Search for reservation utxo by the following procedure:
        For each reservation:
        * Get the corresponding signing key
        * Compute a reservation commitment for the bitasset to be registered
        * If the computed commitment is the same as the reservation commitment,
          then use this utxo. Otherwise, continue */
        // outpoint and nonce, if found
        let mut reservation_outpoint_nonce: Option<(OutPoint, Hash)> = None;
        for (outpoint, filled_output) in self.get_utxos()?.into_iter() {
            if let Some(reservation_commitment) =
                filled_output.reservation_commitment()
            {
                // for each reservation, get the signing key, and
                let reservation_addr = filled_output.address;
                let rotxn = self.env.read_txn()?;
                let reservation_signing_key =
                    self.get_signing_key_for_addr(&rotxn, &reservation_addr)?;
                // hmac(secret, name_hash)
                let nonce = blake3::keyed_hash(
                    reservation_signing_key.as_bytes(),
                    &name_hash,
                )
                .into();
                // hmac(nonce, name_hash)
                let commitment = blake3::keyed_hash(&nonce, &name_hash);
                // WARNING: This comparison MUST be done in constant time.
                // `blake3::Hash` handles this; DO NOT compare as byte arrays
                if commitment == *reservation_commitment {
                    reservation_outpoint_nonce = Some((outpoint, nonce));
                    break;
                }
            }
        }
        // store bitasset data
        let mut rwtxn = self.env.write_txn()?;
        self.known_bitassets
            .put(&mut rwtxn, &bitasset_id, plain_name)?;
        rwtxn.commit()?;
        let (reservation_outpoint, nonce) = reservation_outpoint_nonce
            .ok_or_else(|| Error::NoBitassetReservation {
                plain_name: plain_name.to_owned(),
            })?;
        tx.inputs.push(reservation_outpoint);
        if initial_supply != 0 {
            let mint_output = Output::new(
                registration_addr,
                OutputContent::BitAsset(initial_supply),
            );
            tx.outputs.push(mint_output);
        };
        let control_coin_output =
            Output::new(registration_addr, OutputContent::BitAssetControl);
        tx.outputs.push(control_coin_output);
        tx.data = Some(TxData::BitAssetRegistration {
            name_hash,
            revealed_nonce: nonce,
            bitasset_data: Box::new(bitasset_data.into_owned()),
            initial_supply,
        });
        Ok(())
    }

    pub fn select_bitcoins(
        &self,
        value: u64,
    ) -> Result<(u64, HashMap<OutPoint, Output>), Error> {
        let txn = self.env.read_txn()?;
        let mut bitcoin_utxos = Vec::<(_, Output)>::new();
        for item in self.utxos.iter(&txn)? {
            let (outpoint, output) = item?;
            if output.is_bitcoin() {
                bitcoin_utxos.push((outpoint, output.into()));
            }
        }
        bitcoin_utxos
            .sort_unstable_by_key(|(_, output)| output.get_bitcoin_value());
        let mut unconfirmed_bitcoin_utxos = Vec::new();
        for item in self.unconfirmed_utxos.iter(&txn)? {
            let (outpoint, output) = item?;
            if output.is_bitcoin() {
                unconfirmed_bitcoin_utxos.push((outpoint, output));
            }
        }
        unconfirmed_bitcoin_utxos
            .sort_unstable_by_key(|(_, output)| output.get_bitcoin_value());

        let mut selected = HashMap::new();
        let mut total_sats: u64 = 0;
        for (outpoint, output) in
            bitcoin_utxos.into_iter().chain(unconfirmed_bitcoin_utxos)
        {
            if output.content.is_withdrawal() {
                continue;
            }
            if total_sats >= value {
                break;
            }
            total_sats += output.get_bitcoin_value();
            selected.insert(outpoint, output.clone());
        }
        if total_sats >= value {
            Ok((total_sats, selected))
        } else {
            Err(Error::NotEnoughFunds)
        }
    }

    // Select UTXOs for the specified BitAsset
    pub fn select_bitasset_utxos(
        &self,
        bitasset: BitAssetId,
        value: u64,
    ) -> Result<(u64, HashMap<OutPoint, Output>), Error> {
        let txn = self.env.read_txn()?;
        let mut bitasset_utxos = vec![];
        for item in self.utxos.iter(&txn)? {
            let (outpoint, output) = item?;
            if let Some(output_bitasset) = output.bitasset()
                && bitasset == *output_bitasset
            {
                bitasset_utxos.push((outpoint, output));
            }
        }
        bitasset_utxos.sort_unstable_by_key(|(_, output)| {
            output
                .bitasset_value()
                .map(|(_, bitasset_value)| bitasset_value)
        });

        let mut selected = HashMap::new();
        let mut total_value: u64 = 0;
        for (outpoint, output) in &bitasset_utxos {
            if output.content.is_withdrawal() {
                continue;
            }
            if total_value > value {
                break;
            }
            let (_, bitasset_value) = output.bitasset_value().unwrap();
            total_value += bitasset_value;
            selected.insert(*outpoint, output.clone().into());
        }
        if total_value < value {
            return Err(Error::NotEnoughFunds);
        }
        Ok((total_value, selected))
    }

    // Select control coin for the specified BitAsset
    pub fn select_bitasset_control(
        &self,
        bitasset: BitAssetId,
    ) -> Result<(OutPoint, Output), Error> {
        let txn = self.env.read_txn()?;
        let mut bitasset_utxo = None;
        for item in self.utxos.iter(&txn)? {
            let (outpoint, output) = item?;
            if let Some(output_bitasset) = output.bitasset()
                && bitasset == *output_bitasset
            {
                bitasset_utxo = Some((outpoint, output.into()));
                break;
            }
        }
        bitasset_utxo.ok_or(Error::NotEnoughFunds)
    }

    pub fn select_asset_utxos(
        &self,
        asset: AssetId,
        amount: u64,
    ) -> Result<(u64, HashMap<OutPoint, Output>), Error> {
        match asset {
            AssetId::Bitcoin => self.select_bitcoins(amount),
            AssetId::BitAsset(bitasset) => {
                self.select_bitasset_utxos(bitasset, amount)
            }
            AssetId::BitAssetControl(bitasset) => {
                if amount == 1 {
                    let utxo = self.select_bitasset_control(bitasset)?;
                    Ok((1, HashMap::from_iter([utxo])))
                } else {
                    do yeet Error::NotEnoughFunds
                }
            }
        }
    }

    // Select control coin for the specified LP token
    pub fn select_amm_lp_tokens(
        &self,
        asset0: AssetId,
        asset1: AssetId,
        amount: u64,
    ) -> Result<(u64, HashMap<OutPoint, FilledOutput>), Error> {
        let txn = self.env.read_txn()?;
        let mut amm_lp_token_utxos = vec![];
        for item in self.utxos.iter(&txn)? {
            let (outpoint, output) = item?;
            if let Some((pool_asset0, pool_asset1, _)) =
                output.lp_token_amount()
                && pool_asset0 == asset0
                && pool_asset1 == asset1
            {
                amm_lp_token_utxos.push((outpoint, output));
            }
        }
        amm_lp_token_utxos.sort_unstable_by_key(|(_, output)| {
            output.lp_token_amount().map(|(_, _, amount)| amount)
        });

        let mut selected = HashMap::new();
        let mut total_amount: u64 = 0;
        for (outpoint, output) in &amm_lp_token_utxos {
            if total_amount > amount {
                break;
            }
            let (_, _, lp_token_amount) = output.lp_token_amount().unwrap();
            total_amount += lp_token_amount;
            selected.insert(*outpoint, output.clone());
        }
        if total_amount < amount {
            return Err(Error::NotEnoughFunds);
        }
        Ok((total_amount, selected))
    }

    // Select dutch auction receipt utxo for the specified auction
    pub fn select_dutch_auction_receipt(
        &self,
        auction_id: DutchAuctionId,
    ) -> Result<(OutPoint, FilledOutput), Error> {
        let txn = self.env.read_txn()?;
        let mut receipt_utxo = None;
        for item in self.utxos.iter(&txn)? {
            let (outpoint, output) = item?;
            if let Some(output_auction_id) = output.dutch_auction_receipt()
                && auction_id == output_auction_id
            {
                receipt_utxo = Some((outpoint, output));
                break;
            }
        }
        receipt_utxo.ok_or(Error::NotEnoughFunds)
    }

    /// Given a regular transaction, add an AMM mint.
    pub fn amm_mint(
        &self,
        tx: &mut Transaction,
        asset0: AssetId,
        asset1: AssetId,
        amount0: u64,
        amount1: u64,
        lp_token_mint: u64,
    ) -> Result<(), Error> {
        assert!(tx.is_regular(), "this function only accepts a regular tx");
        // address for the LP token output
        let lp_token_addr = self.get_new_address()?;

        let (input_amount0, asset0_utxos) =
            self.select_asset_utxos(asset0, amount0)?;
        let (input_amount1, asset1_utxos) =
            self.select_asset_utxos(asset1, amount1)?;

        let change_amount0 = input_amount0 - amount0;
        let change_amount1 = input_amount1 - amount1;
        let change_output0 = if change_amount0 != 0 {
            let address = self.get_new_address()?;
            let content = match asset0 {
                AssetId::Bitcoin => {
                    OutputContent::Value(BitcoinOutputContent(change_amount0))
                }
                AssetId::BitAsset(_) => OutputContent::BitAsset(change_amount0),
                AssetId::BitAssetControl(_) => OutputContent::BitAssetControl,
            };
            Some(Output {
                address,
                memo: Vec::new(),
                content,
            })
        } else {
            None
        };
        let change_output1 = if change_amount1 != 0 {
            let address = self.get_new_address()?;
            let content = match asset1 {
                AssetId::Bitcoin => {
                    OutputContent::Value(BitcoinOutputContent(change_amount1))
                }
                AssetId::BitAsset(_) => OutputContent::BitAsset(change_amount1),
                AssetId::BitAssetControl(_) => OutputContent::BitAssetControl,
            };
            Some(Output {
                address,
                memo: Vec::new(),
                content,
            })
        } else {
            None
        };
        let lp_token_output = Output {
            address: lp_token_addr,
            content: OutputContent::AmmLpToken(lp_token_mint),
            memo: Vec::new(),
        };

        /* The first two unique assets in the inputs must be
         * `asset0` and `asset1` */
        tx.inputs.extend(asset0_utxos.keys());
        tx.inputs.extend(asset1_utxos.keys());
        tx.inputs
            .rotate_right(asset0_utxos.len() + asset1_utxos.len());

        tx.outputs.extend(change_output0);
        tx.outputs.extend(change_output1);
        tx.outputs.push(lp_token_output);

        tx.data = Some(TxData::AmmMint {
            amount0,
            amount1,
            lp_token_mint,
        });
        Ok(())
    }

    // Given a regular transaction, add an AMM burn.
    pub fn amm_burn(
        &self,
        tx: &mut Transaction,
        asset0: AssetId,
        asset1: AssetId,
        amount0: u64,
        amount1: u64,
        lp_token_burn: u64,
    ) -> Result<(), Error> {
        assert!(tx.is_regular(), "this function only accepts a regular tx");
        // address for receiving asset0
        let asset0_addr = self.get_new_address()?;
        // address for receiving asset1
        let asset1_addr = self.get_new_address()?;

        let (input_lp_token_amount, lp_token_utxos) =
            self.select_amm_lp_tokens(asset0, asset1, lp_token_burn)?;

        let lp_token_change_amount = input_lp_token_amount - lp_token_burn;
        let lp_token_change_output = if lp_token_change_amount != 0 {
            let address = self.get_new_address()?;
            Some(Output {
                address,
                content: OutputContent::AmmLpToken(lp_token_change_amount),
                memo: Vec::new(),
            })
        } else {
            None
        };
        let asset0_output = Output {
            address: asset0_addr,
            memo: Vec::new(),
            content: match asset0 {
                AssetId::Bitcoin => {
                    OutputContent::Value(BitcoinOutputContent(amount0))
                }
                AssetId::BitAsset(_) => OutputContent::BitAsset(amount0),
                AssetId::BitAssetControl(_) => OutputContent::BitAssetControl,
            },
        };
        let asset1_output = Output {
            address: asset1_addr,
            memo: Vec::new(),
            content: match asset1 {
                AssetId::Bitcoin => {
                    OutputContent::Value(BitcoinOutputContent(amount1))
                }
                AssetId::BitAsset(_) => OutputContent::BitAsset(amount1),
                AssetId::BitAssetControl(_) => OutputContent::BitAssetControl,
            },
        };

        /* The AMM lp token input must occur before any other AMM lp token
         * inputs. */
        tx.inputs.extend(lp_token_utxos.keys());
        tx.inputs.rotate_right(lp_token_utxos.len());

        tx.outputs.extend(lp_token_change_output);
        tx.outputs.push(asset0_output);
        tx.outputs.push(asset1_output);

        tx.data = Some(TxData::AmmBurn {
            amount0,
            amount1,
            lp_token_burn,
        });
        Ok(())
    }

    // Given a regular transaction, add an AMM swap.
    pub fn amm_swap(
        &self,
        tx: &mut Transaction,
        asset_spend: AssetId,
        asset_receive: AssetId,
        amount_spend: u64,
        amount_receive: u64,
    ) -> Result<(), Error> {
        assert!(tx.is_regular(), "this function only accepts a regular tx");
        // Address for receiving `asset_receive`
        let receive_addr = self.get_new_address()?;
        let (input_amount_spend, spend_utxos) =
            self.select_asset_utxos(asset_spend, amount_spend)?;
        let amount_change = input_amount_spend - amount_spend;
        let change_output = if amount_change != 0 {
            let address = self.get_new_address()?;
            let content = match asset_spend {
                AssetId::Bitcoin => {
                    OutputContent::Value(BitcoinOutputContent(amount_change))
                }
                AssetId::BitAsset(_) => OutputContent::BitAsset(amount_change),
                AssetId::BitAssetControl(_) => OutputContent::BitAssetControl,
            };
            Some(Output {
                address,
                memo: Vec::new(),
                content,
            })
        } else {
            None
        };
        let receive_output = Output {
            address: receive_addr,
            memo: Vec::new(),
            content: match asset_receive {
                AssetId::Bitcoin => {
                    OutputContent::Value(BitcoinOutputContent(amount_receive))
                }
                AssetId::BitAsset(_) => OutputContent::BitAsset(amount_receive),
                AssetId::BitAssetControl(_) => OutputContent::BitAssetControl,
            },
        };
        // The first unique asset in the inputs must be `asset_spend`.
        tx.inputs.extend(spend_utxos.keys());
        tx.inputs.rotate_right(spend_utxos.len());
        tx.outputs.extend(change_output);
        tx.outputs.push(receive_output);
        tx.data = Some(TxData::AmmSwap {
            amount_spent: amount_spend,
            amount_receive,
            pair_asset: asset_receive,
        });
        Ok(())
    }

    /// Given a regular transaction, create a dutch auction tx
    pub fn dutch_auction_create(
        &self,
        tx: &mut Transaction,
        dutch_auction_params: DutchAuctionParams,
    ) -> Result<(), Error> {
        assert!(tx.is_regular(), "this function only accepts a regular tx");
        let (input_base_amount, base_utxos) = self.select_asset_utxos(
            dutch_auction_params.base_asset,
            dutch_auction_params.base_amount,
        )?;
        let change_amount =
            input_base_amount - dutch_auction_params.base_amount;
        let change_output = if change_amount != 0 {
            let address = self.get_new_address()?;
            let content = match dutch_auction_params.base_asset {
                AssetId::Bitcoin => {
                    OutputContent::Value(BitcoinOutputContent(change_amount))
                }
                AssetId::BitAsset(_) => OutputContent::BitAsset(change_amount),
                AssetId::BitAssetControl(_) => OutputContent::BitAssetControl,
            };
            Some(Output {
                address,
                memo: Vec::new(),
                content,
            })
        } else {
            None
        };
        let dutch_auction_receipt = Output {
            address: self.get_new_address()?,
            memo: Vec::new(),
            content: OutputContent::DutchAuctionReceipt,
        };
        tx.inputs.extend(base_utxos.keys());
        tx.outputs.extend(change_output);
        tx.outputs.push(dutch_auction_receipt);
        tx.data = Some(TxData::DutchAuctionCreate(dutch_auction_params));
        Ok(())
    }

    /// Given a regular transaction, create a dutch auction bid
    pub fn dutch_auction_bid(
        &self,
        tx: &mut Transaction,
        auction_id: DutchAuctionId,
        base_asset: AssetId,
        quote_asset: AssetId,
        bid_size: u64,
        receive_quantity: u64,
    ) -> Result<(), Error> {
        assert!(tx.is_regular(), "this function only accepts a regular tx");
        let (input_quote_amount, quote_utxos) =
            self.select_asset_utxos(quote_asset, bid_size)?;
        let change_amount = input_quote_amount - bid_size;
        let change_output = if change_amount != 0 {
            let address = self.get_new_address()?;
            let content = match quote_asset {
                AssetId::Bitcoin => {
                    OutputContent::Value(BitcoinOutputContent(change_amount))
                }
                AssetId::BitAsset(_) => OutputContent::BitAsset(change_amount),
                AssetId::BitAssetControl(_) => OutputContent::BitAssetControl,
            };
            Some(Output {
                address,
                memo: Vec::new(),
                content,
            })
        } else {
            None
        };
        let base_output = {
            let content = match base_asset {
                AssetId::Bitcoin => {
                    OutputContent::Value(BitcoinOutputContent(change_amount))
                }
                AssetId::BitAsset(_) => OutputContent::BitAsset(change_amount),
                AssetId::BitAssetControl(_) => OutputContent::BitAssetControl,
            };
            Output {
                address: self.get_new_address()?,
                memo: Vec::new(),
                content,
            }
        };

        // The first unique asset in the inputs must be `quote_asset`.
        tx.inputs.extend(quote_utxos.keys());
        tx.inputs.rotate_right(quote_utxos.len());

        tx.outputs.push(base_output);
        tx.outputs.extend(change_output);
        tx.data = Some(TxData::DutchAuctionBid {
            auction_id,
            receive_asset: base_asset,
            quantity: receive_quantity,
            bid_size,
        });
        Ok(())
    }

    /// Given a regular transaction, create a dutch auction collect tx
    pub fn dutch_auction_collect(
        &self,
        tx: &mut Transaction,
        auction_id: DutchAuctionId,
        base_asset: AssetId,
        quote_asset: AssetId,
        amount_base: u64,
        amount_quote: u64,
    ) -> Result<(), Error> {
        assert!(tx.is_regular(), "this function only accepts a regular tx");
        let (dutch_auction_receipt_input, _) =
            self.select_dutch_auction_receipt(auction_id)?;
        let base_output = if amount_base != 0 {
            let address = self.get_new_address()?;
            let content = match base_asset {
                AssetId::Bitcoin => {
                    OutputContent::Value(BitcoinOutputContent(amount_base))
                }
                AssetId::BitAsset(_) => OutputContent::BitAsset(amount_base),
                AssetId::BitAssetControl(_) => OutputContent::BitAssetControl,
            };
            Some(Output {
                address,
                memo: Vec::new(),
                content,
            })
        } else {
            None
        };
        let quote_output = if amount_quote != 0 {
            let address = self.get_new_address()?;
            let content = match quote_asset {
                AssetId::Bitcoin => {
                    OutputContent::Value(BitcoinOutputContent(amount_quote))
                }
                AssetId::BitAsset(_) => OutputContent::BitAsset(amount_quote),
                AssetId::BitAssetControl(_) => OutputContent::BitAssetControl,
            };
            Some(Output {
                address,
                memo: Vec::new(),
                content,
            })
        } else {
            None
        };

        /* The Dutch auction receipt must occur before any other Dutch auction
        receipts in the inputs. */
        tx.inputs.push(dutch_auction_receipt_input);
        tx.inputs.rotate_right(1);

        tx.outputs.extend(base_output);
        tx.outputs.extend(quote_output);
        tx.data = Some(TxData::DutchAuctionCollect {
            asset_offered: base_asset,
            asset_receive: quote_asset,
            amount_offered_remaining: amount_base,
            amount_received: amount_quote,
        });
        Ok(())
    }

    pub fn spend_utxos(
        &self,
        spent: &[(OutPoint, InPoint)],
    ) -> Result<(), Error> {
        let mut txn = self.env.write_txn()?;
        for (outpoint, inpoint) in spent {
            if let Some(output) = self.utxos.try_get(&txn, outpoint)? {
                self.utxos.delete(&mut txn, outpoint)?;
                let spent_output = SpentOutput {
                    output,
                    inpoint: *inpoint,
                };
                self.stxos.put(&mut txn, outpoint, &spent_output)?;
            } else if let Some(output) =
                self.unconfirmed_utxos.try_get(&txn, outpoint)?
            {
                self.unconfirmed_utxos.delete(&mut txn, outpoint)?;
                let spent_output = SpentOutput {
                    output,
                    inpoint: *inpoint,
                };
                self.spent_unconfirmed_utxos.put(
                    &mut txn,
                    outpoint,
                    &spent_output,
                )?;
            } else {
                continue;
            }
        }
        txn.commit()?;
        Ok(())
    }

    pub fn put_unconfirmed_utxos(
        &self,
        utxos: &HashMap<OutPoint, Output>,
    ) -> Result<(), Error> {
        let mut txn = self.env.write_txn()?;
        for (outpoint, output) in utxos {
            self.unconfirmed_utxos.put(&mut txn, outpoint, output)?;
        }
        txn.commit()?;
        Ok(())
    }

    pub fn put_utxos(
        &self,
        utxos: &HashMap<OutPoint, FilledOutput>,
    ) -> Result<(), Error> {
        let mut txn = self.env.write_txn()?;
        for (outpoint, output) in utxos {
            self.utxos.put(&mut txn, outpoint, output)?;
        }
        txn.commit()?;
        Ok(())
    }

    pub fn get_bitcoin_balance(&self) -> Result<u64, Error> {
        let mut balance: u64 = 0;
        let txn = self.env.read_txn()?;
        for item in self.utxos.iter(&txn)? {
            let (_, utxo) = item?;
            balance += utxo.get_bitcoin_value();
        }
        Ok(balance)
    }

    /// gets the plaintext name associated with a bitasset reservation
    /// commitment, if it is known by the wallet.
    pub fn get_bitasset_reservation_plaintext(
        &self,
        commitment: &Hash,
    ) -> Result<Option<String>, Error> {
        let txn = self.env.read_txn()?;
        let res = self.bitasset_reservations.try_get(&txn, commitment)?;
        Ok(res.map(String::from))
    }

    /// gets the plaintext name associated with a bitasset,
    /// if it is known by the wallet.
    pub fn get_bitasset_plaintext(
        &self,
        bitasset: &BitAssetId,
    ) -> Result<Option<String>, Error> {
        let txn = self.env.read_txn()?;
        let res = self.known_bitassets.try_get(&txn, bitasset)?;
        Ok(res.map(String::from))
    }

    pub fn get_utxos(&self) -> Result<HashMap<OutPoint, FilledOutput>, Error> {
        let txn = self.env.read_txn()?;
        let mut utxos = HashMap::new();
        for item in self.utxos.iter(&txn)? {
            let (outpoint, output) = item?;
            utxos.insert(outpoint, output);
        }
        Ok(utxos)
    }

    pub fn get_unconfirmed_utxos(
        &self,
    ) -> Result<HashMap<OutPoint, Output>, Error> {
        let txn = self.env.read_txn()?;
        let mut utxos = HashMap::new();
        for item in self.unconfirmed_utxos.iter(&txn)? {
            let (outpoint, output) = item?;
            utxos.insert(outpoint, output);
        }
        Ok(utxos)
    }

    pub fn get_stxos(&self) -> Result<HashMap<OutPoint, SpentOutput>, Error> {
        let txn = self.env.read_txn()?;
        let mut stxos = HashMap::new();
        for item in self.stxos.iter(&txn)? {
            let (outpoint, spent_output) = item?;
            stxos.insert(outpoint, spent_output);
        }
        Ok(stxos)
    }

    pub fn get_spent_unconfirmed_utxos(
        &self,
    ) -> Result<HashMap<OutPoint, SpentOutput<OutputContent>>, Error> {
        let txn = self.env.read_txn()?;
        let mut stxos = HashMap::new();
        for item in self.spent_unconfirmed_utxos.iter(&txn)? {
            let (outpoint, spent_output) = item?;
            stxos.insert(outpoint, spent_output);
        }
        Ok(stxos)
    }

    /// get all owned bitasset utxos
    pub fn get_bitassets(
        &self,
    ) -> Result<HashMap<OutPoint, FilledOutput>, Error> {
        let mut utxos = self.get_utxos()?;
        utxos.retain(|_, output| output.is_bitasset());
        Ok(utxos)
    }

    /// get all spent bitasset utxos
    pub fn get_spent_bitassets(
        &self,
    ) -> Result<HashMap<OutPoint, SpentOutput>, Error> {
        let mut stxos = self.get_stxos()?;
        stxos.retain(|_, output| output.output.is_bitasset());
        Ok(stxos)
    }

    pub fn get_addresses(&self) -> Result<HashSet<Address>, Error> {
        let txn = self.env.read_txn()?;
        let mut addresses = HashSet::new();
        for item in self.index_to_address.iter(&txn)? {
            let (_, address) = item?;
            addresses.insert(address);
        }
        Ok(addresses)
    }

    pub fn authorize(
        &self,
        transaction: Transaction,
    ) -> Result<AuthorizedTransaction, Error> {
        let rotxn = self.env.read_txn()?;
        let mut authorizations = vec![];
        for input in &transaction.inputs {
            let spent_utxo =
                if let Some(utxo) = self.utxos.try_get(&rotxn, input)? {
                    utxo.into()
                } else if let Some(utxo) =
                    self.unconfirmed_utxos.try_get(&rotxn, input)?
                {
                    utxo
                } else {
                    return Err(Error::NoUtxo);
                };
            let index = self
                .address_to_index
                .try_get(&rotxn, &spent_utxo.address)?
                .ok_or(Error::NoIndex {
                    address: spent_utxo.address,
                })?;
            let index = BigEndian::read_u32(&index);
            let signing_key = self.get_signing_key(&rotxn, index)?;
            let signature = authorization::sign(&signing_key, &transaction)?;
            authorizations.push(Authorization {
                verifying_key: signing_key.verifying_key(),
                signature,
            });
        }
        Ok(AuthorizedTransaction {
            authorizations,
            transaction,
        })
    }

    pub fn get_num_addresses(&self) -> Result<u32, Error> {
        let txn = self.env.read_txn()?;
        let (last_index, _) = self
            .index_to_address
            .last(&txn)?
            .unwrap_or(([0; 4], [0; 20].into()));
        let last_index = BigEndian::read_u32(&last_index);
        Ok(last_index)
    }
}

impl Watchable<()> for Wallet {
    type WatchStream = impl Stream<Item = ()>;

    /// Get a signal that notifies whenever the wallet changes
    fn watch(&self) -> Self::WatchStream {
        let Self {
            env: _,
            seed,
            address_to_index,
            index_to_address,
            utxos,
            stxos,
            unconfirmed_utxos,
            spent_unconfirmed_utxos,
            bitasset_reservations,
            known_bitassets,
        } = self;
        let watchables = [
            seed.watch(),
            address_to_index.watch(),
            index_to_address.watch(),
            utxos.watch(),
            stxos.watch(),
            unconfirmed_utxos.watch(),
            spent_unconfirmed_utxos.watch(),
            bitasset_reservations.watch(),
            known_bitassets.watch(),
        ];
        let streams = StreamMap::from_iter(
            watchables.into_iter().map(WatchStream::new).enumerate(),
        );
        let streams_len = streams.len();
        streams.ready_chunks(streams_len).map(|signals| {
            assert_ne!(signals.len(), 0);
            #[allow(clippy::unused_unit)]
            ()
        })
    }
}
