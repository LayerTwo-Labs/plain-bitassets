use std::{
    borrow::Cow,
    net::{Ipv4Addr, Ipv6Addr},
    str::FromStr,
};

use borsh::BorshDeserialize;
use eframe::egui::{self, Response};
use hex::FromHex;

use plain_bitassets::{
    authorization::PublicKey,
    bip300301::bitcoin,
    state::AmmPair,
    types::{AssetId, BitAssetData, EncryptionPubKey, Hash, Transaction, Txid},
};

use crate::{app::App, gui::util::InnerResponseExt};

// struct representing the outcome of trying to set an Option<T> from a String
// Err represents unset, Ok(None) represents bad value
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrySetOption<T>(Result<Option<T>, String>);

// try to set BitAsset Data
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TrySetBitAssetData {
    /// commitment to arbitrary data
    pub commitment: TrySetOption<Hash>,
    /// optional ipv4 addr
    pub ipv4_addr: TrySetOption<Ipv4Addr>,
    /// optional ipv6 addr
    pub ipv6_addr: TrySetOption<Ipv6Addr>,
    /// optional pubkey used for encryption
    pub encryption_pubkey: TrySetOption<EncryptionPubKey>,
    /// optional pubkey used for signing messages
    pub signing_pubkey: TrySetOption<PublicKey>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DutchAuctionParams {
    /// Block height at which the auction starts
    start_block: String,
    /// Auction duration, in blocks
    duration: String,
    /// The asset to be auctioned
    base_asset: String,
    /// The amount of the base asset to be auctioned
    base_amount: String,
    /// The asset in which the auction is to be quoted
    quote_asset: String,
    /// Initial price
    initial_price: String,
    /// Final price
    final_price: String,
}
#[derive(Clone, Debug, Default, PartialEq)]
pub enum TxType {
    #[default]
    Regular,
    BitAssetRegistration {
        plaintext_name: String,
        bitasset_data: Box<TrySetBitAssetData>,
        initial_supply: String,
    },
    BitAssetReservation {
        plaintext_name: String,
    },
    DexBurn {
        asset0: String,
        asset1: String,
        amount_lp_tokens: String,
    },
    DexMint {
        asset0: String,
        asset1: String,
        amount0: String,
        amount1: String,
    },
    DexSwap {
        asset_spend: String,
        asset_receive: String,
        amount_spend: String,
        amount_receive: String,
    },
    DutchAuctionCreate {
        auction_params: DutchAuctionParams,
    },
}

#[derive(Debug, Default)]
pub struct TxCreator {
    pub bitcoin_value_in: u64,
    pub bitcoin_value_out: u64,
    pub tx_type: TxType,
    // if the base tx has changed, need to recompute final tx
    base_txid: Txid,
    final_tx: Option<anyhow::Result<Transaction>>,
}

impl<T> std::default::Default for TrySetOption<T> {
    fn default() -> Self {
        Self(Ok(None))
    }
}

impl TryFrom<TrySetBitAssetData> for BitAssetData {
    type Error = String;

    fn try_from(try_set: TrySetBitAssetData) -> Result<Self, Self::Error> {
        let commitment = try_set
            .commitment
            .0
            .map_err(|err| format!("Cannot parse commitment: \"{err}\""))?;
        let ipv4_addr = try_set
            .ipv4_addr
            .0
            .map_err(|err| format!("Cannot parse ipv4 address: \"{err}\""))?;
        let ipv6_addr = try_set
            .ipv6_addr
            .0
            .map_err(|err| format!("Cannot parse ipv6 address: \"{err}\""))?;
        let encryption_pubkey = try_set.encryption_pubkey.0.map_err(|err| {
            format!("Cannot parse encryption pubkey: \"{err}\"")
        })?;
        let signing_pubkey = try_set
            .signing_pubkey
            .0
            .map_err(|err| format!("Cannot parse signing pubkey: \"{err}\""))?;
        Ok(BitAssetData {
            commitment,
            ipv4_addr,
            ipv6_addr,
            encryption_pubkey,
            signing_pubkey,
        })
    }
}

impl std::fmt::Display for TxType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Regular => write!(f, "regular"),
            Self::BitAssetRegistration { .. } => write!(f, "register bitasset"),
            Self::BitAssetReservation { .. } => write!(f, "reserve bitasset"),
            Self::DexBurn { .. } => write!(f, "DEX (Burn Position)"),
            Self::DexMint { .. } => write!(f, "DEX (Mint Position)"),
            Self::DexSwap { .. } => write!(f, "DEX (Swap)"),
            Self::DutchAuctionCreate { .. } => {
                write!(f, "Dutch Auction (Create)")
            }
        }
    }
}

fn borsh_deserialize_hex<T>(hex: impl AsRef<[u8]>) -> anyhow::Result<T>
where
    T: BorshDeserialize,
{
    match hex::decode(hex) {
        Ok(bytes) => borsh::BorshDeserialize::try_from_slice(&bytes)
            .map_err(anyhow::Error::new),
        Err(err) => Err(anyhow::Error::new(err)),
    }
}

impl TxCreator {
    // set tx data for the current transaction
    fn set_tx_data(
        &self,
        app: &mut App,
        mut tx: Transaction,
    ) -> anyhow::Result<Transaction> {
        match &self.tx_type {
            TxType::Regular => Ok(tx),
            TxType::BitAssetRegistration {
                plaintext_name,
                bitasset_data,
                initial_supply,
            } => {
                let bitasset_data: BitAssetData = (bitasset_data.as_ref())
                    .clone()
                    .try_into()
                    .map_err(|err| anyhow::anyhow!("{err}"))?;
                let initial_supply =
                    u64::from_str(initial_supply).map_err(|err| {
                        anyhow::anyhow!("Failed to parse initial supply: {err}")
                    })?;
                let () = app.wallet.register_bitasset(
                    &mut tx,
                    plaintext_name,
                    Cow::Borrowed(&bitasset_data),
                    initial_supply,
                )?;
                Ok(tx)
            }
            TxType::BitAssetReservation { plaintext_name } => {
                let () =
                    app.wallet.reserve_bitasset(&mut tx, plaintext_name)?;
                Ok(tx)
            }
            TxType::DexBurn {
                asset0,
                asset1,
                amount_lp_tokens,
            } => {
                let asset0: AssetId =
                    borsh_deserialize_hex(asset0).map_err(|err| {
                        anyhow::anyhow!("Failed to parse asset 0: {err}")
                    })?;
                let asset1: AssetId =
                    borsh_deserialize_hex(asset1).map_err(|err| {
                        anyhow::anyhow!("Failed to parse asset 1: {err}")
                    })?;
                let amount_lp_tokens = u64::from_str(amount_lp_tokens)
                    .map_err(|err| {
                        anyhow::anyhow!(
                            "Failed to parse LP token amount: {err}"
                        )
                    })?;
                let amm_pair = AmmPair::new(asset0, asset1);
                let (amount0, amount1);
                {
                    let amm_pool_state = app
                        .node
                        .get_amm_pool_state(amm_pair)
                        .map_err(anyhow::Error::new)?;
                    let next_amm_pool_state = amm_pool_state
                        .burn(amount_lp_tokens)
                        .map_err(anyhow::Error::new)?;
                    amount0 =
                        amm_pool_state.reserve0 - next_amm_pool_state.reserve0;
                    amount1 =
                        amm_pool_state.reserve1 - next_amm_pool_state.reserve1;
                };
                let () = app.wallet.amm_burn(
                    &mut tx,
                    amm_pair.asset0(),
                    amm_pair.asset1(),
                    amount0,
                    amount1,
                    amount_lp_tokens,
                )?;
                Ok(tx)
            }
            TxType::DexMint {
                asset0,
                asset1,
                amount0,
                amount1,
            } => {
                let asset0: AssetId =
                    borsh_deserialize_hex(asset0).map_err(|err| {
                        anyhow::anyhow!("Failed to parse asset 0: {err}")
                    })?;
                let asset1: AssetId =
                    borsh_deserialize_hex(asset1).map_err(|err| {
                        anyhow::anyhow!("Failed to parse asset 1: {err}")
                    })?;
                let amount0 = u64::from_str(amount0).map_err(|err| {
                    anyhow::anyhow!("Failed to parse amount (asset 0): {err}")
                })?;
                let amount1 = u64::from_str(amount1).map_err(|err| {
                    anyhow::anyhow!("Failed to parse amount (asset 1): {err}")
                })?;
                let lp_token_mint = {
                    let amm_pair = AmmPair::new(asset0, asset1);
                    let amm_pool_state = app
                        .node
                        .get_amm_pool_state(amm_pair)
                        .map_err(anyhow::Error::new)?;
                    let next_amm_pool_state = amm_pool_state
                        .mint(amount0, amount1)
                        .map_err(anyhow::Error::new)?;
                    next_amm_pool_state.outstanding_lp_tokens
                        - amm_pool_state.outstanding_lp_tokens
                };
                let () = app.wallet.amm_mint(
                    &mut tx,
                    asset0,
                    asset1,
                    amount0,
                    amount1,
                    lp_token_mint,
                )?;
                Ok(tx)
            }
            TxType::DexSwap {
                asset_spend,
                asset_receive,
                amount_spend,
                amount_receive,
            } => {
                let asset_spend: AssetId = borsh_deserialize_hex(asset_spend)
                    .map_err(|err| {
                    anyhow::anyhow!("Failed to parse spend asset: {err}")
                })?;
                let asset_receive: AssetId =
                    borsh_deserialize_hex(asset_receive).map_err(|err| {
                        anyhow::anyhow!("Failed to parse receive asset: {err}")
                    })?;
                let amount_spend =
                    u64::from_str(amount_spend).map_err(|err| {
                        anyhow::anyhow!("Failed to parse spend amount: {err}")
                    })?;
                let amount_receive =
                    u64::from_str(amount_receive).map_err(|err| {
                        anyhow::anyhow!("Failed to parse receive amount: {err}")
                    })?;
                let () = app.wallet.amm_swap(
                    &mut tx,
                    asset_spend,
                    asset_receive,
                    amount_spend,
                    amount_receive,
                )?;
                Ok(tx)
            }
            TxType::DutchAuctionCreate { auction_params } => {
                let start_block = u32::from_str(&auction_params.start_block)
                    .map_err(|err| {
                        anyhow::anyhow!("Failed to parse start block: {err}")
                    })?;
                let duration = u32::from_str(&auction_params.duration)
                    .map_err(|err| {
                        anyhow::anyhow!("Failed to parse duration: {err}")
                    })?;
                let base_asset: AssetId = borsh_deserialize_hex(
                    &auction_params.base_asset,
                )
                .map_err(|err| {
                    anyhow::anyhow!("Failed to parse base asset: {err}")
                })?;
                let base_amount = u64::from_str(&auction_params.base_amount)
                    .map_err(|err| {
                        anyhow::anyhow!("Failed to parse base amount: {err}")
                    })?;
                let quote_asset: AssetId =
                    borsh_deserialize_hex(&auction_params.quote_asset)
                        .map_err(|err| {
                            anyhow::anyhow!(
                                "Failed to parse quote asset: {err}"
                            )
                        })?;
                let initial_price = u64::from_str(
                    &auction_params.initial_price,
                )
                .map_err(|err| {
                    anyhow::anyhow!("Failed to parse initial price: {err}")
                })?;
                let final_price = u64::from_str(&auction_params.final_price)
                    .map_err(|err| {
                        anyhow::anyhow!("Failed to parse final price: {err}")
                    })?;
                let dutch_auction_params =
                    plain_bitassets::types::DutchAuctionParams {
                        start_block,
                        duration,
                        base_asset,
                        base_amount,
                        quote_asset,
                        initial_price,
                        final_price,
                    };
                let () = app
                    .wallet
                    .dutch_auction_create(&mut tx, dutch_auction_params)?;
                Ok(tx)
            }
        }
    }

    // show setter for a single optional field, with default value
    fn show_option_field_default<T, ToStr, TryFromStr, TryFromStrErr>(
        ui: &mut egui::Ui,
        name: &str,
        default: T,
        try_set: &mut TrySetOption<T>,
        try_from_str: TryFromStr,
        to_str: ToStr,
    ) -> Response
    where
        T: PartialEq,
        TryFromStr: Fn(String) -> Result<T, TryFromStrErr>,
        TryFromStrErr: std::error::Error,
        ToStr: Fn(&T) -> String,
    {
        let option_dropdown = egui::ComboBox::from_id_source(name)
            .selected_text(if let Ok(None) = try_set.0 {
                "do not set"
            } else {
                "set"
            })
            .show_ui(ui, |ui| {
                ui.selectable_value(
                    try_set,
                    TrySetOption(Ok(Some(default))),
                    "set",
                ) | ui.selectable_value(
                    try_set,
                    TrySetOption(Ok(None)),
                    "do not set",
                )
            });
        match try_set.0 {
            Ok(None) => option_dropdown.join(),
            Err(ref mut bad_value) => {
                let text_edit = ui.add(egui::TextEdit::singleline(bad_value));
                if text_edit.changed() {
                    if let Ok(value) = try_from_str(bad_value.clone()) {
                        try_set.0 = Ok(Some(value));
                    }
                }
                option_dropdown.join() | text_edit
            }
            Ok(Some(ref mut value)) => {
                let mut text_buffer = to_str(value);
                let text_edit =
                    ui.add(egui::TextEdit::singleline(&mut text_buffer));
                if text_edit.changed() {
                    match try_from_str(text_buffer.clone()) {
                        Ok(new_value) => {
                            *value = new_value;
                        }
                        Err(_) => {
                            try_set.0 = Err(text_buffer);
                        }
                    }
                }
                option_dropdown.join() | text_edit
            }
        }
    }

    fn show_bitasset_options(
        ui: &mut egui::Ui,
        bitasset_data: &mut TrySetBitAssetData,
    ) -> Response {
        let commitment_resp = ui.horizontal(|ui| {
            ui.monospace("Commitment:       ")
                | Self::show_option_field_default(
                    ui,
                    "bitasset_data_commitment",
                    Default::default(),
                    &mut bitasset_data.commitment,
                    Hash::from_hex,
                    |commitment| hex::encode(commitment),
                )
        });
        let ipv4_resp = ui.horizontal(|ui| {
            ui.monospace("IPv4 Address:       ")
                | Self::show_option_field_default(
                    ui,
                    "bitasset_data_ipv4",
                    Ipv4Addr::UNSPECIFIED,
                    &mut bitasset_data.ipv4_addr,
                    |s| Ipv4Addr::from_str(&s),
                    Ipv4Addr::to_string,
                )
        });
        let ipv6_resp = ui.horizontal(|ui| {
            ui.monospace("IPv6 Address:       ")
                | Self::show_option_field_default(
                    ui,
                    "bitasset_data_ipv6",
                    Ipv6Addr::UNSPECIFIED,
                    &mut bitasset_data.ipv6_addr,
                    |s| Ipv6Addr::from_str(&s),
                    Ipv6Addr::to_string,
                )
        });
        let encryption_pubkey_resp = ui.horizontal(|ui| {
            let default_pubkey =
                EncryptionPubKey::from(<[u8; 32] as Default>::default());
            ui.monospace("Encryption PubKey:       ")
                | Self::show_option_field_default(
                    ui,
                    "bitasset_data_encryption_pubkey",
                    default_pubkey,
                    &mut bitasset_data.encryption_pubkey,
                    |s| <[u8; 32]>::from_hex(s).map(EncryptionPubKey::from),
                    |epk| hex::encode(epk.0.as_bytes()),
                )
        });
        let signing_pubkey_resp = ui.horizontal(|ui| {
            let default_pubkey =
                PublicKey::from_bytes(&<[u8; 32] as Default>::default())
                    .unwrap();
            let try_from_str = |s: String| {
                <[u8; 32]>::from_hex(s).map_err(either::Left).and_then(
                    |bytes| {
                        PublicKey::from_bytes(&bytes).map_err(either::Right)
                    },
                )
            };
            ui.monospace("Signing PubKey:       ")
                | Self::show_option_field_default(
                    ui,
                    "bitasset_data_signing_pubkey",
                    default_pubkey,
                    &mut bitasset_data.signing_pubkey,
                    try_from_str,
                    |pk| hex::encode(pk.to_bytes()),
                )
        });
        commitment_resp.join()
            | ipv4_resp.join()
            | ipv6_resp.join()
            | encryption_pubkey_resp.join()
            | signing_pubkey_resp.join()
    }

    pub fn show(
        &mut self,
        app: &mut App,
        ui: &mut egui::Ui,
        base_tx: &mut Transaction,
    ) -> anyhow::Result<()> {
        let tx_type_dropdown = ui.horizontal(|ui| {
            let combobox = egui::ComboBox::from_id_source("tx_type")
                .selected_text(format!("{}", self.tx_type))
                .show_ui(ui, |ui| {
                    ui.selectable_value(
                        &mut self.tx_type,
                        TxType::Regular,
                        "regular",
                    ) | ui.selectable_value(
                        &mut self.tx_type,
                        TxType::BitAssetRegistration {
                            plaintext_name: String::new(),
                            bitasset_data: Box::default(),
                            initial_supply: String::new(),
                        },
                        "register bitasset",
                    ) | ui.selectable_value(
                        &mut self.tx_type,
                        TxType::BitAssetReservation {
                            plaintext_name: String::new(),
                        },
                        "reserve bitasset",
                    ) | ui.selectable_value(
                        &mut self.tx_type,
                        TxType::DexBurn {
                            asset0: String::new(),
                            asset1: String::new(),
                            amount_lp_tokens: String::new(),
                        },
                        "Dex (Burn Position)",
                    ) | ui.selectable_value(
                        &mut self.tx_type,
                        TxType::DexMint {
                            asset0: String::new(),
                            asset1: String::new(),
                            amount0: String::new(),
                            amount1: String::new(),
                        },
                        "Dex (Mint Position)",
                    ) | ui.selectable_value(
                        &mut self.tx_type,
                        TxType::DexSwap {
                            asset_spend: String::new(),
                            asset_receive: String::new(),
                            amount_spend: String::new(),
                            amount_receive: String::new(),
                        },
                        "Dex (Swap)",
                    ) | ui.selectable_value(
                        &mut self.tx_type,
                        TxType::DutchAuctionCreate {
                            auction_params: Default::default(),
                        },
                        "Dutch Auction (Create)",
                    )
                });
            combobox.join() | ui.heading("Transaction")
        });
        let tx_data_ui = match &mut self.tx_type {
            TxType::Regular => None,
            TxType::BitAssetRegistration {
                plaintext_name,
                bitasset_data,
                initial_supply,
            } => {
                let plaintext_name_resp = ui.horizontal(|ui| {
                    ui.monospace("Plaintext Name:       ")
                        | ui.add(egui::TextEdit::singleline(plaintext_name))
                });
                let bitasset_options_resp =
                    Self::show_bitasset_options(ui, bitasset_data.as_mut());
                let initial_supply_resp = ui.horizontal(|ui| {
                    ui.monospace("Initial Supply:       ")
                        | ui.add(egui::TextEdit::singleline(initial_supply))
                });
                let resp = plaintext_name_resp.join()
                    | bitasset_options_resp
                    | initial_supply_resp.join();
                Some(resp)
            }
            TxType::BitAssetReservation { plaintext_name } => {
                let inner_resp = ui.horizontal(|ui| {
                    ui.monospace("Plaintext Name:       ")
                        | ui.add(egui::TextEdit::singleline(plaintext_name))
                });
                Some(inner_resp.join())
            }
            TxType::DexBurn {
                asset0,
                asset1,
                amount_lp_tokens,
            } => {
                let asset0_resp = ui.horizontal(|ui| {
                    ui.monospace("Asset 0:       ")
                        | ui.add(egui::TextEdit::singleline(asset0))
                });
                let asset1_resp = ui.horizontal(|ui| {
                    ui.monospace("Asset 1:       ")
                        | ui.add(egui::TextEdit::singleline(asset1))
                });
                let amount_lp_tokens_resp = ui.horizontal(|ui| {
                    ui.monospace("LP token amount:       ")
                        | ui.add(egui::TextEdit::singleline(amount_lp_tokens))
                });
                let resp = asset0_resp.join()
                    | asset1_resp.join()
                    | amount_lp_tokens_resp.join();
                Some(resp)
            }
            TxType::DexMint {
                asset0,
                asset1,
                amount0,
                amount1,
            } => {
                let asset0_resp = ui.horizontal(|ui| {
                    ui.monospace("Asset 0:       ")
                        | ui.add(egui::TextEdit::singleline(asset0))
                });
                let asset1_resp = ui.horizontal(|ui| {
                    ui.monospace("Asset 1:       ")
                        | ui.add(egui::TextEdit::singleline(asset1))
                });
                let amount0_resp = ui.horizontal(|ui| {
                    ui.monospace("Amount (Asset 0):       ")
                        | ui.add(egui::TextEdit::singleline(amount0))
                });
                let amount1_resp = ui.horizontal(|ui| {
                    ui.monospace("Amount (Asset 1):       ")
                        | ui.add(egui::TextEdit::singleline(amount1))
                });
                let resp = asset0_resp.join()
                    | asset1_resp.join()
                    | amount0_resp.join()
                    | amount1_resp.join();
                Some(resp)
            }
            TxType::DexSwap {
                asset_spend,
                asset_receive,
                amount_spend,
                amount_receive,
            } => {
                let asset_spend_resp = ui.horizontal(|ui| {
                    ui.monospace("Spend Asset:       ")
                        | ui.add(egui::TextEdit::singleline(asset_spend))
                });
                let asset_receive_resp = ui.horizontal(|ui| {
                    ui.monospace("Receive Asset:       ")
                        | ui.add(egui::TextEdit::singleline(asset_receive))
                });
                let amount_spend_resp = ui.horizontal(|ui| {
                    ui.monospace("Spend Amount:       ")
                        | ui.add(egui::TextEdit::singleline(amount_spend))
                });
                let amount_receive_resp = ui.horizontal(|ui| {
                    ui.monospace("Receive Amount:       ")
                        | ui.add(egui::TextEdit::singleline(amount_receive))
                });
                let resp = asset_spend_resp.join()
                    | asset_receive_resp.join()
                    | amount_spend_resp.join()
                    | amount_receive_resp.join();
                Some(resp)
            }
            TxType::DutchAuctionCreate { auction_params } => {
                let start_block_resp = ui.horizontal(|ui| {
                    ui.monospace("Start Block:       ")
                        | ui.add(egui::TextEdit::singleline(
                            &mut auction_params.start_block,
                        ))
                });
                let duration_resp = ui.horizontal(|ui| {
                    ui.monospace("Duration:       ")
                        | ui.add(egui::TextEdit::singleline(
                            &mut auction_params.duration,
                        ))
                });
                let base_asset_resp = ui.horizontal(|ui| {
                    ui.monospace("Base Asset:       ")
                        | ui.add(egui::TextEdit::singleline(
                            &mut auction_params.base_asset,
                        ))
                });
                let base_amount_resp = ui.horizontal(|ui| {
                    ui.monospace("Base Amount:       ")
                        | ui.add(egui::TextEdit::singleline(
                            &mut auction_params.base_amount,
                        ))
                });
                let quote_asset_resp = ui.horizontal(|ui| {
                    ui.monospace("Quote Asset:       ")
                        | ui.add(egui::TextEdit::singleline(
                            &mut auction_params.quote_asset,
                        ))
                });
                let initial_price_resp = ui.horizontal(|ui| {
                    ui.monospace("Initial Price:       ")
                        | ui.add(egui::TextEdit::singleline(
                            &mut auction_params.initial_price,
                        ))
                });
                let final_price_resp = ui.horizontal(|ui| {
                    ui.monospace("Final Price:       ")
                        | ui.add(egui::TextEdit::singleline(
                            &mut auction_params.final_price,
                        ))
                });
                let resp = start_block_resp.join()
                    | duration_resp.join()
                    | base_asset_resp.join()
                    | base_amount_resp.join()
                    | quote_asset_resp.join()
                    | initial_price_resp.join()
                    | final_price_resp.join();
                Some(resp)
            }
        };
        let tx_data_changed = tx_data_ui.is_some_and(|resp| resp.changed());
        // if base txid has changed, store the new txid
        let base_txid = base_tx.txid();
        let base_txid_changed = base_txid != self.base_txid;
        if base_txid_changed {
            self.base_txid = base_txid;
        }
        // (re)compute final tx if:
        // * the tx type, tx data, or base txid has changed
        // * final tx not yet set
        let refresh_final_tx = tx_type_dropdown.join().changed()
            || tx_data_changed
            || base_txid_changed
            || self.final_tx.is_none();
        if refresh_final_tx {
            self.final_tx = Some(self.set_tx_data(app, base_tx.clone()));
        }
        let final_tx = match &self.final_tx {
            None => panic!("impossible! final tx should have been set"),
            Some(Ok(final_tx)) => final_tx,
            Some(Err(wallet_err)) => {
                ui.monospace(format!("{wallet_err}"));
                return Ok(());
            }
        };
        let txid = &format!("{}", final_tx.txid())[0..8];
        ui.monospace(format!("txid: {txid}"));
        if self.bitcoin_value_in >= self.bitcoin_value_out {
            let fee = self.bitcoin_value_in - self.bitcoin_value_out;
            let fee = bitcoin::Amount::from_sat(fee);
            ui.monospace(format!("fee:  {fee}"));
            if ui.button("sign and send").clicked() {
                let () = app.sign_and_send(final_tx.clone())?;
                *base_tx = Transaction::default();
                self.final_tx = None;
            }
        } else {
            ui.label("Not Enough Value In");
        }
        Ok(())
    }
}
