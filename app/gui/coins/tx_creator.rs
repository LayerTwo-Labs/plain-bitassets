use std::{
    borrow::Cow,
    net::{Ipv4Addr, Ipv6Addr},
    str::FromStr,
};

use eframe::egui::{self, Response};
use hex::FromHex;

use plain_bitassets::{
    authorization::PublicKey,
    bip300301::bitcoin,
    types::{BitAssetData, EncryptionPubKey, Hash, Transaction, Txid},
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
    /// optional pubkey used for signing messages
    pub paymail_fee: TrySetOption<u64>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub enum TxType {
    #[default]
    Regular,
    BitAssetRegistration {
        plaintext_name: String,
        bitasset_data: Box<TrySetBitAssetData>,
    },
    BitAssetReservation {
        plaintext_name: String,
    },
}

#[derive(Debug, Default)]
pub struct TxCreator {
    pub value_in: u64,
    pub value_out: u64,
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
        let paymail_fee = try_set
            .paymail_fee
            .0
            .map_err(|err| format!("Cannot parse paymail fee: \"{err}\""))?;
        Ok(BitAssetData {
            commitment,
            ipv4_addr,
            ipv6_addr,
            encryption_pubkey,
            signing_pubkey,
            paymail_fee,
        })
    }
}

impl std::fmt::Display for TxType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Regular => write!(f, "regular"),
            Self::BitAssetRegistration { .. } => write!(f, "register bitasset"),
            Self::BitAssetReservation { .. } => write!(f, "reserve bitasset"),
        }
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
            } => {
                let bitasset_data: BitAssetData = (bitasset_data.as_ref())
                    .clone()
                    .try_into()
                    .map_err(|err| anyhow::anyhow!("{err}"))?;
                let () = app.wallet.register_bitasset(
                    &mut tx,
                    plaintext_name,
                    Cow::Borrowed(&bitasset_data),
                )?;
                Ok(tx)
            }
            TxType::BitAssetReservation { plaintext_name } => {
                let () = app.wallet.reserve_bitasset(&mut tx, plaintext_name)?;
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
        let paymail_fee_resp = ui.horizontal(|ui| {
            ui.monospace("Paymail fee:       ")
                | Self::show_option_field_default(
                    ui,
                    "bitasset_data_paymail_fee",
                    100,
                    &mut bitasset_data.paymail_fee,
                    |s| u64::from_str(&s),
                    u64::to_string,
                )
        });
        commitment_resp.join()
            | ipv4_resp.join()
            | ipv6_resp.join()
            | encryption_pubkey_resp.join()
            | signing_pubkey_resp.join()
            | paymail_fee_resp.join()
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
                        },
                        "register bitasset",
                    ) | ui.selectable_value(
                        &mut self.tx_type,
                        TxType::BitAssetReservation {
                            plaintext_name: String::new(),
                        },
                        "reserve bitasset",
                    )
                });
            combobox.join() | ui.heading("Transaction")
        });
        let tx_data_ui = match &mut self.tx_type {
            TxType::Regular => None,
            TxType::BitAssetRegistration {
                plaintext_name,
                bitasset_data,
            } => {
                let plaintext_name_resp = ui.horizontal(|ui| {
                    ui.monospace("Plaintext Name:       ")
                        | ui.add(egui::TextEdit::singleline(plaintext_name))
                });
                let bitasset_options_resp =
                    Self::show_bitasset_options(ui, bitasset_data.as_mut());
                let resp = plaintext_name_resp.join() | bitasset_options_resp;
                Some(resp)
            }
            TxType::BitAssetReservation { plaintext_name } => {
                let inner_resp = ui.horizontal(|ui| {
                    ui.monospace("Plaintext Name:       ")
                        | ui.add(egui::TextEdit::singleline(plaintext_name))
                });
                Some(inner_resp.join())
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
        if self.value_in >= self.value_out {
            let fee = self.value_in - self.value_out;
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
