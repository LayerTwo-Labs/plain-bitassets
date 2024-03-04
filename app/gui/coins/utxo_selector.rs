use std::collections::HashSet;

use eframe::egui;
use plain_bitassets::{
    bip300301::bitcoin,
    types::{AssetId, FilledOutput, OutPoint, Transaction},
};
use strum::{EnumIter, IntoEnumIterator, IntoStaticStr};

use crate::{
    app::App,
    gui::util::{borsh_deserialize_hex, UiExt},
};

#[derive(
    Clone, Copy, Debug, Default, EnumIter, Eq, IntoStaticStr, PartialEq,
)]
pub enum AssetKind {
    #[default]
    Bitcoin,
    BitAsset,
    #[strum(serialize = "BitAsset Control")]
    BitAssetControl,
}

#[derive(Debug, Default)]
pub struct AssetInput {
    asset_kind: AssetKind,
    hex_input: String,
}

impl PartialEq for AssetInput {
    fn eq(&self, other: &Self) -> bool {
        self.asset_kind == other.asset_kind
            && match self.asset_kind {
                AssetKind::Bitcoin => true,
                AssetKind::BitAsset | AssetKind::BitAssetControl => {
                    self.hex_input == other.hex_input
                }
            }
    }
}

impl Eq for AssetInput {}

impl AssetInput {
    pub fn asset_id(&self) -> anyhow::Result<AssetId> {
        match self.asset_kind {
            AssetKind::Bitcoin => Ok(AssetId::Bitcoin),
            AssetKind::BitAsset => {
                borsh_deserialize_hex(self.hex_input.as_str())
                    .map(AssetId::BitAsset)
            }
            AssetKind::BitAssetControl => {
                borsh_deserialize_hex(self.hex_input.as_str())
                    .map(AssetId::BitAssetControl)
            }
        }
    }

    pub fn show(&mut self, ui: &mut egui::Ui) {
        egui::ComboBox::from_id_source("asset_kind")
            .selected_text(<&'static str>::from(self.asset_kind))
            .show_ui(ui, |ui| {
                for asset_kind in AssetKind::iter() {
                    ui.selectable_value(
                        &mut self.asset_kind,
                        asset_kind,
                        <&'static str>::from(asset_kind),
                    );
                }
            });
        match self.asset_kind {
            AssetKind::Bitcoin => (),
            AssetKind::BitAsset | AssetKind::BitAssetControl => {
                ui.text_edit_singleline(&mut self.hex_input);
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct UtxoSelector {
    asset_input: AssetInput,
}

impl UtxoSelector {
    fn show_utxos(
        app: &mut App,
        ui: &mut egui::Ui,
        tx: &mut Transaction,
        asset_id: AssetId,
    ) {
        let selected: HashSet<_> = tx.inputs.iter().cloned().collect();
        let utxos = app.utxos.read();
        let mut utxos: Vec<_> = utxos
            .iter()
            .filter(|(outpoint, output)| {
                !selected.contains(outpoint)
                    && output.asset_value().is_some_and(
                        |(output_asset_id, _)| output_asset_id == asset_id,
                    )
            })
            .collect();
        let total_value: u64 = utxos
            .iter()
            .filter_map(|(_, output)| {
                output.asset_value().map(|(_, value)| value)
            })
            .sum();
        utxos.sort_by_key(|(outpoint, _)| format!("{outpoint}"));
        ui.separator();
        if asset_id == AssetId::Bitcoin {
            ui.monospace(format!(
                "Total: ₿{}",
                bitcoin::Amount::from_sat(total_value)
            ));
        } else {
            ui.monospace(format!("Total: {total_value}"));
        }
        ui.separator();
        egui::Grid::new("utxos")
            .striped(true)
            .num_columns(3)
            .show(ui, |ui| {
                ui.monospace_selectable_singleline(false, "Kind");
                ui.monospace_selectable_singleline(false, "Outpoint");
                ui.monospace_selectable_singleline(false, "Value");
                ui.end_row();
                for (outpoint, output) in utxos {
                    //ui.horizontal(|ui| {});
                    show_utxo(ui, outpoint, output, false);

                    if ui.button("spend").clicked() {
                        tx.inputs.push(*outpoint);
                    }
                    ui.end_row();
                }
            });
    }

    pub fn show(
        &mut self,
        app: &mut App,
        ui: &mut egui::Ui,
        tx: &mut Transaction,
    ) {
        ui.heading("Spend UTXO");
        self.asset_input.show(ui);
        match self.asset_input.asset_id() {
            Ok(asset_id) => Self::show_utxos(app, ui, tx, asset_id),
            Err(err) => {
                ui.monospace_selectable_multiline(format!("{err:#}"));
            }
        }
    }
}

pub fn show_utxo(
    ui: &mut egui::Ui,
    outpoint: &OutPoint,
    output: &FilledOutput,
    show_asset_id: bool,
) {
    let (kind, hash, vout) = match outpoint {
        OutPoint::Regular { txid, vout } => {
            ("regular", format!("{txid}"), *vout)
        }
        OutPoint::Deposit(outpoint) => {
            ("deposit", format!("{}", outpoint.txid), outpoint.vout)
        }
        OutPoint::Coinbase { merkle_root, vout } => {
            ("coinbase", format!("{merkle_root}"), *vout)
        }
    };
    let hash = &hash[0..8];
    ui.monospace_selectable_singleline(false, kind.to_string());
    ui.monospace_selectable_singleline(true, format!("{hash}:{vout}",));
    match output.asset_value() {
        None => (),
        Some((asset_id @ AssetId::Bitcoin, bitcoin_value)) => {
            ui.with_layout(
                egui::Layout::right_to_left(egui::Align::Max),
                |ui| {
                    let bitcoin_amount =
                        bitcoin::Amount::from_sat(bitcoin_value);
                    if show_asset_id {
                        ui.monospace_selectable_singleline(
                            true,
                            format!("{}", asset_id),
                        );
                    }
                    ui.monospace_selectable_singleline(
                        false,
                        format!("₿{bitcoin_amount}"),
                    );
                },
            );
        }
        Some((
            asset_id @ (AssetId::BitAsset(_) | AssetId::BitAssetControl(_)),
            value,
        )) => {
            if show_asset_id {
                ui.monospace_selectable_singleline(true, format!("{asset_id}"));
            }
            ui.monospace_selectable_singleline(false, format!("{value}"));
        }
    }
}
