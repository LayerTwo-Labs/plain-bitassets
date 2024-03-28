use eframe::egui;
use itertools::{Either, Itertools};

use plain_bitassets::types::FilledOutput;

use crate::{app::App, gui::util::UiExt};

#[derive(Debug, Default)]
pub struct MyBitAssets;

impl MyBitAssets {
    pub fn show_reservations(&mut self, app: &mut App, ui: &mut egui::Ui) {
        let utxos_read = app.utxos.read();
        // all bitasset reservations
        let bitasset_reservations = utxos_read
            .values()
            .filter_map(FilledOutput::reservation_data);
        // split into bitasset reservations for which the names are known,
        // or unknown
        let (
            mut known_name_bitasset_reservations,
            mut unknown_name_bitasset_reservations,
        ): (Vec<_>, Vec<_>) =
            bitasset_reservations.partition_map(|(txid, commitment)| {
                let plain_bitasset = app
                    .wallet
                    .get_bitasset_reservation_plaintext(commitment)
                    .expect("failed to retrieve bitasset reservation data");
                match plain_bitasset {
                    Some(plain_bitasset) => {
                        Either::Left((*txid, *commitment, plain_bitasset))
                    }
                    None => Either::Right((*txid, *commitment)),
                }
            });
        // sort name-known bitasset reservations by plain name
        known_name_bitasset_reservations.sort_by(
            |(_, _, plain_name_l), (_, _, plain_name_r)| {
                plain_name_l.cmp(plain_name_r)
            },
        );
        // sort name-unknown bitasset reservations by txid
        unknown_name_bitasset_reservations.sort_by_key(|(txid, _)| *txid);
        let _response = egui::SidePanel::left("My BitAsset Reservations")
            .exact_width(350.)
            .resizable(false)
            .show_inside(ui, move |ui| {
                ui.heading("BitAsset Reservations");
                egui::Grid::new("My BitAsset Reservations")
                    .num_columns(1)
                    .striped(true)
                    .show(ui, |ui| {
                        for (txid, commitment, plaintext_name) in
                            known_name_bitasset_reservations
                        {
                            let txid = hex::encode(txid.0);
                            let commitment = hex::encode(commitment);
                            ui.vertical(|ui| {
                                ui.monospace_selectable_singleline(
                                    true,
                                    format!("plaintext name: {plaintext_name}"),
                                );
                                ui.monospace_selectable_singleline(
                                    false,
                                    format!("txid: {txid}"),
                                );
                                ui.monospace_selectable_singleline(
                                    false,
                                    format!("commitment: {commitment}"),
                                );
                            });
                            ui.end_row()
                        }
                        for (txid, commitment) in
                            unknown_name_bitasset_reservations
                        {
                            let txid = hex::encode(txid.0);
                            let commitment = hex::encode(commitment);
                            ui.vertical(|ui| {
                                ui.monospace_selectable_singleline(
                                    false,
                                    format!("txid: {txid}"),
                                );
                                ui.monospace_selectable_singleline(
                                    false,
                                    format!("commitment: {commitment}"),
                                );
                            });
                            ui.end_row()
                        }
                    });
            });
    }

    pub fn show_bitassets(&mut self, app: &mut App, ui: &mut egui::Ui) {
        let utxos_read = app.utxos.read();
        // all owned bitassets
        let bitassets = utxos_read.values().filter_map(FilledOutput::bitasset);
        // split into bitassets for which the names are known or unknown
        let (mut known_name_bitassets, mut unknown_name_bitassets): (
            Vec<_>,
            Vec<_>,
        ) = bitassets.partition_map(|bitasset| {
            let plain_bitasset = app
                .wallet
                .get_bitasset_plaintext(bitasset)
                .expect("failed to retrieve bitasset data");
            match plain_bitasset {
                Some(plain_bitasset) => {
                    Either::Left((*bitasset, plain_bitasset))
                }
                None => Either::Right(*bitasset),
            }
        });
        // sort name-known bitassets by plain name
        known_name_bitassets.sort_by(|(_, plain_name_l), (_, plain_name_r)| {
            plain_name_l.cmp(plain_name_r)
        });
        // sort name-unknown bitassets by bitasset value
        unknown_name_bitassets.sort();
        egui::SidePanel::left("My BitAssets")
            .exact_width(350.)
            .resizable(false)
            .show_inside(ui, |ui| {
                ui.heading("BitAssets");
                egui::Grid::new("My BitAssets")
                    .striped(true)
                    .num_columns(1)
                    .show(ui, |ui| {
                        for (bitasset, plaintext_name) in known_name_bitassets {
                            ui.vertical(|ui| {
                                ui.monospace_selectable_singleline(
                                    true,
                                    format!("plaintext name: {plaintext_name}"),
                                );
                                ui.monospace_selectable_singleline(
                                    false,
                                    format!(
                                        "bitasset: {}",
                                        hex::encode(bitasset.0)
                                    ),
                                );
                            });
                            ui.end_row()
                        }
                        for bitasset in unknown_name_bitassets {
                            ui.monospace_selectable_singleline(
                                false,
                                format!(
                                    "bitasset: {}",
                                    hex::encode(bitasset.0)
                                ),
                            );
                            ui.end_row()
                        }
                    });
            });
    }

    pub fn show(&mut self, app: &mut App, ui: &mut egui::Ui) {
        let _reservations_response = self.show_reservations(app, ui);
        let _bitassets_response = self.show_bitassets(app, ui);
    }
}
