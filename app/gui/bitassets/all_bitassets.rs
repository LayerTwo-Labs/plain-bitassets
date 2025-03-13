use std::collections::{BTreeMap, HashMap};

use eframe::egui;
use hex::FromHex;
use plain_bitassets::{
    state::BitAssetSeqId,
    types::{BitAssetData, hashes::BitAssetId},
};

use crate::{
    app::App,
    gui::util::{InnerResponseExt, UiExt},
};

#[derive(Debug, Default)]
pub(super) struct AllBitAssets {
    query: String,
}

fn show_bitasset_data(
    ui: &mut egui::Ui,
    bitasset_data: &BitAssetData,
) -> egui::Response {
    let BitAssetData {
        commitment,
        socket_addr_v4,
        socket_addr_v6,
        encryption_pubkey,
        signing_pubkey,
    } = bitasset_data;
    let commitment = commitment.map_or("Not set".to_owned(), hex::encode);
    let socket_addr_v4 = socket_addr_v4
        .map_or("Not set".to_owned(), |socket_addr_v4| {
            socket_addr_v4.to_string()
        });
    let socket_addr_v6 = socket_addr_v6
        .map_or("Not set".to_owned(), |socket_addr_v6| {
            socket_addr_v6.to_string()
        });
    let encryption_pubkey =
        encryption_pubkey.map_or("Not set".to_owned(), |epk| epk.to_string());
    let signing_pubkey =
        signing_pubkey.map_or("Not set".to_owned(), |svk| svk.to_string());
    ui.horizontal(|ui| {
        ui.monospace_selectable_singleline(
            true,
            format!("Commitment: {commitment}"),
        )
    })
    .join()
        | ui.horizontal(|ui| {
            ui.monospace_selectable_singleline(
                false,
                format!("IPv4 Address: {socket_addr_v4}"),
            )
        })
        .join()
        | ui.horizontal(|ui| {
            ui.monospace_selectable_singleline(
                false,
                format!("IPv6 Address: {socket_addr_v6}"),
            )
        })
        .join()
        | ui.horizontal(|ui| {
            ui.monospace_selectable_singleline(
                true,
                format!("Encryption Pubkey: {encryption_pubkey}"),
            )
        })
        .join()
        | ui.horizontal(|ui| {
            ui.monospace_selectable_singleline(
                true,
                format!("Signing Pubkey: {signing_pubkey}"),
            )
        })
        .join()
}

fn show_bitasset_with_data(
    ui: &mut egui::Ui,
    bitasset_id: &BitAssetId,
    bitasset_data: &BitAssetData,
) -> egui::Response {
    ui.horizontal(|ui| {
        ui.monospace_selectable_singleline(
            true,
            format!("BitAsset ID: {}", hex::encode(bitasset_id.0)),
        )
    })
    .join()
        | show_bitasset_data(ui, bitasset_data)
}

impl AllBitAssets {
    fn show_bitassets(
        &mut self,
        ui: &mut egui::Ui,
        bitassets: Vec<(BitAssetSeqId, BitAssetId, BitAssetData)>,
    ) {
        let (seq_id_to_bitasset_id, bitassets): (
            HashMap<_, _>,
            BTreeMap<_, _>,
        ) = bitassets
            .into_iter()
            .map(|(seq_id, bitasset_id, bitasset_data)| {
                ((seq_id, bitasset_id), (bitasset_id, bitasset_data))
            })
            .unzip();
        ui.horizontal(|ui| {
            let query_edit = egui::TextEdit::singleline(&mut self.query)
                .hint_text("Search")
                .desired_width(150.);
            ui.add(query_edit);
        });
        if self.query.is_empty() {
            bitassets
                .into_iter()
                .for_each(|(bitasset_id, bitasset_data)| {
                    show_bitasset_with_data(ui, &bitasset_id, &bitasset_data);
                })
        } else {
            let name_hash = blake3::hash(self.query.as_bytes()).into();
            let name_hash_pattern = BitAssetId(name_hash);
            if let Some(bitasset_data) = bitassets.get(&name_hash_pattern) {
                show_bitasset_with_data(ui, &name_hash_pattern, bitasset_data);
            };
            if let Ok(bitasset_id_pattern) = BitAssetId::from_hex(&self.query) {
                if let Some(bitasset_data) = bitassets.get(&bitasset_id_pattern)
                {
                    show_bitasset_with_data(
                        ui,
                        &bitasset_id_pattern,
                        bitasset_data,
                    );
                }
            };
            if let Ok(bitasset_seq_id_pattern) =
                self.query.parse().map(BitAssetSeqId)
            {
                if let Some(bitasset_id) =
                    seq_id_to_bitasset_id.get(&bitasset_seq_id_pattern)
                {
                    let bitasset_data = &bitassets[bitasset_id];
                    show_bitasset_with_data(ui, bitasset_id, bitasset_data);
                }
            }
        }
    }

    pub fn show(&mut self, app: Option<&App>, ui: &mut egui::Ui) {
        egui::CentralPanel::default().show_inside(ui, |ui| {
            let Some(app) = app else {
                return;
            };
            match app.node.bitassets() {
                Err(node_err) => {
                    let err = anyhow::Error::from(node_err);
                    ui.monospace_selectable_multiline(format!("{err:#}"));
                }
                Ok(bitassets) => self.show_bitassets(ui, bitassets),
            }
        });
    }
}
