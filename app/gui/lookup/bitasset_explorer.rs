use eframe::egui::{self, Response};

use plain_bitassets::{
    node,
    types::{BitAssetData, BitAssetId, Hash},
};

use crate::{
    app::App,
    gui::util::{InnerResponseExt, UiExt},
};

/// result of the last bitasset lookup query
#[derive(Debug)]
struct LastQueryResult(Result<Option<BitAssetData>, node::Error>);

#[derive(Debug, Default)]
pub struct BitAssetExplorer {
    plaintext_name: String,
    last_query_result: Option<LastQueryResult>,
}

impl BitAssetExplorer {
    fn show_bitasset_data(
        ui: &mut egui::Ui,
        bitasset_data: &BitAssetData,
    ) -> Response {
        let BitAssetData {
            commitment,
            ipv4_addr,
            ipv6_addr,
            encryption_pubkey,
            signing_pubkey,
        } = bitasset_data;
        let commitment = commitment.map_or("Not set".to_owned(), hex::encode);
        let ipv4_addr = ipv4_addr
            .map_or("Not set".to_owned(), |ipv4_addr| ipv4_addr.to_string());
        let ipv6_addr = ipv6_addr
            .map_or("Not set".to_owned(), |ipv6_addr| ipv6_addr.to_string());
        let encryption_pubkey = encryption_pubkey
            .map_or("Not set".to_owned(), |epk| hex::encode(epk.0.as_bytes()));
        let signing_pubkey = signing_pubkey
            .map_or("Not set".to_owned(), |pk| hex::encode(pk.as_bytes()));
        ui.horizontal(|ui| {
            ui.monospace_selectable_singleline(format!(
                "Commitment: {commitment}"
            ))
        })
        .join()
            | ui.horizontal(|ui| {
                ui.monospace_selectable_singleline(format!(
                    "IPv4 Address: {ipv4_addr}"
                ))
            })
            .join()
            | ui.horizontal(|ui| {
                ui.monospace_selectable_singleline(format!(
                    "IPv6 Address: {ipv6_addr}"
                ))
            })
            .join()
            | ui.horizontal(|ui| {
                ui.monospace_selectable_singleline(format!(
                    "Encryption Pubkey: {encryption_pubkey}"
                ))
            })
            .join()
            | ui.horizontal(|ui| {
                ui.monospace_selectable_singleline(format!(
                    "Signing Pubkey: {signing_pubkey}"
                ))
            })
            .join()
    }

    pub fn show(&mut self, app: &mut App, ui: &mut egui::Ui) {
        egui::CentralPanel::default().show_inside(ui, |ui| {
            ui.heading("BitAsset Explorer");
            let text_resp =  ui.horizontal(|ui| {
                ui.monospace("Plaintext BitAsset:       ")
                | ui.add(egui::TextEdit::singleline(&mut self.plaintext_name))
            }).join();
            let refresh_button = ui.button("Refresh");
            // resolve bitasset if changed or refresh button clicked
            if text_resp.changed() || refresh_button.clicked() {
                let name_hash: Hash = blake3::hash(self.plaintext_name.as_bytes()).into();
                let bitasset = BitAssetId(name_hash);
                let last_query_result = app.node.try_get_current_bitasset_data(&bitasset);
                self.last_query_result = Some(LastQueryResult(last_query_result));
            }
            if let Some(LastQueryResult(last_query_result)) = &self.last_query_result {
                match last_query_result {
                    Err(err) => {
                        ui.horizontal(|ui| {
                            ui.monospace(format!("Error encountered when resolving bitasset: {err}"))
                        });
                    }
                    Ok(None) => {
                        ui.horizontal(|ui| {
                            ui.monospace("No BitAsset data found")
                        });
                    }
                    Ok(Some(bitasset_data)) => {
                        let _resp: Response = Self::show_bitasset_data(ui, bitasset_data);
                    }
                }
            }
        });
    }
}
