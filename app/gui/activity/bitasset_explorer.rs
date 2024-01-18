use eframe::egui::{self};

use crate::{
    app::App,
    gui::util::{InnerResponseExt, UiExt},
};

#[derive(Debug, Default)]
pub struct BitAssetExplorer;

impl BitAssetExplorer {
    pub fn show(&mut self, app: &mut App, ui: &mut egui::Ui) {
        let bitassets = app.node.bitassets();
        egui::CentralPanel::default().show_inside(ui, |ui| match bitassets {
            Err(node_err) => {
                let resp =
                    ui.monospace_selectable_multiline(node_err.to_string());
                Some(resp)
            }
            Ok(auctions) => auctions
                .into_iter()
                .map(|(bitasset_id, bitasset_data)| {
                    {
                        ui.horizontal(|ui| {
                            ui.monospace_selectable_singleline(format!(
                                "BitAsset ID: {}",
                                hex::encode(bitasset_id.0)
                            )) | crate::gui::lookup::show_bitasset_data(
                                ui,
                                &bitasset_data,
                            )
                        })
                    }
                    .join()
                })
                .reduce(|resp0, resp1| resp0 | resp1),
        });
    }
}
