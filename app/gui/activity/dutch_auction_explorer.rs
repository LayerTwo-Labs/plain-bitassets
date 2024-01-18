use eframe::egui::{self};

use crate::{
    app::App,
    gui::util::{InnerResponseExt, UiExt},
};

#[derive(Debug, Default)]
pub struct DutchAuctionExplorer;

impl DutchAuctionExplorer {
    pub fn show(&mut self, app: &mut App, ui: &mut egui::Ui) {
        let auctions = app.node.dutch_auctions();
        egui::CentralPanel::default().show_inside(ui, |ui| match auctions {
            Err(node_err) => {
                let resp =
                    ui.monospace_selectable_multiline(node_err.to_string());
                Some(resp)
            }
            Ok(auctions) => auctions
                .into_iter()
                .map(|(auction_id, auction_state)| {
                    {
                        ui.horizontal(|ui| {
                            ui.monospace_selectable_singleline(format!(
                                "Auction ID: {}",
                                auction_id.0
                            )) | crate::gui::lookup::show_dutch_auction_state(
                                ui,
                                &auction_state,
                            )
                        })
                    }
                    .join()
                })
                .reduce(|resp0, resp1| resp0 | resp1),
        });
    }
}
