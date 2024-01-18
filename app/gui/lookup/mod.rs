use eframe::egui;

use crate::app::App;

mod bitasset_explorer;
mod dutch_auction_explorer;

use bitasset_explorer::BitAssetExplorer;
use dutch_auction_explorer::DutchAuctionExplorer;

pub use dutch_auction_explorer::show_dutch_auction_state;

#[derive(Debug, Default, Eq, PartialEq)]
enum Tab {
    #[default]
    BitassetExplorer,
    DutchAuctionExplorer,
}

#[derive(Debug, Default)]
pub struct Lookup {
    tab: Tab,
    bitasset_explorer: BitAssetExplorer,
    dutch_auction_explorer: DutchAuctionExplorer,
}

impl Lookup {
    pub fn show(&mut self, app: &mut App, ui: &mut egui::Ui) {
        egui::TopBottomPanel::top("lookup_tabs").show(ui.ctx(), |ui| {
            ui.horizontal(|ui| {
                ui.selectable_value(
                    &mut self.tab,
                    Tab::BitassetExplorer,
                    "bitasset explorer",
                );
                ui.selectable_value(
                    &mut self.tab,
                    Tab::DutchAuctionExplorer,
                    "dutch auction explorer",
                );
            });
        });
        egui::CentralPanel::default().show(ui.ctx(), |ui| match self.tab {
            Tab::BitassetExplorer => {
                self.bitasset_explorer.show(app, ui);
            }
            Tab::DutchAuctionExplorer => {
                self.dutch_auction_explorer.show(app, ui);
            }
        });
    }
}
