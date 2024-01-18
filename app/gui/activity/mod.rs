use eframe::egui;

use crate::app::App;

mod bitasset_explorer;
mod block_explorer;
mod dutch_auction_explorer;
mod mempool_explorer;

use bitasset_explorer::BitAssetExplorer;
use block_explorer::BlockExplorer;
use dutch_auction_explorer::DutchAuctionExplorer;
use mempool_explorer::MemPoolExplorer;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Default, Eq, PartialEq)]
enum Tab {
    #[default]
    BlockExplorer,
    MemPoolExplorer,
    BitAssetExplorer,
    DutchAuctionExplorer,
}

pub struct Activity {
    tab: Tab,
    block_explorer: BlockExplorer,
    mempool_explorer: MemPoolExplorer,
    bitasset_explorer: BitAssetExplorer,
    dutch_auction_explorer: DutchAuctionExplorer,
}

impl Activity {
    pub fn new(app: &App) -> Self {
        let height = app.node.get_height().unwrap_or(0);
        Self {
            tab: Default::default(),
            block_explorer: BlockExplorer::new(height),
            mempool_explorer: Default::default(),
            bitasset_explorer: Default::default(),
            dutch_auction_explorer: Default::default(),
        }
    }

    pub fn show(&mut self, app: &mut App, ui: &mut egui::Ui) {
        egui::TopBottomPanel::top("activity_tabs").show(ui.ctx(), |ui| {
            ui.horizontal(|ui| {
                ui.selectable_value(
                    &mut self.tab,
                    Tab::BlockExplorer,
                    "block explorer",
                );
                ui.selectable_value(
                    &mut self.tab,
                    Tab::MemPoolExplorer,
                    "mempool explorer",
                );
                ui.selectable_value(
                    &mut self.tab,
                    Tab::BitAssetExplorer,
                    "BitAsset explorer",
                );
                ui.selectable_value(
                    &mut self.tab,
                    Tab::DutchAuctionExplorer,
                    "Dutch auction explorer",
                );
            });
        });
        egui::CentralPanel::default().show(ui.ctx(), |ui| match self.tab {
            Tab::BlockExplorer => {
                self.block_explorer.show(app, ui);
            }
            Tab::MemPoolExplorer => {
                self.mempool_explorer.show(app, ui);
            }
            Tab::BitAssetExplorer => {
                self.bitasset_explorer.show(app, ui);
            }
            Tab::DutchAuctionExplorer => {
                self.dutch_auction_explorer.show(app, ui);
            }
        });
    }
}
