use eframe::egui;
use strum::{EnumIter, IntoEnumIterator};

use crate::app::App;

mod all_bitassets;
mod dutch_auction_explorer;
mod reserve_register;

use all_bitassets::AllBitAssets;
use dutch_auction_explorer::DutchAuctionExplorer;
use reserve_register::ReserveRegister;

#[derive(Default, EnumIter, Eq, PartialEq, strum::Display)]
enum Tab {
    #[default]
    #[strum(to_string = "All BitAssets")]
    AllBitAssets,
    #[strum(to_string = "Reserve & Register")]
    ReserveRegister,
    #[strum(to_string = "Dutch Auction Explorer")]
    DutchAuctionExplorer,
}

#[derive(Default)]
pub struct BitAssets {
    all_bitassets: AllBitAssets,
    dutch_auction_explorer: DutchAuctionExplorer,
    reserve_register: ReserveRegister,
    tab: Tab,
}

impl BitAssets {
    pub fn show(&mut self, app: &mut App, ui: &mut egui::Ui) {
        egui::TopBottomPanel::top("bitassets_tabs").show(ui.ctx(), |ui| {
            ui.horizontal(|ui| {
                Tab::iter().for_each(|tab_variant| {
                    let tab_name = tab_variant.to_string();
                    ui.selectable_value(&mut self.tab, tab_variant, tab_name);
                })
            });
        });
        egui::CentralPanel::default().show(ui.ctx(), |ui| match self.tab {
            Tab::AllBitAssets => {
                let () = self.all_bitassets.show(app, ui);
            }
            Tab::ReserveRegister => {
                let () = self.reserve_register.show(app, ui);
            }
            Tab::DutchAuctionExplorer => {
                let () = self.dutch_auction_explorer.show(app, ui);
            }
        });
    }
}
