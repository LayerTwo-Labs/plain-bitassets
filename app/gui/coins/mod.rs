use eframe::egui;

use crate::app::App;

mod my_bitassets;
mod tx_builder;
mod tx_creator;
mod utxo_creator;
mod utxo_selector;

use my_bitassets::MyBitassets;
use tx_builder::TxBuilder;

#[derive(Debug, Default, Eq, PartialEq)]
enum Tab {
    #[default]
    TransactionBuilder,
    MyBitassets,
}

#[derive(Default)]
pub struct Coins {
    tab: Tab,
    tx_builder: TxBuilder,
    my_bitassets: MyBitassets,
}

impl Coins {
    pub fn show(
        &mut self,
        app: &mut App,
        ui: &mut egui::Ui,
    ) -> anyhow::Result<()> {
        egui::TopBottomPanel::top("coins_tabs").show(ui.ctx(), |ui| {
            ui.horizontal(|ui| {
                ui.selectable_value(
                    &mut self.tab,
                    Tab::TransactionBuilder,
                    "transaction builder",
                );
                ui.selectable_value(
                    &mut self.tab,
                    Tab::MyBitassets,
                    "my bitassets",
                );
            });
        });
        egui::CentralPanel::default().show(ui.ctx(), |ui| match self.tab {
            Tab::TransactionBuilder => {
                let () = self.tx_builder.show(app, ui).unwrap();
            }
            Tab::MyBitassets => {
                self.my_bitassets.show(app, ui);
            }
        });
        Ok(())
    }
}
