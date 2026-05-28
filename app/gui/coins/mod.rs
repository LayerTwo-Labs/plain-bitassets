use eframe::egui;
use strum::{EnumIter, IntoEnumIterator};

use crate::{app::App, gui::util::UiExt};

mod my_bitassets;
mod transfer_receive;
mod tx_builder;
pub(super) mod tx_creator;
mod utxo_creator;
mod utxo_selector;

use my_bitassets::MyBitAssets;
use transfer_receive::TransferReceive;
use tx_builder::TxBuilder;

#[derive(Default, EnumIter, Eq, PartialEq, strum::Display)]
enum Tab {
    #[default]
    #[strum(to_string = "Transfer & Receive")]
    TransferReceive,
    #[strum(to_string = "Transaction Builder")]
    TransactionBuilder,
    #[strum(to_string = "My BitAssets")]
    MyBitAssets,
}

pub struct Coins {
    my_bitassets: MyBitAssets,
    tab: Tab,
    transfer_receive: TransferReceive,
    tx_builder: TxBuilder,
}

impl Coins {
    pub fn new(app: Option<&App>) -> Self {
        Self {
            my_bitassets: MyBitAssets,
            tab: Tab::default(),
            transfer_receive: TransferReceive::new(app),
            tx_builder: TxBuilder::default(),
        }
    }

    pub fn show(
        &mut self,
        app: Option<&App>,
        ui: &mut egui::Ui,
    ) -> anyhow::Result<()> {
        // L-BTC Wallet header wired to elementsd JSON-RPC (real data)
        egui::TopBottomPanel::top("lbtc_header").show(ui.ctx(), |ui| {
            ui.heading("L-BTC Wallet (elementsd)");
            if let Some(app) = app {
                if let Some(rpc) = &app.elements_rpc {
                    // Fetch live (local RPC is fast; block_on is acceptable here)
                    let balance = app
                        .runtime
                        .block_on(rpc.getbalance())
                        .map(|a| format!("{:.8}", a.to_btc()))
                        .unwrap_or_else(|e| format!("error: {e}"));
                    let recv_addr = app
                        .runtime
                        .block_on(rpc.getnewaddress())
                        .unwrap_or_else(|e| format!("error: {e}"));

                    ui.horizontal(|ui| {
                        ui.monospace(format!("Balance: {} L-BTC", balance));
                        if ui.button("Refresh").clicked() {
                            // next frame will refetch
                        }
                    });
                    ui.horizontal(|ui| {
                        ui.monospace("Receive:");
                        ui.monospace_selectable_singleline(true, recv_addr.as_str());
                        if ui.button("Copy").clicked() {
                            ui.output_mut(|o| o.copied_text = recv_addr.clone());
                        }
                    });

                    // UTXOs
                    if let Ok(utxos) = app.runtime.block_on(rpc.listunspent()) {
                        ui.collapsing("UTXOs (listunspent)", |ui| {
                            egui::ScrollArea::vertical()
                                .max_height(120.0)
                                .show(ui, |ui| {
                                    for u in utxos.iter().take(10) {
                                        ui.monospace(format!(
                                            "{}:{}  {} L-BTC  confs:{}",
                                            u.txid,
                                            u.vout,
                                            u.amount.to_btc(),
                                            u.confirmations
                                        ));
                                    }
                                    if utxos.len() > 10 {
                                        ui.monospace(format!("... and {} more", utxos.len() - 10));
                                    }
                                });
                        });
                    }
                } else {
                    ui.monospace("elementsd RPC not connected (no cookie or elementsd down)");
                }
            } else {
                ui.monospace("App not initialized");
            }
        });

        egui::TopBottomPanel::top("coins_tabs").show(ui.ctx(), |ui| {
            ui.horizontal(|ui| {
                Tab::iter().for_each(|tab_variant| {
                    let tab_name = tab_variant.to_string();
                    ui.selectable_value(&mut self.tab, tab_variant, tab_name);
                })
            });
        });
        egui::CentralPanel::default().show(ui.ctx(), |ui| match self.tab {
            Tab::TransferReceive => {
                let () = self.transfer_receive.show(app, ui);
            }
            Tab::TransactionBuilder => {
                let () = self.tx_builder.show(app, ui).unwrap();
            }
            Tab::MyBitAssets => {
                self.my_bitassets.show(app, ui);
            }
        });
        Ok(())
    }
}
