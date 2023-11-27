use std::collections::HashSet;

use eframe::egui;

use plain_bitassets::{
    bip300301::bitcoin,
    types::{GetBitcoinValue, Transaction},
};

use super::{
    tx_creator::TxCreator,
    utxo_creator::UtxoCreator,
    utxo_selector::{show_utxo, UtxoSelector},
};
use crate::app::App;

#[derive(Debug, Default)]
pub struct TxBuilder {
    // regular tx without extra data or special inputs/outputs
    base_tx: Transaction,
    tx_creator: TxCreator,
    utxo_creator: UtxoCreator,
    utxo_selector: UtxoSelector,
}

impl TxBuilder {
    pub fn show_value_in(&mut self, app: &mut App, ui: &mut egui::Ui) {
        ui.heading("Value In");
        let selected: HashSet<_> =
            self.base_tx.inputs.iter().cloned().collect();
        let utxos_read = app.utxos.read();
        let mut spent_utxos: Vec<_> = utxos_read
            .iter()
            .filter(|(outpoint, _)| selected.contains(outpoint))
            .collect();
        let bitcoin_value_in: u64 = spent_utxos
            .iter()
            .map(|(_, output)| output.get_bitcoin_value())
            .sum();
        self.tx_creator.bitcoin_value_in = bitcoin_value_in;
        spent_utxos.sort_by_key(|(outpoint, _)| format!("{outpoint}"));
        ui.separator();
        ui.monospace(format!(
            "Total: {}",
            bitcoin::Amount::from_sat(bitcoin_value_in)
        ));
        ui.separator();
        egui::Grid::new("utxos").striped(true).show(ui, |ui| {
            ui.monospace("kind");
            ui.monospace("outpoint");
            ui.monospace("value");
            ui.end_row();
            let mut remove = None;
            for (vout, outpoint) in self.base_tx.inputs.iter().enumerate() {
                let output = &utxos_read[outpoint];
                if output.get_bitcoin_value() != 0 {
                    show_utxo(ui, outpoint, output);
                    if ui.button("remove").clicked() {
                        remove = Some(vout);
                    }
                    ui.end_row();
                }
            }
            if let Some(vout) = remove {
                self.base_tx.inputs.remove(vout);
            }
        });
    }

    pub fn show_value_out(&mut self, ui: &mut egui::Ui) {
        ui.heading("Value Out");
        ui.separator();
        let bitcoin_value_out: u64 = self
            .base_tx
            .outputs
            .iter()
            .map(GetBitcoinValue::get_bitcoin_value)
            .sum();
        self.tx_creator.bitcoin_value_out = bitcoin_value_out;
        ui.monospace(format!(
            "Total: {}",
            bitcoin::Amount::from_sat(bitcoin_value_out)
        ));
        ui.separator();
        egui::Grid::new("outputs").striped(true).show(ui, |ui| {
            let mut remove = None;
            ui.monospace("vout");
            ui.monospace("address");
            ui.monospace("value");
            ui.end_row();
            for (vout, output) in self.base_tx.indexed_bitcoin_value_outputs() {
                let address = &format!("{}", output.address)[0..8];
                let value =
                    bitcoin::Amount::from_sat(output.get_bitcoin_value());
                ui.monospace(format!("{vout}"));
                ui.monospace(address.to_string());
                ui.with_layout(
                    egui::Layout::right_to_left(egui::Align::Max),
                    |ui| {
                        ui.monospace(format!("₿{value}"));
                    },
                );
                if ui.button("remove").clicked() {
                    remove = Some(vout);
                }
                ui.end_row();
            }
            if let Some(vout) = remove {
                self.base_tx.outputs.remove(vout);
            }
        });
    }

    pub fn show(
        &mut self,
        app: &mut App,
        ui: &mut egui::Ui,
    ) -> anyhow::Result<()> {
        egui::SidePanel::left("spend_utxo")
            .exact_width(250.)
            .resizable(false)
            .show_inside(ui, |ui| {
                self.utxo_selector.show(app, ui, &mut self.base_tx);
            });
        egui::SidePanel::left("value_in")
            .exact_width(250.)
            .resizable(false)
            .show_inside(ui, |ui| {
                let () = self.show_value_in(app, ui);
            });
        egui::SidePanel::left("value_out")
            .exact_width(250.)
            .resizable(false)
            .show_inside(ui, |ui| {
                let () = self.show_value_out(ui);
            });
        egui::SidePanel::left("create_utxo")
            .exact_width(450.)
            .resizable(false)
            .show_separator_line(false)
            .show_inside(ui, |ui| {
                self.utxo_creator.show(app, ui, &mut self.base_tx);
                ui.separator();
                self.tx_creator.show(app, ui, &mut self.base_tx).unwrap();
            });
        Ok(())
    }
}
