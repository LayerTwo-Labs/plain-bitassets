use std::collections::HashSet;

use eframe::egui;
use hex::FromHex;
use plain_bitassets::types::Hash;

#[derive(Debug, Default)]
struct BitassetInboxesState {
    add_bitasset_buffer: String,
    err_msg: Option<String>,
}

#[derive(Debug, Default, Eq, PartialEq)]
enum Tab {
    #[default]
    BitassetInboxes,
}

#[derive(Debug, Default)]
pub struct Settings {
    pub bitasset_inboxes: HashSet<Hash>,
    bitasset_inboxes_state: BitassetInboxesState,
    tab: Tab,
}

impl Settings {
    fn show_bitasset_inboxes(&mut self, ui: &mut egui::Ui) {
        let state = &mut self.bitasset_inboxes_state;
        ui.heading("Bitasset Inboxes");
        ui.horizontal(|ui| {
            ui.text_edit_singleline(&mut state.add_bitasset_buffer);
            if ui.button("Add Bitasset Inbox").clicked() {
                match Hash::from_hex(&state.add_bitasset_buffer) {
                    Ok(bitasset) => {
                        self.bitasset_inboxes.insert(bitasset);
                        state.err_msg = None;
                        state.add_bitasset_buffer.clear();
                    }
                    Err(err) => {
                        state.err_msg = Some(err.to_string());
                    }
                }
            };
        });
        if let Some(err_msg) = &state.err_msg {
            ui.monospace(format!("Error decoding bitasset: {err_msg}"));
        }
        egui::Grid::new("bitasset inboxes").show(ui, |ui| {
            self.bitasset_inboxes.retain(|bitasset| {
                ui.monospace(hex::encode(bitasset));
                let button = ui.button("Remove");
                ui.end_row();
                !button.clicked()
            })
        });
    }

    pub fn show(&mut self, ui: &mut egui::Ui) {
        egui::SidePanel::left("Inbox")
            //.exact_width(250.)
            .show_inside(ui, |ui| {
                ui.vertical(|ui| {
                    ui.selectable_value(
                        &mut self.tab,
                        Tab::BitassetInboxes,
                        "Bitasset Inboxes",
                    );
                });
            });
        egui::CentralPanel::default().show(ui.ctx(), |ui| match self.tab {
            Tab::BitassetInboxes => {
                self.show_bitasset_inboxes(ui);
            }
        });
    }
}
