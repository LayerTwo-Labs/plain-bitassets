use eframe::egui;

use plain_bitassets::types::OutPoint;

use crate::{app::App, gui::util::UiExt};

use super::settings::Settings;

#[derive(Debug, Default)]
pub struct Inbox {
    selected: Option<(OutPoint, Vec<u8>)>,
}

impl Inbox {
    fn show_error(ui: &mut egui::Ui, error: &anyhow::Error) {
        ui.horizontal_wrapped(|ui| {
            ui.monospace("Error: ");
            ui.code(format!("{error}"));
        });
    }

    fn show_paymail(ui: &mut egui::Ui, outpoint: &OutPoint, memo: &Vec<u8>) {
        let (kind, hash, vout) = match outpoint {
            OutPoint::Regular { txid, vout } => {
                ("regular", format!("{txid}"), *vout)
            }
            OutPoint::Deposit(outpoint) => {
                ("deposit", format!("{}", outpoint.txid), outpoint.vout)
            }
            OutPoint::Coinbase { merkle_root, vout } => {
                ("coinbase", format!("{merkle_root}"), *vout)
            }
        };
        let hash = &hash[0..8];
        ui.monospace(kind.to_string());
        ui.monospace(format!("{hash}:{vout}",));
        ui.monospace(hex::encode(memo));
    }

    fn show_inbox(
        &mut self,
        app: &mut App,
        settings: &Settings,
        ui: &mut egui::Ui,
    ) -> Result<(), anyhow::Error> {
        let paymail = app.get_paymail(Some(&settings.bitasset_inboxes))?;
        let mut paymail: Vec<_> = paymail.iter().collect();
        // FIXME: sort by block/index
        paymail.sort_by_key(|(outpoint, _)| format!("{outpoint}"));
        egui::Grid::new("Inbox").striped(true).show(ui, |ui| {
            ui.monospace("outpoint");
            ui.monospace("memo");
            ui.end_row();
            let mut rows: Vec<(OutPoint, Vec<u8>, egui::Response)> = Vec::new();
            paymail.into_iter().for_each(|(outpoint, output)| {
                let row_response = ui
                    .horizontal(|ui| {
                        Self::show_paymail(ui, outpoint, &output.memo)
                    })
                    .response;
                rows.push((*outpoint, output.memo.clone(), row_response));
                ui.end_row();
            });
            for (outpoint, memo, resp) in rows {
                if resp.clicked() {
                    self.selected = Some((outpoint, memo));
                    break;
                }
            }
        });
        Ok(())
    }

    fn show_selected(
        ui: &mut egui::Ui,
        (outpoint, memo): &(OutPoint, Vec<u8>),
    ) {
        ui.heading(outpoint.to_string());
        ui.separator();
        ui.monospace_selectable_multiline(hex::encode(memo));
    }

    pub fn show(
        &mut self,
        app: &mut App,
        settings: &Settings,
        ui: &mut egui::Ui,
    ) {
        egui::SidePanel::left("Inbox")
            //.exact_width(250.)
            .show_inside(ui, |ui| {
                let () = self
                    .show_inbox(app, settings, ui)
                    .unwrap_or_else(|err| Self::show_error(ui, &err));
            });
        if let Some(selected) = &self.selected {
            egui::CentralPanel::default().show_inside(ui, |ui| {
                let () = Self::show_selected(ui, selected);
            });
        }
    }
}
