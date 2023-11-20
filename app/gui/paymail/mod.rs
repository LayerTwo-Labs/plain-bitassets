use eframe::egui;

use crate::app::App;

mod inbox;
mod settings;

use inbox::Inbox;
use settings::Settings;

#[derive(Debug, Default, Eq, PartialEq)]
enum Tab {
    #[default]
    Inbox,
    Settings,
}

#[derive(Default)]
pub struct Paymail {
    tab: Tab,
    inbox: Inbox,
    settings: Settings,
}

impl Paymail {
    pub fn show(
        &mut self,
        app: &mut App,
        ui: &mut egui::Ui,
    ) -> anyhow::Result<()> {
        ui.heading("My Paymail");
        egui::TopBottomPanel::top("paymail_tabs").show(ui.ctx(), |ui| {
            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.tab, Tab::Inbox, "inbox");
                ui.selectable_value(&mut self.tab, Tab::Settings, "settings");
            });
        });
        egui::CentralPanel::default().show(ui.ctx(), |ui| match self.tab {
            Tab::Inbox => {
                self.inbox.show(app, &self.settings, ui);
            }
            Tab::Settings => {
                self.settings.show(ui);
            }
        });
        Ok(())
    }
}
