use eframe::egui;
use strum::{EnumIter, IntoEnumIterator};

use crate::app::App;

mod decrypt;
mod encrypt;

use decrypt::DecryptMessage;
use encrypt::EncryptMessage;

#[derive(Default, EnumIter, Eq, PartialEq, strum::Display)]
enum Tab {
    #[strum(to_string = "Decrypt")]
    Decrypt,
    #[default]
    #[strum(to_string = "Encrypt")]
    Encrypt,
}

pub struct Messaging {
    decrypt: DecryptMessage,
    encrypt: EncryptMessage,
    tab: Tab,
}

impl Messaging {
    pub fn new() -> Self {
        Self {
            decrypt: DecryptMessage::new(),
            encrypt: EncryptMessage::new(),
            tab: Tab::default(),
        }
    }

    pub fn show(&mut self, app: Option<&App>, ui: &mut egui::Ui) {
        egui::TopBottomPanel::top("messaging_tabs").show(ui.ctx(), |ui| {
            ui.horizontal(|ui| {
                Tab::iter().for_each(|tab_variant| {
                    let tab_name = tab_variant.to_string();
                    ui.selectable_value(&mut self.tab, tab_variant, tab_name);
                })
            });
        });
        egui::CentralPanel::default().show(ui.ctx(), |ui| match self.tab {
            Tab::Decrypt => {
                self.decrypt.show(app, ui);
            }
            Tab::Encrypt => {
                self.encrypt.show(app, ui);
            }
        });
    }
}
