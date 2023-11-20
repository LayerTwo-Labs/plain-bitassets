use eframe::egui::{self, Color32};

use crate::app::App;

mod activity;
mod bitasset_explorer;
mod coins;
mod deposit;
mod encrypt_message;
mod miner;
mod paymail;
mod seed;
mod util;

use activity::Activity;
use bitasset_explorer::BitassetExplorer;
use coins::Coins;
use deposit::Deposit;
use encrypt_message::EncryptMessage;
use miner::Miner;
use paymail::Paymail;
use seed::SetSeed;

pub struct EguiApp {
    app: App,
    set_seed: SetSeed,
    miner: Miner,
    deposit: Deposit,
    tab: Tab,
    activity: Activity,
    coins: Coins,
    bitasset_explorer: BitassetExplorer,
    paymail: Paymail,
    encrypt_message: EncryptMessage,
}

#[derive(Eq, PartialEq)]
enum Tab {
    Coins,
    BitassetExplorer,
    Paymail,
    EncryptMessage,
    Activity,
}

impl EguiApp {
    pub fn new(app: App, cc: &eframe::CreationContext<'_>) -> Self {
        // Customize egui here with cc.egui_ctx.set_fonts and cc.egui_ctx.set_visuals.
        // Restore app state using cc.storage (requires the "persistence" feature).
        // Use the cc.gl (a glow::Context) to create graphics shaders and buffers that you can use
        // for e.g. egui::PaintCallback.
        let mut style = (*cc.egui_ctx.style()).clone();
        style.visuals.panel_fill = Color32::from_rgb(0, 0, 0x33);
        style.visuals.extreme_bg_color = Color32::from_gray(0x3c);
        style.visuals.faint_bg_color = Color32::from_rgb(0, 0, 0x44);
        style.visuals.widgets.noninteractive.bg_stroke.color = Color32::YELLOW;

        cc.egui_ctx.set_style(style);

        let activity = Activity::new(&app);
        Self {
            app,
            set_seed: SetSeed::default(),
            miner: Miner,
            deposit: Deposit::default(),
            bitasset_explorer: BitassetExplorer::default(),
            tab: Tab::Coins,
            activity,
            coins: Coins::default(),
            encrypt_message: EncryptMessage::new(),
            paymail: Paymail::default(),
        }
    }
}

impl eframe::App for EguiApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if self.app.wallet.has_seed().unwrap_or(false) {
            egui::TopBottomPanel::top("tabs").show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.selectable_value(&mut self.tab, Tab::Coins, "coins");
                    ui.selectable_value(
                        &mut self.tab,
                        Tab::BitassetExplorer,
                        "lookup",
                    );
                    ui.selectable_value(
                        &mut self.tab,
                        Tab::Paymail,
                        "my paymail",
                    );
                    ui.selectable_value(
                        &mut self.tab,
                        Tab::EncryptMessage,
                        "messaging",
                    );
                    ui.selectable_value(
                        &mut self.tab,
                        Tab::Activity,
                        "activity",
                    );
                });
            });
            egui::TopBottomPanel::bottom("util").show(ctx, |ui| {
                ui.horizontal(|ui| {
                    self.miner.show(&mut self.app, ui);
                    ui.separator();
                    self.deposit.show(&mut self.app, ui);
                });
            });
            egui::CentralPanel::default().show(ctx, |ui| match self.tab {
                Tab::Coins => {
                    let () = self.coins.show(&mut self.app, ui).unwrap();
                }
                Tab::BitassetExplorer => {
                    self.bitasset_explorer.show(&mut self.app, ui);
                }
                Tab::Paymail => self.paymail.show(&mut self.app, ui).unwrap(),
                Tab::EncryptMessage => {
                    self.encrypt_message.show(&mut self.app, ui);
                }
                Tab::Activity => {
                    self.activity.show(&mut self.app, ui);
                }
            });
        } else {
            egui::CentralPanel::default().show(ctx, |_ui| {
                egui::Window::new("Set Seed").show(ctx, |ui| {
                    self.set_seed.show(&self.app, ui);
                });
            });
        }
    }
}
