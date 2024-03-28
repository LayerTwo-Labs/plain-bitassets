use eframe::egui::{self, Color32};
use strum::{EnumIter, IntoEnumIterator};

use crate::{app::App, logs::LogsCapture};

mod activity;
mod bitassets;
mod coins;
mod encrypt_message;
mod logs;
mod miner;
mod parent_chain;
mod seed;
mod util;

use activity::Activity;
use bitassets::BitAssets;
use coins::Coins;
use encrypt_message::EncryptMessage;
use logs::Logs;
use miner::Miner;
use parent_chain::ParentChain;
use seed::SetSeed;

pub struct EguiApp {
    activity: Activity,
    app: App,
    bitassets: BitAssets,
    coins: Coins,
    encrypt_message: EncryptMessage,
    logs: Logs,
    miner: Miner,
    parent_chain: ParentChain,
    set_seed: SetSeed,
    tab: Tab,
}

#[derive(Default, EnumIter, Eq, PartialEq, strum::Display)]
enum Tab {
    #[default]
    #[strum(to_string = "Parent Chain")]
    ParentChain,
    #[strum(to_string = "Coins")]
    Coins,
    #[strum(to_string = "BitAssets")]
    BitAssets,
    #[strum(to_string = "Messaging")]
    EncryptMessage,
    #[strum(to_string = "Activity")]
    Activity,
    #[strum(to_string = "Logs")]
    Logs,
}

impl EguiApp {
    pub fn new(
        app: App,
        cc: &eframe::CreationContext<'_>,
        logs_capture: LogsCapture,
    ) -> Self {
        // Customize egui here with cc.egui_ctx.set_fonts and cc.egui_ctx.set_visuals.
        // Restore app state using cc.storage (requires the "persistence" feature).
        // Use the cc.gl (a glow::Context) to create graphics shaders and buffers that you can use
        // for e.g. egui::PaintCallback.
        let mut style = (*cc.egui_ctx.style()).clone();
        // Palette found using https://coolors.co/005c80-a0a0a0-93032e-ff5400-ffbd00
        // Default blue, eg. selected buttons
        const _LAPIS_LAZULI: Color32 = Color32::from_rgb(0x0D, 0x5c, 0x80);
        // Default grey, eg. grid lines
        const _CADET_GREY: Color32 = Color32::from_rgb(0xa0, 0xa0, 0xa0);
        const _BURGUNDY: Color32 = Color32::from_rgb(0x93, 0x03, 0x2e);
        const ORANGE: Color32 = Color32::from_rgb(0xff, 0x54, 0x00);
        const _AMBER: Color32 = Color32::from_rgb(0xff, 0xbd, 0x00);
        // Accent color
        const ACCENT: Color32 = ORANGE;
        // Grid color / accent color
        style.visuals.widgets.noninteractive.bg_stroke.color = ACCENT;

        cc.egui_ctx.set_style(style);

        let activity = Activity::new(&app);
        let coins = Coins::new(&app);
        let parent_chain = ParentChain::new(&app);
        Self {
            activity,
            app,
            bitassets: BitAssets::default(),
            coins,
            encrypt_message: EncryptMessage::new(),
            logs: Logs::new(logs_capture),
            miner: Miner::default(),
            parent_chain,
            set_seed: SetSeed::default(),
            tab: Tab::default(),
        }
    }

    fn bottom_panel_content(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            // Fill center space,
            // see https://github.com/emilk/egui/discussions/3908#discussioncomment-8270353

            // this frame target width
            // == this frame initial max rect width - last frame others width
            let id_cal_target_size = egui::Id::new("cal_target_size");
            let this_init_max_width = ui.max_rect().width();
            let last_others_width = ui.data(|data| {
                data.get_temp(id_cal_target_size)
                    .unwrap_or(this_init_max_width)
            });
            // this is the total available space for expandable widgets, you can divide
            // it up if you have multiple widgets to expand, even with different ratios.
            let this_target_width = this_init_max_width - last_others_width;

            ui.add_space(this_target_width);
            ui.separator();
            self.miner.show(&self.app, ui);
            // this frame others width
            // == this frame final min rect width - this frame target width
            ui.data_mut(|data| {
                data.insert_temp(
                    id_cal_target_size,
                    ui.min_rect().width() - this_target_width,
                )
            });
        });
    }
}

impl eframe::App for EguiApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if self.app.wallet.has_seed().unwrap_or(false) {
            egui::TopBottomPanel::top("tabs").show(ctx, |ui| {
                ui.horizontal(|ui| {
                    Tab::iter().for_each(|tab_variant| {
                        let tab_name = tab_variant.to_string();
                        ui.selectable_value(
                            &mut self.tab,
                            tab_variant,
                            tab_name,
                        );
                    })
                });
            });
            egui::TopBottomPanel::bottom("util")
                .show(ctx, |ui| self.bottom_panel_content(ui));
            egui::CentralPanel::default().show(ctx, |ui| match self.tab {
                Tab::ParentChain => {
                    self.parent_chain.show(&mut self.app, ui);
                }
                Tab::Coins => {
                    let () = self.coins.show(&mut self.app, ui).unwrap();
                }
                Tab::BitAssets => {
                    self.bitassets.show(&mut self.app, ui);
                }
                Tab::EncryptMessage => {
                    self.encrypt_message.show(&mut self.app, ui);
                }
                Tab::Activity => {
                    self.activity.show(&mut self.app, ui);
                }
                Tab::Logs => {
                    self.logs.show(ui);
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
