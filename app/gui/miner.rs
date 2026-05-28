use std::sync::{
    Arc,
    atomic::{self, AtomicBool},
};

use eframe::egui::{self, Button};
use futures::FutureExt as _;

use crate::app::App;

#[derive(Debug)]
pub struct Miner {
    running: Arc<AtomicBool>,
}

impl Default for Miner {
    fn default() -> Self {
        Self {
            running: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Miner {
    pub fn show(&mut self, app: Option<&App>, ui: &mut egui::Ui) {
        ui.heading("BMM Status");
        ui.horizontal(|ui| {
            ui.label("Sidechain height (elementsd):");
            ui.monospace("query via :18443");
        });
        ui.horizontal(|ui| {
            ui.label("Latest BMM h* commitment:");
            ui.monospace("enforcer at :50051");
        });
        ui.horizontal(|ui| {
            ui.label("Mainchain height (enforcer):");
            ui.monospace("enforcer at :50051");
        });
        ui.horizontal(|ui| {
            ui.label("Status:");
            ui.colored_label(egui::Color32::from_rgb(0x2e, 0x7d, 0x32), "Simplicity ALWAYS_ACTIVE");
        });
        ui.add_space(4.0);

        // keep original internal tip + mine button (safe, compiles)
        let block_height = app
            .and_then(|app| app.node.try_get_tip_height().ok().flatten())
            .unwrap_or(0);
        ui.label("Internal tip: ");
        ui.monospace(format!("{block_height}"));
        let running = self.running.load(atomic::Ordering::SeqCst);
        if let Some(app) = app
            && ui
                .add_enabled(!running, Button::new("Mine / Refresh Block"))
                .clicked()
        {
            self.running.store(true, atomic::Ordering::SeqCst);
            app.local_pool.spawn_pinned({
                let app = app.clone();
                let running = self.running.clone();
                || async move {
                    drop(app.mine(None).await);
                    running.store(false, atomic::Ordering::SeqCst);
                }
            });
        }
    }
}
