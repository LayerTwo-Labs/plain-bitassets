use std::sync::{
    Arc,
    atomic::{self, AtomicBool},
};

use eframe::egui::{self, Button};
use futures::FutureExt as _;
use liquid_simplicity::types::proto::mainchain;

use crate::app::App;

#[derive(Debug, Default)]
struct LiveData {
    mainchain_tip: Option<anyhow::Result<mainchain::BlockHeaderInfo>>,
    elements_height: Option<anyhow::Result<u64>>,
}

#[derive(Debug)]
pub struct Miner {
    running: Arc<AtomicBool>,
    live: LiveData,
    loaded: bool,
}

impl Default for Miner {
    fn default() -> Self {
        Self {
            running: Arc::new(AtomicBool::new(false)),
            live: LiveData::default(),
            loaded: false,
        }
    }
}

impl Miner {
    fn refresh(&mut self, app: &App) {
        self.live.mainchain_tip = Some(
            app.runtime
                .block_on(
                    app.node
                        .with_cusf_mainchain(|c| c.get_chain_tip().boxed()),
                )
                .map_err(anyhow::Error::from),
        );
        self.live.elements_height = app.elements_rpc.as_ref().map(|rpc| {
            app.runtime
                .block_on(rpc.getblockcount())
                .map_err(anyhow::Error::from)
        });
        self.loaded = true;
    }

    pub fn show(&mut self, app: Option<&App>, ui: &mut egui::Ui) {
        if !self.loaded {
            if let Some(app) = app {
                self.refresh(app);
            }
        }

        ui.heading("BMM Status");
        ui.add_space(4.0);

        ui.horizontal(|ui| {
            ui.label("Elementsd block count (:18443):");
            match self.live.elements_height.as_ref() {
                Some(Ok(h)) => {
                    ui.monospace(h.to_string());
                }
                Some(Err(e)) => {
                    ui.colored_label(egui::Color32::RED, format!("err: {e:#}"));
                }
                None => {
                    ui.monospace("(no elements_rpc configured)");
                }
            }
        });

        ui.horizontal(|ui| {
            ui.label("Mainchain height (enforcer :50051):");
            match self.live.mainchain_tip.as_ref() {
                Some(Ok(tip)) => {
                    ui.monospace(tip.height.to_string());
                }
                Some(Err(e)) => {
                    ui.colored_label(egui::Color32::RED, format!("err: {e:#}"));
                }
                None => {
                    ui.monospace("—");
                }
            }
        });

        ui.horizontal(|ui| {
            ui.label("Latest mainchain tip hash:");
            match self.live.mainchain_tip.as_ref() {
                Some(Ok(tip)) => {
                    let hash = tip.block_hash.to_string();
                    let short = if hash.len() > 16 {
                        format!("{}…", &hash[..16])
                    } else {
                        hash
                    };
                    ui.monospace(short);
                }
                Some(Err(_)) => {
                    ui.monospace("(error)");
                }
                None => {
                    ui.monospace("—");
                }
            }
        });

        ui.horizontal(|ui| {
            ui.label("Status:");
            ui.colored_label(
                egui::Color32::from_rgb(0x2e, 0x7d, 0x32),
                "Simplicity ALWAYS_ACTIVE",
            );
        });

        ui.add_space(4.0);

        let sidechain_tip = app
            .and_then(|app| app.node.try_get_tip_height().ok().flatten())
            .unwrap_or(0);
        ui.horizontal(|ui| {
            ui.label("Internal sidechain tip height:");
            ui.monospace(sidechain_tip.to_string());
        });

        ui.add_space(8.0);

        ui.horizontal(|ui| {
            if ui
                .add_enabled(app.is_some(), Button::new("Refresh"))
                .clicked()
            {
                self.refresh(app.unwrap());
            }

            let running = self.running.load(atomic::Ordering::SeqCst);
            if let Some(app) = app {
                if ui
                    .add_enabled(!running, Button::new("Mine / Request BMM Block"))
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
        });
    }
}
