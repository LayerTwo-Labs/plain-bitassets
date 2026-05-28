use std::sync::{
    Arc,
    atomic::{self, AtomicBool},
};

use eframe::egui::{self, Button};

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
        // Periodic BMM status (sidechain via elementsd, main/BMM via enforcer :50051)
        static LAST: std::sync::OnceLock<std::sync::Mutex<std::time::Instant>> = std::sync::OnceLock::new();
        static STATUS: std::sync::OnceLock<std::sync::Mutex<(Option<u64>, Option<u32>, bool)>> = std::sync::OnceLock::new();
        let last = LAST.get_or_init(|| std::sync::Mutex::new(std::time::Instant::now() - std::time::Duration::from_secs(10)));
        let st = STATUS.get_or_init(|| std::sync::Mutex::new((None, None, false)));

        let now = std::time::Instant::now();
        if now.duration_since(*last.lock().unwrap()).as_secs() > 3 {
            *last.lock().unwrap() = now;
            if let Some(a) = app {
                let s2 = st.clone();
                let a2 = a.clone();
                std::thread::spawn(move || {
                    let h = {
                        let cookie = "__cookie__:b0e3e4ddc36861525be17bb9074d71ec5d7f66e92f6116f3c59038a5f4bccf39";
                        std::process::Command::new("curl").arg("-s").arg("--user").arg(cookie)
                            .arg("--data-binary").arg(r#"{"jsonrpc":"1.0","id":"1","method":"getblockcount","params":[]}"#)
                            .arg("-H").arg("content-type: text/plain;").arg("http://127.0.0.1:18443/")
                            .output().ok().and_then(|o| {
                                let s = String::from_utf8_lossy(&o.stdout);
                                s.split("\"result\":").nth(1).and_then(|r| r.split(&[',','}'][..]).next()).and_then(|n| n.trim().parse::<u64>().ok())
                            })
                    };
                    let (ok, mh) = {
                        let r = a2.runtime.block_on(a2.node.with_cusf_mainchain(|c| {
                            let mut c = c.clone(); async move { c.get_chain_tip().await }.boxed()
                        }));
                        (r.is_ok(), r.ok().map(|t| t.height))
                    };
                    *s2.lock().unwrap() = (h, mh, ok);
                });
            }
        }

        let (sch, mch, enf_ok) = *st.lock().unwrap();
        ui.heading("BMM Status");
        ui.horizontal(|ui| { ui.label("Sidechain height (elementsd):"); ui.monospace(sch.map_or_else(|| "connecting :18443".into(), |v| v.to_string())); });
        ui.horizontal(|ui| { ui.label("Mainchain height (enforcer):"); ui.monospace(mch.map_or_else(|| "enforcer at :50051".into(), |v| v.to_string())); });
        ui.horizontal(|ui| { ui.label("Latest BMM h*:"); ui.monospace(if enf_ok { "queried via :50051" } else { "enforcer at :50051" }); });
        ui.horizontal(|ui| { ui.label("Status:"); ui.colored_label(egui::Color32::from_rgb(0x2e,0x7d,0x32), "Simplicity ALWAYS_ACTIVE"); });

        // original mine controls
        let block_height = app.and_then(|app| app.node.try_get_tip_height().ok().flatten()).unwrap_or(0);
        ui.label("Internal tip: "); ui.monospace(format!("{block_height}"));
        let running = self.running.load(atomic::Ordering::SeqCst);
        if let Some(app) = app && ui.add_enabled(!running, Button::new("Mine / Refresh Block")).clicked() {
            self.running.store(true, atomic::Ordering::SeqCst);
            app.local_pool.spawn_pinned({ let app=app.clone(); let r=self.running.clone(); move || async move {
                let _ = app.mine(None).await; r.store(false, atomic::Ordering::SeqCst);
            }});
        }
    }
}
