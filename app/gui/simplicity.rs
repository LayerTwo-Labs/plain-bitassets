use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use eframe::egui::{self, ComboBox};

use crate::app::App;

use super::util::UiExt;

const MINIMAL_PROGRAM_HEX: &str = "e0094081020408102040810205b46da080";
const CMR: &str = "8745774d6c695d360bb788311e7a0396d397bcbb6ac4ef02916b6468ef28a4f4";

const DEFAULT_PYTHON_SCRIPT: &str =
    "/Volumes/T705/code/liquid-signet-sidechain/drivechain-liquid-sidechain/tests/simplicity_e2e_tx.py";
const DEFAULT_ELEMENTS_CLI: &str =
    "/Volumes/T705/code/liquid-signet-sidechain/src/elements-cli";

fn resolve_script_path() -> String {
    if let Ok(p) = std::env::var("SIMPLICITY_E2E_SCRIPT") {
        return p;
    }
    if let Some(home) = std::env::var_os("HOME") {
        let candidate = std::path::Path::new(&home)
            .join(".config/liquid-simplicity/simplicity_e2e_tx.py");
        if candidate.exists() {
            return candidate.to_string_lossy().into_owned();
        }
    }
    DEFAULT_PYTHON_SCRIPT.to_string()
}

fn resolve_elements_cli() -> String {
    std::env::var("ELEMENTS_CLI_BIN").unwrap_or_else(|_| DEFAULT_ELEMENTS_CLI.to_string())
}

#[derive(Default)]
pub struct Simplicity {
    selected_program: String,
    amount: String,
    result: Arc<Mutex<Option<Result<String, String>>>>,
    running: Arc<AtomicBool>,
}

impl Simplicity {
    pub fn new() -> Self {
        Self {
            selected_program: "Minimal unit program (test.c:642)".to_string(),
            amount: "0.001".to_string(),
            result: Arc::new(Mutex::new(None)),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn show(&mut self, app: Option<&App>, ui: &mut egui::Ui) {
        ui.heading("Simplicity TX Builder");
        ui.add_space(8.0);

        // Program dropdown
        ui.horizontal(|ui| {
            ui.label("Program:");
            ComboBox::from_id_salt("simplicity_program")
                .selected_text(&self.selected_program)
                .width(320.0)
                .show_ui(ui, |ui| {
                    ui.selectable_value(
                        &mut self.selected_program,
                        "Minimal unit program (test.c:642)".to_string(),
                        "Minimal unit program (test.c:642)",
                    );
                });
        });

        ui.add_space(4.0);

        // Program bytes (read-only)
        ui.horizontal(|ui| {
            ui.label("Program bytes (hex):");
            let mut hex = MINIMAL_PROGRAM_HEX.to_string();
            ui.add(
                egui::TextEdit::singleline(&mut hex)
                    .font(egui::TextStyle::Monospace)
                    .desired_width(380.0)
                    .interactive(false),
            );
        });

        // CMR (read-only)
        ui.horizontal(|ui| {
            ui.label("CMR:");
            let mut cmr = CMR.to_string();
            ui.add(
                egui::TextEdit::singleline(&mut cmr)
                    .font(egui::TextStyle::Monospace)
                    .desired_width(380.0)
                    .interactive(false),
            );
        });

        ui.add_space(4.0);

        // Amount (L-BTC)
        ui.horizontal(|ui| {
            ui.label("Amount (L-BTC):");
            ui.add(
                egui::TextEdit::singleline(&mut self.amount)
                    .desired_width(80.0)
                    .hint_text("0.001"),
            );
        });

        ui.add_space(8.0);

        let is_running = self.running.load(Ordering::SeqCst);
        let can_send = app.is_some() && !is_running;

        if ui
            .add_enabled(can_send, egui::Button::new("Send Simplicity TX"))
            .clicked()
        {
            if let Some(app) = app {
                self.send_tx(app);
            }
        }

        if is_running {
            ui.label(
                egui::RichText::new("Sending... (mining blocks, broadcasting 0xbe tx — may take 1-2 minutes)")
                    .italics(),
            );
        }

        // Result area
        let mut result_guard = self.result.lock().unwrap();
        if let Some(res) = result_guard.as_ref() {
            ui.add_space(8.0);
            ui.separator();
            ui.add_space(4.0);
            match res {
                Ok(txid) => {
                    ui.colored_label(egui::Color32::from_rgb(0x2e, 0x7d, 0x32), "✓ Success");
                    ui.horizontal(|ui| {
                        ui.label("txid:");
                        ui.monospace_selectable_singleline(false, txid.as_str());
                    });
                    ui.label(
                        egui::RichText::new(
                            "Check elementsd with: elements-cli -regtest getrawtransaction <txid> 1",
                        )
                        .small()
                        .weak(),
                    );
                }
                Err(err) => {
                    ui.colored_label(egui::Color32::from_rgb(0xc6, 0x28, 0x28), "✗ Error");
                    // Show error, possibly truncated
                    let display_err = if err.len() > 2000 {
                        format!("{}...\n[truncated]", &err[..2000])
                    } else {
                        err.clone()
                    };
                    ui.monospace_selectable_multiline(display_err.as_str());
                }
            }
        }

        // Clear button when not running and have result
        if result_guard.is_some() && !is_running {
            ui.add_space(4.0);
            if ui.button("Clear result").clicked() {
                *result_guard = None;
            }
        }
        drop(result_guard);
    }

    fn send_tx(&mut self, app: &App) {
        // Clear previous result
        *self.result.lock().unwrap() = None;

        let result = self.result.clone();
        let running = self.running.clone();
        // amount captured for future use / logging
        let _amount = self.amount.clone();

        app.runtime.spawn_blocking(move || {
            running.store(true, Ordering::SeqCst);

            let script = resolve_script_path();
            let elements_cli = resolve_elements_cli();
            let repo_root = std::env::var("SIMPLICITY_REPO_ROOT")
                .unwrap_or_else(|_| "/Volumes/T705/code/liquid-signet-sidechain".to_string());
            let output = std::process::Command::new("python3")
                .arg(&script)
                .env("REPO_ROOT", &repo_root)
                .env("LIQUID_ID5_DATADIR", "/tmp/liquid-id5-regtest")
                .env("LIQUID_ID5_RPCPORT", "18443")
                .env("ELEMENTS_CLI", &elements_cli)
                .output();

            let res = match output {
                Ok(out) => {
                    let stdout = String::from_utf8_lossy(&out.stdout);
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    let combined = if stderr.trim().is_empty() {
                        stdout.to_string()
                    } else {
                        format!("{}\n--- STDERR ---\n{}", stdout, stderr)
                    };

                    if out.status.success() {
                        if let Some(txid) = extract_txid(&stdout) {
                            Ok(txid)
                        } else {
                            // Fallback: last non-empty line
                            let last = stdout
                                .lines()
                                .rev()
                                .find(|l| !l.trim().is_empty())
                                .unwrap_or("success (no txid line found)")
                                .to_string();
                            Ok(last)
                        }
                    } else {
                        Err(format!(
                            "python exited with status {}\n{}",
                            out.status, combined
                        ))
                    }
                }
                Err(e) => Err(format!("Failed to execute python3: {}", e)),
            };

            *result.lock().unwrap() = Some(res);
            running.store(false, Ordering::SeqCst);
        });
    }
}

fn extract_txid(output: &str) -> Option<String> {
    // Look for 64-char hex strings near txid mentions, prefer later lines
    let lines: Vec<&str> = output.lines().collect();
    for line in lines.iter().rev() {
        let lower = line.to_lowercase();
        if lower.contains("txid") || lower.contains("broadcast") || lower.contains("success") {
            for word in line.split_whitespace() {
                let clean: String = word.chars().filter(|c| c.is_ascii_hexdigit()).collect();
                if clean.len() == 64 {
                    return Some(clean);
                }
            }
        }
    }
    // Broad search for any 64 hex in output (last occurrence preferred)
    let mut last: Option<String> = None;
    for line in &lines {
        for word in line.split_whitespace() {
            let clean: String = word.chars().filter(|c| c.is_ascii_hexdigit()).collect();
            if clean.len() == 64 {
                last = Some(clean);
            }
        }
    }
    last
}
