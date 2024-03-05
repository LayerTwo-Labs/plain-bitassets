use eframe::egui::{self, TextEdit, TextStyle, Widget as _};

use crate::logs::LogsCapture;

pub struct Logs(LogsCapture);

impl Logs {
    pub fn new(capture: LogsCapture) -> Self {
        Self(capture)
    }

    pub fn show(&self, ui: &mut egui::Ui) {
        egui::CentralPanel::default().show(ui.ctx(), |ui| {
            let text_read = self.0.as_str();
            let mut text: &str = &text_read;
            TextEdit::multiline(&mut text)
                .font(TextStyle::Monospace)
                .desired_width(f32::INFINITY)
                .ui(ui)
        });
    }
}
