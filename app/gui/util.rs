use std::borrow::Borrow;

use borsh::BorshDeserialize;
use eframe::egui::{self, InnerResponse, Response, Ui};
use libes::{auth::HmacSha256, enc::Aes256Gcm, key::X25519};

/// ecies crypto scheme over x25519
pub type Ecies = libes::Ecies<X25519, Aes256Gcm, HmacSha256>;

// extension for InnerResponse<Response> and InnerResponse<Option<Response>>
pub trait InnerResponseExt {
    fn join(self) -> Response;
}

/// extension trait for egui::Ui
pub trait UiExt {
    fn monospace_selectable_singleline<Text>(&mut self, text: Text) -> Response
    where
        Text: Borrow<str>;

    fn monospace_selectable_multiline<Text>(&mut self, text: Text) -> Response
    where
        Text: Borrow<str>;
}

impl InnerResponseExt for InnerResponse<Response> {
    fn join(self) -> Response {
        self.response | self.inner
    }
}

impl InnerResponseExt for InnerResponse<Option<Response>> {
    fn join(self) -> Response {
        match self.inner {
            Some(inner) => self.response | inner,
            None => self.response,
        }
    }
}

impl UiExt for Ui {
    fn monospace_selectable_singleline<Text>(&mut self, text: Text) -> Response
    where
        Text: Borrow<str>,
    {
        use egui::{TextEdit, TextStyle, Widget};
        let mut text: &str = text.borrow();
        TextEdit::singleline(&mut text)
            .font(TextStyle::Monospace)
            .ui(self)
    }

    fn monospace_selectable_multiline<Text>(&mut self, text: Text) -> Response
    where
        Text: Borrow<str>,
    {
        use egui::{TextEdit, TextStyle, Widget};
        let mut text: &str = text.borrow();
        TextEdit::multiline(&mut text)
            .font(TextStyle::Monospace)
            .ui(self)
    }
}

pub fn borsh_deserialize_hex<T>(hex: impl AsRef<[u8]>) -> anyhow::Result<T>
where
    T: BorshDeserialize,
{
    match hex::decode(hex) {
        Ok(bytes) => borsh::BorshDeserialize::try_from_slice(&bytes)
            .map_err(anyhow::Error::new),
        Err(err) => Err(anyhow::Error::new(err)),
    }
}
