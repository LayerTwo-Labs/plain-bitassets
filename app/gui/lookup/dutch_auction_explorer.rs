use std::fmt::Display;

use eframe::egui::{self, InnerResponse, Response};

use plain_bitassets::{node, state::DutchAuctionState};

use crate::{
    app::App,
    gui::util::{borsh_deserialize_hex, InnerResponseExt, UiExt},
};

/// result of the last auction lookup query
#[derive(Debug)]
struct LastQueryResult(Result<Option<DutchAuctionState>, node::Error>);

#[derive(Debug, Default)]
pub struct DutchAuctionExplorer {
    auction_id: String,
    err_msg: Option<anyhow::Error>,
    last_query_result: Option<LastQueryResult>,
}

pub fn show_dutch_auction_state(
    ui: &mut egui::Ui,
    auction_state: &DutchAuctionState,
) -> Response {
    fn show_line(
        ui: &mut egui::Ui,
        value: &dyn Display,
        descriptor: &str,
    ) -> InnerResponse<Response> {
        ui.horizontal(|ui| {
            ui.monospace_selectable_singleline(format!("{descriptor}: {value}"))
        })
    }
    let DutchAuctionState {
        start_block,
        most_recent_bid_block,
        duration,
        base_asset,
        initial_base_amount,
        base_amount_remaining,
        quote_asset,
        quote_amount,
        initial_price,
        price_after_most_recent_bid,
        initial_end_price,
        end_price_after_most_recent_bid,
    } = auction_state;
    [
        (start_block as &dyn Display, "Start Block"),
        (most_recent_bid_block, "Most Recent Bid Block"),
        (duration, "Duration"),
        (base_asset, "Base Asset"),
        (initial_base_amount, "Initial Base Amount"),
        (base_amount_remaining, "Base Amount Remaining"),
        (quote_asset, "Quote Asset"),
        (quote_amount, "Quote Amount"),
        (initial_price, "Initial Price"),
        (price_after_most_recent_bid, "Price after most recent bid"),
        (initial_end_price, "Initial End Price"),
        (
            end_price_after_most_recent_bid,
            "End Price after most recent bid",
        ),
    ]
    .into_iter()
    .map(|(value, descriptor)| show_line(ui, value, descriptor).join())
    .reduce(|resp0, resp1| resp0 | resp1)
    .unwrap()
}

impl DutchAuctionExplorer {
    pub fn show(&mut self, app: &mut App, ui: &mut egui::Ui) {
        egui::CentralPanel::default().show_inside(ui, |ui| {
            ui.heading("Dutch Auction Explorer");
            let text_resp =  ui.horizontal(|ui| {
                ui.monospace("Auction ID:       ")
                | ui.add(egui::TextEdit::singleline(&mut self.auction_id))
            }).join();
            let refresh_button = ui.button("Refresh");
            // resolve auction if changed or refresh button clicked
            if text_resp.changed() || refresh_button.clicked() {
                match borsh_deserialize_hex(&self.auction_id) {
                    Ok(auction_id) => {
                        let last_query_result =
                            app.node.try_get_dutch_auction_state(auction_id);
                        self.last_query_result = Some(LastQueryResult(last_query_result));
                    },
                    Err(err) => {
                        self.err_msg = Some(err);
                    }
                }
            }
            if let Some(LastQueryResult(last_query_result)) = &self.last_query_result {
                match last_query_result {
                    Err(err) => {
                        ui.horizontal(|ui| {
                            ui.monospace(format!("Error encountered when resolving auction: {err}"))
                        });
                    }
                    Ok(None) => {
                        ui.horizontal(|ui| {
                            ui.monospace("No auction data found")
                        });
                    }
                    Ok(Some(auction_state)) => {
                        let _resp: Response = show_dutch_auction_state(ui, auction_state);
                    }
                }
            }
        });
    }
}
