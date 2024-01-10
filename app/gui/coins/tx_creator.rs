use std::{
    borrow::Cow,
    fmt::Display,
    net::{Ipv4Addr, Ipv6Addr},
    str::FromStr,
};

use eframe::egui::{self, InnerResponse, Response, TextBuffer};
use hex::FromHex;

use plain_bitassets::{
    authorization::PublicKey,
    bip300301::bitcoin,
    state::AmmPair,
    types::{
        AssetId, BitAssetData, DutchAuctionId, EncryptionPubKey, Hash,
        Transaction, Txid,
    },
};

use crate::{
    app::App,
    gui::util::{borsh_deserialize_hex, InnerResponseExt},
};

// struct representing the outcome of trying to set an Option<T> from a String
// Err represents unset, Ok(None) represents bad value
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrySetOption<T>(Result<Option<T>, String>);

// try to set BitAsset Data
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TrySetBitAssetData {
    /// commitment to arbitrary data
    pub commitment: TrySetOption<Hash>,
    /// optional ipv4 addr
    pub ipv4_addr: TrySetOption<Ipv4Addr>,
    /// optional ipv6 addr
    pub ipv6_addr: TrySetOption<Ipv6Addr>,
    /// optional pubkey used for encryption
    pub encryption_pubkey: TrySetOption<EncryptionPubKey>,
    /// optional pubkey used for signing messages
    pub signing_pubkey: TrySetOption<PublicKey>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DutchAuctionParams {
    /// Block height at which the auction starts
    start_block: String,
    /// Auction duration, in blocks
    duration: String,
    /// The asset to be auctioned
    base_asset: String,
    /// The amount of the base asset to be auctioned
    base_amount: String,
    /// The asset in which the auction is to be quoted
    quote_asset: String,
    /// Initial price
    initial_price: String,
    /// Final price
    final_price: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BitAssetRegistration {
    plaintext_name: String,
    bitasset_data: Box<TrySetBitAssetData>,
    initial_supply: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DexBurn {
    asset0: String,
    asset1: String,
    amount_lp_tokens: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DexMint {
    asset0: String,
    asset1: String,
    amount0: String,
    amount1: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DexSwap {
    asset_spend: String,
    asset_receive: String,
    amount_spend: String,
    amount_receive: String,
}

#[derive(
    Clone, Debug, Default, strum::Display, strum::EnumIter, Eq, PartialEq,
)]
pub enum TxType {
    #[default]
    Regular,
    #[strum(to_string = "Register BitAsset")]
    BitAssetRegistration(BitAssetRegistration),
    #[strum(to_string = "Reserve BitAsset")]
    BitAssetReservation { plaintext_name: String },
    #[strum(to_string = "DEX (Burn Position)")]
    DexBurn(DexBurn),
    #[strum(to_string = "DEX (Mint Position)")]
    DexMint(DexMint),
    #[strum(to_string = "DEX (Swap)")]
    DexSwap(DexSwap),
    #[strum(to_string = "Dutch Auction (Bid)")]
    DutchAuctionBid {
        auction_id: String,
        bid_size: String,
    },
    #[strum(to_string = "Dutch Auction (Collect)")]
    DutchAuctionCollect { auction_id: String },
    #[strum(to_string = "Dutch Auction (Create)")]
    DutchAuctionCreate { auction_params: DutchAuctionParams },
}

#[derive(Debug, Default)]
pub struct TxCreator {
    pub bitcoin_value_in: u64,
    pub bitcoin_value_out: u64,
    pub tx_type: TxType,
    // if the base tx has changed, need to recompute final tx
    base_txid: Txid,
    final_tx: Option<anyhow::Result<Transaction>>,
}

impl<T> std::default::Default for TrySetOption<T> {
    fn default() -> Self {
        Self(Ok(None))
    }
}

impl TryFrom<TrySetBitAssetData> for BitAssetData {
    type Error = String;

    fn try_from(try_set: TrySetBitAssetData) -> Result<Self, Self::Error> {
        fn parse_err_msg<E: Display>(
            item_name: &str,
        ) -> impl Fn(E) -> String + '_ {
            move |err| format!("Cannot parse {item_name}: \"{err}\"")
        }
        let TrySetBitAssetData {
            commitment,
            ipv4_addr,
            ipv6_addr,
            encryption_pubkey,
            signing_pubkey,
        } = try_set;
        let commitment = commitment.0.map_err(parse_err_msg("commitment"))?;
        let ipv4_addr = ipv4_addr.0.map_err(parse_err_msg("ipv4 address"))?;
        let ipv6_addr = ipv6_addr.0.map_err(parse_err_msg("ipv6 address"))?;
        let encryption_pubkey = encryption_pubkey
            .0
            .map_err(parse_err_msg("encryption pubkey"))?;
        let signing_pubkey =
            signing_pubkey.0.map_err(parse_err_msg("signing pubkey"))?;
        Ok(BitAssetData {
            commitment,
            ipv4_addr,
            ipv6_addr,
            encryption_pubkey,
            signing_pubkey,
        })
    }
}

fn show_monospace_single_line_input(
    ui: &mut egui::Ui,
    text_buffer: &mut dyn TextBuffer,
    descriptor: &str,
) -> InnerResponse<Response> {
    ui.horizontal(|ui| {
        ui.monospace(format!("{descriptor}:       "))
            | ui.add(egui::TextEdit::singleline(text_buffer))
    })
}

fn show_monospace_single_line_inputs<'iter, I>(
    ui: &mut egui::Ui,
    iter: I,
) -> Option<Response>
where
    I: IntoIterator<Item = (&'iter mut dyn TextBuffer, &'iter str)>,
{
    iter.into_iter()
        .map(|(text_buffer, descriptor)| {
            show_monospace_single_line_input(ui, text_buffer, descriptor).join()
        })
        .reduce(|resp0, resp1| resp0 | resp1)
}

impl TxCreator {
    fn set_bitasset_registration(
        app: &App,
        mut tx: Transaction,
        bitasset_registration: &BitAssetRegistration,
    ) -> anyhow::Result<Transaction> {
        let bitasset_data: BitAssetData =
            (bitasset_registration.bitasset_data.as_ref())
                .clone()
                .try_into()
                .map_err(|err| anyhow::anyhow!("{err}"))?;
        let initial_supply =
            u64::from_str(&bitasset_registration.initial_supply).map_err(
                |err| anyhow::anyhow!("Failed to parse initial supply: {err}"),
            )?;
        let () = app.wallet.register_bitasset(
            &mut tx,
            &bitasset_registration.plaintext_name,
            Cow::Borrowed(&bitasset_data),
            initial_supply,
        )?;
        Ok(tx)
    }

    fn set_dex_burn(
        app: &App,
        mut tx: Transaction,
        dex_burn: &DexBurn,
    ) -> anyhow::Result<Transaction> {
        let asset0: AssetId = borsh_deserialize_hex(&dex_burn.asset0)
            .map_err(|err| anyhow::anyhow!("Failed to parse asset 0: {err}"))?;
        let asset1: AssetId = borsh_deserialize_hex(&dex_burn.asset1)
            .map_err(|err| anyhow::anyhow!("Failed to parse asset 1: {err}"))?;
        let amount_lp_tokens = u64::from_str(&dex_burn.amount_lp_tokens)
            .map_err(|err| {
                anyhow::anyhow!("Failed to parse LP token amount: {err}")
            })?;
        let amm_pair = AmmPair::new(asset0, asset1);
        let (amount0, amount1);
        {
            let amm_pool_state = app
                .node
                .get_amm_pool_state(amm_pair)
                .map_err(anyhow::Error::new)?;
            let next_amm_pool_state = amm_pool_state
                .burn(amount_lp_tokens)
                .map_err(anyhow::Error::new)?;
            amount0 = amm_pool_state.reserve0 - next_amm_pool_state.reserve0;
            amount1 = amm_pool_state.reserve1 - next_amm_pool_state.reserve1;
        };
        let () = app.wallet.amm_burn(
            &mut tx,
            amm_pair.asset0(),
            amm_pair.asset1(),
            amount0,
            amount1,
            amount_lp_tokens,
        )?;
        Ok(tx)
    }

    fn set_dex_mint(
        app: &App,
        mut tx: Transaction,
        dex_mint: &DexMint,
    ) -> anyhow::Result<Transaction> {
        let asset0: AssetId = borsh_deserialize_hex(&dex_mint.asset0)
            .map_err(|err| anyhow::anyhow!("Failed to parse asset 0: {err}"))?;
        let asset1: AssetId = borsh_deserialize_hex(&dex_mint.asset1)
            .map_err(|err| anyhow::anyhow!("Failed to parse asset 1: {err}"))?;
        let amount0 = u64::from_str(&dex_mint.amount0).map_err(|err| {
            anyhow::anyhow!("Failed to parse amount (asset 0): {err}")
        })?;
        let amount1 = u64::from_str(&dex_mint.amount1).map_err(|err| {
            anyhow::anyhow!("Failed to parse amount (asset 1): {err}")
        })?;
        let lp_token_mint = {
            let amm_pair = AmmPair::new(asset0, asset1);
            let amm_pool_state = app
                .node
                .get_amm_pool_state(amm_pair)
                .map_err(anyhow::Error::new)?;
            let next_amm_pool_state = amm_pool_state
                .mint(amount0, amount1)
                .map_err(anyhow::Error::new)?;
            next_amm_pool_state.outstanding_lp_tokens
                - amm_pool_state.outstanding_lp_tokens
        };
        let () = app.wallet.amm_mint(
            &mut tx,
            asset0,
            asset1,
            amount0,
            amount1,
            lp_token_mint,
        )?;
        Ok(tx)
    }

    fn set_dex_swap(
        app: &App,
        mut tx: Transaction,
        dex_swap: &DexSwap,
    ) -> anyhow::Result<Transaction> {
        let asset_spend: AssetId = borsh_deserialize_hex(&dex_swap.asset_spend)
            .map_err(|err| {
                anyhow::anyhow!("Failed to parse spend asset: {err}")
            })?;
        let asset_receive: AssetId =
            borsh_deserialize_hex(&dex_swap.asset_receive).map_err(|err| {
                anyhow::anyhow!("Failed to parse receive asset: {err}")
            })?;
        let amount_spend =
            u64::from_str(&dex_swap.amount_spend).map_err(|err| {
                anyhow::anyhow!("Failed to parse spend amount: {err}")
            })?;
        let amount_receive =
            u64::from_str(&dex_swap.amount_receive).map_err(|err| {
                anyhow::anyhow!("Failed to parse receive amount: {err}")
            })?;
        let () = app.wallet.amm_swap(
            &mut tx,
            asset_spend,
            asset_receive,
            amount_spend,
            amount_receive,
        )?;
        Ok(tx)
    }

    fn set_dutch_auction_bid(
        app: &App,
        mut tx: Transaction,
        auction_id: &str,
        bid_size: &str,
    ) -> anyhow::Result<Transaction> {
        let auction_id: DutchAuctionId = borsh_deserialize_hex(auction_id)
            .map_err(|err| {
                anyhow::anyhow!("Failed to parse auction ID: {err}")
            })?;
        let bid_size = u64::from_str(bid_size).map_err(|err| {
            anyhow::anyhow!("Failed to parse bid size: {err}")
        })?;
        let height = app.node.get_height().unwrap_or(0);
        let auction_state = app
            .node
            .get_dutch_auction_state(auction_id)
            .map_err(anyhow::Error::new)?;
        let next_auction_state = auction_state
            .bid(bid_size, height)
            .map_err(anyhow::Error::new)?;
        let receive_quantity =
            auction_state.base_amount - next_auction_state.base_amount;
        let () = app.wallet.dutch_auction_bid(
            &mut tx,
            auction_id,
            auction_state.base_asset,
            auction_state.quote_asset,
            bid_size,
            receive_quantity,
        )?;
        Ok(tx)
    }

    fn set_dutch_auction_collect(
        app: &App,
        mut tx: Transaction,
        auction_id: &str,
    ) -> anyhow::Result<Transaction> {
        let auction_id: DutchAuctionId = borsh_deserialize_hex(auction_id)
            .map_err(|err| {
                anyhow::anyhow!("Failed to parse auction ID: {err}")
            })?;
        let auction_state = app
            .node
            .get_dutch_auction_state(auction_id)
            .map_err(anyhow::Error::new)?;
        let () = app.wallet.dutch_auction_collect(
            &mut tx,
            auction_id,
            auction_state.base_asset,
            auction_state.quote_asset,
            auction_state.base_amount,
            auction_state.quote_amount,
        )?;
        Ok(tx)
    }

    fn set_dutch_auction_create(
        app: &App,
        mut tx: Transaction,
        auction_params: &DutchAuctionParams,
    ) -> anyhow::Result<Transaction> {
        let start_block =
            u32::from_str(&auction_params.start_block).map_err(|err| {
                anyhow::anyhow!("Failed to parse start block: {err}")
            })?;
        let duration =
            u32::from_str(&auction_params.duration).map_err(|err| {
                anyhow::anyhow!("Failed to parse duration: {err}")
            })?;
        let base_asset: AssetId = borsh_deserialize_hex(
            &auction_params.base_asset,
        )
        .map_err(|err| anyhow::anyhow!("Failed to parse base asset: {err}"))?;
        let base_amount =
            u64::from_str(&auction_params.base_amount).map_err(|err| {
                anyhow::anyhow!("Failed to parse base amount: {err}")
            })?;
        let quote_asset: AssetId = borsh_deserialize_hex(
            &auction_params.quote_asset,
        )
        .map_err(|err| anyhow::anyhow!("Failed to parse quote asset: {err}"))?;
        let initial_price = u64::from_str(&auction_params.initial_price)
            .map_err(|err| {
                anyhow::anyhow!("Failed to parse initial price: {err}")
            })?;
        let final_price =
            u64::from_str(&auction_params.final_price).map_err(|err| {
                anyhow::anyhow!("Failed to parse final price: {err}")
            })?;
        let dutch_auction_params = plain_bitassets::types::DutchAuctionParams {
            start_block,
            duration,
            base_asset,
            base_amount,
            quote_asset,
            initial_price,
            final_price,
        };
        let () = app
            .wallet
            .dutch_auction_create(&mut tx, dutch_auction_params)?;
        Ok(tx)
    }

    // set tx data for the current transaction
    fn set_tx_data(
        &self,
        app: &mut App,
        mut tx: Transaction,
    ) -> anyhow::Result<Transaction> {
        match &self.tx_type {
            TxType::Regular => Ok(tx),
            TxType::BitAssetRegistration(bitasset_registration) => {
                Self::set_bitasset_registration(app, tx, bitasset_registration)
            }
            TxType::BitAssetReservation { plaintext_name } => {
                let () =
                    app.wallet.reserve_bitasset(&mut tx, plaintext_name)?;
                Ok(tx)
            }
            TxType::DexBurn(dex_burn) => Self::set_dex_burn(app, tx, dex_burn),
            TxType::DexMint(dex_mint) => Self::set_dex_mint(app, tx, dex_mint),
            TxType::DexSwap(dex_swap) => Self::set_dex_swap(app, tx, dex_swap),
            TxType::DutchAuctionBid {
                auction_id,
                bid_size,
            } => Self::set_dutch_auction_bid(app, tx, auction_id, bid_size),
            TxType::DutchAuctionCollect { auction_id } => {
                Self::set_dutch_auction_collect(app, tx, auction_id)
            }
            TxType::DutchAuctionCreate { auction_params } => {
                Self::set_dutch_auction_create(app, tx, auction_params)
            }
        }
    }

    // show setter for a single optional field, with default value
    fn show_option_field_default<T, ToStr, TryFromStr, TryFromStrErr>(
        ui: &mut egui::Ui,
        name: &str,
        default: T,
        try_set: &mut TrySetOption<T>,
        try_from_str: TryFromStr,
        to_str: ToStr,
    ) -> Response
    where
        T: PartialEq,
        TryFromStr: Fn(String) -> Result<T, TryFromStrErr>,
        TryFromStrErr: std::error::Error,
        ToStr: Fn(&T) -> String,
    {
        let option_dropdown = egui::ComboBox::from_id_source(name)
            .selected_text(if let Ok(None) = try_set.0 {
                "do not set"
            } else {
                "set"
            })
            .show_ui(ui, |ui| {
                ui.selectable_value(
                    try_set,
                    TrySetOption(Ok(Some(default))),
                    "set",
                ) | ui.selectable_value(
                    try_set,
                    TrySetOption(Ok(None)),
                    "do not set",
                )
            });
        match try_set.0 {
            Ok(None) => option_dropdown.join(),
            Err(ref mut bad_value) => {
                let text_edit = ui.add(egui::TextEdit::singleline(bad_value));
                if text_edit.changed() {
                    if let Ok(value) = try_from_str(bad_value.clone()) {
                        try_set.0 = Ok(Some(value));
                    }
                }
                option_dropdown.join() | text_edit
            }
            Ok(Some(ref mut value)) => {
                let mut text_buffer = to_str(value);
                let text_edit =
                    ui.add(egui::TextEdit::singleline(&mut text_buffer));
                if text_edit.changed() {
                    match try_from_str(text_buffer.clone()) {
                        Ok(new_value) => {
                            *value = new_value;
                        }
                        Err(_) => {
                            try_set.0 = Err(text_buffer);
                        }
                    }
                }
                option_dropdown.join() | text_edit
            }
        }
    }

    fn show_bitasset_options(
        ui: &mut egui::Ui,
        bitasset_data: &mut TrySetBitAssetData,
    ) -> Response {
        let commitment_resp = ui.horizontal(|ui| {
            ui.monospace("Commitment:       ")
                | Self::show_option_field_default(
                    ui,
                    "bitasset_data_commitment",
                    Default::default(),
                    &mut bitasset_data.commitment,
                    Hash::from_hex,
                    |commitment| hex::encode(commitment),
                )
        });
        let ipv4_resp = ui.horizontal(|ui| {
            ui.monospace("IPv4 Address:       ")
                | Self::show_option_field_default(
                    ui,
                    "bitasset_data_ipv4",
                    Ipv4Addr::UNSPECIFIED,
                    &mut bitasset_data.ipv4_addr,
                    |s| Ipv4Addr::from_str(&s),
                    Ipv4Addr::to_string,
                )
        });
        let ipv6_resp = ui.horizontal(|ui| {
            ui.monospace("IPv6 Address:       ")
                | Self::show_option_field_default(
                    ui,
                    "bitasset_data_ipv6",
                    Ipv6Addr::UNSPECIFIED,
                    &mut bitasset_data.ipv6_addr,
                    |s| Ipv6Addr::from_str(&s),
                    Ipv6Addr::to_string,
                )
        });
        let encryption_pubkey_resp = ui.horizontal(|ui| {
            let default_pubkey =
                EncryptionPubKey::from(<[u8; 32] as Default>::default());
            ui.monospace("Encryption PubKey:       ")
                | Self::show_option_field_default(
                    ui,
                    "bitasset_data_encryption_pubkey",
                    default_pubkey,
                    &mut bitasset_data.encryption_pubkey,
                    |s| <[u8; 32]>::from_hex(s).map(EncryptionPubKey::from),
                    |epk| hex::encode(epk.0.as_bytes()),
                )
        });
        let signing_pubkey_resp = ui.horizontal(|ui| {
            let default_pubkey =
                PublicKey::from_bytes(&<[u8; 32] as Default>::default())
                    .unwrap();
            let try_from_str = |s: String| {
                <[u8; 32]>::from_hex(s).map_err(either::Left).and_then(
                    |bytes| {
                        PublicKey::from_bytes(&bytes).map_err(either::Right)
                    },
                )
            };
            ui.monospace("Signing PubKey:       ")
                | Self::show_option_field_default(
                    ui,
                    "bitasset_data_signing_pubkey",
                    default_pubkey,
                    &mut bitasset_data.signing_pubkey,
                    try_from_str,
                    |pk| hex::encode(pk.to_bytes()),
                )
        });
        commitment_resp.join()
            | ipv4_resp.join()
            | ipv6_resp.join()
            | encryption_pubkey_resp.join()
            | signing_pubkey_resp.join()
    }

    fn show_bitasset_registration(
        ui: &mut egui::Ui,
        bitasset_registration: &mut BitAssetRegistration,
    ) -> Option<Response> {
        let plaintext_name_resp = show_monospace_single_line_input(
            ui,
            &mut bitasset_registration.plaintext_name,
            "Plaintext Name",
        );
        let bitasset_options_resp = Self::show_bitasset_options(
            ui,
            bitasset_registration.bitasset_data.as_mut(),
        );
        let initial_supply_resp = show_monospace_single_line_input(
            ui,
            &mut bitasset_registration.initial_supply,
            "Initial Supply",
        );
        let resp = plaintext_name_resp.join()
            | bitasset_options_resp
            | initial_supply_resp.join();
        Some(resp)
    }

    fn show_bitasset_reservation(
        ui: &mut egui::Ui,
        plaintext_name: &mut dyn TextBuffer,
    ) -> Option<Response> {
        let inner_resp = show_monospace_single_line_input(
            ui,
            plaintext_name,
            "Plaintext Name",
        );
        Some(inner_resp.join())
    }

    fn show_dex_burn(
        ui: &mut egui::Ui,
        dex_burn: &mut DexBurn,
    ) -> Option<Response> {
        show_monospace_single_line_inputs(
            ui,
            [
                (&mut dex_burn.asset0 as &mut dyn TextBuffer, "Asset 0"),
                (&mut dex_burn.asset1, "Asset 1"),
                (&mut dex_burn.amount_lp_tokens, "LP Token Amount"),
            ],
        )
    }

    fn show_dex_mint(
        ui: &mut egui::Ui,
        dex_mint: &mut DexMint,
    ) -> Option<Response> {
        show_monospace_single_line_inputs(
            ui,
            [
                (&mut dex_mint.asset0 as &mut dyn TextBuffer, "Asset 0"),
                (&mut dex_mint.asset1, "Asset 1"),
                (&mut dex_mint.amount0, "Amount (Asset 0)"),
                (&mut dex_mint.amount1, "Amount (Asset 1)"),
            ],
        )
    }

    fn show_dex_swap(
        ui: &mut egui::Ui,
        dex_swap: &mut DexSwap,
    ) -> Option<Response> {
        show_monospace_single_line_inputs(
            ui,
            [
                (
                    &mut dex_swap.asset_spend as &mut dyn TextBuffer,
                    "Spend Asset",
                ),
                (&mut dex_swap.asset_receive, "Receive Asset"),
                (&mut dex_swap.amount_spend, "Spend Amount"),
                (&mut dex_swap.amount_receive, "Receive Amount"),
            ],
        )
    }

    fn show_dutch_auction_bid<'a>(
        ui: &mut egui::Ui,
        auction_id: &'a mut dyn TextBuffer,
        bid_size: &'a mut dyn TextBuffer,
    ) -> Option<Response> {
        show_monospace_single_line_inputs(
            ui,
            [(auction_id, "Auction ID"), (bid_size, "Bid Size")],
        )
    }

    fn show_dutch_auction_collect(
        ui: &mut egui::Ui,
        auction_id: &mut dyn TextBuffer,
    ) -> Option<Response> {
        let auction_id_resp =
            show_monospace_single_line_input(ui, auction_id, "Auction ID");
        let resp = auction_id_resp.join();
        Some(resp)
    }

    fn show_dutch_auction_create(
        ui: &mut egui::Ui,
        auction_params: &mut DutchAuctionParams,
    ) -> Option<Response> {
        show_monospace_single_line_inputs(
            ui,
            [
                (
                    &mut auction_params.start_block as &mut dyn TextBuffer,
                    "Start Block",
                ),
                (&mut auction_params.duration, "Duration"),
                (&mut auction_params.base_asset, "Base Asset"),
                (&mut auction_params.base_amount, "Base Amount"),
                (&mut auction_params.quote_asset, "Quote Asset"),
                (&mut auction_params.initial_price, "Initial Price"),
                (&mut auction_params.final_price, "Final Price"),
            ],
        )
    }

    pub fn show(
        &mut self,
        app: &mut App,
        ui: &mut egui::Ui,
        base_tx: &mut Transaction,
    ) -> anyhow::Result<()> {
        let tx_type_dropdown = ui.horizontal(|ui| {
            let combobox = egui::ComboBox::from_id_source("tx_type")
                .selected_text(format!("{}", self.tx_type))
                .show_ui(ui, |ui| {
                    use strum::IntoEnumIterator;
                    TxType::iter()
                        .map(|tx_type| {
                            let text = tx_type.to_string();
                            ui.selectable_value(
                                &mut self.tx_type,
                                tx_type,
                                text,
                            )
                        })
                        .reduce(|resp0, resp1| resp0 | resp1)
                        .unwrap()
                });
            combobox.join() | ui.heading("Transaction")
        });
        let tx_data_ui = match &mut self.tx_type {
            TxType::Regular => None,
            TxType::BitAssetRegistration(bitasset_registration) => {
                Self::show_bitasset_registration(ui, bitasset_registration)
            }
            TxType::BitAssetReservation { plaintext_name } => {
                Self::show_bitasset_reservation(ui, plaintext_name)
            }
            TxType::DexBurn(dex_burn) => Self::show_dex_burn(ui, dex_burn),
            TxType::DexMint(dex_mint) => Self::show_dex_mint(ui, dex_mint),
            TxType::DexSwap(dex_swap) => Self::show_dex_swap(ui, dex_swap),
            TxType::DutchAuctionBid {
                auction_id,
                bid_size,
            } => Self::show_dutch_auction_bid(ui, auction_id, bid_size),
            TxType::DutchAuctionCollect { auction_id } => {
                Self::show_dutch_auction_collect(ui, auction_id)
            }
            TxType::DutchAuctionCreate { auction_params } => {
                Self::show_dutch_auction_create(ui, auction_params)
            }
        };
        let tx_data_changed = tx_data_ui.is_some_and(|resp| resp.changed());
        // if base txid has changed, store the new txid
        let base_txid = base_tx.txid();
        let base_txid_changed = base_txid != self.base_txid;
        if base_txid_changed {
            self.base_txid = base_txid;
        }
        // (re)compute final tx if:
        // * the tx type, tx data, or base txid has changed
        // * final tx not yet set
        let refresh_final_tx = tx_type_dropdown.join().changed()
            || tx_data_changed
            || base_txid_changed
            || self.final_tx.is_none();
        if refresh_final_tx {
            self.final_tx = Some(self.set_tx_data(app, base_tx.clone()));
        }
        let final_tx = match &self.final_tx {
            None => panic!("impossible! final tx should have been set"),
            Some(Ok(final_tx)) => final_tx,
            Some(Err(wallet_err)) => {
                ui.monospace(format!("{wallet_err}"));
                return Ok(());
            }
        };
        let txid = &format!("{}", final_tx.txid())[0..8];
        ui.monospace(format!("txid: {txid}"));
        if self.bitcoin_value_in >= self.bitcoin_value_out {
            let fee = self.bitcoin_value_in - self.bitcoin_value_out;
            let fee = bitcoin::Amount::from_sat(fee);
            ui.monospace(format!("fee:  {fee}"));
            if ui.button("sign and send").clicked() {
                let () = app.sign_and_send(final_tx.clone())?;
                *base_tx = Transaction::default();
                self.final_tx = None;
            }
        } else {
            ui.label("Not Enough Value In");
        }
        Ok(())
    }
}
