//! Functions and types related to Dutch Auctions

use heed::types::SerdeBincode;
use serde::{Deserialize, Serialize};
use sneed::{DatabaseUnique, RoDatabaseUnique, RwTxn};

use crate::{
    state::{
        error::dutch_auction::{self as error, Error},
        rollback::{RollBack, TxidStamped},
    },
    types::{
        AssetId, DutchAuctionBid, DutchAuctionCollect, DutchAuctionId,
        DutchAuctionParams, FilledTransaction, Txid,
    },
};

/// Parameters of a Dutch Auction
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DutchAuctionState {
    /// Block height at which the auction starts
    pub start_block: u32,
    /// Block height at the most recent bid
    pub most_recent_bid_block: RollBack<TxidStamped<u32>>,
    /// Auction duration, in blocks
    pub duration: u32,
    /// The asset to be auctioned
    pub base_asset: AssetId,
    /// The initial amount of base asset to be auctioned
    pub initial_base_amount: u64,
    /// The remaining amount of the base asset to be auctioned
    pub base_amount_remaining: RollBack<TxidStamped<u64>>,
    /// The asset in which the auction is to be quoted
    pub quote_asset: AssetId,
    /// The amount of the quote asset that has been received
    pub quote_amount: RollBack<TxidStamped<u64>>,
    /// Initial price
    pub initial_price: u64,
    /// Price immediately after the most recent bid
    pub price_after_most_recent_bid: RollBack<TxidStamped<u64>>,
    /// End price as initially specified
    pub initial_end_price: u64,
    /// End price after the most recent bid
    pub end_price_after_most_recent_bid: RollBack<TxidStamped<u64>>,
}

impl DutchAuctionState {
    /// Returns the new auction state after a bid
    pub fn bid(
        &self,
        txid: Txid,
        bid_amount: u64,
        height: u32,
    ) -> Result<Self, error::Bid> {
        let DutchAuctionState {
            start_block,
            most_recent_bid_block,
            duration,
            base_asset: _,
            initial_base_amount: _,
            base_amount_remaining,
            quote_asset: _,
            quote_amount,
            initial_price: _,
            price_after_most_recent_bid,
            initial_end_price: _,
            end_price_after_most_recent_bid,
        } = self;
        if height < *start_block {
            do yeet error::Bid::AuctionNotStarted
        };
        // Blocks elapsed since last bid
        let elapsed_blocks = height - most_recent_bid_block.latest().data;
        let end_block = start_block.saturating_add(*duration - 1);
        if height > end_block {
            do yeet error::Bid::AuctionEnded
        };
        let remaining_duration_at_most_recent_bid =
            end_block - most_recent_bid_block.latest().data;
        // Calculate current price
        let price = if remaining_duration_at_most_recent_bid == 0 {
            price_after_most_recent_bid.latest().data
        } else {
            let price_decrease: u128 = {
                /* ((price_after_most_recent_bid
                 * - end_price_after_most_recent_bid)
                 * / (remaining_duration_at_most_recent_bid)) * elapsed
                 * == ((price_after_most_recent_bid
                 * - end_price_after_most_recent_bid) * elapsed)
                 * / (remaining_duration_at_most_recent_bid) */
                let max_price_decrease =
                    (price_after_most_recent_bid.latest().data
                        - end_price_after_most_recent_bid.latest().data)
                        as u128;
                (max_price_decrease * elapsed_blocks as u128)
                    / (remaining_duration_at_most_recent_bid as u128)
            };
            // This is safe, as `(elapsed_blocks / remaining_duration_at_most_recent_bid) < 1`
            let price_decrease = {
                assert!(
                    elapsed_blocks / remaining_duration_at_most_recent_bid < 1
                );
                price_decrease as u64
            };
            price_after_most_recent_bid.latest().data - price_decrease
        };
        if price == 0 {
            do yeet error::Bid::InvalidPrice
        };
        // Calculate order quantity for this bid, in terms of the base
        let order_quantity: u128 = {
            /* bid_amount / (price / base_amount_remaining)
             * == (bid_amount * base_amount_remaining) / price */
            (bid_amount as u128 * base_amount_remaining.latest().data as u128)
                .div_ceil(price as u128)
        };
        let order_quantity: u64 =
            if order_quantity <= base_amount_remaining.latest().data as u128 {
                order_quantity as u64
            } else {
                do yeet error::Bid::QuantityTooLarge
            };
        let new_base_amount_remaining =
            base_amount_remaining.latest().data - order_quantity;
        let end_price = {
            /* Truncation to `u64` here is safe as
            `(new_base_amount_remaining / base_amount_remaining) < 1` */
            (end_price_after_most_recent_bid.latest().data as u128
                * new_base_amount_remaining as u128)
                .div_ceil(base_amount_remaining.latest().data as u128)
                as u64
        };
        let mut most_recent_bid_block = most_recent_bid_block.clone();
        most_recent_bid_block.push(height, txid, height);
        let mut base_amount_remaining = base_amount_remaining.clone();
        base_amount_remaining.push(new_base_amount_remaining, txid, height);
        let mut quote_amount = quote_amount.clone();
        quote_amount.push(
            quote_amount.latest().data + bid_amount,
            txid,
            height,
        );
        let mut price_after_most_recent_bid =
            price_after_most_recent_bid.clone();
        price_after_most_recent_bid.push(price - bid_amount, txid, height);
        let mut end_price_after_most_recent_bid =
            end_price_after_most_recent_bid.clone();
        end_price_after_most_recent_bid.push(end_price, txid, height);
        Ok(Self {
            most_recent_bid_block,
            base_amount_remaining,
            quote_amount,
            price_after_most_recent_bid,
            end_price_after_most_recent_bid,
            ..*self
        })
    }

    /// Returns the dutch auction state after reverting a bid
    fn revert_bid(&self, txid: Txid) -> Result<Self, Error> {
        let mut most_recent_bid_block = self.most_recent_bid_block.clone();
        assert!(
            most_recent_bid_block
                .pop()
                .is_some_and(|ts| ts.txid == txid)
        );
        let mut base_amount_remaining = self.base_amount_remaining.clone();
        assert!(
            base_amount_remaining
                .pop()
                .is_some_and(|ts| ts.txid == txid)
        );
        let mut quote_amount = self.quote_amount.clone();
        assert!(quote_amount.pop().is_some_and(|ts| ts.txid == txid));
        let mut price_after_most_recent_bid =
            self.price_after_most_recent_bid.clone();
        assert!(
            price_after_most_recent_bid
                .pop()
                .is_some_and(|ts| ts.txid == txid)
        );
        let mut end_price_after_most_recent_bid =
            self.end_price_after_most_recent_bid.clone();
        assert!(
            end_price_after_most_recent_bid
                .pop()
                .is_some_and(|ts| ts.txid == txid)
        );
        Ok(Self {
            most_recent_bid_block,
            base_amount_remaining,
            quote_amount,
            price_after_most_recent_bid,
            end_price_after_most_recent_bid,
            ..*self
        })
    }
}

/// Associates Dutch auction sequence numbers with auction state
pub type Db = DatabaseUnique<
    SerdeBincode<DutchAuctionId>,
    SerdeBincode<DutchAuctionState>,
>;
/// Associates Dutch auction sequence numbers with auction state
pub type RoDb = RoDatabaseUnique<
    SerdeBincode<DutchAuctionId>,
    SerdeBincode<DutchAuctionState>,
>;

// Apply Dutch auction bid
pub(in crate::state) fn apply_bid(
    db: &Db,
    rwtxn: &mut RwTxn,
    filled_tx: &FilledTransaction,
    height: u32,
) -> Result<(), Error> {
    let DutchAuctionBid {
        auction_id,
        asset_spend,
        asset_receive,
        amount_spend,
        amount_receive,
    } = filled_tx
        .dutch_auction_bid()
        .ok_or(error::Bid::InvalidTxData)?;
    let dutch_auction_state = db
        .try_get(rwtxn, &auction_id)?
        .ok_or(error::Bid::MissingAuction)?;
    if asset_receive != dutch_auction_state.base_asset {
        do yeet error::Bid::IncorrectReceiveAsset
    }
    if asset_spend != dutch_auction_state.quote_asset {
        do yeet error::Bid::IncorrectSpendAsset
    }
    if amount_receive > dutch_auction_state.base_amount_remaining.latest().data
    {
        do yeet error::Bid::QuantityTooLarge
    };
    let new_dutch_auction_state =
        dutch_auction_state.bid(filled_tx.txid(), amount_spend, height)?;
    let order_quantity =
        dutch_auction_state.base_amount_remaining.latest().data
            - new_dutch_auction_state.base_amount_remaining.latest().data;
    if amount_receive != order_quantity {
        do yeet error::Bid::InvalidPrice
    };
    db.put(rwtxn, &auction_id, &new_dutch_auction_state)?;
    Ok(())
}

// Revert Dutch auction bid
pub(in crate::state) fn revert_bid(
    db: &Db,
    rwtxn: &mut RwTxn,
    filled_tx: &FilledTransaction,
) -> Result<(), Error> {
    let DutchAuctionBid {
        auction_id,
        asset_spend,
        asset_receive,
        amount_spend: _,
        amount_receive: _,
    } = filled_tx
        .dutch_auction_bid()
        .ok_or(error::Bid::InvalidTxData)?;
    let dutch_auction_state = db
        .try_get(rwtxn, &auction_id)?
        .ok_or(error::Bid::MissingAuction)?;
    if asset_receive != dutch_auction_state.base_asset {
        do yeet error::Bid::IncorrectReceiveAsset
    }
    if asset_spend != dutch_auction_state.quote_asset {
        do yeet error::Bid::IncorrectSpendAsset
    }
    let new_dutch_auction_state =
        dutch_auction_state.revert_bid(filled_tx.txid())?;
    db.put(rwtxn, &auction_id, &new_dutch_auction_state)?;
    Ok(())
}

// Apply Dutch auction create
pub(in crate::state) fn apply_create(
    db: &Db,
    rwtxn: &mut RwTxn,
    filled_tx: &FilledTransaction,
    dutch_auction_params: DutchAuctionParams,
    height: u32,
) -> Result<(), Error> {
    let DutchAuctionParams {
        start_block,
        duration,
        base_asset,
        base_amount,
        quote_asset,
        initial_price,
        final_price,
    } = dutch_auction_params;
    if height >= start_block {
        do yeet error::Create::Expired;
    };
    if final_price > initial_price {
        do yeet error::Create::FinalPrice;
    };
    match duration {
        0 => do yeet error::Create::ZeroDuration,
        1 => {
            if final_price != initial_price {
                do yeet error::Create::PriceMismatch
            }
        }
        _ => (),
    };
    let txid = filled_tx.txid();
    let dutch_auction_id = DutchAuctionId(txid);
    let dutch_auction_state = DutchAuctionState {
        start_block,
        most_recent_bid_block: RollBack::<TxidStamped<_>>::new(
            start_block,
            txid,
            height,
        ),
        duration,
        base_asset,
        initial_base_amount: base_amount,
        base_amount_remaining: RollBack::<TxidStamped<_>>::new(
            base_amount,
            txid,
            height,
        ),
        quote_asset,
        quote_amount: RollBack::<TxidStamped<_>>::new(0, txid, height),
        initial_price,
        price_after_most_recent_bid: RollBack::<TxidStamped<_>>::new(
            initial_price,
            txid,
            height,
        ),
        initial_end_price: final_price,
        end_price_after_most_recent_bid: RollBack::<TxidStamped<_>>::new(
            final_price,
            txid,
            height,
        ),
    };
    db.put(rwtxn, &dutch_auction_id, &dutch_auction_state)?;
    Ok(())
}

// Revert Dutch auction create
pub(in crate::state) fn revert_create(
    db: &Db,
    rwtxn: &mut RwTxn,
    filled_tx: &FilledTransaction,
) -> Result<(), Error> {
    let dutch_auction_id = DutchAuctionId(filled_tx.txid());
    if !db.delete(rwtxn, &dutch_auction_id)? {
        return Err(Error::Missing(dutch_auction_id));
    };
    Ok(())
}

// Apply Dutch auction collect
pub(in crate::state) fn apply_collect(
    db: &Db,
    rwtxn: &mut RwTxn,
    filled_tx: &FilledTransaction,
    height: u32,
) -> Result<(), Error> {
    let DutchAuctionCollect {
        auction_id,
        asset_offered,
        asset_receive,
        amount_offered_remaining,
        amount_received,
    } = filled_tx
        .dutch_auction_collect()
        .ok_or(error::Collect::InvalidTxData)?;
    let mut auction_state = db
        .try_get(rwtxn, &auction_id)?
        .ok_or(error::Collect::MissingAuction)?;
    if auction_state.base_asset != asset_offered {
        do yeet error::Collect::IncorrectOfferedAsset
    }
    if auction_state.quote_asset != asset_receive {
        do yeet error::Collect::IncorrectReceiveAsset
    }
    if height
        < auction_state
            .start_block
            .saturating_add(auction_state.duration)
    {
        do yeet error::Collect::AuctionNotFinished
    }
    if amount_offered_remaining
        != auction_state.base_amount_remaining.latest().data
    {
        do yeet error::Collect::IncorrectOfferedAssetAmount
    }
    if amount_received != auction_state.quote_amount.latest().data {
        do yeet error::Collect::IncorrectReceiveAssetAmount
    }
    let txid = filled_tx.txid();
    auction_state.base_amount_remaining.push(0, txid, height);
    auction_state.quote_amount.push(0, txid, height);
    db.put(rwtxn, &auction_id, &auction_state)?;
    Ok(())
}

// Revert Dutch auction collect
pub(in crate::state) fn revert_collect(
    db: &Db,
    rwtxn: &mut RwTxn,
    filled_tx: &FilledTransaction,
) -> Result<(), Error> {
    let DutchAuctionCollect {
        auction_id,
        asset_offered: _,
        asset_receive: _,
        amount_offered_remaining,
        amount_received,
    } = filled_tx
        .dutch_auction_collect()
        .ok_or(error::Collect::InvalidTxData)?;
    let txid = filled_tx.txid();
    let mut auction_state = db
        .try_get(rwtxn, &auction_id)?
        .ok_or(error::Collect::Revert)?;
    assert!(
        auction_state
            .base_amount_remaining
            .pop()
            .is_some_and(|ts| { ts.txid == txid && ts.data == 0 })
    );
    assert_eq!(
        auction_state.base_amount_remaining.latest().data,
        amount_offered_remaining
    );
    assert!(
        auction_state
            .quote_amount
            .pop()
            .is_some_and(|ts| { ts.txid == txid && ts.data == 0 })
    );
    assert_eq!(auction_state.quote_amount.latest().data, amount_received);
    db.put(rwtxn, &auction_id, &auction_state)?;
    Ok(())
}
