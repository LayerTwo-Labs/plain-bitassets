use crate::types::Txid;
use nonempty::NonEmpty;
use serde::{Deserialize, Serialize};

/// Data of type `T` paired with block height at which it was last updated
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HeightStamped<T> {
    pub value: T,
    pub height: u32,
}

/// Data of type `T` paired with
//  * the txid at which it was last updated
//  * block height at which it was last updated
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TxidStamped<T> {
    pub data: T,
    pub txid: Txid,
    pub height: u32,
}

/// Wrapper struct for fields that support rollbacks
#[derive(Clone, Debug, Deserialize, Serialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct RollBack<T>(pub(in crate::state) NonEmpty<T>);

impl<T> RollBack<HeightStamped<T>> {
    pub(in crate::state) fn new(value: T, height: u32) -> Self {
        let height_stamped = HeightStamped { value, height };
        Self(NonEmpty::new(height_stamped))
    }

    /// Pop the most recent value
    pub(in crate::state) fn pop(mut self) -> (Option<Self>, HeightStamped<T>) {
        if let Some(value) = self.0.pop() {
            (Some(self), value)
        } else {
            (None, self.0.head)
        }
    }

    /// Attempt to push a value as the new most recent.
    /// Returns the value if the operation fails.
    pub(in crate::state) fn push(
        &mut self,
        value: T,
        height: u32,
    ) -> Result<(), T> {
        if self.0.last().height >= height {
            return Err(value);
        }
        let height_stamped = HeightStamped { value, height };
        self.0.push(height_stamped);
        Ok(())
    }

    /// Returns the earliest value
    pub(in crate::state) fn earliest(&self) -> &HeightStamped<T> {
        self.0.first()
    }

    /// Returns the most recent value
    pub fn latest(&self) -> &HeightStamped<T> {
        self.0.last()
    }
}

impl<T> RollBack<TxidStamped<T>> {
    pub(in crate::state) fn new(value: T, txid: Txid, height: u32) -> Self {
        let txid_stamped = TxidStamped {
            data: value,
            txid,
            height,
        };
        Self(NonEmpty::new(txid_stamped))
    }

    /// pop the most recent value
    pub(in crate::state) fn pop(&mut self) -> Option<TxidStamped<T>> {
        self.0.pop()
    }

    /// push a value as the new most recent
    pub(in crate::state) fn push(&mut self, value: T, txid: Txid, height: u32) {
        let txid_stamped = TxidStamped {
            data: value,
            txid,
            height,
        };
        self.0.push(txid_stamped)
    }

    /** Returns the value as it was, at the specified block height.
     *  If a value was updated several times in the block, returns the
     *  last value seen in the block. */
    pub(in crate::state) fn at_block_height(
        &self,
        height: u32,
    ) -> Option<&TxidStamped<T>> {
        self.0
            .iter()
            .rev()
            .find(|txid_stamped| txid_stamped.height <= height)
    }

    /// returns the most recent value, along with it's txid
    pub fn latest(&self) -> &TxidStamped<T> {
        self.0.last()
    }
}
