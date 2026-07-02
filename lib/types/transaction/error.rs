pub mod bitcoin_fee {
    use thiserror::Error;

    use crate::types::AmountOverflowError;

    #[derive(Debug, Error)]
    pub enum Inner {
        #[error("underfunded; value in ({value_in}) < value out ({value_out})")]
        Underfunded {
            value_in: bitcoin::Amount,
            value_out: bitcoin::Amount,
        },
        #[error("value in overflow")]
        ValueInOverflow(#[source] AmountOverflowError),
        #[error("value out overflow")]
        ValueOutOverflow(#[source] AmountOverflowError),
    }

    #[derive(Debug, Error)]
    #[error("failed to determine bitcoin fee")]
    #[repr(transparent)]
    pub struct Error(#[from] pub Inner);
}
pub use bitcoin_fee::Error as BitcoinFee;
