use heed::types::SerdeBincode;
use serde::{Deserialize, Serialize};
use sneed::{DatabaseUnique, RoDatabaseUnique, RwTxn};
use utoipa::ToSchema;

use crate::{
    state::error::Amm as Error,
    types::{AmmBurn, AmmMint, AmmSwap, AssetId, FilledTransaction, Txid},
};

/// Ordered pair of [`AssetId`]s
#[derive(Clone, Copy, Debug, Serialize)]
pub struct AmmPair(AssetId, AssetId);

impl AmmPair {
    pub fn new(asset0: AssetId, asset1: AssetId) -> Self {
        if asset0 <= asset1 {
            Self(asset0, asset1)
        } else {
            Self(asset1, asset0)
        }
    }

    /// Returns the lower [`AssetId`]
    pub fn asset0(&self) -> AssetId {
        self.0
    }

    /// Returns the greater [`AssetId`]
    pub fn asset1(&self) -> AssetId {
        self.1
    }
}

/// Current state of an AMM pool
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema,
)]
pub struct PoolState {
    /// Reserve of the first asset
    pub reserve0: u64,
    /// Reserve of the second asset
    pub reserve1: u64,
    /// Total amount of outstanding LP tokens
    pub outstanding_lp_tokens: u64,
    /// tx that created the pool
    creation_txid: Txid,
}

impl PoolState {
    fn new(txid: Txid) -> Self {
        Self {
            reserve0: 0,
            reserve1: 0,
            outstanding_lp_tokens: 0,
            creation_txid: txid,
        }
    }

    /// Returns the new pool state after minting a position
    pub fn mint(&self, amount0: u64, amount1: u64) -> Result<Self, Error> {
        // Geometric mean of two [`u64`]s
        fn geometric_mean(x: u64, y: u64) -> u64 {
            num::integer::sqrt(x as u128 * y as u128)
            // u64 truncation of u128 square root is always safe
            as u64
        }
        let PoolState {
            reserve0,
            reserve1,
            outstanding_lp_tokens,
            creation_txid,
        } = self;
        let new_reserve0 =
            reserve0.checked_add(amount0).ok_or(Error::InvalidMint)?;
        let new_reserve1 =
            reserve1.checked_add(amount1).ok_or(Error::InvalidMint)?;
        if *reserve0 == 0 || *reserve1 == 0 || *outstanding_lp_tokens == 0 {
            let lp_tokens_minted = geometric_mean(new_reserve0, new_reserve1);
            let new_outstanding_lp_tokens =
                outstanding_lp_tokens + lp_tokens_minted;
            Ok(PoolState {
                reserve0: new_reserve0,
                reserve1: new_reserve1,
                outstanding_lp_tokens: new_outstanding_lp_tokens,
                creation_txid: *creation_txid,
            })
        } else {
            // LP tokens minted based on asset 0
            let lp_tokens_minted_0: u128 = (*outstanding_lp_tokens as u128
                * amount0 as u128)
                / *reserve0 as u128;
            // LP tokens minted based on asset 1
            let lp_tokens_minted_1: u128 = (*outstanding_lp_tokens as u128
                * amount1 as u128)
                / *reserve1 as u128;
            // LP tokens minted is the minimum of the two calculations
            let lp_tokens_minted: u64 =
                u128::min(lp_tokens_minted_0, lp_tokens_minted_1)
                    .try_into()
                    .map_err(|_| Error::LpTokenOverflow)?;
            let new_outstanding_lp_tokens = outstanding_lp_tokens
                .checked_add(lp_tokens_minted)
                .ok_or(Error::LpTokenOverflow)?;
            Ok(PoolState {
                reserve0: new_reserve0,
                reserve1: new_reserve1,
                outstanding_lp_tokens: new_outstanding_lp_tokens,
                creation_txid: *creation_txid,
            })
        }
    }

    /// Returns the new pool state after reverting a mint.
    fn revert_mint(
        &self,
        amount0: u64,
        amount1: u64,
        lp_tokens_minted: u64,
    ) -> Result<Self, Error> {
        let new_reserve0 = self
            .reserve0
            .checked_sub(amount0)
            .ok_or(Error::InvalidMint)?;
        let new_reserve1 = self
            .reserve1
            .checked_sub(amount1)
            .ok_or(Error::InvalidMint)?;
        let new_outstanding_lp_tokens = self
            .outstanding_lp_tokens
            .checked_sub(lp_tokens_minted)
            .ok_or(Error::LpTokenUnderflow)?;
        let new_state = Self {
            reserve0: new_reserve0,
            reserve1: new_reserve1,
            outstanding_lp_tokens: new_outstanding_lp_tokens,
            creation_txid: self.creation_txid,
        };
        if *self == new_state.mint(amount0, amount1)? {
            Ok(new_state)
        } else {
            Err(Error::RevertMint)
        }
    }

    /// Returns the new pool state after burning a position
    pub fn burn(&self, lp_token_burn: u64) -> Result<Self, Error> {
        let PoolState {
            reserve0,
            reserve1,
            outstanding_lp_tokens,
            creation_txid,
        } = self;
        if *outstanding_lp_tokens == 0 {
            do yeet Error::InvalidBurn
        };
        // compute payout based on either asset
        let payout = |reserve: u64| -> Result<u64, Error> {
            let payout: u128 = (reserve as u128 * lp_token_burn as u128)
                / (*outstanding_lp_tokens as u128);
            payout.try_into().map_err(|_| Error::BurnOverflow)
        };
        // payout in asset 0
        let payout_0 = payout(*reserve0)?;
        // payout in asset 1
        let payout_1 = payout(*reserve1)?;
        let new_reserve0 =
            reserve0.checked_sub(payout_0).ok_or(Error::BurnUnderflow)?;
        let new_reserve1 =
            reserve1.checked_sub(payout_1).ok_or(Error::BurnUnderflow)?;
        let new_outstanding_lp_tokens = outstanding_lp_tokens
            .checked_sub(lp_token_burn)
            .ok_or(Error::BurnUnderflow)?;
        Ok(PoolState {
            reserve0: new_reserve0,
            reserve1: new_reserve1,
            outstanding_lp_tokens: new_outstanding_lp_tokens,
            creation_txid: *creation_txid,
        })
    }

    /// Returns the new pool state after reverting a burn
    pub fn unburn(
        &self,
        lp_token_unburn: u64,
        payout_0: u64,
        payout_1: u64,
    ) -> Result<Self, Error> {
        let PoolState {
            reserve0,
            reserve1,
            outstanding_lp_tokens,
            creation_txid,
        } = self;
        let new_outstanding_lp_tokens = outstanding_lp_tokens
            .checked_add(lp_token_unburn)
            .ok_or(Error::BurnOverflow)?;
        let new_reserve0 =
            reserve0.checked_add(payout_0).ok_or(Error::BurnOverflow)?;
        let new_reserve1 =
            reserve1.checked_add(payout_1).ok_or(Error::BurnOverflow)?;
        Ok(PoolState {
            reserve0: new_reserve0,
            reserve1: new_reserve1,
            outstanding_lp_tokens: new_outstanding_lp_tokens,
            creation_txid: *creation_txid,
        })
    }

    /// Returns the new pool state after a swap
    pub fn swap_asset0_for_asset1(
        &self,
        amount_spend: u64,
    ) -> Result<Self, Error> {
        let PoolState {
            reserve0,
            reserve1,
            outstanding_lp_tokens,
            creation_txid,
        } = self;
        let reserve_product: u128 = *reserve0 as u128 * *reserve1 as u128;
        let spend_after_fee = ((amount_spend as u128 * 997) / 1000) as u64;
        let _spend_fee = amount_spend
            .checked_sub(spend_after_fee)
            .ok_or(Error::InvalidSwap)?;

        // used for computing product for swap price
        let effective_spend_asset_reserve = reserve0 + spend_after_fee;
        let new_receive_asset_reserve_before_fee: u64 = reserve_product
            .div_ceil(effective_spend_asset_reserve as u128)
            .try_into()
            .map_err(|_| Error::InvalidSwap)?;
        let amount_receive_before_fee: u64 = reserve1
            .checked_sub(new_receive_asset_reserve_before_fee)
            .ok_or(Error::InvalidSwap)?;
        let amount_receive_after_fee =
            ((amount_receive_before_fee as u128 * 997) / 1000) as u64;
        let _receive_fee = amount_receive_before_fee
            .checked_sub(amount_receive_before_fee)
            .ok_or(Error::InvalidSwap)?;
        let (new_reserve0, new_reserve1) = {
            let new_reserve1 = reserve1
                .checked_sub(amount_receive_after_fee)
                .ok_or(Error::InsufficientLiquidity)?;
            (reserve0 + amount_spend, new_reserve1)
        };
        Ok(PoolState {
            reserve0: new_reserve0,
            reserve1: new_reserve1,
            outstanding_lp_tokens: *outstanding_lp_tokens,
            creation_txid: *creation_txid,
        })
    }

    /// Returns the new pool state after a swap
    pub fn swap_asset1_for_asset0(
        &self,
        amount_spend: u64,
    ) -> Result<Self, Error> {
        let PoolState {
            reserve0,
            reserve1,
            outstanding_lp_tokens,
            creation_txid,
        } = self;
        let reserve_product: u128 = *reserve0 as u128 * *reserve1 as u128;
        let spend_after_fee = ((amount_spend as u128 * 997) / 1000) as u64;
        let _spend_fee = amount_spend
            .checked_sub(spend_after_fee)
            .ok_or(Error::InvalidSwap)?;
        // used for computing product for swap price
        let effective_spend_asset_reserve = reserve1 + spend_after_fee;
        let new_receive_asset_reserve_before_fee: u64 = reserve_product
            .div_ceil(effective_spend_asset_reserve as u128)
            .try_into()
            .map_err(|_| Error::InvalidSwap)?;
        let amount_receive_before_fee: u64 = reserve0
            .checked_sub(new_receive_asset_reserve_before_fee)
            .ok_or(Error::InvalidSwap)?;
        let amount_receive_after_fee =
            ((amount_receive_before_fee as u128 * 997) / 1000) as u64;
        let _receive_fee = amount_receive_before_fee
            .checked_sub(amount_receive_before_fee)
            .ok_or(Error::InvalidSwap)?;
        let (new_reserve0, new_reserve1) = {
            let new_reserve0 = reserve0
                .checked_sub(amount_receive_after_fee)
                .ok_or(Error::InsufficientLiquidity)?;
            (new_reserve0, reserve1 + amount_spend)
        };
        Ok(PoolState {
            reserve0: new_reserve0,
            reserve1: new_reserve1,
            outstanding_lp_tokens: *outstanding_lp_tokens,
            creation_txid: *creation_txid,
        })
    }

    /// Returns the pool state after reverting a swap
    fn revert_swap(&self, swap: AmmSwap) -> Result<Self, Error> {
        let amm_pair = AmmPair::new(swap.asset_receive, swap.asset_spend);
        let (new_reserve0, new_reserve1) =
            if swap.asset_spend == amm_pair.asset1() {
                let new_reserve0 = self.reserve0 + swap.amount_receive;
                let new_reserve1 = self.reserve1 - swap.amount_spend;
                (new_reserve0, new_reserve1)
            } else {
                let new_reserve0 = self.reserve0 - swap.amount_spend;
                let new_reserve1 = self.reserve1 + swap.amount_receive;
                (new_reserve0, new_reserve1)
            };
        let new_state = Self {
            reserve0: new_reserve0,
            reserve1: new_reserve1,
            outstanding_lp_tokens: self.outstanding_lp_tokens,
            creation_txid: self.creation_txid,
        };
        // apply the swap again to see if the reverted state is correct
        let check_state = if swap.asset_spend == amm_pair.asset0() {
            new_state.swap_asset0_for_asset1(swap.amount_spend)?
        } else {
            new_state.swap_asset1_for_asset0(swap.amount_spend)?
        };
        if check_state == *self {
            Ok(new_state)
        } else {
            Err(Error::RevertSwap)
        }
    }
}

pub type PoolsDb =
    DatabaseUnique<SerdeBincode<AmmPair>, SerdeBincode<PoolState>>;
pub type RoPoolsDb =
    RoDatabaseUnique<SerdeBincode<AmmPair>, SerdeBincode<PoolState>>;

// Apply AMM burn
pub(in crate::state) fn apply_burn(
    pools: &PoolsDb,
    rwtxn: &mut RwTxn,
    filled_tx: &FilledTransaction,
) -> Result<(), Error> {
    let AmmBurn {
        asset0,
        asset1,
        lp_token_burn,
        amount0,
        amount1,
    } = filled_tx.amm_burn().ok_or(Error::InvalidBurn)?;
    let amm_pair = AmmPair::new(asset0, asset1);
    let amm_pool_state = pools.try_get(rwtxn, &amm_pair)?.ok_or_else(|| {
        Error::MissingPoolState {
            asset0: amm_pair.asset0(),
            asset1: amm_pair.asset1(),
        }
    })?;
    let new_amm_pool_state = amm_pool_state.burn(lp_token_burn)?;
    // payout in asset 0
    let payout0 = amm_pool_state.reserve0 - new_amm_pool_state.reserve0;
    if payout0 != amount0 {
        return Err(Error::InvalidBurn);
    }
    // payout in asset 1
    let payout1 = amm_pool_state.reserve1 - new_amm_pool_state.reserve1;
    if payout1 != amount1 {
        return Err(Error::InvalidBurn);
    }
    pools.put(rwtxn, &amm_pair, &new_amm_pool_state)?;
    Ok(())
}

pub(in crate::state) fn revert_burn(
    pools: &PoolsDb,
    rwtxn: &mut RwTxn,
    filled_tx: &FilledTransaction,
) -> Result<(), Error> {
    let AmmBurn {
        asset0,
        asset1,
        lp_token_burn,
        amount0,
        amount1,
    } = filled_tx.amm_burn().ok_or(Error::InvalidBurn)?;
    let amm_pair = AmmPair::new(asset0, asset1);
    let amm_pool_state = pools.try_get(rwtxn, &amm_pair)?.ok_or_else(|| {
        Error::MissingPoolState {
            asset0: amm_pair.asset0(),
            asset1: amm_pair.asset1(),
        }
    })?;
    let prev_amm_pool_state =
        amm_pool_state.unburn(lp_token_burn, amount0, amount1)?;
    pools.put(rwtxn, &amm_pair, &prev_amm_pool_state)?;
    Ok(())
}

// Apply AMM mint
pub(in crate::state) fn apply_mint(
    pools: &PoolsDb,
    rwtxn: &mut RwTxn,
    filled_tx: &FilledTransaction,
) -> Result<(), Error> {
    let AmmMint {
        asset0,
        asset1,
        amount0,
        amount1,
        lp_token_mint,
    } = filled_tx.amm_mint().ok_or(Error::InvalidMint)?;
    if asset0 == asset1 {
        return Err(Error::InvalidMint);
    }
    let amm_pair = AmmPair::new(asset0, asset1);
    let amm_pool_state = pools
        .try_get(rwtxn, &amm_pair)?
        .unwrap_or_else(|| PoolState::new(filled_tx.txid()));
    let new_amm_pool_state = amm_pool_state.mint(amount0, amount1)?;
    let lp_tokens_minted = new_amm_pool_state
        .outstanding_lp_tokens
        .checked_sub(amm_pool_state.outstanding_lp_tokens)
        .ok_or(Error::InvalidMint)?;
    if lp_tokens_minted != lp_token_mint {
        do yeet Error::InvalidMint;
    }
    pools.put(rwtxn, &amm_pair, &new_amm_pool_state)?;
    Ok(())
}

// Revert AMM mint
pub(in crate::state) fn revert_mint(
    pools: &PoolsDb,
    rwtxn: &mut RwTxn,
    filled_tx: &FilledTransaction,
) -> Result<(), Error> {
    let AmmMint {
        asset0,
        asset1,
        amount0,
        amount1,
        lp_token_mint,
    } = filled_tx.amm_mint().ok_or(Error::InvalidMint)?;
    if asset0 == asset1 {
        return Err(Error::InvalidMint);
    }
    let amm_pair = AmmPair::new(asset0, asset1);
    let amm_pool_state = pools.try_get(rwtxn, &amm_pair)?.ok_or_else(|| {
        Error::MissingPoolState {
            asset0: amm_pair.asset0(),
            asset1: amm_pair.asset1(),
        }
    })?;
    if amm_pool_state.creation_txid == filled_tx.txid() {
        pools.delete(rwtxn, &amm_pair)?;
    } else {
        let new_amm_pool_state =
            amm_pool_state.revert_mint(amount0, amount1, lp_token_mint)?;
        pools.put(rwtxn, &amm_pair, &new_amm_pool_state)?;
    }
    Ok(())
}

// Apply AMM swap
pub(in crate::state) fn apply_swap(
    pools: &PoolsDb,
    rwtxn: &mut RwTxn,
    filled_tx: &FilledTransaction,
) -> Result<(), Error> {
    let AmmSwap {
        asset_spend,
        asset_receive,
        amount_spend,
        amount_receive,
    } = filled_tx.amm_swap().ok_or(Error::InvalidSwap)?;
    let amm_pair = AmmPair::new(asset_spend, asset_receive);
    let amm_pool_state = pools.try_get(rwtxn, &amm_pair)?.ok_or_else(|| {
        Error::MissingPoolState {
            asset0: amm_pair.asset0(),
            asset1: amm_pair.asset1(),
        }
    })?;
    let (new_amm_pool_state, amount_receive_after_fee) =
        if asset_spend < asset_receive {
            let new_amm_pool_state =
                amm_pool_state.swap_asset0_for_asset1(amount_spend)?;
            let amount_receive_after_fee =
                amm_pool_state.reserve1 - new_amm_pool_state.reserve1;
            (new_amm_pool_state, amount_receive_after_fee)
        } else {
            let new_amm_pool_state =
                amm_pool_state.swap_asset1_for_asset0(amount_spend)?;
            let amount_receive_after_fee =
                amm_pool_state.reserve0 - new_amm_pool_state.reserve0;
            (new_amm_pool_state, amount_receive_after_fee)
        };
    if amount_receive != amount_receive_after_fee {
        return Err(Error::InvalidSwap);
    }
    pools.put(rwtxn, &amm_pair, &new_amm_pool_state)?;
    Ok(())
}

// Revert AMM swap
pub(in crate::state) fn revert_swap(
    pools: &PoolsDb,
    rwtxn: &mut RwTxn,
    filled_tx: &FilledTransaction,
) -> Result<(), Error> {
    let amm_swap @ AmmSwap {
        asset_spend,
        asset_receive,
        amount_spend: _,
        amount_receive: _,
    } = filled_tx.amm_swap().ok_or(Error::InvalidSwap)?;
    let amm_pair = AmmPair::new(asset_spend, asset_receive);
    let amm_pool_state = pools.try_get(rwtxn, &amm_pair)?.ok_or_else(|| {
        Error::MissingPoolState {
            asset0: amm_pair.asset0(),
            asset1: amm_pair.asset1(),
        }
    })?;
    let new_amm_pool_state = amm_pool_state.revert_swap(amm_swap)?;
    pools.put(rwtxn, &amm_pair, &new_amm_pool_state)?;
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::{
        state::{
            amm::{AmmPair, PoolState, apply_burn, apply_mint},
            test::fresh_state,
        },
        types::{
            Address, AssetId, BitAssetId, FilledOutput, FilledOutputContent,
            FilledTransaction, OutPoint, Output, OutputContent, Transaction,
            TxData, Txid,
        },
    };

    fn bitasset(byte: u8) -> BitAssetId {
        BitAssetId([byte; blake3::OUT_LEN])
    }

    fn txid(byte: u8) -> Txid {
        Txid([byte; blake3::OUT_LEN])
    }

    fn outpoint(byte: u8, vout: u32) -> OutPoint {
        OutPoint::Regular {
            txid: txid(byte),
            vout,
        }
    }

    fn bitasset_output(asset: BitAssetId, value: u64) -> FilledOutput {
        Output::new(
            Address::ALL_ZEROS,
            FilledOutputContent::BitAsset(asset, value),
        )
    }

    fn lp_output(
        asset0: AssetId,
        asset1: AssetId,
        amount: u64,
    ) -> FilledOutput {
        Output::new(
            Address::ALL_ZEROS,
            FilledOutputContent::AmmLpToken {
                asset0,
                asset1,
                amount,
            },
        )
    }

    #[test]
    fn apply_mint_burn_wrong_lp_baseline() -> anyhow::Result<()> {
        let (env, state) = fresh_state("apply_mint_burn_wrong_lp_baseline")?;

        let asset_a = bitasset(1);
        let asset_b = bitasset(2);
        let asset0 = AssetId::BitAsset(asset_a);
        let asset1 = AssetId::BitAsset(asset_b);
        let pair = AmmPair::new(asset0, asset1);

        let initial_pool = PoolState {
            reserve0: 1_000_000,
            reserve1: 1_000_000,
            outstanding_lp_tokens: 1_000_000,
            creation_txid: txid(9),
        };
        {
            let mut rwtxn = env.write_txn()?;
            state.amm_pools.put(&mut rwtxn, &pair, &initial_pool)?;
            rwtxn.commit()?;
        }

        let correct_after_mint = initial_pool.mint(2, 2)?;
        let actual_lp_tokens_for_deposit = correct_after_mint
            .outstanding_lp_tokens
            - initial_pool.outstanding_lp_tokens;
        anyhow::ensure!(actual_lp_tokens_for_deposit == 2);

        let mint_tx =
            |lp_token_mint: u64| -> anyhow::Result<FilledTransaction> {
                let res = FilledTransaction {
                    transaction: Transaction {
                        inputs: vec![outpoint(10, 0), outpoint(11, 0)],
                        outputs: vec![Output::new(
                            Address::ALL_ZEROS,
                            OutputContent::AmmLpToken(lp_token_mint),
                        )],
                        memo: Vec::new(),
                        data: Some(TxData::AmmMint {
                            amount0: 2,
                            amount1: 2,
                            lp_token_mint,
                        }),
                    },
                    spent_utxos: vec![
                        bitasset_output(asset_a, 2),
                        bitasset_output(asset_b, 2),
                    ],
                };
                let filled_mint_outputs = res
                    .filled_outputs()
                    .ok_or_else(|| anyhow::anyhow!("AMM LP output fills"))?;
                anyhow::ensure!(
                    filled_mint_outputs[0].content()
                        == &FilledOutputContent::AmmLpToken {
                            asset0,
                            asset1,
                            amount: lp_token_mint,
                        }
                );
                Ok(res)
            };

        // Attempting to apply a mint with incorrect declared lp_tokens should
        // fail
        {
            let attacker_declared_lp = 500_001;
            let mint_tx = mint_tx(attacker_declared_lp)?;
            let mut rwtxn = env.write_txn()?;
            anyhow::ensure!(
                apply_mint(&state.amm_pools, &mut rwtxn, &mint_tx).is_err()
            );
        }
        // Attempting to apply a mint with correctly declared lp_tokens should
        // succeed
        {
            let mint_tx = mint_tx(actual_lp_tokens_for_deposit)?;
            let mut rwtxn = env.write_txn()?;
            let () = apply_mint(&state.amm_pools, &mut rwtxn, &mint_tx)?;
            rwtxn.commit()?;
        }

        let lp_token_burn = 500_001;
        let burn_tx = FilledTransaction {
            transaction: Transaction {
                inputs: vec![outpoint(12, 0)],
                outputs: vec![
                    Output::new(
                        Address::ALL_ZEROS,
                        OutputContent::BitAsset(500_001),
                    ),
                    Output::new(
                        Address::ALL_ZEROS,
                        OutputContent::BitAsset(500_001),
                    ),
                ],
                memo: Vec::new(),
                data: Some(TxData::AmmBurn {
                    amount0: 500_001,
                    amount1: 500_001,
                    lp_token_burn,
                }),
            },
            spent_utxos: vec![lp_output(asset0, asset1, lp_token_burn)],
        };

        {
            let mut rwtxn = env.write_txn()?;
            let () = apply_burn(&state.amm_pools, &mut rwtxn, &burn_tx)?;
            rwtxn.commit()?;
        }
        let pool_after_burn = {
            let rotxn = env.read_txn()?;
            state.amm_pools.get(&rotxn, &pair)?
        };
        assert_eq!(pool_after_burn.reserve0, 500_001);
        assert_eq!(pool_after_burn.reserve1, 500_001);
        assert_eq!(pool_after_burn.outstanding_lp_tokens, 500_001);

        // cleanup
        {
            drop(state);
            let path = env.path().clone();
            drop(env);
            std::fs::remove_dir_all(path)?;
        }
        Ok(())
    }
}
