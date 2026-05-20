# BitAssets Lite-Wallet PR Notes

## Public Protocol Surface

This branch adds the plain-bitassets side of the issue #28 lite-wallet path:

- `get_lite_wallet_update(script_hashes, from_block_hash)` returns script-hash-scoped wallet deltas from the current sidechain view.
- `submit_authorized_transaction` accepts raw locally signed BitAssets transactions for broadcast.
- `--lite-wallet-quic-addr <ADDR>` starts the QUIC lite-wallet service in app/headless mode.

The existing wallet RPCs and delegated wallet flows remain unchanged for compatibility.

## `get_lite_wallet_update`

Inputs:

- `script_hashes`: non-empty array of 32-byte lowercase or uppercase hex script hashes. The server normalizes to lowercase, deduplicates, and rejects more than 256 entries.
- `from_block_hash`: optional sidechain block hash. `null` returns a snapshot for the watched script hashes; a known block hash returns a delta from that point.

Output includes:

- current sidechain `tip_hash` and `tip_height`
- confirmed watched UTXOs
- watched spent outpoints
- mempool watched created UTXOs
- mempool watched spent outpoints
- relevant transactions when readily available
- current sidechain Utreexo roots
- proof refs for confirmed watched UTXOs and relevant confirmed transactions

Invalid watch sets fail consistently for JSON-RPC and QUIC: empty arrays, malformed hex, wrong-length script hashes, and oversized requests are rejected with clear errors.

## QUIC Lite-Wallet Service

The QUIC service is a dedicated app-layer lite-wallet transport. It does not replace the existing block-sync P2P path.

Messages:

- `Subscribe { script_hashes, from_block_hash }`
- `Snapshot(LiteWalletUpdate)`
- `Mempool(LiteWalletUpdate)`
- `Confirmed(LiteWalletUpdate)`
- `Error { message }`

Framing is intentionally simple for this PR: one JSON message per line on a bidirectional QUIC stream. Request bodies are capped at 64 KiB. Mempool relevance is polled at a bounded interval, and confirmed updates are pushed from node watch-state notifications.

## Live Smoke Result

The coordinating Floresta branch uses the local smoke harness in `local-dev`:

```bash
cd /Users/lukekensik/drivechain-wallet-dev/local-dev
PREPARE_STACK=0 \
BITASSETS_IMAGE=local/plain-bitassets:codex-proof \
BITASSETS_QUIC_URL=127.0.0.1:6104 \
BMM_MINE_ATTEMPTS=8 \
BMM_REQUEST_SETTLE_SECS=40 \
BITASSETS_MINE_TIMEOUT=120 \
./scripts/floresta-bitassets-native-wallet-smoke.sh
```

Latest passing result proved QUIC sync, restart persistence, transfer, reserve/register for two assets, AMM mint/swap/burn, and Dutch auction create/bid/collect:

The post-rebase validation run used the same harness with stack preparation and longer Mac/QEMU wait windows:

```bash
PREPARE_STACK=1 \
BITASSETS_IMAGE=local/plain-bitassets:codex-proof \
BITASSETS_QUIC_URL=127.0.0.1:6104 \
BMM_MINE_ATTEMPTS=12 \
BMM_REQUEST_SETTLE_SECS=40 \
BITASSETS_MINE_TIMEOUT=180 \
WALLET_WAIT_SECS=240 \
QUIC_WAIT_SECS=90 \
./scripts/floresta-bitassets-native-wallet-smoke.sh
```

```json
{
  "mode": "native-wallet",
  "asset_a": "7c7bc226ca3a53bc549cdb17c6b7002fc2c56c2086e48579598ff6a950ea482f",
  "asset_b": "993f25719b66763ffcb36683b58cfa0edd42a9defa0ddb2d3bdd920f5d732c58",
  "txids": {
    "transfer": "2c50b836c2d49441112060a0a4bc6e6ba0d34a211fc1d61c5b4dcc3a45eeebe1",
    "reserve_a": "0a057b47396b4541821ac896713bfe33c0e6cc3aced6166bdf2039a9b8b9082b",
    "register_a": "ead5fc378486d91ab32e3e1dcb4e277c18f51db55cdb051ecd1ab642040b8221",
    "reserve_b": "772a996b6f957bcaab427ccc853e54e569a6df33a5e62ad162056c09305fa885",
    "register_b": "bb48994f50d81c0f5eb325b6227009958415ef08674dedc6a2cb34d4a32eeda9",
    "amm_mint": "d9e8a63e925631a6e7a991fc7a643043e0dda4a17214f8d291f59636062e0bc7",
    "amm_swap": "4dafb6dbb72638e480cece9d774526364c63f688d65c74eb2d3dce0e6a624cc4",
    "amm_burn": "e3480003519a2141945fbf5c6242dc1150c61c6fdc0341b1c055f75ab9731d5f",
    "dutch_auction_create": "9e294e6f60c705e7d7f197ca6c85792c2bedfd1436ad18362523fda086204e63",
    "dutch_auction_bid": "2b34ef4cde69036fcbf22f93b9cc13e5c5d8ae610d60f6d0daac38438f18decc",
    "dutch_auction_collect": "de5d48259183581b9d2ad4b25e928f21c162d134308527dfdc977106242d7c90"
  },
  "final_balances": {
    "7c7bc226ca3a53bc549cdb17c6b7002fc2c56c2086e48579598ff6a950ea482f": 9090,
    "993f25719b66763ffcb36683b58cfa0edd42a9defa0ddb2d3bdd920f5d732c58": 9106,
    "control:7c7bc226ca3a53bc549cdb17c6b7002fc2c56c2086e48579598ff6a950ea482f": 1,
    "control:993f25719b66763ffcb36683b58cfa0edd42a9defa0ddb2d3bdd920f5d732c58": 1,
    "lp:7c7bc226ca3a53bc549cdb17c6b7002fc2c56c2086e48579598ff6a950ea482f:993f25719b66763ffcb36683b58cfa0edd42a9defa0ddb2d3bdd920f5d732c58": 900
  }
}
```

## Validation Commands

```bash
cargo check -p plain_bitassets_app_rpc_api -p plain_bitassets_app_cli -p plain_bitassets_app
cargo test -p plain_bitassets --lib -- --quiet
cargo test -p plain_bitassets_app --bin plain_bitassets_app -- --quiet
```

## PR Draft Notes

Suggested title:

```text
Add script-hash BitAssets lite-wallet RPC and QUIC updates
```

Summary bullets:

- Adds script-hash-scoped lite-wallet snapshots/deltas with proof refs and current sidechain roots.
- Adds a dedicated app-layer QUIC subscription service for lite-wallet snapshots, mempool updates, and confirmed updates.
- Adds server-side validation limits for watch sets and malformed script hashes.
- Keeps existing wallet RPC compatibility and raw authorized transaction broadcast.

Known limits for reviewers:

- QUIC framing is newline-delimited JSON for this PR-ready pass.
- Compact filters and additional privacy features are out of scope for issue #28 closure.
- Full Floresta wallet usage is covered in the coordinated Floresta PR.
