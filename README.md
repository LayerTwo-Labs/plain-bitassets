# BitAssets

## Install

Check out the repo with `git clone`, and then

```
git submodule update --init
cargo build
```

## Run BitAssets for BitWindow

BitWindow expects the BitAssets sidechain JSON-RPC service on port `6004`.
This node can also expose:

- CUSF sidechain gRPC for BitWindow-compatible sidechain calls on port `50052`
- lite-wallet QUIC updates for phone/local wallets on UDP port `6104`

Start BitWindow's L1/enforcer stack first, then run BitAssets from this repo:

```sh
cargo run -p plain_bitassets_app -- \
  --headless \
  --network signet \
  --mainchain-grpc-host 127.0.0.1 \
  --mainchain-grpc-port 50051 \
  --rpc-host 127.0.0.1 \
  --rpc-port 6004 \
  --sidechain-grpc-port 50052 \
  --lite-wallet-quic-addr 127.0.0.1:6104
```

If BitWindow is already running, leave it open. `bitwindowd` and
`orchestratord` both proxy BitAssets by dialing `127.0.0.1:6004`, so a running
plain-BitAssets node on that port is enough for BitWindow to connect.

### Verify the BitWindow connection

Confirm the node is listening on the expected ports:

```sh
lsof -nP -iTCP:6004 -sTCP:LISTEN
lsof -nP -iTCP:50052 -sTCP:LISTEN
lsof -nP -iUDP:6104
```

Probe the JSON-RPC API BitWindow uses:

```sh
curl --fail --silent --show-error \
  --data-binary '{"jsonrpc":"2.0","id":"bitassets","method":"getblockcount","params":[]}' \
  -H 'content-type: application/json' \
  http://127.0.0.1:6004
```

A healthy node returns a JSON-RPC `result`, for example:

```json
{"jsonrpc":"2.0","id":"bitassets","result":0}
```

You can also verify BitWindow has an active connection to the node:

```sh
lsof -nP -iTCP | grep '127.0.0.1:6004'
```

Look for a `bitwindowd -> 127.0.0.1:6004` established connection.

### BitWindow configuration expectations

BitWindow's sidechain config should contain a `bitassets` entry like this:

```json
{
  "name": "BitAssets",
  "port": 6004,
  "slot": 4,
  "type": "sidechain"
}
```

On macOS, BitWindow commonly reads the merged runtime config from:

```sh
~/Library/Application Support/bitwindow/chains_config.json
```

If BitWindow previously tried to auto-download or auto-start a packaged
BitAssets backend and failed, keep this plain-BitAssets node running on `6004`
and restart or reopen BitWindow. BitWindow will reconnect to the already
listening JSON-RPC service.

### Common issue: sidechain deposit list error

An error like this in BitWindow's Sidechains tab is not a BitAssets node
connection failure:

```text
WalletException: could not list sidechain deposits: unable to fetch wallet transaction ...
No such mempool or blockchain transaction
```

That comes from BitWindow asking the enforcer wallet for L1 sidechain deposit
history. BitAssets is still connected if `getblockcount` on `127.0.0.1:6004`
works and `bitwindowd` has an established connection to port `6004`.

## Connect a Wallet to Utreexo Lite-Wallet Messages

BitAssets exposes a lite-wallet update API for wallets that do not want to run
the full sidechain node. The wallet watches its own BitAssets script hashes,
receives scoped transaction/UTXO updates, and verifies the included Utreexo
roots/proofs against the sidechain tip it trusts.

There are two supported transports:

- JSON-RPC polling with `get_lite_wallet_update(script_hashes, from_block_hash)`
- QUIC subscriptions started with `--lite-wallet-quic-addr <ADDR>`

The QUIC path is the preferred live wallet path. JSON-RPC is useful for first
sync, recovery after disconnects, and debugging.

### Start the BitAssets node

Run the app/headless node with JSON-RPC and the lite-wallet QUIC listener:

```sh
cargo run -p plain_bitassets_app -- \
  --headless \
  --network signet \
  --rpc-host 127.0.0.1 \
  --rpc-port 6004 \
  --lite-wallet-quic-addr 127.0.0.1:6104
```

Use a host/port reachable by the wallet. On a phone or another machine,
`127.0.0.1` means the phone itself, not the Mac/server running BitAssets, so
advertise a LAN, tunnel, or USB-forwarded address instead.

### Export Utreexo peer information for Floresta wallets

Floresta-compatible wallets can import Utreexo peer anchors. If the wallet
already knows the Bitcoin private-signet Utreexo peers, export them explicitly:

```sh
cargo run -p plain_bitassets_app_cli -- \
  --rpc-port 6004 \
  export-private-signet-utreexo-anchors \
  --peer <bitcoin-utreexo-peer-host:port> \
  --output anchors.json
```

If this BitAssets node is already connected to active private-signet peers,
export those instead:

```sh
cargo run -p plain_bitassets_app_cli -- \
  --rpc-port 6004 \
  export-private-signet-utreexo-anchors \
  --active \
  --output anchors.json
```

To give a wallet a single discovery document that includes both the Utreexo
anchor and this node's lite-wallet QUIC endpoint:

```sh
cargo run -p plain_bitassets_app_cli -- \
  --rpc-port 6004 \
  private-signet-utreexo-peer-source \
  --peer <externally-reachable-bitcoin-utreexo-peer-host:port> \
  --output bitassets-peer-source.json
```

The exported `bitassets-peer-source.json` includes:

- the Bitcoin private-signet Utreexo anchor
- this BitAssets node's JSON-RPC URL
- this BitAssets node's lite-wallet QUIC address
- network metadata for the wallet

These export helpers are only valid on `signet`.

### Watch script hashes over JSON-RPC

The wallet must derive the BitAssets addresses it owns and send their 32-byte
script hashes to the node. Script hashes are lowercase or uppercase hex strings;
the server normalizes and deduplicates them. A request may include at most 256
script hashes.

Initial snapshot:

```sh
curl --data-binary '{
  "jsonrpc": "2.0",
  "id": "bitassets-wallet-snapshot",
  "method": "get_lite_wallet_update",
  "params": [["<32-byte-script-hash-hex>"], null]
}' \
  -H 'content-type: application/json' \
  http://127.0.0.1:6004
```

Recovery/delta from a known sidechain block:

```sh
curl --data-binary '{
  "jsonrpc": "2.0",
  "id": "bitassets-wallet-delta",
  "method": "get_lite_wallet_update",
  "params": [["<32-byte-script-hash-hex>"], "<previous-tip-hash>"]
}' \
  -H 'content-type: application/json' \
  http://127.0.0.1:6004
```

The returned `LiteWalletUpdate` includes:

- `tip_hash` and `tip_height`
- confirmed watched UTXOs
- watched spent outpoints
- mempool watched created/spent outpoints
- relevant transactions when available
- `utreexo_leaf_count`
- `utreexo_roots`
- compact `proof_refs`
- `utreexo_proofs` for confirmed watched UTXOs

Wallet behavior:

1. Store the latest accepted `tip_hash`.
2. Store wallet UTXOs only when the script hash belongs to the wallet.
3. Verify included Utreexo proof data against the advertised roots/tip before
   treating confirmed outputs as spendable.
4. Use `from_block_hash` on reconnect. If the node rejects the cursor because
   it is no longer on the active chain, discard the stale cursor and request a
   fresh snapshot with `from_block_hash = null`.

### Subscribe over QUIC

The QUIC service uses one newline-delimited JSON message per bidirectional
stream. Open a QUIC connection to `--lite-wallet-quic-addr`, open a
bidirectional stream, write one `Subscribe` JSON request, finish the write side,
and then read newline-delimited responses until disconnect.

Request:

```json
{
  "Subscribe": {
    "script_hashes": ["<32-byte-script-hash-hex>"],
    "from_block_hash": null
  }
}
```

Responses:

```json
{"Snapshot":{"update":{"tip_hash":"...","tip_height":0}}}
{"Mempool":{"update":{"tip_hash":"...","tip_height":0}}}
{"Confirmed":{"update":{"tip_hash":"...","tip_height":1}}}
{"Error":{"message":"..."}}
```

Use `Snapshot` as the initial wallet state, `Mempool` for unconfirmed display,
and `Confirmed` for block-connected updates. Persist the latest confirmed
`tip_hash` and reconnect with it as `from_block_hash`.

Request bodies are capped at 64 KiB. Empty watch sets, malformed hex,
wrong-length script hashes, and oversized requests return `Error`.

### Submit locally signed transactions

Wallets should sign BitAssets transactions locally. After building and signing
an authorized transaction, submit it through JSON-RPC:

```sh
cargo run -p plain_bitassets_app_cli -- \
  --rpc-port 6004 \
  submit-authorized-transaction <hex-borsh-authorized-tx>
```

After submission, the wallet should expect a `Mempool` update if the transaction
touches a watched script hash, followed by a `Confirmed` update once a BMMed
sidechain block includes it.
