use std::path::Path;

use bitcoin::{Amount, Txid};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ElementsRpcError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("RPC error: {code} {message}")]
    Rpc { code: i32, message: String },
    #[error("IO error reading cookie: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid cookie format")]
    InvalidCookie,
    #[error("Amount parse error: {0}")]
    AmountParse(#[from] bitcoin::amount::ParseAmountError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Utxo {
    pub txid: Txid,
    pub vout: u32,
    pub address: Option<String>,
    #[serde(deserialize_with = "deserialize_amount")]
    pub amount: Amount,
    pub confirmations: u32,
    #[serde(default)]
    pub spendable: bool,
}

fn deserialize_amount<'de, D>(deserializer: D) -> Result<Amount, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v: f64 = Deserialize::deserialize(deserializer)?;
    let sats = (v * 100_000_000.0).round() as u64;
    Ok(Amount::from_sat(sats))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionInfo {
    pub txid: Txid,
    #[serde(default)]
    pub address: Option<String>,
    pub category: String,
    pub amount: f64,
    #[serde(default)]
    pub fee: Option<f64>,
    pub confirmations: i32,
    #[serde(default)]
    pub blockhash: Option<bitcoin::BlockHash>,
    #[serde(default)]
    pub blockheight: Option<u32>,
}

#[derive(Clone)]
pub struct ElementsRpc {
    client: Client,
    url: String,
    auth_user: String,
    auth_pass: String,
}

impl ElementsRpc {
    pub fn new(rpc_url: &str, elements_datadir: Option<&Path>) -> Result<Self, ElementsRpcError> {
        let cookie_paths = [
            elements_datadir.map(|d| d.join("regtest/.cookie")),
            elements_datadir.map(|d| d.join(".cookie")),
            dirs::home_dir().map(|h| h.join(".elements/regtest/.cookie")),
            Some(Path::new("/tmp/liquid-id5-regtest/regtest/.cookie").to_path_buf()),
        ];

        let mut auth_user = "__cookie__".to_string();
        let mut auth_pass = String::new();

        for path in cookie_paths.iter().flatten() {
            if let Ok(content) = std::fs::read_to_string(path) {
                let content = content.trim();
                if let Some((user, pass)) = content.split_once(':') {
                    auth_user = user.to_string();
                    auth_pass = pass.to_string();
                    eprintln!("Loaded elementsd RPC cookie from {}", path.display());
                    break;
                }
            }
        }

        if auth_pass.is_empty() {
            eprintln!("WARNING: No elementsd cookie found; may fail auth");
        }

        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()?;

        Ok(Self {
            client,
            url: rpc_url.trim_end_matches('/').to_string(),
            auth_user,
            auth_pass,
        })
    }

    async fn call<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: Vec<Value>,
    ) -> Result<T, ElementsRpcError> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1u64,
            "method": method,
            "params": params,
        });

        let resp = self
            .client
            .post(&self.url)
            .basic_auth(&self.auth_user, Some(&self.auth_pass))
            .json(&body)
            .send()
            .await?;

        let json: Value = resp.json().await?;

        if let Some(err) = json.get("error") {
            if !err.is_null() {
                let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(-1) as i32;
                let msg = err.get("message").and_then(|m| m.as_str()).unwrap_or("unknown").to_string();
                return Err(ElementsRpcError::Rpc { code, message: msg });
            }
        }

        let result = json.get("result").cloned().unwrap_or(Value::Null);
        Ok(serde_json::from_value(result)?)
    }

    pub async fn getblockcount(&self) -> Result<u64, ElementsRpcError> {
        self.call("getblockcount", vec![]).await
    }

    pub async fn getbalance(&self) -> Result<Amount, ElementsRpcError> {
        let val: Value = self.call("getbalance", vec![]).await?;
        let btc = val.as_f64().or_else(|| val.as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
        let sats = (btc * 100_000_000.0).round() as u64;
        Ok(Amount::from_sat(sats))
    }

    pub async fn getnewaddress(&self) -> Result<String, ElementsRpcError> {
        self.call("getnewaddress", vec![]).await
    }

    pub async fn listunspent(&self) -> Result<Vec<Utxo>, ElementsRpcError> {
        self.call("listunspent", vec![]).await
    }

    pub async fn listtransactions(&self, count: u32) -> Result<Vec<TransactionInfo>, ElementsRpcError> {
        self.call("listtransactions", vec![json!("*"), json!(count)]).await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("=== L-BTC ELEMENTS RPC VERIFIER (recovery evidence) ===");
    eprintln!("Connecting to elementsd at http://127.0.0.1:18443 ...");

    let rpc = ElementsRpc::new("http://127.0.0.1:18443", Some(std::path::Path::new("/tmp/liquid-id5-regtest")))?;

    let height = rpc.getblockcount().await?;
    eprintln!("getblockcount: {}", height);

    let bal = rpc.getbalance().await?;
    eprintln!("getbalance (L-BTC): {:.8}", bal.to_btc());

    let addr = rpc.getnewaddress().await?;
    eprintln!("getnewaddress: {}", addr);

    let utxos = rpc.listunspent().await?;
    eprintln!("listunspent: {} UTXOs (showing first 3)", utxos.len());
    for u in utxos.iter().take(3) {
        eprintln!("  {}:{}  amt={:.8} confs={}", u.txid, u.vout, u.amount.to_btc(), u.confirmations);
    }

    let txs = rpc.listtransactions(5).await?;
    eprintln!("listtransactions (last 5):");
    for t in &txs {
        eprintln!("  {}  amt={:.8} confs={} cat={}", t.txid, t.amount, t.confirmations, t.category);
    }

    eprintln!("=== SUCCESS: Real L-BTC data retrieved from elementsd ===");
    println!("VERIFIED_HEIGHT={}", height);
    println!("VERIFIED_BALANCE_LBTC={:.8}", bal.to_btc());
    println!("VERIFIED_ADDR_PREFIX={}", &addr[..16]);
    println!("VERIFIED_UTXO_COUNT={}", utxos.len());
    println!("VERIFIED_TX_COUNT={}", txs.len());
    Ok(())
}
