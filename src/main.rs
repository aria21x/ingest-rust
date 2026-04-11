use anyhow::{Context, Result};
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tokio_postgres::NoTls;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

// Single program ID to monitor (Pump.fun - meme coin launches and initial trades)
const MONITORED_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

#[derive(Debug, Deserialize)]
struct SubscriptionResult {
    result: serde_json::Value,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct LogsNotification {
    params: LogsParams,
}

#[derive(Debug, Deserialize)]
struct LogsParams {
    result: LogsResult,
    subscription: u64,
}

#[derive(Debug, Deserialize)]
struct LogsResult {
    signature: String,
    err: Option<serde_json::Value>,
    logs: Vec<String>,
}

#[derive(Debug, Clone)]
struct Config {
    wss_url: String,
    database_url: String,
}

impl Config {
    fn from_env() -> Result<Self> {
        let wss_url = std::env::var("HELIUS_WSS_URL")
            .context("HELIUS_WSS_URL not set")?;
        let database_url = std::env::var("DATABASE_URL")
            .context("DATABASE_URL not set")?;
        Ok(Self { wss_url, database_url })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),
        )
        .init();

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let config = Config::from_env()?;

    let manager = PostgresConnectionManager::new(
        config.database_url.parse()?,
        NoTls,
    );
    let pool = Pool::builder().build(manager).await?;
    info!("Postgres pool created");

    init_db(&pool).await?;

    loop {
        match run_ingest(&config, &pool).await {
            Ok(()) => {
                info!("Ingest loop exited normally, reconnecting in 5s");
                sleep(Duration::from_secs(5)).await;
            }
            Err(e) => {
                error!("Ingest error: {}, reconnecting in 5s", e);
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

async fn init_db(pool: &Pool<PostgresConnectionManager<NoTls>>) -> Result<()> {
    let conn = pool.get().await?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS raw_signatures (
            id BIGSERIAL PRIMARY KEY,
            signature TEXT NOT NULL UNIQUE,
            slot BIGINT NOT NULL,
            block_time TIMESTAMPTZ,
            ingested_at TIMESTAMPTZ DEFAULT NOW(),
            processed BOOLEAN DEFAULT FALSE,
            error_count INTEGER DEFAULT 0
        )",
        &[],
    )
    .await?;
    Ok(())
}

async fn run_ingest(config: &Config, pool: &Pool<PostgresConnectionManager<NoTls>>) -> Result<()> {
    info!("Connecting to Helius WSS: {}", config.wss_url);
    let (ws_stream, _) = connect_async(&config.wss_url).await?;
    info!("WebSocket connected");

    let (mut write, mut read) = ws_stream.split();

    // Subscribe to logs from a single program (mentions expects a string, not array)
    let subscribe_msg = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "logsSubscribe",
        "params": [
            { "mentions": MONITORED_PROGRAM },
            { "commitment": "confirmed" }
        ]
    });
    write.send(Message::Text(subscribe_msg.to_string())).await?;

    let sub_id = wait_for_subscription_confirmation(&mut read).await?;
    info!("Logs subscription confirmed with id: {}", sub_id);

    let mut last_ping = tokio::time::Instant::now();
    loop {
        let msg = timeout(Duration::from_secs(60), read.next()).await?;
        match msg {
            Some(Ok(Message::Text(text))) => {
                if let Ok(notif) = serde_json::from_str::<LogsNotification>(&text) {
                    let sig = &notif.params.result.signature;
                    let slot = 0u64; // logsSubscribe doesn't provide slot; worker will fetch
                    let timestamp = None;

                    debug!("Received log signature: {}", sig);
                    if let Err(e) = insert_signature(pool, sig, slot, timestamp).await {
                        error!("Failed to insert signature {}: {}", sig, e);
                    }
                } else {
                    debug!("Non-logs text message: {}", text);
                }
                last_ping = tokio::time::Instant::now();
            }
            Some(Ok(Message::Ping(data))) => {
                write.send(Message::Pong(data)).await?;
                last_ping = tokio::time::Instant::now();
            }
            Some(Ok(Message::Close(frame))) => {
                warn!("WebSocket closed: {:?}", frame);
                break;
            }
            Some(Err(e)) => {
                error!("WebSocket error: {}", e);
                break;
            }
            None => break,
            _ => {
                last_ping = tokio::time::Instant::now();
                continue;
            }
        }

        if last_ping.elapsed() > Duration::from_secs(20) {
            write.send(Message::Ping(vec![])).await?;
            last_ping = tokio::time::Instant::now();
        }
    }

    Ok(())
}

async fn wait_for_subscription_confirmation(
    read: &mut (impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin),
) -> Result<u64> {
    loop {
        let msg = timeout(Duration::from_secs(10), read.next())
            .await?
            .context("WebSocket closed before subscription confirmation")??;

        if let Message::Text(text) = msg {
            info!("Raw subscription response: {}", text);

            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(err) = json.get("error") {
                    anyhow::bail!("Subscription error: {}", err);
                }
                if let Some(result) = json.get("result") {
                    if let Some(id) = result.as_u64() {
                        return Ok(id);
                    } else {
                        info!("Subscription result is not a number: {:?}", result);
                        return Ok(0);
                    }
                }
                if json.get("method").is_some() {
                    debug!("Received notification before subscription confirmation, continuing...");
                    continue;
                }
            }
        }
        warn!("Unexpected message while waiting for subscription confirmation");
    }
}

async fn insert_signature(
    pool: &Pool<PostgresConnectionManager<NoTls>>,
    signature: &str,
    slot: u64,
    timestamp: Option<i64>,
) -> Result<()> {
    let conn = pool.get().await?;
    let block_time = timestamp.map(|ts| chrono::DateTime::from_timestamp(ts, 0));
    conn.execute(
        "INSERT INTO raw_signatures (signature, slot, block_time)
         VALUES ($1, $2, $3)
         ON CONFLICT (signature) DO NOTHING",
        &[&signature, &(slot as i64), &block_time],
    )
    .await?;
    Ok(())
}
