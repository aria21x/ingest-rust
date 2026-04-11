use anyhow::{Context, Result};
use bb8::{Pool, PooledConnection};
use bb8_postgres::PostgresConnectionManager;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tokio_postgres::NoTls;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

// Use rustls for WebSocket (avoid OpenSSL)
type WebSocketStream = tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>;

#[derive(Debug, Deserialize)]
struct SubscriptionResult {
    result: serde_json::Value,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct TransactionNotification {
    params: TransactionParams,
}

#[derive(Debug, Deserialize)]
struct TransactionParams {
    result: TransactionResult,
    subscription: u64,
}

#[derive(Debug, Deserialize)]
struct TransactionResult {
    signature: String,
    slot: u64,
    #[serde(default)]
    timestamp: Option<i64>,
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

    let config = Config::from_env()?;

    // Setup Postgres connection pool
    let manager = PostgresConnectionManager::new(
        config.database_url.parse()?,
        NoTls,
    );
    let pool = Pool::builder().build(manager).await?;
    info!("Postgres pool created");

    // Ensure raw_signatures table exists
    init_db(&pool).await?;

    // Main loop with reconnect
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
            ingested_at TIMESTAMPTZ DEFAULT NOW()
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

    // Subscribe to transactionSubscribe
    let subscribe_msg = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "transactionSubscribe",
        "params": [
            { "vote": false, "failed": false, "accountInclude": [] },
            { "commitment": "confirmed", "encoding": "jsonParsed", "transactionDetails": "signatures" }
        ]
    });
    write.send(Message::Text(subscribe_msg.to_string())).await?;

    // Wait for subscription confirmation
    if let Some(Ok(Message::Text(text))) = timeout(Duration::from_secs(10), read.next()).await? {
        let sub_resp: SubscriptionResult = serde_json::from_str(&text)?;
        if let Some(err) = sub_resp.error {
            anyhow::bail!("Subscription error: {:?}", err);
        }
        info!("Subscription confirmed: {:?}", sub_resp.result);
    } else {
        anyhow::bail!("Subscription confirmation timeout");
    }

    // Main message loop
    let mut last_ping = tokio::time::Instant::now();
    loop {
        let msg = timeout(Duration::from_secs(60), read.next()).await?;
        match msg {
            Some(Ok(Message::Text(text))) => {
                // Handle transaction notification
                if let Ok(notif) = serde_json::from_str::<TransactionNotification>(&text) {
                    let sig = &notif.params.result.signature;
                    let slot = notif.params.result.slot;
                    let timestamp = notif.params.result.timestamp;

                    debug!("Received signature: {}", sig);
                    if let Err(e) = insert_signature(pool, sig, slot, timestamp).await {
                        error!("Failed to insert signature {}: {}", sig, e);
                    }
                } else {
                    // Could be a ping or other message
                    debug!("Non-transaction message: {}", text);
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
        }

        // Keepalive: send ping if idle for >20s
        if last_ping.elapsed() > Duration::from_secs(20) {
            write.send(Message::Ping(vec![])).await?;
            last_ping = tokio::time::Instant::now();
        }
    }

    Ok(())
}

async fn insert_signature(
    pool: &Pool<PostgresConnectionManager<NoTls>>,
    signature: &str,
    slot: u64,
    timestamp: Option<i64>,
) -> Result<()> {
    let conn = pool.get().await?;
    let block_time = timestamp.map(|ts| chrono::DateTime::from_timestamp(ts, 0));
    // Use ON CONFLICT DO NOTHING to ignore duplicates
    conn.execute(
        "INSERT INTO raw_signatures (signature, slot, block_time)
         VALUES ($1, $2, $3)
         ON CONFLICT (signature) DO NOTHING",
        &[&signature, &(slot as i64), &block_time],
    )
    .await?;
    Ok(())
}
