# Solana Meme Coin Insider Tracker – Ingest (Rust)

Connects to Helius Enhanced WebSocket (`transactionSubscribe`) and writes signature/slot/block_time into Postgres table `raw_signatures`.

## Railway Deployment

**Start Command**: `./ingest-rust` (binary built via Nixpacks)

**Environment Variables**:
- `HELIUS_WSS_URL` – Helius WebSocket endpoint with API key
- `DATABASE_URL` – Postgres connection string (provided by Railway)
- `RUST_LOG` – optional, e.g. `info`

**Pre‑deploy**: None.

**Health Check**: The process runs indefinitely; Railway will restart on exit.
