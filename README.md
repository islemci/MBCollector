# MoneroBar Collector

MoneroBar Collector fetches Monero network and pool statistics, normalizes hashrates from multiple pool APIs, and stores the aggregated payload in Upstash Redis.

## What It Collects

- Network height and difficulty from multiple Monero nodes
- Calculated network hashrate
- Latest 10 Monero block headers from JSON-RPC (`get_block_headers_range`) stored in Redis key `monero:blocks`, including:
	- height
	- timestamp (unix)
	- difficulty
	- reward
	- num_txes
	- hash
	- orphan_status
	- depth
	- cumulative_difficulty
- CoinGecko market + metadata snapshot stored in Redis key `monero:info`, including:
	- XMR price (USD)
	- 24h total volume (USD)
	- market cap (USD)
	- 24h price change percentage
	- 24h low/high (USD)
	- extra dashboard metrics (7d/30d/1y change, ATH/ATL, supply, sentiment, links)
- Pool hashrates and online/offline status for:
	- SupportXMR
	- NanoPool
	- P2Pool
	- Hashvault
	- C3Pool
	- MoneroOcean
	- SkyPool
	- XMRPoolEU
	- Monerod

## Requirements

- Bun
- Upstash Redis database

## Environment Variables

Create a `.env` file with:

```env
UPSTASH_REDIS_REST_URL=your_upstash_redis_url
UPSTASH_REDIS_REST_TOKEN=your_upstash_redis_token
```

## Install

```bash
bun install
```

## Run

```bash
bun run index.ts
```

The collector runs immediately and then repeats on separate schedules:

- `monero:stats` (network/pools): every 90 seconds
- `monero:info` (CoinGecko): every 5 minutes

## Redis Keys

- `monero:stats`: network + node + pool aggregate
- `monero:blocks`: last 10 block headers from Monero JSON-RPC
- `monero:info`: CoinGecko Monero asset + market snapshot
