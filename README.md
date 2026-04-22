# MoneroBar Collector

MoneroBar Collector fetches Monero network and pool statistics, normalizes hashrates from multiple pool APIs, and stores the aggregated payload in Redis via `ioredis`.

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
- Latest 100 Monero block headers from the Mullvad daemon stored in Redis key `monero:explorer` (headers-only mode), including:
	- height
	- hash
	- timestamp (unix)
	- difficulty
	- reward
	- num_txes (as tx count)
	- block_weight
	- prev_hash
	- nonce
	- normalized header payload + range metadata (`startHeight`, `latestHeight`, `count`) and source node URL
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
- Redis database accessible from the collector host

## Environment Variables

Create a `.env` file with:

```env
REDIS_URL=redis://localhost:6379
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
- `monero:explorer`: latest 100 blocks (headers-only, no tx decoding)
- `monero:info`: CoinGecko Monero asset + market snapshot
