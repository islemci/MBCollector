# MoneroBar Collector

MoneroBar Collector fetches Monero network and pool statistics, normalizes hashrates from multiple pool APIs, and stores the aggregated payload in Upstash Redis.

## What It Collects

- Network height and difficulty from multiple Monero nodes
- Calculated network hashrate
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

The collector runs immediately and then repeats every 30 seconds.
