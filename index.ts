// No imports needed for fetch in Bun!
import { Redis } from "@upstash/redis";

const redis = new Redis({
    url: Bun.env.UPSTASH_REDIS_REST_URL!,
    token: Bun.env.UPSTASH_REDIS_REST_TOKEN!,
});

const POOLS = [
    { name: 'SupportXMR', url: 'https://www.supportxmr.com/api/pool/stats' },
    { name: 'NanoPool', url: 'https://api.nanopool.org/v1/xmr/pool/hashrate' },
    { name: 'P2Pool', url: 'https://p2pool.io/api/pool/stats' },
    { name: 'Hashvault', url: 'https://api.hashvault.pro/v3/monero' },
    { name: 'C3Pool', url: 'https://api.c3pool.org/pool/stats' },
    { name: 'MoneroOcean', url: 'https://api.moneroocean.stream/pool/stats' },
    { name: 'SkyPool', url: 'https://api.skypool.xyz/pool/stats' },
    { name: 'XMRPoolEU', url: 'https://web.xmrpool.eu:8119/stats' },
    { name: 'Monerod', url: 'https://np-api.monerod.org/pool/stats' }
];

const NODES = [
    'https://node.sethforprivacy.com/get_info',
    'http://xmr.support:18081/get_info',
    'https://xmr.0xrpc.io/get_info',
    'http://moneronode.org:18081/get_info',
    'http://monero.mullvad.net:18081/get_info',
    'https://node.sethforprivacy.com/get_info',
    'http://mony.st:18081/get_info',
    'http://xmr.nack.cafe:18081/get_info'
];

type NodeInfo = {
    height?: number;
    difficulty?: number;
};

async function aggregate() {
    console.log(`[${new Date().toISOString()}] Starting aggregation...`);

    // Parallel fetch with 5-second timeout using native AbortSignal
    const fetchWithTimeout = <T>(url: string): Promise<T | null> =>
        fetch(url, { signal: AbortSignal.timeout(5000) })
            .then(res => res.json() as Promise<T>)
            .catch(() => null);

    const [poolResults, nodeResults] = await Promise.all([
        Promise.all(POOLS.map(p => fetchWithTimeout<any>(p.url))),
        Promise.all(NODES.map(url => fetchWithTimeout<NodeInfo>(url)))
    ]);

    // Logic: Consensus & Normalization
    const validNodes = nodeResults.filter(
        (n): n is NodeInfo & { height: number } => typeof n?.height === 'number'
    );
    const bestHeight = Math.max(...validNodes.map(n => n.height), 0);
    const difficulty = validNodes.find(n => n.height === bestHeight)?.difficulty || 0;

    const networkHashrate = difficulty / 120;

    const payload = {
        network: {
            height: bestHeight,
            hashrate: networkHashrate,
            difficulty: difficulty
        },
        pools: POOLS.map((p, i) => ({
            name: p.name,
            hashrate: poolResults[i] ? extractHash(p.name, poolResults[i]) : 0,
            status: poolResults[i] ? 'online' : 'offline'
        })),
        updatedAt: Date.now()
    };

    // Push to Upstash
    await redis.set("monero:stats", payload);
    console.log(`Done. Height: ${bestHeight}`);
}

function extractHash(name: string, data: any): number {
    const toNumber = (value: unknown): number | null => {
        if (typeof value === 'number' && Number.isFinite(value)) return value;
        if (typeof value === 'string') {
            const parsed = Number(value);
            return Number.isFinite(parsed) ? parsed : null;
        }
        return null;
    };

    const firstNumber = (...values: unknown[]): number => {
        for (const value of values) {
            const parsed = toNumber(value);
            if (parsed !== null) return parsed;
        }
        return 0;
    };

    switch (name.toLowerCase()) {
        case 'supportxmr':
            return firstNumber(
                data?.pool_statistics?.hashRate,
                data?.pool_statistics?.hashrate,
                data?.hashRate,
                data?.hashrate
            );

        case 'nanopool':
            return firstNumber(data?.data, data?.hashRate, data?.hashrate);

        case 'hashvault':
            return firstNumber(
                data?.pool_statistics?.collective?.hashRate,
                data?.pool_statistics?.collective?.hashrate,
                data?.pool_statistics?.hashRate,
                data?.pool_statistics?.hashrate,
                data?.hashRate,
                data?.hashrate
            );

        case 'c3pool':
        case 'moneroocean':
        case 'skypool':
        case 'monerod':
            return firstNumber(
                data?.pool_statistics?.hashRate,
                data?.pool_statistics?.hashrate,
                data?.hashRate,
                data?.hashrate
            );

        case 'p2pool':
            return firstNumber(
                data?.pool_statistics?.hashRate,
                data?.pool_statistics?.hashrate,
                data?.hashRate,
                data?.hashrate,
                // Some sidechain payloads omit hashRate, but expose sidechainDifficulty.
                toNumber(data?.pool_statistics?.sidechainDifficulty) !== null
                    ? Number(data?.pool_statistics?.sidechainDifficulty) / 10
                    : null
            );

        case 'xmrpooleu':
            return firstNumber(
                data?.pool?.hashrate,
                data?.hashrate,
                data?.pool?.stats?.hashrate,
                data?.pool?.stats?.hashRate
            );

        default:
            return firstNumber(
                data?.pool_statistics?.collective?.hashRate,
                data?.pool_statistics?.hashRate,
                data?.pool_statistics?.hashrate,
                data?.pool?.hashrate,
                data?.hashRate,
                data?.hashrate,
                data?.data
            );
    }
}

// 30-second loop
setInterval(aggregate, 30000);
aggregate();