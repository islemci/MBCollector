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

type NodeConfig = {
    name: string;
    url: string;
};

const NODES = [
    { name: 'SethForPrivacy', url: 'https://node.sethforprivacy.com/get_info' },
    { name: 'SupportXMR', url: 'http://xmr.support:18081/get_info' },
    { name: '0xRPC', url: 'https://xmr.0xrpc.io/get_info' },
    { name: 'MoneroNodeOrg', url: 'http://moneronode.org:18081/get_info' },
    { name: 'Mullvad', url: 'http://monero.mullvad.net:18081/get_info' },
    { name: 'SethForPrivacyMirror', url: 'https://node.sethforprivacy.com/get_info' },
    { name: 'MonyST', url: 'http://mony.st:18081/get_info' },
    { name: 'NackCafe', url: 'http://xmr.nack.cafe:18081/get_info' }
] as const satisfies ReadonlyArray<NodeConfig>;

type NodeInfo = {
    height?: number;
    difficulty?: number;
};

type NodeMetric = {
    name: string;
    url: string;
    status: 'online' | 'offline';
    pingMs: number | null;
    height: number | null;
    difficulty: number | null;
};

type NetworkConsensus = {
    height: number;
    difficulty: number;
};

function getMajorityNetworkTruth(nodes: NodeMetric[]): NetworkConsensus {
    const validNodes = nodes.filter(
        (n): n is NodeMetric & { height: number; difficulty: number } =>
            typeof n.height === 'number' && typeof n.difficulty === 'number'
    );

    if (validNodes.length === 0) {
        return { height: 0, difficulty: 0 };
    }

    const pairCounts = new Map<string, { count: number; height: number; difficulty: number }>();

    for (const node of validNodes) {
        const key = `${node.height}:${node.difficulty}`;
        const current = pairCounts.get(key);

        if (current) {
            current.count += 1;
        } else {
            pairCounts.set(key, {
                count: 1,
                height: node.height,
                difficulty: node.difficulty
            });
        }
    }

    let winner: { count: number; height: number; difficulty: number } | null = null;

    for (const value of pairCounts.values()) {
        if (!winner) {
            winner = value;
            continue;
        }

        if (value.count > winner.count) {
            winner = value;
            continue;
        }

        // Tie-breaker: prefer the higher chain height, then higher difficulty.
        if (value.count === winner.count) {
            if (value.height > winner.height) {
                winner = value;
            } else if (value.height === winner.height && value.difficulty > winner.difficulty) {
                winner = value;
            }
        }
    }

    return {
        height: winner?.height ?? 0,
        difficulty: winner?.difficulty ?? 0
    };
}

function roundUpHashrate(value: number): number {
    if (!Number.isFinite(value) || value <= 0) return 0;
    return Math.ceil(value);
}

async function aggregate() {
    console.log(`[${new Date().toISOString()}] Starting aggregation...`);

    // Parallel fetch with 5-second timeout using native AbortSignal
    const fetchWithTimeout = <T>(url: string): Promise<T | null> =>
        fetch(url, { signal: AbortSignal.timeout(5000) })
            .then(res => res.json() as Promise<T>)
            .catch(() => null);

    const fetchNodeWithMetrics = async (node: NodeConfig): Promise<NodeMetric> => {
        const startedAt = Date.now();

        try {
            const response = await fetch(node.url, { signal: AbortSignal.timeout(5000) });
            const data = await response.json() as NodeInfo;
            const pingMs = Date.now() - startedAt;

            return {
                name: node.name,
                url: node.url,
                status: 'online',
                pingMs,
                height: typeof data?.height === 'number' ? data.height : null,
                difficulty: typeof data?.difficulty === 'number' ? data.difficulty : null
            };
        } catch {
            return {
                name: node.name,
                url: node.url,
                status: 'offline',
                pingMs: null,
                height: null,
                difficulty: null
            };
        }
    };

    const [poolResults, nodeResults] = await Promise.all([
        Promise.all(POOLS.map(p => fetchWithTimeout<any>(p.url))),
        Promise.all(NODES.map(node => fetchNodeWithMetrics(node)))
    ]);

    // Logic: Consensus & Normalization
    const consensus = getMajorityNetworkTruth(nodeResults);
    const bestHeight = consensus.height;
    const difficulty = consensus.difficulty;

    const networkHashrate = roundUpHashrate(difficulty / 120);

    for (const node of nodeResults) {
        console.log(
            `[Node] ${node.name} | ${node.status} | ping=${node.pingMs ?? 'timeout'}ms | height=${node.height ?? '-'} | difficulty=${node.difficulty ?? '-'}`
        );
    }

    const payload = {
        network: {
            height: bestHeight,
            hashrate: networkHashrate,
            difficulty: difficulty
        },
        nodes: nodeResults,
        pools: POOLS.map((p, i) => ({
            name: p.name,
            hashrate: poolResults[i] ? roundUpHashrate(extractHash(p.name, poolResults[i])) : 0,
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