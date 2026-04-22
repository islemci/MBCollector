// No imports needed for fetch in Bun!
import { Redis } from "@upstash/redis";

const redis = new Redis({
    url: Bun.env.UPSTASH_REDIS_REST_URL!,
    token: Bun.env.UPSTASH_REDIS_REST_TOKEN!,
});

const MONERO_JSON_RPC_URL = 'http://monero.mullvad.net:18081/json_rpc';
const BLOCKS_TO_COLLECT = 10;
const BLOCK_RPC_TIMEOUT_MS = 7000;
const BLOCK_RPC_MAX_RETRIES = 2;

let lastBlocksHeightFetched: number | null = null;

const POOLS = [
    { name: 'SupportXMR', url: 'https://www.supportxmr.com/api/pool/stats', homeUrl: 'https://www.supportxmr.com' },
    { name: 'NanoPool', url: 'https://api.nanopool.org/v1/xmr/pool/hashrate', homeUrl: 'https://xmr.nanopool.org/' },
    { name: 'P2Pool', url: 'https://p2pool.io/api/pool/stats', homeUrl: 'https://p2pool.io' },
    { name: 'Hashvault', url: 'https://api.hashvault.pro/v3/monero', homeUrl: 'https://hashvault.pro/monero/' },
    { name: 'C3Pool', url: 'https://api.c3pool.org/pool/stats', homeUrl: 'https://c3pool.org' },
    { name: 'MoneroOcean', url: 'https://api.moneroocean.stream/pool/stats', homeUrl: 'https://moneroocean.stream' },
    { name: 'SkyPool', url: 'https://api.skypool.xyz/pool/stats', homeUrl: 'https://pool.skypool.xyz/search/cpu' },
    { name: 'XMRPoolEU', url: 'https://web.xmrpool.eu:8119/stats', homeUrl: 'https://xmrpool.eu' },
    { name: 'Monerod', url: 'https://np-api.monerod.org/pool/stats', homeUrl: 'https://monerod.org' }
];

type NodeConfig = {
    name: string;
    url: string;
};

const NODES = [
    /* { name: 'SethForPrivacy', url: 'https://node.sethforprivacy.com/get_info' }, To be added later as they currently block requests */
    { name: 'SupportXMR', url: 'http://xmr.support:18081/get_info' },
    { name: '0xRPC', url: 'https://xmr.0xrpc.io/get_info' },
    { name: 'MoneroNodeOrg', url: 'http://moneronode.org:18081/get_info' },
    { name: 'Mullvad', url: 'http://monero.mullvad.net:18081/get_info' },
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

type CoinGeckoMoneroResponse = {
    id?: string;
    symbol?: string;
    name?: string;
    hashing_algorithm?: string | null;
    block_time_in_minutes?: number | null;
    genesis_date?: string | null;
    sentiment_votes_up_percentage?: number | null;
    sentiment_votes_down_percentage?: number | null;
    watchlist_portfolio_users?: number | null;
    market_cap_rank?: number | null;
    image?: {
        thumb?: string;
        small?: string;
        large?: string;
    };
    links?: {
        homepage?: string[];
        subreddit_url?: string | null;
        repos_url?: {
            github?: string[];
        };
    };
    market_data?: {
        current_price?: { usd?: number };
        total_volume?: { usd?: number };
        market_cap?: { usd?: number };
        low_24h?: { usd?: number };
        high_24h?: { usd?: number };
        ath?: { usd?: number };
        atl?: { usd?: number };
        ath_change_percentage?: { usd?: number };
        atl_change_percentage?: { usd?: number };
        circulating_supply?: number;
        total_supply?: number;
        max_supply?: number | null;
        max_supply_infinite?: boolean;
        price_change_24h?: number;
        price_change_percentage_24h?: number;
        price_change_percentage_7d?: number;
        price_change_percentage_30d?: number;
        price_change_percentage_1y?: number;
        market_cap_change_24h?: number;
        market_cap_change_percentage_24h?: number;
        last_updated?: string;
    };
    last_updated?: string;
};

type MoneroBlockHeaderRpc = {
    depth?: number;
    difficulty?: number;
    hash?: string;
    height?: number;
    num_txes?: number;
    orphan_status?: boolean;
    reward?: number;
    timestamp?: number;
    cumulative_difficulty?: number;
};

type MoneroGetBlockHeadersRangeResponse = {
    result?: {
        headers?: MoneroBlockHeaderRpc[];
        status?: string;
    };
    error?: {
        code?: number;
        message?: string;
    };
};

type NormalizedMoneroBlockHeader = {
    height: number | null;
    timestamp: number | null;
    difficulty: number | null;
    reward: number | null;
    numTxes: number | null;
    hash: string | null;
    orphanStatus: boolean | null;
    depth: number | null;
    cumulativeDifficulty: number | null;
};

function toFiniteNumber(value: unknown): number | null {
    if (typeof value !== 'number' || !Number.isFinite(value)) {
        return null;
    }

    return value;
}

function buildMoneroInfoPayload(data: CoinGeckoMoneroResponse) {
    const market = data.market_data;

    return {
        asset: {
            id: data.id ?? 'monero',
            symbol: data.symbol ?? 'xmr',
            name: data.name ?? 'Monero',
            hashingAlgorithm: data.hashing_algorithm ?? null,
            blockTimeMinutes: toFiniteNumber(data.block_time_in_minutes),
            genesisDate: data.genesis_date ?? null,
            marketCapRank: toFiniteNumber(data.market_cap_rank),
            image: {
                thumb: data.image?.thumb ?? null,
                small: data.image?.small ?? null,
                large: data.image?.large ?? null
            },
            links: {
                homepage: data.links?.homepage?.[0] ?? null,
                subreddit: data.links?.subreddit_url ?? null,
                github: data.links?.repos_url?.github?.[0] ?? null
            }
        },
        market: {
            priceUsd: toFiniteNumber(market?.current_price?.usd),
            totalVolumeUsd: toFiniteNumber(market?.total_volume?.usd),
            marketCapUsd: toFiniteNumber(market?.market_cap?.usd),
            low24hUsd: toFiniteNumber(market?.low_24h?.usd),
            high24hUsd: toFiniteNumber(market?.high_24h?.usd),
            priceChange24hUsd: toFiniteNumber(market?.price_change_24h),
            priceChange24hPct: toFiniteNumber(market?.price_change_percentage_24h),
            priceChange7dPct: toFiniteNumber(market?.price_change_percentage_7d),
            priceChange30dPct: toFiniteNumber(market?.price_change_percentage_30d),
            priceChange1yPct: toFiniteNumber(market?.price_change_percentage_1y),
            marketCapChange24hUsd: toFiniteNumber(market?.market_cap_change_24h),
            marketCapChange24hPct: toFiniteNumber(market?.market_cap_change_percentage_24h),
            athUsd: toFiniteNumber(market?.ath?.usd),
            athChangePct: toFiniteNumber(market?.ath_change_percentage?.usd),
            atlUsd: toFiniteNumber(market?.atl?.usd),
            atlChangePct: toFiniteNumber(market?.atl_change_percentage?.usd)
        },
        supply: {
            circulating: toFiniteNumber(market?.circulating_supply),
            total: toFiniteNumber(market?.total_supply),
            max: toFiniteNumber(market?.max_supply),
            maxSupplyInfinite: market?.max_supply_infinite ?? null
        },
        sentiment: {
            upVotesPct: toFiniteNumber(data.sentiment_votes_up_percentage),
            downVotesPct: toFiniteNumber(data.sentiment_votes_down_percentage),
            watchlistUsers: toFiniteNumber(data.watchlist_portfolio_users)
        },
        source: 'coingecko',
        sourceUpdatedAt: market?.last_updated ?? data.last_updated ?? null,
        updatedAt: Date.now()
    };
}

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

function toFiniteInteger(value: unknown): number | null {
    if (typeof value !== 'number' || !Number.isFinite(value)) {
        return null;
    }

    return Math.trunc(value);
}

function normalizeBlockHeader(header: MoneroBlockHeaderRpc): NormalizedMoneroBlockHeader {
    return {
        height: toFiniteInteger(header.height),
        timestamp: toFiniteInteger(header.timestamp),
        difficulty: toFiniteInteger(header.difficulty),
        reward: toFiniteInteger(header.reward),
        numTxes: toFiniteInteger(header.num_txes),
        hash: typeof header.hash === 'string' && header.hash.length > 0 ? header.hash : null,
        orphanStatus: typeof header.orphan_status === 'boolean' ? header.orphan_status : null,
        depth: toFiniteInteger(header.depth),
        cumulativeDifficulty: toFiniteInteger(header.cumulative_difficulty)
    };
}

async function sleepMs(ms: number): Promise<void> {
    await new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchLatestBlockHeadersRange(startHeight: number, endHeight: number): Promise<NormalizedMoneroBlockHeader[] | null> {
    for (let attempt = 0; attempt <= BLOCK_RPC_MAX_RETRIES; attempt += 1) {
        try {
            const response = await fetch(MONERO_JSON_RPC_URL, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    jsonrpc: '2.0',
                    id: '0',
                    method: 'get_block_headers_range',
                    params: {
                        start_height: startHeight,
                        end_height: endHeight
                    }
                }),
                signal: AbortSignal.timeout(BLOCK_RPC_TIMEOUT_MS)
            });

            if (!response.ok) {
                throw new Error(`RPC HTTP ${response.status}`);
            }

            const rpcPayload = await response.json() as MoneroGetBlockHeadersRangeResponse;

            if (rpcPayload.error) {
                throw new Error(`RPC error ${rpcPayload.error.code ?? 'unknown'}: ${rpcPayload.error.message ?? 'unknown'}`);
            }

            const headers = rpcPayload.result?.headers;

            if (!Array.isArray(headers)) {
                throw new Error('RPC response missing headers array');
            }

            const normalized = headers.map(normalizeBlockHeader);
            return normalized;
        } catch (error) {
            const isLastAttempt = attempt === BLOCK_RPC_MAX_RETRIES;

            if (isLastAttempt) {
                console.log(`Failed to fetch block headers after retries: ${String(error)}`);
                return null;
            }

            const backoffMs = 300 * (2 ** attempt);
            console.log(`Block header fetch failed (attempt ${attempt + 1}), retrying in ${backoffMs}ms...`);
            await sleepMs(backoffMs);
        }
    }

    return null;
}

async function collectRecentBlocks(latestHeight: number) {
    if (!Number.isFinite(latestHeight) || latestHeight <= 0) {
        console.log('Skipping monero:blocks update, invalid latest height.');
        return;
    }

    // get_info height can represent chain length, while header RPC expects top block index.
    const latestBlockHeight = Math.max(0, latestHeight - 1);

    if (lastBlocksHeightFetched === latestBlockHeight) {
        console.log(`Skipping monero:blocks update, height unchanged at ${latestBlockHeight}.`);
        return;
    }

    const startHeight = Math.max(0, latestBlockHeight - (BLOCKS_TO_COLLECT - 1));
    const expectedCount = latestBlockHeight - startHeight + 1;

    const headers = await fetchLatestBlockHeadersRange(startHeight, latestBlockHeight);

    if (!headers) {
        return;
    }

    if (headers.length !== expectedCount) {
        console.log(
            `Skipping monero:blocks update, partial response (${headers.length}/${expectedCount} headers).`
        );
        return;
    }

    const hasRequiredFields = headers.every(h => h.height !== null && h.timestamp !== null && h.hash !== null);
    if (!hasRequiredFields) {
        console.log('Skipping monero:blocks update, one or more headers are missing required fields.');
        return;
    }

    const blocks = [...headers].sort((a, b) => (b.height ?? 0) - (a.height ?? 0));

    await redis.set('monero:blocks', {
        range: {
            startHeight,
            latestHeight: latestBlockHeight,
            count: blocks.length
        },
        blocks,
        node: MONERO_JSON_RPC_URL,
        updatedAt: Date.now()
    });

    lastBlocksHeightFetched = latestBlockHeight;
    console.log(`Done. Updated monero:blocks (${blocks.length} headers).`);
}

const fetchWithTimeout = <T>(url: string): Promise<T | null> =>
    fetch(url, { signal: AbortSignal.timeout(5000) })
        .then(res => res.json() as Promise<T>)
        .catch(() => null);

async function aggregate() {
    console.log(`[${new Date().toISOString()}] Starting aggregation...`);

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
            homeUrl: p.homeUrl,
            apiUrl: p.url,
            hashrate: poolResults[i] ? roundUpHashrate(extractHash(p.name, poolResults[i])) : 0,
            status: poolResults[i] ? 'online' : 'offline'
        })),
        updatedAt: Date.now()
    };

    // Push to Upstash
    await redis.set("monero:stats", payload);

    if (bestHeight > 0) {
        await collectRecentBlocks(bestHeight);
    } else {
        console.log('Skipping monero:blocks update, no consensus height available.');
    }

    console.log(`Done. Height: ${bestHeight}`);
}

async function collectMoneroInfo() {
    console.log(`[${new Date().toISOString()}] Fetching CoinGecko monero info...`);

    const coingeckoData = await fetchWithTimeout<CoinGeckoMoneroResponse>(
        'https://api.coingecko.com/api/v3/coins/monero?localization=false&tickers=false&community_data=false&developer_data=false&sparkline=false'
    );

    if (!coingeckoData) {
        console.log('CoinGecko fetch failed. Keeping last monero:info payload.');
        return;
    }

    const moneroInfo = buildMoneroInfoPayload(coingeckoData);
    await redis.set("monero:info", moneroInfo);

    console.log('Done. Updated monero:info.');
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

setInterval(aggregate, 90000);
setInterval(collectMoneroInfo, 300000);

aggregate();
collectMoneroInfo();