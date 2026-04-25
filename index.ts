import Redis from "ioredis";

const redisUrl = Bun.env.REDIS_URL;

if (!redisUrl) {
    throw new Error('Missing REDIS_URL environment variable');
}

const redis = new Redis(redisUrl);

const MONERO_DAEMON_URL = 'http://monero.mullvad.net:18081';
const MONERO_DAEMON_NAME = 'Mullvad';
const MONERO_JSON_RPC_URL = `${MONERO_DAEMON_URL}/json_rpc`;
const BLOCKS_TO_COLLECT = 10;
const EXPLORER_BLOCKS_TO_COLLECT = 2500;
const TXS_PER_REQUEST = 100;
const BLOCK_FETCH_CONCURRENCY = 5;
const BLOCK_RPC_TIMEOUT_MS = 7000;
const BLOCK_RPC_MAX_RETRIES = 2;
const MAX_HEADERS_PER_RANGE_REQUEST = 500;

type LogLevel = 'INFO' | 'WARN' | 'ERROR';

function formatError(error: unknown): Record<string, unknown> {
    if (error instanceof Error) {
        return {
            name: error.name,
            message: error.message,
            stack: error.stack
        };
    }

    return {
        error: String(error)
    };
}

function log(level: LogLevel, message: string, context?: Record<string, unknown>): void {
    const timestamp = new Date().toISOString();
    const payload = context ? ` ${JSON.stringify(context)}` : '';
    const line = `[${timestamp}] [${level}] ${message}${payload}`;

    if (level === 'ERROR') {
        console.error(line);
        return;
    }

    if (level === 'WARN') {
        console.warn(line);
        return;
    }

    console.info(line);
}

function logInfo(message: string, context?: Record<string, unknown>): void {
    log('INFO', message, context);
}

function logWarn(message: string, context?: Record<string, unknown>): void {
    log('WARN', message, context);
}

function logError(message: string, error?: unknown, context?: Record<string, unknown>): void {
    log('ERROR', message, {
        ...context,
        ...(error === undefined ? {} : formatError(error))
    });
}

let lastBlocksHeightFetched: number | null = null;
let lastExplorerHeightFetched: number | null = null;

const POOLS = [
    { name: 'SupportXMR', url: 'https://www.supportxmr.com/api/pool/stats', homeUrl: 'https://www.supportxmr.com' },
    { name: 'NanoPool', url: 'https://api.nanopool.org/v1/xmr/pool/hashrate', homeUrl: 'https://xmr.nanopool.org/' },
    { name: 'P2Pool', url: 'https://p2pool.io/api/pool/stats', homeUrl: 'https://p2pool.io' },
    { name: 'Hashvault', url: 'https://api.hashvault.pro/v3/monero', homeUrl: 'https://hashvault.pro/monero/' },
    { name: 'C3Pool', url: 'https://api.c3pool.org/pool/stats', homeUrl: 'https://c3pool.org' },
    { name: 'MoneroOcean', url: 'https://api.moneroocean.stream/pool/stats', homeUrl: 'https://moneroocean.stream' },
    { name: 'SkyPool', url: 'https://api.skypool.xyz/pool/stats', homeUrl: 'https://pool.skypool.xyz/search/cpu' },
    { name: 'XMRPoolEU', url: 'https://web.xmrpool.eu:8119/stats', homeUrl: 'https://xmrpool.eu' },
    { name: 'Monerod', url: 'https://np-api.monerod.org/pool/stats', homeUrl: 'https://monerod.org' },
    { name: 'SoloPool', url: 'https://xmr.solopool.org/api/stats', homeUrl: 'https://xmr.solopool.org' },
    { name: 'HeroMiners', url: 'https://monero.herominers.com/api/stats', homeUrl: 'https://monero.herominers.com' }
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
    top_block_hash?: string;
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
    block_weight?: number;
    prev_hash?: string;
    nonce?: number;
    [key: string]: unknown;
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

type MoneroJsonRpcResponse<T> = {
    result?: T;
    error?: {
        code?: number;
        message?: string;
    };
};

type MoneroGetBlockCountResult = {
    count?: number;
    status?: string;
};

type MoneroGetBlockResult = {
    blob?: string;
    block_header?: MoneroBlockHeaderRpc;
    json?: string;
    miner_tx_hash?: string;
    status?: string;
    tx_hashes?: string[];
    untrusted?: boolean;
};

type MoneroGetTransactionsResponse = {
    txs?: Array<Record<string, unknown>>;
    missed_tx?: string[];
    status?: string;
    untrusted?: boolean;
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
    blockWeight: number | null;
    prevHash: string | null;
    nonce: number | null;
};

type MoneroBlockWithFinder = NormalizedMoneroBlockHeader & {
    finder: string | null;
};

type ExplorerBlockRecord = {
    height: number;
    header: NormalizedMoneroBlockHeader;
    hash: string | null;
    finder: string | null;
    difficulty: number | null;
    timestamp: number | null;
    reward: number | null;
    txCount: number;
    blockWeight: number | null;
    prevHash: string | null;
    nonce: number | null;
};

type MoneroBlockJson = {
    prev_id?: string;
    tx_hashes?: string[];
};

type ExplorerBlockDetailRecord = {
    height: number;
    hash: string | null;
    prevHash: string | null;
    timestamp: number | null;
    difficulty: number | null;
    reward: number | null;
    blockWeight: number | null;
    nonce: number | null;
    txCount: number;
    txHashes: string[];
    minerTxHash: string | null;
    finder: string | null;
    sourceNode: string;
    updatedAt: number;
};

type ExplorerSnapshot = {
    range?: {
        startHeight?: number;
        latestHeight?: number;
        count?: number;
    };
    blocks?: ExplorerBlockRecord[];
};

type PoolBlockAnnouncement = {
    hash: string | null;
    height: number | null;
    finder: string;
};

type SupportXmrBlockResponse = Array<{
    hash?: string;
    height?: number;
}>;

type NanoPoolBlocksResponse = {
    status?: boolean;
    data?: Array<{
        hash?: string;
        number?: number;
    }>;
};

type GenericPoolBlocksResponse = Array<{
    hash?: string;
    height?: number;
}>;

type XmrPoolEuStatsResponse = {
    pool?: {
        blocks?: unknown;
    };
};

type SoloPoolBlocksResponse = {
    matured?: Array<{
        hash?: string;
        height?: number;
    }>;
    immature?: Array<{
        hash?: string;
        height?: number;
    }> | null;
    candidates?: Array<{
        hash?: string;
        height?: number;
    }> | null;
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

function toFiniteIntegerLike(value: unknown): number | null {
    if (typeof value === 'number') {
        return Number.isFinite(value) ? Math.trunc(value) : null;
    }

    if (typeof value === 'string' && value.trim().length > 0) {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? Math.trunc(parsed) : null;
    }

    return null;
}

function toNonEmptyString(value: unknown): string | null {
    return typeof value === 'string' && value.length > 0 ? value : null;
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
        cumulativeDifficulty: toFiniteInteger(header.cumulative_difficulty),
        blockWeight: toFiniteInteger(header.block_weight),
        prevHash: typeof header.prev_hash === 'string' && header.prev_hash.length > 0 ? header.prev_hash : null,
        nonce: toFiniteInteger(header.nonce)
    };
}

function normalizePoolAnnouncement(hash: unknown, height: unknown, finder: string): PoolBlockAnnouncement | null {
    const normalizedHash = toNonEmptyString(hash);
    const normalizedHeight = toFiniteIntegerLike(height);

    if (!normalizedHash || normalizedHeight === null) {
        return null;
    }

    return {
        hash: normalizedHash,
        height: normalizedHeight,
        finder
    };
}

function filterAnnouncementsForTargetBlocks(
    announcements: Array<PoolBlockAnnouncement | null>,
    targetHeights: Set<number>,
    targetHashes: Set<string>
): PoolBlockAnnouncement[] {
    return announcements.filter((announcement): announcement is PoolBlockAnnouncement => {
        if (!announcement || !announcement.hash) {
            return false;
        }

        return targetHashes.has(announcement.hash)
            || (announcement.height !== null && targetHeights.has(announcement.height));
    });
}

async function setRedisJson(key: string, value: unknown): Promise<void> {
    await redis.set(key, JSON.stringify(value));
}

async function sleepMs(ms: number): Promise<void> {
    await new Promise(resolve => setTimeout(resolve, ms));
}

function parseJsonIfString(value: unknown): unknown | null {
    if (typeof value !== 'string') {
        return null;
    }

    try {
        return JSON.parse(value);
    } catch (error) {
        logWarn('Failed to parse JSON string value.', {
            valuePreview: value.slice(0, 200),
            ...formatError(error)
        });
        return null;
    }
}

function toStringArray(value: unknown): string[] {
    if (!Array.isArray(value)) {
        return [];
    }

    return value.filter((item): item is string => typeof item === 'string' && item.length > 0);
}

function uniqueStrings(values: string[]): string[] {
    return [...new Set(values)];
}

function chunkArray<T>(values: T[], chunkSize: number): T[][] {
    if (chunkSize <= 0) {
        return [values];
    }

    const chunks: T[][] = [];

    for (let i = 0; i < values.length; i += chunkSize) {
        chunks.push(values.slice(i, i + chunkSize));
    }

    return chunks;
}

function buildDescendingHeights(latestHeight: number, startHeight: number): number[] {
    const heights: number[] = [];

    for (let height = latestHeight; height >= startHeight; height -= 1) {
        heights.push(height);
    }

    return heights;
}

function parseExplorerSnapshot(value: string | null): ExplorerSnapshot | null {
    if (!value) {
        return null;
    }

    try {
        const parsed = JSON.parse(value) as ExplorerSnapshot;

        if (!parsed || typeof parsed !== 'object') {
            return null;
        }

        if (!Array.isArray(parsed.blocks)) {
            return null;
        }

        return parsed;
    } catch (error) {
        logWarn('Failed to parse existing explorer snapshot from Redis.', {
            valuePreview: value.slice(0, 200),
            ...formatError(error)
        });
        return null;
    }
}

function getMissingHeightRanges(startHeight: number, endHeight: number, presentHeights: Set<number>): Array<{ startHeight: number; endHeight: number }> {
    const ranges: Array<{ startHeight: number; endHeight: number }> = [];
    let cursor = startHeight;

    while (cursor <= endHeight) {
        if (presentHeights.has(cursor)) {
            cursor += 1;
            continue;
        }

        const rangeStart = cursor;
        while (cursor <= endHeight && !presentHeights.has(cursor)) {
            cursor += 1;
        }

        ranges.push({
            startHeight: rangeStart,
            endHeight: cursor - 1
        });
    }

    return ranges;
}

function splitHeightRange(startHeight: number, endHeight: number, maxPerRange: number): Array<{ startHeight: number; endHeight: number }> {
    const ranges: Array<{ startHeight: number; endHeight: number }> = [];

    if (maxPerRange <= 0 || endHeight < startHeight) {
        return ranges;
    }

    let cursor = startHeight;

    while (cursor <= endHeight) {
        const chunkEnd = Math.min(endHeight, cursor + maxPerRange - 1);
        ranges.push({
            startHeight: cursor,
            endHeight: chunkEnd
        });
        cursor = chunkEnd + 1;
    }

    return ranges;
}

function parseMoneroBlockJson(rawJson: unknown): MoneroBlockJson | null {
    if (typeof rawJson !== 'string' || rawJson.length === 0) {
        return null;
    }

    try {
        const parsed = JSON.parse(rawJson) as MoneroBlockJson;
        return parsed && typeof parsed === 'object' ? parsed : null;
    } catch (error) {
        logWarn('Failed to parse Monero block JSON payload.', {
            valuePreview: rawJson.slice(0, 200),
            ...formatError(error)
        });
        return null;
    }
}

function parseExplorerBlockDetail(value: string | null): ExplorerBlockDetailRecord | null {
    if (!value) {
        return null;
    }

    try {
        const parsed = JSON.parse(value) as ExplorerBlockDetailRecord;
        if (!parsed || typeof parsed !== 'object') {
            return null;
        }

        return {
            height: toFiniteInteger(parsed.height) ?? 0,
            hash: toNonEmptyString(parsed.hash),
            prevHash: toNonEmptyString(parsed.prevHash),
            timestamp: toFiniteInteger(parsed.timestamp),
            difficulty: toFiniteInteger(parsed.difficulty),
            reward: toFiniteInteger(parsed.reward),
            blockWeight: toFiniteInteger(parsed.blockWeight),
            nonce: toFiniteInteger(parsed.nonce),
            txCount: Math.max(0, toFiniteInteger(parsed.txCount) ?? 0),
            txHashes: uniqueStrings(toStringArray(parsed.txHashes)),
            minerTxHash: toNonEmptyString(parsed.minerTxHash),
            finder: toNonEmptyString(parsed.finder),
            sourceNode: toNonEmptyString(parsed.sourceNode) ?? MONERO_DAEMON_NAME,
            updatedAt: toFiniteInteger(parsed.updatedAt) ?? 0
        };
    } catch (error) {
        logWarn('Failed to parse explorer block detail record.', {
            valuePreview: value.slice(0, 200),
            ...formatError(error)
        });
        return null;
    }
}

async function mapWithConcurrency<T, R>(
    values: T[],
    concurrency: number,
    mapper: (value: T, index: number) => Promise<R>
): Promise<R[]> {
    if (values.length === 0) {
        return [];
    }

    const results = new Array<R>(values.length);
    const workerCount = Math.max(1, Math.min(concurrency, values.length));
    let cursor = 0;

    const workers = Array.from({ length: workerCount }, async () => {
        while (true) {
            const index = cursor;
            cursor += 1;

            if (index >= values.length) {
                return;
            }

            const currentValue = values[index];
            if (currentValue === undefined) {
                return;
            }

            results[index] = await mapper(currentValue, index);
        }
    });

    await Promise.all(workers);
    return results;
}

async function callMoneroJsonRpc<T>(method: string, params?: Record<string, unknown>): Promise<T | null> {
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
                    method,
                    params
                }),
                signal: AbortSignal.timeout(BLOCK_RPC_TIMEOUT_MS)
            });

            if (!response.ok) {
                throw new Error(`RPC HTTP ${response.status}`);
            }

            const rpcPayload = await response.json() as MoneroJsonRpcResponse<T>;

            if (rpcPayload.error) {
                throw new Error(`RPC error ${rpcPayload.error.code ?? 'unknown'}: ${rpcPayload.error.message ?? 'unknown'}`);
            }

            if (!rpcPayload.result) {
                throw new Error('RPC response missing result payload');
            }

            return rpcPayload.result;
        } catch (error) {
            const isLastAttempt = attempt === BLOCK_RPC_MAX_RETRIES;

            if (isLastAttempt) {
                logError('RPC call failed.', error, { method, params, attempt: attempt + 1 });
                return null;
            }

            const backoffMs = 300 * (2 ** attempt);
            logWarn('RPC call failed, retrying.', {
                method,
                params,
                attempt: attempt + 1,
                nextRetryInMs: backoffMs,
                ...formatError(error)
            });
            await sleepMs(backoffMs);
        }
    }

    return null;
}

async function callMoneroEndpoint<T>(path: string, body: Record<string, unknown>): Promise<T | null> {
    for (let attempt = 0; attempt <= BLOCK_RPC_MAX_RETRIES; attempt += 1) {
        try {
            const response = await fetch(`${MONERO_DAEMON_URL}${path}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(body),
                signal: AbortSignal.timeout(BLOCK_RPC_TIMEOUT_MS)
            });

            if (!response.ok) {
                throw new Error(`Daemon HTTP ${response.status}`);
            }

            return await response.json() as T;
        } catch (error) {
            const isLastAttempt = attempt === BLOCK_RPC_MAX_RETRIES;

            if (isLastAttempt) {
                logError('Daemon call failed.', error, { path, body, attempt: attempt + 1 });
                return null;
            }

            const backoffMs = 300 * (2 ** attempt);
            logWarn('Daemon call failed, retrying.', {
                path,
                body,
                attempt: attempt + 1,
                nextRetryInMs: backoffMs,
                ...formatError(error)
            });
            await sleepMs(backoffMs);
        }
    }

    return null;
}

async function fetchLatestBlockHeadersRange(startHeight: number, endHeight: number): Promise<NormalizedMoneroBlockHeader[] | null> {
    const rpcPayload = await callMoneroJsonRpc<MoneroGetBlockHeadersRangeResponse['result']>('get_block_headers_range', {
        start_height: startHeight,
        end_height: endHeight
    });

    const headers = rpcPayload?.headers;

    if (!Array.isArray(headers)) {
        logError('RPC response missing headers array for get_block_headers_range.', undefined, {
            startHeight,
            endHeight,
            rpcPayload
        });
        return null;
    }

    return headers.map(normalizeBlockHeader);
}

async function fetchLatestBlockHeightFromMullvad(): Promise<number | null> {
    const info = await callMoneroEndpoint<NodeInfo>('/get_info', {});
    const height = toFiniteInteger(info?.height);

    if (height === null || height <= 0) {
        return null;
    }

    return Math.max(0, height - 1);
}

async function fetchFullBlockByHeight(height: number): Promise<MoneroGetBlockResult | null> {
    return callMoneroJsonRpc<MoneroGetBlockResult>('get_block', { height });
}

function buildExplorerBlockDetailRecord(
    block: ExplorerBlockRecord,
    txHashes: string[],
    minerTxHash: string | null,
    updatedAt: number
): ExplorerBlockDetailRecord {
    return {
        height: block.height,
        hash: block.hash,
        prevHash: block.prevHash,
        timestamp: block.timestamp,
        difficulty: block.difficulty,
        reward: block.reward,
        blockWeight: block.blockWeight,
        nonce: block.nonce,
        txCount: block.txCount,
        txHashes: uniqueStrings(txHashes),
        minerTxHash,
        finder: block.finder,
        sourceNode: MONERO_DAEMON_NAME,
        updatedAt
    };
}

async function syncExplorerBlockDetails(blocks: ExplorerBlockRecord[]): Promise<void> {
    if (blocks.length === 0) {
        return;
    }

    const detailKeys = blocks.map(block => `monero:block-detail:${block.height}`);
    const existingRaw = await redis.mget(...detailKeys);
    const existingByHeight = new Map<number, ExplorerBlockDetailRecord>();

    existingRaw.forEach((value, index) => {
        const parsed = parseExplorerBlockDetail(value);
        const block = blocks[index];
        if (parsed) {
            if (block) {
                existingByHeight.set(block.height, parsed);
            }
        }
    });

    const fetchTargets = blocks.filter(block => {
        const existing = existingByHeight.get(block.height);

        if (!existing) {
            return true;
        }

        if (existing.hash !== block.hash) {
            return true;
        }

        if (existing.txHashes.length !== block.txCount) {
            return true;
        }

        if (!existing.minerTxHash) {
            return true;
        }

        return false;
    });

    if (fetchTargets.length > 0) {
        logInfo('Explorer sync fetching block detail payloads.', {
            requestedCount: fetchTargets.length,
            concurrency: BLOCK_FETCH_CONCURRENCY
        });
    }

    const fetchedDetails = new Map<number, { txHashes: string[]; minerTxHash: string | null }>();
    const failedHeights: number[] = [];

    const fetchedResults = await mapWithConcurrency(fetchTargets, BLOCK_FETCH_CONCURRENCY, async (block) => {
        try {
            const rpcPayload = await fetchFullBlockByHeight(block.height);
            if (!rpcPayload) {
                throw new Error(`Missing block payload for height ${block.height}`);
            }

            const parsedBlockJson = parseMoneroBlockJson(rpcPayload.json);
            const txHashes = uniqueStrings([
                ...toStringArray(rpcPayload.tx_hashes),
                ...toStringArray(parsedBlockJson?.tx_hashes)
            ]);
            const minerTxHash = toNonEmptyString(rpcPayload.miner_tx_hash);

            return {
                height: block.height,
                txHashes,
                minerTxHash,
                ok: true as const
            };
        } catch (error) {
            logWarn('Failed to fetch explorer block detail for one block.', {
                height: block.height,
                ...formatError(error)
            });

            return {
                height: block.height,
                txHashes: [],
                minerTxHash: null,
                ok: false as const
            };
        }
    });

    for (const detail of fetchedResults) {
        if (detail.ok) {
            fetchedDetails.set(detail.height, {
                txHashes: detail.txHashes,
                minerTxHash: detail.minerTxHash
            });
            continue;
        }

        failedHeights.push(detail.height);
    }

    const updatedAt = Date.now();
    const pipeline = redis.pipeline();
    let writes = 0;
    let commandCount = 0;

    for (const block of blocks) {
        const existing = existingByHeight.get(block.height);
        const fetched = fetchedDetails.get(block.height);
        const hasFreshDetail = fetched !== undefined;
        const canReuseExisting = existing?.hash === block.hash;

        if (!hasFreshDetail && !canReuseExisting) {
            if (block.hash) {
                pipeline.set(`monero:block-detail-hash:${block.hash}`, String(block.height));
                commandCount += 1;
            }
            continue;
        }

        const currentTxHashes = fetched?.txHashes ?? (existing?.hash === block.hash ? existing.txHashes : []);
        const currentMinerTxHash = fetched?.minerTxHash ?? (existing?.hash === block.hash ? existing.minerTxHash : null);
        const nextRecord = buildExplorerBlockDetailRecord(block, currentTxHashes, currentMinerTxHash, updatedAt);

        if (existing?.hash && existing.hash !== nextRecord.hash) {
            pipeline.del(`monero:block-detail-hash:${existing.hash}`);
            commandCount += 1;
        }

        if (JSON.stringify(existing) !== JSON.stringify(nextRecord)) {
            pipeline.set(`monero:block-detail:${block.height}`, JSON.stringify(nextRecord));
            writes += 1;
            commandCount += 1;
        }

        if (nextRecord.hash) {
            pipeline.set(`monero:block-detail-hash:${nextRecord.hash}`, String(block.height));
            commandCount += 1;
        }
    }

    if (writes > 0) {
        await pipeline.exec();
        logInfo('Updated explorer block detail cache.', {
            blockCount: blocks.length,
            writes
        });
        return;
    }

    if (commandCount > 0) {
        await pipeline.exec();
    }

    if (failedHeights.length > 0) {
        logWarn('Explorer block detail sync completed with partial failures.', {
            failedCount: failedHeights.length,
            failedHeights: failedHeights.slice(0, 20)
        });
    }
}

async function collectExplorerBlocks() {
    const startedAt = Date.now();
    logInfo('Explorer sync started.');

    const latestBlockHeight = await fetchLatestBlockHeightFromMullvad();

    if (latestBlockHeight === null) {
        logWarn('Skipping monero:explorer update because Mullvad latest height is unavailable.');
        return;
    }

    if (lastExplorerHeightFetched === latestBlockHeight) {
        logInfo('Skipping monero:explorer update because height is unchanged.', {
            latestBlockHeight
        });
    }

    const targetStartHeight = Math.max(0, latestBlockHeight - (EXPLORER_BLOCKS_TO_COLLECT - 1));
    const targetEndHeight = latestBlockHeight;

    const existingRaw = await redis.get('monero:explorer');
    const existingSnapshot = parseExplorerSnapshot(existingRaw);
    const mergedByHeight = new Map<number, NormalizedMoneroBlockHeader>();

    for (const block of existingSnapshot?.blocks ?? []) {
        const height = typeof block?.height === 'number' ? block.height : null;
        if (height === null || height < targetStartHeight || height > targetEndHeight) {
            continue;
        }

        const normalizedHeader = block.header ?? {
            height,
            timestamp: toFiniteInteger(block.timestamp),
            difficulty: toFiniteInteger(block.difficulty),
            reward: toFiniteInteger(block.reward),
            numTxes: toFiniteInteger(block.txCount),
            hash: typeof block.hash === 'string' ? block.hash : null,
            orphanStatus: null,
            depth: null,
            cumulativeDifficulty: null,
            blockWeight: toFiniteInteger(block.blockWeight),
            prevHash: typeof block.prevHash === 'string' ? block.prevHash : null,
            nonce: toFiniteInteger(block.nonce)
        };

        mergedByHeight.set(height, {
            ...normalizedHeader,
            height
        });
    }

    const presentHeights = new Set<number>(mergedByHeight.keys());
    const missingRanges = getMissingHeightRanges(targetStartHeight, targetEndHeight, presentHeights);

    if (missingRanges.length > 0) {
        logInfo('Explorer sync found missing ranges to backfill.', {
            missingRangeCount: missingRanges.length,
            missingHeaderCount: missingRanges.reduce((acc, r) => acc + (r.endHeight - r.startHeight + 1), 0)
        });
    }

    for (const range of missingRanges) {
        const chunkedRanges = splitHeightRange(range.startHeight, range.endHeight, MAX_HEADERS_PER_RANGE_REQUEST);

        for (const chunk of chunkedRanges) {
            const headers = await fetchLatestBlockHeadersRange(chunk.startHeight, chunk.endHeight);
            if (!headers) {
                logError('Skipping monero:explorer update because a missing range fetch failed.', undefined, {
                    startHeight: chunk.startHeight,
                    endHeight: chunk.endHeight
                });
                return;
            }

            for (const header of headers) {
                if (header.height !== null && header.height >= targetStartHeight && header.height <= targetEndHeight) {
                    mergedByHeight.set(header.height, header);
                }
            }
        }
    }

    const headersToStore = [...mergedByHeight.values()]
        .sort((a, b) => (b.height ?? 0) - (a.height ?? 0))
        .slice(0, EXPLORER_BLOCKS_TO_COLLECT);

    const blocksToStore: ExplorerBlockRecord[] = headersToStore
        .filter((header): header is NormalizedMoneroBlockHeader & { height: number } => header.height !== null)
        .map(header => ({
            height: header.height,
            header,
            hash: header.hash,
            finder: null,
            timestamp: header.timestamp,
            difficulty: header.difficulty,
            reward: header.reward,
            txCount: header.numTxes ?? 0,
            blockWeight: header.blockWeight,
            prevHash: header.prevHash,
            nonce: header.nonce
        }));

    const explorerFinderByHash = await fetchRecentBlockFinders(headersToStore);
    for (const block of blocksToStore) {
        block.finder = block.hash ? (explorerFinderByHash.get(block.hash) ?? null) : null;
    }

    try {
        await syncExplorerBlockDetails(blocksToStore);
    } catch (error) {
        logWarn('Continuing explorer snapshot update without refreshed block details.', formatError(error));
    }

    const startHeight = blocksToStore.at(-1)?.height ?? targetStartHeight;
    const collectedTxCount = blocksToStore.reduce((sum, block) => sum + block.txCount, 0);

    await setRedisJson('monero:explorer', {
        range: {
            startHeight,
            latestHeight: latestBlockHeight,
            count: blocksToStore.length
        },
        node: MONERO_DAEMON_NAME,
        blocks: blocksToStore,
        collectedTxCount,
        mode: 'headers-only',
        updatedAt: Date.now()
    });

    lastExplorerHeightFetched = latestBlockHeight;
    const tookMs = Date.now() - startedAt;
    logInfo('Updated monero:explorer.', {
        headerCount: blocksToStore.length,
        mode: 'headers-only',
        tookMs
    });
}

async function collectRecentBlocks(latestHeight: number) {
    if (!Number.isFinite(latestHeight) || latestHeight <= 0) {
        logWarn('Skipping monero:blocks update because latest height is invalid.', {
            latestHeight
        });
        return;
    }

    // get_info height can represent chain length, while header RPC expects top block index.
    const latestBlockHeight = Math.max(0, latestHeight - 1);

    if (lastBlocksHeightFetched === latestBlockHeight) {
        logInfo('Skipping monero:blocks update because height is unchanged.', {
            latestBlockHeight
        });
        return;
    }

    const startHeight = Math.max(0, latestBlockHeight - (BLOCKS_TO_COLLECT - 1));
    const expectedCount = latestBlockHeight - startHeight + 1;

    const headers = await fetchLatestBlockHeadersRange(startHeight, latestBlockHeight);

    if (!headers) {
        return;
    }

    if (headers.length !== expectedCount) {
        logWarn('Skipping monero:blocks update because the header response was partial.', {
            receivedHeaders: headers.length,
            expectedHeaders: expectedCount,
            startHeight,
            latestBlockHeight
        });
        return;
    }

    const hasRequiredFields = headers.every(h => h.height !== null && h.timestamp !== null && h.hash !== null);
    if (!hasRequiredFields) {
        logError('Skipping monero:blocks update because required header fields are missing.', undefined, {
            startHeight,
            latestBlockHeight
        });
        return;
    }

    const finderByHash = await fetchRecentBlockFinders(headers);
    const blocks: MoneroBlockWithFinder[] = [...headers]
        .sort((a, b) => (b.height ?? 0) - (a.height ?? 0))
        .map(header => ({
            ...header,
            finder: header.hash ? (finderByHash.get(header.hash) ?? null) : null
        }));

    await setRedisJson('monero:blocks', {
        range: {
            startHeight,
            latestHeight: latestBlockHeight,
            count: blocks.length
        },
        blocks,
        node: MONERO_DAEMON_NAME,
        updatedAt: Date.now()
    });

    lastBlocksHeightFetched = latestBlockHeight;
    logInfo('Updated monero:blocks.', {
        headerCount: blocks.length
    });
}

const fetchWithTimeout = <T>(url: string): Promise<T | null> =>
    fetch(url, { signal: AbortSignal.timeout(5000) })
        .then(async res => {
            if (!res.ok) {
                throw new Error(`HTTP ${res.status}`);
            }

            return await res.json() as T;
        })
        .catch(error => {
            logWarn('HTTP JSON fetch failed.', {
                url,
                ...formatError(error)
            });
            return null;
        });

const fetchWithTimeoutText = <T>(url: string): Promise<T | null> =>
    fetch(url, { signal: AbortSignal.timeout(5000) })
        .then(async res => {
            if (!res.ok) {
                throw new Error(`HTTP ${res.status}`);
            }

            try {
                return await res.json() as T;
            } catch (jsonError) {
                try {
                    return JSON.parse(await res.text()) as T;
                } catch (textError) {
                    logWarn('Failed to parse HTTP response body as JSON.', {
                        url,
                        initialJsonError: formatError(jsonError),
                        ...formatError(textError)
                    });
                    return null;
                }
            }
        })
        .catch(error => {
            logWarn('HTTP fetch failed.', {
                url,
                ...formatError(error)
            });
            return null;
        });

async function fetchSupportXmrAnnouncements(targetHeights: Set<number>, targetHashes: Set<string>): Promise<PoolBlockAnnouncement[]> {
    const response = await fetchWithTimeout<SupportXmrBlockResponse>('https://www.supportxmr.com/api/pool/blocks');
    const announcements = (response ?? []).map(block => normalizePoolAnnouncement(block.hash, block.height, 'SupportXMR'));
    return filterAnnouncementsForTargetBlocks(announcements, targetHeights, targetHashes);
}

async function fetchNanoPoolAnnouncements(targetHeights: Set<number>, targetHashes: Set<string>): Promise<PoolBlockAnnouncement[]> {
    const response = await fetchWithTimeout<NanoPoolBlocksResponse>('https://api.nanopool.org/v1/xmr/blocks/0/50');

    if (!response?.status || !Array.isArray(response.data)) {
        return [];
    }

    const announcements = response.data.map(block => normalizePoolAnnouncement(block.hash, block.number, 'NanoPool'));
    return filterAnnouncementsForTargetBlocks(announcements, targetHeights, targetHashes);
}

async function fetchGenericPoolAnnouncements(
    url: string,
    finder: string,
    targetHeights: Set<number>,
    targetHashes: Set<string>
): Promise<PoolBlockAnnouncement[]> {
    const response = await fetchWithTimeout<GenericPoolBlocksResponse>(url);
    const announcements = (response ?? []).map(block => normalizePoolAnnouncement(block.hash, block.height, finder));
    return filterAnnouncementsForTargetBlocks(announcements, targetHeights, targetHashes);
}

async function fetchXmrPoolEuAnnouncements(targetHeights: Set<number>, targetHashes: Set<string>): Promise<PoolBlockAnnouncement[]> {
    const response = await fetchWithTimeoutText<XmrPoolEuStatsResponse>('https://web.xmrpool.eu:8119/stats');
    const rawBlocks = response?.pool?.blocks;

    if (!Array.isArray(rawBlocks)) {
        return [];
    }

    const announcements: PoolBlockAnnouncement[] = [];

    for (let index = 0; index < rawBlocks.length - 1; index += 2) {
        const encodedBlock = rawBlocks[index];
        const heightValue = rawBlocks[index + 1];

        if (typeof encodedBlock !== 'string') {
            continue;
        }

        const [hash] = encodedBlock.split(':');
        const announcement = normalizePoolAnnouncement(hash, heightValue, 'XMRPoolEU');
        if (announcement) {
            announcements.push(announcement);
        }
    }

    return filterAnnouncementsForTargetBlocks(announcements, targetHeights, targetHashes);
}

async function fetchSoloPoolAnnouncements(targetHeights: Set<number>, targetHashes: Set<string>): Promise<PoolBlockAnnouncement[]> {
    const response = await fetchWithTimeout<SoloPoolBlocksResponse>('https://xmr.solopool.org/api/blocks');
    const rawBlocks = [
        ...(response?.candidates ?? []),
        ...(response?.immature ?? []),
        ...(response?.matured ?? [])
    ];
    const announcements = rawBlocks.map(block => normalizePoolAnnouncement(block.hash, block.height, 'SoloPool'));
    return filterAnnouncementsForTargetBlocks(announcements, targetHeights, targetHashes);
}

async function fetchRecentBlockFinders(headers: NormalizedMoneroBlockHeader[]): Promise<Map<string, string>> {
    const targetHeights = new Set<number>();
    const targetHashes = new Set<string>();

    for (const header of headers) {
        if (header.height !== null) {
            targetHeights.add(header.height);
        }

        if (header.hash) {
            targetHashes.add(header.hash);
        }
    }

    if (targetHashes.size === 0) {
        return new Map();
    }

    const settled = await Promise.allSettled([
        fetchSupportXmrAnnouncements(targetHeights, targetHashes),
        fetchNanoPoolAnnouncements(targetHeights, targetHashes),
        fetchGenericPoolAnnouncements('https://p2pool.io/api/pool/blocks', 'P2Pool', targetHeights, targetHashes),
        fetchGenericPoolAnnouncements('https://api.c3pool.org/pool/blocks', 'C3Pool', targetHeights, targetHashes),
        fetchGenericPoolAnnouncements('https://api.moneroocean.stream/pool/blocks', 'MoneroOcean', targetHeights, targetHashes),
        fetchGenericPoolAnnouncements('https://api.skypool.xyz/pool/blocks', 'SkyPool', targetHeights, targetHashes),
        fetchXmrPoolEuAnnouncements(targetHeights, targetHashes),
        fetchSoloPoolAnnouncements(targetHeights, targetHashes),
        fetchGenericPoolAnnouncements('https://np-api.monerod.org/pool/blocks', 'Monerod', targetHeights, targetHashes)
    ]);

    const finderByHash = new Map<string, string>();

    for (const result of settled) {
        if (result.status !== 'fulfilled') {
            logWarn('Failed to fetch block finder announcements from one source.', formatError(result.reason));
            continue;
        }

        for (const announcement of result.value) {
            if (!announcement.hash) {
                continue;
            }

            const existingFinder = finderByHash.get(announcement.hash);
            if (existingFinder && existingFinder !== announcement.finder) {
                logWarn('Conflicting block finder announcements detected.', {
                    hash: announcement.hash,
                    existingFinder,
                    incomingFinder: announcement.finder
                });
                continue;
            }

            finderByHash.set(announcement.hash, announcement.finder);
        }
    }

    return finderByHash;
}

async function aggregate() {
    logInfo('Starting aggregation.');

    const fetchNodeWithMetrics = async (node: NodeConfig): Promise<NodeMetric> => {
        const startedAt = Date.now();

        try {
            const response = await fetch(node.url, { signal: AbortSignal.timeout(5000) });
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

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
        } catch (error) {
            logWarn('Node health check failed.', {
                node: node.name,
                url: node.url,
                ...formatError(error)
            });
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

    // Normalize pool hashrate extraction to prefer API-provided data when available
    const getPoolHashrateFromApi = (name: string, data: any): number => {
        try {
            if (data && typeof data === 'object') {
                if (typeof data.data === 'number') return data.data;
                if (typeof data.data === 'string') {
                    const n = Number(data.data);
                    if (Number.isFinite(n)) return n;
                }
                if (data?.raw && typeof data.raw === 'object' && typeof data.raw.data === 'number') {
                    return data.raw.data;
                }
            }
        } catch (error) {
            logWarn('Pool hashrate normalization failed; falling back to generic extractor.', {
                pool: name,
                ...formatError(error)
            });
        }
        return extractHash(name, data);
    };

    const [poolResults, nodeResults] = await Promise.all([
        Promise.all(POOLS.map(async p => {
            const result = await fetchWithTimeoutText<any>(p.url);
            const hashApi = getPoolHashrateFromApi(p.name, result);
            logInfo('Pool fetch completed.', {
                pool: p.name,
                status: result ? 'online' : 'offline',
                hash: hashApi,
                rawPreview: JSON.stringify(result)?.slice(0, 200) ?? null
            });
            return result;
        })),
        Promise.all(NODES.map(node => fetchNodeWithMetrics(node)))
    ]);

    // Logic: Consensus & Normalization
    const consensus = getMajorityNetworkTruth(nodeResults);
    const bestHeight = consensus.height;
    const difficulty = consensus.difficulty;

    const networkHashrate = roundUpHashrate(difficulty / 120);

    for (const node of nodeResults) {
        logInfo('Node metric collected.', {
            node: node.name,
            status: node.status,
            pingMs: node.pingMs,
            height: node.height,
            difficulty: node.difficulty
        });
    }

    const payload = {
        network: {
            height: bestHeight,
            hashrate: networkHashrate,
            difficulty: difficulty
        },
        nodes: nodeResults,
        pools: POOLS.map((p, i) => {
            const data = poolResults[i];
            const hr = (data && typeof data === 'object') ? (() => {
                if (typeof data.data === 'number') return data.data;
                if (typeof data.data === 'string') {
                    const n = Number(data.data);
                    if (Number.isFinite(n)) return n;
                }
                if (data?.raw && typeof data.raw.data === 'number') return data.raw.data;
                return extractHash(p.name, data);
            })() : 0;
            return {
                name: p.name,
                homeUrl: p.homeUrl,
                apiUrl: p.url,
                hashrate: hr,
                status: poolResults[i] ? 'online' : 'offline'
            };
        }),
        updatedAt: Date.now()
    };

    await setRedisJson("monero:stats", payload);

    if (bestHeight > 0) {
        await collectRecentBlocks(bestHeight);
    } else {
        logWarn('Skipping monero:blocks update because no consensus height is available.');
    }

    await collectExplorerBlocks();

    logInfo('Aggregation finished.', {
        bestHeight,
        difficulty,
        networkHashrate
    });
}

async function collectMoneroInfo() {
    logInfo('Fetching CoinGecko monero info.');

    const coingeckoData = await fetchWithTimeout<CoinGeckoMoneroResponse>(
        'https://api.coingecko.com/api/v3/coins/monero?localization=false&tickers=false&community_data=false&developer_data=false&sparkline=false'
    );

    if (!coingeckoData) {
        logWarn('CoinGecko fetch failed. Keeping last monero:info payload.');
        return;
    }

    const moneroInfo = buildMoneroInfoPayload(coingeckoData);
    await setRedisJson("monero:info", moneroInfo);

    logInfo('Updated monero:info.');
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

        case 'solopool':
            return firstNumber(data?.hashrate);

        case 'herominers':
            return firstNumber(data?.pool?.hashrate);

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

function runJob(name: string, job: () => Promise<void>): void {
    job().catch(error => {
        logError(`Scheduled job "${name}" failed.`, error);
    });
}

process.on('unhandledRejection', reason => {
    logError('Unhandled promise rejection.', reason);
});

process.on('uncaughtException', error => {
    logError('Uncaught exception.', error);
});

setInterval(() => runJob('aggregate', aggregate), 300000);
setInterval(() => runJob('collectMoneroInfo', collectMoneroInfo), 300000);

runJob('aggregate', aggregate);
runJob('collectMoneroInfo', collectMoneroInfo);
