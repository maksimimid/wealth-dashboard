// ------------------ CONFIG ------------------
const DEFAULT_CONFIG = {
    FINNHUB_KEY: '',
    AIRTABLE_API_KEY: '',
    AIRTABLE_BASE_ID: 'appSxixo1i122KyBS',
    AIRTABLE_TABLE_NAME: 'Operations',
    MASSIVE_API_KEY: '',
    MASSIVE_API_BASE_URL: 'https://api.massive.com/v1'
};

const RUNTIME_CONFIG = (typeof window !== 'undefined' && window.DASHBOARD_CONFIG) ? window.DASHBOARD_CONFIG : {};

const loadingOverlay = typeof document !== 'undefined' ? document.getElementById('loading-overlay') : null;

const FINNHUB_KEY = RUNTIME_CONFIG.FINNHUB_KEY || DEFAULT_CONFIG.FINNHUB_KEY;
const HAS_FINNHUB_KEY = Boolean(FINNHUB_KEY);
const FINNHUB_REST = 'https://finnhub.io/api/v1';
const MAX_REST_BATCH = 5;

const AIRTABLE_API_KEY = RUNTIME_CONFIG.AIRTABLE_API_KEY || DEFAULT_CONFIG.AIRTABLE_API_KEY;
const AIRTABLE_BASE_ID = RUNTIME_CONFIG.AIRTABLE_BASE_ID || DEFAULT_CONFIG.AIRTABLE_BASE_ID;
const AIRTABLE_TABLE_NAME = RUNTIME_CONFIG.AIRTABLE_TABLE_NAME || DEFAULT_CONFIG.AIRTABLE_TABLE_NAME;
const AIRTABLE_URL = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(AIRTABLE_TABLE_NAME)}`;

const MASSIVE_API_KEY = RUNTIME_CONFIG.MASSIVE_API_KEY || DEFAULT_CONFIG.MASSIVE_API_KEY;
const MASSIVE_API_BASE_URL = RUNTIME_CONFIG.MASSIVE_API_BASE_URL || DEFAULT_CONFIG.MASSIVE_API_BASE_URL;
const SNAPSHOT_STORAGE_KEYS = {
    records: 'airtableSnapshotRecords',
    savedAt: 'airtableSnapshotSavedAt',
    csv: 'airtableSnapshotCsv'
};

// ----------------- STATE --------------------
let positions = [];
let finnhubIndex = new Map();
let symbolSet = new Set();
let charts = {};
let ws;
let lastUpdated = null;
let operationsMeta = {count: 0, fetchedAt: null};
const UI_REFRESH_INTERVAL = 10000;
const CRYPTO_UI_INTERVAL = 10000;
let lastRenderAt = 0;
let scheduledRender = null;
let isBootstrapping = true;
let currentRangeTotalPnl = 0;
let currentCategoryRangeTotals = { crypto: 0, stock: 0, realEstate: 0, other: 0 };
let rangeDirty = true;
const referencePriceCache = new Map();
function rebuildSymbolIndex(){
    symbolSet = new Set();
    finnhubIndex = new Map();
    positions.forEach(p=>{
        if(p && p.finnhubSymbol){
            symbolSet.add(p.finnhubSymbol);
            finnhubIndex.set(p.finnhubSymbol, p);
        }
    });
}
const PNL_RANGE_CONFIG = [
    { key: '1D', label: 'Daily PNL' },
    { key: '1W', label: 'Weekly PNL' },
    { key: '1M', label: 'Monthly PNL' },
    { key: '1Y', label: 'Yearly PNL' },
    { key: 'ALL', label: 'All' }
];
const RANGE_LOOKBACK = {
    '1D': 24 * 60 * 60,
    '1W': 7 * 24 * 60 * 60,
    '1M': 30 * 24 * 60 * 60,
    '1Y': 365 * 24 * 60 * 60
};
const RANGE_LABELS = PNL_RANGE_CONFIG.reduce((acc, item)=>{
    acc[item.key] = item.label;
    return acc;
}, {});
const DATA_SOURCE_LABELS = {
    loading: 'Loading data…',
    airtable: 'Live Airtable',
    snapshot: 'Cached snapshot',
    fallback: 'Offline test data'
};
const PNL_CHART_TIME_CONFIG = {
    '1D': { unit: 'hour', displayFormats: { hour: 'ha' }, maxTicks: 6 },
    '1W': { unit: 'day', displayFormats: { day: 'MMM d' }, maxTicks: 7 },
    '1M': { unit: 'day', displayFormats: { day: 'MMM d' }, maxTicks: 8 },
    '1Y': { unit: 'month', displayFormats: { month: 'MMM' }, maxTicks: 6 },
    'ALL': { unit: 'month', displayFormats: { month: 'MMM yyyy' }, maxTicks: 6 }
};
let pnlRange = 'ALL';
let pnlRangeTabsContainer = null;
const pnlRangeButtons = new Map();
let pnlCardLabelElement = null;
let pnlCardLabelBase = 'Total P&L';
const yahooQuoteCache = new Map();
const previousKpiValues = { totalPnl: null, netWorth: null, netContribution: null, cashAvailable: null, pnlCrypto: null, pnlStock: null, pnlRealEstate: null };
const bestPerformerCache = { crypto: null, stock: null, realEstate: null };
let bestInfoPopover = null;
let bestInfoPopoverElements = null;
let activeBestInfoCategory = null;
let activeBestInfoTrigger = null;
let bestInfoListenersAttached = false;
let assetYearSeries = { labels: [], datasets: [] };
let assetYearSeriesDirty = true;
const assetColorCache = new Map();
let realEstateRentSeries = { labels: [], datasets: [] };
let realEstateRentSeriesDirty = true;
let lastRealEstateAnalytics = null;
const realEstateRentFilters = new Map();
const realEstateGroupState = { active: true, passive: false };
let transactionModal = null;
let transactionModalTitle = null;
let transactionModalOperations = null;
let transactionModalMeta = null;
let transactionModalCanvas = null;
let transactionChart = null;
let lastTransactionTrigger = null;
let lastTransactionData = null;
let lastTransactionPosition = null;
let modalChartContainer = null;
let transactionLotsContainer = null;
let transactionLotsControls = null;
let transactionLotsList = null;
let transactionLotsSortSelect = null;
let transactionLotsDirectionSelect = null;
let transactionLotsGroupSelect = null;
let transactionLotsSortField = 'date';
let transactionLotsSortDirection = 'desc';
let transactionLotsGroupKey = 'none';
let viewLotsButton = null;
let assetViewToggleButton = null;
let assetViewMode = 'rows';
let currentModalView = 'chart';
const VIEW_MODE_STORAGE_KEY = 'assetViewMode';
const MARKET_TIME_ZONE = 'America/New_York';
const MARKET_OPEN_MINUTES = 9 * 60 + 30;
const MARKET_CLOSE_MINUTES = 16 * 60;
const MARKET_STATUS_INTERVAL = 60 * 1000;
const NET_WORTH_LABEL_MAP = {
    crypto: 'Crypto',
    stock: 'Stocks',
    realestate: 'Real Estate',
    automobile: 'Automobile',
    cash: 'Cash',
    other: 'Other',
    unclassified: 'Unclassified'
};
const WEEKDAY_NAME_TO_INDEX = { sun: 0, mon: 1, tue: 2, wed: 3, thu: 4, fri: 5, sat: 6 };
const previousCategorySummaries = {
    crypto: { market: null, pnl: null, allocation: null },
    stock: { market: null, pnl: null, allocation: null }
};
const categorySectionState = {
    crypto: { open: true, closed: false },
    stock: { open: true, closed: false }
};
let pnlTrendChart = null;
let marketStatusTimer = null;
let marketStatusVisibilityBound = false;
let netWorthSparklineChart = null;
const SPARKLINE_ACTUAL_STEP_DAYS = 7;
const SPARKLINE_PROJECTED_STEP_DAYS = 14;
const SPARKLINE_ACTUAL_SMOOTHING = 0.24;
const SPARKLINE_PROJECTED_SMOOTHING = 0.32;
let pnlPercentageToggleButton = null;
let showPnlPercentages = false;
const PNL_PERCENTAGE_STORAGE_KEY = 'showPnlPercentages';
const MILLION_TARGET = 1_000_000;
let netWorthBubbleToggleButton = null;
let netWorthInlineViewMode = 'chart';
const NET_WORTH_INLINE_VIEW_STORAGE_KEY = 'netWorthInlineViewMode';
let lastNetWorthTotals = {};
let lastNetWorthTotalValue = 0;
let lastMindmapRenderHash = null;
let lastMindmapDimensions = { width: 0, height: 0 };
let netWorthSparklineCanvas = null;
let lastNetWorthTimeline = null;
let netWorthDetailModal = null;
let netWorthDetailCanvas = null;
let netWorthDetailChart = null;
let netWorthBubbleNeedsRender = false;
let netWorthBubbleSnapshot = null;
let netWorthBubbleSnapshotTotal = 0;
let netWorthDetailSubtitle = null;
let netWorthDetailMeta = null;
let dataSourceMode = 'loading';
let isSwitchingDataSource = false;
let zoomPluginRegistered = false;
const sparklineCrosshairPlugin = {
    id: 'sparklineCrosshair',
    afterDraw(chart){
        const tooltip = chart?.tooltip;
        if(!tooltip || tooltip.opacity === 0){
            return;
        }
        const dataPoint = tooltip.dataPoints && tooltip.dataPoints[0];
        if(!dataPoint || !dataPoint.element){
            return;
        }
        const x = dataPoint.element.x;
        if(typeof x !== 'number'){
            return;
        }
        const { top, bottom } = chart.chartArea;
        const ctx = chart.ctx;
        ctx.save();
        ctx.strokeStyle = 'rgba(148, 163, 184, 0.22)';
        ctx.lineWidth = 1;
        ctx.setLineDash([2, 6]);
        ctx.beginPath();
        ctx.moveTo(x, top);
        ctx.lineTo(x, bottom);
        ctx.stroke();
        ctx.restore();
    }
};

function registerZoomPluginIfNeeded(){
    if(zoomPluginRegistered) return;
    if(typeof Chart === 'undefined' || !Chart.register) return;
    const zoomPlugin = typeof window !== 'undefined' ? window['chartjs-plugin-zoom'] || window.chartjsPluginZoom : null;
    if(zoomPlugin){
        Chart.register(zoomPlugin);
        zoomPluginRegistered = true;
    }
}

function ensurePanSupport(options, mode = 'x'){
    if(!options || typeof options !== 'object'){
        return options;
    }
    registerZoomPluginIfNeeded();
    if(!options.plugins){
        options.plugins = {};
    }
    if(!options.plugins.zoom){
        options.plugins.zoom = {};
    }
    const zoomOptions = options.plugins.zoom;
    if(!zoomOptions.pan){
        zoomOptions.pan = {};
    }
    if(typeof zoomOptions.pan.enabled === 'undefined'){
        zoomOptions.pan.enabled = true;
    }
    if(!zoomOptions.pan.mode){
        zoomOptions.pan.mode = mode || 'x';
    }
    if(typeof zoomOptions.pan.threshold !== 'number'){
        zoomOptions.pan.threshold = 8;
    }
    if(!zoomOptions.zoom){
        zoomOptions.zoom = {};
    }
    if(!zoomOptions.zoom.wheel){
        zoomOptions.zoom.wheel = { enabled: false };
    }
    if(!zoomOptions.zoom.pinch){
        zoomOptions.zoom.pinch = { enabled: false };
    }
    if(!zoomOptions.zoom.drag){
        zoomOptions.zoom.drag = { enabled: false };
    }
    return options;
}

const transactionHoverPlugin = {
    id: 'transactionHoverHelper',
    afterDraw(chart){
        const baselineValue = chart?.options?.plugins?.hoverBaseline?.value;
        if(!Number.isFinite(baselineValue)) return;
        const tooltip = chart?.tooltip;
        if(!tooltip || tooltip.opacity === 0 || !tooltip.dataPoints?.length) return;
        const activePoint = tooltip.dataPoints[0];
        if(!activePoint) return;
        const dataset = chart.data?.datasets?.[activePoint.datasetIndex];
        if(!dataset || dataset.label !== 'Purchases') return;
        const element = activePoint.element;
        if(!element) return;
        const yScale = chart.scales?.y;
        if(!yScale) return;
        const chartArea = chart.chartArea || { left: 0, right: chart.width, top: 0, bottom: chart.height };
        const clampY = value => Math.min(Math.max(value, chartArea.top), chartArea.bottom);
        const baselineYRaw = yScale.getPixelForValue(baselineValue);
        const x = element.x;
        const y = element.y;
        if(!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(baselineYRaw)) return;
        const baselineY = clampY(baselineYRaw);
        const pointY = clampY(y);
        const pointPrice = Number(activePoint.raw?.price ?? activePoint.raw?.y ?? activePoint.parsed?.y);
        if(!Number.isFinite(pointPrice)) return;
        const diffValue = baselineValue - pointPrice;
        const diffPct = Number.isFinite(baselineValue) && Math.abs(baselineValue) > 1e-9 ? (diffValue / baselineValue) * 100 : null;
        const rawQty = Number(activePoint.raw?.quantity ?? activePoint.raw?.rawQty ?? 0);
        const absQty = Math.abs(rawQty);
        const quantityAwareDiff = diffValue * (absQty > 0 ? absQty : 1);
        const ctx = chart.ctx;
        const color = diffValue >= 0 ? 'rgba(34, 197, 94, 0.9)' : 'rgba(248, 113, 113, 0.9)';
        ctx.save();
        ctx.strokeStyle = color;
        ctx.setLineDash([4, 4]);
        ctx.lineWidth = 1.2;
        ctx.beginPath();
        ctx.moveTo(x, pointY);
        ctx.lineTo(x, baselineY);
        ctx.stroke();
        ctx.restore();

        const descriptor = diffValue >= 0 ? 'Potential profit' : 'Potential loss';
        const deltaMoney = quantityAwareDiff > 0
            ? `+${money(quantityAwareDiff)}`
            : quantityAwareDiff < 0
                ? money(quantityAwareDiff)
                : money(0);
        const pctText = diffPct !== null ? ` (${pct(diffPct)})` : '';
        const label = `${descriptor} ${deltaMoney}${pctText}`;
        const font = '11px "Inter", system-ui, -apple-system, BlinkMacSystemFont, sans-serif';
        ctx.save();
        ctx.font = font;
        const textMetrics = ctx.measureText(label);
        const textWidth = textMetrics.width;
        const textHeight = (textMetrics.actualBoundingBoxAscent || 9) + (textMetrics.actualBoundingBoxDescent || 3);
        const paddingX = 8;
        const paddingY = 4;
        const boxWidth = textWidth + paddingX * 2;
        const boxHeight = textHeight + paddingY * 2;
        const midY = (pointY + baselineY) / 2;
        let boxX = x - boxWidth / 2;
        boxX = Math.max(chartArea.left + 4, Math.min(boxX, chartArea.right - boxWidth - 4));
        let boxY = midY - boxHeight / 2;
        boxY = Math.max(chartArea.top + 4, Math.min(boxY, chartArea.bottom - boxHeight - 4));
        const borderColor = diffValue >= 0 ? 'rgba(34, 197, 94, 1)' : 'rgba(248, 113, 113, 1)';
        const backgroundColor = diffValue >= 0 ? 'rgba(34, 197, 94, 0.9)' : 'rgba(248, 113, 113, 0.9)';

        const drawRoundedRect = (context, xPos, yPos, width, height, radius)=>{
            const r = Math.min(radius, width / 2, height / 2);
            context.beginPath();
            context.moveTo(xPos + r, yPos);
            context.lineTo(xPos + width - r, yPos);
            context.quadraticCurveTo(xPos + width, yPos, xPos + width, yPos + r);
            context.lineTo(xPos + width, yPos + height - r);
            context.quadraticCurveTo(xPos + width, yPos + height, xPos + width - r, yPos + height);
            context.lineTo(xPos + r, yPos + height);
            context.quadraticCurveTo(xPos, yPos + height, xPos, yPos + height - r);
            context.lineTo(xPos, yPos + r);
            context.quadraticCurveTo(xPos, yPos, xPos + r, yPos);
            context.closePath();
        };

        drawRoundedRect(ctx, boxX, boxY, boxWidth, boxHeight, 6);
        ctx.fillStyle = backgroundColor;
        ctx.fill();
        ctx.lineWidth = 1;
        ctx.strokeStyle = borderColor;
        ctx.stroke();
        ctx.fillStyle = '#0f172a';
        ctx.textBaseline = 'middle';
        ctx.fillText(label, boxX + paddingX, boxY + boxHeight / 2);
        ctx.restore();
    }
};
let lastCryptoUiSync = 0;
let pendingCryptoUiSync = null;
const CATEGORY_CONFIG = {
    crypto: {
        metricKey: 'crypto',
        listId: 'crypto-positions',
        summary: {
            market: 'crypto-market-value',
            pnl: 'crypto-pnl',
            allocation: 'crypto-allocation'
        },
        emptyLabel: 'crypto',
        chartTabs: [
            { key: 'allocation', label: 'Allocation', chartId: 'cryptoChartAllocation', type: 'bar' },
            { key: 'performance', label: 'Performance', chartId: 'cryptoChartPerformance', type: 'bar' },
            { key: 'exposure', label: 'Exposure', chartId: 'cryptoChartExposure', type: 'doughnut' }
        ]
    },
    stock: {
        metricKey: 'stock',
        listId: 'stock-positions',
        summary: {
            market: 'stock-market-value',
            pnl: 'stock-pnl',
            allocation: 'stock-allocation'
        },
        emptyLabel: 'stock',
        chartTabs: [
            { key: 'allocation', label: 'Allocation', chartId: 'stockChartAllocation', type: 'bar' },
            { key: 'performance', label: 'Performance', chartId: 'stockChartPerformance', type: 'bar' },
            { key: 'exposure', label: 'Exposure', chartId: 'stockChartExposure', type: 'doughnut' }
        ]
    }
};
const CRYPTO_ICON_PROVIDERS = [
    symbol => `https://raw.githubusercontent.com/spothq/cryptocurrency-icons/master/svg/color/${symbol}.svg`,
    symbol => `https://raw.githubusercontent.com/spothq/cryptocurrency-icons/master/svg/black/${symbol}.svg`,
    symbol => `https://raw.githubusercontent.com/ErikThiart/cryptocurrency-icons/master/svg/color/${symbol}.svg`
];
const assetIconSourceCache = new Map();
const transactionPriceCache = new Map();
const localHistoricalCache = new Map();
const LOCAL_HISTORIC_DIR = 'assets/historic';
const LOCAL_HISTORIC_PREFIX = 'historic-';
const LOCAL_HISTORIC_SUFFIX = '-usd.csv';
const DEFAULT_HISTORIC_HEADER = '"Time","Close"';
const categoryChartTabState = {};
const HISTORIC_SYMBOL_SUFFIXES = ['USDT','USDC','USD'];
const MS_IN_DAY = 24 * 60 * 60 * 1000;

function sanitizePriceSeries(series){
    if(!Array.isArray(series)) return [];
    const byTime = new Map();
    series.forEach(point=>{
        if(!point) return;
        let time = point.time ?? point.x ?? point.t ?? point.timestamp ?? null;
        if(time instanceof Date){
            time = time.getTime();
        }else if(typeof time === 'string' && time){
            const parsed = Date.parse(time);
            time = Number.isNaN(parsed) ? Number(time) : parsed;
        }else if(typeof time !== 'number'){
            time = Number(time);
        }
        if(!Number.isFinite(time)) return;
        const candidates = [
            point.price,
            point.y,
            point.c,
            point.close,
            point.value
        ];
        let price = null;
        for(const candidate of candidates){
            const num = Number(candidate);
            if(Number.isFinite(num) && num >= 0){
                price = num;
                break;
            }
        }
        if(price === null) return;
        byTime.set(time, price);
    });
    const entries = Array.from(byTime.entries()).sort((a, b)=> a[0] - b[0]);
    return entries.map(([time, price])=> ({
        x: time,
        y: price,
        time,
        price
    }));
}

function formatDateKey(value){
    const normalized = toValidDate(value);
    if(!normalized) return null;
    return normalized.toISOString().slice(0, 10);
}

function addDays(value, days){
    const normalized = toValidDate(value);
    if(!normalized || !Number.isFinite(days)) return null;
    return new Date(normalized.getTime() + days * MS_IN_DAY);
}

function getCryptoBaseTicker(position){
    if(!position) return null;
    let raw = position.Symbol || position.displayName || position.Name || position.id || '';
    raw = String(raw).toUpperCase();
    if(raw.includes(':')){
        raw = raw.split(':').pop();
    }
    raw = raw.replace(/[^A-Z0-9]/g, '');
    if(!raw){
        return null;
    }
    const suffix = HISTORIC_SYMBOL_SUFFIXES.find(item => raw.endsWith(item));
    if(suffix){
        const trimmed = raw.slice(0, raw.length - suffix.length);
        if(trimmed) raw = trimmed;
    }
    return raw || null;
}

function getLocalHistoricCacheKey(position){
    const typeKey = String(position?.type || position?.Category || '').toLowerCase();
    if(typeKey !== 'crypto') return null;
    const base = getCryptoBaseTicker(position);
    if(!base) return null;
    return base.toLowerCase();
}

function buildHistoricFilePath(cacheKey){
    if(!cacheKey) return null;
    return `${LOCAL_HISTORIC_DIR}/${LOCAL_HISTORIC_PREFIX}${cacheKey}${LOCAL_HISTORIC_SUFFIX}`;
}

function buildHistoricHeader(position){
    const base = getCryptoBaseTicker(position);
    if(!base) return DEFAULT_HISTORIC_HEADER;
    return `"Time","${base.toUpperCase()} / USD Close"`;
}

function splitCsvLine(line){
    const result = [];
    let current = '';
    let inQuotes = false;
    for(let i = 0; i < line.length; i++){
        const char = line[i];
        if(char === '"'){
            if(inQuotes && line[i + 1] === '"'){
                current += '"';
                i++;
            }else{
                inQuotes = !inQuotes;
            }
        }else if(char === ',' && !inQuotes){
            result.push(current);
            current = '';
        }else{
            current += char;
        }
    }
    result.push(current);
    return result.map(value => value.trim());
}

function parseLocalHistoricalCsv(text){
    if(typeof text !== 'string') return null;
    const lines = text.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
    if(!lines.length) return null;
    const header = lines.shift();
    const series = [];
    lines.forEach(line=>{
        if(!line) return;
        const parts = splitCsvLine(line);
        if(parts.length < 2) return;
        const dateStr = parts[0].replace(/"/g, '').trim();
        const priceStr = parts[1].replace(/"/g, '').replace(/,/g,'').trim();
        const date = toValidDate(dateStr);
        const price = Number(priceStr);
        if(!date || !Number.isFinite(price)) return;
        const point = createSeriesPoint(date.getTime(), price);
        if(point){
            series.push(point);
        }
    });
    if(!series.length) return null;
    series.sort((a,b)=> a.time - b.time);
    const lastDate = new Date(series[series.length - 1].time);
    return {
        header: header || DEFAULT_HISTORIC_HEADER,
        series,
        lastDate
    };
}

async function loadLocalHistoricalSeries(position){
    const cacheKey = getLocalHistoricCacheKey(position);
    if(!cacheKey) return null;
    if(localHistoricalCache.has(cacheKey)){
        return localHistoricalCache.get(cacheKey);
    }
    const relativePath = buildHistoricFilePath(cacheKey);
    if(!relativePath) return null;
    let text = null;
    if(typeof window === 'undefined'){
        try{
            const fs = await import('fs/promises');
            const pathModule = await import('path');
            const absolutePath = pathModule.resolve(process.cwd(), relativePath);
            text = await fs.readFile(absolutePath, 'utf8');
        }catch(error){
            localHistoricalCache.set(cacheKey, null);
            return null;
        }
    }else{
        try{
            const response = await fetch(relativePath, { cache: 'no-cache' });
            if(!response.ok){
                localHistoricalCache.set(cacheKey, null);
                return null;
            }
            text = await response.text();
        }catch(error){
            localHistoricalCache.set(cacheKey, null);
            return null;
        }
    }
    const parsed = parseLocalHistoricalCsv(text);
    if(!parsed){
        localHistoricalCache.set(cacheKey, null);
        return null;
    }
    const info = {
        cacheKey,
        path: relativePath,
        header: parsed.header,
        series: parsed.series,
        lastDate: parsed.lastDate,
        lastChecked: null
    };
    localHistoricalCache.set(cacheKey, info);
    return info;
}

function needsHistoricalUpdate(lastDate){
    if(!(lastDate instanceof Date) || Number.isNaN(lastDate.getTime())) return true;
    const todayKey = formatDateKey(new Date());
    const lastKey = formatDateKey(lastDate);
    if(!todayKey || !lastKey) return true;
    return lastKey < todayKey;
}

function mergeHistoricalSeries(existingSeries, newSeries){
    const merged = new Map();
    const ingest = point=>{
        if(!point) return;
        let time = point.time ?? point.x ?? point.timestamp ?? null;
        if(!Number.isFinite(time)){
            const normalisedDate = toValidDate(point.date ?? point.Date ?? point.Time);
            if(normalisedDate){
                time = normalisedDate.getTime();
            }
        }
        const priceCandidates = [
            point.price,
            point.y,
            point.close,
            point.c,
            point.value,
            point.Close,
            point.close_price,
            point.adjusted_close
        ];
        let price = null;
        for(const candidate of priceCandidates){
            const num = Number(candidate);
            if(Number.isFinite(num)){
                price = num;
                break;
            }
        }
        if(!Number.isFinite(time) || price === null) return;
        const normalized = createSeriesPoint(time, price);
        if(normalized){
            merged.set(normalized.time, normalized);
        }
    };
    (existingSeries || []).forEach(ingest);
    (newSeries || []).forEach(ingest);
    return Array.from(merged.values()).sort((a,b)=> a.time - b.time);
}

async function writeHistoricalSeriesFile(relativePath, header, series){
    if(typeof window !== 'undefined') return false;
    try{
        const fs = await import('fs/promises');
        const pathModule = await import('path');
        const absolutePath = pathModule.resolve(process.cwd(), relativePath);
        const lines = [header || DEFAULT_HISTORIC_HEADER];
        (series || []).forEach(point=>{
            const date = toValidDate(point.time ?? point.x ?? point.timestamp ?? point.date);
            const price = Number(point.price ?? point.y ?? point.close ?? point.c ?? point.value);
            if(!date || !Number.isFinite(price)) return;
            const dateKey = formatDateKey(date);
            if(!dateKey) return;
            lines.push(`"${dateKey}",${price}`);
        });
        await fs.writeFile(absolutePath, lines.join('\n'), 'utf8');
        return true;
    }catch(error){
        console.warn('Failed to persist historical CSV', relativePath, error);
        return false;
    }
}

function mapMassiveSymbol(position){
    const base = getCryptoBaseTicker(position);
    if(!base) return null;
    return `${base.toUpperCase()}-USD`;
}

async function fetchMassiveHistoricalSeries(position, startDate, endDate){
    if(!MASSIVE_API_KEY) return [];
    const symbol = mapMassiveSymbol(position);
    if(!symbol) return [];
    const startKey = formatDateKey(startDate);
    const endKey = formatDateKey(endDate);
    if(!startKey || !endKey || startKey > endKey) return [];
    const baseRaw = (MASSIVE_API_BASE_URL || '').trim();
    const baseUrl = baseRaw ? baseRaw.replace(/\/+$/,'') : 'https://api.massive.com/v1';
    const hasPredefinedEndpoint = /\/(timeseries|time-series|ohlcv|markets|crypto)\b/i.test(baseUrl);
    const baseCandidates = hasPredefinedEndpoint ? [''] : [
        `/crypto/tickers/${symbol}/ohlcv`,
        `/crypto/tickers/${symbol}/ohlcv/eod`,
        `/crypto/tickers/${symbol}/ohlcv/history`,
        `/ticks/${symbol}/ohlcv`,
        `/markets/crypto/${symbol}/ohlcv`,
        `/markets/timeseries/eod`,
        `/timeseries/eod`,
        `/time-series/eod`
    ];
    const attempted = [];
    for(const suffix of baseCandidates){
        const endpoint = hasPredefinedEndpoint ? baseUrl : `${baseUrl}${suffix}`;
        let url;
        try{
            url = new URL(endpoint);
        }catch(error){
            continue;
        }
        if(!hasPredefinedEndpoint){
            if(suffix.includes(symbol)){
                url.searchParams.set('interval', '1d');
                url.searchParams.set('start', startKey);
                url.searchParams.set('end', endKey);
            }else{
                url.searchParams.set('ticker', symbol);
                url.searchParams.set('interval', '1d');
                url.searchParams.set('start', startKey);
                url.searchParams.set('end', endKey);
            }
        }else{
            url.searchParams.set('ticker', symbol);
            url.searchParams.set('interval', '1d');
            url.searchParams.set('start', startKey);
            url.searchParams.set('end', endKey);
        }
        try{
            const response = await fetch(url.toString(), {
                headers: {
                    'Authorization': `Bearer ${MASSIVE_API_KEY}`,
                    'Accept': 'application/json'
                }
            });
            attempted.push({ status: response.status, url: url.toString() });
            if(!response.ok){
                if(response.status === 401 || response.status === 403){
                    console.warn('Massive historical fetch unauthorized/forbidden', symbol, response.status, url.toString());
                    return [];
                }
                if(response.status === 404 || response.status === 400){
                    continue;
                }
                continue;
            }
            const json = await response.json();
            const rows = Array.isArray(json?.data) ? json.data
                : Array.isArray(json?.results) ? json.results
                : Array.isArray(json?.values) ? json.values
                : Array.isArray(json?.timeseries) ? json.timeseries
                : Array.isArray(json) ? json
                : [];
            if(!rows.length){
                continue;
            }
            const points = rows.map(row=>{
                const dateValue = row.date ?? row.Date ?? row.time ?? row.timestamp ?? row.period ?? row.start;
                const closeValue = row.close ?? row.Close ?? row.c ?? row.close_price ?? row.adjusted_close ?? row.value ?? row.end_price;
                const date = toValidDate(dateValue);
                const price = Number(closeValue);
                return createSeriesPoint(date ? date.getTime() : null, price);
            }).filter(Boolean);
            if(points.length){
                return points.sort((a,b)=> a.time - b.time);
            }
        }catch(error){
            console.warn('Massive historical fetch exception', symbol, error, url?.toString());
        }
    }
    if(attempted.length){
        const last = attempted[attempted.length - 1];
        console.warn('Massive historical fetch failed', symbol, last.status, last.url);
    }
    return [];
}

function toValidDate(value){
    if(!value && value !== 0) return null;
    if(value instanceof Date){
        return Number.isNaN(value.getTime()) ? null : value;
    }
    if(typeof value === 'number' && Number.isFinite(value)){
        const dateFromNumber = new Date(value);
        return Number.isNaN(dateFromNumber.getTime()) ? null : dateFromNumber;
    }
    if(typeof value === 'string'){
        const parsedIso = Date.parse(value);
        if(!Number.isNaN(parsedIso)){
            const parsedDate = new Date(parsedIso);
            return Number.isNaN(parsedDate.getTime()) ? null : parsedDate;
        }
        const numeric = Number(value);
        if(Number.isFinite(numeric)){
            const dateFromNumeric = new Date(numeric);
            return Number.isNaN(dateFromNumeric.getTime()) ? null : dateFromNumeric;
        }
    }
    const fallback = new Date(value);
    return Number.isNaN(fallback.getTime()) ? null : fallback;
}

function getOperationDate(operation){
    if(!operation) return null;
    const candidates = [
        operation.date,
        operation.rawDate,
        operation.Date,
        operation.timestamp
    ];
    for(const candidate of candidates){
        const date = toValidDate(candidate);
        if(date) return date;
    }
    return null;
}

function getFirstPurchaseTime(position){
    if(!position) return null;
    const operations = Array.isArray(position.operations) ? position.operations : [];
    let earliest = null;
    operations.forEach(op=>{
        const type = String(op.type || '').toLowerCase();
        if(type !== 'purchasesell') return;
        const amount = Number(op.amount || 0);
        if(!(amount > 0)) return;
        const opDate = getOperationDate(op);
        if(!opDate) return;
        if(!earliest || opDate < earliest){
            earliest = opDate;
        }
    });
    if(!earliest && Array.isArray(position.lots)){
        position.lots.forEach(lot=>{
            const lotDate = toValidDate(lot?.date);
            if(!lotDate) return;
            if(!earliest || lotDate < earliest){
                earliest = lotDate;
            }
        });
    }
    return earliest ? earliest.getTime() : null;
}

function createSeriesPoint(time, price){
    const ts = Number(time);
    const val = Number(price);
    if(!Number.isFinite(ts) || !Number.isFinite(val)){
        return null;
    }
    return { x: ts, y: val, time: ts, price: val };
}

function preparePriceSeries(series, firstPurchaseTime, position){
    if(!Array.isArray(series) || !series.length){
        return [];
    }
    const base = series.map(point=>{
        const time = point?.time ?? point?.x ?? point?.t ?? point?.timestamp;
        const price = point?.price ?? point?.y ?? point?.value ?? point?.c ?? point?.close;
        return createSeriesPoint(time, price);
    }).filter(Boolean);
    if(!base.length){
        return [];
    }
    base.sort((a, b)=> a.time - b.time);
    let result = base.slice();
    if(Number.isFinite(firstPurchaseTime)){
        const firstIndex = result.findIndex(point => point.time >= firstPurchaseTime);
        if(firstIndex === -1){
            const fallbackPrice = Number(position?.displayPrice || position?.currentPrice || position?.lastKnownPrice || position?.avgPrice || result[result.length - 1].price);
            if(Number.isFinite(fallbackPrice)){
                result.push(createSeriesPoint(firstPurchaseTime, fallbackPrice));
                result.sort((a, b)=> a.time - b.time);
            }
        }else{
            result = result.slice(firstIndex);
            const firstPoint = result[0];
            if(firstPoint){
                if(firstPoint.time > firstPurchaseTime){
                    result.unshift(createSeriesPoint(firstPurchaseTime, firstPoint.price));
                }else if(firstPoint.time < firstPurchaseTime){
                    result[0] = createSeriesPoint(firstPurchaseTime, firstPoint.price);
                }
            }
        }
    }
    const nowTs = Date.now();
    if(result.length){
        const lastPoint = result[result.length - 1];
        const fallbackPrice = Number(position?.displayPrice || position?.currentPrice || position?.lastKnownPrice || position?.avgPrice || lastPoint.price);
        if(Number.isFinite(fallbackPrice)){
            if(nowTs - lastPoint.time > 30 * 60 * 1000){
                result.push(createSeriesPoint(nowTs, fallbackPrice));
            }else if(nowTs > lastPoint.time){
                result[result.length - 1] = createSeriesPoint(nowTs, fallbackPrice);
            }else if(Math.abs(nowTs - lastPoint.time) <= 30 * 60 * 1000 && !Number.isFinite(lastPoint.price)){
                result[result.length - 1] = createSeriesPoint(nowTs, fallbackPrice);
            }
        }
    }
    const dedup = new Map();
    result.forEach(point=>{
        if(point){
            dedup.set(point.time, point);
        }
    });
    return Array.from(dedup.values()).sort((a, b)=> a.time - b.time);
}

function getPriceHistoryForPosition(position){
    if(!position) return [];
    if(Array.isArray(position.priceHistory) && position.priceHistory.length){
        return position.priceHistory;
    }
    const typeKey = String(position.type || '').toLowerCase();
    const finnhubSymbol = position.finnhubSymbol || mapFinnhubSymbol(position.Symbol || position.displayName || position.Name, position.type, false);
    const coinGeckoId = typeKey === 'crypto' ? mapCoinGeckoId(position) : null;
    const yahooSymbol = mapYahooSymbol(position);
    const cacheKey = `${finnhubSymbol || ''}|${coinGeckoId || yahooSymbol || ''}|${typeKey}`;
    const firstPurchaseTime = getFirstPurchaseTime(position);
    if(transactionPriceCache.has(cacheKey)){
        const cached = transactionPriceCache.get(cacheKey);
        const baseSeries = Array.isArray(cached) && cached.length && typeof cached[0]?.time === 'number'
            ? cached
            : sanitizePriceSeries(cached);
        const prepared = preparePriceSeries(baseSeries, firstPurchaseTime, position);
        if(prepared.length){
            position.priceHistory = prepared;
            transactionPriceCache.set(cacheKey, prepared);
            return prepared;
        }
    }
    return [];
}
let netContributionTotal = 0;
let isRangeUpdateInFlight = false;
const TRANSACTION_CHART_COLORS = {
    buys: 'rgba(34, 197, 94, 0.85)',
    buysBorder: 'rgba(16, 185, 129, 0.95)',
    sells: 'rgba(248, 113, 113, 0.85)',
    sellsBorder: 'rgba(248, 113, 113, 0.95)',
    baseline: 'rgba(148, 163, 184, 0.55)',
    priceLine: 'rgba(59, 130, 246, 0.8)'
};
const TRANSACTION_HISTORY_LOOKBACK_DAYS = 180;
const FLASH_DURATION = 1500;
const RENT_TAGS = ['rent', 'rental', 'lease', 'tenant', 'tenancy', 'airbnb', 'booking'];
const EXPENSE_TAGS = ['expense', 'expenses', 'maintenance', 'repair', 'repairs', 'tax', 'taxes', 'property tax', 'property-tax', 'insurance', 'mortgage', 'mortgagepayment', 'hoa', 'hoa fees', 'utility', 'utilities', 'water', 'electric', 'electricity', 'gas', 'cleaning', 'management', 'interest', 'service', 'fee', 'fees'];
const MINDMAP_MAIN_STYLE = {
    background: 'radial-gradient(circle at 35% 30%, rgba(34, 197, 94, 0.9), rgba(6, 95, 70, 0.65))',
    borderColor: 'rgba(52, 211, 153, 0.9)',
    boxShadow: '0 0 32px rgba(16, 185, 129, 0.45), inset 0 0 28px rgba(5, 150, 105, 0.55)',
    textColor: '#ecfdf5',
    titleColor: '#bbf7d0',
    lineColor: 'rgba(52, 211, 153, 0.5)'
};
const MINDMAP_COLOR_PALETTE = [
    {
        background: 'radial-gradient(circle at 32% 28%, rgba(59, 130, 246, 0.85), rgba(29, 78, 216, 0.55))',
        borderColor: 'rgba(59, 130, 246, 0.95)',
        boxShadow: '0 0 24px rgba(59, 130, 246, 0.35), inset 0 0 20px rgba(29, 78, 216, 0.45)',
        textColor: '#f8fafc',
        lineColor: 'rgba(59, 130, 246, 0.6)'
    },
    {
        background: 'radial-gradient(circle at 30% 32%, rgba(129, 140, 248, 0.85), rgba(91, 33, 182, 0.55))',
        borderColor: 'rgba(129, 140, 248, 0.9)',
        boxShadow: '0 0 24px rgba(129, 140, 248, 0.35), inset 0 0 20px rgba(91, 33, 182, 0.45)',
        textColor: '#eef2ff',
        lineColor: 'rgba(99, 102, 241, 0.58)'
    },
    {
        background: 'radial-gradient(circle at 30% 30%, rgba(251, 191, 36, 0.85), rgba(245, 158, 11, 0.6))',
        borderColor: 'rgba(245, 158, 11, 0.85)',
        boxShadow: '0 0 24px rgba(245, 158, 11, 0.4), inset 0 0 20px rgba(217, 119, 6, 0.45)',
        textColor: '#0f172a',
        lineColor: 'rgba(245, 158, 11, 0.58)'
    },
    {
        background: 'radial-gradient(circle at 32% 32%, rgba(244, 114, 182, 0.85), rgba(190, 24, 93, 0.55))',
        borderColor: 'rgba(244, 114, 182, 0.9)',
        boxShadow: '0 0 24px rgba(244, 114, 182, 0.35), inset 0 0 20px rgba(190, 24, 93, 0.45)',
        textColor: '#fdf2f8',
        lineColor: 'rgba(236, 72, 153, 0.58)'
    },
    {
        background: 'radial-gradient(circle at 30% 28%, rgba(45, 212, 191, 0.85), rgba(13, 148, 136, 0.55))',
        borderColor: 'rgba(45, 212, 191, 0.9)',
        boxShadow: '0 0 24px rgba(45, 212, 191, 0.35), inset 0 0 20px rgba(13, 148, 136, 0.45)',
        textColor: '#ecfeff',
        lineColor: 'rgba(20, 184, 166, 0.55)'
    },
    {
        background: 'radial-gradient(circle at 30% 32%, rgba(165, 180, 252, 0.85), rgba(67, 56, 202, 0.55))',
        borderColor: 'rgba(99, 102, 241, 0.9)',
        boxShadow: '0 0 24px rgba(99, 102, 241, 0.35), inset 0 0 20px rgba(67, 56, 202, 0.45)',
        textColor: '#eef2ff',
        lineColor: 'rgba(99, 102, 241, 0.58)'
    }
];
const MINDMAP_LABEL_OVERRIDES = {
    automobile: 'Auto',
    'real estate': 'Real Est',
    realestate: 'Real Est',
    stock: 'Stocks',
    crypto: 'Crypto',
    cash: 'Cash',
    other: 'Other',
    unclassified: 'Other'
};

const CRYPTO_FH_PREFIXES = [
    'BINANCE:',
    'COINBASE:',
    'KRAKEN:',
    'GEMINI:',
    'BITFINEX:',
    'BITSTAMP:',
    'HUOBI:',
    'OKX:',
    'POLONIEX:',
    'BYBIT:'
];

function applyRangeButtons(activeRange){
    if(!pnlRangeButtons.size) return;
    pnlRangeButtons.forEach((button, range)=>{
        const isActive = range === activeRange;
        button.classList.toggle('active', isActive);
        button.setAttribute('aria-selected', isActive ? 'true' : 'false');
        button.setAttribute('tabindex', isActive ? '0' : '-1');
    });
    if(pnlRangeTabsContainer){
        pnlRangeTabsContainer.setAttribute('data-active-range', activeRange);
    }
    if(pnlCardLabelElement){
        const config = PNL_RANGE_CONFIG.find(item => item.key === activeRange);
        const nextLabel = activeRange === 'ALL'
            ? pnlCardLabelBase
            : (config?.label || pnlCardLabelBase);
        pnlCardLabelElement.textContent = nextLabel;
    }
}

function initializePnlRangeTabs(){
    if(typeof document === 'undefined') return;
    pnlRangeTabsContainer = document.getElementById('pnl-range-tabs');
    pnlRangeButtons.clear();
    if(!pnlRangeTabsContainer) return;
    if(!pnlCardLabelElement){
        pnlCardLabelElement = document.querySelector('#pnl-card .label');
        if(pnlCardLabelElement){
            pnlCardLabelBase = pnlCardLabelElement.textContent || pnlCardLabelBase;
        }
    }
    const buttons = pnlRangeTabsContainer.querySelectorAll('[data-pnl-range]');
    buttons.forEach(button=>{
        const range = button.dataset.pnlRange;
        if(!range) return;
        pnlRangeButtons.set(range, button);
        button.addEventListener('click', ()=>{
            setPnlRange(range);
        });
    });
    if(!pnlRangeButtons.has(pnlRange) && pnlRangeButtons.size){
        const first = pnlRangeButtons.keys().next().value;
        if(first){
            pnlRange = first;
        }
    }
    applyRangeButtons(pnlRange);
}

function setLoadingState(state, message){
    if(!loadingOverlay) return;
    const textEl = loadingOverlay.querySelector('.loading-text');
    loadingOverlay.classList.remove('error');
    if(state === 'error'){
        loadingOverlay.classList.add('error');
        if(textEl && message) textEl.textContent = message;
        loadingOverlay.classList.remove('hidden');
        return;
    }
    if(textEl && message) textEl.textContent = message;
    if(state === 'hidden'){
        loadingOverlay.classList.add('hidden');
    }else{
        loadingOverlay.classList.remove('hidden');
    }
}

function reportLoading(message){
    if(!message) return;
    if(isBootstrapping){
        setLoadingState('visible', message);
    }
    setStatus(message);
}

const CORS_PROXY_PREFIXES = ['https://r.jina.ai/'];

async function fetchJsonWithCorsFallback(url, options){
    const attempts = [url];
    if(typeof window !== 'undefined'){
        CORS_PROXY_PREFIXES.forEach(prefix=>{
            const proxied = `${prefix}${url}`;
            if(!attempts.includes(proxied)){
                attempts.push(proxied);
            }
        });
    }
    for(const target of attempts){
        try{
            const response = await fetch(target, options);
            if(!response.ok){
                continue;
            }
            const contentType = response.headers?.get?.('content-type') || '';
            const text = await response.text();
            if(!text){
                continue;
            }
            const trimmed = text.trim();
            if(!trimmed){
                continue;
            }
            if(contentType.includes('application/json') || trimmed.startsWith('{') || trimmed.startsWith('[')){
                try{
                    return JSON.parse(trimmed);
                }catch(parseError){
                    console.warn('Failed to parse JSON for', target, parseError);
                }
            }
        }catch(error){
            console.warn('fetchJsonWithCorsFallback error', target, error);
        }
    }
    return null;
}

// ----------------- UTIL ---------------------
function money(v){
    if(v===null || v===undefined || Number.isNaN(Number(v))) return '—';
    return '$' + Number(v).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});
}

function formatPriceTickValue(value){
    if(!Number.isFinite(value)) return '—';
    const abs = Math.abs(value);
    if(abs >= 1_000_000){
        const scaled = value / 1_000_000;
        const decimals = Math.abs(scaled) >= 100 ? 0 : 1;
        return `${scaled.toFixed(decimals)}M`;
    }
    if(abs >= 10_000){
        const scaled = value / 1000;
        const decimals = Math.abs(scaled) >= 100 ? 0 : 1;
        return `${scaled.toFixed(decimals)}K`;
    }
    if(abs >= 100){
        return value.toFixed(0);
    }
    if(abs >= 10){
        return value.toFixed(1);
    }
    return value.toFixed(2);
}

function pct(v){
    if(v===null || v===undefined || Number.isNaN(Number(v))) return '—';
    const num = Number(v);
    return `${num >= 0 ? '+' : ''}${num.toFixed(2)}%`;
}

function formatPercent(value, digits = 1){
    if(value === null || value === undefined || !Number.isFinite(Number(value))) return '—';
    const num = Number(value);
    const sign = num > 0 ? '+' : '';
    return `${sign}${num.toFixed(digits)}%`;
}

function formatMoneyWithPercent(amount, percent, digits = 1){
    if(amount === null || amount === undefined || !Number.isFinite(Number(amount))){
        return '—';
    }
    const numericAmount = Number(amount);
    const base = money(numericAmount);
    if(!showPnlPercentages) return base;
    if(percent === null || percent === undefined || !Number.isFinite(Number(percent))) return base;
    return `${base} (${formatPercent(percent, digits)})`;
}

function monthsBetween(start, end){
    if(!(start instanceof Date) || !(end instanceof Date)) return 0;
    let months = (end.getFullYear() - start.getFullYear()) * 12 + (end.getMonth() - start.getMonth());
    if(end.getDate() >= start.getDate()) months += 1;
    return Math.max(months, 1);
}

function formatDurationFromMonths(months){
    if(months === null || months === undefined || !Number.isFinite(months)) return '—';
    if(months <= 0) return 'Paid off';
    const total = Math.ceil(months);
    const years = Math.floor(total / 12);
    const remainder = total % 12;
    const parts = [];
    if(years) parts.push(`${years}y`);
    if(remainder) parts.push(`${remainder}m`);
    if(!parts.length) return '<1m';
    return parts.join(' ');
}

function formatDateShort(date){
    if(!(date instanceof Date) || Number.isNaN(date.getTime())) return '—';
    return date.toLocaleDateString(undefined,{year:'numeric', month:'short', day:'numeric'});
}

function formatQty(qty){
    if(qty===null || qty===undefined) return '—';
    const abs = Math.abs(qty);
    if(abs >= 1000) return qty.toFixed(0);
    if(abs >= 100) return qty.toFixed(1);
    if(abs >= 10) return qty.toFixed(2);
    return qty.toFixed(4).replace(/0+$/,'').replace(/\.$/,'');
}

function startOfDay(value){
    const date = value instanceof Date ? new Date(value) : new Date(value);
    if(Number.isNaN(date.getTime())){
        const fallback = new Date();
        fallback.setHours(0, 0, 0, 0);
        return fallback;
    }
    date.setHours(0, 0, 0, 0);
    return date;
}

function formatCompactMoney(value){
    if(value === null || value === undefined || !Number.isFinite(value)){
        return '$0';
    }
    const sign = value < 0 ? '-' : '';
    const abs = Math.abs(value);
    const format = (num, suffix)=>{
        const trimmed = Number.isInteger(num) ? num.toString() : num.toFixed(1).replace(/\.0$/, '');
        return `${sign}$${trimmed}${suffix}`;
    };
    if(abs >= 1e12) return format(abs / 1e12, 't');
    if(abs >= 1e9) return format(abs / 1e9, 'b');
    if(abs >= 1e6) return format(abs / 1e6, 'm');
    if(abs >= 1e3) return format(abs / 1e3, 'k');
    return `${sign}$${Math.round(abs).toString()}`;
}

function getRangeStartDate(range){
    if(range === 'ALL') return null;
    const now = new Date();
    const start = new Date(now);
    start.setHours(0, 0, 0, 0);
    switch(range){
        case '1D':
            start.setDate(start.getDate() - 1);
            break;
        case '1W':{
            const weekday = start.getDay(); // Sunday = 0, Monday = 1
            const offsetFromMonday = (weekday + 6) % 7;
            start.setDate(start.getDate() - offsetFromMonday);
            break;
        }
        case '1M':
            start.setDate(1);
            break;
        case '1Y':
            start.setMonth(0, 1);
            break;
        default:
            return null;
    }
    return start;
}

function getCategoryDisplayName(categoryKey){
    switch(categoryKey){
        case 'crypto':
            return 'Crypto';
        case 'stock':
            return 'Stocks';
        case 'realEstate':
            return 'Real Estate';
        default:
            if(!categoryKey) return 'Assets';
            const spaced = String(categoryKey)
                .replace(/([A-Z])/g, ' $1')
                .replace(/[_-]+/g, ' ')
                .trim();
            return spaced ? spaced.charAt(0).toUpperCase() + spaced.slice(1) : 'Assets';
    }
}

function getRealEstateRangePnl(position, range){
    if(!position) return 0;
    if(range === 'ALL'){
        if(!position.rentRangeTotals){
            position.rentRangeTotals = {};
        }
        const total = Number(position.rentRealized || 0) || 0;
        position.rentRangeTotals.ALL = total;
        return total;
    }
    if(!position.rentRangeTotals){
        position.rentRangeTotals = {};
    }
    if(position.rentRangeTotals[range] !== undefined){
        return position.rentRangeTotals[range];
    }
    const rangeStart = getRangeStartDate(range);
    if(!(rangeStart instanceof Date)){
        const total = Number(position.rentRealized || 0) || 0;
        position.rentRangeTotals[range] = total;
        return total;
    }
    const operations = Array.isArray(position.operations) ? position.operations : [];
    const total = operations.reduce((sum, op)=>{
        if(!op || !op.isRent) return sum;
        const opDate = op.date instanceof Date ? op.date : (op.rawDate ? new Date(op.rawDate) : null);
        if(!(opDate instanceof Date) || Number.isNaN(opDate.getTime())) return sum;
        if(opDate < rangeStart) return sum;
        const amount = Number(op.spent || 0);
        if(!Number.isFinite(amount)) return sum;
        return sum + (amount < 0 ? -amount : amount);
    }, 0);
    position.rentRangeTotals[range] = total;
    return total;
}

function computeCategoryBest(categoryKey, context = {}){
    const label = getCategoryDisplayName(categoryKey);
    const normalizedKey = categoryKey === 'realEstate' ? 'real estate' : categoryKey;
    const eligiblePositions = positions.filter(position=>{
        if(!position) return false;
        const type = (position.type || '').toLowerCase();
        if(categoryKey === 'realEstate'){
            return type === 'real estate';
        }
        return type === normalizedKey;
    }).filter(position=>{
        if(categoryKey === 'realEstate'){
            return true;
        }
        const qty = Number(position.qty || 0);
        const marketValue = Number(position.marketValue || 0);
        return Math.abs(qty) > 1e-6 || Math.abs(marketValue) > 1e-2;
    });
    if(!eligiblePositions.length){
        return null;
    }
    let best = null;
    eligiblePositions.forEach(position=>{
        let pnlValue;
        if(categoryKey === 'realEstate'){
            pnlValue = getRealEstateRangePnl(position, pnlRange);
        }else{
            const raw = position.rangePnl ?? position.pnl;
            pnlValue = Number(raw);
        }
        if(!Number.isFinite(pnlValue)) return;
        if(best === null || pnlValue > best.pnl){
            best = { position, pnl: pnlValue };
        }
    });
    if(!best){
        return null;
    }
    const position = best.position;
    const displayName = position.displayName || position.Symbol || position.Name || position.id || '—';
    const priceCandidates = [
        position.displayPrice,
        position.currentPrice,
        position.lastKnownPrice,
        position.lastPurchasePrice,
        position.avgPrice
    ].map(value=> Number(value)).filter(value=> Number.isFinite(value));
    const prioritizedPrice = priceCandidates.find(value => Math.abs(value) > 1e-9);
    const fallbackPrice = Number(position.displayPrice ?? position.currentPrice ?? position.lastKnownPrice ?? position.avgPrice ?? position.lastPurchasePrice ?? 0) || 0;
    const price = Number.isFinite(prioritizedPrice) ? prioritizedPrice : fallbackPrice;
    const qty = Number(position.qty || 0);
    const marketValue = Number(position.marketValue || 0);
    let changePct = Number(position.rangeChangePct);
    if(!Number.isFinite(changePct)){
        changePct = Number(position.changePct);
    }
    const rangeLabel = RANGE_LABELS[pnlRange] || pnlRange;
    const rangeStart = getRangeStartDate(pnlRange);
    const result = {
        id: position.id,
        label,
        position,
        displayName,
        pnl: best.pnl,
        changePct: Number.isFinite(changePct) ? changePct : null,
        rangeLabel,
        rangeStart,
        meta: '',
        extraItems: []
    };
    if(categoryKey === 'realEstate'){
        const analytics = context.realEstateAnalytics;
        const stat = analytics && Array.isArray(analytics.rows)
            ? analytics.rows.find(row => row.positionRef === position)
            : null;
        const rentTotal = stat ? Number(stat.rentCollected || 0) : Number(position.rentRealized || 0);
        const metaParts = [];
        const utilization = stat && Number.isFinite(stat.utilization)
            ? Math.max(0, Math.min(Number(stat.utilization), 100))
            : null;
        if(stat && stat.category){
            metaParts.push(`Category ${stat.category}`);
        }else if(position.type){
            metaParts.push(position.type);
        }
        if(utilization !== null){
            metaParts.push(`Utilization ${utilization.toFixed(1)}%`);
        }
        if(metaParts.length){
            result.meta = metaParts.join(' · ');
        }else{
            result.meta = `Rent total ${money(rentTotal)}`;
        }
        const extraItems = [];
        extraItems.push({ label: 'Rent total', value: money(rentTotal) });
        if(stat){
            extraItems.push({ label: 'Rent YTD', value: money(stat.rentYtd) });
            if(Number.isFinite(stat.avgMonthlyRent)){
                extraItems.push({ label: 'Avg rent/mo', value: money(stat.avgMonthlyRent) });
            }
            extraItems.push({ label: 'Projected value', value: money(stat.projectedValue) });
            if(Number.isFinite(stat.netOutstanding)){
                extraItems.push({ label: 'Outstanding', value: money(stat.netOutstanding) });
            }
            if(Number.isFinite(stat.payoffMonths)){
                extraItems.push({ label: 'Payoff ETA', value: formatDurationFromMonths(stat.payoffMonths) });
            }
        }else{
            extraItems.push({ label: 'Rent realized', value: money(position.rentRealized || 0) });
        }
        const totalMarketValue = Number(context.categoryTotal || 0);
        if(Number.isFinite(totalMarketValue) && totalMarketValue > 0 && Number.isFinite(marketValue) && marketValue > 0){
            const share = (marketValue / totalMarketValue) * 100;
            extraItems.push({ label: 'Category share', value: `${share.toFixed(1)}%` });
        }
        result.extraItems = extraItems;
        return result;
    }
    const metaParts = [`Qty ${formatQty(qty)}`, `Price ${money(price)}`];
    result.meta = metaParts.join(' · ');
    const extraItems = [
        { label: 'Market value', value: money(marketValue) }
    ];
    const totalCategoryValue = Number(context.categoryTotal || 0);
    if(Number.isFinite(totalCategoryValue) && totalCategoryValue > 0 && Number.isFinite(marketValue) && marketValue > 0){
        const share = (marketValue / totalCategoryValue) * 100;
        extraItems.push({ label: 'Category share', value: `${share.toFixed(1)}%` });
    }
    const realized = Number(position.realized || 0);
    if(Number.isFinite(realized) && Math.abs(realized) > 1e-2){
        extraItems.push({ label: 'Realized P&L', value: money(realized) });
    }
    const reinvestedValue = Number(position.reinvestedValue || 0);
    if(Number.isFinite(reinvestedValue) && Math.abs(reinvestedValue) > 1e-2){
        extraItems.push({ label: 'Reinvested', value: money(reinvestedValue) });
    }
    result.extraItems = extraItems;
    return result;
}

function updateBestPerformerCache(realEstateAnalytics){
    const totals = {
        crypto: positions.filter(p => (p.type || '').toLowerCase() === 'crypto').reduce((sum, p)=> sum + Number(p.marketValue || 0), 0),
        stock: positions.filter(p => (p.type || '').toLowerCase() === 'stock').reduce((sum, p)=> sum + Number(p.marketValue || 0), 0),
        realEstate: positions.filter(p => (p.type || '').toLowerCase() === 'real estate').reduce((sum, p)=> sum + Number(p.marketValue || 0), 0)
    };
    bestPerformerCache.crypto = computeCategoryBest('crypto', { categoryTotal: totals.crypto });
    bestPerformerCache.stock = computeCategoryBest('stock', { categoryTotal: totals.stock });
    bestPerformerCache.realEstate = computeCategoryBest('realEstate', {
        categoryTotal: totals.realEstate,
        realEstateAnalytics
    });
}

function deriveCryptoIconKey(position){
    const candidates = [position.Symbol, position.finnhubSymbol, position.displayName, position.Name, position.id];
    for(const value of candidates){
        if(!value) continue;
        let token = String(value).toUpperCase().trim();
        if(!token) continue;
        if(token.includes(':')){
            const parts = token.split(':');
            token = parts[parts.length - 1];
        }
        if(token.includes('/')){
            token = token.split('/')[0];
        }
        const quoteSymbols = ['USDT','USD','USDC','BUSD','EUR','GBP','BTC','ETH','DAI','AUD','CAD','JPY','KRW','TRY','CHF','SGD','MXN'];
        for(const quote of quoteSymbols){
            if(token.endsWith(quote)){ token = token.slice(0, -quote.length); break; }
        }
        token = token.replace(/[^A-Z0-9]/g,'');
        if(token.length >= 2 && token.length <= 10){
            return token.toLowerCase();
        }
    }
    return null;
}

function deriveStockIconKey(position){
    const candidates = [position.Symbol, position.finnhubSymbol, position.displayName, position.Name, position.id];
    for(const value of candidates){
        if(!value) continue;
        let token = String(value).trim().toUpperCase();
        if(!token) continue;
        if(token.includes(':')){
            const parts = token.split(':');
            token = parts[parts.length - 1];
        }
        token = token.replace(/[^A-Z0-9.-]/g, '');
        if(token.length >= 1 && token.length <= 8){
            return token;
        }
    }
    return null;
}

function deriveAssetInitial(position){
    if(!position) return '??';
    const candidates = [position.displayName, position.Symbol, position.Name, position.id];
    for(const value of candidates){
        if(value === undefined || value === null) continue;
        const raw = String(value).trim();
        if(!raw) continue;
        const wordParts = raw.split(/[^A-Za-z0-9]+/).filter(Boolean);
        if(wordParts.length >= 2){
            const first = wordParts[0].replace(/[^A-Za-z0-9]/g,'');
            const second = wordParts[1].replace(/[^A-Za-z0-9]/g,'');
            const combo = (first[0] || '') + (second[0] || '');
            if(combo){
                return combo.toUpperCase();
            }
        }
        const cleaned = raw.replace(/[^A-Za-z0-9]/g,'');
        if(cleaned.length){
            const length = cleaned.length >= 2 ? 2 : 1;
            return cleaned.slice(0, length).toUpperCase();
        }
    }
    return '??';
}

function resolveAssetIcon(position){
    if(!position) return null;
    const typeKey = String(position.type || '').toLowerCase();
    const overrides = {
        GOOG: 'assets/tickers/goog.svg',
        GOOGLE: 'assets/tickers/goog.svg',
        ISAC: 'assets/tickers/isac.svg',
        IBIT: 'assets/tickers/ibit.svg'
    };
    const rawSymbol = position.Symbol || position.id || position.displayName || '';
    const symbolUpper = rawSymbol.toUpperCase();
    if(symbolUpper){
        const symbolCandidates = [
            symbolUpper,
            symbolUpper.includes('.') ? symbolUpper.split('.')[0] : null,
            symbolUpper.includes(' ') ? symbolUpper.split(' ')[0] : null,
            symbolUpper.replace(/[^A-Z0-9]/g, '')
        ].filter(candidate => candidate && candidate.length);
        const matchedKey = symbolCandidates.find(candidate => overrides[candidate]);
        if(matchedKey){
            const overridePath = overrides[matchedKey];
            return { sources: [overridePath], alt: `${matchedKey} logo`, override: overridePath };
        }
    }
    if(typeKey === 'real estate'){
        return { sources: ['assets/real-estate.svg'], alt: 'Real estate asset' };
    }
    if(['car','vehicle','auto','automobile','transport','automotive'].includes(typeKey)){
        return { sources: ['assets/car.svg'], alt: 'Vehicle asset' };
    }
    if(typeKey === 'crypto'){
        const symbolKey = deriveCryptoIconKey(position);
        if(symbolKey){
            return {
                sources: CRYPTO_ICON_PROVIDERS.map(provider => provider(symbolKey)),
                alt: `${symbolKey.toUpperCase()} icon`
            };
        }
    }
    if(typeKey === 'stock'){
        const stockKey = deriveStockIconKey(position);
        if(stockKey){
            const cleanSecondary = stockKey.toLowerCase().replace(/[^a-z0-9]/g,'');
            const sources = [
                `https://storage.googleapis.com/iex/api/logos/${encodeURIComponent(stockKey)}.svg`,
                cleanSecondary ? `https://raw.githubusercontent.com/andreasbm/web-logos/master/logos/${cleanSecondary}.svg` : null
            ].filter(Boolean);
            if(sources.length){
                return {
                    sources,
                    alt: `${stockKey} logo`
                };
            }
        }
    }
    return null;
}

function getIconCacheKey(position){
    if(!position) return null;
    if(position.iconCacheKey) return position.iconCacheKey;
    const candidates = [position.id, position.Symbol, position.displayName, position.Name];
    const key = candidates.find(value => value !== undefined && value !== null && String(value).trim().length);
    if(key !== undefined && key !== null){
        const resolved = String(key).trim();
        position.iconCacheKey = resolved;
        return resolved;
    }
    return null;
}

function createAssetIconElement(position){
    const wrapper = document.createElement('span');
    wrapper.className = 'asset-icon-wrapper';
    wrapper.setAttribute('aria-hidden','true');

    const labelCandidates = [position?.displayName, position?.Symbol, position?.Name];
    const tooltipLabel = labelCandidates.find(text => text !== undefined && text !== null && String(text).trim().length) || 'Asset';
    wrapper.title = String(tooltipLabel).trim();

    const fallbackInitial = deriveAssetInitial(position);
    let img = null;

    const applyFallback = ()=>{
        if(img){
            img.removeAttribute('src');
            img.remove();
            img = null;
        }
        if(wrapper.dataset.iconState === 'fallback'){
            return wrapper;
        }
        wrapper.dataset.iconState = 'fallback';
        wrapper.classList.add('asset-icon-fallback');
        wrapper.innerHTML = '';
        const textEl = document.createElement('span');
        textEl.className = 'asset-icon-fallback-text';
        textEl.textContent = fallbackInitial;
        wrapper.appendChild(textEl);
        return wrapper;
    };

    const icon = resolveAssetIcon(position);
    if(icon && icon.override){
        wrapper.dataset.plateOverride = icon.override;
    }
    if(!icon || !Array.isArray(icon.sources) || !icon.sources.length){
        const cacheKeyFallback = getIconCacheKey(position);
        if(cacheKeyFallback && !assetIconSourceCache.has(cacheKeyFallback)){
            assetIconSourceCache.set(cacheKeyFallback, null);
        }
        return applyFallback();
    }

    img = document.createElement('img');
    img.className = 'asset-icon';
    img.alt = icon.alt || '';
    img.loading = 'eager';
    img.decoding = 'sync';
    img.referrerPolicy = 'no-referrer';
    img.width = 36;
    img.height = 36;
    const cacheKey = getIconCacheKey(position);
    if(cacheKey && position.iconCacheKey !== cacheKey){
        position.iconCacheKey = cacheKey;
    }
    if(icon.override && cacheKey && assetIconSourceCache.get(cacheKey) === null){
        assetIconSourceCache.delete(cacheKey);
    }
    if(cacheKey && assetIconSourceCache.has(cacheKey)){
        const cachedSource = assetIconSourceCache.get(cacheKey);
        if(cachedSource){
            img.src = cachedSource;
            wrapper.appendChild(img);
            wrapper.dataset.iconState = 'image';
            wrapper.classList.remove('asset-icon-fallback');
            return wrapper;
        }
        return applyFallback();
    }

    const sources = icon.sources.slice();
    let index = 0;
    const loadNextSource = ()=>{
        if(index >= sources.length){
            if(cacheKey){
                assetIconSourceCache.set(cacheKey, null);
            }
            applyFallback();
            return;
        }
        img.src = sources[index];
        index += 1;
    };

    img.addEventListener('error', ()=>{
        loadNextSource();
    });

    img.addEventListener('load', ()=>{
        if(cacheKey && !assetIconSourceCache.has(cacheKey)){
            const resolvedSrc = img.currentSrc || img.src;
            if(resolvedSrc){
                assetIconSourceCache.set(cacheKey, resolvedSrc);
            }
        }
        wrapper.dataset.iconState = 'image';
        wrapper.classList.remove('asset-icon-fallback');
        wrapper.innerHTML = '';
        wrapper.appendChild(img);
    }, { once: true });

    wrapper.appendChild(img);
    loadNextSource();
    return wrapper;
}

function getAssetPlateImage(position){
    const cacheKey = getIconCacheKey(position);
    if(cacheKey && assetIconSourceCache.has(cacheKey)){
        const cached = assetIconSourceCache.get(cacheKey);
        if(cached){
            return cached;
        }
    }
    const icon = resolveAssetIcon(position);
    if(icon && Array.isArray(icon.sources) && icon.sources.length){
        return icon.sources[0];
    }
    return null;
}

function ensurePositionDefaults(position){
    if(position.displayPrice === undefined){
        const base = position.currentPrice ?? position.lastKnownPrice ?? position.lastPurchasePrice ?? position.avgPrice ?? 0;
        position.displayPrice = base;
    }
    if(position.priceDirection === undefined){
        position.priceDirection = null;
    }
    if(position.rentRealized === undefined){
        position.rentRealized = 0;
    }
    if(position.reinvested === undefined){
        position.reinvested = 0;
    }
}

function applyLivePrice(position, price){
    if(price === undefined || price === null || Number(price) === 0){
        if(!position.priceStatus){
            position.priceStatus = 'Price failed to update';
        }
        return;
    }
    ensurePositionDefaults(position);
    const previous = position.displayPrice;
    if(previous !== undefined && previous !== null && price !== previous){
        position.priceDirection = price > previous ? 'up' : 'down';
    }else{
        position.priceDirection = null;
    }
    position.displayPrice = price;
    position.currentPrice = price;
    position.lastPriceUpdate = Date.now();
    position.priceStatus = null;
}

function applyPriceUpdate(position, price, prevClose, source){
    if(!Number.isFinite(price) || price <= 0){
        return false;
    }
    applyLivePrice(position, price);
    if(Number.isFinite(prevClose) && prevClose > 0){
        position.prevClose = prevClose;
    }
    const reference = Number.isFinite(position.prevClose) && Math.abs(position.prevClose) > 1e-9
        ? position.prevClose
        : (Number.isFinite(position.avgPrice) && Math.abs(position.avgPrice) > 1e-9
            ? position.avgPrice
            : (Number.isFinite(position.lastKnownPrice) ? position.lastKnownPrice : price));
    if(Number.isFinite(reference) && Math.abs(reference) > 1e-9){
        position.change = price - reference;
        position.changePct = reference ? ((price - reference) / reference) * 100 : 0;
    }else{
        position.change = 0;
        position.changePct = 0;
    }
    if(source){
        position.priceSource = source;
    }
    position.priceStatus = null;
    recomputePositionMetrics(position);
    return true;
}

function flashElement(element, direction){
    if(!element || !direction) return;
    element.classList.remove('flash-up','flash-down','flash-up-text','flash-down-text');
    void element.offsetWidth; // force reflow
    requestAnimationFrame(()=>{
        element.classList.add(direction === 'up' ? 'flash-up-text' : 'flash-down-text');
        setTimeout(()=>{
            element.classList.remove('flash-up','flash-down','flash-up-text','flash-down-text');
        }, FLASH_DURATION);
    });
}

function setMoneyWithFlash(elementId, value, key){
    const el = document.getElementById(elementId);
    if(!el) return;
    const prev = previousKpiValues[key];
    el.textContent = money(value);
    if(prev !== null && value !== prev){
        flashElement(el, value > prev ? 'up' : 'down');
    }
    previousKpiValues[key] = value;
}

function setCategoryPnl(elementId, value, key){
    const el = document.getElementById(elementId);
    if(!el) return;
    el.classList.remove('delta-positive','delta-negative');
    const numeric = Number(value || 0);
    const totals = currentCategoryRangeTotals || {};
    const grandTotal = Number(totals.crypto || 0) + Number(totals.stock || 0) + Number(totals.realEstate || 0);
    const pct = grandTotal ? (numeric / grandTotal) * 100 : null;
    el.textContent = formatMoneyWithPercent(numeric, pct, 1);
    if(numeric > 0){
        el.classList.add('delta-positive');
    }else if(numeric < 0){
        el.classList.add('delta-negative');
    }
}

function formatCategorySummaryPnl(amount, categoryMarketValue){
    if(amount === null || amount === undefined || !Number.isFinite(Number(amount))){
        return '—';
    }
    const numericAmount = Number(amount);
    const denominator = Number(categoryMarketValue);
    const percent = Number.isFinite(denominator) && Math.abs(denominator) > 1e-6
        ? (numericAmount / denominator) * 100
        : null;
    return formatMoneyWithPercent(numericAmount, percent, 1);
}

function updateThemeToggleIcon(button, isLight){
    if(!button) return;
    const icon = button.querySelector('.theme-icon');
    const srOnly = button.querySelector('.sr-only');
    const label = isLight ? 'Switch to dark mode' : 'Switch to light mode';
    if(icon) icon.innerHTML = isLight ? '&#9790;' : '&#9728;';
    button.setAttribute('aria-label', label);
    if(srOnly) srOnly.textContent = label;
}

function formatTime(date){
    if(!(date instanceof Date)) return '—';
    const hours = String(date.getHours()).padStart(2,'0');
    const minutes = String(date.getMinutes()).padStart(2,'0');
    const seconds = String(date.getSeconds()).padStart(2,'0');
    return `${hours}:${minutes}:${seconds}`;
}

function getReferenceBucket(position){
    if(!position.referencePrices) position.referencePrices = {};
    return position.referencePrices;
}

async function ensureRangeReference(position, range){
    const bucket = getReferenceBucket(position);
    if(range === 'ALL'){
        const base = position.avgPrice ?? position.lastKnownPrice ?? position.displayPrice ?? 0;
        bucket.ALL = base;
        return base;
    }
    if(range === '1D'){
        if(position.prevClose !== undefined && position.prevClose !== null && Number.isFinite(position.prevClose) && position.prevClose > 0){
            bucket['1D'] = position.prevClose;
            return position.prevClose;
        }
        if(bucket['1D'] !== undefined){
            return bucket['1D'];
        }
    }
    if(bucket[range] !== undefined){
        return bucket[range];
    }
    if(!FINNHUB_KEY || !position.finnhubSymbol){
        const fallback = position.avgPrice ?? position.lastKnownPrice ?? position.displayPrice ?? 0;
        bucket[range] = fallback;
        return fallback;
    }
    const cacheKey = `${position.finnhubSymbol}_${range}`;
    if(referencePriceCache.has(cacheKey)){
        const cached = referencePriceCache.get(cacheKey);
        bucket[range] = cached;
        return cached;
    }
    const now = Math.floor(Date.now()/1000);
    const lookback = RANGE_LOOKBACK[range] ?? 0;
    const from = now - lookback - 3600; // pad an hour to ensure candle availability
    const endpoint = isCryptoFinnhubSymbol(position.finnhubSymbol, position.type) ? 'crypto/candle' : 'stock/candle';
    const url = `${FINNHUB_REST}/${endpoint}?symbol=${encodeURIComponent(position.finnhubSymbol)}&resolution=D&from=${from}&to=${now}&token=${FINNHUB_KEY}`;
    try{
        const res = await fetch(url);
        if(res.ok){
            const data = await res.json();
            if(data && data.s === 'ok' && Array.isArray(data.c) && data.c.length){
                const refPrice = data.c[0] ?? data.c[data.c.length-1];
                referencePriceCache.set(cacheKey, refPrice);
                bucket[range] = refPrice;
                return refPrice;
            }
        }
    }catch(error){
        console.warn('Reference price fetch failed', position.finnhubSymbol, range, error);
    }
    const fallback = position.avgPrice ?? position.lastKnownPrice ?? position.displayPrice ?? 0;
    bucket[range] = fallback;
    return fallback;
}

function recomputeRangeMetrics(range){
    let total = 0;
    const categoryTotals = { crypto: 0, stock: 0, realEstate: 0, other: 0 };
    positions.forEach(position=>{
        ensurePositionDefaults(position);
        const bucket = getReferenceBucket(position);
        let base;
        if(range === 'ALL'){
            base = bucket.ALL ?? position.avgPrice ?? position.lastKnownPrice ?? position.displayPrice ?? 0;
            bucket.ALL = base;
        }else if(range === '1D'){
            base = bucket['1D'] ?? position.prevClose ?? position.avgPrice ?? position.lastKnownPrice ?? position.displayPrice ?? 0;
            bucket['1D'] = base;
        }else{
            base = bucket[range] ?? position.avgPrice ?? position.lastKnownPrice ?? position.displayPrice ?? 0;
        }
        const price = Number(position.displayPrice ?? position.currentPrice ?? position.lastKnownPrice ?? position.avgPrice ?? 0);
        const qty = Number(position.qty || 0);
        const prevRange = position.rangePnl ?? null;
        const unrealized = (price - base) * qty;
        const realized = Number(position.realized || 0);
        const reinvestedQty = Math.max(0, Number(position.reinvested || 0));
        const reinvestBasePrice = Math.abs(price) > 1e-9 ? price : base;
        const reinvestedValue = (reinvestedQty > 1e-6 && Math.abs(reinvestBasePrice) > 1e-9) ? reinvestBasePrice * reinvestedQty : 0;
        let pnl;
        if(range === '1D'){
            pnl = unrealized;
        }else{
            pnl = unrealized + realized + reinvestedValue;
        }
        const typeKey = (position.type || '').toLowerCase();
        if(typeKey === 'real estate'){
            const rentPnl = getRealEstateRangePnl(position, range);
            pnl = rentPnl;
            categoryTotals.realEstate += rentPnl;
            position.rangeReinvestedValue = 0;
        }else if(typeKey === 'crypto'){
            categoryTotals.crypto += pnl;
            position.rangeReinvestedValue = reinvestedValue;
        }else if(typeKey === 'stock'){
            categoryTotals.stock += pnl;
            position.rangeReinvestedValue = reinvestedValue;
        }else{
            categoryTotals.other += pnl;
            position.rangeReinvestedValue = reinvestedValue;
        }
        position.rangePnl = pnl;
        const baseValue = base * qty;
        let denominator = baseValue !== 0 ? baseValue : Math.abs(position.invested) || Math.abs(realized) || 1;
        if(typeKey === 'real estate'){
            denominator = Math.abs(position.invested) || Math.abs(pnl) || 1;
        }
        position.rangeBaseDenominator = denominator;
        position.rangeChangePct = denominator ? (pnl / denominator) * 100 : 0;
        if(prevRange !== null){
            position.rangeDirection = pnl > prevRange ? 'up' : pnl < prevRange ? 'down' : null;
        }else{
            position.rangeDirection = null;
        }
        total += pnl;
    });
    currentRangeTotalPnl = total;
    currentCategoryRangeTotals = categoryTotals;
    rangeDirty = false;
}

async function setPnlRange(range){
    const normalized = PNL_RANGE_CONFIG.some(item=> item.key === range) ? range : 'ALL';
    if(isRangeUpdateInFlight || normalized === pnlRange) return;
    isRangeUpdateInFlight = true;
    try{
        pnlRange = normalized;
        applyRangeButtons(pnlRange);
        renderPnlTrendChart(pnlRange);
        if(pnlRange !== 'ALL'){
            const tasks = positions.map(position => ensureRangeReference(position, pnlRange));
            await Promise.all(tasks);
        }
        rangeDirty = true;
        scheduleUIUpdate({immediate:true});
    }finally{
        isRangeUpdateInFlight = false;
    }
}

function normalizeCategory(category, asset){
    if(!category && asset){
        if(asset.toLowerCase()==='cash') return 'Cash';
    }
    if(!category) return 'Unclassified';
    const map = {
        'crypto':'Crypto',
        'stock':'Stock',
        'real estate':'Real Estate',
        'cash':'Cash',
        'deposit':'Cash',
        'automobile':'Automobile',
        'automotive':'Automobile',
        'vehicle':'Automobile',
        'auto':'Automobile',
        'car':'Automobile',
        'transport':'Automobile'
    };
    const key = String(category).toLowerCase();
    return map[key] || category;
}

function mapFinnhubSymbol(asset, category, isOverride = false){
    if(!asset) return null;
    if(isOverride) return asset;
    const cat = (category||'').toLowerCase();
    const cleaned = String(asset).trim().toUpperCase();
    const overrides = {
        ISAC: 'LSE:ISAC',
        'NASDAQ:ISAC': 'LSE:ISAC'
    };
    if(overrides[cleaned]) return overrides[cleaned];
    if(cat==='cash') return null;
    if(cat==='crypto'){
        const base = cleaned.replace(/[^A-Z0-9]/g,'');
        if(!base || base.length < 2 || base.length > 10) return null;
        return `BINANCE:${base}USDT`;
    }
    if(cleaned.includes(':')) return cleaned;
    if(/[A-Z0-9]{1,5}/.test(cleaned) && !cleaned.includes(' ')) return cleaned;
    return null;
}

function isCryptoFinnhubSymbol(symbol, type){
    if(!symbol){
        return String(type || '').toLowerCase() === 'crypto';
    }
    const typeKey = String(type || '').toLowerCase();
    if(typeKey === 'crypto'){
        return true;
    }
    const upper = String(symbol).toUpperCase();
    return CRYPTO_FH_PREFIXES.some(prefix => upper.startsWith(prefix));
}

function shouldPreferYahoo(position){
    if(!position) return false;
    const symbol = (position.finnhubSymbol || position.Symbol || position.id || '').toUpperCase();
    return symbol === 'ISAC.L' || symbol === 'LSE:ISAC' || symbol === 'NASDAQ:ISAC' || symbol === 'ISAC';
}

function mapYahooSymbol(position){
    const typeKey = String(position.type || '').toLowerCase();
    const rawSymbol = position.finnhubSymbol || position.Symbol || position.displayName || position.Name;
    if(!rawSymbol) return null;
    if(typeKey === 'crypto'){
        let token = rawSymbol;
        if(token.includes(':')) token = token.split(':').pop();
        token = token.replace(/[^a-z0-9]/gi,'').toUpperCase();
        const stableSuffixes = ['USDT','USDC','BUSD','USD'];
        let base = token;
        const suffix = stableSuffixes.find(s=> token.endsWith(s));
        if(suffix){
            base = token.slice(0, token.length - suffix.length);
        }
        if(!base) base = token;
        return `${base}-USD`;
    }
    let symbol = rawSymbol.replace(/\s+/g,'').toUpperCase();
    const overrides = {
        'NASDAQ:ISAC': 'ISAC.L',
        'ISAC': 'ISAC.L',
        'LSE:ISAC': 'ISAC.L'
    };
    if(overrides[symbol]) return overrides[symbol];
    if(symbol.includes(':')){
        symbol = symbol.replace(':','-');
    }
    return symbol;
}

function mapCoinGeckoId(position){
    const explicit = position.coinGeckoId || position.coinGecko || position.coingecko;
    if(explicit) return String(explicit).toLowerCase();
    const candidates = [position.Symbol, position.displayName, position.Name, position.id].filter(Boolean);
    if(!candidates.length) return null;
    const known = new Map([
        ['btc','bitcoin'], ['bitcoin','bitcoin'],
        ['xbt','bitcoin'],
        ['eth','ethereum'], ['ethereum','ethereum'],
        ['sol','solana'], ['solana','solana'],
        ['ada','cardano'], ['cardano','cardano'],
        ['dot','polkadot'], ['polkadot','polkadot'],
        ['doge','dogecoin'], ['dogecoin','dogecoin'],
        ['matic','matic-network'], ['polygon','matic-network'],
        ['xrp','ripple'],
        ['ltc','litecoin'], ['litecoin','litecoin'],
        ['bnb','binancecoin'], ['bch','bitcoin-cash'],
        ['avax','avalanche-2'], ['atom','cosmos'],
        ['link','chainlink'], ['uni','uniswap']
    ]);

    const normalize = value => {
        if(!value) return '';
        let raw = String(value).trim().toLowerCase();
        if(raw.includes(':')) raw = raw.split(':').pop();
        raw = raw.replace(/[^a-z0-9-]+/g,'-');
        raw = raw.replace(/--+/g,'-').replace(/^-|-$/g,'');
        if(raw.endsWith('-usdt') || raw.endsWith('-usd')){
            raw = raw.replace(/-(usdt|usd)$/,'');
        }
        if(raw.endsWith('usdt') || raw.endsWith('usd')){
            raw = raw.replace(/(usdt|usd)$/,'');
        }
        return raw;
    };

    for(const candidate of candidates){
        const normalized = normalize(candidate);
        if(!normalized) continue;
        if(known.has(normalized)) return known.get(normalized);
        const hyphenLess = normalized.replace(/-/g,'');
        if(known.has(hyphenLess)) return known.get(hyphenLess);
        if(known.has(normalized.toUpperCase())) return known.get(normalized.toUpperCase());
        if(known.has(normalized.toLowerCase())) return known.get(normalized.toLowerCase());
        if(normalized.includes('-')){
            const base = normalized.split('-')[0];
            if(known.has(base)) return known.get(base);
        }
        return normalized;
    }
    return null;
}

async function refineFinnhubSymbols(progressCb){
    if(!FINNHUB_KEY || typeof fetch !== 'function') return;
    const unresolved = positions.filter(position=>{
        const cat = (position.type || '').toLowerCase();
        if(cat === 'cash' || position.finnhubOverride) return false;
        const symbol = position.finnhubSymbol || '';
        if(cat === 'crypto'){
            if(!symbol) return true;
            const heuristic = /^BINANCE:[A-Z0-9]+USDT$/;
            return heuristic.test(symbol);
        }
        if(!symbol) return true;
        if(symbol.includes(':')) return false;
        return true;
    });
    const total = unresolved.length;
    if(typeof progressCb === 'function' && total){
        progressCb(0, total);
    }
    if(!total) return;

    const queue = unresolved.slice();
    let processed = 0;
    const MAX_SYMBOL_CONCURRENCY = 6;

    async function worker(){
        while(queue.length){
            const position = queue.shift();
            if(!position) break;
            try{
                const resolved = await fetchFinnhubSymbol(position);
                if(resolved){
                    position.finnhubSymbol = resolved;
                    position.Symbol = resolved;
                }
            }catch(error){
                console.warn('Finnhub search failed', position.Name, error);
            }finally{
                processed += 1;
                if(typeof progressCb === 'function' && total){
                    progressCb(processed, total, position);
                }
                if(queue.length){
                    await new Promise(resolve=>setTimeout(resolve, 80));
                }
            }
        }
    }

    const workerCount = Math.min(MAX_SYMBOL_CONCURRENCY, queue.length);
    await Promise.all(Array.from({length: workerCount}, worker));
}

async function fetchFinnhubSymbol(position){
    const cat = (position.type || '').toLowerCase();
    const queries = Array.from(new Set([
        position.Symbol,
        position.displayName,
        position.Name,
        position.id
    ].filter(Boolean)));
    for(const query of queries){
        const url = `${FINNHUB_REST}/search?q=${encodeURIComponent(query)}&token=${FINNHUB_KEY}`;
        try{
            const res = await fetch(url);
            if(!res.ok) continue;
            const data = await res.json();
            const results = Array.isArray(data.result) ? data.result : [];
            if(!results.length) continue;
            if(cat === 'crypto'){
                const cryptoMatches = results.filter(item=> String(item.type || '').toLowerCase() === 'crypto');
                const base = (position.Symbol || position.displayName || '').replace(/[^A-Z0-9]/g,'').toUpperCase();
                const preferred = cryptoMatches.find(item=> (item.symbol || '').toUpperCase().includes(base)) || cryptoMatches.find(item=> /USDT|USD/.test(item.symbol || '')) || cryptoMatches[0];
                if(preferred && preferred.symbol) return preferred.symbol;
            }else{
                const stockMatches = results.filter(item=>{
                    const type = String(item.type || '').toLowerCase();
                    return type.includes('stock') || type.includes('etf');
                });
                const base = (position.Symbol || position.displayName || '').replace(/[^A-Z0-9]/g,'').toUpperCase();
                const exact = stockMatches.find(item=> (item.symbol || '').toUpperCase() === base);
                const exchangeMatch = stockMatches.find(item=> (item.symbol || '').toUpperCase().endsWith(`:${base}`));
                const best = exact || exchangeMatch || stockMatches[0] || results[0];
                if(best && best.symbol) return best.symbol;
            }
        }catch(error){
            console.warn('Finnhub search error', query, error);
        }
    }
    return null;
}

function recomputePositionMetrics(position){
    const qty = Number(position.qty||0);
    const costBasis = Number(position.costBasis||0);
    const realized = Number(position.realized||0);
    const fallbackPrice = position.lastKnownPrice || position.lastPurchasePrice || position.avgPrice || 0;
    const prevMarketValue = position.marketValue ?? null;
    const price = position.currentPrice!==undefined && position.currentPrice!==null ? position.currentPrice : fallbackPrice;
    const marketValue = qty ? price * qty : 0;
    position.avgPrice = qty !== 0 ? costBasis / qty : fallbackPrice;
    position.marketValue = marketValue;
    position.unrealized = marketValue - costBasis;
    const priceCandidates = [
        position.displayPrice,
        position.currentPrice,
        position.lastKnownPrice,
        position.lastPurchasePrice,
        position.avgPrice,
        fallbackPrice
    ].map(value=> Number(value)).filter(value=> Number.isFinite(value));
    const prioritizedPrice = priceCandidates.find(value => Math.abs(value) > 1e-9);
    const effectivePrice = Number.isFinite(prioritizedPrice) ? prioritizedPrice : 0;
    const reinvestedQty = Math.max(0, Number(position.reinvested || 0));
    const reinvestedValue = (reinvestedQty > 1e-6 && Math.abs(effectivePrice) > 1e-9) ? reinvestedQty * effectivePrice : 0;
    position.reinvestedValue = reinvestedValue;
    position.pnl = position.unrealized + realized + reinvestedValue;
    position.totalReturn = position.pnl;
    if(prevMarketValue !== null){
        position.marketDirection = marketValue > prevMarketValue ? 'up' : marketValue < prevMarketValue ? 'down' : null;
    }else{
        position.marketDirection = null;
    }
    position.prevMarketValue = marketValue;

    const typeKey = (position.type || '').toLowerCase();
    if(typeKey === 'real estate'){
        const propertyValue = Math.abs(costBasis);
        position.marketValue = propertyValue;
        position.unrealized = 0;
        const rentPnl = position.rentRealized !== undefined ? Number(position.rentRealized) : realized;
        position.pnl = rentPnl;
        position.totalReturn = rentPnl;
        position.prevMarketValue = propertyValue;
        position.displayPrice = propertyValue;
        position.reinvestedValue = 0;
    }
    return position;
}

function sortByDateAscending(records){
    return [...records].sort((a,b)=>{
        const da = a.fields?.Date ? new Date(a.fields.Date) : null;
        const db = b.fields?.Date ? new Date(b.fields.Date) : null;
        if(!da && !db) return 0;
        if(!da) return -1;
        if(!db) return 1;
        return da - db;
    });
}

// ----------------- DATA LOADERS ---------------------
async function fetchAllAirtableOperations(onProgress){
    if(!AIRTABLE_API_KEY) throw new Error('Missing Airtable API key');
    const records = [];
    let offset = '';
    let page = 1;
    if(typeof onProgress === 'function'){
        onProgress('Connecting to Airtable API…');
    }
    do{
        const params = new URLSearchParams();
        params.append('pageSize','100');
        params.append('sort[0][field]','Date');
        params.append('sort[0][direction]','asc');
        if(offset) params.append('offset', offset);
        if(typeof onProgress === 'function'){
            onProgress(`Fetching Airtable batch ${page}…`);
        }
        const url = `${AIRTABLE_URL}?${params.toString()}`;
        const res = await fetch(url,{headers:{Authorization:`Bearer ${AIRTABLE_API_KEY}`}});
        if(!res.ok){
            const text = await res.text();
            throw new Error(`Airtable responded ${res.status}: ${text}`);
        }
        const json = await res.json();
        records.push(...(json.records||[]));
        offset = json.offset;
        if(typeof onProgress === 'function'){
            const suffix = offset ? '…' : '.';
            onProgress(`Fetched ${records.length} Airtable records${suffix}`);
        }
        page += 1;
    }while(offset);
    return records;
}

function buildSnapshotCsv(records){
    const header = ['Asset','Category','Operation','Amount','Price','Spent','Date','Tags'];
    const rows = [header.join(',')];
    records.forEach(rec=>{
        const fields = rec?.fields || {};
        const tags = Array.isArray(fields.Tags) ? fields.Tags.join('|') : '';
        const amountValue = Number(fields.Amount);
        const priceValue = Number(fields['Asset price on invest date'] ?? fields.Price);
        const spentValue = Number(fields['Spent on operation']);
        const values = [
            fields.Asset || fields.Name || '',
            fields.Category || '',
            fields['Operation type'] || fields.Operation || '',
            Number.isFinite(amountValue) ? amountValue : '',
            Number.isFinite(priceValue) ? priceValue : '',
            Number.isFinite(spentValue) ? spentValue : '',
            fields.Date || '',
            tags
        ].map(value => `"${String(value ?? '').replace(/"/g,'""')}"`);
        rows.push(values.join(','));
    });
    return rows.join('\n');
}

function persistAirtableSnapshot(records){
    try{
        if(typeof window === 'undefined' || !window.localStorage || !Array.isArray(records) || !records.length){
            return;
        }
        window.localStorage.setItem(SNAPSHOT_STORAGE_KEYS.records, JSON.stringify(records));
        window.localStorage.setItem(SNAPSHOT_STORAGE_KEYS.savedAt, new Date().toISOString());
        window.localStorage.setItem(SNAPSHOT_STORAGE_KEYS.csv, buildSnapshotCsv(records));
    }catch(error){
        console.warn('Failed to persist Airtable snapshot', error);
    }
}

function loadSnapshotRecords(){
    try{
        if(typeof window === 'undefined' || !window.localStorage) return null;
        const raw = window.localStorage.getItem(SNAPSHOT_STORAGE_KEYS.records);
        if(!raw) return null;
        return JSON.parse(raw);
    }catch(error){
        console.warn('Failed to read cached Airtable snapshot', error);
        return null;
    }
}

function getSnapshotSavedAt(){
    if(typeof window === 'undefined' || !window.localStorage) return null;
    const ts = window.localStorage.getItem(SNAPSHOT_STORAGE_KEYS.savedAt);
    return ts ? new Date(ts) : null;
}

function transformOperations(records, progressCb){
    const ordered = sortByDateAscending(records);
    const map = new Map();
    const total = ordered.length;
    if(typeof progressCb === 'function'){
        progressCb(0, total);
    }
    ordered.forEach((rec, index)=>{
        const fields = rec.fields || {};
        const assetRaw = fields.Asset || fields.Name;
        if(!assetRaw) return;
        const asset = String(assetRaw).trim();
        const category = normalizeCategory(fields.Category, asset);
        const opType = fields['Operation type'] || fields.Operation || 'Unknown';
        const amount = Number(fields.Amount ?? 0) || 0;
        const price = Number(fields['Asset price on invest date'] ?? fields.Price ?? 0) || 0;
        const spent = Number(fields['Spent on operation'] ?? (amount * (price || 0))) || 0;
        const date = fields.Date ? new Date(fields.Date) : null;
        const tagsNormalized = Array.isArray(fields.Tags) ? fields.Tags.map(t=>String(t).toLowerCase()) : [];
        const opTypeLower = String(opType).toLowerCase();
        const isRentOp = (opTypeLower === 'profitloss' || opTypeLower.includes('rent')) && tagsNormalized.some(tag=>RENT_TAGS.includes(tag));
        const isReinvesting = tagsNormalized.includes('reinvesting');

        const categoryKey = typeof category === 'string' ? category.toLowerCase() : '';
        const trackLots = categoryKey === 'stock' || categoryKey === 'crypto';
        if(!map.has(asset)){
            const finnhubOverride = fields['Finnhub Symbol'] || fields['Finnhub symbol'] || fields['finnhubSymbol'] || fields['FINNHUB_SYMBOL'];
            const finnhubSymbol = mapFinnhubSymbol(finnhubOverride || asset, category, Boolean(finnhubOverride));
            map.set(asset, {
                id: asset,
                Name: asset,
                displayName: asset,
                Category: category,
                type: category,
                Symbol: finnhubSymbol || asset,
                finnhubSymbol,
                finnhubOverride: Boolean(finnhubOverride),
                qty: 0,
                costBasis: 0,
                invested: 0,
                realized: 0,
                reinvested: 0,
                rentRealized: 0,
                cashflow: 0,
                lastKnownPrice: 0,
                lastPurchasePrice: 0,
                operations: [],
                lots: trackLots ? [] : [],
                closedSales: []
            });
        }

        const entry = map.get(asset);
        entry.operations.push({
            id: rec.id,
            date,
            rawDate: fields.Date,
            type: opType,
            amount,
            price,
            spent,
            tags: tagsNormalized,
            isReinvesting,
            isRent: Boolean(isRentOp),
            skipInCharts: Boolean(isReinvesting)
        });

        if(opType === 'PurchaseSell'){
            const useLots = trackLots;
            if(amount > 0){
                const totalCostRaw = spent !== 0 ? Math.abs(spent) : Math.abs(price * amount);
                const totalCost = Number.isFinite(totalCostRaw) ? totalCostRaw : Math.abs(price * amount);
                const unitPrice = amount !== 0 ? totalCost / amount : price;
                if(useLots){
                    entry.lots.push({
                        qty: amount,
                        costPerUnit: Number.isFinite(unitPrice) ? unitPrice : 0,
                        date
                    });
                    const qtySum = entry.lots.reduce((sum, lot)=> sum + lot.qty, 0);
                    entry.qty = qtySum;
                    entry.costBasis = entry.lots.reduce((sum, lot)=> sum + lot.qty * lot.costPerUnit, 0);
                }else{
                    entry.qty += amount;
                    entry.costBasis += totalCost;
                }
                if(totalCost > 0){
                    entry.invested += totalCost;
                }
                if(Number.isFinite(unitPrice) && unitPrice){
                    entry.lastPurchasePrice = unitPrice;
                    entry.lastKnownPrice = unitPrice;
                }
            }else if(amount < 0){
                const sellQty = Math.abs(amount);
                const proceeds = spent < 0 ? Math.abs(spent) : Math.abs(price * sellQty);
                const saleId = rec.id || `sale-${index}`;
                const sellDate = date instanceof Date ? date : (date ? new Date(date) : null);
                if(useLots){
                    let remaining = sellQty;
                    let costOut = 0;
                    const saleChunks = [];
                    while(remaining > 1e-9 && entry.lots.length){
                        const lot = entry.lots[0];
                        const matched = Math.min(lot.qty, remaining);
                        const lotCostPerUnit = Number(lot.costPerUnit) || 0;
                        const lotDate = lot.date instanceof Date ? lot.date : (lot.date ? new Date(lot.date) : null);
                        saleChunks.push({
                            qty: matched,
                            buyCostPerUnit: lotCostPerUnit,
                            buyDate: lotDate
                        });
                        costOut += matched * lotCostPerUnit;
                        lot.qty -= matched;
                        remaining -= matched;
                        if(lot.qty <= 1e-9){
                            entry.lots.shift();
                        }
                    }
                    if(remaining > 1e-9){
                        const fallbackUnit = entry.lastPurchasePrice || entry.avgPrice || price || 0;
                        saleChunks.push({
                            qty: remaining,
                            buyCostPerUnit: fallbackUnit,
                            buyDate: null
                        });
                        costOut += remaining * fallbackUnit;
                        remaining = 0;
                    }
                    saleChunks.forEach(chunk=>{
                        entry.closedSales.push({
                            saleId,
                            qty: chunk.qty,
                            buyCostPerUnit: chunk.buyCostPerUnit,
                            buyDate: chunk.buyDate,
                            sellPricePerUnit: price,
                            sellDate,
                            totalCost: chunk.qty * chunk.buyCostPerUnit,
                            totalProceeds: chunk.qty * price
                        });
                    });
                    const qtySum = entry.lots.reduce((sum, lot)=> sum + lot.qty, 0);
                    entry.qty = qtySum;
                    entry.costBasis = entry.lots.reduce((sum, lot)=> sum + lot.qty * lot.costPerUnit, 0);
                    entry.realized += proceeds - costOut;
                }else{
                    const prevQty = entry.qty;
                    const prevCost = entry.costBasis;
                    const avgCost = prevQty > 0 ? prevCost / prevQty : entry.lastPurchasePrice || price || 0;
                    const costOut = avgCost * sellQty;
                    entry.qty = Math.max(0, prevQty + amount);
                    entry.costBasis = Math.max(0, prevCost - costOut);
                    entry.realized += proceeds - costOut;
                    entry.closedSales.push({
                        saleId,
                        qty: sellQty,
                        buyCostPerUnit: avgCost,
                        buyDate: null,
                        sellPricePerUnit: price,
                        sellDate,
                        totalCost: costOut,
                        totalProceeds: proceeds
                    });
                }
                if(entry.qty <= 1e-9){
                    if(price){
                        entry.lastKnownPrice = price;
                    }else if(entry.lastPurchasePrice){
                        entry.lastKnownPrice = entry.lastPurchasePrice;
                    }
                }
            }
            entry.cashflow += spent;
    }else if(opType === 'ProfitLoss'){
        if(isReinvesting){
            let reinvestAmount = spent < 0 ? -spent : spent;
            if(!reinvestAmount){
                const fallbackAmount = Math.abs(Number(price || 0) * Number(amount || 0));
                if(fallbackAmount) reinvestAmount = fallbackAmount;
            }
            let reinvestQty = Math.abs(Number(amount || 0));
            if((!reinvestQty || !Number.isFinite(reinvestQty)) && Number.isFinite(reinvestAmount)){
                const effectivePrice = Number(price || entry.lastKnownPrice || entry.lastPurchasePrice || 0);
                if(Number.isFinite(effectivePrice) && Math.abs(effectivePrice) > 1e-9){
                    const derivedQty = Math.abs(reinvestAmount) / Math.abs(effectivePrice);
                    if(Number.isFinite(derivedQty) && derivedQty > 0){
                        reinvestQty = derivedQty;
                    }
                }
            }
            if(Number.isFinite(reinvestQty) && reinvestQty > 0){
                entry.reinvested = (entry.reinvested || 0) + reinvestQty;
            }
        }else{
            entry.realized += spent;
            if(isRentOp){
                entry.rentRealized = (entry.rentRealized || 0) + (spent < 0 ? -spent : spent);
            }else{
                entry.cashflow += spent;
            }
        }
    }else if(opType === 'DepositWithdrawal'){
        entry.qty += amount;
        entry.costBasis += spent;
        entry.cashflow += spent;
        if(!entry.lastKnownPrice && price){
            entry.lastKnownPrice = price;
        }
    }else{
        entry.cashflow += spent;
    }

        if(entry.qty <= 1e-9){
            entry.qty = 0;
            entry.costBasis = 0;
            if(Array.isArray(entry.lots)){
                entry.lots = [];
            }
        }
        if(!entry.lastKnownPrice && entry.lastPurchasePrice){
            entry.lastKnownPrice = entry.lastPurchasePrice;
        }
        if(typeof progressCb === 'function'){
            progressCb(index + 1, total, asset);
        }
    });

    return Array.from(map.values()).map(p=>{
        if(p.Category === 'Cash'){
            p.displayName = 'Cash Reserve';
        }
        if(!p.lastKnownPrice){
            p.lastKnownPrice = p.lastPurchasePrice || p.avgPrice || 0;
        }
        if(p.rentRealized === undefined){
            p.rentRealized = 0;
        }
        if(Array.isArray(p.lots)){
            p.lots = p.lots.filter(lot=> Number.isFinite(lot.qty) && lot.qty > 1e-9);
            const qtySum = p.lots.reduce((sum, lot)=> sum + lot.qty, 0);
            const costSum = p.lots.reduce((sum, lot)=> sum + lot.qty * (Number(lot.costPerUnit) || 0), 0);
            p.qty = qtySum;
            p.costBasis = costSum;
        }else{
            p.lots = null;
        }
        if(Array.isArray(p.closedSales)){
            p.closedSales = p.closedSales.map(chunk=>({
                saleId: chunk.saleId,
                qty: Number(chunk.qty) || 0,
                buyCostPerUnit: Number(chunk.buyCostPerUnit) || 0,
                buyDate: chunk.buyDate instanceof Date ? chunk.buyDate : (chunk.buyDate ? new Date(chunk.buyDate) : null),
                sellPricePerUnit: Number(chunk.sellPricePerUnit) || 0,
                sellDate: chunk.sellDate instanceof Date ? chunk.sellDate : (chunk.sellDate ? new Date(chunk.sellDate) : null),
                totalCost: Number(chunk.totalCost) || 0,
                totalProceeds: Number(chunk.totalProceeds) || 0
            }));
        }else{
            p.closedSales = null;
        }
        recomputePositionMetrics(p);
        ensurePositionDefaults(p);
        p.referencePrices = {};
        p.rentRangeTotals = {};
        return p;
    });
}

function useFallbackPositions(){
    const fallback = [
        {Name:'Bitcoin',category:'Crypto',symbol:'BINANCE:BTCUSDT',qty:0.25,avgPrice:30000,lotDate:'2023-07-15T00:00:00Z'},
        {Name:'Solana',category:'Crypto',symbol:'BINANCE:SOLUSDT',qty:12,avgPrice:85,lotDate:'2024-01-18T00:00:00Z'},
        {Name:'XRP',category:'Crypto',symbol:'BINANCE:XRPUSDT',qty:1200,avgPrice:0.5,lotDate:'2023-11-03T00:00:00Z'},
        {Name:'Cash Reserve',category:'Cash',symbol:null,qty:2500,avgPrice:1,lotDate:'2024-02-01T00:00:00Z'}
    ];
    const map = fallback.map(f=>{
        const normalizedCategory = normalizeCategory(f.category, f.Name);
        const typeLower = normalizedCategory.toLowerCase();
        const costBasis = f.qty * f.avgPrice;
        const trackLots = typeLower === 'stock' || typeLower === 'crypto';
        const lotDate = f.lotDate ? new Date(f.lotDate) : new Date();
        const lots = trackLots && f.qty > 0
            ? [{
                qty: f.qty,
                costPerUnit: f.avgPrice,
                date: lotDate
            }]
            : trackLots ? [] : null;
        const operations = trackLots && f.qty > 0
            ? [{
                type: 'PurchaseSell',
                amount: f.qty,
                spent: f.qty * f.avgPrice,
                price: f.avgPrice,
                date: lotDate,
                rawDate: lotDate
            }]
            : [];
        return {
            Name: f.Name,
            displayName: f.Name,
            Category: normalizedCategory,
            type: normalizedCategory,
            Symbol: f.symbol || f.Name,
            finnhubSymbol: f.symbol,
            qty: f.qty,
            costBasis,
            invested: costBasis,
            realized: 0,
            cashflow: 0,
            lastKnownPrice: f.avgPrice,
            lots,
            closedSales: [],
            operations
        };
    });
    return map.map(p=>{
        recomputePositionMetrics(p);
        ensurePositionDefaults(p);
        p.referencePrices = {};
        p.finnhubOverride = false;
        p.rentRealized = 0;
        p.rentRangeTotals = {};
        return p;
    });
}

async function loadPositions(){
    symbolSet = new Set();
    finnhubIndex = new Map();
    assetIconSourceCache.clear();
    transactionPriceCache.clear();
    try{
        reportLoading('Connecting to Airtable API…');
        const records = await fetchAllAirtableOperations(reportLoading);
        persistAirtableSnapshot(records);
        operationsMeta = {count: records.length, fetchedAt: new Date()};
        if(records.length){
            reportLoading(`Processing operations… 0/${records.length}`);
        }else{
            reportLoading('Processing operations…');
        }
        positions = transformOperations(records, (processed, total, asset)=>{
            if(!total){
                reportLoading('Processing operations…');
                return;
            }
            const safeProcessed = Math.min(processed, total);
            const label = asset && safeProcessed ? ` · ${asset}` : '';
            reportLoading(`Processing operations… ${safeProcessed}/${total}${label}`);
        });
        reportLoading('Resolving market symbols…');
        await refineFinnhubSymbols((processed, total, position)=>{
            if(!total){
                reportLoading('Resolving market symbols…');
                return;
            }
            const safeProcessed = Math.min(processed, total);
            const labelSource = position ? (position.displayName || position.Symbol || position.Name) : '';
            const label = labelSource && safeProcessed ? ` · ${labelSource}` : '';
            reportLoading(`Resolving market symbols… ${safeProcessed}/${total}${label}`);
        });
        reportLoading('Preparing dashboard data…');
        netContributionTotal = positions.reduce((sum,p)=>sum + Number(p.cashflow || 0),0);
        rebuildSymbolIndex();
        if(!positions.length){
            throw new Error('No positions available after transformation');
        }
        dataSourceMode = 'airtable';
        updateDataSourceBadge();
    }catch(err){
        console.warn('Airtable load failed, attempting snapshot fallback', err);
        const snapshotRecords = loadSnapshotRecords();
        if(Array.isArray(snapshotRecords) && snapshotRecords.length){
            positions = transformOperations(snapshotRecords, null);
            netContributionTotal = positions.reduce((sum,p)=>sum + Number(p.cashflow || 0),0);
            rebuildSymbolIndex();
            operationsMeta = {count: snapshotRecords.length, fetchedAt: getSnapshotSavedAt()};
            setStatus('Cached Airtable snapshot loaded');
            dataSourceMode = 'snapshot';
        }else{
            setStatus('Airtable unavailable — showing demo data');
            positions = useFallbackPositions();
            netContributionTotal = positions.reduce((sum,p)=>sum + Number(p.cashflow || 0),0);
            rebuildSymbolIndex();
            operationsMeta = {count: 0, fetchedAt: null};
            dataSourceMode = 'fallback';
        }
        setLoadingState('error','Airtable unavailable — showing demo data');
        setTimeout(()=>setLoadingState('hidden'), 1400);
        updateDataSourceBadge();
    }
    rangeDirty = true;
    assetYearSeriesDirty = true;
    realEstateRentSeriesDirty = true;
}

async function switchDataSource(mode){
    const target = mode === 'fallback' ? 'fallback' : mode === 'snapshot' ? 'snapshot' : 'airtable';
    if(isSwitchingDataSource) return;
    if(target === dataSourceMode && target !== 'loading') return;
    isSwitchingDataSource = true;
    const previousMode = dataSourceMode;
    try{
        if(target === 'fallback'){
            dataSourceMode = 'loading';
            updateDataSourceBadge();
            positions = useFallbackPositions();
            netContributionTotal = positions.reduce((sum,p)=>sum + Number(p.cashflow || 0),0);
            rebuildSymbolIndex();
            operationsMeta = {count: 0, fetchedAt: null};
            rangeDirty = true;
            assetYearSeriesDirty = true;
            realEstateRentSeriesDirty = true;
            dataSourceMode = 'fallback';
            setStatus('Offline test data loaded');
            updateDataSourceBadge();
            scheduleUIUpdate({immediate:true});
            subscribeAll();
            return;
        }
        if(target === 'snapshot'){
            dataSourceMode = 'loading';
            updateDataSourceBadge();
            const snapshotRecords = loadSnapshotRecords();
            if(Array.isArray(snapshotRecords) && snapshotRecords.length){
                positions = transformOperations(snapshotRecords, null);
                netContributionTotal = positions.reduce((sum,p)=>sum + Number(p.cashflow || 0),0);
                rebuildSymbolIndex();
                operationsMeta = {count: snapshotRecords.length, fetchedAt: getSnapshotSavedAt()};
                rangeDirty = true;
                assetYearSeriesDirty = true;
                realEstateRentSeriesDirty = true;
                dataSourceMode = 'snapshot';
                setStatus('Cached Airtable snapshot loaded');
                updateDataSourceBadge();
                scheduleUIUpdate({immediate:true});
                subscribeAll();
            }else{
                setStatus('No cached snapshot available');
                dataSourceMode = (previousMode === 'airtable' || previousMode === 'snapshot') ? previousMode : 'fallback';
                updateDataSourceBadge();
            }
            return;
        }
        if(loadingOverlay){
            setLoadingState('visible','Loading Airtable…');
        }
        dataSourceMode = 'loading';
        updateDataSourceBadge();
        await loadPositions();
        await preloadHistoricalPriceSeries();
        positions.forEach(recomputePositionMetrics);
        lastUpdated = new Date();
        rangeDirty = true;
        assetYearSeriesDirty = true;
        realEstateRentSeriesDirty = true;
        dataSourceMode = 'airtable';
        updateDataSourceBadge();
        scheduleUIUpdate({immediate:true});
        subscribeAll();
    }catch(error){
        console.error('Failed to switch data source', error);
        positions = useFallbackPositions();
        netContributionTotal = positions.reduce((sum,p)=>sum + Number(p.cashflow || 0),0);
        rebuildSymbolIndex();
        operationsMeta = {count: 0, fetchedAt: null};
        dataSourceMode = 'fallback';
        updateDataSourceBadge();
        setStatus('Offline test data loaded');
        rangeDirty = true;
        assetYearSeriesDirty = true;
        realEstateRentSeriesDirty = true;
        scheduleUIUpdate({immediate:true});
        subscribeAll();
    }finally{
        if(loadingOverlay){
            setLoadingState('hidden');
        }
        isSwitchingDataSource = false;
        updateDataSourceBadge();
    }
}

// ----------------- FINNHUB REST (snapshot) -----------------
async function fetchSnapshotBatch(symbols){
    const promises = symbols.map(async sym => {
        try{
            const res = await fetch(`${FINNHUB_REST}/quote?symbol=${encodeURIComponent(sym)}&token=${FINNHUB_KEY}`);
            if(!res.ok){
                return {symbol: sym, ok:false};
            }
            const data = await res.json();
            return {symbol:sym, ok:true, data};
        }catch(error){
            console.warn('Snapshot error', sym, error);
            return {symbol:sym, ok:false};
        }
    });
    return Promise.all(promises);
}

async function applySnapshotResults(results){
    const fallbackTasks = [];
    results.forEach(result=>{
        const pos = finnhubIndex.get(result.symbol);
        if(!pos){
            return;
        }
        if(!result.ok){
            fallbackTasks.push(scheduleYahooFallback(pos));
            return;
        }
        const data = result.data || {};
        const price = Number(data.c ?? data.p ?? data.lp ?? data.lastPrice);
        const prevClose = Number(data.pc ?? data.previousClose);
        const applied = applyPriceUpdate(pos, price, prevClose, 'finnhub');
        if(!applied){
            fallbackTasks.push(scheduleYahooFallback(pos));
        }
    });
    if(fallbackTasks.length){
        await Promise.allSettled(fallbackTasks);
    }
    rangeDirty = true;
}

// ----------------- WEBSOCKET REAL-TIME -----------------
function initFinnhubWS(){
    if(!HAS_FINNHUB_KEY){
        console.warn('Finnhub websocket disabled — API key missing');
        return;
    }
    try{
        ws = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_KEY}`);
        ws.onopen = ()=>{ setStatus('Finnhub WS connected'); subscribeAll(); };
        ws.onmessage = event => {
            const payload = JSON.parse(event.data);
            if(payload.type === 'trade'){
                payload.data.forEach(trade => {
                    applyRealtime(trade.s, trade.p);
                });
            }
        };
        ws.onclose = ()=>{
            setStatus('Finnhub WS disconnected – retrying…');
            setTimeout(initFinnhubWS, 2000);
        };
        ws.onerror = err =>{
            console.error('Finnhub websocket error', err);
            setStatus('Finnhub WS error');
            try{ ws.close(); }catch(_){/* ignore */}
        };
    }catch(err){
        console.error('WS init failed', err);
    }
}

function subscribeAll(){
    if(!HAS_FINNHUB_KEY) return;
    if(!ws || ws.readyState !== WebSocket.OPEN) return;
    symbolSet.forEach(sym=>{
        try{ ws.send(JSON.stringify({type:'subscribe', symbol:sym})); }
        catch(error){ console.warn('Failed to subscribe', sym, error); }
    });
}

function applyRealtime(symbol, price){
    const pos = finnhubIndex.get(symbol);
    if(!pos) return;
    const numericPrice = Number(price);
    if(applyPriceUpdate(pos, numericPrice, pos.prevClose ?? null, 'finnhub')){
        lastUpdated = new Date();
        rangeDirty = true;
        const type = (pos.type || '').toLowerCase();
        if(type === 'crypto'){
            scheduleCryptoUiUpdate();
        }else{
            scheduleUIUpdate();
        }
    }else{
        scheduleYahooFallback(pos);
    }
}

// ----------------- RENDER CHARTS -----------------
function computeAssetYearSeries(){
    const yearAssetMap = new Map();
    const yearSet = new Set();
    const assetNames = new Set();

    positions.forEach(position=>{
        const ops = Array.isArray(position.operations) ? position.operations : [];
        if(!ops.length) return;
        const name = position.displayName || position.Symbol || position.Name;
        if(!name) return;
        const qty = Number(position.qty || 0);
        const marketValue = Number(position.marketValue || 0);
        const isClosed = Math.abs(qty) <= 1e-6 && Math.abs(marketValue) <= 1e-6;
        if(isClosed) return;
        ops.forEach(op=>{
            if(!(op.date instanceof Date)) return;
            const year = op.date.getFullYear();
            yearSet.add(year);
            assetNames.add(name);
            if(!yearAssetMap.has(year)) yearAssetMap.set(year, new Map());
            const assetMap = yearAssetMap.get(year);
            const prev = Number(assetMap.get(name) || 0);
            const delta = Number(op.spent || 0) || 0;
            assetMap.set(name, prev + delta);
        });
    });

    const years = Array.from(yearSet).sort((a,b)=>a-b);
    if(!years.length || !assetNames.size){
        assetYearSeries = { labels: [], datasets: [] };
        assetYearSeriesDirty = false;
        return;
    }

    const assets = Array.from(assetNames);
    const cumulative = new Map(assets.map(name=>[name,0]));
    const assetDatasets = assets.map((name, idx)=>{
        const data = years.map(year=>{
            const assetMap = yearAssetMap.get(year);
            const delta = assetMap && assetMap.has(name) ? Number(assetMap.get(name)) : 0;
            const running = (cumulative.get(name) || 0) + delta;
            cumulative.set(name, running);
            return Number(running.toFixed(2));
        });
        if(!assetColorCache.has(name)){
            const hue = (idx * 53) % 360;
            assetColorCache.set(name, {
                border: `hsl(${hue},70%,55%)`,
                background: `hsla(${hue},70%,55%,0.1)`
            });
        }
        const colors = assetColorCache.get(name);
        return {
            label: name,
            data,
            tension: 0.35,
            borderWidth: 2,
            fill: false,
            borderColor: colors.border,
            backgroundColor: colors.background,
            pointRadius: 2,
            pointHoverRadius: 5,
            type: 'line'
        };
    }).filter(ds => ds.data.some(value => Math.abs(value) > 0.5));

    const totalSeries = years.map((_, idx)=>
        assetDatasets.reduce((sum, ds)=> sum + Number(ds.data[idx] || 0), 0)
    ).map(value=> Number(value.toFixed(2)));

    const totalDataset = {
        label: 'Total Contributions',
        data: totalSeries.slice(),
        borderColor: 'rgba(59, 130, 246, 0.9)',
        backgroundColor: 'transparent',
        borderWidth: 2,
        fill: false,
        tension: 0.35,
        pointRadius: 3,
        pointHoverRadius: 6,
        type: 'line'
    };

    let labels = years.map(String);
    const datasets = [totalDataset, ...assetDatasets];

    if(totalSeries.length){
        const projectionYear = years[years.length - 1] + 1;
        let projectionValue = totalSeries[totalSeries.length - 1];
        if(totalSeries.length > 1){
            const deltas = [];
            for(let i=1; i<totalSeries.length; i++){
                deltas.push(totalSeries[i] - totalSeries[i-1]);
            }
            const avgDelta = deltas.reduce((sum,val)=>sum+val,0) / deltas.length;
            projectionValue += avgDelta;
        }
        projectionValue = Number(Math.max(projectionValue, 0).toFixed(2));
        labels = [...labels, String(projectionYear)];
        datasets.forEach(ds=>{
            ds.data = ds.data.slice();
            ds.data.push(null);
        });
        const projectionData = new Array(labels.length).fill(null);
        projectionData[labels.length - 2] = totalSeries[totalSeries.length - 1];
        projectionData[labels.length - 1] = projectionValue;
        datasets.push({
            label: `Projected ${projectionYear}`,
            data: projectionData,
            borderDash: [6, 4],
            borderColor: 'rgba(16, 185, 129, 0.9)',
            backgroundColor: 'transparent',
            borderWidth: 2,
            fill: false,
            tension: 0.35,
            pointRadius: 0,
            spanGaps: true,
            type: 'line'
        });
    }

    assetYearSeries = { labels, datasets };
    assetYearSeriesDirty = false;
}

function mergeDeep(target, source){
    if(!source) return target;
    Object.keys(source).forEach(key=>{
        const value = source[key];
        if(Array.isArray(value)){
            target[key] = value.slice();
        }else if(value && typeof value === 'object' && !(value instanceof Date)){
            if(!target[key] || typeof target[key] !== 'object'){
                target[key] = {};
            }
            mergeDeep(target[key], value);
        }else{
            target[key] = value;
        }
    });
    return target;
}

function applyDatasetChanges(target, source){
    if(!source) return;
    Object.keys(source).forEach(key=>{
        const value = source[key];
        if(Array.isArray(value)){
            target[key] = value.slice();
        }else{
            target[key] = value;
        }
    });
}

function createOrUpdateChart(id,type,data,options){
    registerZoomPluginIfNeeded();
    const preferredPanMode = type === 'scatter' ? 'xy' : 'x';
    const defaultOptions = {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 600 },
        plugins: { legend: { display: true } },
        layout: { padding: 12 }
    };

    const existing = charts[id];
    if(existing){
        existing.config.type = type;
        existing.data.labels = Array.isArray(data.labels) ? data.labels.slice() : [];
        const datasets = Array.isArray(data.datasets) ? data.datasets : [];
        datasets.forEach((dataset, index)=>{
            if(existing.data.datasets[index]){
                applyDatasetChanges(existing.data.datasets[index], dataset);
                if(Array.isArray(dataset.data)){
                    existing.data.datasets[index].data = dataset.data.slice();
                }
            }else{
                existing.data.datasets[index] = Object.assign({}, dataset);
                if(Array.isArray(dataset.data)){
                    existing.data.datasets[index].data = dataset.data.slice();
                }
                if(Array.isArray(dataset.backgroundColor)){
                    existing.data.datasets[index].backgroundColor = dataset.backgroundColor.slice();
                }
            }
        });
        existing.data.datasets.length = datasets.length;
        if(options){
            mergeDeep(existing.options, options);
        }
        ensurePanSupport(existing.options, preferredPanMode);
        existing.update('none');
        return existing;
    }
    const canvas = document.getElementById(id);
    if(!canvas) return null;
    const ctx = canvas.getContext('2d');
    const mergedOptions = mergeDeep(mergeDeep({}, defaultOptions), options || {});
    ensurePanSupport(mergedOptions, preferredPanMode);
    const initialData = {
        labels: Array.isArray(data.labels) ? data.labels.slice() : [],
        datasets: Array.isArray(data.datasets) ? data.datasets.map(ds=>{
            const clone = Object.assign({}, ds);
            if(Array.isArray(ds.data)) clone.data = ds.data.slice();
            if(Array.isArray(ds.backgroundColor)) clone.backgroundColor = ds.backgroundColor.slice();
            return clone;
        }) : []
    };
    charts[id] = new Chart(ctx, { type, data: initialData, options: mergedOptions });
    return charts[id];
}

function renderAllCharts(){
    if(assetYearSeriesDirty){
        computeAssetYearSeries();
    }

    if(assetYearSeries.labels.length && assetYearSeries.datasets.length){
        createOrUpdateChart('assetDynamicChart','line', assetYearSeries, {
            plugins: { legend: { position: 'bottom' } },
            interaction: { mode: 'index', intersect: false },
            scales: {
                x: { ticks: { autoSkip: true, maxTicksLimit: 10 } },
                y: {
                    beginAtZero: true,
                    ticks: { callback: value => money(value) }
                }
            },
            elements: { point: { radius: 3, hoverRadius: 6 } }
        });
    }else if(charts['assetDynamicChart']){
        charts['assetDynamicChart'].destroy();
        delete charts['assetDynamicChart'];
    }

}

// ----------------- UI -----------------
function setStatus(s){ const el = document.getElementById('status'); if(el) el.textContent = s; }

function updateDataSourceBadge(){
    if(typeof document === 'undefined') return;
    const select = document.getElementById('data-source-select');
    if(!select) return;
    const normalized = (dataSourceMode === 'airtable' || dataSourceMode === 'fallback' || dataSourceMode === 'snapshot') ? dataSourceMode : 'loading';
    select.setAttribute('data-source', normalized);
    if(normalized === 'loading'){
        let loadingOption = select.querySelector('option[value="loading"]');
        if(!loadingOption){
            loadingOption = document.createElement('option');
            loadingOption.value = 'loading';
            loadingOption.disabled = true;
            loadingOption.hidden = true;
            select.insertBefore(loadingOption, select.firstChild);
        }
        loadingOption.textContent = DATA_SOURCE_LABELS.loading;
        select.value = 'loading';
        select.disabled = true;
    }else{
        const loadingOption = select.querySelector('option[value="loading"]');
        if(loadingOption){
            loadingOption.remove();
        }
        select.disabled = isSwitchingDataSource;
        if(select.value !== normalized){
            select.value = normalized;
        }
    }
    select.title = DATA_SOURCE_LABELS[dataSourceMode] || DATA_SOURCE_LABELS.loading;
}

function computeRealEstateAnalytics(){
    const now = new Date();
    const currentYear = now.getFullYear();
    const cutoffStart = new Date(now.getFullYear(), now.getMonth(), 1);
    cutoffStart.setMonth(cutoffStart.getMonth() - 11);
    const THIRTY_DAYS_MS = 30 * 24 * 3600 * 1000;
    const last30Cutoff = new Date(now.getTime() - THIRTY_DAYS_MS);
    const results = [];
    const rentYearTotals = new Map();
    const rentYearTotalsByAsset = new Map();
    const rentYearSet = new Set();
    let realEstateTotalValue = 0;

    const portfolioCategories = new Set(['real estate','automobile','automotive']);
    positions.filter(p=> portfolioCategories.has((p.type || '').toLowerCase())).forEach((position, idx)=>{
        const ops = Array.isArray(position.operations) ? position.operations : [];
        if(!ops.length) return;
        let totalPurchase = 0;
        let totalExpenses = 0;
        let rentCollected = 0;
        let rentYtd = 0;
        let rentLast12 = 0;
        let rentLast30 = 0;
        let latestRentDate = null;
        let latestRentAmount = 0;
        let earliestPurchase = null;
        let earliestEvent = null;
        const rentMonths = new Set();
        const rentMonthsLast12 = new Set();
        const typeKey = String(position.type || '').toLowerCase();

        ops.forEach(op=>{
            const spentRaw = Number(op.spent || 0);
            const spent = Number.isFinite(spentRaw) ? spentRaw : 0;
            const amount = Number(op.amount || 0) || 0;
            const price = Number(op.price || 0) || 0;
            const type = (op.type || '').toLowerCase();
            const tags = Array.isArray(op.tags) ? op.tags.map(t=>String(t).toLowerCase()) : [];
            const date = op.date instanceof Date ? op.date : (op.rawDate ? new Date(op.rawDate) : null);
            const isReinvestingOp = Boolean(op.isReinvesting || tags.includes('reinvesting'));

            if(date && (!earliestEvent || date < earliestEvent)){
                earliestEvent = date;
            }

            if(isReinvestingOp){
                return;
            }

            const hasRentTag = tags.some(tag=>RENT_TAGS.includes(tag));
            const hasExpenseTag = tags.some(tag=>EXPENSE_TAGS.includes(tag));
            const cashImpact = spent !== 0 ? spent : (amount !== 0 && price !== 0 ? amount * price : 0);
            const isRent = (type === 'profitloss') && hasRentTag;
            const isPurchase = type === 'purchasesell' && cashImpact > 0;
            const expenseKeywords = ['expense','maintenance','hoa','tax','mortgage','interest','service','fee','fees','insurance','repair','repairs','utility','utilities','management'];
            const matchesExpenseKeyword = expenseKeywords.some(keyword=> type.includes(keyword));
            const isExpenseCandidate = !isPurchase && !isRent;
            const isProfitLoss = type === 'profitloss';
            const isExpense = isExpenseCandidate && (hasExpenseTag || matchesExpenseKeyword || (isProfitLoss && cashImpact > 0));
            const isCashOutExpense = isExpenseCandidate && cashImpact < 0 && !type.includes('deposit') && !type.includes('withdraw');

            if(isPurchase){
                if(date && (!earliestPurchase || date < earliestPurchase)){
                    earliestPurchase = date;
                }
                const cashOut = Math.abs(cashImpact);
                totalPurchase += cashOut;
                return;
            }

            if(isRent){
                let rentAmount = spent < 0 ? -spent : spent;

                if(rentAmount > 0){
                    rentCollected += rentAmount;
                    if(date){
                        if(!latestRentDate || (date instanceof Date && date > latestRentDate)){
                            latestRentDate = date instanceof Date ? date : latestRentDate;
                            latestRentAmount = rentAmount;
                        }
                        const key = `${date.getFullYear()}-${String(date.getMonth()+1).padStart(2,'0')}`;
                        rentMonths.add(key);
                        const yr = date.getFullYear();
                        rentYearTotals.set(yr, (rentYearTotals.get(yr) || 0) + rentAmount);
                        rentYearSet.add(yr);
                        const assetKey = position.displayName || position.Symbol || position.Name || `Asset ${idx+1}`;
                        if(!rentYearTotalsByAsset.has(assetKey)) rentYearTotalsByAsset.set(assetKey, new Map());
                        const assetYearMap = rentYearTotalsByAsset.get(assetKey);
                        assetYearMap.set(yr, (assetYearMap.get(yr) || 0) + rentAmount);
                        if(date >= cutoffStart){
                            rentLast12 += rentAmount;
                            rentMonthsLast12.add(key);
                        }
                        if(date.getFullYear() === currentYear){
                            rentYtd += rentAmount;
                        }
                        if(date >= last30Cutoff){
                            rentLast30 += rentAmount;
                        }
                    }
                }
                return;
            }

            if((isExpense || isCashOutExpense) && cashImpact !== 0){
                const expenseAmount = Math.abs(cashImpact);
                totalExpenses += expenseAmount;
            }
        });

        const finalAssetPrice = totalPurchase + totalExpenses;
        const outstanding = Math.max(0, finalAssetPrice - rentCollected);
        const baseDate = earliestPurchase || earliestEvent;
        const totalMonths = baseDate ? monthsBetween(baseDate, now) : 0;
        const utilization = finalAssetPrice ? (Math.min(rentCollected, finalAssetPrice) / finalAssetPrice) * 100 : 0;
        const monthsForAvg = rentMonthsLast12.size || (rentMonths.size ? Math.min(12, rentMonths.size) : 0);
        const avgMonthlyRent = monthsForAvg ? rentLast12 / monthsForAvg : null;
        let payoffMonths = null;
        if(outstanding <= 0){
            payoffMonths = 0;
        }else if(avgMonthlyRent && avgMonthlyRent > 0){
            payoffMonths = outstanding / avgMonthlyRent;
        }

        const marketValue = Number(position.marketValue || 0);
        realEstateTotalValue += finalAssetPrice;
        const baseForHoldingPeriod = earliestPurchase instanceof Date ? earliestPurchase : (earliestEvent instanceof Date ? earliestEvent : null);
        const yearsHeld = baseForHoldingPeriod ? Math.max(0, (now - baseForHoldingPeriod) / (365 * 24 * 3600 * 1000)) : 0;
        let projectedValue = finalAssetPrice;
        if(typeKey === 'automobile' || typeKey === 'automotive'){
            const depreciationRate = 0.004; // 0.4% per year
            const yearsForDepreciation = baseForHoldingPeriod ? yearsHeld : 0;
            const depreciationAmount = finalAssetPrice * depreciationRate * yearsForDepreciation;
            projectedValue = Math.max(0, finalAssetPrice - depreciationAmount);
        }else{
            if(baseForHoldingPeriod){
                projectedValue = finalAssetPrice * Math.pow(1.05, Math.max(yearsHeld, 1));
            }else{
                projectedValue = finalAssetPrice * 1.05;
            }
        }
        projectedValue = Number(projectedValue.toFixed(2));

        const arrPercent = finalAssetPrice > 0 && rentLast12 > 0 ? (rentLast12 / finalAssetPrice) * 100 : null;
        const lastMonthShare = finalAssetPrice > 0 && rentLast30 > 0 ? Math.min(rentLast30 / finalAssetPrice, 1) : 0;

        results.push({
            name: position.displayName || position.Symbol || position.Name || `Asset ${idx+1}`,
            totalPurchase,
            totalExpenses,
            finalAssetPrice,
            rentCollected,
            rentYtd,
            avgMonthlyRent,
            arrPercent,
            lastRentAmount: latestRentAmount,
            lastRentDate: latestRentDate,
            lastMonthShare,
            utilization,
            netOutstanding: outstanding,
            payoffMonths,
            projectedValue,
            category: position.type || '—',
            positionRef: position
        });
    });

    const years = Array.from(rentYearSet).sort((a,b)=>a-b);
    if(years.length){
        const datasets = [];
        let hueIndex = 0;
        rentYearTotalsByAsset.forEach((yearMap, assetName)=>{
            if(!assetColorCache.has(assetName)){
                const hue = (hueIndex * 47) % 360;
                assetColorCache.set(assetName, {
                    border: `hsl(${hue},70%,52%)`,
                    background: `hsla(${hue},70%,52%,0.6)`
                });
                hueIndex += 1;
            }
            const colors = assetColorCache.get(assetName);
            const data = years.map(year=> Number((yearMap.get(year) || 0).toFixed(2)));
            datasets.push({
                label: assetName,
                data,
                backgroundColor: colors.background,
                borderColor: colors.border,
                borderWidth: 1,
                borderRadius: 4,
                stack: 'rent',
                type: 'bar'
            });
        });
        const totalData = years.map(year=> Number((rentYearTotals.get(year) || 0).toFixed(2)));
        datasets.push({
            label: 'All Assets',
            data: totalData,
            borderColor: 'rgba(37, 99, 235, 0.9)',
            borderWidth: 2,
            tension: 0.35,
            fill: false,
            pointRadius: 3,
            pointHoverRadius: 5,
            type: 'line'
        });
        const labelSet = new Set(datasets.map(ds=>ds.label));
        Array.from(realEstateRentFilters.keys()).forEach(label=>{
            if(!labelSet.has(label)) realEstateRentFilters.delete(label);
        });
        datasets.forEach(ds=>{
            if(!realEstateRentFilters.has(ds.label)) realEstateRentFilters.set(ds.label, true);
        });
        realEstateRentSeries = {
            labels: years.map(String),
            datasets
        };
    }else{
        realEstateRentSeries = { labels: [], datasets: [] };
        realEstateRentFilters.clear();
    }
    realEstateRentSeriesDirty = false;

    const rows = results.sort((a,b)=> b.netOutstanding - a.netOutstanding || b.finalAssetPrice - a.finalAssetPrice);
    const otherAssetsValue = positions
        .filter(p=> !portfolioCategories.has((p.type || '').toLowerCase()))
        .reduce((sum,p)=> sum + Number(p.marketValue || 0), 0);
    const denominator = realEstateTotalValue + otherAssetsValue;
    const allocation = denominator ? (realEstateTotalValue / denominator) * 100 : null;
    return { rows, totalValue: realEstateTotalValue, allocation };
}

function createRealEstateRow(stat){
    const row = document.createElement('div');
    row.className = 'realestate-row';

    const main = document.createElement('div');
    main.className = 'realestate-main';
    const heading = document.createElement('div');
    heading.className = 'asset-heading';
    const iconEl = stat.positionRef ? createAssetIconElement(stat.positionRef) : null;
    if(iconEl){
        heading.appendChild(iconEl);
    }
    const label = document.createElement('div');
    label.className = 'asset-label';
    const nameEl = document.createElement('div');
    nameEl.className = 'symbol';
    nameEl.textContent = stat.name;
    const metaEl = document.createElement('div');
    metaEl.className = 'pos';
    const categoryText = stat.category || '—';
    metaEl.textContent = `Category ${categoryText} · Purchase ${money(stat.totalPurchase)} · Expenses ${money(stat.totalExpenses)}`;
    label.appendChild(nameEl);
    label.appendChild(metaEl);
    heading.appendChild(label);
    main.appendChild(heading);
    row.appendChild(main);

    const metrics = document.createElement('div');
    metrics.className = 'realestate-metrics';
    const utilizationValueRaw = Number(stat.utilization);
    const hasUtilization = Number.isFinite(utilizationValueRaw);
    const utilizationValue = hasUtilization ? Math.max(0, Math.min(utilizationValueRaw, 100)) : null;
    const utilizationDisplay = hasUtilization ? `${utilizationValue.toFixed(1)}%` : '—';
    const utilizationProgress = hasUtilization ? (utilizationValue / 100) : 0;
    const isPassive = !stat.rentCollected && !stat.rentYtd && !stat.avgMonthlyRent;
    const lastRentDate = stat.lastRentDate instanceof Date
        ? stat.lastRentDate
        : (stat.lastRentDate ? new Date(stat.lastRentDate) : null);
    const daysSinceLastRent = lastRentDate instanceof Date
        ? (Date.now() - lastRentDate.getTime()) / (24 * 3600 * 1000)
        : null;
    const arrPercent = Number(stat.arrPercent);
    const hasArrPercent = Number.isFinite(arrPercent);
    const arrDisplay = hasArrPercent ? `${arrPercent.toFixed(1)}%` : null;
    const lastMonthShare = Number.isFinite(stat.lastMonthShare) ? Math.max(0, Math.min(Number(stat.lastMonthShare), 1)) : 0;
    const lastMonthPercentDisplay = lastMonthShare > 0 ? `${(lastMonthShare * 100).toFixed(1)}%` : null;
    const rows = [
        `<div><span class="label">Final Asset Price</span><span class="value">${money(stat.finalAssetPrice)}</span></div>`,
        `<div><span class="label">Projected Value</span><span class="value">${money(stat.projectedValue)}</span></div>`
    ];
    if(!isPassive){
        rows.splice(1, 0, `<div><span class="label">Outstanding</span><span class="value">${money(stat.netOutstanding)}</span></div>`);
    }
    if(!isPassive){
        rows.push(
            `<div><span class="label">Rent Collected</span><span class="value">${money(stat.rentCollected)}</span></div>`,
            `<div><span class="label">Rent YTD</span><span class="value">${money(stat.rentYtd)}</span></div>`,
            `<div><span class="label">Rent / Mo</span><span class="value">${money(stat.avgMonthlyRent)}</span></div>`,
            (arrDisplay ? `<div><span class="label">ARR</span><span class="value arr-value">${arrDisplay}</span></div>` : ''),
            (() => {
                const classes = ['circle-progress'];
                if(lastRentDate && Number.isFinite(daysSinceLastRent) && daysSinceLastRent <= 60){
                    classes.push('recent-rent');
                }
                const lastRentShare = lastMonthShare;
                const progressAngle = utilizationProgress * 360;
                const highlightAngle = Math.min(progressAngle, lastRentShare * 360);
                const startAngle = Math.max(progressAngle - highlightAngle, 0);
                let backgroundStyle = '';
                if(hasUtilization){
                    const baseGradient = `conic-gradient(var(--fill-color) 0deg ${progressAngle}deg, var(--track-color) ${progressAngle}deg 360deg)`;
                    const highlightGradient = highlightAngle > 0
                        ? `, conic-gradient(transparent 0deg ${startAngle}deg, var(--highlight-color) ${startAngle}deg ${progressAngle}deg, transparent ${progressAngle}deg 360deg)`
                        : '';
                    backgroundStyle = `background:${baseGradient}${highlightGradient};`;
                }
                const styleAttr = `--progress:${utilizationProgress};${backgroundStyle}`;
                const title = lastRentDate
                    ? ` title="Last rent ${money(stat.lastRentAmount || 0)} · ${formatDateShort(lastRentDate)}"`
                    : '';
                let html = `<div class="utilization-block"><span class="label">Utilization</span>`
                    + `<div class="${classes.join(' ')}" style="${styleAttr}"${title}>`
                    + `<div class="circle-progress-inner"><span>${utilizationDisplay}</span>${lastMonthPercentDisplay ? `<span class="inner-note">(${lastMonthPercentDisplay})</span>` : ''}</div>`
                    + `</div>`;
                html += `</div>`;
                return html;
            })(),
            `<div><span class="label">Payoff ETA</span><span class="value">${formatDurationFromMonths(stat.payoffMonths)}</span></div>`
        );
    }
    metrics.innerHTML = rows.filter(Boolean).join('');
    row.appendChild(metrics);

    const canOpenModal = stat.positionRef && Array.isArray(stat.positionRef.operations) && stat.positionRef.operations.some(op => Number(op.amount || 0));
    if(canOpenModal){
        row.classList.add('interactive-asset-row','realestate-row-interactive');
        row.setAttribute('role','button');
        row.setAttribute('tabindex','0');
        row.addEventListener('click', event=>{
            event.preventDefault();
            event.stopPropagation();
            lastTransactionTrigger = row;
            openTransactionModal(stat.positionRef);
        });
        row.addEventListener('keydown', event=>{
            if(event.key === 'Enter' || event.key === ' '){
                event.preventDefault();
                event.stopPropagation();
                lastTransactionTrigger = row;
                openTransactionModal(stat.positionRef);
            }
        });
    }

    return row;
}

function updateRealEstateRentals(){
    const container = document.getElementById('realestate-stats');
    if(!container) return;
    const analytics = computeRealEstateAnalytics();
    const stats = (analytics && Array.isArray(analytics.rows)) ? analytics.rows : [];
    renderRealEstateRentControls();
    renderRealEstateRentChart();
    const headerValueEl = document.getElementById('realestate-value');
    const headerAllocationEl = document.getElementById('realestate-allocation');
    if(headerValueEl){
        const totalValue = analytics && typeof analytics.totalValue === 'number' ? analytics.totalValue : 0;
        const projectedSum = stats.reduce((sum, stat)=> sum + Number(stat.projectedValue || 0), 0);
        const displayValue = Number.isFinite(projectedSum) && projectedSum > 0 ? projectedSum : totalValue;
        headerValueEl.textContent = money(displayValue);
    }
    if(headerAllocationEl){
        const allocationValue = analytics && typeof analytics.allocation === 'number' ? analytics.allocation : null;
        headerAllocationEl.textContent = Number.isFinite(allocationValue) ? `${allocationValue.toFixed(1)}%` : '—';
    }
    if(!stats.length){
        container.innerHTML = '<div class="pos">No rental activity recorded yet.</div>';
        return;
    }
    const isActiveStat = stat => {
        return ['rentCollected','rentYtd','avgMonthlyRent'].some(key=> Math.abs(Number(stat[key] || 0)) > 1e-6);
    };
    const activeStats = stats.filter(isActiveStat);
    const passiveStats = stats.filter(stat=> !isActiveStat(stat));

    container.innerHTML = '';

    const groups = [
        { id: 'active', title: 'Active assets', stats: activeStats, open: true, empty: 'No active rental or automobile assets yet.' },
        { id: 'passive', title: 'Passive assets', stats: passiveStats, open: false, empty: 'No passive rental or automobile assets right now.' }
    ];

    groups.forEach(group=>{
        const section = document.createElement('details');
        section.className = 'analytics-section realestate-group';
        const desiredOpen = realEstateGroupState[group.id];
        const shouldOpen = desiredOpen === undefined ? group.open : desiredOpen;
        section.open = Boolean(shouldOpen);
        const summary = document.createElement('summary');
        summary.innerHTML = `
            <div class="summary-left">
                <span class="summary-title">${group.title}</span>
                <span class="summary-count">${group.stats.length}</span>
            </div>
        `;
        section.appendChild(summary);
        const content = document.createElement('div');
        content.className = 'analytics-section-content';
        if(!group.stats.length){
            const empty = document.createElement('div');
            empty.className = 'pos';
            empty.textContent = group.empty;
            content.appendChild(empty);
        }else{
            group.stats.forEach(stat=>{
                content.appendChild(createRealEstateRow(stat));
            });
        }
        section.appendChild(content);
        section.addEventListener('toggle', ()=>{
            realEstateGroupState[group.id] = section.open;
        });
        container.appendChild(section);
    });
}

function renderRealEstateRentControls(){
    const container = document.getElementById('realestate-rent-filters');
    if(!container) return;
    container.innerHTML = '';
    if(!realEstateRentSeries.datasets.length){
        const empty = document.createElement('div');
        empty.className = 'pos';
        empty.textContent = 'No rent history yet.';
        container.appendChild(empty);
        return;
    }
    realEstateRentSeries.datasets.forEach(dataset=>{
        const label = dataset.label || 'Series';
        if(!realEstateRentFilters.has(label)){
            realEstateRentFilters.set(label, true);
        }
        const wrapper = document.createElement('label');
        wrapper.className = 'rent-filter';
        const input = document.createElement('input');
        input.type = 'checkbox';
        input.dataset.series = label;
        input.checked = realEstateRentFilters.get(label) !== false;
        input.addEventListener('change', (event)=>{
            const series = event.target.dataset.series;
            realEstateRentFilters.set(series, event.target.checked);
            renderRealEstateRentChart();
        });
        const span = document.createElement('span');
        span.textContent = label;
        wrapper.appendChild(input);
        wrapper.appendChild(span);
        container.appendChild(wrapper);
    });
}

function renderRealEstateRentChart(){
    const chartId = 'realestateRentChart';
    if(realEstateRentSeriesDirty){
        computeRealEstateAnalytics();
        renderRealEstateRentControls();
    }
    const filteredDatasets = realEstateRentSeries.datasets.filter(ds=> realEstateRentFilters.get(ds.label) !== false);
    const hasData = realEstateRentSeries.labels.length && filteredDatasets.some(ds=>Array.isArray(ds.data) && ds.data.some(v=>Math.abs(Number(v)) > 0));
    if(hasData){
        const chartData = {
            labels: realEstateRentSeries.labels,
            datasets: filteredDatasets
        };
        createOrUpdateChart(chartId, 'bar', chartData, {
            animation: { duration: 0 },
            plugins: { legend: { display: false } },
            interaction: { mode: 'index', intersect: false },
            scales: {
                x: {
                    ticks: { autoSkip: true },
                    grid: { display: false }
                },
                y: {
                    beginAtZero: true,
                    ticks: {
                        callback: value => money(value)
                    }
                }
            },
            elements: { point: { radius: 3, hoverRadius: 5 } },
            layout: { padding: { top: 6, bottom: 6, left: 6, right: 6 } }
        });
    }else if(charts[chartId]){
        charts[chartId].destroy();
        delete charts[chartId];
    }
}

function ensureTransactionModalElements(){
    if(transactionModal) return;
    transactionModal = document.getElementById('transaction-modal');
    if(!transactionModal) return;
    transactionModalTitle = document.getElementById('transaction-modal-title');
    transactionModalOperations = document.getElementById('transaction-modal-operations-count');
    transactionModalMeta = document.getElementById('transaction-modal-meta');
    transactionModalCanvas = document.getElementById('transaction-chart');
    modalChartContainer = transactionModal.querySelector('.modal-chart');
    transactionLotsContainer = document.getElementById('transaction-lots');
    if(transactionLotsContainer){
        ensureTransactionLotsControls();
    }
    viewLotsButton = document.getElementById('view-lots-btn');
    const closeButton = transactionModal.querySelector('.modal-close');
    if(closeButton){
        closeButton.addEventListener('click', closeTransactionModal);
    }
    transactionModal.addEventListener('click', event => {
        if(event.target === transactionModal){
            closeTransactionModal();
        }
    });
    if(viewLotsButton && !viewLotsButton.dataset.bound){
        viewLotsButton.addEventListener('click', ()=>{
            if(currentModalView === 'lots'){
                setModalView('chart');
            }else{
                if(lastTransactionPosition && lastTransactionData){
                    renderTransactionLots(lastTransactionPosition, lastTransactionData);
                }
                setModalView('lots');
            }
        });
        viewLotsButton.dataset.bound = 'true';
    }
}

function closeTransactionModal(){
    if(!transactionModal) return;
    transactionModal.classList.add('hidden');
    transactionModal.setAttribute('aria-hidden','true');
    if(!netWorthDetailModal || netWorthDetailModal.classList.contains('hidden')){
        document.body.classList.remove('modal-open');
    }
    if(lastTransactionTrigger && typeof lastTransactionTrigger.focus === 'function'){
        lastTransactionTrigger.focus({ preventScroll: false });
    }
    lastTransactionTrigger = null;
    lastTransactionData = null;
    lastTransactionPosition = null;
    setModalView('chart');
}

async function fetchHistoricalPriceSeries(position){
    const typeKey = String(position.type || '').toLowerCase();
    const finnhubSymbol = position.finnhubSymbol || mapFinnhubSymbol(position.Symbol || position.displayName || position.Name, position.type, false);
    const coinGeckoId = typeKey === 'crypto' ? mapCoinGeckoId(position) : null;
    const yahooSymbol = mapYahooSymbol(position);
    const cacheKey = `${finnhubSymbol || ''}|${coinGeckoId || yahooSymbol || ''}|${typeKey}`;
    const firstPurchaseTime = getFirstPurchaseTime(position);

    const prepareAndCacheSeries = rawSeries=>{
        const sanitized = sanitizePriceSeries(rawSeries);
        if(!sanitized.length){
            transactionPriceCache.set(cacheKey, []);
            position.priceHistory = [];
            return [];
        }
        const prepared = preparePriceSeries(sanitized, firstPurchaseTime, position);
        transactionPriceCache.set(cacheKey, prepared);
        position.priceHistory = prepared;
        return prepared;
    };

    if(transactionPriceCache.has(cacheKey)){
        const cached = transactionPriceCache.get(cacheKey);
        if(Array.isArray(cached) && cached.length){
            position.priceHistory = cached;
            return cached;
        }
    }

    let combinedSeries = [];
    const appendSeries = incoming=>{
        if(!Array.isArray(incoming) || !incoming.length) return false;
        combinedSeries = mergeHistoricalSeries(combinedSeries, incoming);
        return true;
    };
    const getLastPointDate = ()=>{
        if(!combinedSeries.length) return null;
        const last = combinedSeries[combinedSeries.length - 1];
        return toValidDate(last.time ?? last.x ?? last.date ?? last.timestamp ?? null);
    };
    const needsSeriesUpdate = ()=> needsHistoricalUpdate(getLastPointDate());

    const cacheKeyLocal = getLocalHistoricCacheKey(position);
    const localHeader = buildHistoricHeader(position);
    let localInfo = cacheKeyLocal ? await loadLocalHistoricalSeries(position) : null;
    let localPath = localInfo?.path || (cacheKeyLocal ? buildHistoricFilePath(cacheKeyLocal) : null);
    let shouldPersistLocal = false;

    if(localInfo && Array.isArray(localInfo.series) && localInfo.series.length){
        appendSeries(localInfo.series);
    }

    if(typeKey === 'crypto'){
        if(MASSIVE_API_KEY && needsSeriesUpdate()){
            const lastDate = getLastPointDate();
            let startDate = lastDate ? addDays(lastDate, 1) : null;
            const endDate = new Date();
            if(!startDate || startDate > endDate){
                startDate = addDays(endDate, -730);
            }
            if(startDate && startDate <= endDate){
                const updates = await fetchMassiveHistoricalSeries(position, startDate, endDate);
                if(Array.isArray(updates) && updates.length){
                    appendSeries(updates);
                    shouldPersistLocal = true;
                }
            }
        }
        if((!combinedSeries.length || needsSeriesUpdate()) && coinGeckoId){
            const updates = await fetchCoinGeckoSeries(coinGeckoId, firstPurchaseTime);
            if(Array.isArray(updates) && updates.length){
                appendSeries(updates);
                shouldPersistLocal = true;
            }
        }
    }

    if((!combinedSeries.length || needsSeriesUpdate()) && finnhubSymbol && FINNHUB_KEY){
        const series = await fetchFinnhubSeries(position, finnhubSymbol, firstPurchaseTime);
        if(Array.isArray(series) && series.length){
            appendSeries(series);
        }
    }

    if((!combinedSeries.length || needsSeriesUpdate()) && coinGeckoId && typeKey !== 'crypto'){
        const series = await fetchCoinGeckoSeries(coinGeckoId, firstPurchaseTime);
        if(Array.isArray(series) && series.length){
            appendSeries(series);
        }
    }

    if(!combinedSeries.length || needsSeriesUpdate()){
        if(yahooSymbol){
            const series = await fetchAlphaVantageSeries(yahooSymbol, typeKey, firstPurchaseTime);
            if(Array.isArray(series) && series.length){
                appendSeries(series);
            }
        }
    }

    if(combinedSeries.length){
        if(shouldPersistLocal && cacheKeyLocal && localPath){
            if(typeof window === 'undefined'){
                try{
                    await writeHistoricalSeriesFile(localPath, localHeader, combinedSeries);
                }catch(error){
                    console.warn('Failed to persist historical CSV', localPath, error);
                }
            }
            localHistoricalCache.set(cacheKeyLocal, {
                cacheKey: cacheKeyLocal,
                path: localPath,
                header: localHeader,
                series: combinedSeries,
                lastDate: getLastPointDate(),
                lastChecked: formatDateKey(new Date())
            });
        }
        return prepareAndCacheSeries(combinedSeries);
    }

    transactionPriceCache.set(cacheKey, []);
    position.priceHistory = [];
    return [];
}

function shouldPreloadPriceHistory(position){
    if(!position) return false;
    if(Array.isArray(position.priceHistory) && position.priceHistory.length){
        return false;
    }
    if(!Array.isArray(position.operations) || !position.operations.length){
        return false;
    }
    const typeKey = String(position.type || position.Category || '').toLowerCase();
    return typeKey === 'crypto' || typeKey === 'stock';
}

async function preloadHistoricalPriceSeries(){
    if(!Array.isArray(positions) || !positions.length){
        return;
    }
    const eligible = positions.filter(shouldPreloadPriceHistory);
    if(!eligible.length){
        return;
    }
    let completed = 0;
    const total = eligible.length;
    const updateProgress = ()=>{
        reportLoading(`Loading historical price data… ${completed}/${total}`);
    };
    updateProgress();
    const CONCURRENCY = 3;
    let nextIndex = 0;
    async function worker(){
        while(nextIndex < eligible.length){
            const currentIndex = nextIndex++;
            const position = eligible[currentIndex];
            try{
                await fetchHistoricalPriceSeries(position);
            }catch(error){
                console.warn('Historical preload failed', position?.Symbol || position?.displayName || position?.Name, error);
            }finally{
                completed += 1;
                updateProgress();
            }
        }
    }
    const workerCount = Math.min(CONCURRENCY, eligible.length);
    await Promise.all(Array.from({length: workerCount}, worker));
    reportLoading('Preparing dashboard data…');
}

async function fetchFinnhubSeries(position, rawSymbol, firstPurchaseTime){
    try{
        if(!position.finnhubSymbol){
            position.finnhubSymbol = rawSymbol;
            finnhubIndex.set(rawSymbol, position);
            symbolSet.add(rawSymbol);
            if(ws && ws.readyState === WebSocket.OPEN){
                try{ ws.send(JSON.stringify({type:'subscribe', symbol: rawSymbol})); }
                catch(error){ console.warn('Failed to subscribe for history symbol', rawSymbol, error); }
            }
        }
        const nowSec = Math.floor(Date.now()/1000);
        let fromMs = Date.now() - TRANSACTION_HISTORY_LOOKBACK_DAYS * 24 * 3600 * 1000;
        if(firstPurchaseTime){
            const marginMs = 30 * 24 * 3600 * 1000;
            fromMs = Math.max(fromMs, firstPurchaseTime - marginMs);
        }
        const fromSec = Math.floor(fromMs / 1000);
        const endpoint = isCryptoFinnhubSymbol(rawSymbol, position.type) ? 'crypto/candle' : 'stock/candle';
        const url = `${FINNHUB_REST}/${endpoint}?symbol=${encodeURIComponent(rawSymbol)}&resolution=D&from=${fromSec}&to=${nowSec}&token=${FINNHUB_KEY}`;
        const response = await fetch(url);
        if(!response.ok) return [];
        const json = await response.json();
        if(json && json.s === 'ok' && Array.isArray(json.t) && Array.isArray(json.c)){
            return json.t.map((ts, idx)=>{
                const close = Number(json.c?.[idx]);
                if(!Number.isFinite(close) || close <= 0) return null;
                return { x: ts * 1000, y: close };
            }).filter(Boolean);
        }
    }catch(error){
        console.warn('Historical price fetch failed', rawSymbol, error);
    }
    return [];
}

function getAlphaVantageKey(){
    if(typeof window !== 'undefined' && window.DASHBOARD_CONFIG && window.DASHBOARD_CONFIG.ALPHA_VANTAGE_KEY){
        return window.DASHBOARD_CONFIG.ALPHA_VANTAGE_KEY;
    }
    if(typeof ALPHA_VANTAGE_KEY !== 'undefined') return ALPHA_VANTAGE_KEY;
    return null;
}

async function fetchAlphaVantageSeries(symbol, typeKey, firstPurchaseTime){
    const apiKey = getAlphaVantageKey();
    if(!apiKey) return [];
    const params = new URLSearchParams();
    if(typeKey === 'crypto'){
        params.set('function', 'DIGITAL_CURRENCY_DAILY');
        params.set('symbol', symbol.replace(/-USD$/i, '')); // expect BTC-USD style
        params.set('market', 'USD');
    }else{
        params.set('function', 'TIME_SERIES_DAILY');
        params.set('symbol', symbol);
        params.set('outputsize', 'full');
    }
    params.set('apikey', apiKey);
    const url = `https://www.alphavantage.co/query?${params.toString()}`;
    try{
        const response = await fetch(url);
        if(!response.ok) return [];
        const json = await response.json();
        const series = [];
        let timeSeries;
        if(typeKey === 'crypto'){
            timeSeries = json['Time Series (Digital Currency Daily)'];
        }else{
            timeSeries = json['Time Series (Daily)'];
        }
        if(!timeSeries || typeof timeSeries !== 'object') return [];
        Object.entries(timeSeries).forEach(([dateStr, row])=>{
            const closeKey = typeKey === 'crypto' ? '4a. close (USD)' : '4. close';
            const close = Number(row?.[closeKey]);
            if(!Number.isFinite(close) || close <= 0) return;
            const date = new Date(dateStr + 'T00:00:00Z');
            if(firstPurchaseTime){
                const marginMs = 30 * 24 * 3600 * 1000;
                if(date.getTime() < firstPurchaseTime - marginMs) return;
            }
            series.push({ x: date.getTime(), y: close });
        });
        return series.sort((a,b)=> a.x - b.x);
    }catch(error){
        console.warn('Alpha Vantage history fetch failed', symbol, error);
    }
    return [];
}

async function fetchYahooQuote(symbol){
    const cacheKey = `quote:${symbol}`;
    const cached = yahooQuoteCache.get(cacheKey);
    const now = Date.now();
    if(cached && (now - cached.time) < 60 * 1000){
        return cached.value;
    }
    const url = `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${encodeURIComponent(symbol)}`;
    try{
        const json = await fetchJsonWithCorsFallback(url);
        const quote = json?.quoteResponse?.result?.[0];
        if(!quote) return null;
        const price = Number(quote.regularMarketPrice ?? quote.postMarketPrice ?? quote.preMarketPrice);
        const prevClose = Number(quote.regularMarketPreviousClose ?? quote.previousClose);
        const currency = quote.currency || null;
        const value = {
            price: Number.isFinite(price) && price > 0 ? price : null,
            prevClose: Number.isFinite(prevClose) && prevClose > 0 ? prevClose : null,
            currency
        };
        yahooQuoteCache.set(cacheKey, { time: now, value });
        return value;
    }catch(error){
        console.warn('Yahoo quote fetch failed', symbol, error);
        return null;
    }
}

function scheduleYahooFallback(position){
    if(!position) return Promise.resolve(false);
    if(position._yahooFallbackPromise){
        return position._yahooFallbackPromise;
    }
    const yahooSymbol = mapYahooSymbol(position);
    if(!yahooSymbol){
        return Promise.resolve(false);
    }
    const task = (async ()=>{
        const quote = await fetchYahooQuote(yahooSymbol);
        if(quote && Number.isFinite(quote.price) && quote.price > 0){
            const success = applyPriceUpdate(position, quote.price, quote.prevClose ?? null, 'yahoo');
            if(success){
                rangeDirty = true;
                scheduleUIUpdate({ immediate: true });
            }
            return success;
        }
        return false;
    })().catch(error=>{
        console.warn('Yahoo fallback error', yahooSymbol, error);
        return false;
    }).finally(()=>{
        position._yahooFallbackPromise = null;
    });
    position._yahooFallbackPromise = task;
    return task;
}

async function fetchCoinGeckoSeries(coinId, firstPurchaseTime){
    try{
        let fromMs = Date.now() - TRANSACTION_HISTORY_LOOKBACK_DAYS * 24 * 3600 * 1000;
        if(firstPurchaseTime){
            const marginMs = 30 * 24 * 3600 * 1000;
            fromMs = Math.max(fromMs, firstPurchaseTime - marginMs);
        }
        const days = Math.max(30, Math.ceil((Date.now() - fromMs) / (24 * 3600 * 1000)));
        const url = `https://api.coingecko.com/api/v3/coins/${encodeURIComponent(coinId)}/market_chart?vs_currency=usd&days=${days}`;
        const response = await fetch(url, { headers: { 'accept': 'application/json' } });
        if(!response.ok) return [];
        const json = await response.json();
        if(!json || !Array.isArray(json.prices)) return [];
        const series = json.prices.map(([timestamp, price])=>{
            const close = Number(price);
            if(!Number.isFinite(close) || close <= 0) return null;
            if(firstPurchaseTime){
                const marginMs = 30 * 24 * 3600 * 1000;
                if(timestamp < firstPurchaseTime - marginMs) return null;
            }
            return { x: timestamp, y: close };
        }).filter(Boolean);
        return series.sort((a,b)=> a.x - b.x);
    }catch(error){
        console.warn('CoinGecko history fetch failed', coinId, error);
    }
    return [];
}

function registerFinancialControllers(){ /* no-op placeholder */ }

function getOperationCount(position){
    if(!position || !Array.isArray(position.operations)) return 0;
    return position.operations.filter(op=>{
        const type = String(op.type || '').toLowerCase();
        if(type !== 'purchasesell') return false;
        if(op.skipInCharts) return false;
        return true;
    }).length;
}

function formatOperationCount(value){
    const count = Number(value);
    if(!Number.isFinite(count) || count < 0){
        return '0';
    }
    return count.toLocaleString();
}

function buildTransactionChartData(position){
    const operations = Array.isArray(position.operations) ? position.operations.filter(op => String(op.type || '').toLowerCase() === 'purchasesell' && !op.skipInCharts) : [];
    if(!operations.length){
        return {
            purchases: [],
            sales: [],
            baseline: [],
            fallbackPriceSeries: [],
            summary: {
                totalBuys: 0,
                totalSells: 0,
                netQty: 0,
                totalSpent: 0,
                totalProceeds: 0
            }
        };
    }

    const fallbackPrice = Number(position.displayPrice || position.currentPrice || position.lastKnownPrice || position.avgPrice || 0);
    const sorted = operations.map((op, index)=>{
        const date = op.date instanceof Date ? op.date : (op.rawDate ? new Date(op.rawDate) : new Date(Date.now() - (operations.length - index) * 24 * 3600 * 1000));
        return Object.assign({}, op, { date });
    }).sort((a,b)=>{
        if(a.date instanceof Date && b.date instanceof Date){
            return a.date - b.date;
        }
        return 0;
    });

    const purchases = [];
    const sales = [];
    const xValues = new Set();
    const fallbackPriceSeries = [];
    let totalBuys = 0;
    let totalSells = 0;
    let totalSpent = 0;
    let totalProceeds = 0;
    let maxAbsQty = 0;

    sorted.forEach((op, index)=>{
        const qty = Number(op.amount || 0);
        if(!qty) return;
        const rawSpent = Number(op.spent);
        let price = Number(op.price);
        const absQty = Math.abs(qty);
        if(absQty > maxAbsQty) maxAbsQty = absQty;
        if(!Number.isFinite(price) || price <= 0){
            if(Number.isFinite(rawSpent) && rawSpent !== 0 && qty){
                price = Math.abs(rawSpent / qty);
            }else{
                price = fallbackPrice;
            }
        }
        const date = op.date instanceof Date ? op.date : new Date(Date.now() - (sorted.length - index) * 24 * 3600 * 1000);
        const x = date.getTime();
        const spent = Number.isFinite(rawSpent) ? rawSpent : price * qty;
        const point = {
            x,
            y: price,
            r: 8,
            quantity: qty,
            price,
            date,
            spent,
            spentAbs: Math.abs(spent),
            rawQty: absQty
        };
        if(qty > 0){
            purchases.push(point);
            totalBuys += qty;
            totalSpent += Math.abs(spent);
        }else{
            sales.push(point);
            totalSells += Math.abs(qty);
            totalProceeds += Math.abs(spent);
        }
        xValues.add(x);
        fallbackPriceSeries.push({ x, y: price });
    });

    const meaningfulQty = purchases.concat(sales).map(point => point.rawQty || 0).filter(value => value > 0);
    const minAbsQty = meaningfulQty.length ? Math.min(...meaningfulQty) : 0;
    const maxQty = Math.max(maxAbsQty, minAbsQty || 1);
    const logDenominator = Math.log10((maxQty / Math.max(minAbsQty || maxQty, 1e-12)) + 1);
    const computeRadius = qty => {
        if(!qty || !Number.isFinite(qty)) return 8;
        if(!minAbsQty || logDenominator === 0){
            return 6 + (Math.sqrt(qty / maxQty) || 0) * 18;
        }
        const ratio = Math.log10((qty / Math.max(minAbsQty, 1e-12)) + 1) / logDenominator;
        return 6 + Math.min(1, Math.max(0, ratio)) * 26;
    };
    purchases.forEach(point => { point.r = computeRadius(point.rawQty || 0); });
    sales.forEach(point => { point.r = computeRadius(point.rawQty || 0); });

    const baseline = Array.from(xValues).sort((a,b)=>a-b).map(x=>({ x, y: fallbackPrice }));

    return {
        purchases,
        sales,
        baseline,
        fallbackPriceSeries: fallbackPriceSeries.sort((a,b)=> a.x - b.x),
        summary: {
            totalBuys,
            totalSells,
            netQty: totalBuys - totalSells,
            totalSpent,
            totalProceeds
        }
    };
}

function buildTransactionChartConfig(data, position, priceSeries = []){
    const datasets = [];
    const basePriceSeries = (priceSeries.length ? priceSeries : (data.fallbackPriceSeries || [])).map(point=> ({ x: point.x, y: point.y ?? point.c ?? point.price ?? point.value }));
    const livePrice = Number(position?.currentPrice ?? position?.displayPrice ?? position?.lastKnownPrice ?? position?.avgPrice ?? 0);
    const fallbackPrice = Number(position.displayPrice || position.currentPrice || position.lastKnownPrice || position.avgPrice || 0) || 0;
    const chartHasTransactions = data.purchases.length || data.sales.length;
    const avgPurchasePrice = data.purchases.length
        ? data.purchases.reduce((sum, point)=> sum + Number(point.price ?? point.y ?? 0), 0) / data.purchases.length
        : fallbackPrice;
    const hasLivePrice = Number.isFinite(livePrice) && livePrice > 0;
    const avgLineValue = Number.isFinite(avgPurchasePrice) ? avgPurchasePrice : fallbackPrice;
    const currentLineValue = hasLivePrice ? livePrice : null;

    const effectivePriceSeries = (()=> {
        const series = [...basePriceSeries];
        if(Number.isFinite(livePrice) && livePrice > 0){
            const nowPoint = { x: new Date(), y: livePrice };
            const lastPoint = series[series.length - 1];
            if(!lastPoint){
                series.push(nowPoint);
            }else{
                const lastTime = lastPoint.x instanceof Date ? lastPoint.x.getTime() : new Date(lastPoint.x).getTime();
                const nowTime = nowPoint.x.getTime();
                if(!Number.isFinite(lastTime) || Math.abs(nowTime - lastTime) > 60 * 1000){
                    series.push(nowPoint);
                }else{
                    series[series.length - 1] = nowPoint;
                }
            }
        }
        return series;
    })();

    if(effectivePriceSeries.length){
        datasets.push({
            type: 'line',
            label: 'Price history',
            data: effectivePriceSeries,
            borderColor: TRANSACTION_CHART_COLORS.priceLine,
            backgroundColor: 'rgba(59, 130, 246, 0.12)',
            borderWidth: 2,
            fill: 'origin',
            tension: 0.25,
            pointRadius: 0,
            order: 0
        });
    }

    const baselineAnchors = data.purchases.length ? data.purchases : (data.fallbackPriceSeries || []);
    const lineAnchorSource = effectivePriceSeries.length
        ? effectivePriceSeries
        : (baselineAnchors.length ? baselineAnchors : []);

    if(lineAnchorSource.length && Number.isFinite(avgLineValue)){
        const avgLineData = lineAnchorSource.map(point=> ({ x: point.x, y: avgLineValue }));
        datasets.push({
            type: 'line',
            label: 'Avg buy price',
            data: avgLineData,
            borderColor: 'rgba(251, 146, 60, 0.95)',
            borderDash: [4, 4],
            borderWidth: 1.5,
            pointRadius: 0,
            tension: 0,
            order: 1,
            yAxisID: 'y'
        });
    }

    if(lineAnchorSource.length && Number.isFinite(currentLineValue)){
        const currentLineData = lineAnchorSource.map(point=> ({ x: point.x, y: currentLineValue }));
        datasets.push({
            type: 'line',
            label: 'Current price',
            data: currentLineData,
            borderColor: 'rgba(239, 68, 68, 0.95)',
            borderDash: [6, 4],
            borderWidth: 1.5,
            pointRadius: 0,
            tension: 0,
            order: 1,
            yAxisID: 'y'
        });
    }

    if(chartHasTransactions){
        if(data.purchases.length){
            datasets.push({
                type: 'scatter',
                label: 'Purchases',
                data: data.purchases,
                backgroundColor: TRANSACTION_CHART_COLORS.buys,
                borderColor: TRANSACTION_CHART_COLORS.buysBorder,
                pointBorderWidth: 1.5,
                pointRadius: ctx => ctx.raw ? ctx.raw.r : 6,
                pointHoverRadius: ctx => ctx.raw ? ctx.raw.r + 2 : 8,
                pointHoverBorderWidth: 2,
                order: 2
            });
        }
        if(data.sales.length){
            datasets.push({
                type: 'scatter',
                label: 'Sales',
                data: data.sales,
                backgroundColor: TRANSACTION_CHART_COLORS.sells,
                borderColor: TRANSACTION_CHART_COLORS.sellsBorder,
                pointBorderWidth: 1.5,
                pointRadius: ctx => ctx.raw ? ctx.raw.r : 6,
                pointHoverRadius: ctx => ctx.raw ? ctx.raw.r + 2 : 8,
                pointHoverBorderWidth: 2,
                order: 2
            });
        }
    }

    const yValues = [...data.purchases, ...data.sales].map(point=> point.y);
    const priceYValues = effectivePriceSeries.map(point=> point.y);
    const combinedValues = [...yValues, ...priceYValues];
    if(Number.isFinite(avgLineValue)) combinedValues.push(avgLineValue);
    if(Number.isFinite(currentLineValue)) combinedValues.push(currentLineValue);
    const filteredCombined = combinedValues.filter(value => Number.isFinite(value));
    const minY = filteredCombined.length ? Math.min(...filteredCombined) : fallbackPrice;
    const maxY = filteredCombined.length ? Math.max(...filteredCombined) : fallbackPrice;

    const markerConfig = {
        avg: Number.isFinite(avgLineValue) ? { value: avgLineValue, label: 'Avg price', shortLabel: 'Avg', color: 'rgba(251, 146, 60, 1)' } : null,
        current: Number.isFinite(currentLineValue) ? { value: currentLineValue, label: 'Current price', shortLabel: 'Current', color: 'rgba(239, 68, 68, 1)' } : null
    };
    const markerList = Object.values(markerConfig).filter(Boolean);
    const matchMarker = (value, range)=>{
        const tolerance = Math.max(1e-6, (range || Math.abs(maxY - minY) || 1) * 0.002);
        return markerList.find(marker => Math.abs(marker.value - value) <= tolerance) || null;
    };

    const getMarkerForContext = ctx => {
        if(!ctx || !ctx.tick) return null;
        const tickValue = Number(ctx.tick.value);
        if(!Number.isFinite(tickValue)) return null;
        const range = ctx.scale && Number.isFinite(ctx.scale.max) && Number.isFinite(ctx.scale.min)
            ? Math.abs(ctx.scale.max - ctx.scale.min)
            : null;
        return matchMarker(tickValue, range);
    };

    const config = {
        type: 'scatter',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            parsing: false,
            animation: false,
            onClick(event, elements, chart){
                if(!elements || !elements.length) return;
                const element = elements[0];
                const dataset = chart.data?.datasets?.[element.datasetIndex];
                if(!dataset || dataset.label !== 'Purchases') return;
                const baseline = chart.options?.plugins?.hoverBaseline;
                if(!baseline) return;
                if(baseline.mode === 'avg' && Number.isFinite(baseline.currentValue)){
                    baseline.mode = 'current';
                    baseline.value = baseline.currentValue;
                }else if(Number.isFinite(baseline.avgValue)){
                    baseline.mode = 'avg';
                    baseline.value = baseline.avgValue;
                }
                chart.update('none');
            },
            plugins: {
                legend: { labels: { usePointStyle: true } },
                tooltip: {
                    callbacks: {
                        label(ctx){
                            const raw = ctx.raw || {};
                            if(ctx.dataset.type === 'line'){
                                if(ctx.dataset.label === 'Avg buy price'){
                                    const value = Number(raw.y ?? ctx.parsed?.y ?? 0);
                                    return `Avg buy price ${money(value)}`;
                                }
                                if(ctx.dataset.label === 'Current price'){
                                    const value = Number(raw.y ?? ctx.parsed?.y ?? 0);
                                    return `Current price ${money(value)}`;
                                }
                                if(ctx.dataset.label === 'Price history'){
                                    const value = Number(raw.y ?? ctx.parsed?.y ?? 0);
                                    return `Price history ${money(value)}`;
                                }
                            }
                            const type = raw.quantity > 0 ? 'Buy' : 'Sell';
                            const qtyText = `Qty ${formatQty(Math.abs(raw.quantity || 0))}`;
                            const priceText = `@ ${money(raw.price || raw.y || 0)}`;
                            const usdText = raw.spentAbs ? ` · ${money(raw.spentAbs)}` : '';
                            const dateLabel = raw.date instanceof Date ? formatDateShort(raw.date) : formatDateShort(new Date(Number(raw.x || ctx.parsed.x)));
                            return `${type} ${qtyText} ${priceText}${usdText}${dateLabel ? ' · ' + dateLabel : ''}`;
                        }
                    }
                },
                hoverBaseline: {
                    value: Number.isFinite(avgLineValue) ? avgLineValue : (Number.isFinite(currentLineValue) ? currentLineValue : null),
                    mode: Number.isFinite(avgLineValue) ? 'avg' : (Number.isFinite(currentLineValue) ? 'current' : null),
                    avgValue: Number.isFinite(avgLineValue) ? avgLineValue : null,
                    currentValue: Number.isFinite(currentLineValue) ? currentLineValue : null
                },
                zoom: {
                    limits: {
                        x: { min: 'original', max: 'original' },
                        y: { min: 'original', max: 'original' }
                    },
                    zoom: {
                        wheel: { enabled: true, speed: 0.1 },
                        pinch: { enabled: true },
                        mode: 'xy'
                    },
                    pan: {
                        enabled: true,
                        mode: 'xy'
                    }
                }
            },
            scales: {
                x: {
                    type: 'time',
                    time: { tooltipFormat: 'PP' },
                    title: { display: true, text: 'Date' },
                    grid: { color: 'rgba(148, 163, 184, 0.25)' }
                },
                y: {
                    customMarkers: markerConfig,
                    afterBuildTicks(axis){
                        const range = Math.abs(axis.max - axis.min) || 1;
                        markerList.forEach(marker=>{
                            if(!marker) return;
                            const exists = axis.ticks.some(tick => Math.abs(tick.value - marker.value) <= Math.max(1e-6, range * 0.002));
                            if(!exists){
                                axis.ticks.push({ value: marker.value });
                            }
                        });
                        axis.ticks.sort((a,b)=> a.value - b.value);
                    },
                    beginAtZero: false,
                    suggestedMin: Number.isFinite(minY) ? minY * 0.92 : undefined,
                    suggestedMax: Number.isFinite(maxY) ? maxY * 1.08 : undefined,
                    title: { display: false },
                    grid: { color: 'rgba(148, 163, 184, 0.25)' },
                    ticks: {
                        callback(value){
                            const range = this && Number.isFinite(this.max) && Number.isFinite(this.min)
                                ? Math.abs(this.max - this.min)
                                : null;
                            const marker = matchMarker(Number(value), range);
                            const text = formatPriceTickValue(Number(value));
                            return marker ? `${marker.shortLabel}: ${text}` : text;
                        },
                        color(ctx){
                            const marker = getMarkerForContext(ctx);
                            return marker ? marker.color : 'rgba(226, 232, 240, 0.82)';
                        },
                        font(ctx){
                            const marker = getMarkerForContext(ctx);
                            return marker ? { weight: '600', size: 12 } : { size: 11 };
                        }
                    }
                }
            }
        }
    };
    config.plugins = [...(config.plugins || []), transactionHoverPlugin];
    return config;
}

function setModalView(view){
    currentModalView = view;
    if(viewLotsButton){
        viewLotsButton.textContent = view === 'lots' ? 'View chart' : 'View lots';
        viewLotsButton.classList.toggle('active', view === 'lots');
    }
    if(modalChartContainer){
        modalChartContainer.classList.toggle('hidden', view === 'lots');
    }
    if(transactionLotsContainer){
        transactionLotsContainer.classList.toggle('hidden', view !== 'lots');
    }
    if(view === 'chart' && !transactionChart && lastTransactionPosition){
        loadHistoricalPriceSeries(lastTransactionPosition);
    }
}

const LOT_SORT_FIELDS = [
    { value: 'date', label: 'Date' },
    { value: 'qty', label: 'Quantity' },
    { value: 'price', label: 'Price' },
    { value: 'amount', label: 'Amount' },
    { value: 'pnl', label: 'PnL' }
];

const LOT_DIRECTION_OPTIONS = [
    { value: 'desc', label: 'Descending' },
    { value: 'asc', label: 'Ascending' }
];

const LOT_GROUP_OPTIONS = [
    { value: 'none', label: 'No grouping' },
    { value: 'type', label: 'Buy / Sell' },
    { value: 'year', label: 'Year' },
    { value: 'gain', label: 'Gains / Losses' }
];

function updateLotsSort(field, direction){
    const allowed = new Set(LOT_SORT_FIELDS.map(option => option.value));
    const nextField = allowed.has(field) ? field : transactionLotsSortField;
    const nextDirection = direction === 'asc' ? 'asc' : direction === 'desc' ? 'desc' : transactionLotsSortDirection;
    const changed = nextField !== transactionLotsSortField || nextDirection !== transactionLotsSortDirection;
    transactionLotsSortField = nextField;
    transactionLotsSortDirection = nextDirection;
    return changed;
}

function ensureTransactionLotsControls(){
    if(!transactionLotsContainer) return;
    if(!transactionLotsContainer.dataset.controls){
        transactionLotsContainer.innerHTML = '';
        transactionLotsControls = document.createElement('div');
        transactionLotsControls.className = 'lots-controls';
        transactionLotsControls.setAttribute('role','group');

        transactionLotsList = document.createElement('div');
        transactionLotsList.className = 'lots-list';

        transactionLotsContainer.appendChild(transactionLotsControls);
        transactionLotsContainer.appendChild(transactionLotsList);

        buildTransactionLotsControls();
        transactionLotsContainer.dataset.controls = 'true';
    }else{
        transactionLotsControls = transactionLotsContainer.querySelector('.lots-controls');
        transactionLotsList = transactionLotsContainer.querySelector('.lots-list');
    }
    if(transactionLotsGroupSelect){
        transactionLotsGroupSelect.value = transactionLotsGroupKey;
    }
}

function buildTransactionLotsControls(){
    if(!transactionLotsControls) return;
    transactionLotsControls.innerHTML = '';

    const groupLabel = document.createElement('label');
    groupLabel.className = 'lots-control';
    groupLabel.innerHTML = '<span>Group by</span>';
    transactionLotsGroupSelect = document.createElement('select');
    transactionLotsGroupSelect.id = 'lots-group-by';
    transactionLotsGroupSelect.setAttribute('aria-label', 'Group lots by');
    LOT_GROUP_OPTIONS.forEach(option=>{
        const opt = document.createElement('option');
        opt.value = option.value;
        opt.textContent = option.label;
        transactionLotsGroupSelect.appendChild(opt);
    });
    groupLabel.appendChild(transactionLotsGroupSelect);
    transactionLotsControls.appendChild(groupLabel);

    transactionLotsGroupSelect.addEventListener('change', ()=>{
        transactionLotsGroupKey = transactionLotsGroupSelect.value;
        if(lastTransactionPosition && lastTransactionData){
            renderTransactionLots(lastTransactionPosition, lastTransactionData);
        }
    });
}

function getLotGroupKey(item){
    switch(transactionLotsGroupKey){
        case 'type':
            return item.typeLabel || 'Other';
        case 'year':
            return item.date instanceof Date && !Number.isNaN(item.date.getTime())
                ? String(item.date.getFullYear())
                : '__no-date__';
        case 'gain':
            return item.pnlValue >= 0 ? 'gain' : 'loss';
        default:
            return '__all__';
    }
}

function getLotGroupLabel(key){
    switch(transactionLotsGroupKey){
        case 'type':
            return key;
        case 'year':
            return key === '__no-date__' ? 'No date' : `Year ${key}`;
        case 'gain':
            if(key === 'gain') return 'Gains';
            if(key === 'loss') return 'Losses';
            return key;
        default:
            return '';
    }
}

function renderTransactionLots(position, data){
    if(!transactionLotsContainer) return;
    ensureTransactionLotsControls();
    const listElement = transactionLotsList || transactionLotsContainer;
    const operations = Array.isArray(position.operations) ? position.operations.slice() : [];
    const currentPrice = Number(position.displayPrice || position.currentPrice || position.lastKnownPrice || position.avgPrice || 0);
    const processed = [];
    let usingCustomLots = false;
    const supportsLots = ['stock','crypto'].includes((position.type || '').toLowerCase());
    const openLots = supportsLots && Array.isArray(position.lots) ? position.lots.filter(lot=> lot && Number(lot.qty) > 1e-9) : [];
    const closedSegments = supportsLots && Array.isArray(position.closedSales) ? position.closedSales.slice() : [];
    const hasOpenQty = Math.abs(Number(position.qty || 0)) > 1e-6;

    if(supportsLots){
        if(hasOpenQty && openLots.length){
            usingCustomLots = true;
            const sortedLots = openLots.slice().sort((a,b)=>{
                const ad = a.date instanceof Date ? a.date.getTime() : -Infinity;
                const bd = b.date instanceof Date ? b.date.getTime() : -Infinity;
                return ad - bd;
            });
            sortedLots.forEach((lot, index)=>{
                const lotDate = lot.date instanceof Date ? lot.date : (lot.date ? new Date(lot.date) : null);
                const buyPrice = Number(lot.costPerUnit) || 0;
                const qty = Number(lot.qty) || 0;
                const baseAmount = qty * buyPrice;
                const currentValue = qty * currentPrice;
                const pnlValue = currentValue - baseAmount;
                const pnlPct = baseAmount ? (pnlValue / baseAmount) * 100 : null;
                processed.push({
                    id: lot.id || `open-${index}`,
                    date: lotDate,
                    dateValue: lotDate instanceof Date && !Number.isNaN(lotDate.getTime()) ? lotDate.getTime() : -Infinity,
                    dateLabel: lotDate instanceof Date && !Number.isNaN(lotDate.getTime()) ? formatDateShort(lotDate) : '—',
                    typeLabel: 'Open lot',
                    absQty: qty,
                    price: buyPrice,
                    baseAmount,
                    amountLabel: 'Invested',
                    pnlValue,
                    pnlPct,
                    pnlClass: pnlValue >= 0 ? 'lot-positive' : 'lot-negative'
                });
            });
        }else if(!hasOpenQty && closedSegments.length){
            usingCustomLots = true;
            const groupMap = new Map();
            closedSegments.forEach((chunk, idx)=>{
                const key = chunk.saleId || `sale-${idx}`;
                if(!groupMap.has(key)){
                    groupMap.set(key, {
                        saleId: key,
                        sellDate: chunk.sellDate instanceof Date ? chunk.sellDate : (chunk.sellDate ? new Date(chunk.sellDate) : null),
                        sellPricePerUnit: Number(chunk.sellPricePerUnit) || 0,
                        chunks: [],
                        totalProceeds: 0,
                        totalCost: 0
                    });
                }
                const group = groupMap.get(key);
                const buyDate = chunk.buyDate instanceof Date ? chunk.buyDate : (chunk.buyDate ? new Date(chunk.buyDate) : null);
                group.chunks.push({
                    qty: Number(chunk.qty) || 0,
                    buyCostPerUnit: Number(chunk.buyCostPerUnit) || 0,
                    buyDate
                });
                group.totalProceeds += Number(chunk.totalProceeds) || 0;
                group.totalCost += Number(chunk.totalCost) || 0;
                if(!group.sellDate && chunk.sellDate){
                    group.sellDate = chunk.sellDate instanceof Date ? chunk.sellDate : new Date(chunk.sellDate);
                }
                if(!group.sellPricePerUnit && Number.isFinite(chunk.sellPricePerUnit)){
                    group.sellPricePerUnit = Number(chunk.sellPricePerUnit);
                }
            });
            const groups = Array.from(groupMap.values()).sort((a,b)=>{
                const ad = a.sellDate instanceof Date ? a.sellDate.getTime() : -Infinity;
                const bd = b.sellDate instanceof Date ? b.sellDate.getTime() : -Infinity;
                return ad - bd;
            });
            groups.forEach(group=>{
                group.chunks.sort((a,b)=>{
                    const ad = a.buyDate instanceof Date ? a.buyDate.getTime() : -Infinity;
                    const bd = b.buyDate instanceof Date ? b.buyDate.getTime() : -Infinity;
                    return ad - bd;
                });
                group.chunks.forEach((chunk, idx)=>{
                    const cost = chunk.qty * chunk.buyCostPerUnit;
                    processed.push({
                        id: `buy-${group.saleId}-${idx}`,
                        date: chunk.buyDate,
                        dateValue: chunk.buyDate instanceof Date && !Number.isNaN(chunk.buyDate.getTime()) ? chunk.buyDate.getTime() : -Infinity,
                        dateLabel: chunk.buyDate instanceof Date && !Number.isNaN(chunk.buyDate.getTime()) ? formatDateShort(chunk.buyDate) : '—',
                        typeLabel: 'Buy lot',
                        absQty: chunk.qty,
                        price: chunk.buyCostPerUnit,
                        baseAmount: cost,
                        amountLabel: 'Invested',
                        pnlValue: 0,
                        pnlPct: null,
                        pnlClass: 'lot-neutral',
                        pnlDisplay: '—'
                    });
                });
                const saleQty = group.chunks.reduce((sum, chunk)=> sum + chunk.qty, 0);
                const saleDate = group.sellDate;
                const pnlValue = group.totalProceeds - group.totalCost;
                const pnlPct = group.totalCost ? (pnlValue / group.totalCost) * 100 : null;
                processed.push({
                    id: `sell-${group.saleId}`,
                    date: saleDate,
                    dateValue: saleDate instanceof Date && !Number.isNaN(saleDate.getTime()) ? saleDate.getTime() : -Infinity,
                    dateLabel: saleDate instanceof Date && !Number.isNaN(saleDate.getTime()) ? formatDateShort(saleDate) : '—',
                    typeLabel: 'Sell lot',
                    absQty: saleQty,
                    price: group.sellPricePerUnit,
                    baseAmount: group.totalProceeds,
                    amountLabel: 'Proceeds',
                    pnlValue,
                    pnlPct,
                    pnlClass: pnlValue >= 0 ? 'lot-positive' : 'lot-negative'
                });
            });
        }
    }

    if(!usingCustomLots && !operations.length){
        listElement.innerHTML = '<div class="pos">No transactions recorded yet.</div>';
        return;
    }

    if(!usingCustomLots){
        operations.forEach((op, index)=>{
            if(op.skipInCharts || op.isReinvesting) return;
            const qtySigned = Number(op.amount || 0);
            if(!qtySigned) return;
            const absQty = Math.abs(qtySigned);
            const date = op.date instanceof Date ? op.date : (op.rawDate ? new Date(op.rawDate) : null);
            const price = Number(op.price || currentPrice || 0) || 0;
            const rawSpent = Number(op.spent);
            let baseAmount = Number.isFinite(rawSpent) ? Math.abs(rawSpent) : Math.abs(price * absQty);
            const typeLabel = qtySigned > 0 ? 'Buy' : 'Sell';
            const amountLabel = qtySigned > 0 ? 'Invested' : 'Proceeds';
            let pnlValue = 0;
            if(qtySigned > 0){
                if(!baseAmount) baseAmount = Math.abs(price * absQty);
                const currentValue = currentPrice * absQty;
                pnlValue = currentValue - baseAmount;
            }else{
                if(!baseAmount) baseAmount = Math.abs(price * absQty);
                const estimatedCost = Math.abs(price * absQty);
                pnlValue = baseAmount - estimatedCost;
            }
            const pnlPct = baseAmount ? (pnlValue / baseAmount) * 100 : 0;
            const pnlClass = pnlValue >= 0 ? 'lot-positive' : 'lot-negative';
            processed.push({
                id: op.id || `lot-${index}`,
                date,
                dateValue: date instanceof Date && !Number.isNaN(date.getTime()) ? date.getTime() : -Infinity,
                dateLabel: date instanceof Date && !Number.isNaN(date.getTime()) ? formatDateShort(date) : '—',
                typeLabel,
                absQty,
                price,
                baseAmount,
                amountLabel,
                pnlValue,
                pnlPct,
                pnlClass
            });
        });
    }

    if(!processed.length){
        listElement.innerHTML = '<div class="pos">No lots to display yet.</div>';
        return;
    }

    const sortComparators = {
        date: (a, b)=> a.dateValue - b.dateValue,
        qty: (a, b)=> a.absQty - b.absQty,
        price: (a, b)=> a.price - b.price,
        amount: (a, b)=> a.baseAmount - b.baseAmount,
        pnl: (a, b)=> a.pnlValue - b.pnlValue
    };

    const comparator = sortComparators[transactionLotsSortField] || sortComparators.date;
    const sorted = processed.slice().sort((a, b)=>{
        const result = comparator(a, b);
        return transactionLotsSortDirection === 'desc' ? -result : result;
    });

    const groups = [];
    if(transactionLotsGroupKey === 'none'){
        groups.push({ label: null, items: sorted });
    }else{
        const map = new Map();
        sorted.forEach(item=>{
            const key = getLotGroupKey(item);
            if(!map.has(key)){
                map.set(key, []);
            }
            map.get(key).push(item);
        });
        map.forEach((items, key)=>{
            groups.push({ label: getLotGroupLabel(key), items });
        });
    }

    listElement.innerHTML = '';
    const LOT_COLUMNS = [
        { key: 'date', label: 'Date', className: 'lot-cell-date' },
        { key: 'qty', label: 'Type / Qty', className: 'lot-cell-type' },
        { key: 'price', label: 'Price', className: 'lot-cell-price' },
        { key: 'amount', label: 'Amount', className: 'lot-cell-amount' },
        { key: 'pnl', label: 'P&L', className: 'lot-cell-pnl' }
    ];

    const appendLotsHeader = target => {
        const header = document.createElement('div');
        header.className = 'lot-header';
        LOT_COLUMNS.forEach(column=>{
            const button = document.createElement('button');
            button.type = 'button';
            button.className = 'lot-header-cell';
            button.textContent = column.label;
            const isActive = transactionLotsSortField === column.key;
            button.setAttribute('aria-sort', isActive ? (transactionLotsSortDirection === 'asc' ? 'ascending' : 'descending') : 'none');
            if(isActive){
                button.classList.add('sorted', `dir-${transactionLotsSortDirection}`);
            }
            button.addEventListener('click', ()=>{
                const currentlyActive = transactionLotsSortField === column.key;
                const nextDirection = currentlyActive && transactionLotsSortDirection === 'asc' ? 'desc' : 'asc';
                updateLotsSort(column.key, nextDirection);
                if(lastTransactionPosition && lastTransactionData){
                    renderTransactionLots(lastTransactionPosition, lastTransactionData);
                }
            });
            header.appendChild(button);
        });
        target.appendChild(header);
    };

    let headerInserted = false;
    groups.forEach(group=>{
        if(transactionLotsGroupKey !== 'none'){
            const heading = document.createElement('div');
            heading.className = 'lot-group-heading';
            heading.textContent = group.label;
            listElement.appendChild(heading);
            appendLotsHeader(listElement);
        }else if(group.items.length && !headerInserted){
            appendLotsHeader(listElement);
            headerInserted = true;
        }
        group.items.forEach(item=>{
            const pnlPercentDisplay = Number.isFinite(item.pnlPct) ? formatPercent(item.pnlPct) : '—';
            const pnlClass = item.pnlClass || (item.pnlValue >= 0 ? 'lot-positive' : 'lot-negative');
            const pnlContent = item.pnlDisplay !== undefined
                ? item.pnlDisplay
                : `${money(item.pnlValue)} (${pnlPercentDisplay})`;
            const row = document.createElement('div');
            row.className = 'lot-row';
            row.innerHTML = `
                <div class="lot-cell lot-cell-date"><strong>${item.dateLabel}</strong></div>
                <div class="lot-cell lot-cell-type">${item.typeLabel} · ${formatQty(item.absQty)}</div>
                <div class="lot-cell lot-cell-price">${money(item.price)}</div>
                <div class="lot-cell lot-cell-amount">${item.amountLabel} ${money(item.baseAmount)}</div>
                <div class="lot-cell lot-cell-pnl"><span class="lot-value ${pnlClass}">${pnlContent}</span></div>
            `;
            listElement.appendChild(row);
        });
    });

    listElement.scrollTop = 0;
}

function renderTransactionMeta(position, data){
    if(!transactionModalMeta) return;
    const summary = data?.summary || {
        netQty: 0,
        totalBuys: 0,
        totalSells: 0,
        totalSpent: 0,
        totalProceeds: 0
    };
    const purchases = Array.isArray(data?.purchases) ? data.purchases : [];
    const fallbackAverage = Number(position.avgPrice || position.lastPurchasePrice || position.displayPrice || position.currentPrice || 0) || 0;
    const avgPurchasePrice = purchases.length
        ? purchases.reduce((sum, point)=> sum + Number(point.price ?? point.y ?? 0), 0) / purchases.length
        : fallbackAverage;
    const currentPriceCandidates = [
        position.displayPrice,
        position.currentPrice,
        position.lastKnownPrice,
        position.lastPurchasePrice,
        position.avgPrice
    ].map(value=> Number(value)).filter(value=> Number.isFinite(value) && Math.abs(value) > 1e-9);
    const currentPrice = currentPriceCandidates.length ? currentPriceCandidates[0] : fallbackAverage;
    const purchaseCount = purchases.length;
    const createPlate = (html, detail = '')=> ({ html, detail });
    const netQtyDetail = `Buys ${formatQty(summary.totalBuys)} minus sells ${formatQty(summary.totalSells)} equals ${formatQty(summary.netQty)} net units.`;
    const avgPriceDetail = purchaseCount
        ? `${purchaseCount} buy${purchaseCount === 1 ? '' : 's'} averaged (sum of fill prices ÷ count) = ${money(avgPurchasePrice)}.`
        : `No recorded buys; fallback price ${money(fallbackAverage)} used for averages.`;
    const currentPriceDetail = `Comparison price picked from display → current → last known → last purchase → avg. Using ${money(currentPrice)} for insights.`;
    const totalBoughtDetail = `Aggregate quantity across buy operations: ${formatQty(summary.totalBuys)}.`;
    const totalSoldDetail = `Aggregate quantity across sell operations: ${formatQty(summary.totalSells)}.`;
    const investedDetail = `Absolute cash paid for buys. Sum of spends = ${money(summary.totalSpent)}.`;
    const returnedDetail = `Cash received from sells/refunds. Sum of proceeds = ${money(summary.totalProceeds)}.`;
    const primaryStats = [
        createPlate(`<strong>Avg buy price:</strong> ${money(avgPurchasePrice)}`, avgPriceDetail),
        createPlate(`<strong>Current price:</strong> ${money(currentPrice)}`, currentPriceDetail),
        createPlate(`<strong>Net quantity:</strong> ${formatQty(summary.netQty)}`, netQtyDetail),
        createPlate(`<strong>Total bought:</strong> ${formatQty(summary.totalBuys)}`, totalBoughtDetail),
        createPlate(`<strong>Total sold:</strong> ${formatQty(summary.totalSells)}`, totalSoldDetail),
        createPlate(`<strong>Cash invested:</strong> ${money(summary.totalSpent)}`, investedDetail),
        createPlate(`<strong>Cash returned:</strong> ${money(summary.totalProceeds)}`, returnedDetail)
    ];
    const insightItems = buildPurchaseInsightItems(purchases, avgPurchasePrice, currentPrice, summary.totalSpent, summary.totalProceeds);
    const combinedItems = [...primaryStats, ...insightItems];
    if(!combinedItems.length){
        transactionModalMeta.innerHTML = '';
        return;
    }
    const normalizedPlates = combinedItems.map(item => {
        if(!item){
            return null;
        }
        if(typeof item === 'string'){
            return { html: item, detail: '' };
        }
        if(typeof item === 'object' && typeof item.html === 'string'){
            return {
                html: item.html,
                detail: typeof item.detail === 'string' ? item.detail : ''
            };
        }
        return { html: String(item), detail: '' };
    }).filter(Boolean);
    if(!normalizedPlates.length){
        transactionModalMeta.innerHTML = '';
        return;
    }
    const platesMarkup = normalizedPlates.map(plate => {
        const detailAttr = plate.detail ? ` data-detail="${escapeHtmlAttribute(plate.detail)}"` : '';
        return `<div class="modal-insight-plate"${detailAttr}>${plate.html}</div>`;
    }).join('');
    transactionModalMeta.innerHTML = `
        <details class="modal-insights">
            <summary>
                <span>Purchase insights</span>
                <span class="insight-hint">tap to expand</span>
            </summary>
            <div class="modal-insights-plates">
                ${platesMarkup}
            </div>
        </details>
    `;
    bindInsightPlateDetails(transactionModalMeta);
}

function buildPurchaseInsightItems(purchases, avgPrice, currentPrice, totalSpent = 0, totalProceeds = 0){
    if(!Array.isArray(purchases) || !purchases.length){
        return [];
    }
    const priceValues = purchases
        .map(point=> Number(point.price ?? point.y ?? 0))
        .filter(value => Number.isFinite(value));
    if(!priceValues.length){
        return [];
    }
    const EPS = 1e-9;
    const DAY_IN_MS = 24 * 60 * 60 * 1000;
    const toStartOfDayMs = date => {
        if(!(date instanceof Date)) return NaN;
        return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
    };
    const purchaseDates = purchases
        .map(point => point.date instanceof Date ? point.date : (point.x ? new Date(point.x) : null))
        .filter(date => date instanceof Date && !Number.isNaN(date.getTime()))
        .sort((a,b)=> a - b);
    const firstPurchase = purchaseDates[0] || null;
    const lastPurchase = purchaseDates[purchaseDates.length - 1] || null;
    const pricePoints = purchases
        .map(point=>{
            const rawDate = point.date instanceof Date ? point.date : (point.x ? new Date(point.x) : null);
            const price = Number(point.price ?? point.y ?? point.value ?? 0);
            if(!(rawDate instanceof Date) || Number.isNaN(rawDate.getTime()) || !Number.isFinite(price) || price <= EPS){
                return null;
            }
            return { date: rawDate, price };
        })
        .filter(Boolean)
        .sort((a,b)=> a.date - b.date);
    const totalPriceSum = priceValues.reduce((sum, value)=> sum + value, 0);
    const items = [];
    const addInsight = (html, detail = '')=>{
        items.push({ html, detail });
    };

    if(firstPurchase && lastPurchase){
        const inclusiveDays = Math.max(1, Math.floor(Math.abs(lastPurchase - firstPurchase) / DAY_IN_MS) + 1);
        const netCashInvested = Number(totalSpent || 0) - Number(totalProceeds || 0);
        if(netCashInvested > EPS){
            const projectedDailyBuy = netCashInvested / inclusiveDays;
            const rangeLabel = firstPurchase.getTime() === lastPurchase.getTime()
                ? formatDateShort(firstPurchase)
                : `${formatDateShort(firstPurchase)} → ${formatDateShort(lastPurchase)}`;
            const daysLabel = inclusiveDays === 1 ? 'day' : 'days';
            const projectionDetail = `Assumes ${money(netCashInvested)} split evenly (${money(projectedDailyBuy)}/day) from ${rangeLabel}.`;
            addInsight(`<strong>DCA projection:</strong> ${money(netCashInvested)} allocated across ${inclusiveDays} ${daysLabel}${rangeLabel ? ` · ${rangeLabel}` : ''}`, projectionDetail);
            const fallbackPriceForDca = pricePoints.length
                ? pricePoints[0].price
                : (Number.isFinite(currentPrice) && currentPrice > EPS ? currentPrice : (Number.isFinite(avgPrice) ? avgPrice : projectedDailyBuy));
            const startDayMs = toStartOfDayMs(firstPurchase);
            const endDayMs = toStartOfDayMs(lastPurchase);
            let cursorMs = Number.isFinite(startDayMs) ? startDayMs : NaN;
            let pointer = 0;
            let lastKnownPrice = fallbackPriceForDca > EPS ? fallbackPriceForDca : projectedDailyBuy;
            const normalizedPrices = [];
            if(Number.isFinite(cursorMs) && Number.isFinite(endDayMs)){
                while(cursorMs <= endDayMs){
                    while(pointer < pricePoints.length){
                        const candidate = toStartOfDayMs(pricePoints[pointer].date);
                        if(Number.isFinite(candidate) && candidate <= cursorMs){
                            if(pricePoints[pointer].price > EPS){
                                lastKnownPrice = pricePoints[pointer].price;
                            }
                            pointer += 1;
                        }else{
                            break;
                        }
                    }
                    normalizedPrices.push(lastKnownPrice > EPS ? lastKnownPrice : fallbackPriceForDca || projectedDailyBuy);
                    cursorMs += DAY_IN_MS;
                }
            }
            const perDayAllocation = projectedDailyBuy;
            let projectedDcaAvgPrice = fallbackPriceForDca;
            let totalUnits = 0;
            if(normalizedPrices.length){
                totalUnits = normalizedPrices.reduce((sum, price)=> price > EPS ? sum + (perDayAllocation / price) : sum, 0);
                if(totalUnits > EPS){
                    projectedDcaAvgPrice = netCashInvested / totalUnits;
                }
            }
            if(!(projectedDcaAvgPrice > EPS)){
                projectedDcaAvgPrice = perDayAllocation;
            }
            const dcaAvgDetail = normalizedPrices.length
                ? `Simulated ${inclusiveDays} equal buys created ${formatQty(totalUnits)} projected units. ${money(netCashInvested)} ÷ ${formatQty(totalUnits)} = ${money(projectedDcaAvgPrice)}.`
                : `Using fallback price ${money(fallbackPriceForDca)} to approximate per-day accumulation.`;
            addInsight(`<strong>DCA AVG:</strong> ${money(projectedDailyBuy)} per day · projected avg ${money(projectedDcaAvgPrice)}`, dcaAvgDetail);
        }
    }

    if(Number.isFinite(avgPrice)){
        const countAboveAvg = priceValues.filter(value => value > avgPrice + EPS).length;
        const countBelowAvg = priceValues.filter(value => value < avgPrice - EPS).length;
        const countNearAvg = priceValues.length - countAboveAvg - countBelowAvg;
        const avgDetail = `${priceValues.length} buys benchmarked against ${money(avgPrice)}: ${countAboveAvg} above, ${countBelowAvg} below, ${countNearAvg} near.`;
        addInsight(`<strong>Vs avg:</strong> ${countAboveAvg} buys above · ${countBelowAvg} below${countNearAvg > 0 ? ` · ${countNearAvg} near` : ''}`, avgDetail);
    }
    if(Number.isFinite(currentPrice)){
        const countAboveCurrent = priceValues.filter(value => value > currentPrice + EPS).length;
        const countBelowCurrent = priceValues.filter(value => value < currentPrice - EPS).length;
        const currentDetail = `${priceValues.length} buys compared to current ${money(currentPrice)}: ${countAboveCurrent} bought higher, ${countBelowCurrent} bought lower.`;
        addInsight(`<strong>Vs current:</strong> ${countAboveCurrent} buys above · ${countBelowCurrent} below`, currentDetail);
    }
    const medianPrice = computeMedian(priceValues);
    const priceStdDev = computeStdDeviation(priceValues);
    const minPrice = Math.min(...priceValues);
    const maxPrice = Math.max(...priceValues);
    const medianDetail = `Median of buy prices after sorting; σ is sample std dev (${money(priceStdDev)}).`;
    const spanDetail = `Range from ${money(minPrice)} to ${money(maxPrice)} equals ${money(maxPrice - minPrice)} spread.`;
    addInsight(`<strong>Median buy:</strong> ${money(medianPrice)} · σ ${money(priceStdDev)}`, medianDetail);
    addInsight(`<strong>Price span:</strong> ${money(minPrice)} – ${money(maxPrice)} (${money(maxPrice - minPrice)})`, spanDetail);
    if(purchaseDates.length >= 2){
        let totalDiff = 0;
        for(let i=1;i<purchaseDates.length;i+=1){
            totalDiff += Math.abs(purchaseDates[i] - purchaseDates[i-1]);
        }
        const avgDiff = totalDiff / (purchaseDates.length - 1);
        const approxDays = Math.max(1, Math.round(avgDiff / DAY_IN_MS));
        const spacingDetail = `${purchaseDates.length} timestamps → average gap ${formatDuration(avgDiff)} (~${approxDays} day${approxDays === 1 ? '' : 's'}).`;
        addInsight(`<strong>Avg spacing:</strong> ${formatDuration(avgDiff)}`, spacingDetail);
    }
    if(purchaseDates.length){
        const latestPurchase = purchaseDates[purchaseDates.length - 1];
        const lastBuyDetail = `Most recent buy recorded on ${latestPurchase.toLocaleString()}.`;
        addInsight(`<strong>Last buy:</strong> ${formatDateShort(latestPurchase)}`, lastBuyDetail);
    }
    if(priceValues.length >= 3){
        const sorted = priceValues.slice().sort((a,b)=> b - a);
        const topCount = Math.min(3, sorted.length);
        const topSum = sorted.slice(0, topCount).reduce((sum, value)=> sum + value, 0);
        const totalSum = totalPriceSum;
        if(totalSum !== 0){
            const share = (topSum / totalSum) * 100;
            const topDetail = `Top ${topCount} buys total ${money(topSum)} of ${money(totalSum)} spent (${share.toFixed(1)}%).`;
            addInsight(`<strong>Top buys share:</strong> ${share.toFixed(1)}% of buy capital in top ${topCount}`, topDetail);
        }
    }
    if(Number.isFinite(currentPrice)){
        const deltas = priceValues.map(value => currentPrice - value);
        const best = Math.max(...deltas);
        const worst = Math.min(...deltas);
        const deltaDetail = `Best gain: buy at ${money(currentPrice - best)} now up ${money(best)}. Worst: buy at ${money(currentPrice - worst)} trails by ${money(Math.abs(worst))}.`;
        addInsight(`<strong>Best vs current:</strong> ${money(best)} · worst ${money(worst)}`, deltaDetail);
    }
    return items;
}

function escapeHtmlAttribute(value){
    if(value === undefined || value === null){
        return '';
    }
    return String(value)
        .replace(/&/g, '&amp;')
        .replace(/"/g, '&quot;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function bindInsightPlateDetails(container){
    if(!container){
        return;
    }
    if(typeof container.__insightCleanup === 'function'){
        container.__insightCleanup();
        container.__insightCleanup = null;
    }
    const plates = Array.from(container.querySelectorAll('.modal-insight-plate[data-detail]'));
    if(!plates.length){
        return;
    }
    const closeAll = ()=>{
        plates.forEach(plate => plate.classList.remove('detail-open'));
    };
    const updatePlacementDirections = ()=>{
        if(!plates.length){
            return;
        }
        const tops = plates.map(plate => plate.offsetTop);
        const firstRowTop = tops.length ? Math.min(...tops) : 0;
        plates.forEach(plate=>{
            const isFirstRow = Math.abs(plate.offsetTop - firstRowTop) < 4;
            plate.classList.toggle('detail-position-below', isFirstRow);
            plate.classList.toggle('detail-position-above', !isFirstRow);
        });
    };
    const schedulePlacementUpdate = ()=>{
        window.requestAnimationFrame(updatePlacementDirections);
    };
    schedulePlacementUpdate();
    plates.forEach(plate=>{
        if(plate.dataset.detailPrepared === 'true'){
            return;
        }
        const detailText = plate.getAttribute('data-detail');
        if(detailText && !plate.querySelector('.modal-insight-detail')){
            const detailEl = document.createElement('div');
            detailEl.className = 'modal-insight-detail';
            detailEl.textContent = detailText;
            plate.appendChild(detailEl);
        }
        plate.setAttribute('tabindex', '0');
        plate.dataset.detailPrepared = 'true';
        plate.addEventListener('click', event=>{
            const wasOpen = plate.classList.contains('detail-open');
            closeAll();
            if(!wasOpen){
                plate.classList.add('detail-open');
            }
            schedulePlacementUpdate();
            event.stopPropagation();
        });
        plate.addEventListener('keydown', event=>{
            if(event.key === 'Enter' || event.key === ' '){
                event.preventDefault();
                plate.click();
            }
        });
    });
    const outsideHandler = event=>{
        if(!container.contains(event.target)){
            closeAll();
        }
    };
    const keyHandler = event=>{
        if(event.key === 'Escape'){
            closeAll();
        }
    };
    document.addEventListener('click', outsideHandler, true);
    document.addEventListener('keydown', keyHandler, true);
    const resizeHandler = ()=>{
        schedulePlacementUpdate();
    };
    window.addEventListener('resize', resizeHandler);
    const mutationObserver = new MutationObserver(()=>{
        schedulePlacementUpdate();
    });
    mutationObserver.observe(container, { childList: true, subtree: true, attributes: true });
    container.__insightCleanup = ()=>{
        document.removeEventListener('click', outsideHandler, true);
        document.removeEventListener('keydown', keyHandler, true);
        window.removeEventListener('resize', resizeHandler);
        mutationObserver.disconnect();
    };
}

function computeMedian(values){
    if(!Array.isArray(values) || !values.length) return 0;
    const sorted = values.slice().sort((a,b)=> a-b);
    const mid = Math.floor(sorted.length / 2);
    if(sorted.length % 2 === 0){
        return (sorted[mid - 1] + sorted[mid]) / 2;
    }
    return sorted[mid];
}

function computeStdDeviation(values){
    if(!Array.isArray(values) || values.length < 2) return 0;
    const mean = values.reduce((sum, value)=> sum + value, 0) / values.length;
    const variance = values.reduce((sum, value)=> sum + Math.pow(value - mean, 2), 0) / (values.length - 1);
    return Math.sqrt(Math.max(variance, 0));
}

function openTransactionModal(position){
    ensureTransactionModalElements();
    ensureNetWorthDetailModalElements();
    if(!transactionModal || !transactionModalCanvas) return;
    const data = buildTransactionChartData(position);
    lastTransactionData = data;
    lastTransactionPosition = position;
    const displayName = position.displayName || position.Symbol || position.Name || position.id || 'Asset';
    if(transactionModalTitle){
        transactionModalTitle.textContent = `${displayName} transactions`;
    }
    if(transactionModalOperations){
        const operationCount = getOperationCount(position);
        transactionModalOperations.textContent = formatOperationCount(operationCount);
    }

    const hasData = data.purchases.length || data.sales.length;
    transactionModalCanvas.classList.remove('hidden');
    registerFinancialControllers();
    const config = buildTransactionChartConfig(data, position, data.fallbackPriceSeries);
    if(transactionChart){
        transactionChart.data = config.data;
        transactionChart.options = config.options;
        transactionChart.update('none');
    }else{
        const existing = window.Chart && typeof window.Chart.getChart === 'function' ? window.Chart.getChart(transactionModalCanvas) : null;
        if(existing){
            existing.destroy();
        }
            const ctx = transactionModalCanvas.getContext('2d');
            transactionChart = new Chart(ctx, config);
            transactionChart.canvas.classList.remove('hidden');
    }
    if(hasData){
        renderTransactionMeta(position, data);
    }else if(transactionModalMeta){
        transactionModalMeta.innerHTML = '<div class="pos">No purchase or sale operations recorded yet.</div>';
    }
    renderTransactionLots(position, data);
    setModalView('chart');
    loadHistoricalPriceSeries(position);

    transactionModal.classList.remove('hidden');
    transactionModal.setAttribute('aria-hidden','false');
    document.body.classList.add('modal-open');
    const closeButton = transactionModal.querySelector('.modal-close');
    if(closeButton){
        closeButton.focus();
    }
}

async function loadHistoricalPriceSeries(position){
    try{
        const series = await fetchHistoricalPriceSeries(position);
        if(!series.length){
            if(transactionModalMeta){
                const existing = transactionModalMeta.querySelector('.price-history-note');
                if(!existing){
                    const note = document.createElement('div');
                    note.className = 'pos price-history-note';
                    note.textContent = 'Historical price data unavailable.';
                    transactionModalMeta.appendChild(note);
                }
            }
            return;
        }
        position.priceHistory = series;
        if(transactionModalMeta){
            const existing = transactionModalMeta.querySelector('.price-history-note');
            if(existing) existing.remove();
        }
        if(!lastTransactionData || lastTransactionPosition !== position) return;
        const config = buildTransactionChartConfig(lastTransactionData, position, series);
        if(transactionChart){
            transactionChart.destroy();
        }
        const ctx = transactionModalCanvas.getContext('2d');
        transactionChart = new Chart(ctx, config);
        transactionChart.canvas.classList.remove('hidden');
        renderPnlTrendChart(pnlRange);
    }catch(error){
        console.warn('Failed to load local historical price series', error);
    }
}

function buildAnalyticsSection(categoryKey, title, positions, options){
    const details = document.createElement('details');
    details.className = 'analytics-section';
    if(options.open) details.open = true;
    const summary = document.createElement('summary');
    summary.innerHTML = `
        <div class="summary-left">
            <span class="summary-title">${title}</span>
            <span class="summary-count">${positions.length}</span>
        </div>
        <div class="summary-extra">${options.extra || ''}</div>
    `;
    details.appendChild(summary);
    const content = document.createElement('div');
    content.className = 'analytics-section-content';

    if(!positions.length){
        const empty = document.createElement('div');
        empty.className = 'pos';
        empty.textContent = options.emptyMessage || 'No positions.';
        content.appendChild(empty);
    }else{
        positions.forEach(position=>{
            const row = options.type === 'closed'
                ? createClosedPositionRow(position)
                : createOpenPositionRow(position, options.totalCategoryValue);
            content.appendChild(row);
        });
    }

    if(categorySectionState[categoryKey] && options.sectionKey){
        details.open = Boolean(options.open);
        details.addEventListener('toggle', ()=>{
            categorySectionState[categoryKey][options.sectionKey] = details.open;
        });
    }

    details.appendChild(content);
    return details;
}

function createOpenPositionRow(position, totalCategoryValue){
    const row = document.createElement('div');
    row.className = 'analytics-row';
    const priceCandidates = [
        position.displayPrice,
        position.currentPrice,
        position.lastKnownPrice,
        position.lastPurchasePrice,
        position.avgPrice
    ].map(value=> Number(value)).filter(value=> Number.isFinite(value));
    const prioritizedPrice = priceCandidates.find(value => Math.abs(value) > 1e-9);
    const fallbackPrice = Number(position.displayPrice ?? position.currentPrice ?? position.lastKnownPrice ?? position.avgPrice ?? position.lastPurchasePrice ?? 0) || 0;
    const price = Number.isFinite(prioritizedPrice) ? prioritizedPrice : fallbackPrice;
    const marketValue = Number(position.marketValue || 0);
    const pnlValue = Number((position.rangePnl ?? position.pnl) || 0);
    const share = totalCategoryValue ? (marketValue / totalCategoryValue) * 100 : null;
    const reinvestedQty = Math.max(0, Number(position.reinvested || 0));
    const reinvestPrice = Math.abs(price) > 1e-9 ? price : fallbackPrice;
    const hasReinvestValue = reinvestedQty > 1e-6 && Math.abs(reinvestPrice) > 1e-9;
    const reinvestedValue = hasReinvestValue ? reinvestPrice * reinvestedQty : 0;
    const displayPnlValue = pnlValue + reinvestedValue;
    const shareText = Number.isFinite(share) ? `${share.toFixed(1)}%` : '—';
    const main = document.createElement('div');
    main.className = 'analytics-main';
    const iconEl = createAssetIconElement(position);
    if(iconEl){
        main.appendChild(iconEl);
    }
    const label = document.createElement('div');
    label.className = 'asset-label';
    const nameEl = document.createElement('div');
    nameEl.className = 'symbol';
    nameEl.textContent = position.displayName || position.Symbol || position.Name;
    const metaEl = document.createElement('div');
    metaEl.className = 'pos';
    const qtyText = `Qty ${formatQty(Number(position.qty || 0))}`;
    const metaParts = [
        qtyText,
        position.priceStatus
            ? `Price <span class="price-warning">${money(price)} · ${position.priceStatus}</span>`
            : `Price ${money(price)}`
    ];
    metaEl.innerHTML = metaParts.join(' · ');
    label.appendChild(nameEl);
    label.appendChild(metaEl);
    main.appendChild(label);
    row.appendChild(main);

    const values = document.createElement('div');
    values.className = 'analytics-values';
    const marketEl = document.createElement('div');
    marketEl.className = 'value-strong';
    marketEl.textContent = money(marketValue);
    const pnlEl = document.createElement('div');
    pnlEl.className = displayPnlValue >= 0 ? 'delta-positive' : 'delta-negative';
    const denominator = Number(position.rangeBaseDenominator);
    const basePercent = Number(position.rangeChangePct);
    const effectivePercent = Number.isFinite(denominator) && Math.abs(denominator) > 1e-6
        ? (displayPnlValue / denominator) * 100
        : (Number.isFinite(basePercent) ? basePercent : null);
    pnlEl.textContent = formatMoneyWithPercent(displayPnlValue, Number.isFinite(effectivePercent) ? effectivePercent : null, 1);
    if(showPnlPercentages && hasReinvestValue){
        const reinvestSpan = document.createElement('span');
        reinvestSpan.className = 'reinvested-note';
        reinvestSpan.textContent = ` · Reinvested ${money(reinvestedValue)}`;
        pnlEl.appendChild(reinvestSpan);
    }
    const shareEl = document.createElement('div');
    shareEl.className = 'muted';
    shareEl.textContent = `Category ${shareText}`;
    values.appendChild(marketEl);
    values.appendChild(pnlEl);
    values.appendChild(shareEl);
    row.appendChild(values);

    const plateImage = getAssetPlateImage(position);
    if(plateImage){
        row.style.setProperty('--plate-image', `url("${plateImage}")`);
        row.dataset.plateImage = plateImage;
    }else{
        row.style.removeProperty('--plate-image');
        delete row.dataset.plateImage;
    }

    row.classList.add('interactive-asset-row');
    row.setAttribute('role', 'button');
    row.setAttribute('tabindex', '0');
    row.addEventListener('click', event => {
        event.preventDefault();
        event.stopPropagation();
        lastTransactionTrigger = row;
        openTransactionModal(position);
    });
    row.addEventListener('keydown', event => {
        if(event.key === 'Enter' || event.key === ' '){
            event.preventDefault();
            lastTransactionTrigger = row;
            openTransactionModal(position);
        }
    });
    return row;
}

function createClosedPositionRow(position){
    const row = document.createElement('div');
    row.className = 'analytics-row';
    const realized = Number(position.realized || 0);
    const priceCandidates = [
        position.displayPrice,
        position.currentPrice,
        position.lastKnownPrice,
        position.lastPurchasePrice,
        position.avgPrice
    ].map(value=> Number(value)).filter(value=> Number.isFinite(value));
    const prioritizedPrice = priceCandidates.find(value => Math.abs(value) > 1e-9);
    const fallbackPrice = Number(position.displayPrice ?? position.currentPrice ?? position.lastKnownPrice ?? position.lastPurchasePrice ?? position.avgPrice ?? 0) || 0;
    const price = Number.isFinite(prioritizedPrice) ? prioritizedPrice : fallbackPrice;
    const displayRealized = realized;
    const main = document.createElement('div');
    main.className = 'analytics-main';
    const iconEl = createAssetIconElement(position);
    if(iconEl){
        main.appendChild(iconEl);
    }
    const label = document.createElement('div');
    label.className = 'asset-label';
    const nameEl = document.createElement('div');
    nameEl.className = 'symbol';
    nameEl.textContent = position.displayName || position.Symbol || position.Name;
    const metaEl = document.createElement('div');
    metaEl.className = 'pos';
    const realizedPercent = Number(position.rangeChangePct);
    const metaParts = [
        `Realized P&L ${formatMoneyWithPercent(realized, Number.isFinite(realizedPercent) ? realizedPercent : null, 1)}`
    ];
    metaEl.innerHTML = metaParts.join(' · ');
    label.appendChild(nameEl);
    label.appendChild(metaEl);
    main.appendChild(label);
    row.appendChild(main);

    const values = document.createElement('div');
    values.className = 'analytics-values';
    const pnlEl = document.createElement('div');
    pnlEl.className = displayRealized >= 0 ? 'delta-positive' : 'delta-negative';
    const denominator = Number(position.rangeBaseDenominator);
    const basePercent = Number(position.rangeChangePct);
    const effectivePercent = Number.isFinite(denominator) && Math.abs(denominator) > 1e-6
        ? (displayRealized / denominator) * 100
        : (Number.isFinite(realizedPercent) ? realizedPercent : null);
    pnlEl.textContent = formatMoneyWithPercent(displayRealized, Number.isFinite(effectivePercent) ? effectivePercent : null, 1);
    const statusEl = document.createElement('div');
    statusEl.className = 'muted';
    const stillOpen = Math.abs(Number(position.qty || 0)) > 1e-6;
    statusEl.textContent = stillOpen ? 'Position partially closed' : 'Position closed';
    values.appendChild(pnlEl);
    values.appendChild(statusEl);
    row.appendChild(values);

    const plateImage = getAssetPlateImage(position);
    if(plateImage){
        row.style.setProperty('--plate-image', `url("${plateImage}")`);
        row.dataset.plateImage = plateImage;
    }else{
        row.style.removeProperty('--plate-image');
        delete row.dataset.plateImage;
    }

    row.classList.add('interactive-asset-row');
    row.setAttribute('role', 'button');
    row.setAttribute('tabindex', '0');
    row.addEventListener('click', event => {
        event.preventDefault();
        event.stopPropagation();
        lastTransactionTrigger = row;
        openTransactionModal(position);
    });
    row.addEventListener('keydown', event => {
        if(event.key === 'Enter' || event.key === ' '){
            event.preventDefault();
            lastTransactionTrigger = row;
            openTransactionModal(position);
        }
    });
    return row;
}

function applyAssetViewMode(){
    const lists = [
        document.getElementById('crypto-positions'),
        document.getElementById('stock-positions')
    ].filter(Boolean);
    lists.forEach(list => {
        list.classList.toggle('plates', assetViewMode === 'plates');
    });
    if(assetViewToggleButton){
        const isPlates = assetViewMode === 'plates';
        const labelText = isPlates ? 'Switch to rows view' : 'Switch to plates view';
        assetViewToggleButton.setAttribute('aria-label', labelText);
        assetViewToggleButton.setAttribute('title', labelText);
        const srLabel = assetViewToggleButton.querySelector('#asset-view-toggle-label');
        if(srLabel){
            srLabel.textContent = labelText;
        }
        assetViewToggleButton.classList.toggle('active', isPlates);
        assetViewToggleButton.classList.toggle('mode-plates', isPlates);
        assetViewToggleButton.classList.toggle('mode-rows', !isPlates);
    }
}

function setCategoryChartTab(categoryKey, tabKey, options = {}){
    const normalized = String(categoryKey || '').toLowerCase();
    const config = CATEGORY_CONFIG[normalized];
    if(!config) return;
    const tabs = Array.isArray(config.chartTabs) && config.chartTabs.length ? config.chartTabs : [];
    if(!tabs.length) return;
    const effectiveKey = tabs.some(tab => tab.key === tabKey) ? tabKey : (tabs[0]?.key || tabKey);
    if(!effectiveKey) return;
    categoryChartTabState[normalized] = effectiveKey;
    const panel = document.querySelector(`.chart-tab-panel[data-category="${normalized}"]`);
    if(panel){
        panel.querySelectorAll('.chart-tab').forEach(button=>{
            button.classList.toggle('active', button.dataset.chartKey === effectiveKey);
        });
        panel.querySelectorAll('canvas').forEach(canvas=>{
            canvas.classList.toggle('active', canvas.dataset.chartKey === effectiveKey);
        });
    }
    if(!options.silent){
        const activeTab = tabs.find(tab => tab.key === effectiveKey);
        if(activeTab){
            const chart = charts[activeTab.chartId];
            if(chart){
                chart.resize();
                chart.update('none');
            }
        }
    }
}

function initializeChartTabs(){
    document.querySelectorAll('.chart-tab-panel').forEach(panel=>{
        const category = panel.dataset.category;
        if(!category) return;
        panel.querySelectorAll('.chart-tab').forEach(button=>{
            button.addEventListener('click', ()=>{
                setCategoryChartTab(category, button.dataset.chartKey);
            });
        });
        const config = CATEGORY_CONFIG[category];
        const defaultKey = categoryChartTabState[category] || (config?.chartTabs?.[0]?.key);
        if(defaultKey){
            setCategoryChartTab(category, defaultKey, { silent: true });
        }
    });
}

function setAssetViewMode(mode){
    const nextMode = mode === 'plates' ? 'plates' : 'rows';
    if(assetViewMode === nextMode) return;
    assetViewMode = nextMode;
    try{
        if(typeof window !== 'undefined' && window.localStorage){
            window.localStorage.setItem(VIEW_MODE_STORAGE_KEY, assetViewMode);
        }
    }catch(error){
        console.warn('Failed to persist asset view mode', error);
    }
    applyAssetViewMode();
}

function getNetWorthKey(position){
    const baseType = position.type || position.Category || 'Other';
    const normalized = normalizeCategory(baseType, position.displayName || position.Name || position.Symbol || '');
    const sanitized = String(normalized || 'Other').toLowerCase().replace(/[^a-z]/g, '');
    if(NET_WORTH_LABEL_MAP[sanitized]) return sanitized;
    return 'other';
}

function computeNetWorthTimeline(totalValue){
    const MS_PER_DAY = 24 * 60 * 60 * 1000;
    const operations = positions.flatMap(position => Array.isArray(position.operations) ? position.operations : []);
    const datedOps = operations.filter(op => op && op.date instanceof Date).map(op => {
        let spent = Number(op.spent);
        if(!Number.isFinite(spent)){
            const amount = Number(op.amount || 0);
            const price = Number(op.price || 0);
            if(Number.isFinite(amount) && Number.isFinite(price)){
                spent = amount * price;
            }else{
                spent = 0;
            }
        }
        return { date: new Date(op.date), spent };
    }).filter(entry => Number.isFinite(entry.spent) && entry.spent !== 0).sort((a,b)=> a.date - b.date);

    const dayBuckets = new Map();
    datedOps.forEach(({date, spent})=>{
        const bucketDate = new Date(date.getFullYear(), date.getMonth(), date.getDate());
        const key = bucketDate.getTime();
        dayBuckets.set(key, (dayBuckets.get(key) || 0) + spent);
    });

    const sortedKeys = Array.from(dayBuckets.keys()).sort((a,b)=>a-b);
    let cumulative = 0;
    const actualRaw = [];
    sortedKeys.forEach(ts=>{
        cumulative += dayBuckets.get(ts);
        const safeValue = Number.isFinite(cumulative) ? Math.max(0, cumulative) : 0;
        actualRaw.push({ date: new Date(ts), value: safeValue });
    });

    const now = new Date();
    if(!actualRaw.length){
        actualRaw.push({ date: new Date(now.getTime() - 90 * MS_PER_DAY), value: 0 });
    }
    actualRaw.push({ date: now, value: Number.isFinite(totalValue) ? Math.max(0, totalValue) : 0 });
    actualRaw.sort((a,b)=> a.date - b.date);

    const actual = actualRaw.map(entry => ({
        x: entry.date instanceof Date ? entry.date : new Date(entry.date),
        y: Number.isFinite(entry.value) ? Math.max(0, entry.value) : 0
    }));

    if(actual.length < 2){
        const anchorDate = new Date(actual[0].x.getTime() - 90 * MS_PER_DAY);
        actual.unshift({ x: anchorDate, y: actual[0].y });
    }

    const firstDate = actual[0].x instanceof Date ? actual[0].x : new Date(actual[0].x);
    const startOfFirstYear = new Date(firstDate.getFullYear(), 0, 1);
    const lastActual = actual[actual.length - 1];
    const baseProjectedValue = actual.reduce((max, point)=> Math.max(max, point.y), lastActual.y);
    const projectedShort = [{ x: lastActual.x, y: lastActual.y }];
    let projectedValue = Math.max(lastActual.y, baseProjectedValue);
    for(let offset = 1; offset <= 2; offset += 1){
        projectedValue = projectedValue * 1.1;
        projectedShort.push({
            x: new Date(now.getFullYear() + offset, 0, 1),
            y: Number.isFinite(projectedValue) && projectedValue > 0 ? projectedValue : 0
        });
    }

    const projectedExtended = [...projectedShort];
    const MAX_PROJECTION_YEARS = 40;
    let yearOffset = projectedShort.length - 1;
    let extendedValue = projectedShort[projectedShort.length - 1].y;
    while(projectedExtended[projectedExtended.length - 1].y < MILLION_TARGET && yearOffset < MAX_PROJECTION_YEARS){
        yearOffset += 1;
        extendedValue = extendedValue * 1.1;
        projectedExtended.push({
            x: new Date(now.getFullYear() + yearOffset, 0, 1),
            y: Number.isFinite(extendedValue) && extendedValue > 0 ? extendedValue : 0
        });
    }

    const lastProjectedPoint = projectedShort[projectedShort.length - 1];

    return {
        actual,
        projected: projectedShort,
        projectedExtended,
        domain: {
            min: startOfFirstYear,
            max: lastProjectedPoint?.x || projectedShort[projectedShort.length - 1].x
        }
    };
}

function generateSmoothSeries(points, options = {}){
    const stepDays = Math.max(1, Number.isFinite(options.stepDays) ? options.stepDays : 7);
    const smoothing = Math.max(0, Math.min(0.9, Number.isFinite(options.smoothing) ? options.smoothing : 0.25));
    const stepMs = stepDays * 24 * 60 * 60 * 1000;
    const sorted = Array.isArray(points) ? points.map(point => {
        const sourceDate = point?.x ?? point?.date;
        const date = sourceDate instanceof Date ? new Date(sourceDate) : new Date(sourceDate || Date.now());
        const value = Number(point?.y ?? point?.value ?? 0) || 0;
        return { x: date, y: value < 0 ? 0 : value };
    }).filter(entry => entry.x instanceof Date && !Number.isNaN(entry.x.getTime())) : [];
    sorted.sort((a, b) => a.x - b.x);
    if(!sorted.length){
        return [];
    }
    const result = [];
    let previous = sorted[0].y;
    result.push({ x: sorted[0].x, y: previous });
    for(let i = 0; i < sorted.length - 1; i += 1){
        const start = sorted[i];
        const end = sorted[i + 1];
        const span = end.x.getTime() - start.x.getTime();
        if(span <= 0){
            previous = end.y;
            result.push({ x: end.x, y: previous });
            continue;
        }
        const steps = Math.max(1, Math.round(span / stepMs));
        for(let step = 1; step <= steps; step += 1){
            const ratio = step / steps;
            const moment = new Date(start.x.getTime() + ratio * span);
            const linearValue = start.y + (end.y - start.y) * ratio;
            previous = previous + smoothing * (linearValue - previous);
            result.push({ x: moment, y: previous });
        }
    }
    const last = sorted[sorted.length - 1];
    result[result.length - 1] = { x: last.x, y: last.y };
    return result;
}

function renderNetWorthSparkline(timeline){
    const canvas = document.getElementById('networth-sparkline');
    if(!canvas){
        if(netWorthSparklineChart){
            netWorthSparklineChart.destroy();
            netWorthSparklineChart = null;
        }
        return;
    }
    netWorthSparklineCanvas = canvas;
    if(!canvas.dataset.networthDetailBound){
        canvas.style.cursor = 'pointer';
        canvas.setAttribute('role', 'button');
        canvas.setAttribute('tabindex', '0');
        canvas.setAttribute('aria-label', 'Open detailed net worth chart');
        canvas.addEventListener('click', openNetWorthDetailModal);
        canvas.addEventListener('keydown', event => {
            if(event.key === 'Enter' || event.key === ' '){
                event.preventDefault();
                openNetWorthDetailModal();
            }
        });
        canvas.dataset.networthDetailBound = 'true';
    }
    const actualRaw = Array.isArray(timeline?.actual) ? timeline.actual : [];
    const projectedRaw = Array.isArray(timeline?.projected) ? timeline.projected : [];

    if(!actualRaw.length){
        if(netWorthSparklineChart){
            netWorthSparklineChart.destroy();
            netWorthSparklineChart = null;
        }
        const ctx = canvas.getContext('2d');
        if(ctx){
            ctx.clearRect(0, 0, canvas.width, canvas.height);
        }
        return;
    }

    const actualSeries = generateSmoothSeries(actualRaw, {
        stepDays: SPARKLINE_ACTUAL_STEP_DAYS,
        smoothing: SPARKLINE_ACTUAL_SMOOTHING
    });

    const projectedSeries = projectedRaw.length ? generateSmoothSeries(
        [actualRaw[actualRaw.length - 1], ...projectedRaw],
        {
            stepDays: SPARKLINE_PROJECTED_STEP_DAYS,
            smoothing: SPARKLINE_PROJECTED_SMOOTHING
        }
    ) : [];

    if(projectedSeries.length){
        const lastActual = actualSeries[actualSeries.length - 1];
        projectedSeries[0] = { x: lastActual.x, y: lastActual.y };
    }

    const combinedData = [
        ...actualSeries.map(point => ({ ...point, projected: false })),
        ...projectedSeries.slice(1).map(point => ({ ...point, projected: true }))
    ];

    const axisMin = timeline?.domain?.min || new Date(actualSeries[0].x.getFullYear(), 0, 1);
    const axisMax = timeline?.domain?.max || (projectedSeries.length ? projectedSeries[projectedSeries.length - 1].x : actualSeries[actualSeries.length - 1].x);

    const dataset = {
        type: 'line',
        data: combinedData,
        parsing: false,
        spanGaps: true,
        borderWidth: 2,
        tension: 0.62,
        borderCapStyle: 'round',
        borderJoinStyle: 'round',
        pointRadius: 0,
        pointHoverRadius: 0,
        pointHitRadius: 18,
        fill: 'origin',
        segment: {
            borderColor(context){
                return context.p1?.raw?.projected ? 'rgba(129, 140, 248, 0.9)' : 'rgba(56, 189, 248, 0.92)';
            },
            backgroundColor(context){
                return context.p1?.raw?.projected ? 'rgba(129, 140, 248, 0.06)' : 'rgba(56, 189, 248, 0.12)';
            },
            borderDash(context){
                return context.p1?.raw?.projected ? [6, 4] : undefined;
            }
        }
    };

    const options = {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 420, easing: 'easeOutCubic' },
        plugins: {
            legend: { display: false },
            tooltip: {
                enabled: true,
                intersect: false,
                mode: 'nearest',
                displayColors: false,
                position: 'nearest',
                callbacks: {
                    title(items){
                        const item = items && items[0];
                        if(!item) return '';
                        const rawDate = item.raw?.x;
                        const date = rawDate instanceof Date ? rawDate : new Date(rawDate);
                        return date ? formatDateShort(date) : '';
                    },
                    label(item){
                        const raw = item.raw || {};
                        const value = Number(raw.y ?? item.parsed?.y ?? 0);
                        const label = raw.projected ? 'Projected net worth' : 'Net worth';
                        return `${label}: ${formatCompactMoney(value)}`;
                    }
                }
            }
        },
        scales: {
            x: {
                type: 'time',
                min: axisMin,
                max: axisMax,
                offset: false,
                grid: {
                    display: true,
                    color: 'rgba(148, 163, 184, 0.12)',
                    borderDash: [2, 6],
                    drawTicks: false
                },
                ticks: {
                    display: true,
                    autoSkip: false,
                    maxRotation: 0,
                    align: 'start',
                    font: { size: 10, family: 'Inter, system-ui' },
                    callback(value){
                        const date = new Date(value);
                        return Number.isFinite(date.getFullYear()) ? date.getFullYear().toString() : '';
                    }
                },
                time: {
                    unit: 'year',
                    round: 'year',
                    displayFormats: { year: 'yyyy' }
                }
            },
            y: {
                display: false,
                beginAtZero: false
            }
        },
        interaction: {
            intersect: false,
            mode: 'nearest',
            axis: 'x'
        },
        layout: {
            padding: { top: 16, bottom: 6, left: 6, right: 6 }
        }
    };
    ensurePanSupport(options, 'x');

    if(netWorthSparklineChart){
        netWorthSparklineChart.data.datasets = [dataset];
        netWorthSparklineChart.options = options;
        netWorthSparklineChart.update('none');
        if(netWorthDetailModal && !netWorthDetailModal.classList.contains('hidden')){
            renderNetWorthDetailChart(timeline);
        }
        return;
    }

    if(typeof Chart === 'undefined'){
        return;
    }

    const ctx = canvas.getContext('2d');
    netWorthSparklineChart = new Chart(ctx, {
        type: 'line',
        data: { datasets: [dataset] },
        options,
        plugins: [sparklineCrosshairPlugin]
    });
    if(netWorthDetailModal && !netWorthDetailModal.classList.contains('hidden')){
        renderNetWorthDetailChart(timeline);
    }
}

function flattenTimelinePoints(timeline, options = {}){
    const includeProjected = options.includeProjected !== false;
    const map = new Map();
    const addPoint = point => {
        if(!point) return;
        const rawDate = point.x ?? point.date;
        const date = rawDate instanceof Date ? new Date(rawDate) : new Date(rawDate || Date.now());
        if(!(date instanceof Date) || Number.isNaN(date.getTime())){
            return;
        }
        const valueRaw = point.y ?? point.value ?? 0;
        const value = Number.isFinite(Number(valueRaw)) ? Number(valueRaw) : 0;
        map.set(date.getTime(), { x: date, y: value });
    };
    if(Array.isArray(timeline?.actual)){
        timeline.actual.forEach(addPoint);
    }
    if(includeProjected && Array.isArray(timeline?.projected)){
        timeline.projected.forEach(addPoint);
    }
    if(includeProjected && Array.isArray(timeline?.projectedExtended)){
        timeline.projectedExtended.forEach(addPoint);
    }
    return Array.from(map.values()).sort((a,b)=> a.x - b.x);
}

function computeMillionTargetDate(timeline){
    const points = flattenTimelinePoints(timeline, { includeProjected: true });
    if(!points.length){
        return null;
    }
    for(let i = 0; i < points.length; i += 1){
        const current = points[i];
        if(current.y >= MILLION_TARGET){
            if(i === 0){
                return current.x;
            }
            const previous = points[i - 1];
            if(!previous || previous.y >= MILLION_TARGET){
                return current.x;
            }
            const span = current.x.getTime() - previous.x.getTime();
            if(span <= 0 || current.y === previous.y){
                return current.x;
            }
            const ratio = (MILLION_TARGET - previous.y) / (current.y - previous.y);
            const clamped = Math.max(0, Math.min(1, ratio));
            const interpolated = new Date(previous.x.getTime() + clamped * span);
            return interpolated;
        }
    }
    return null;
}

function updateMillionTargetNote(timeline){
    const noteEl = document.getElementById('networth-million-note');
    if(!noteEl){
        return;
    }
    const targetDate = computeMillionTargetDate(timeline);
    if(targetDate){
        noteEl.textContent = `1M target: ${formatDateShort(targetDate)}`;
        noteEl.classList.remove('unavailable');
    }else{
        noteEl.textContent = '1M target: —';
        noteEl.classList.add('unavailable');
    }
}

function createMindmapNode(options = {}){
    const size = Math.max(60, Math.min(220, Number(options.size) || 80));
    const wrapper = document.createElement('div');
    wrapper.className = 'mindmap-node';
    if(options.className){
        wrapper.classList.add(options.className);
    }
    wrapper.style.width = `${size}px`;
    wrapper.style.height = `${size}px`;
    const inner = document.createElement('div');
    inner.className = 'mindmap-node-inner';
    const titleEl = document.createElement('div');
    titleEl.className = 'mindmap-node-title';
    titleEl.textContent = options.label || '';
    inner.appendChild(titleEl);
    if(options.valueText){
        const valueEl = document.createElement('div');
        valueEl.className = 'mindmap-node-value';
        valueEl.textContent = options.valueText;
        inner.appendChild(valueEl);
    }
    if(options.detailText){
        titleEl.dataset.fullLabel = options.detailText;
    }
    if(options.styles){
        const { background, borderColor, boxShadow, textColor, titleColor } = options.styles;
        if(background) inner.style.background = background;
        if(borderColor) inner.style.borderColor = borderColor;
        if(boxShadow) inner.style.boxShadow = boxShadow;
        if(textColor) inner.style.color = textColor;
        if(titleColor) titleEl.style.color = titleColor;
    }
    const duration = options.animationDuration || 16 + Math.random() * 6;
    const delay = options.animationDelay || Math.random() * -10;
    inner.style.animationDuration = `${duration.toFixed(2)}s`;
    inner.style.animationDelay = `${delay.toFixed(2)}s`;
    wrapper.appendChild(inner);
    return { node: wrapper, inner, size };
}

function getMindmapLabel(key, fallbackLabel){
    const normalizedKey = String(key || '').toLowerCase();
    if(MINDMAP_LABEL_OVERRIDES[normalizedKey]){
        return MINDMAP_LABEL_OVERRIDES[normalizedKey];
    }
    const label = fallbackLabel || key || '';
    if(label.length <= 12){
        return label;
    }
    const words = label.split(/\s+/).filter(Boolean);
    if(words.length > 1){
        const acronym = words.map(word => word[0].toUpperCase()).join('');
        if(acronym.length >= 2 && acronym.length <= 5){
            return acronym;
        }
        const trimmedWords = words.slice(0, 2).map(word => word.slice(0, 3));
        const candidate = trimmedWords.join(' ');
        if(candidate.length <= 12){
            return candidate;
        }
    }
    return `${label.slice(0, 9)}…`;
}

function renderNetWorthMindmap(categoryMap = {}, totalValue = 0, attempt = 0){
    const container = document.getElementById('networth-mindmap');
    const linksLayer = document.getElementById('networth-mindmap-links');
    const nodesLayer = document.getElementById('networth-mindmap-nodes');
    if(!container || !linksLayer || !nodesLayer){
        return false;
    }
    const rect = container.getBoundingClientRect();
    const width = rect.width || container.clientWidth || container.offsetWidth || 0;
    const height = rect.height || container.clientHeight || container.offsetHeight || 0;
    if((width <= 0 || height <= 0) && attempt < 3){
        requestAnimationFrame(()=> renderNetWorthMindmap(categoryMap, totalValue, attempt + 1));
        return null;
    }
    if(width <= 0 || height <= 0){
        return false;
    }

    const filteredEntries = Object.entries(categoryMap || {})
        .filter(([, value])=> Math.abs(Number(value)) > 1e-2)
        .sort(([a],[b])=> a.localeCompare(b));

    const hash = JSON.stringify({
        total: Number(totalValue) || 0,
        entries: filteredEntries.map(([key, value])=> [key, Number(value) || 0])
    });
    if(
        attempt === 0 &&
        hash === lastMindmapRenderHash &&
        width === lastMindmapDimensions.width &&
        height === lastMindmapDimensions.height
    ){
        return true;
    }

    nodesLayer.innerHTML = '';
    linksLayer.innerHTML = '';

    linksLayer.setAttribute('viewBox', `0 0 ${width} ${height}`);
    linksLayer.setAttribute('width', width);
    linksLayer.setAttribute('height', height);
    linksLayer.setAttribute('preserveAspectRatio', 'xMidYMid meet');

    const centerX = width / 2;
    const centerY = height / 2;
    const total = Number(totalValue) || 0;
    const magnitudeSum = filteredEntries.reduce((sum, [, rawValue])=> sum + Math.abs(Number(rawValue) || 0), 0);

    const nodesData = filteredEntries.map(([key, rawValue])=>{
        const numericValue = Number(rawValue) || 0;
        const magnitude = Math.abs(numericValue);
        const percent = magnitudeSum > 0 ? (magnitude / magnitudeSum) * 100 : 0;
        const baseSize = Math.max(68, Math.min(200, 68 + percent * 1.6));
        const size = Math.max(58, Math.min(180, Math.round(baseSize * 0.85)));
        const labelKey = key || 'other';
        const fullLabel = NET_WORTH_LABEL_MAP[labelKey] || labelKey.replace(/([a-z])([A-Z])/g, '$1 $2').replace(/^./, letter => letter.toUpperCase());
        const shortLabel = getMindmapLabel(labelKey, fullLabel);
        return {
            key: labelKey,
            label: shortLabel,
            fullLabel,
            value: numericValue,
            size,
            percent
        };
    });

    const bubbleArea = nodesData.reduce((sum, node)=> sum + (node.size * node.size), 0);
    const allowedBubbleArea = Math.max(1, width * height * 0.35);
    const bubbleAreaScale = bubbleArea > allowedBubbleArea ? Math.sqrt(allowedBubbleArea / bubbleArea) : 1;
    if(bubbleAreaScale < 1){
        nodesData.forEach(node=>{
            node.size = Math.max(48, Math.round(node.size * bubbleAreaScale));
        });
    }

    const mainDetail = total > 0 ? 'Total value' : 'Awaiting data';
    const mainTitle = total > 0 ? `Total value: ${money(total)}` : 'Awaiting data';
    const mainDisplayValue = total === 0 ? '$0' : formatCompactMoney(total);
    const minDimension = Math.max(120, Math.min(width, height));
    const sizeLimit = Math.max(110, Math.min(minDimension - 24, 220));
    const baseMainSize = Math.min(sizeLimit, Math.max(110, Math.min(minDimension * 0.6, 200)));
    const mainSize = Math.max(94, Math.min(sizeLimit, Math.round(baseMainSize * 0.85)));
    const minSpacing = Math.max(18, Math.min(34, minDimension * 0.08));
    const boundaryPadding = Math.max(16, Math.min(32, minDimension * 0.08));

    const calculateRadius = ()=>{
        const currentMaxDiameter = nodesData.reduce((max, node)=> Math.max(max, node.size), 0);
        const availableRadius = Math.max(110, Math.min(width, height) / 2 - 30);
        const perimeterNeed = nodesData.reduce((sum, node)=> sum + node.size + minSpacing, 0);
        const radiusFromPerimeter = perimeterNeed > 0 ? perimeterNeed / (Math.PI * 2) : 0;
        const minRequiredRadius = (mainSize * 0.5) + (currentMaxDiameter * 0.5) + minSpacing * 2;
        const rawRadius = Math.max(minRequiredRadius, radiusFromPerimeter);
        return Math.min(availableRadius, Math.max(rawRadius, availableRadius * 0.55));
    };
    let dynamicRadius = calculateRadius();
    const refreshRadius = ()=>{
        dynamicRadius = calculateRadius();
    };

    const mainNode = createMindmapNode({
        label: 'Net Worth',
        valueText: mainDisplayValue,
        detailText: mainDetail,
        size: mainSize,
        className: 'mindmap-node-main',
        animationDuration: 20,
        animationDelay: -6,
        styles: MINDMAP_MAIN_STYLE
    });
    mainNode.node.style.left = `${centerX}px`;
    mainNode.node.style.top = `${centerY}px`;
    mainNode.node.title = mainTitle;
    mainNode.node.setAttribute('aria-hidden', 'true');
    nodesLayer.appendChild(mainNode.node);

    if(!nodesData.length){
        lastMindmapRenderHash = hash;
        lastMindmapDimensions = { width, height };
        return true;
    }

    const twoPi = Math.PI * 2;
    const anchorBubble = { x: centerX, y: centerY, half: mainSize / 2 + minSpacing };
    const buildBasePlacements = ()=>{
        return nodesData.map((node, index)=>{
            const fraction = nodesData.length ? (index / nodesData.length) : 0;
            const angle = twoPi * fraction - Math.PI / 2;
            const jitterDirection = index % 2 === 0 ? 1 : -1;
            return { node, angle, jitterDirection };
        });
    };
    let basePlacements = buildBasePlacements();
    const refreshBasePlacements = ()=>{
        basePlacements = buildBasePlacements();
    };

    const createPlacementSet = (multiplier = 1)=>{
        const radialBoost = dynamicRadius * multiplier;
        return basePlacements.map(base=>{
            const jitter = base.jitterDirection * base.node.size * 0.12;
            const offset = Math.max(0, radialBoost + jitter);
            return {
                node: base.node,
                x: centerX + Math.cos(base.angle) * offset,
                y: centerY + Math.sin(base.angle) * offset,
                half: base.node.size / 2
            };
        });
    };

    const clampBubble = bubble=>{
        const prevX = bubble.x;
        const prevY = bubble.y;
        const minX = bubble.half + boundaryPadding;
        const maxX = Math.max(minX, width - bubble.half - boundaryPadding);
        const minY = bubble.half + boundaryPadding;
        const maxY = Math.max(minY, height - bubble.half - boundaryPadding);
        bubble.x = Math.max(minX, Math.min(maxX, bubble.x));
        bubble.y = Math.max(minY, Math.min(maxY, bubble.y));
        return prevX !== bubble.x || prevY !== bubble.y;
    };

    const separateBubbles = (a, b, lockSecond = false)=>{
        const dx = a.x - b.x;
        const dy = a.y - b.y;
        const distance = Math.sqrt(dx * dx + dy * dy);
        const minDistance = a.half + b.half + minSpacing;
        if(distance >= minDistance){
            return false;
        }
        const overlap = Math.max(minDistance - (distance || 0), minSpacing);
        const angle = distance === 0 ? Math.random() * twoPi : Math.atan2(dy, dx);
        const pushX = Math.cos(angle) * overlap;
        const pushY = Math.sin(angle) * overlap;
        if(lockSecond){
            a.x += pushX;
            a.y += pushY;
        }else{
            a.x += pushX / 2;
            a.y += pushY / 2;
            b.x -= pushX / 2;
            b.y -= pushY / 2;
        }
        return true;
    };

    const resolveMindmapCollisions = bubbles=>{
        const iterations = 120;
        for(let iter = 0; iter < iterations; iter+=1){
            let moved = false;
            for(let i = 0; i < bubbles.length; i+=1){
                for(let j = i + 1; j < bubbles.length; j+=1){
                    moved = separateBubbles(bubbles[i], bubbles[j]) || moved;
                }
            }
            for(const bubble of bubbles){
                moved = separateBubbles(bubble, anchorBubble, true) || moved;
                moved = clampBubble(bubble) || moved;
            }
            if(!moved){
                break;
            }
        }
    };

    const hasOverlap = bubbles=>{
        for(let i = 0; i < bubbles.length; i+=1){
            for(let j = i + 1; j < bubbles.length; j+=1){
                const dx = bubbles[i].x - bubbles[j].x;
                const dy = bubbles[i].y - bubbles[j].y;
                const distance = Math.sqrt(dx * dx + dy * dy);
                const minDistance = bubbles[i].half + bubbles[j].half + (minSpacing - 2);
                if(distance < minDistance){
                    return true;
                }
            }
        }
        return false;
    };

    const computePlacements = ()=>{
        if(!basePlacements.length){
            return [];
        }
        let attempt = 0;
        let placements = createPlacementSet(1);
        const maxAttempts = 5;
        while(attempt < maxAttempts){
            resolveMindmapCollisions(placements);
            if(!hasOverlap(placements)){
                break;
            }
            attempt += 1;
            const multiplier = 1 + attempt * 0.2;
            placements = createPlacementSet(multiplier);
        }
        return placements;
    };

    const isOutOfBounds = placements => placements.some(p=>
        (p.x - p.half < boundaryPadding) ||
        (p.x + p.half > width - boundaryPadding) ||
        (p.y - p.half < boundaryPadding) ||
        (p.y + p.half > height - boundaryPadding)
    );

    const getPlacementsWithFit = ()=>{
        let attempts = 0;
        let placements = [];
        while(attempts < 5){
            refreshRadius();
            placements = computePlacements();
            if(!placements || !placements.length){
                break;
            }
            if(!isOutOfBounds(placements)){
                break;
            }
            nodesData.forEach(node=>{
                node.size = Math.max(48, Math.round(node.size * 0.9));
            });
            refreshBasePlacements();
            attempts += 1;
        }
        return placements;
    };

    const finalPlacements = getPlacementsWithFit();
    if(!finalPlacements || !finalPlacements.length){
        lastMindmapRenderHash = hash;
        lastMindmapDimensions = { width, height };
        return true;
    }

    const clampPlacementsToBounds = placements=>{
        placements.forEach(placement=>{
            placement.x = Math.min(width - placement.half - boundaryPadding, Math.max(boundaryPadding + placement.half, placement.x));
            placement.y = Math.min(height - placement.half - boundaryPadding, Math.max(boundaryPadding + placement.half, placement.y));
        });
    };
    clampPlacementsToBounds(finalPlacements);

    finalPlacements.forEach((placement, index)=>{
        const node = placement.node;
        const percentLabel = node.percent > 0 ? `${node.percent.toFixed(1)}%` : '';
        const valueWithPercent = percentLabel
            ? `${formatCompactMoney(node.value)} (${percentLabel})`
            : formatCompactMoney(node.value);
        const detailText = node.fullLabel && node.fullLabel !== node.label ? node.fullLabel : '';
        const colorTheme = MINDMAP_COLOR_PALETTE[index % MINDMAP_COLOR_PALETTE.length];
        const bubble = createMindmapNode({
            label: node.label,
            valueText: valueWithPercent,
            detailText,
            size: node.size,
            styles: colorTheme
        });
        bubble.node.style.left = `${placement.x}px`;
        bubble.node.style.top = `${placement.y}px`;
        const titlePercent = percentLabel ? ` (${percentLabel})` : '';
        bubble.node.title = `${node.fullLabel}: ${money(node.value)}${titlePercent}`;
        bubble.node.setAttribute('aria-hidden', 'true');
        bubble.inner.style.animationDuration = `${(14 + Math.random() * 6).toFixed(2)}s`;
        bubble.inner.style.animationDelay = `${(Math.random() * -12).toFixed(2)}s`;
        nodesLayer.appendChild(bubble.node);

        const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
        line.setAttribute('x1', centerX);
        line.setAttribute('y1', centerY);
        line.setAttribute('x2', placement.x);
        line.setAttribute('y2', placement.y);
        line.setAttribute('stroke-width', '2');
        line.setAttribute('stroke-dasharray', '6 6');
        line.setAttribute('stroke-linecap', 'round');
        if(colorTheme && colorTheme.lineColor){
            line.setAttribute('stroke', colorTheme.lineColor);
        }else{
            line.setAttribute('stroke', 'rgba(56, 189, 248, 0.55)');
        }
        linksLayer.appendChild(line);
    });

    lastMindmapRenderHash = hash;
    lastMindmapDimensions = { width, height };
    return true;
}

function applyNetWorthInlineViewMode(){
    const card = document.getElementById('net-worth-card');
    if(!card){
        return;
    }
    const isBubble = netWorthInlineViewMode === 'bubble';
    card.classList.toggle('bubble-mode', isBubble);

    if(netWorthBubbleToggleButton){
        const labelText = isBubble ? 'Show chart view' : 'Show bubbles';
        netWorthBubbleToggleButton.classList.toggle('active', isBubble);
        netWorthBubbleToggleButton.setAttribute('aria-expanded', isBubble ? 'true' : 'false');
        netWorthBubbleToggleButton.setAttribute('aria-pressed', isBubble ? 'true' : 'false');
        const srLabel = netWorthBubbleToggleButton.querySelector('.sr-only');
        if(srLabel){
            srLabel.textContent = labelText;
        }else{
            netWorthBubbleToggleButton.textContent = labelText;
        }
        netWorthBubbleToggleButton.setAttribute('aria-label', labelText);
        netWorthBubbleToggleButton.setAttribute('title', labelText);
    }

    const bubbleContainer = document.getElementById('networth-bubble-container');
    if(bubbleContainer){
        bubbleContainer.classList.toggle('hidden', !isBubble);
        bubbleContainer.setAttribute('aria-hidden', isBubble ? 'false' : 'true');
    }
    const noteEl = document.getElementById('networth-million-note');
    if(noteEl){
        noteEl.setAttribute('aria-hidden', isBubble ? 'true' : 'false');
    }

    if(isBubble){
        netWorthBubbleSnapshot = { ...(lastNetWorthTotals || {}) };
        netWorthBubbleSnapshotTotal = lastNetWorthTotalValue;
        netWorthBubbleNeedsRender = true;
        lastMindmapRenderHash = null;
        const attemptRender = ()=>{
            const sourceTotals = netWorthBubbleSnapshot || lastNetWorthTotals;
            const sourceTotalValue = netWorthBubbleSnapshot ? netWorthBubbleSnapshotTotal : lastNetWorthTotalValue;
            const result = renderNetWorthMindmap(sourceTotals, sourceTotalValue);
            if(result === null){
                requestAnimationFrame(attemptRender);
            }else if(result){
                netWorthBubbleNeedsRender = false;
            }
        };
        attemptRender();
    }else{
        netWorthBubbleNeedsRender = false;
        netWorthBubbleSnapshot = null;
        netWorthBubbleSnapshotTotal = 0;
    }
}

function setNetWorthInlineViewMode(mode){
    const next = mode === 'bubble' ? 'bubble' : 'chart';
    if(netWorthInlineViewMode === next){
        return;
    }
    netWorthInlineViewMode = next;
    lastMindmapRenderHash = null;
    if(next === 'chart'){
        lastMindmapDimensions = { width: 0, height: 0 };
    }
    try{
        if(typeof window !== 'undefined' && window.localStorage){
            window.localStorage.setItem(NET_WORTH_INLINE_VIEW_STORAGE_KEY, netWorthInlineViewMode);
        }
    }catch(error){
        console.warn('Failed to persist net worth view preference', error);
    }
    applyNetWorthInlineViewMode();
}

function ensureNetWorthDetailModalElements(){
    if(netWorthDetailModal && netWorthDetailCanvas){
        return;
    }
    netWorthDetailModal = document.getElementById('networth-detail-modal');
    if(!netWorthDetailModal){
        return;
    }
    netWorthDetailCanvas = document.getElementById('networth-detail-chart');
    netWorthDetailSubtitle = document.getElementById('networth-detail-subtitle');
    netWorthDetailMeta = document.getElementById('networth-detail-meta');
    const closeButton = netWorthDetailModal.querySelector('.modal-close');
    if(closeButton && !closeButton.dataset.networthBound){
        closeButton.addEventListener('click', closeNetWorthDetailModal);
        closeButton.dataset.networthBound = 'true';
    }
    if(!netWorthDetailModal.dataset.networthBound){
        netWorthDetailModal.addEventListener('click', event => {
            if(event.target === netWorthDetailModal){
                closeNetWorthDetailModal();
            }
        });
        netWorthDetailModal.dataset.networthBound = 'true';
    }
}

function renderNetWorthDetailChart(timeline){
    ensureNetWorthDetailModalElements();
    if(!netWorthDetailCanvas){
        return;
    }
    const usableTimeline = timeline || lastNetWorthTimeline;
    if(!usableTimeline){
        return;
    }
    if(typeof Chart === 'undefined'){
        return;
    }
    const actualSeries = generateSmoothSeries(usableTimeline.actual || [], {
        stepDays: 5,
        smoothing: 0.2
    });
    if(!actualSeries.length){
        if(netWorthDetailChart){
            netWorthDetailChart.destroy();
            netWorthDetailChart = null;
        }
        return;
    }
    let projectedSeries = [];
    const projectedSource = Array.isArray(usableTimeline.projectedExtended) && usableTimeline.projectedExtended.length > 1
        ? usableTimeline.projectedExtended
        : usableTimeline.projected;
    if(Array.isArray(projectedSource) && projectedSource.length > 1){
        const stitched = [
            usableTimeline.actual?.[usableTimeline.actual.length - 1],
            ...projectedSource.slice(1)
        ].filter(Boolean);
        projectedSeries = generateSmoothSeries(stitched, {
            stepDays: 10,
            smoothing: 0.26
        });
    }
    const datasets = [{
        label: 'Net worth (actual)',
        data: actualSeries,
        borderColor: 'rgba(56, 189, 248, 0.95)',
        backgroundColor: 'rgba(56, 189, 248, 0.16)',
        borderWidth: 2.4,
        tension: 0.55,
        fill: 'origin',
        pointRadius: 0,
        pointHoverRadius: 3,
        pointHitRadius: 12,
        spanGaps: true
    }];
    if(projectedSeries.length){
        datasets.push({
            label: 'Net worth (projected)',
            data: projectedSeries,
            borderColor: 'rgba(129, 140, 248, 0.95)',
            backgroundColor: 'rgba(129, 140, 248, 0.08)',
            borderDash: [6, 4],
            borderWidth: 2.1,
            tension: 0.6,
            fill: false,
            pointRadius: 0,
            pointHoverRadius: 3,
            pointHitRadius: 12,
            spanGaps: true
        });
    }
    const firstPoint = actualSeries[0];
    const lastActualPoint = actualSeries[actualSeries.length - 1];
    const endPoint = (projectedSeries.length ? projectedSeries[projectedSeries.length - 1] : lastActualPoint);
    const targetDate = computeMillionTargetDate(usableTimeline);
    if(netWorthDetailSubtitle){
        netWorthDetailSubtitle.textContent = `${formatDateShort(firstPoint.x)} – ${formatDateShort(endPoint.x)}`;
    }
    if(netWorthDetailMeta){
        const targetText = targetDate ? formatDateShort(targetDate) : '—';
        netWorthDetailMeta.innerHTML = `
            <div>Current net worth: <strong>${money(lastActualPoint.y)}</strong></div>
            <div>Projection horizon: <strong>${money(endPoint.y)}</strong></div>
            <div>1M target: <strong>${targetText}</strong></div>
        `;
    }
    const isLightTheme = document.body.classList.contains('light-theme');
    const gridColor = isLightTheme ? 'rgba(148, 163, 184, 0.28)' : 'rgba(148, 163, 184, 0.18)';
    const tickColor = isLightTheme ? 'rgba(30, 41, 59, 0.78)' : 'rgba(226, 232, 240, 0.82)';
    const legendColor = isLightTheme ? 'rgba(30, 41, 59, 0.85)' : 'rgba(226, 232, 240, 0.88)';
    const tooltipBg = isLightTheme ? 'rgba(255, 255, 255, 0.95)' : 'rgba(15, 23, 42, 0.95)';
    const tooltipText = isLightTheme ? '#0f172a' : '#f8fafc';
    const options = {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: 'nearest', intersect: false },
        plugins: {
            legend: {
                display: true,
                labels: {
                    color: legendColor,
                    usePointStyle: true,
                    padding: 12
                }
            },
            tooltip: {
                enabled: true,
                backgroundColor: tooltipBg,
                titleColor: tooltipText,
                bodyColor: tooltipText,
                borderColor: isLightTheme ? 'rgba(148, 163, 184, 0.45)' : 'rgba(51, 65, 85, 0.45)',
                borderWidth: 1,
                callbacks: {
                    title(items){
                        const item = items && items[0];
                        if(!item) return '';
                        const rawDate = item.raw?.x;
                        const date = rawDate instanceof Date ? rawDate : new Date(rawDate);
                        return date ? formatDateShort(date) : '';
                    },
                    label(item){
                        const value = item.raw?.y ?? item.parsed?.y ?? 0;
                        return `${item.dataset?.label || 'Value'}: ${money(value)}`;
                    }
                }
            }
        },
        scales: {
            x: {
                type: 'time',
                grid: {
                    color: gridColor,
                    borderDash: [2, 6],
                    drawTicks: false
                },
                ticks: {
                    color: tickColor,
                    autoSkip: true,
                    maxRotation: 0,
                    major: { enabled: false }
                },
                time: {
                    unit: 'month',
                    displayFormats: { month: 'MMM yyyy' }
                }
            },
            y: {
                grid: {
                    color: gridColor,
                    borderDash: [4, 6]
                },
                ticks: {
                    color: tickColor,
                    callback: value => money(value)
                }
            }
        },
        layout: {
            padding: { top: 8, right: 12, bottom: 8, left: 8 }
        }
    };
    ensurePanSupport(options, 'x');
    const data = { datasets };
    if(netWorthDetailChart){
        netWorthDetailChart.data = data;
        netWorthDetailChart.options = options;
        netWorthDetailChart.update('none');
    }else{
        const ctx = netWorthDetailCanvas.getContext('2d');
        netWorthDetailChart = new Chart(ctx, {
            type: 'line',
            data,
            options
        });
    }
}

function openNetWorthDetailModal(){
    ensureNetWorthDetailModalElements();
    if(!netWorthDetailModal){
        return;
    }
    if(netWorthDetailModal && !netWorthDetailModal.classList.contains('hidden')){
        renderNetWorthDetailChart(lastNetWorthTimeline);
        return;
    }
    const fallbackTotal = positions.reduce((sum, position)=> sum + Number(position.marketValue || 0), 0);
    const timeline = lastNetWorthTimeline || computeNetWorthTimeline(fallbackTotal);
    if(!lastNetWorthTimeline && timeline){
        lastNetWorthTimeline = timeline;
    }
    renderNetWorthDetailChart(timeline);
    netWorthDetailModal.classList.remove('hidden');
    netWorthDetailModal.setAttribute('aria-hidden', 'false');
    document.body.classList.add('modal-open');
    const closeButton = netWorthDetailModal.querySelector('.modal-close');
    if(closeButton && typeof closeButton.focus === 'function'){
        closeButton.focus({ preventScroll: true });
    }
}

function closeNetWorthDetailModal(){
    if(!netWorthDetailModal){
        return;
    }
    netWorthDetailModal.classList.add('hidden');
    netWorthDetailModal.setAttribute('aria-hidden', 'true');
    if(netWorthSparklineCanvas && typeof netWorthSparklineCanvas.focus === 'function'){
        netWorthSparklineCanvas.focus({ preventScroll: false });
    }
    if(!transactionModal || transactionModal.classList.contains('hidden')){
        document.body.classList.remove('modal-open');
    }
}

function extractEtContext(date){
    const numericFormatter = new Intl.DateTimeFormat('en-US', {
        timeZone: MARKET_TIME_ZONE,
        hour12: false,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
    });
    const offsetFormatter = new Intl.DateTimeFormat('en-US', {
        timeZone: MARKET_TIME_ZONE,
        timeZoneName: 'shortOffset'
    });
    const weekdayFormatter = new Intl.DateTimeFormat('en-US', {
        timeZone: MARKET_TIME_ZONE,
        weekday: 'short'
    });
    const parts = numericFormatter.formatToParts(date);
    const partValue = type => Number(parts.find(part => part.type === type)?.value || 0);
    const year = partValue('year');
    const month = partValue('month');
    const day = partValue('day');
    const hour = partValue('hour');
    const minute = partValue('minute');
    const second = partValue('second');
    const offsetParts = offsetFormatter.formatToParts(date);
    const offsetString = offsetParts.find(part => part.type === 'timeZoneName')?.value || '';
    const offsetMatch = /GMT([+-])(\d{1,2})(?::(\d{2}))?/i.exec(offsetString);
    let offsetMinutes = 240;
    if(offsetMatch){
        const sign = offsetMatch[1] === '-' ? -1 : 1;
        const hours = Number(offsetMatch[2] || 0);
        const minutes = Number(offsetMatch[3] || 0);
        const gmtOffset = sign * (hours * 60 + minutes);
        offsetMinutes = -gmtOffset;
    }
    const weekdayString = weekdayFormatter.format(date).toLowerCase();
    const weekdayIndex = WEEKDAY_NAME_TO_INDEX[weekdayString.slice(0,3)] ?? 0;
    return { year, month, day, hour, minute, second, offsetMinutes, weekdayIndex };
}

function createEtInstant(context, minutes){
    const hour = Math.floor(minutes / 60);
    const minute = minutes % 60;
    const utcMs = Date.UTC(context.year, context.month - 1, context.day, hour, minute, 0) + context.offsetMinutes * 60 * 1000;
    return new Date(utcMs);
}

function formatDuration(ms){
    if(!Number.isFinite(ms)) return '';
    const totalMinutes = Math.max(0, Math.round(ms / 60000));
    const hours = Math.floor(totalMinutes / 60);
    const minutes = totalMinutes % 60;
    if(hours <= 0 && minutes <= 0) return '0m';
    if(hours <= 0) return `${minutes}m`;
    if(minutes === 0) return `${hours}h`;
    return `${hours}h ${minutes}m`;
}

function computeMarketStatusInfo(reference = new Date()){
    const context = extractEtContext(reference);
    const minutes = context.hour * 60 + context.minute;
    const openInstant = createEtInstant(context, MARKET_OPEN_MINUTES);
    const closeInstant = createEtInstant(context, MARKET_CLOSE_MINUTES);
    const isWeekday = context.weekdayIndex >= 1 && context.weekdayIndex <= 5;
    const isOpen = isWeekday && minutes >= MARKET_OPEN_MINUTES && minutes < MARKET_CLOSE_MINUTES;
    let nextOpenInstant = null;
    if(isOpen){
        nextOpenInstant = closeInstant;
    }else if(isWeekday && minutes < MARKET_OPEN_MINUTES){
        nextOpenInstant = openInstant;
    }else{
        let searchDate = new Date(reference.getTime());
        for(let i = 0; i < 7; i += 1){
            searchDate = new Date(searchDate.getTime() + 24 * 60 * 60 * 1000);
            const candidateContext = extractEtContext(searchDate);
            const weekdayCandidate = candidateContext.weekdayIndex;
            if(weekdayCandidate >= 1 && weekdayCandidate <= 5){
                nextOpenInstant = createEtInstant(candidateContext, MARKET_OPEN_MINUTES);
                break;
            }
        }
    }
    return { isOpen, openInstant, closeInstant, nextOpenInstant };
}

function updateMarketStatus(){
    const statusEl = document.getElementById('market-status');
    if(!statusEl) return;
    const textEl = document.getElementById('market-status-text');
    const usEl = document.getElementById('market-hours-us');
    const localEl = document.getElementById('market-hours-local');
    const nextEl = document.getElementById('market-next-open');
    const now = new Date();
    const info = computeMarketStatusInfo(now);
    statusEl.classList.toggle('closed', !info.isOpen);
    if(textEl){
        textEl.textContent = info.isOpen ? 'Market open' : 'Market closed';
    }
    const usFormatter = new Intl.DateTimeFormat('en-US', {
        timeZone: MARKET_TIME_ZONE,
        hour: 'numeric',
        minute: '2-digit'
    });
    const localFormatter = new Intl.DateTimeFormat(undefined, {
        hour: 'numeric',
        minute: '2-digit'
    });
    const descriptorFormatter = new Intl.DateTimeFormat(undefined, {
        weekday: 'short',
        hour: 'numeric',
        minute: '2-digit'
    });
    if(usEl){
        usEl.textContent = `US hours: ${usFormatter.format(info.openInstant)} – ${usFormatter.format(info.closeInstant)} ET`;
    }
    if(localEl){
        localEl.textContent = `Local: ${localFormatter.format(info.openInstant)} – ${localFormatter.format(info.closeInstant)}`;
    }
    if(nextEl){
        if(info.isOpen){
            const remaining = info.closeInstant.getTime() - now.getTime();
            nextEl.textContent = `Closes ${descriptorFormatter.format(info.closeInstant)} (${formatDuration(remaining)})`;
        }else if(info.nextOpenInstant){
            const untilOpen = info.nextOpenInstant.getTime() - now.getTime();
            nextEl.textContent = `Opens ${descriptorFormatter.format(info.nextOpenInstant)} (${formatDuration(untilOpen)})`;
        }else{
            nextEl.textContent = '—';
        }
    }
}

function initMarketStatusWatcher(){
    updateMarketStatus();
    if(marketStatusTimer){
        clearInterval(marketStatusTimer);
    }
    marketStatusTimer = setInterval(updateMarketStatus, MARKET_STATUS_INTERVAL);
    if(typeof document !== 'undefined' && !marketStatusVisibilityBound){
        document.addEventListener('visibilitychange', ()=>{
            if(document.visibilityState === 'visible'){
                updateMarketStatus();
            }
        });
        marketStatusVisibilityBound = true;
    }
}

function setCategoryMetric(categoryKey, metricKey, value, elementId, formatter){
    const el = document.getElementById(elementId);
    if(!el) return;
    let displayValue;
    if(value === null || value === undefined || Number.isNaN(value)){
        displayValue = formatter ? formatter(null) : '—';
    }else{
        displayValue = formatter ? formatter(value) : value;
    }
    el.textContent = displayValue;
    const store = previousCategorySummaries[categoryKey];
    if(!store) return;
    const previous = store[metricKey];
    if(previous !== null && value !== null && value !== undefined && !Number.isNaN(value) && previous !== value){
        flashElement(el, value > previous ? 'up' : 'down');
    }
    store[metricKey] = (value === null || value === undefined || Number.isNaN(value)) ? null : value;
}

function applyDeltaClass(element, value){
    if(!element) return;
    element.classList.remove('delta-positive','delta-negative');
    if(!Number.isFinite(value)) return;
    if(value > 0){
        element.classList.add('delta-positive');
    }else if(value < 0){
        element.classList.add('delta-negative');
    }
}

function ensureBestInfoPopoverElements(){
    if(typeof document === 'undefined') return null;
    if(bestInfoPopoverElements){
        return bestInfoPopoverElements;
    }
    bestInfoPopover = document.getElementById('best-info-popover');
    if(!bestInfoPopover){
        return null;
    }
    const content = bestInfoPopover.querySelector('.best-info-content');
    if(content && !content.hasAttribute('tabindex')){
        content.setAttribute('tabindex', '-1');
    }
    bestInfoPopoverElements = {
        popover: bestInfoPopover,
        content,
        title: document.getElementById('best-info-title'),
        range: document.getElementById('best-info-range'),
        category: document.getElementById('best-info-category'),
        name: document.getElementById('best-info-name'),
        pnl: document.getElementById('best-info-pnl'),
        change: document.getElementById('best-info-change'),
        meta: document.getElementById('best-info-meta'),
        extra: document.getElementById('best-info-extra')
    };
    return bestInfoPopoverElements;
}

function positionBestInfoPopover(trigger){
    const elements = ensureBestInfoPopoverElements();
    if(!elements || !elements.popover || !trigger) return;
    const popover = elements.popover;
    const previousVisibility = popover.style.visibility;
    popover.style.visibility = 'hidden';
    popover.classList.remove('hidden');
    const triggerRect = trigger.getBoundingClientRect();
    const popRect = popover.getBoundingClientRect();
    const viewportWidth = document.documentElement.clientWidth || window.innerWidth || 0;
    const viewportHeight = document.documentElement.clientHeight || window.innerHeight || 0;
    const padding = 16;
    let top = triggerRect.bottom + window.scrollY + 12;
    let left = triggerRect.right + window.scrollX - popRect.width;
    if(left + popRect.width > window.scrollX + viewportWidth - padding){
        left = window.scrollX + viewportWidth - popRect.width - padding;
    }
    if(left < window.scrollX + padding){
        left = window.scrollX + padding;
    }
    const maxTop = window.scrollY + viewportHeight - popRect.height - padding;
    if(top > maxTop){
        top = Math.max(triggerRect.top + window.scrollY - popRect.height - 12, window.scrollY + padding);
    }
    popover.style.left = `${Math.max(window.scrollX + padding, left)}px`;
    popover.style.top = `${Math.max(window.scrollY + padding, top)}px`;
    popover.style.visibility = previousVisibility || '';
}

function renderBestInfoPopover(entry, categoryKey){
    const elements = ensureBestInfoPopoverElements();
    if(!elements || !elements.popover) return;
    const categoryLabel = entry?.label || getCategoryDisplayName(categoryKey);
    if(elements.category){
        elements.category.textContent = categoryLabel;
    }
    if(elements.range){
        elements.range.textContent = entry?.rangeLabel || (RANGE_LABELS[pnlRange] || pnlRange);
    }
    elements.popover.dataset.category = categoryKey || '';
    elements.popover.setAttribute('data-has-data', entry ? 'true' : 'false');
    if(!entry){
        if(elements.name) elements.name.textContent = 'No data available';
        if(elements.pnl) elements.pnl.textContent = '—';
        if(elements.change) elements.change.textContent = '—';
        applyDeltaClass(elements.pnl, null);
        applyDeltaClass(elements.change, null);
        if(elements.meta){
            elements.meta.textContent = 'No holdings available for this asset group in the selected range.';
        }
        if(elements.extra){
            elements.extra.innerHTML = '';
            const empty = document.createElement('div');
            empty.className = 'best-info-extra-empty';
            empty.textContent = 'No additional metrics for this range.';
            elements.extra.appendChild(empty);
        }
        return;
    }
    if(elements.name){
        elements.name.textContent = entry.displayName || '—';
    }
    if(elements.pnl){
        elements.pnl.textContent = money(entry.pnl);
        applyDeltaClass(elements.pnl, entry.pnl);
    }
    if(elements.change){
        if(Number.isFinite(entry.changePct)){
            elements.change.textContent = `${entry.changePct >= 0 ? '+' : ''}${entry.changePct.toFixed(2)}%`;
        }else{
            elements.change.textContent = '—';
        }
        applyDeltaClass(elements.change, Number.isFinite(entry.changePct) ? entry.changePct : null);
    }
    if(elements.meta){
        elements.meta.textContent = entry.meta || '—';
    }
    if(elements.extra){
        elements.extra.innerHTML = '';
        const rows = [];
        if(entry.rangeStart instanceof Date && !Number.isNaN(entry.rangeStart.getTime()) && pnlRange !== 'ALL'){
            rows.push({
                label: 'Window start',
                value: entry.rangeStart.toLocaleDateString(undefined,{ year:'numeric', month:'short', day:'numeric' })
            });
        }
        if(Array.isArray(entry.extraItems)){
            entry.extraItems.forEach(item=>{
                if(item && item.label){
                    rows.push({ label: item.label, value: item.value });
                }
            });
        }
        if(rows.length){
            rows.forEach(item=>{
                const row = document.createElement('div');
                row.className = 'best-info-extra-row';
                const labelEl = document.createElement('span');
                labelEl.className = 'label';
                labelEl.textContent = item.label;
                const valueEl = document.createElement('span');
                valueEl.className = 'value';
                valueEl.textContent = item.value;
                row.appendChild(labelEl);
                row.appendChild(valueEl);
                elements.extra.appendChild(row);
            });
        }else{
            const empty = document.createElement('div');
            empty.className = 'best-info-extra-empty';
            empty.textContent = 'No additional metrics for this range.';
            elements.extra.appendChild(empty);
        }
    }
}

function handleBestInfoOutsideClick(event){
    const elements = bestInfoPopoverElements;
    if(!elements || !elements.popover || elements.popover.classList.contains('hidden')) return;
    if(elements.popover.contains(event.target)) return;
    if(activeBestInfoTrigger && activeBestInfoTrigger.contains(event.target)){
        return;
    }
    hideBestInfoPopover();
}

function handleBestInfoWindowChange(){
    if(!activeBestInfoTrigger) return;
    const elements = bestInfoPopoverElements;
    if(!elements || !elements.popover || elements.popover.classList.contains('hidden')) return;
    positionBestInfoPopover(activeBestInfoTrigger);
}

function attachBestInfoListeners(){
    if(bestInfoListenersAttached) return;
    document.addEventListener('pointerdown', handleBestInfoOutsideClick, true);
    window.addEventListener('scroll', handleBestInfoWindowChange, true);
    window.addEventListener('resize', handleBestInfoWindowChange, true);
    bestInfoListenersAttached = true;
}

function detachBestInfoListeners(){
    if(!bestInfoListenersAttached) return;
    document.removeEventListener('pointerdown', handleBestInfoOutsideClick, true);
    window.removeEventListener('scroll', handleBestInfoWindowChange, true);
    window.removeEventListener('resize', handleBestInfoWindowChange, true);
    bestInfoListenersAttached = false;
}

function showBestInfoPopover(categoryKey, trigger){
    const elements = ensureBestInfoPopoverElements();
    if(!elements || !elements.popover || !trigger) return;
    activeBestInfoCategory = categoryKey;
    activeBestInfoTrigger = trigger;
    renderBestInfoPopover(bestPerformerCache[categoryKey] || null, categoryKey);
    elements.popover.classList.remove('hidden');
    elements.popover.setAttribute('aria-hidden','false');
    trigger.setAttribute('aria-expanded','true');
    positionBestInfoPopover(trigger);
    attachBestInfoListeners();
    if(elements.content){
        elements.content.focus({ preventScroll: true });
    }
}

function hideBestInfoPopover(){
    const elements = ensureBestInfoPopoverElements();
    if(!elements || !elements.popover || elements.popover.classList.contains('hidden')) return;
    elements.popover.classList.add('hidden');
    elements.popover.setAttribute('aria-hidden','true');
    detachBestInfoListeners();
    if(activeBestInfoTrigger){
        activeBestInfoTrigger.setAttribute('aria-expanded','false');
        if(typeof activeBestInfoTrigger.focus === 'function'){
            activeBestInfoTrigger.focus({ preventScroll: true });
        }
    }
    activeBestInfoCategory = null;
    activeBestInfoTrigger = null;
}

function initializeBestInfoTriggers(){
    if(typeof document === 'undefined') return;
    const buttons = document.querySelectorAll('[data-best-info-trigger]');
    if(!buttons.length) return;
    buttons.forEach(button=>{
        button.setAttribute('aria-controls', 'best-info-popover');
        button.addEventListener('click', event=>{
            event.preventDefault();
            const categoryKey = button.dataset.bestInfoTrigger;
            if(!categoryKey) return;
            const elements = bestInfoPopoverElements;
            const isActive = activeBestInfoCategory === categoryKey
                && elements
                && elements.popover
                && !elements.popover.classList.contains('hidden');
            if(isActive){
                hideBestInfoPopover();
                return;
            }
            showBestInfoPopover(categoryKey, button);
        });
    });
    ensureBestInfoPopoverElements();
}

function refreshActiveBestInfoPopover(){
    if(!activeBestInfoCategory) return;
    const elements = ensureBestInfoPopoverElements();
    if(!elements || !elements.popover || elements.popover.classList.contains('hidden')) return;
    renderBestInfoPopover(bestPerformerCache[activeBestInfoCategory] || null, activeBestInfoCategory);
    if(activeBestInfoTrigger){
        positionBestInfoPopover(activeBestInfoTrigger);
    }
}

function buildPnlAssetState(position, rangeStartTs){
    const priceHistory = getPriceHistoryForPosition(position);
    if(!Array.isArray(priceHistory) || !priceHistory.length){
        return null;
    }
    const priceSeries = priceHistory.slice().map(point=>({
        time: Number(point.time ?? point.x),
        price: Number(point.price ?? point.y ?? point.value ?? point.c)
    })).filter(point=> Number.isFinite(point.time) && Number.isFinite(point.price));
    if(!priceSeries.length){
        return null;
    }
    priceSeries.sort((a, b)=> a.time - b.time);
    const earliestTime = priceSeries[0].time;
    const latestTime = priceSeries[priceSeries.length - 1].time;
    let startTs = Number.isFinite(rangeStartTs) ? Math.max(rangeStartTs, earliestTime) : earliestTime;
    if(startTs > latestTime){
        return null;
    }
    const operationsRaw = Array.isArray(position.operations) ? position.operations : [];
    const operations = operationsRaw.map(op=>{
        const date = getOperationDate(op);
        if(!date) return null;
        return {
            type: String(op.type || '').toLowerCase(),
            date,
            amount: Number(op.amount || 0),
            spent: Number(op.spent || 0)
        };
    }).filter(Boolean).sort((a, b)=> a.date - b.date);
    let qty = 0;
    let baselineSpent = 0;
    const quantityOps = [];
    const contributionOps = [];
    operations.forEach(op=>{
        const ts = op.date.getTime();
        const hasAmount = Number.isFinite(op.amount) && Math.abs(op.amount) > 1e-9;
        const hasSpent = Number.isFinite(op.spent) && Math.abs(op.spent) > 1e-6;
        if(ts <= startTs){
            if(op.type === 'purchasesell' && hasAmount){
                qty += op.amount;
            }
            if(hasSpent){
                baselineSpent += op.spent;
            }
        }else{
            if(op.type === 'purchasesell' && hasAmount){
                quantityOps.push({ date: op.date, amount: op.amount });
            }
            if(hasSpent){
                contributionOps.push({ date: op.date, spent: op.spent });
            }
        }
    });
    let priceIndex = 0;
    let currentPrice = null;
    while(priceIndex < priceSeries.length && priceSeries[priceIndex].time <= startTs){
        currentPrice = priceSeries[priceIndex].price;
        priceIndex++;
    }
    if(currentPrice === null){
        currentPrice = priceSeries[0].price;
        startTs = priceSeries[0].time;
        priceIndex = priceSeries.length > 1 ? 1 : priceSeries.length;
    }
    quantityOps.sort((a, b)=> a.date - b.date);
    contributionOps.sort((a, b)=> a.date - b.date);
    return {
        position,
        priceSeries,
        priceIndex,
        currentPrice,
        qty,
        quantityOps,
        quantityIndex: 0,
        contributionOps,
        baselineSpent,
        startTs
    };
}

function computePnlTrend(range){
    if(!Array.isArray(positions) || !positions.length){
        return { points: [], hasData: false };
    }
    const rangeStart = getRangeStartDate(range);
    const rangeStartTs = rangeStart instanceof Date && !Number.isNaN(rangeStart.getTime())
        ? rangeStart.getTime()
        : null;
    const assetStates = [];
    let baselineSpentTotal = 0;
    const timelineSet = new Set();
    let globalStartTs = null;
    positions.forEach(position=>{
        const state = buildPnlAssetState(position, rangeStartTs);
        if(!state) return;
        assetStates.push(state);
        baselineSpentTotal += state.baselineSpent;
        if(globalStartTs === null || state.startTs < globalStartTs){
            globalStartTs = state.startTs;
        }
        timelineSet.add(state.startTs);
        state.priceSeries.forEach(point=>{
            if(point.time >= state.startTs){
                timelineSet.add(point.time);
            }
        });
        state.quantityOps.forEach(event=> timelineSet.add(event.date.getTime()));
        state.contributionOps.forEach(event=> timelineSet.add(event.date.getTime()));
    });
    if(!assetStates.length){
        return { points: [], hasData: false };
    }
    const nowTs = Date.now();
    timelineSet.add(nowTs);
    if(globalStartTs !== null){
        timelineSet.add(globalStartTs);
    }
    const sortedTimeline = Array.from(timelineSet).sort((a, b)=> a - b).filter(ts => globalStartTs === null ? true : ts >= globalStartTs);
    const contributions = [];
    assetStates.forEach(state=>{
        state.contributionOps.forEach(event=>{
            contributions.push({ ts: event.date.getTime(), spent: event.spent });
        });
    });
    contributions.sort((a, b)=> a.ts - b.ts);
    let contributionIndex = 0;
    let cumulativeSpent = baselineSpentTotal;
    const points = [];
    sortedTimeline.forEach(ts=>{
        while(contributionIndex < contributions.length && contributions[contributionIndex].ts <= ts){
            cumulativeSpent += contributions[contributionIndex].spent;
            contributionIndex++;
        }
        let totalValue = 0;
        assetStates.forEach(state=>{
            while(state.priceIndex < state.priceSeries.length && state.priceSeries[state.priceIndex].time <= ts){
                state.currentPrice = state.priceSeries[state.priceIndex].price;
                state.priceIndex++;
            }
            while(state.quantityIndex < state.quantityOps.length && state.quantityOps[state.quantityIndex].date.getTime() <= ts){
                state.qty += state.quantityOps[state.quantityIndex].amount;
                state.quantityIndex++;
            }
            const price = Number(state.currentPrice);
            if(!Number.isFinite(price)) return;
            const qty = Number(state.qty || 0);
            totalValue += qty * price;
        });
        points.push({ x: new Date(ts), y: totalValue - cumulativeSpent });
    });
    if(!points.length){
        return { points: [], hasData: false };
    }
    const baseline = Number(points[0].y) || 0;
    const normalized = points.map(point=> ({
        x: point.x,
        y: Number(point.y) - baseline
    }));
    return {
        points: normalized,
        hasData: normalized.length > 1 || normalized.some(point => Math.abs(point.y) > 1e-6)
    };
}

function renderPnlTrendChart(range){
    if(typeof document === 'undefined') return;
    const canvas = document.getElementById('pnl-trend-chart');
    if(!canvas){
        if(pnlTrendChart){
            pnlTrendChart.destroy();
            pnlTrendChart = null;
        }
        return;
    }
    const { points, hasData } = computePnlTrend(range);
    if(!points || !points.length){
        if(pnlTrendChart){
            pnlTrendChart.destroy();
            pnlTrendChart = null;
        }
        const ctx = canvas.getContext('2d');
        if(ctx){
            ctx.clearRect(0, 0, canvas.width, canvas.height);
        }
        return;
    }
    const timeConfig = PNL_CHART_TIME_CONFIG[range] || PNL_CHART_TIME_CONFIG.ALL;
    const values = points.map(point=> Number(point.y || 0));
    const minValue = Math.min(...values);
    const maxValue = Math.max(...values);
    let padding = (maxValue - minValue) * 0.15;
    if(!Number.isFinite(padding) || padding <= 0){
        padding = Math.max(5, Math.abs(maxValue || 0) * 0.15 + 5);
    }
    const suggestedMin = minValue === 0 && maxValue === 0 ? -10 : minValue - padding;
    const suggestedMax = minValue === 0 && maxValue === 0 ? 10 : maxValue + padding;
    const dataset = {
        type: 'line',
        data: points.map(point=> ({ x: point.x, y: Number(point.y) })),
        parsing: false,
        tension: 0.35,
        borderWidth: 2,
        pointRadius: 0,
        pointHoverRadius: 4,
        pointHitRadius: 14,
        segment: {
            borderColor(ctx){
                const current = ctx.p1?.parsed?.y ?? 0;
                const previous = ctx.p0?.parsed?.y ?? 0;
                return current >= previous ? 'rgba(56, 189, 248, 0.85)' : 'rgba(248, 113, 113, 0.75)';
            }
        },
        borderColor: 'rgba(56, 189, 248, 0.85)',
        fill: {
            target: 'origin',
            above: 'rgba(56, 189, 248, 0.15)',
            below: 'rgba(248, 113, 113, 0.18)'
        }
    };
    const options = {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 420, easing: 'easeOutCubic' },
        plugins: {
            legend: { display: false },
            tooltip: {
                mode: 'index',
                intersect: false,
                displayColors: false,
                callbacks: {
                    title(items){
                        const item = items && items[0];
                        if(!item) return '';
                        const rawDate = item.raw?.x ?? item.parsed?.x;
                        const date = rawDate instanceof Date ? rawDate : new Date(rawDate);
                        if(!(date instanceof Date) || Number.isNaN(date.getTime())) return '';
                        if(range === '1D'){
                            const time = date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
                            return `${formatDateShort(date)} ${time}`;
                        }
                        return formatDateShort(date);
                    },
                    label(item){
                        const value = Number(item.parsed?.y || 0);
                        return `P&L ${money(value)}`;
                    }
                }
            }
        },
        scales: {
            x: {
                type: 'time',
                time: {
                    unit: timeConfig.unit,
                    displayFormats: timeConfig.displayFormats
                },
                grid: {
                    display: true,
                    color: 'rgba(148, 163, 184, 0.18)',
                    borderDash: [4, 6]
                },
                ticks: {
                    color: 'rgba(226, 232, 240, 0.65)',
                    maxTicksLimit: timeConfig.maxTicks || 6,
                    autoSkip: true,
                    maxRotation: 0
                }
            },
            y: {
                display: false,
                suggestedMin,
                suggestedMax,
                grid: {
                    display: false,
                    drawBorder: false
                }
            }
        },
        interaction: {
            intersect: false,
            mode: 'index'
        }
    };
    ensurePanSupport(options, 'x');
    if(pnlTrendChart){
        pnlTrendChart.data.datasets = [dataset];
        pnlTrendChart.options = options;
        pnlTrendChart.update('none');
        return;
    }
    if(typeof Chart === 'undefined'){
        return;
    }
    const ctx = canvas.getContext('2d');
    pnlTrendChart = new Chart(ctx, {
        type: 'line',
        data: { datasets: [dataset] },
        options
    });
}

function renderCategoryAnalytics(categoryKey, config){
    const normalized = categoryKey.toLowerCase();
    const listEl = document.getElementById(config.listId);
    const chartTabs = Array.isArray(config.chartTabs) && config.chartTabs.length
        ? config.chartTabs
        : [{ key: 'allocation', chartId: config.chartId, type: 'bar' }];
    const items = positions.filter(p=> (p.type || '').toLowerCase() === normalized);
    const totalPortfolioValue = positions.reduce((sum,p)=> sum + Number(p.marketValue || 0), 0);
    const totalCategoryValue = items.reduce((sum,p)=> sum + Number(p.marketValue || 0), 0);
    const totalPnl = items.reduce((sum,p)=> sum + Number((p.rangePnl ?? p.pnl) || 0), 0);
    const allocation = totalPortfolioValue ? (totalCategoryValue / totalPortfolioValue) * 100 : null;

    setCategoryMetric(config.metricKey, 'market', totalCategoryValue, config.summary.market, money);
    setCategoryMetric(
        config.metricKey,
        'pnl',
        totalPnl,
        config.summary.pnl,
        value => formatCategorySummaryPnl(value, totalCategoryValue)
    );
    setCategoryMetric(config.metricKey, 'allocation', allocation, config.summary.allocation, value => {
        if(value === null) return '—';
        return `${value.toFixed(1)}%`;
    });

    if(!items.length){
        if(listEl) listEl.innerHTML = `<div class="pos">No ${config.emptyLabel} holdings yet.</div>`;
        chartTabs.forEach(tab=>{
            if(tab.chartId && charts[tab.chartId]){
                charts[tab.chartId].destroy();
                delete charts[tab.chartId];
            }
        });
        return;
    }

    const sorted = [...items].sort((a,b)=> (b.marketValue || 0) - (a.marketValue || 0));
    const openPositions = sorted.filter(p=> Math.abs(Number(p.qty || 0)) > 1e-6);
    const closedPositions = sorted.filter(p=>{
        const realized = Number(p.realized || 0);
        const isClosed = Math.abs(Number(p.qty || 0)) <= 1e-6;
        return isClosed || Math.abs(realized) > 1e-2;
    });
    const openMarketValue = openPositions.reduce((sum,p)=> sum + Number(p.marketValue || 0), 0);
    const closedRealizedValue = closedPositions.reduce((sum,p)=> sum + Number(p.realized || 0), 0);

    const chartSource = openPositions.length
        ? openPositions.filter(p=> Number(p.marketValue || 0) > 0)
        : sorted.filter(p=> Number(p.marketValue || 0) > 0);

    const allocationPayload = buildAllocationChartPayload(chartSource);
    const performancePayload = buildPerformanceChartPayload(sorted);
const exposurePayload = buildExposureChartPayload(openPositions, closedPositions);

    chartTabs.forEach(tab=>{
        let payload = null;
        switch(tab.key){
            case 'performance':
                payload = performancePayload;
                break;
            case 'exposure':
                payload = exposurePayload;
                break;
            default:
                payload = allocationPayload;
        }
        if(payload && tab.chartId){
            createOrUpdateChart(tab.chartId, tab.type || payload.type || 'bar', payload.data, payload.options);
        }else if(tab.chartId && charts[tab.chartId]){
            charts[tab.chartId].destroy();
            delete charts[tab.chartId];
        }
    });

    if(listEl){
        const state = categorySectionState[config.metricKey] || { open: true, closed: false };
        categorySectionState[config.metricKey] = state;
        listEl.innerHTML = '';
        listEl.appendChild(buildAnalyticsSection(config.metricKey, 'Open positions', openPositions, {
            open: Boolean(state.open),
            sectionKey: 'open',
            extra: `Total ${money(openMarketValue)}`,
            emptyMessage: `No ${config.emptyLabel} open positions yet.`,
            totalCategoryValue
        }));

        listEl.appendChild(buildAnalyticsSection(config.metricKey, 'Closed positions', closedPositions, {
            open: Boolean(state.closed),
            sectionKey: 'closed',
            extra: `Total realized ${money(closedRealizedValue)}`,
            emptyMessage: `No ${config.emptyLabel} closed positions yet.`,
            type: 'closed'
        }));
    }
    applyAssetViewMode();
    const desiredTab = categoryChartTabState[normalized] || (chartTabs[0]?.key);
    if(desiredTab){
        setCategoryChartTab(normalized, desiredTab, { silent: true });
    }
}

function renderCryptoAnalytics(){
    renderCategoryAnalytics('crypto', CATEGORY_CONFIG.crypto);
}

function renderStockAnalytics(){
    renderCategoryAnalytics('stock', CATEGORY_CONFIG.stock);
    updateMarketStatus();
}

function buildAllocationChartPayload(source){
    if(!Array.isArray(source) || !source.length){
        return null;
    }
    const labels = source.map(p=> p.displayName || p.Symbol || p.Name);
    const data = source.map(p=> Number(p.marketValue || 0));
    const backgroundColors = labels.map((_, idx)=> `hsla(${(idx * 47) % 360},70%,60%,0.75)`);
    const borderColors = labels.map((_, idx)=> `hsla(${(idx * 47) % 360},70%,50%,1)`);
    return {
        type: 'bar',
        data: { labels, datasets: [{ data, backgroundColor: backgroundColors, borderColor: borderColors, borderWidth: 1, borderRadius: 8 }] },
        options: {
            plugins: { legend: { display: false } },
            interaction: { mode: 'index', intersect: false },
            scales: {
                x: { ticks: { autoSkip: false, maxRotation: 45, minRotation: 0 } },
                y: {
                    beginAtZero: true,
                    ticks: { callback: value => money(value) }
                }
            }
        }
    };
}

function buildPerformanceChartPayload(positions){
    if(!Array.isArray(positions) || !positions.length){
        return null;
    }
    const sample = positions.slice(0, Math.min(8, positions.length));
    if(!sample.length){
        return null;
    }
    const labels = sample.map(p=> p.displayName || p.Symbol || p.Name);
    const values = sample.map(p=>{
        const pctValue = Number(p.rangeChangePct);
        if(Number.isFinite(pctValue)) return pctValue;
        const market = Number(p.marketValue || 0);
        const cost = Number(p.costBasis || 0);
        if(!cost) return 0;
        return ((market - cost) / Math.abs(cost)) * 100;
    });
    const backgroundColors = values.map(value=> value >= 0 ? 'rgba(34, 197, 94, 0.75)' : 'rgba(239, 68, 68, 0.75)');
    return {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                data: values,
                backgroundColor: backgroundColors,
                borderRadius: 6,
                borderSkipped: false
            }]
        },
        options: {
            indexAxis: 'y',
            plugins: { legend: { display: false } },
            scales: {
                x: {
                    beginAtZero: true,
                    ticks: {
                        callback: value => `${Number(value).toFixed(0)}%`
                    },
                    grid: { color: 'rgba(148, 163, 184, 0.2)' }
                },
                y: {
                    ticks: { autoSkip: false },
                    grid: { display: false }
                }
            }
        }
    };
}

function buildExposureChartPayload(openPositions, closedPositions){
    const invested = openPositions.reduce((sum,p)=> sum + Number(p.costBasis || 0), 0);
    const marketValue = openPositions.reduce((sum,p)=> sum + Number(p.marketValue || 0), 0);
    const realized = closedPositions.reduce((sum,p)=> sum + Number(p.realized || 0), 0);
    const data = [
        Math.max(marketValue, 0),
        Math.max(invested, 0),
        Math.max(Math.abs(realized), 0)
    ];
    if(!data.some(value => value > 0)){
        return null;
    }
    const realizedColor = realized >= 0 ? 'rgba(34, 197, 94, 0.85)' : 'rgba(239, 68, 68, 0.85)';
    return {
        type: 'doughnut',
        data: {
            labels: ['Market value', 'Invested capital', 'Realized P&L'],
            datasets: [{
                data,
                backgroundColor: [
                    'rgba(59, 130, 246, 0.85)',
                    'rgba(251, 191, 36, 0.92)',
                    realizedColor
                ],
                borderColor: 'rgba(15, 23, 42, 0.9)',
                borderWidth: 1
            }]
        },
        options: {
            plugins: {
                legend: { position: 'bottom' },
                tooltip: {
                    callbacks: {
                        label(ctx){
                            const label = ctx.label || '';
                            const value = Number(ctx.parsed || 0);
                            return `${label}: ${money(value)}`;
                        }
                    }
                }
            },
            cutout: '58%'
        }
    };
}

function updateKpis(){
    positions.forEach(recomputePositionMetrics);
    const totalPnl = currentCategoryRangeTotals.crypto + currentCategoryRangeTotals.stock + currentCategoryRangeTotals.realEstate;
    const netWorthTotals = positions.reduce((acc, position)=>{
        const value = Number(position.marketValue || 0);
        if(!value) return acc;
        const key = getNetWorthKey(position);
        acc[key] = (acc[key] || 0) + value;
        return acc;
    }, {});
    let realEstateAnalytics = null;
    try{
        realEstateAnalytics = computeRealEstateAnalytics();
        if(realEstateAnalytics && Array.isArray(realEstateAnalytics.rows) && realEstateAnalytics.rows.length){
            const projectedByCategory = realEstateAnalytics.rows.reduce((acc, stat)=>{
                const value = Number(stat.projectedValue || 0);
                if(!Number.isFinite(value) || value <= 0) return acc;
                const key = getNetWorthKey(stat.positionRef || { type: stat.category, Name: stat.name });
                acc[key] = (acc[key] || 0) + value;
                return acc;
            }, {});
            Object.entries(projectedByCategory).forEach(([key, value])=>{
                if(Number.isFinite(value)){
                    netWorthTotals[key] = value;
                }
            });
        }
    }catch(error){
        console.warn('Failed to compute real estate projected totals for net worth', error);
    }
    lastRealEstateAnalytics = realEstateAnalytics;

    const totalMarketValue = Object.values(netWorthTotals).reduce((sum, value)=> sum + Number(value || 0), 0);

    lastNetWorthTotals = { ...(netWorthTotals || {}) };
    lastNetWorthTotalValue = totalMarketValue;

    const netWorthTimeline = computeNetWorthTimeline(totalMarketValue);
    lastNetWorthTimeline = netWorthTimeline;
    renderNetWorthSparkline(netWorthTimeline);
    updateMillionTargetNote(netWorthTimeline);

    setMoneyWithFlash('total-pnl', totalPnl, 'totalPnl');
    const totalPnlEl = document.getElementById('total-pnl');
    if(totalPnlEl){
        const totalPct = totalMarketValue ? (totalPnl / totalMarketValue) * 100 : null;
        totalPnlEl.textContent = formatMoneyWithPercent(totalPnl, Number.isFinite(totalPct) ? totalPct : null, 1);
    }
    setMoneyWithFlash('equity', totalMarketValue, 'netWorth');
    setCategoryPnl('pnl-category-crypto', currentCategoryRangeTotals.crypto || 0, 'pnlCrypto');
    setCategoryPnl('pnl-category-stock', currentCategoryRangeTotals.stock || 0, 'pnlStock');
    setCategoryPnl('pnl-category-realestate', currentCategoryRangeTotals.realEstate || 0, 'pnlRealEstate');
    updateBestPerformerCache(lastRealEstateAnalytics);
    refreshActiveBestInfoPopover();
    renderPnlTrendChart(pnlRange);
    if(netWorthInlineViewMode === 'bubble'){
        const snapshotHasData = netWorthBubbleSnapshot && Object.keys(netWorthBubbleSnapshot).length > 0;
        const currentHasData = lastNetWorthTotals && Object.keys(lastNetWorthTotals).length > 0;
        if(!snapshotHasData && currentHasData){
            netWorthBubbleSnapshot = { ...lastNetWorthTotals };
            netWorthBubbleSnapshotTotal = lastNetWorthTotalValue;
            netWorthBubbleNeedsRender = true;
        }
    }
    if(netWorthInlineViewMode === 'bubble' && netWorthBubbleNeedsRender){
        const sourceTotals = netWorthBubbleSnapshot || lastNetWorthTotals;
        const sourceTotalValue = netWorthBubbleSnapshot ? netWorthBubbleSnapshotTotal : lastNetWorthTotalValue;
        const result = renderNetWorthMindmap(sourceTotals, sourceTotalValue);
        if(result){
            netWorthBubbleNeedsRender = false;
        }
    }

}

function renderDashboard(){
    updateKpis();
    renderCryptoAnalytics();
    renderStockAnalytics();
    updateRealEstateRentals();
    renderAllCharts();
    const opsText = operationsMeta.count ? ` · ${operationsMeta.count} Airtable ops` : '';
    setStatus(`Live ${new Date().toLocaleTimeString()}${opsText}`);
}

function scheduleUIUpdate(options = {}){
    const immediate = options.immediate;
    if(immediate){
        if(scheduledRender){
            clearTimeout(scheduledRender);
            scheduledRender = null;
        }
        lastRenderAt = Date.now();
        if(rangeDirty){
            recomputeRangeMetrics(pnlRange);
        }
        renderDashboard();
        return;
    }
    const now = Date.now();
    const elapsed = now - lastRenderAt;
    if(elapsed >= UI_REFRESH_INTERVAL){
        if(scheduledRender){
            clearTimeout(scheduledRender);
            scheduledRender = null;
        }
        lastRenderAt = now;
        if(rangeDirty){
            recomputeRangeMetrics(pnlRange);
        }
        renderDashboard();
    }else if(!scheduledRender){
        scheduledRender = setTimeout(()=>{
            scheduledRender = null;
            lastRenderAt = Date.now();
            if(rangeDirty){
                recomputeRangeMetrics(pnlRange);
            }
            renderDashboard();
        }, UI_REFRESH_INTERVAL - elapsed);
    }
}

function scheduleCryptoUiUpdate(){
    const now = Date.now();
    const elapsed = now - lastCryptoUiSync;
    if(elapsed >= CRYPTO_UI_INTERVAL){
        if(pendingCryptoUiSync){
            clearTimeout(pendingCryptoUiSync);
            pendingCryptoUiSync = null;
        }
        lastCryptoUiSync = now;
        scheduleUIUpdate({immediate:true});
    }else if(!pendingCryptoUiSync){
        const remaining = CRYPTO_UI_INTERVAL - elapsed;
        pendingCryptoUiSync = setTimeout(()=>{
            pendingCryptoUiSync = null;
            lastCryptoUiSync = Date.now();
            scheduleUIUpdate({immediate:true});
        }, remaining);
    }
}

function updatePnlPercentageButtonState(){
    if(!pnlPercentageToggleButton) return;
    const active = showPnlPercentages;
    const label = active ? 'Hide P&L percentages' : 'Show P&L percentages';
    pnlPercentageToggleButton.classList.toggle('active', active);
    pnlPercentageToggleButton.setAttribute('aria-pressed', active ? 'true' : 'false');
    pnlPercentageToggleButton.setAttribute('aria-label', label);
    pnlPercentageToggleButton.setAttribute('title', label);
    const srLabel = pnlPercentageToggleButton.querySelector('#pnl-percentage-toggle-label');
    if(srLabel){
        srLabel.textContent = label;
    }
}

function setPnlPercentageVisibility(enabled){
    const next = Boolean(enabled);
    if(showPnlPercentages === next){
        updatePnlPercentageButtonState();
        return;
    }
    showPnlPercentages = next;
    if(typeof document !== 'undefined' && document.body){
        document.body.classList.toggle('show-pnl-percentages', showPnlPercentages);
    }
    updatePnlPercentageButtonState();
    try{
        if(typeof window !== 'undefined' && window.localStorage){
            window.localStorage.setItem(PNL_PERCENTAGE_STORAGE_KEY, showPnlPercentages ? 'true' : 'false');
        }
    }catch(error){
        console.warn('Failed to persist P&L percentage preference', error);
    }
    scheduleUIUpdate({immediate:true});
}

// ----------------- BOOTSTRAP LOGIC -----------------
async function bootstrap(){
    await loadPositions();
    preloadHistoricalPriceSeries().catch(error=>{
        console.warn('Historical price preload failed', error);
    });

    const finnhubSymbols = HAS_FINNHUB_KEY ? Array.from(symbolSet) : [];
    if(finnhubSymbols.length){
        const batches = [];
        for(let i=0;i<finnhubSymbols.length;i+=MAX_REST_BATCH){
            batches.push(finnhubSymbols.slice(i, i + MAX_REST_BATCH));
        }
        const MAX_SNAPSHOT_CONCURRENCY = 4;
        let nextIndex = 0;

        async function snapshotWorker(){
            while(nextIndex < batches.length){
                const currentIndex = nextIndex;
                nextIndex += 1;
                const batch = batches[currentIndex];
                if(!batch || !batch.length) continue;
                try{
                    const results = await fetchSnapshotBatch(batch);
                    await applySnapshotResults(results);
                }catch(error){
                    console.warn('Snapshot batch failed', batch, error);
                }finally{
                    if(nextIndex < batches.length && batches.length > MAX_SNAPSHOT_CONCURRENCY){
                        await new Promise(resolve=>setTimeout(resolve, 60));
                    }
                }
            }
        }

        const workerCount = Math.min(MAX_SNAPSHOT_CONCURRENCY, batches.length);
        await Promise.all(Array.from({length: workerCount}, snapshotWorker));
    }else if(!HAS_FINNHUB_KEY){
        console.warn('Skipping Finnhub snapshot bootstrap — API key missing');
    }

    positions.forEach(recomputePositionMetrics);
    lastUpdated = new Date();
    scheduleUIUpdate({immediate:true});
    initFinnhubWS();
}

// ---------- INTERACTIONS ----------
document.addEventListener('DOMContentLoaded', ()=>{
    if(loadingOverlay){
        setLoadingState('visible','Loading Airtable…');
    }
    registerZoomPluginIfNeeded();
    updateDataSourceBadge();
    const dataSourceSelect = document.getElementById('data-source-select');
    if(dataSourceSelect){
        dataSourceSelect.addEventListener('change', event=>{
            const value = event.target.value;
            if(value === 'loading'){
                event.target.value = dataSourceMode;
                return;
            }
            switchDataSource(value);
        });
    }
    ensureTransactionModalElements();
    ensureNetWorthDetailModalElements();
    initializePnlRangeTabs();
    renderPnlTrendChart(pnlRange);
    initializeBestInfoTriggers();
    initializeChartTabs();
    document.addEventListener('keydown', event => {
        if(event.key === 'Escape'){
            let handled = false;
            if(netWorthDetailModal && !netWorthDetailModal.classList.contains('hidden')){
                closeNetWorthDetailModal();
                handled = true;
            }
            if(transactionModal && !transactionModal.classList.contains('hidden')){
                closeTransactionModal();
                handled = true;
            }
            if(bestInfoPopoverElements && bestInfoPopoverElements.popover && !bestInfoPopoverElements.popover.classList.contains('hidden')){
                hideBestInfoPopover();
                handled = true;
            }
            if(handled){
                event.preventDefault();
            }
        }
    });
    const themeToggle = document.getElementById('theme-toggle');
    if(themeToggle){
        updateThemeToggleIcon(themeToggle, document.body.classList.contains('light-theme'));
        themeToggle.addEventListener('click', ()=>{
            const isLight = document.body.classList.toggle('light-theme');
            updateThemeToggleIcon(themeToggle, isLight);
            if(netWorthDetailModal && !netWorthDetailModal.classList.contains('hidden')){
                renderNetWorthDetailChart(lastNetWorthTimeline);
            }
            if(netWorthInlineViewMode === 'bubble'){
                netWorthBubbleNeedsRender = true;
                const sourceTotals = netWorthBubbleSnapshot || lastNetWorthTotals;
                const sourceTotalValue = netWorthBubbleSnapshot ? netWorthBubbleSnapshotTotal : lastNetWorthTotalValue;
                const result = renderNetWorthMindmap(sourceTotals, sourceTotalValue);
                if(result){
                    netWorthBubbleNeedsRender = false;
                }
            }
        });
    }

    assetViewToggleButton = document.getElementById('asset-view-toggle');
    try{
        if(typeof window !== 'undefined' && window.localStorage){
            const storedMode = window.localStorage.getItem(VIEW_MODE_STORAGE_KEY);
            if(storedMode === 'plates' || storedMode === 'rows'){
                assetViewMode = storedMode;
            }
        }
    }catch(error){
        console.warn('Failed to read stored asset view mode', error);
    }
    applyAssetViewMode();
    if(assetViewToggleButton){
        assetViewToggleButton.addEventListener('click', ()=>{
            const nextMode = assetViewMode === 'plates' ? 'rows' : 'plates';
            setAssetViewMode(nextMode);
        });
    }

    netWorthBubbleToggleButton = document.getElementById('networth-mindmap-button');
    try{
        if(typeof window !== 'undefined' && window.localStorage){
            const storedViewMode = window.localStorage.getItem(NET_WORTH_INLINE_VIEW_STORAGE_KEY);
            if(storedViewMode === 'bubble' || storedViewMode === 'chart'){
                netWorthInlineViewMode = storedViewMode;
            }
        }
    }catch(error){
        console.warn('Failed to read net worth view preference', error);
    }
    if(netWorthBubbleToggleButton){
        netWorthBubbleToggleButton.addEventListener('click', ()=>{
            const nextMode = netWorthInlineViewMode === 'bubble' ? 'chart' : 'bubble';
            setNetWorthInlineViewMode(nextMode);
        });
    }
    applyNetWorthInlineViewMode();

    pnlPercentageToggleButton = document.getElementById('pnl-percentage-toggle');
    if(pnlPercentageToggleButton){
        try{
            if(typeof window !== 'undefined' && window.localStorage){
                const storedPercentPref = window.localStorage.getItem(PNL_PERCENTAGE_STORAGE_KEY);
                if(storedPercentPref === 'true' || storedPercentPref === 'false'){
                    showPnlPercentages = storedPercentPref === 'true';
                }
            }
        }catch(error){
            console.warn('Failed to read P&L percentage preference', error);
        }
        if(typeof document !== 'undefined' && document.body){
            document.body.classList.toggle('show-pnl-percentages', showPnlPercentages);
        }
        updatePnlPercentageButtonState();
        pnlPercentageToggleButton.addEventListener('click', ()=>{
            setPnlPercentageVisibility(!showPnlPercentages);
        });
    }

    initMarketStatusWatcher();

    document.querySelectorAll('details[data-chart], details[data-chart-group]').forEach(section=>{
        section.addEventListener('toggle', ()=>{
            if(!section.open) return;
            const chartId = section.getAttribute('data-chart');
            if(chartId){
                requestAnimationFrame(()=>{
                    const chart = charts[chartId];
                    if(chart){
                        chart.resize();
                        chart.update('none');
                    }
                });
            }
            const groupKey = section.getAttribute('data-chart-group');
            if(groupKey){
                const config = CATEGORY_CONFIG[groupKey];
                const tabs = Array.isArray(config?.chartTabs) ? config.chartTabs : [];
                requestAnimationFrame(()=>{
                    tabs.forEach(tab=>{
                        const chart = charts[tab.chartId];
                        if(chart){
                            chart.resize();
                            chart.update('none');
                        }
                    });
                });
            }
        });
    });

    let resizeTimer;
    window.addEventListener('resize', ()=>{
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(()=>{
            Object.values(charts).forEach(chart=>chart && chart.resize());
            if(netWorthInlineViewMode === 'bubble'){
                netWorthBubbleNeedsRender = true;
                const sourceTotals = netWorthBubbleSnapshot || lastNetWorthTotals;
                const sourceTotalValue = netWorthBubbleSnapshot ? netWorthBubbleSnapshotTotal : lastNetWorthTotalValue;
                const result = renderNetWorthMindmap(sourceTotals, sourceTotalValue);
                if(result){
                    netWorthBubbleNeedsRender = false;
                }
            }
        }, 160);
    });

    bootstrap().then(()=>{
        isBootstrapping = false;
        if(loadingOverlay){
            setLoadingState('hidden');
        }
    }).catch(err=>{
        console.error('Bootstrap failed', err);
        setStatus('Bootstrap failed — see console');
        isBootstrapping = false;
        if(loadingOverlay){
            setLoadingState('error','Bootstrap failed — see console');
        }
    });
});
