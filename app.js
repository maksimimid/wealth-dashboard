// ------------------ CONFIG ------------------
const DEFAULT_CONFIG = {
    FINNHUB_KEY: '',
    AIRTABLE_API_KEY: '',
    AIRTABLE_BASE_ID: 'appSxixo1i122KyBS',
    AIRTABLE_TABLE_NAME: 'Operations'
};

const RUNTIME_CONFIG = (typeof window !== 'undefined' && window.DASHBOARD_CONFIG) ? window.DASHBOARD_CONFIG : {};

const loadingOverlay = typeof document !== 'undefined' ? document.getElementById('loading-overlay') : null;

const FINNHUB_KEY = RUNTIME_CONFIG.FINNHUB_KEY || DEFAULT_CONFIG.FINNHUB_KEY;
const FINNHUB_REST = 'https://finnhub.io/api/v1';
const MAX_REST_BATCH = 5;

const AIRTABLE_API_KEY = RUNTIME_CONFIG.AIRTABLE_API_KEY || DEFAULT_CONFIG.AIRTABLE_API_KEY;
const AIRTABLE_BASE_ID = RUNTIME_CONFIG.AIRTABLE_BASE_ID || DEFAULT_CONFIG.AIRTABLE_BASE_ID;
const AIRTABLE_TABLE_NAME = RUNTIME_CONFIG.AIRTABLE_TABLE_NAME || DEFAULT_CONFIG.AIRTABLE_TABLE_NAME;
const AIRTABLE_URL = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(AIRTABLE_TABLE_NAME)}`;

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
let currentRangeTotalPnl = 0;
let currentCategoryRangeTotals = { crypto: 0, stock: 0, realEstate: 0, other: 0 };
let rangeDirty = true;
const referencePriceCache = new Map();
const RANGE_LOOKBACK = {};
const RANGE_LABELS = { 'ALL': 'All Time' };
let pnlRange = 'ALL';
const previousKpiValues = { totalPnl: null, netWorth: null, netContribution: null, cashAvailable: null, pnlCrypto: null, pnlStock: null, pnlRealEstate: null };
const previousBestPerformer = { id: null, pnl: null, change: null };
let assetYearSeries = { labels: [], datasets: [] };
let assetYearSeriesDirty = true;
const assetColorCache = new Map();
let realEstateRentSeries = { labels: [], datasets: [] };
let realEstateRentSeriesDirty = true;
const realEstateRentFilters = new Map();
const realEstateGroupState = { active: true, passive: false };
let transactionModal = null;
let transactionModalTitle = null;
let transactionModalSubtitle = null;
let transactionModalMeta = null;
let transactionModalCanvas = null;
let transactionChart = null;
let lastTransactionTrigger = null;
let lastTransactionData = null;
let lastTransactionPosition = null;
let modalChartContainer = null;
let transactionLotsContainer = null;
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
let lastCryptoUiSync = 0;
let pendingCryptoUiSync = null;
const CATEGORY_CONFIG = {
    crypto: {
        metricKey: 'crypto',
        chartId: 'cryptoPortfolioChart',
        listId: 'crypto-positions',
        summary: {
            market: 'crypto-market-value',
            pnl: 'crypto-pnl',
            allocation: 'crypto-allocation'
        },
        emptyLabel: 'crypto'
    },
    stock: {
        metricKey: 'stock',
        chartId: 'stockPortfolioChart',
        listId: 'stock-positions',
        summary: {
            market: 'stock-market-value',
            pnl: 'stock-pnl',
            allocation: 'stock-allocation'
        },
        emptyLabel: 'stock'
    }
};
const CRYPTO_ICON_PROVIDERS = [
    symbol => `https://raw.githubusercontent.com/spothq/cryptocurrency-icons/master/svg/color/${symbol}.svg`,
    symbol => `https://raw.githubusercontent.com/spothq/cryptocurrency-icons/master/svg/black/${symbol}.svg`,
    symbol => `https://raw.githubusercontent.com/ErikThiart/cryptocurrency-icons/master/svg/color/${symbol}.svg`
];
const assetIconSourceCache = new Map();
const transactionPriceCache = new Map();
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

function applyRangeButtons(){ /* no-op */ }

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
    setLoadingState('visible', message);
    setStatus(message);
}

// ----------------- UTIL ---------------------
function money(v){
    if(v===null || v===undefined || Number.isNaN(Number(v))) return '—';
    return '$' + Number(v).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});
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
        const base = position.prevClose ?? position.avgPrice ?? position.lastKnownPrice ?? position.displayPrice ?? 0;
        bucket['1D'] = base;
        return base;
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
    const endpoint = position.finnhubSymbol.includes(':') ? 'crypto/candle' : 'stock/candle';
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
        let pnl = unrealized + realized;
        const typeKey = (position.type || '').toLowerCase();
        if(typeKey === 'real estate'){
            const rentPnl = position.rentRealized || 0;
            pnl = rentPnl;
            categoryTotals.realEstate += rentPnl;
        }else if(typeKey === 'crypto'){
            categoryTotals.crypto += pnl;
        }else if(typeKey === 'stock'){
            categoryTotals.stock += pnl;
        }else{
            categoryTotals.other += pnl;
        }
        position.rangePnl = pnl;
        const baseValue = base * qty;
        let denominator = baseValue !== 0 ? baseValue : Math.abs(position.invested) || Math.abs(realized) || 1;
        if(typeKey === 'real estate'){
            denominator = Math.abs(position.invested) || Math.abs(pnl) || 1;
        }
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
    if(isRangeUpdateInFlight || range === pnlRange) return;
    isRangeUpdateInFlight = true;
    try{
        pnlRange = range;
        applyRangeButtons(range);
        if(range !== 'ALL'){
            const tasks = positions.map(position => ensureRangeReference(position, range));
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
        ISAC: 'NASDAQ:ISAC'
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
        'NASDAQ:ISAC': 'ISAC',
        'ISAC': 'ISAC'
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
    position.pnl = position.unrealized + realized;
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
                operations: []
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
            skipInCharts: Boolean(isReinvesting)
        });

        if(opType === 'PurchaseSell'){
            if(amount > 0){
                entry.qty += amount;
                entry.costBasis += spent;
                if(spent > 0) entry.invested += spent;
                const totalCost = spent !== 0 ? spent : (price * amount);
                const unitPrice = amount !== 0 ? totalCost / amount : price;
                if(unitPrice){
                    entry.lastPurchasePrice = unitPrice;
                    entry.lastKnownPrice = unitPrice;
                }
            }else if(amount < 0){
                const sellQty = Math.abs(amount);
                const prevQty = entry.qty;
                const prevCost = entry.costBasis;
                const avgCost = prevQty > 0 ? prevCost / prevQty : entry.lastPurchasePrice || price || 0;
                const costOut = avgCost * sellQty;
                entry.qty = Math.max(0, prevQty + amount);
                entry.costBasis = Math.max(0, prevCost - costOut);
                const proceeds = spent < 0 ? Math.abs(spent) : sellQty * price;
                entry.realized += proceeds - costOut;
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

        if(entry.qty <= 0){
            entry.qty = 0;
            entry.costBasis = 0;
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
        recomputePositionMetrics(p);
        ensurePositionDefaults(p);
        p.referencePrices = {};
        return p;
    });
}

function useFallbackPositions(){
    const fallback = [
        {Name:'Apple',category:'Stock',symbol:'AAPL',qty:10,avgPrice:150},
        {Name:'Bitcoin',category:'Crypto',symbol:'BINANCE:BTCUSDT',qty:0.25,avgPrice:30000},
        {Name:'Cash Reserve',category:'Cash',symbol:null,qty:2500,avgPrice:1}
    ];
    const map = fallback.map(f=>({
        Name:f.Name,
        displayName:f.Name,
        Category:normalizeCategory(f.category, f.Name),
        type:normalizeCategory(f.category, f.Name),
        Symbol:f.symbol || f.Name,
        finnhubSymbol:f.symbol,
        qty:f.qty,
        costBasis:f.qty * f.avgPrice,
        invested: f.qty * f.avgPrice,
        realized:0,
        cashflow:0,
        lastKnownPrice:f.avgPrice
    }));
    return map.map(p=>{
        recomputePositionMetrics(p);
        ensurePositionDefaults(p);
        p.referencePrices = {};
        p.finnhubOverride = false;
        p.rentRealized = 0;
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
        symbolSet = new Set();
        finnhubIndex = new Map();
        positions.forEach(p=>{
            if(p.finnhubSymbol){
                symbolSet.add(p.finnhubSymbol);
                finnhubIndex.set(p.finnhubSymbol, p);
            }
        });
        if(!positions.length){
            throw new Error('No positions available after transformation');
        }
    }catch(err){
        console.warn('Airtable load failed, using fallback', err);
        setStatus('Airtable unavailable — showing demo data');
        positions = useFallbackPositions();
        netContributionTotal = positions.reduce((sum,p)=>sum + Number(p.cashflow || 0),0);
        symbolSet = new Set();
        finnhubIndex = new Map();
        positions.forEach(p=>{
            if(p.finnhubSymbol){
                symbolSet.add(p.finnhubSymbol);
                finnhubIndex.set(p.finnhubSymbol, p);
            }
        });
        setLoadingState('error','Airtable unavailable — showing demo data');
        setTimeout(()=>setLoadingState('hidden'), 1400);
    }
    rangeDirty = true;
    assetYearSeriesDirty = true;
    realEstateRentSeriesDirty = true;
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

function applySnapshotResults(results){
    results.forEach(result=>{
        if(!result.ok) return;
        const pos = finnhubIndex.get(result.symbol);
        if(!pos) return;
        const data = result.data || {};
        const price = data.c ?? pos.currentPrice ?? pos.lastKnownPrice ?? pos.avgPrice;
        applyLivePrice(pos, price);
        pos.prevClose = data.pc ?? pos.prevClose ?? null;
        if(pos.prevClose && price !== null && price !== undefined){
            pos.change = price - pos.prevClose;
            pos.changePct = pos.prevClose ? ((price - pos.prevClose) / pos.prevClose) * 100 : 0;
        }else{
            const reference = pos.avgPrice || pos.lastKnownPrice || price;
            pos.change = price - reference;
            pos.changePct = reference ? (pos.change / reference) * 100 : 0;
        }
        recomputePositionMetrics(pos);
    });
    rangeDirty = true;
}

// ----------------- WEBSOCKET REAL-TIME -----------------
function initFinnhubWS(){
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
    if(!ws || ws.readyState !== WebSocket.OPEN) return;
    symbolSet.forEach(sym=>{
        try{ ws.send(JSON.stringify({type:'subscribe', symbol:sym})); }
        catch(error){ console.warn('Failed to subscribe', sym, error); }
    });
}

function applyRealtime(symbol, price){
    const pos = finnhubIndex.get(symbol);
    if(!pos) return;
    applyLivePrice(pos, price);
    if(pos.prevClose){
        pos.change = price - pos.prevClose;
        pos.changePct = pos.prevClose ? ((price - pos.prevClose) / pos.prevClose) * 100 : 0;
    }else{
        const ref = pos.avgPrice || pos.lastKnownPrice || price;
        pos.change = price - ref;
        pos.changePct = ref ? ((price - ref)/ref)*100 : 0;
    }
    recomputePositionMetrics(pos);
    lastUpdated = new Date();
    rangeDirty = true;
    const type = (pos.type || '').toLowerCase();
    if(type === 'crypto'){
        scheduleCryptoUiUpdate();
    }else{
        scheduleUIUpdate();
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
        existing.update('none');
        return existing;
    }
    const canvas = document.getElementById(id);
    if(!canvas) return null;
    const ctx = canvas.getContext('2d');
    const mergedOptions = mergeDeep(mergeDeep({}, defaultOptions), options || {});
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

function computeRealEstateAnalytics(){
    const now = new Date();
    const currentYear = now.getFullYear();
    const cutoffStart = new Date(now.getFullYear(), now.getMonth(), 1);
    cutoffStart.setMonth(cutoffStart.getMonth() - 11);
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

        results.push({
            name: position.displayName || position.Symbol || position.Name || `Asset ${idx+1}`,
            totalPurchase,
            totalExpenses,
            finalAssetPrice,
            rentCollected,
            rentYtd,
            avgMonthlyRent,
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
            `<div class="utilization-block">`
            + `<span class="label">Utilization</span>`
            + `<div class="circle-progress" style="--progress:${utilizationProgress};">`
            + `<div class="circle-progress-inner"><span>${utilizationDisplay}</span></div>`
            + `</div>`
            + `</div>`,
            `<div><span class="label">Payoff ETA</span><span class="value">${formatDurationFromMonths(stat.payoffMonths)}</span></div>`
        );
    }
    metrics.innerHTML = rows.join('');
    row.appendChild(metrics);

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
    transactionModalSubtitle = document.getElementById('transaction-modal-subtitle');
    transactionModalMeta = document.getElementById('transaction-modal-meta');
    transactionModalCanvas = document.getElementById('transaction-chart');
    modalChartContainer = transactionModal.querySelector('.modal-chart');
    transactionLotsContainer = document.getElementById('transaction-lots');
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
    if(transactionPriceCache.has(cacheKey)){
        return transactionPriceCache.get(cacheKey);
    }

    const operations = Array.isArray(position.operations) ? position.operations : [];
    const firstPurchase = operations
        .filter(op => String(op.type || '').toLowerCase() === 'purchasesell' && Number(op.amount || 0) > 0 && op.date instanceof Date)
        .sort((a,b)=> a.date - b.date)[0];
    const firstPurchaseTime = firstPurchase ? firstPurchase.date.getTime() : null;
    let series = [];
    if(finnhubSymbol && FINNHUB_KEY){
        series = await fetchFinnhubSeries(position, finnhubSymbol, firstPurchaseTime);
        if(series.length){
            transactionPriceCache.set(cacheKey, series);
            return series;
        }
    }

    if(coinGeckoId){
        series = await fetchCoinGeckoSeries(coinGeckoId, firstPurchaseTime);
        if(series.length){
            transactionPriceCache.set(cacheKey, series);
            return series;
        }
    }

    if(yahooSymbol){
        series = await fetchAlphaVantageSeries(yahooSymbol, typeKey, firstPurchaseTime);
        if(series.length){
            transactionPriceCache.set(cacheKey, series);
            return series;
        }
    }

    transactionPriceCache.set(cacheKey, []);
    return [];
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
        const endpoint = (/crypto/i.test(position.type || '') || rawSymbol.includes(':')) ? 'crypto/candle' : 'stock/candle';
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
    const effectivePriceSeries = (priceSeries.length ? priceSeries : (data.fallbackPriceSeries || [])).map(point=> ({ x: point.x, y: point.y ?? point.c ?? point.price ?? point.value }));
    const fallbackPrice = Number(position.displayPrice || position.currentPrice || position.lastKnownPrice || position.avgPrice || 0) || 0;
    const chartHasTransactions = data.purchases.length || data.sales.length;

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
        const avgClose = effectivePriceSeries.reduce((sum, point)=> sum + Number(point.y || 0), 0) / effectivePriceSeries.length || fallbackPrice;
        datasets.push({
            type: 'line',
            label: 'Avg trade price',
            data: effectivePriceSeries.map(point=> ({ x: point.x, y: avgClose })),
            borderColor: 'rgba(56, 189, 248, 0.55)',
            borderDash: [4, 4],
            borderWidth: 1.5,
            pointRadius: 0,
            tension: 0,
            order: 1
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
    const combinedValues = [...yValues, ...priceYValues].filter(value => Number.isFinite(value));
    const minY = combinedValues.length ? Math.min(...combinedValues) : fallbackPrice;
    const maxY = combinedValues.length ? Math.max(...combinedValues) : fallbackPrice;

    return {
        type: 'scatter',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            parsing: false,
            animation: false,
            plugins: {
                legend: { labels: { usePointStyle: true } },
                tooltip: {
                    callbacks: {
                        label(ctx){
                            const raw = ctx.raw || {};
                            if(ctx.dataset.type === 'line'){
                                if(ctx.dataset.label === 'Avg trade price'){
                                    return `Avg trade price ${money(raw.y)}`;
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
                    beginAtZero: false,
                    suggestedMin: Number.isFinite(minY) ? minY * 0.92 : undefined,
                    suggestedMax: Number.isFinite(maxY) ? maxY * 1.08 : undefined,
                    title: { display: true, text: 'Price' },
                    grid: { color: 'rgba(148, 163, 184, 0.25)' },
                    ticks: { callback: value => money(value) }
                }
            }
        }
    };
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

function renderTransactionLots(position, data){
    if(!transactionLotsContainer) return;
    const operations = Array.isArray(position.operations) ? position.operations.slice().sort((a,b)=>{
        const da = a.date instanceof Date ? a.date.getTime() : (a.rawDate ? new Date(a.rawDate).getTime() : 0);
        const db = b.date instanceof Date ? b.date.getTime() : (b.rawDate ? new Date(b.rawDate).getTime() : 0);
        return da - db;
    }) : [];
    if(!operations.length){
        transactionLotsContainer.innerHTML = '<div class="pos">No transactions recorded yet.</div>';
        return;
    }
    const currentPrice = Number(position.displayPrice || position.currentPrice || position.lastKnownPrice || position.avgPrice || 0);
    const fragment = document.createDocumentFragment();
    let rows = 0;
    operations.forEach((op, index)=>{
        if(op.skipInCharts) return;
        const qty = Number(op.amount || 0);
        if(!qty) return;
        const absQty = Math.abs(qty);
        const date = op.date instanceof Date ? op.date : (op.rawDate ? new Date(op.rawDate) : null);
        const price = Number(op.price || currentPrice || 0);
        const rawSpent = Number(op.spent);
        const spentAbs = Number.isFinite(rawSpent) ? Math.abs(rawSpent) : Math.abs(price * qty);
        const typeLabel = qty > 0 ? 'Buy' : 'Sell';
        const amountLabel = qty > 0 ? 'Invested' : 'Proceeds';
        let pnlValue = 0;
        let baseAmount = spentAbs;
        if(qty > 0){
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
        const row = document.createElement('div');
        row.className = 'lot-row';
        row.innerHTML = `
            <div><strong>${date ? formatDateShort(date) : '—'}</strong></div>
            <div>${typeLabel} · ${formatQty(absQty)}</div>
            <div>Price ${money(price)}</div>
            <div>${amountLabel} ${money(baseAmount)}</div>
            <div class="lot-value ${pnlClass}">${money(pnlValue)} (${formatPercent(pnlPct)})</div>
        `;
        fragment.appendChild(row);
        rows += 1;
    });
    if(!rows){
        transactionLotsContainer.innerHTML = '<div class="pos">No transactions recorded yet.</div>';
        return;
    }
    transactionLotsContainer.innerHTML = '';
    transactionLotsContainer.appendChild(fragment);
}

function renderTransactionMeta(position, summary){
    if(!transactionModalMeta) return;
    transactionModalMeta.innerHTML = `
        <div class="modal-meta-grid">
            <div><strong>Net qty:</strong> ${formatQty(summary.netQty)}</div>
            <div><strong>Total bought:</strong> ${formatQty(summary.totalBuys)}</div>
            <div><strong>Total sold:</strong> ${formatQty(summary.totalSells)}</div>
            <div><strong>Cash invested:</strong> ${money(summary.totalSpent)}</div>
            <div><strong>Cash returned:</strong> ${money(summary.totalProceeds)}</div>
        </div>
    `;
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
    if(transactionModalSubtitle){
            transactionModalSubtitle.textContent = position.type || '—';
            const note = document.createElement('div');
            note.className = 'pos modal-note';
            note.textContent = 'Dot size represents traded quantity.';
            transactionModalSubtitle.appendChild(note);
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
        renderTransactionMeta(position, data.summary);
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
    }catch(error){
        console.warn('Failed to load historical price series', error);
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
    const reinvestedValue = reinvestedQty > 1e-6 && Math.abs(reinvestPrice) > 1e-9
        ? reinvestPrice * reinvestedQty
        : 0;
    const reinvestedDisplay = reinvestedQty > 1e-6
        ? (Math.abs(reinvestPrice) > 1e-9
            ? `${money(reinvestedValue)} (${formatQty(reinvestedQty)} units)`
            : `${formatQty(reinvestedQty)} units`)
        : null;
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
    if(reinvestedDisplay){
        metaParts.push(`Reinvested ${reinvestedDisplay}`);
    }
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
    pnlEl.className = pnlValue >= 0 ? 'delta-positive' : 'delta-negative';
    const pnlPercent = Number(position.rangeChangePct);
    pnlEl.textContent = formatMoneyWithPercent(pnlValue, Number.isFinite(pnlPercent) ? pnlPercent : null, 1);
    const shareEl = document.createElement('div');
    shareEl.className = 'muted';
    shareEl.textContent = `Category ${shareText}`;
    values.appendChild(marketEl);
    values.appendChild(pnlEl);
    values.appendChild(shareEl);
    if(reinvestedDisplay){
        const reinvestEl = document.createElement('div');
        reinvestEl.className = 'reinvested-chip';
        reinvestEl.textContent = `Reinvested ${reinvestedDisplay}`;
        values.appendChild(reinvestEl);
    }
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
    const reinvestedQty = Math.max(0, Number(position.reinvested || 0));
    const reinvestPrice = Math.abs(price) > 1e-9 ? price : fallbackPrice;
    const reinvestedValue = reinvestedQty > 1e-6 && Math.abs(reinvestPrice) > 1e-9
        ? reinvestPrice * reinvestedQty
        : 0;
    const reinvestedDisplay = reinvestedQty > 1e-6
        ? (Math.abs(reinvestPrice) > 1e-9
            ? `${money(reinvestedValue)} (${formatQty(reinvestedQty)} units)`
            : `${formatQty(reinvestedQty)} units`)
        : null;
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
    if(reinvestedDisplay){
        metaParts.push(`Reinvested ${reinvestedDisplay}`);
    }
    metaEl.innerHTML = metaParts.join(' · ');
    label.appendChild(nameEl);
    label.appendChild(metaEl);
    main.appendChild(label);
    row.appendChild(main);

    const values = document.createElement('div');
    values.className = 'analytics-values';
    const pnlEl = document.createElement('div');
    pnlEl.className = realized >= 0 ? 'delta-positive' : 'delta-negative';
    pnlEl.textContent = formatMoneyWithPercent(realized, Number.isFinite(realizedPercent) ? realizedPercent : null, 1);
    const statusEl = document.createElement('div');
    statusEl.className = 'muted';
    statusEl.textContent = 'Position closed';
    values.appendChild(pnlEl);
    values.appendChild(statusEl);
    if(reinvestedDisplay){
        const reinvestEl = document.createElement('div');
        reinvestEl.className = 'reinvested-chip';
        reinvestEl.textContent = `Reinvested ${reinvestedDisplay}`;
        values.appendChild(reinvestEl);
    }
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

function updateNetWorthBreakdown(categoryMap){
    const container = document.getElementById('networth-breakdown');
    if(!container) return;
    container.innerHTML = '';
    const entries = Object.entries(categoryMap || {}).filter(([, value])=> Math.abs(value) > 1e-2).sort((a,b)=> b[1] - a[1]);
    if(!entries.length){
        const empty = document.createElement('div');
        empty.className = 'pos';
        empty.textContent = 'Awaiting holdings data…';
        container.appendChild(empty);
        return;
    }
    entries.forEach(([key, value])=>{
        const item = document.createElement('div');
        item.className = 'networth-sub';
        const labelKey = key || 'other';
        const label = NET_WORTH_LABEL_MAP[labelKey] || labelKey.replace(/([a-z])([A-Z])/g, '$1 $2').replace(/^./, letter=>letter.toUpperCase());
        item.innerHTML = `
            <span class="networth-sub-label">${label}</span>
            <span class="networth-sub-value">${money(value)}</span>
        `;
        container.appendChild(item);
    });
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

    const mainDetail = total > 0 ? 'Total value' : 'Awaiting data';
    const mainTitle = total > 0 ? `Total value: ${money(total)}` : 'Awaiting data';
    const mainDisplayValue = total === 0 ? '$0' : formatCompactMoney(total);
    const minDimension = Math.max(120, Math.min(width, height));
    const sizeLimit = Math.max(110, Math.min(minDimension - 24, 220));
    const baseMainSize = Math.min(sizeLimit, Math.max(110, Math.min(minDimension * 0.6, 200)));
    const mainSize = Math.max(94, Math.min(sizeLimit, Math.round(baseMainSize * 0.85)));

    const maxDiameter = nodesData.reduce((max, node)=> Math.max(max, node.size), 0);
    const availableRadius = Math.max(110, Math.min(width, height) / 2 - 30);
    const radius = Math.max(
        availableRadius * 0.9,
        availableRadius - (mainSize * 0.45) - (maxDiameter * 0.25),
        (mainSize * 0.6) + 120
    );

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
    nodesData.forEach((node, index)=>{
        const angle = twoPi * (index / nodesData.length) - Math.PI / 2;
        const rawX = centerX + Math.cos(angle) * radius;
        const rawY = centerY + Math.sin(angle) * radius;
        const half = node.size / 2;
        const clampedX = Math.min(width - half - 8, Math.max(half + 8, rawX));
        const clampedY = Math.min(height - half - 8, Math.max(half + 8, rawY));
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
        bubble.node.style.left = `${clampedX}px`;
        bubble.node.style.top = `${clampedY}px`;
        const titlePercent = percentLabel ? ` (${percentLabel})` : '';
        bubble.node.title = `${node.fullLabel}: ${money(node.value)}${titlePercent}`;
        bubble.node.setAttribute('aria-hidden', 'true');
        bubble.inner.style.animationDuration = `${(14 + Math.random() * 6).toFixed(2)}s`;
        bubble.inner.style.animationDelay = `${(Math.random() * -12).toFixed(2)}s`;
        nodesLayer.appendChild(bubble.node);

        const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
        line.setAttribute('x1', centerX);
        line.setAttribute('y1', centerY);
        line.setAttribute('x2', clampedX);
        line.setAttribute('y2', clampedY);
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

function renderCategoryAnalytics(categoryKey, config){
    const normalized = categoryKey.toLowerCase();
    const listEl = document.getElementById(config.listId);
    const chartId = config.chartId;
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
        if(charts[chartId]){
            charts[chartId].destroy();
            delete charts[chartId];
        }
        return;
    }

    const sorted = [...items].sort((a,b)=> (b.marketValue || 0) - (a.marketValue || 0));
    const openPositions = sorted.filter(p=> Number(p.qty || 0) > 0 || Number(p.marketValue || 0) > 1e-6);
    const closedPositions = sorted.filter(p=> !openPositions.includes(p));
    const openMarketValue = openPositions.reduce((sum,p)=> sum + Number(p.marketValue || 0), 0);
    const closedRealizedValue = closedPositions.reduce((sum,p)=> sum + Number(p.realized || 0), 0);

    const chartSource = openPositions.length
        ? openPositions.filter(p=> Number(p.marketValue || 0) > 0)
        : sorted.filter(p=> Number(p.marketValue || 0) > 0);

    if(chartSource.length){
        const labels = chartSource.map(p=> p.displayName || p.Symbol || p.Name);
        const data = chartSource.map(p=> Number(p.marketValue || 0));
        const backgroundColors = labels.map((_, idx)=> `hsla(${(idx * 47) % 360},70%,60%,0.75)`);
        const borderColors = labels.map((_, idx)=> `hsla(${(idx * 47) % 360},70%,50%,1)`);
        const chartData = { labels, datasets: [{ data, backgroundColor: backgroundColors, borderColor: borderColors, borderWidth: 1, borderRadius: 8 }] };
        createOrUpdateChart(chartId, 'bar', chartData, {
            plugins: { legend: { display: false } },
            interaction: { mode: 'index', intersect: false },
            scales: {
                x: { ticks: { autoSkip: false, maxRotation: 45, minRotation: 0 } },
                y: {
                    beginAtZero: true,
                    ticks: { callback: value => money(value) }
                }
            }
        });
    }else if(charts[chartId]){
        charts[chartId].destroy();
        delete charts[chartId];
    }

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
}

function renderCryptoAnalytics(){
    renderCategoryAnalytics('crypto', CATEGORY_CONFIG.crypto);
}

function renderStockAnalytics(){
    renderCategoryAnalytics('stock', CATEGORY_CONFIG.stock);
    updateMarketStatus();
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
    try{
        const realEstateAnalytics = computeRealEstateAnalytics();
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
    updateNetWorthBreakdown(netWorthTotals);
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

    const bestNameEl = document.getElementById('best-performer-name');
    const bestPnlEl = document.getElementById('best-performer-pnl');
    const bestChangeEl = document.getElementById('best-performer-change');
    const bestMetaEl = document.getElementById('best-performer-meta');
    if(bestNameEl || bestPnlEl || bestChangeEl || bestMetaEl){
        const eligible = positions.filter(position=>{
            const type = (position.type || '').toLowerCase();
            if(type === 'real estate') return false;
            const qty = Number(position.qty || 0);
            const mv = Number(position.marketValue || 0);
            if(Math.abs(qty) <= 1e-6 && Math.abs(mv) <= 1e-6) return false;
            return true;
        });
        const bestCandidate = eligible.reduce((acc, position)=>{
            const raw = position.rangePnl ?? position.pnl ?? Number.NEGATIVE_INFINITY;
            const value = Number(raw);
            const usable = Number.isFinite(value) ? value : Number.NEGATIVE_INFINITY;
            if(acc === null) return { position, value: usable };
            return usable > acc.value ? { position, value: usable } : acc;
        }, null);

        if(bestCandidate && bestCandidate.position && bestCandidate.value !== Number.NEGATIVE_INFINITY){
            const best = bestCandidate.position;
            const displayName = best.displayName || best.Symbol || best.Name || '—';
            const bestChanged = previousBestPerformer.id && previousBestPerformer.id !== best.id;
            if(bestNameEl){
                bestNameEl.textContent = displayName;
                if(bestChanged){
                    flashElement(bestNameEl, 'up');
                }
            }
            if(bestMetaEl){
                bestMetaEl.textContent = `${best.type || '—'} · Qty ${formatQty(Number(best.qty || 0))}`;
            }
            if(bestPnlEl){
                bestPnlEl.textContent = money(bestCandidate.value);
                if(bestChanged){
                    flashElement(bestPnlEl, 'up');
                }else if(previousBestPerformer.pnl !== null){
                    const direction = bestCandidate.value > previousBestPerformer.pnl ? 'up' : bestCandidate.value < previousBestPerformer.pnl ? 'down' : null;
                    flashElement(bestPnlEl, direction);
                }
            }
            if(bestChangeEl){
                const changeRaw = best.rangeChangePct ?? best.changePct ?? 0;
                const changeVal = Number(changeRaw);
                const hasChange = Number.isFinite(changeVal);
                bestChangeEl.textContent = hasChange ? `${changeVal >= 0 ? '+' : ''}${changeVal.toFixed(2)}%` : '—';
                if(bestChanged && hasChange){
                    flashElement(bestChangeEl, changeVal >= 0 ? 'up' : 'down');
                }else if(hasChange && previousBestPerformer.change !== null){
                    const direction = changeVal > previousBestPerformer.change ? 'up' : changeVal < previousBestPerformer.change ? 'down' : null;
                    flashElement(bestChangeEl, direction);
                }
                previousBestPerformer.change = hasChange ? changeVal : null;
            }
            previousBestPerformer.id = best.id;
            previousBestPerformer.pnl = bestCandidate.value;
        }else{
            if(bestNameEl) bestNameEl.textContent = '—';
            if(bestPnlEl) bestPnlEl.textContent = '—';
            if(bestChangeEl) bestChangeEl.textContent = '—';
            if(bestMetaEl) bestMetaEl.textContent = 'Awaiting data…';
            previousBestPerformer.id = null;
            previousBestPerformer.pnl = null;
            previousBestPerformer.change = null;
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

    const finnhubSymbols = Array.from(symbolSet);
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
                    applySnapshotResults(results);
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
    ensureTransactionModalElements();
    ensureNetWorthDetailModalElements();
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

    document.querySelectorAll('details[data-chart]').forEach(section=>{
        section.addEventListener('toggle', ()=>{
            if(!section.open) return;
            const chartId = section.getAttribute('data-chart');
            if(!chartId) return;
            requestAnimationFrame(()=>{
                const chart = charts[chartId];
                if(chart){
                    chart.resize();
                    chart.update('none');
                }
            });
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
        if(loadingOverlay){
            setLoadingState('hidden');
        }
    }).catch(err=>{
        console.error('Bootstrap failed', err);
        setStatus('Bootstrap failed — see console');
        if(loadingOverlay){
            setLoadingState('error','Bootstrap failed — see console');
        }
    });
});
