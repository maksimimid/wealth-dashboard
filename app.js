// ------------------ CONFIG ------------------
const DEFAULT_CONFIG = {
    FINNHUB_KEY: '',
    AIRTABLE_API_KEY: '',
    AIRTABLE_BASE_ID: 'appSxixo1i122KyBS',
    AIRTABLE_TABLE_NAME: 'Operations'
};

const RUNTIME_CONFIG = (typeof window !== 'undefined' && window.DASHBOARD_CONFIG) ? window.DASHBOARD_CONFIG : {};

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
let sparkData = [];
let ws;
let lastUpdated = null;
let operationsMeta = {count: 0, fetchedAt: null};
const UI_REFRESH_INTERVAL = 5000;
let lastRenderAt = 0;
let scheduledRender = null;
let pnlRange = '1D';
let currentRangeTotalPnl = 0;
let rangeDirty = true;
const referencePriceCache = new Map();
const RANGE_LOOKBACK = { '1W': 7*86400, '1M': 30*86400, '1Y': 365*86400 };
const RANGE_LABELS = { '1D': 'Daily', '1W': 'Weekly', '1M': 'Monthly', '1Y': 'Yearly', 'ALL': 'All Time' };
const previousKpiValues = { totalPnl: null, netWorth: null, cashAvailable: null };
let isRangeUpdateInFlight = false;
const FLASH_DURATION = 900;

function applyRangeButtons(range){
    const buttons = document.querySelectorAll('#pnl-range-controls button');
    buttons.forEach(btn=>{
        btn.classList.toggle('active', btn.dataset.range === range);
    });
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

function formatQty(qty){
    if(qty===null || qty===undefined) return '—';
    const abs = Math.abs(qty);
    if(abs >= 1000) return qty.toFixed(0);
    if(abs >= 100) return qty.toFixed(1);
    if(abs >= 10) return qty.toFixed(2);
    return qty.toFixed(4).replace(/0+$/,'').replace(/\.$/,'');
}

function ensurePositionDefaults(position){
    if(position.displayPrice === undefined){
        const base = position.currentPrice ?? position.lastKnownPrice ?? position.lastPurchasePrice ?? position.avgPrice ?? 0;
        position.displayPrice = base;
    }
    if(position.priceDirection === undefined){
        position.priceDirection = null;
    }
}

function applyLivePrice(position, price){
    if(price === undefined || price === null) return;
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
}

function flashElement(element, direction){
    if(!element || !direction) return;
    element.classList.remove('flash-up', 'flash-down');
    void element.offsetWidth; // force reflow
    requestAnimationFrame(()=>{
        element.classList.add(direction === 'up' ? 'flash-up' : 'flash-down');
        setTimeout(()=>{
            element.classList.remove('flash-up', 'flash-down');
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
        const pnl = unrealized + realized;
        const baseValue = base * qty;
        const denominator = baseValue !== 0 ? baseValue : Math.abs(position.invested) || Math.abs(realized) || 1;
        position.rangePnl = pnl;
        position.rangeChangePct = denominator ? (pnl / denominator) * 100 : 0;
        if(prevRange !== null){
            position.rangeDirection = pnl > prevRange ? 'up' : pnl < prevRange ? 'down' : null;
        }else{
            position.rangeDirection = null;
        }
        total += pnl;
    });
    currentRangeTotalPnl = total;
    rangeDirty = false;
}

async function setPnlRange(range){
    if(isRangeUpdateInFlight || range === pnlRange) return;
    isRangeUpdateInFlight = true;
    try{
        pnlRange = range;
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
    const map = { 'crypto':'Crypto', 'stock':'Stock', 'real estate':'Real Estate', 'cash':'Cash', 'deposit':'Cash' };
    const key = String(category).toLowerCase();
    return map[key] || category;
}

function mapFinnhubSymbol(asset, category){
    if(!asset) return null;
    const upper = asset.toUpperCase();
    const cat = (category||'').toLowerCase();
    if(cat==='crypto'){
        if(upper==='BTC') return 'BINANCE:BTCUSDT';
        if(upper==='ETH') return 'BINANCE:ETHUSDT';
    }
    if(cat==='cash') return null;
    if(/[A-Z0-9]{1,5}/.test(upper) && !upper.includes(' ')) return upper;
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
async function fetchAllAirtableOperations(){
    if(!AIRTABLE_API_KEY) throw new Error('Missing Airtable API key');
    const records = [];
    let offset = '';
    do{
        const params = new URLSearchParams();
        params.append('pageSize','100');
        params.append('sort[0][field]','Date');
        params.append('sort[0][direction]','asc');
        if(offset) params.append('offset', offset);
        const url = `${AIRTABLE_URL}?${params.toString()}`;
        const res = await fetch(url,{headers:{Authorization:`Bearer ${AIRTABLE_API_KEY}`}});
        if(!res.ok){
            const text = await res.text();
            throw new Error(`Airtable responded ${res.status}: ${text}`);
        }
        const json = await res.json();
        records.push(...(json.records||[]));
        offset = json.offset;
    }while(offset);
    return records;
}

function transformOperations(records){
    const ordered = sortByDateAscending(records);
    const map = new Map();
    ordered.forEach(rec=>{
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

        if(!map.has(asset)){
            const finnhubSymbol = mapFinnhubSymbol(asset, category);
            map.set(asset, {
                id: asset,
                Name: asset,
                displayName: asset,
                Category: category,
                type: category,
                Symbol: finnhubSymbol || asset,
                finnhubSymbol,
                qty: 0,
                costBasis: 0,
                invested: 0,
                realized: 0,
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
            tags: fields.Tags || []
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
            entry.realized += spent;
            entry.cashflow += spent;
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
    });

    return Array.from(map.values()).map(p=>{
        if(p.Category === 'Cash'){
            p.displayName = 'Cash Reserve';
        }
        if(!p.lastKnownPrice){
            p.lastKnownPrice = p.lastPurchasePrice || p.avgPrice || 0;
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
        return p;
    });
}

async function loadPositions(){
    symbolSet = new Set();
    finnhubIndex = new Map();
    try{
        setStatus('Loading Airtable operations…');
        const records = await fetchAllAirtableOperations();
        operationsMeta = {count: records.length, fetchedAt: new Date()};
        positions = transformOperations(records);
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
        positions.forEach(p=>{
            if(p.finnhubSymbol){
                symbolSet.add(p.finnhubSymbol);
                finnhubIndex.set(p.finnhubSymbol, p);
            }
        });
    }
    rangeDirty = true;
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
    scheduleUIUpdate();
}

// ----------------- RENDER CHARTS -----------------
function createOrUpdateChart(id,type,data,options){
    if(charts[id]){
        charts[id].data = data;
        charts[id].options = Object.assign(charts[id].options||{},options||{});
        charts[id].update();
        return charts[id];
    }
    const ctx = document.getElementById(id).getContext('2d');
    const cfg = {type, data, options: Object.assign({responsive:true,maintainAspectRatio:false,animation:{duration:600},plugins:{legend:{display:true}},layout:{padding:12}}, options||{}) };
    charts[id] = new Chart(ctx, cfg);
    return charts[id];
}

function renderAllCharts(){
    if(!positions.length) return;
    const sortedForPnl = [...positions].sort((a,b)=> (b.rangePnl||0)-(a.rangePnl||0));
    const labels = sortedForPnl.map(p=>p.displayName || p.Symbol || p.Name);
    const pnlData = sortedForPnl.map(p=>Number((p.rangePnl||0).toFixed(2)));
    const pnlColors = pnlData.map(v=> v>=0 ? 'rgba(52,211,153,0.85)' : 'rgba(248,113,113,0.85)');
    const rangeLabel = RANGE_LABELS[pnlRange] || pnlRange;
    createOrUpdateChart('pnlChart','bar',{labels,datasets:[{label:`P&L (${rangeLabel})`,data:pnlData,borderRadius:6,backgroundColor:pnlColors}]},{indexAxis:'x',scales:{y:{beginAtZero:true}}});

    const compLabels = positions.map(p=>p.displayName || p.Symbol || p.Name);
    const compData = positions.map(p=>Math.max(0, Number(p.marketValue||0)));
    createOrUpdateChart('compositionChart','doughnut',{labels:compLabels,datasets:[{data:compData,backgroundColor:compLabels.map((_,i)=>`hsla(${(i*47)%360},70%,60%,0.85)`)}]},{plugins:{legend:{position:'bottom'}}});

    const changeLabels = positions.map(p=>p.displayName || p.Symbol || p.Name);
    const changeData = positions.map(p=>Number((p.changePct||0).toFixed(2)));
    createOrUpdateChart('dailyChangeChart','bar',{labels:changeLabels,datasets:[{label:'Daily Change (%)',data:changeData,backgroundColor:changeData.map(v=>v>=0? 'rgba(52,211,153,0.85)':'rgba(248,113,113,0.85)')}]},{indexAxis:'y',scales:{x:{ticks:{callback:v=>v+'%'}}}});

    const byType = positions.reduce((acc,p)=>{
        const key = p.type || 'Other';
        acc[key] = (acc[key]||0) + Number(p.marketValue||0);
        return acc;
    },{});
    createOrUpdateChart('typeAllocationChart','pie',{labels:Object.keys(byType),datasets:[{data:Object.values(byType)}]},{plugins:{legend:{position:'bottom'}}});

    sparkData = sparkData.slice(-40);
    const sparkCfg = {labels: sparkData.map((_,i)=>i+1), datasets:[{data:sparkData,fill:true,tension:0.35,borderColor:'rgba(96,165,250,0.8)',backgroundColor:'rgba(96,165,250,0.18)'}]};
    createOrUpdateChart('sparkline','line',sparkCfg,{plugins:{legend:{display:false}},scales:{x:{display:false},y:{display:false}},elements:{point:{radius:0}}});
}

// ----------------- UI -----------------
function setStatus(s){ const el = document.getElementById('status'); if(el) el.textContent = s; }

function updateAssetsList(){
    const el = document.getElementById('assets-list');
    if(!el) return;
    el.innerHTML = '';
    const sorted = [...positions].sort((a,b)=> (b.marketValue||0) - (a.marketValue||0));
    sorted.forEach(p=>{
        ensurePositionDefaults(p);
        const row = document.createElement('div');
        row.className = 'row';
        const changeBadge = Number.isFinite(p.rangeChangePct) && p.rangeChangePct !== 0 ? `${p.rangeChangePct>=0?'+':''}${p.rangeChangePct.toFixed(2)}%` : '—';
        const priceValue = money(p.displayPrice ?? p.currentPrice ?? p.avgPrice ?? 0);
        row.innerHTML = `
            <div>
                <div class="symbol">${p.displayName || p.Symbol || p.Name}</div>
                <div class="pos" style="font-size:13px">${p.type || '—'} · Qty ${formatQty(Number(p.qty||0))}</div>
            </div>
            <div class="row-right">
                <div class="price-row"><span class="price-label">Price</span><span class="price-value">${priceValue}</span></div>
                <div class="value-strong">${money(p.marketValue)}</div>
                <div class="pos">P&L <span class="pnl-value">${money(p.rangePnl ?? p.pnl ?? 0)}</span> · ${changeBadge}</div>
            </div>`;
        el.appendChild(row);
        const priceEl = row.querySelector('.price-value');
        if(priceEl){
            flashElement(priceEl, p.priceDirection);
        }
        const valueEl = row.querySelector('.value-strong');
        if(valueEl){
            flashElement(valueEl, p.marketDirection);
        }
        const pnlEl = row.querySelector('.pnl-value');
        if(pnlEl){
            flashElement(pnlEl, p.rangeDirection);
        }
        p.priceDirection = null;
        p.marketDirection = null;
        p.rangeDirection = null;
    });
}

function updateKpis(){
    positions.forEach(recomputePositionMetrics);
    const totalPnl = currentRangeTotalPnl;
    const totalEquity = positions.reduce((sum,p)=>sum + Number(p.marketValue||0),0);
    const cashAvailable = positions.filter(p=> (p.type||'').toLowerCase()==='cash').reduce((sum,p)=>sum + Number(p.marketValue||0),0);
    const buyingPower = totalEquity * 0.3;

    const updatedEl = document.getElementById('last-updated');

    setMoneyWithFlash('total-pnl', totalPnl, 'totalPnl');
    setMoneyWithFlash('equity', totalEquity, 'equity');
    setMoneyWithFlash('buying-power', buyingPower, 'buyingPower');
    if(updatedEl) updatedEl.textContent = lastUpdated ? lastUpdated.toLocaleTimeString() : '—';

    if(totalPnl || sparkData.length===0){
        const rounded = Number(totalPnl.toFixed(2));
        if(!sparkData.length || sparkData[sparkData.length-1] !== rounded){
            sparkData.push(rounded);
        }
        if(sparkData.length > 60) sparkData.shift();
    }
}

function renderDashboard(){
    updateKpis();
    updateAssetsList();
    renderAllCharts();
    const opsText = operationsMeta.count ? ` · ${operationsMeta.count} Airtable ops` : '';
    setStatus(`Live ${new Date().toLocaleTimeString()}${opsText}`);
    const rangeLabelEl = document.getElementById('pnl-range-label');
    if(rangeLabelEl){
        rangeLabelEl.textContent = RANGE_LABELS[pnlRange] || pnlRange;
    }
    applyRangeButtons(pnlRange);
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

// ----------------- BOOTSTRAP LOGIC -----------------
async function bootstrap(){
    await loadPositions();

    const finnhubSymbols = Array.from(symbolSet);
    if(finnhubSymbols.length){
        for(let i=0;i<finnhubSymbols.length;i+=MAX_REST_BATCH){
            const batch = finnhubSymbols.slice(i, i + MAX_REST_BATCH);
            const results = await fetchSnapshotBatch(batch);
            applySnapshotResults(results);
            await new Promise(resolve=>setTimeout(resolve,120));
        }
    }

    positions.forEach(recomputePositionMetrics);
    lastUpdated = new Date();
    scheduleUIUpdate({immediate:true});
    initFinnhubWS();
}

// ---------- INTERACTIONS ----------
document.addEventListener('DOMContentLoaded', ()=>{
    const sortBtn = document.getElementById('sort-pnl');
    if(sortBtn){
        let pnlSortDesc = true;
        sortBtn.addEventListener('click', ()=>{
            pnlSortDesc = !pnlSortDesc;
            positions.sort((a,b)=> pnlSortDesc ? (b.rangePnl||0)-(a.rangePnl||0) : (a.rangePnl||0)-(b.rangePnl||0));
            scheduleUIUpdate({immediate:true});
            sortBtn.textContent = pnlSortDesc ? 'Sort: High → Low' : 'Sort: Low → High';
        });
    }

    const pnlRangeControls = document.getElementById('pnl-range-controls');
    if(pnlRangeControls){
        pnlRangeControls.addEventListener('click', event=>{
            const button = event.target.closest('button[data-range]');
            if(!button) return;
            const selected = button.dataset.range;
            if(!selected) return;
            applyRangeButtons(selected);
            setPnlRange(selected).catch(err=>console.error('Failed to update P&L range', err));
        });
    }

    const exportBtn = document.getElementById('export-csv');
    if(exportBtn){
        exportBtn.addEventListener('click', ()=>{
            const rows = [['Display','Symbol','Category','Qty','AvgPrice','CurrentPrice','MarketValue','P&L','Change%']];
            positions.forEach(p=>{
                rows.push([
                    p.displayName || p.Name,
                    p.finnhubSymbol || p.Symbol || '',
                    p.type || '',
                    Number(p.qty||0),
                    Number(p.avgPrice||0),
                    Number(p.currentPrice||0),
                    Number(p.marketValue||0),
                    Number((p.rangePnl ?? p.pnl) || 0),
                    Number((p.rangeChangePct ?? p.changePct) || 0)
                ]);
            });
            const csv = rows.map(r=>r.map(cell => `"${String(cell??'').replace(/"/g,'""')}"`).join(',')).join('\n');
            const blob = new Blob([csv],{type:'text/csv'});
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'wealth-dashboard.csv';
            a.click();
            URL.revokeObjectURL(url);
        });
    }

    const themeToggle = document.getElementById('theme-toggle');
    if(themeToggle){
        themeToggle.addEventListener('click', ()=>{
            const isLight = document.body.classList.toggle('light-theme');
            themeToggle.textContent = isLight ? 'Dark Theme' : 'Light Theme';
        });
    }

    let resizeTimer;
    window.addEventListener('resize', ()=>{
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(()=>{
            Object.values(charts).forEach(chart=>chart && chart.resize());
        }, 160);
    });

    bootstrap().catch(err=>{
        console.error('Bootstrap failed', err);
        setStatus('Bootstrap failed — see console');
    });
});
