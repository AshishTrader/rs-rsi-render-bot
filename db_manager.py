"""
MongoDB manager — persistent Close price storage.
Uses individual yf.Ticker downloads (constant memory, no OOM).
4 threads for speed: ~2-3 min for 550 stocks.
"""
import os, gc, logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import yfinance as yf

log      = logging.getLogger('DB_MGR')
_col     = None
MONGO_URI = os.environ.get('MONGODB_URI', '')
LOOKBACK  = 100   # trading-day history to keep (RS=70 + 30 buffer)
THREADS   = 5     # parallel downloads


# ── MongoDB helpers ──────────────────────────────────────────────────────────

def _get_col():
    global _col
    if _col is None:
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
        _col = client['rs_rsi_bot']['closes']
        log.info("MongoDB connected.")
    return _col


def _save(sym: str, s: pd.Series):
    s = s.dropna().sort_index().tail(LOOKBACK)
    if s.empty:
        return
    _get_col().update_one(
        {'_id': sym},
        {'$set': {'dates':  [str(d.date()) for d in s.index],
                  'closes': [round(float(v), 4) for v in s.values]}},
        upsert=True
    )


def _load(sym: str) -> pd.Series:
    doc = _get_col().find_one({'_id': sym}, {'dates': 1, 'closes': 1})
    if not doc:
        return pd.Series(dtype=float)
    return pd.Series(doc['closes'],
                     index=pd.to_datetime(doc['dates']),
                     dtype=float)


def get_last_update() -> str:
    doc = _get_col().find_one({'_id': '__meta__'})
    return doc['last_update'] if doc else ''


def _set_last_update(d: str):
    _get_col().update_one({'_id': '__meta__'},
                          {'$set': {'last_update': d}}, upsert=True)


# ── Download one stock (called in thread) ───────────────────────────────────

def _dl_one(sym: str, start: str, existing: pd.Series) -> tuple:
    """Download a single ticker; returns (sym, close_series)."""
    try:
        t = yf.Ticker(sym + '.NS')
        hist = t.history(start=start, auto_adjust=True)
        if hist.empty or 'Close' not in hist.columns:
            return sym, pd.Series(dtype=float)
        new_s = hist['Close'].dropna()
        new_s.index = new_s.index.tz_localize(None)
        if not existing.empty:
            new_s = pd.concat([existing, new_s])
            new_s = new_s[~new_s.index.duplicated(keep='last')]
        return sym, new_s
    except Exception as e:
        log.debug(f"{sym} download error: {e}")
        return sym, pd.Series(dtype=float)


# ── Main public function ─────────────────────────────────────────────────────

def init_or_update(all_symbols: list, notify_fn=None):
    """
    First run  → downloads LOOKBACK+10 calendar days for every symbol.
    Later runs → downloads last 12 days and merges.
    """
    today = datetime.utcnow().strftime('%Y-%m-%d')
    last  = get_last_update()

    if last == today:
        if notify_fn:
            notify_fn("✅ DB already up to date.")
        return

    is_init = (last == '')
    days    = LOOKBACK * 2 if is_init else 14   # 200 cal-days covers ~100 trading-days
    start   = (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%d')
    mode    = "INIT (first run)" if is_init else "daily update"
    total   = len(all_symbols)

    log.info(f"DB {mode}: {total} symbols from {start}")
    if notify_fn:
        notify_fn(f"📦 DB {mode} — {total} stocks, {THREADS} threads. "
                  f"{'~3 min' if is_init else '~1 min'}...")

    # Nifty index (sequential, fast)
    try:
        ni = yf.Ticker('^NSEI').history(start=start, auto_adjust=True)
        if not ni.empty:
            s = ni['Close'].dropna()
            s.index = s.index.tz_localize(None)
            if not is_init:
                ex = _load('__NSEI__')
                if not ex.empty:
                    s = pd.concat([ex, s])
                    s = s[~s.index.duplicated(keep='last')]
            _save('__NSEI__', s)
    except Exception as e:
        log.error(f"Nifty error: {e}")

    # Load existing data for incremental merge
    existing = {} if is_init else {sym: _load(sym) for sym in all_symbols}

    # Download stocks in parallel
    ok = 0
    done = 0
    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        futs = {ex.submit(_dl_one, sym, start, existing.get(sym, pd.Series(dtype=float))): sym
                for sym in all_symbols}
        for fut in as_completed(futs):
            sym, s = fut.result()
            if len(s) >= 10:
                _save(sym, s)
                ok += 1
            done += 1
            if done % 50 == 0:
                log.info(f"  {done}/{total} done ({ok} saved)")
                if notify_fn:
                    notify_fn(f"⏳ DB {mode}: {done}/{total} stocks done ({ok} saved)...")

    _set_last_update(today)
    log.info(f"DB {mode} complete: {ok}/{total} saved.")
    if notify_fn:
        notify_fn(f"✅ DB {mode} complete — {ok}/{total} stocks stored.")


# ── Load universe for signal computation ────────────────────────────────────

def load_universe(symbols: list, rs_period: int = 70):
    """Load Close data from MongoDB — fast, low memory."""
    nifty = _load('__NSEI__')
    data  = {}
    syms  = [s['_id'] for s in _get_col().find(
        {'_id': {'$in': symbols}}, {'dates': 1, 'closes': 1})]
    # Actually load full docs
    for doc in _get_col().find({'_id': {'$in': symbols}}, {'dates': 1, 'closes': 1}):
        sym = doc['_id']
        s   = pd.Series(doc['closes'],
                        index=pd.to_datetime(doc['dates']),
                        dtype=float)
        if len(s) >= rs_period + 5:
            data[sym] = pd.DataFrame({'Close': s})
    log.info(f"Loaded from DB: {len(data)}/{len(symbols)} stocks")
    return nifty, data
