"""
MongoDB manager — stores Close prices for all stocks.
First run: downloads 200 days in batches of 20 (low memory).
Daily: fetches only last 7 days and merges.
"""
import os, gc, logging
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf

log = logging.getLogger('DB_MGR')
_col = None  # MongoDB collection (lazy init)

MONGO_URI = os.environ.get('MONGODB_URI', '')
LOOKBACK  = 200  # days of history to keep


def _get_col():
    global _col
    if _col is None:
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
        _col = client['rs_rsi_bot']['closes']
        log.info("MongoDB connected.")
    return _col


def _save(sym, series: pd.Series):
    """Store a Close Series for one symbol."""
    series = series.sort_index().tail(LOOKBACK)
    _get_col().update_one(
        {'_id': sym},
        {'$set': {
            'dates':  [str(d.date()) for d in series.index],
            'closes': [round(float(v), 4) for v in series.values]
        }},
        upsert=True
    )


def _load(sym) -> pd.Series:
    doc = _get_col().find_one({'_id': sym}, {'dates': 1, 'closes': 1})
    if not doc:
        return pd.Series(dtype=float)
    return pd.Series(
        doc['closes'],
        index=pd.to_datetime(doc['dates']),
        dtype=float
    )


def get_last_update() -> str:
    doc = _get_col().find_one({'_id': '__meta__'})
    return doc['last_update'] if doc else ''


def _set_last_update(date_str: str):
    _get_col().update_one(
        {'_id': '__meta__'},
        {'$set': {'last_update': date_str}},
        upsert=True
    )


def _fetch_and_merge(symbols, start, is_init=False):
    """Download close prices in batches of 20 and merge into MongoDB."""
    BATCH = 20
    batches = [symbols[i:i+BATCH] for i in range(0, len(symbols), BATCH)]
    ok = 0
    for b_idx, batch in enumerate(batches):
        tickers = [s + '.NS' for s in batch]
        try:
            raw = yf.download(
                tickers, start=start,
                auto_adjust=True, progress=False, group_by='ticker'
            )
            for sym in batch:
                try:
                    if isinstance(raw.columns, pd.MultiIndex):
                        new_s = raw[sym + '.NS']['Close'].dropna()
                    else:
                        new_s = raw['Close'].dropna()
                    if len(new_s) < 3:
                        continue
                    if not is_init:
                        existing = _load(sym)
                        if not existing.empty:
                            new_s = pd.concat([existing, new_s])
                            new_s = new_s[~new_s.index.duplicated(keep='last')]
                    _save(sym, new_s)
                    ok += 1
                except Exception:
                    pass
            del raw
            gc.collect()
        except Exception as e:
            log.error(f"Batch {b_idx+1} error: {e}")
        log.info(f"  Batch {b_idx+1}/{len(batches)} — {ok} symbols stored")
    return ok


def init_or_update(all_symbols, notify_fn=None):
    """
    Call this before every scan.
    First time: downloads LOOKBACK days for all symbols (slow, ~5 min).
    After that:  fetches only last 10 calendar days and merges (fast, ~1 min).
    """
    today = datetime.utcnow().strftime('%Y-%m-%d')
    last  = get_last_update()

    if last == today:
        if notify_fn:
            notify_fn("✅ DB already up to date for today.")
        return

    is_init  = (last == '')
    days     = LOOKBACK + 10 if is_init else 12
    start    = (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%d')
    label    = f"{'INIT (200 days)' if is_init else 'daily update'}"

    log.info(f"DB {label}: {len(all_symbols)} symbols from {start}")
    if notify_fn:
        notify_fn(f"📦 DB {label} — fetching {len(all_symbols)} stocks in batches of 20...")

    # Nifty index
    try:
        ni = yf.download('^NSEI', start=start, auto_adjust=True, progress=False)
        if not ni.empty:
            s = ni['Close'].dropna()
            if not is_init:
                existing = _load('__NSEI__')
                if not existing.empty:
                    s = pd.concat([existing, s])
                    s = s[~s.index.duplicated(keep='last')]
            _save('__NSEI__', s)
    except Exception as e:
        log.error(f"Nifty DB error: {e}")

    ok = _fetch_and_merge(all_symbols, start, is_init=is_init)
    _set_last_update(today)

    if notify_fn:
        notify_fn(f"✅ DB updated — {ok}/{len(all_symbols)} stocks stored.")


def load_universe(symbols, rs_period=70):
    """Load nifty + stock Close data from MongoDB for signal computation."""
    nifty = _load('__NSEI__')

    data = {}
    for sym in symbols:
        s = _load(sym)
        if len(s) >= rs_period + 5:
            data[sym] = pd.DataFrame({'Close': s})

    log.info(f"Loaded from DB: {len(data)}/{len(symbols)} stocks")
    return nifty, data
