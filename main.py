"""
RS + RSI Telegram Bot — Render Cloud Edition
- Auto-runs at 3:15 PM IST every weekday
- /run command triggers immediate scan
- All results sent to Telegram
- No state persistence, no Angel One, no Excel
"""
import os, gc, logging, threading, time, requests
import db_manager
from datetime import datetime, timedelta, date
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import pandas as pd
import numpy as np
import yfinance as yf
import warnings
warnings.filterwarnings('ignore')

# ── CONFIG FROM ENV VARS ─────────────────────────────────────
BOT_TOKEN   = os.environ.get('TELEGRAM_BOT_TOKEN', '')
CHAT_ID     = os.environ.get('TELEGRAM_CHAT_ID', '')
RENDER_URL  = os.environ.get('RENDER_URL', '')  # e.g. https://your-app.onrender.com

RS_P        = int(os.environ.get('RS_PERIOD', 70))
RSI_L       = int(os.environ.get('RSI_LENGTH', 5))
RSI_T       = float(os.environ.get('RSI_THRESHOLD', 40))
SMA_L       = int(os.environ.get('SMA_LENGTH', 15))
TOP_N       = int(os.environ.get('TOP_N', 5))
SMA_ENT     = os.environ.get('SMA_ENTRY_FILTER', 'true').lower() == 'true'
REBAL_N     = int(os.environ.get('REBAL_DAYS', 5))
CAP_N200    = float(os.environ.get('CAPITAL_N200', 50000))
CAP_N500    = float(os.environ.get('CAPITAL_N500', 50000))
UNIVERSE    = os.environ.get('UNIVERSE', 'BOTH')  # N200, N500, or BOTH

IST = pytz.timezone('Asia/Kolkata')

# ── LOGGING ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
log = logging.getLogger('RS_RSI_BOT')

# ── UNIVERSES ────────────────────────────────────────────────
NIFTY200 = (
    "360ONE,ABB,APLAPOLLO,AUBANK,ADANIENSOL,ADANIENT,ADANIGREEN,ADANIPORTS,ADANIPOWER,ATGL,ABCAPITAL,ALKEM,"
    "AMBUJACEM,APOLLOHOSP,ASHOKLEY,ASIANPAINT,ASTRAL,AUROPHARMA,DMART,AXISBANK,BSE,BAJAJ-AUTO,BAJFINANCE,"
    "BAJAJFINSV,BAJAJHLDNG,BANKBARODA,BANKINDIA,BDL,BEL,BHARATFORG,BHEL,BPCL,BHARTIARTL,GROWW,BIOCON,"
    "BLUESTARCO,BOSCHLTD,BRITANNIA,CGPOWER,CANBK,CHOLAFIN,CIPLA,COALINDIA,COCHINSHIP,COFORGE,COLPAL,CONCOR,"
    "COROMANDEL,CUMMINSIND,DLF,DABUR,DIVISLAB,DIXON,DRREDDY,DUMMYVEDL1,DUMMYVEDL2,DUMMYVEDL3,DUMMYVEDL4,"
    "EICHERMOT,ETERNAL,EXIDEIND,NYKAA,FEDERALBNK,FORTIS,GAIL,GVT&D,GMRAIRPORT,GLENMARK,GODFRYPHLP,GODREJCP,"
    "GODREJPROP,GRASIM,HCLTECH,HDFCAMC,HDFCBANK,HDFCLIFE,HAVELLS,HEROMOTOCO,HINDALCO,HAL,HINDPETRO,HINDUNILVR,"
    "HINDZINC,POWERINDIA,HUDCO,HYUNDAI,ICICIBANK,ICICIGI,ICICIAMC,IDFCFIRSTB,ITC,INDIANB,INDHOTEL,IOC,IRCTC,"
    "IRFC,IREDA,INDUSTOWER,INDUSINDBK,NAUKRI,INFY,INDIGO,JSWENERGY,JSWSTEEL,JINDALSTEL,JIOFIN,JUBLFOOD,KEI,"
    "KPITTECH,KALYANKJIL,KOTAKBANK,LTF,LGEINDIA,LICHSGFIN,LTM,LT,LAURUSLABS,LENSKART,LODHA,LUPIN,MRF,M&MFIN,"
    "M&M,MANKIND,MARICO,MARUTI,MFSL,MAXHEALTH,MAZDOCK,MOTILALOFS,MPHASIS,MCX,MUTHOOTFIN,NHPC,NMDC,NTPC,"
    "NATIONALUM,NESTLEIND,OBEROIRLTY,ONGC,OIL,PAYTM,OFSS,POLICYBZR,PIIND,PAGEIND,PATANJALI,PERSISTENT,"
    "PHOENIXLTD,PIDILITIND,POLYCAB,PFC,POWERGRID,PREMIERENE,PRESTIGE,PNB,RECLTD,RADICO,RVNL,RELIANCE,SBICARD,"
    "SBILIFE,SRF,MOTHERSON,SHREECEM,SHRIRAMFIN,ENRIN,SIEMENS,SOLARINDS,SBIN,SAIL,SUNPHARMA,SUPREMEIND,SUZLON,"
    "SWIGGY,TVSMOTOR,TATACAP,TATACOMM,TCS,TATACONSUM,TATAELXSI,TATAINVEST,TMCV,TMPV,TATAPOWER,TATASTEEL,TECHM,"
    "TITAN,TORNTPHARM,TRENT,TIINDIA,UPL,ULTRACEMCO,UNIONBANK,UNITDSPR,VBL,VEDL,VMM,IDEA,VOLTAS,WAAREEENER,"
    "WIPRO,YESBANK,ZYDUSLIFE"
).split(",")

NIFTY500 = (
    "360ONE,3MINDIA,ABB,ACC,ACMESOLAR,AIAENG,APLAPOLLO,AUBANK,AWL,AADHARHFC,AARTIIND,AAVAS,ABBOTINDIA,ACE,"
    "ACUTAAS,ADANIENSOL,ADANIENT,ADANIGREEN,ADANIPORTS,ADANIPOWER,ATGL,ABCAPITAL,ABFRL,ABLBL,ABREL,ABSLAMC,"
    "CPPLUS,AEGISLOG,AEGISVOPAK,AFCONS,AFFLE,AJANTPHARM,ALKEM,ABDL,ARE&M,AMBER,AMBUJACEM,ANANDRATHI,ANANTRAJ,"
    "ANGELONE,ANTHEM,ANURAS,APARINDS,APOLLOHOSP,APOLLOTYRE,APTUS,ASAHIINDIA,ASHOKLEY,ASIANPAINT,ASTERDM,"
    "ASTRAL,ATHERENERG,ATUL,AUROPHARMA,AIIL,DMART,AXISBANK,BEML,BLS,BSE,BAJAJ-AUTO,BAJFINANCE,BAJAJFINSV,"
    "BAJAJHLDNG,BAJAJHFL,BALKRISIND,BALRAMCHIN,BANDHANBNK,BANKBARODA,BANKINDIA,MAHABANK,BATAINDIA,BAYERCROP,"
    "BELRISE,BERGEPAINT,BDL,BEL,BHARATFORG,BHEL,BPCL,BHARTIARTL,BHARTIHEXA,BIKAJI,GROWW,BIOCON,BSOFT,BLUEDART,"
    "BLUEJET,BLUESTARCO,BBTC,BOSCHLTD,FIRSTCRY,BRIGADE,BRITANNIA,MAPMYINDIA,CCL,CESC,CGPOWER,CRISIL,"
    "CANFINHOME,CANBK,CANHLIFE,CAPLIPOINT,CGCL,CARBORUNIV,CARTRADE,CASTROLIND,CEATLTD,CEMPRO,CENTRALBK,CDSL,"
    "CHALET,CHAMBLFERT,CHENNPETRO,CHOICEIN,CHOLAHLDNG,CHOLAFIN,CIPLA,CUB,CLEAN,COALINDIA,COCHINSHIP,COFORGE,"
    "COHANCE,COLPAL,CAMS,CONCORDBIO,CONCOR,COROMANDEL,CRAFTSMAN,CREDITACC,CROMPTON,CUMMINSIND,CYIENT,"
    "DCMSHRIRAM,DLF,DOMS,DABUR,DALBHARAT,DATAPATTNS,DEEPAKFERT,DEEPAKNTR,DELHIVERY,DEVYANI,DIVISLAB,DIXON,"
    "LALPATHLAB,DRREDDY,DUMMYVEDL1,DUMMYVEDL2,DUMMYVEDL3,DUMMYVEDL4,EIDPARRY,EIHOTEL,EICHERMOT,ELECON,"
    "ELGIEQUIP,EMAMILTD,EMCURE,EMMVEE,ENDURANCE,ENGINERSIN,ERIS,ESCORTS,ETERNAL,EXIDEIND,NYKAA,FEDERALBNK,"
    "FACT,FINCABLES,FSL,FIVESTAR,FORCEMOT,FORTIS,GAIL,GVT&D,GMRAIRPORT,GABRIEL,GALLANTT,GRSE,GICRE,GILLETTE,"
    "GLAND,GLAXO,GLENMARK,MEDANTA,GODIGIT,GPIL,GODFRYPHLP,GODREJCP,GODREJIND,GODREJPROP,GRANULES,GRAPHITE,"
    "GRASIM,GRAVITA,GESHIP,FLUOROCHEM,GMDCLTD,GSPL,HEG,HBLENGINE,HCLTECH,HDBFS,HDFCAMC,HDFCBANK,HDFCLIFE,HFCL,"
    "HAVELLS,HEROMOTOCO,HEXT,HSCL,HINDALCO,HAL,HINDCOPPER,HINDPETRO,HINDUNILVR,HINDZINC,POWERINDIA,HOMEFIRST,"
    "HONASA,HONAUT,HUDCO,HYUNDAI,ICICIBANK,ICICIGI,ICICIAMC,ICICIPRULI,IDBI,IDFCFIRSTB,IFCI,IIFL,IRB,IRCON,"
    "ITCHOTELS,ITC,ITI,INDGN,INDIACEM,INDIAMART,INDIANB,IEX,INDHOTEL,IOC,IOB,IRCTC,IRFC,IREDA,IGL,INDUSTOWER,"
    "INDUSINDBK,NAUKRI,INFY,INOXWIND,INTELLECT,INDIGO,IGIL,IKS,IPCALAB,JBCHEPHARM,JKCEMENT,JBMA,JKTYRE,"
    "JMFINANCIL,JSWCEMENT,JSWDULUX,JSWENERGY,JSWINFRA,JSWSTEEL,JAINREC,JPPOWER,J&KBANK,JINDALSAW,JSL,"
    "JINDALSTEL,JIOFIN,JUBLFOOD,JUBLINGREA,JUBLPHARMA,JWL,JYOTICNC,KPRMILL,KEI,KPITTECH,KAJARIACER,KPIL,"
    "KALYANKJIL,KARURVYSYA,KAYNES,KEC,KFINTECH,KIRLOSENG,KOTAKBANK,KIMS,LTF,LTTS,LGEINDIA,LICHSGFIN,LTFOODS,"
    "LTM,LT,LATENTVIEW,LAURUSLABS,THELEELA,LEMONTREE,LENSKART,LICI,LINDEINDIA,LLOYDSME,LODHA,LUPIN,MMTC,MRF,"
    "MGL,M&MFIN,M&M,MANAPPURAM,MRPL,MANKIND,MARICO,MARUTI,MFSL,MAXHEALTH,MAZDOCK,MEESHO,MINDACORP,MSUMI,"
    "MOTILALOFS,MPHASIS,MCX,MUTHOOTFIN,NATCOPHARM,NBCC,NCC,NHPC,NLCINDIA,NMDC,NSLNISP,NTPCGREEN,NTPC,NH,"
    "NATIONALUM,NAVA,NAVINFLUOR,NESTLEIND,NETWEB,NEULANDLAB,NEWGEN,NAM-INDIA,NIVABUPA,NUVAMA,NUVOCO,"
    "OBEROIRLTY,ONGC,OIL,OLAELEC,OLECTRA,PAYTM,ONESOURCE,OFSS,POLICYBZR,PCBL,PGEL,PIIND,PNBHOUSING,PTCIL,"
    "PVRINOX,PAGEIND,PARADEEP,PATANJALI,PERSISTENT,PETRONET,PFIZER,PHOENIXLTD,PWL,PIDILITIND,PINELABS,"
    "PIRAMALFIN,PPLPHARMA,POLYMED,POLYCAB,POONAWALLA,PFC,POWERGRID,PREMIERENE,PRESTIGE,PNB,RRKABEL,RBLBANK,"
    "RECLTD,RHIM,RITES,RADICO,RVNL,RAILTEL,RAINBOW,RKFORGE,REDINGTON,RELIANCE,RPOWER,SBFC,SBICARD,SBILIFE,"
    "SJVN,SRF,SAGILITY,SAILIFE,SAMMAANCAP,MOTHERSON,SAPPHIRE,SARDAEN,SAREGAMA,SCHAEFFLER,SCHNEIDER,SCI,"
    "SHREECEM,SHRIRAMFIN,SHYAMMETL,ENRIN,SIEMENS,SIGNATURE,SOBHA,SOLARINDS,SONACOMS,SONATSOFTW,STARHEALTH,"
    "SBIN,SAIL,SUMICHEM,SUNPHARMA,SUNTV,SUNDARMFIN,SUPREMEIND,SPLPETRO,SUZLON,SWANCORP,SWIGGY,SYNGENE,SYRMA,"
    "TBOTEK,TVSMOTOR,TATACAP,TATACHEM,TATACOMM,TCS,TATACONSUM,TATAELXSI,TATAINVEST,TMCV,TMPV,TATAPOWER,"
    "TATASTEEL,TATATECH,TTML,TECHM,TECHNOE,TEGA,TEJASNET,TENNIND,NIACL,RAMCOCEM,THERMAX,TIMKEN,TITAGARH,TITAN,"
    "TORNTPHARM,TORNTPOWER,TARIL,TRAVELFOOD,TRENT,TRIDENT,TRITURBINE,TIINDIA,UCOBANK,UNOMINDA,UPL,UTIAMC,"
    "ULTRACEMCO,UNIONBANK,UBL,UNITDSPR,URBANCO,USHAMART,VTL,VBL,VEDL,VIJAYA,VMM,IDEA,VOLTAS,WAAREEENER,"
    "WELCORP,WELSPUNLIV,WHIRLPOOL,WIPRO,WOCKPHARMA,YESBANK,ZFCVINDIA,ZEEL,ZENTEC,ZENSARTECH,ZYDUSLIFE,"
    "ZYDUSWELL,ECLERX"
).split(",")

UNIVERSES = {}
if UNIVERSE in ('BOTH', 'N200'):
    UNIVERSES['Nifty200'] = {'stocks': NIFTY200, 'capital': CAP_N200}
if UNIVERSE in ('BOTH', 'N500'):
    UNIVERSES['Nifty500'] = {'stocks': NIFTY500, 'capital': CAP_N500}

# ── IN-MEMORY STATE (resets on restart, that's OK) ───────────
_state = {}
_last_run_time = None
_last_run_summary = "No scan run yet."

# ── TELEGRAM HELPERS ─────────────────────────────────────────
def tg_send(text, chat_id=None):
    """Send a message to Telegram, splitting if > 4000 chars."""
    cid = chat_id or CHAT_ID
    if not BOT_TOKEN or not cid:
        log.warning("Telegram not configured.")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    for chunk in _split_msg(text):
        try:
            requests.post(url, json={
                "chat_id": cid,
                "text": chunk,
                "parse_mode": "Markdown"
            }, timeout=10)
        except Exception as e:
            log.error(f"Telegram send error: {e}")

def _split_msg(text, limit=4000):
    """Split long messages into chunks."""
    lines, chunk, chunks = text.split('\n'), [], []
    for line in lines:
        if sum(len(l) + 1 for l in chunk) + len(line) > limit:
            chunks.append('\n'.join(chunk))
            chunk = []
        chunk.append(line)
    if chunk:
        chunks.append('\n'.join(chunk))
    return chunks or [text]

def tg_answer(chat_id, text):
    """Reply to a specific chat."""
    tg_send(text, chat_id=chat_id)

# ── INDICATORS ───────────────────────────────────────────────
def calc_rsi(s, w):
    d = s.diff()
    g = d.where(d > 0, 0.0).rolling(w).mean()
    l = (-d.where(d < 0, 0.0)).rolling(w).mean()
    rs = g / l.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

# ── DATA FETCH ───────────────────────────────────────────────────────
# Only Close prices needed — saves ~80% memory vs OHLCV on Render 512MB
def fetch_data(universe_stocks):
    lookback_days = max(RS_P * 2, 200)
    start = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
    BATCH = 100  # Safe with Close-only

    # Fetch Nifty index
    nifty = pd.Series(dtype=float)
    try:
        ni = yf.download('^NSEI', start=start, auto_adjust=True, progress=False)
        if not ni.empty:
            nifty = (ni['Close'] if 'Close' in ni.columns else ni.iloc[:, 0]).dropna()
    except Exception as e:
        log.error(f"Nifty fetch error: {e}")

    data = {}
    batches = [universe_stocks[i:i+BATCH] for i in range(0, len(universe_stocks), BATCH)]
    log.info(f"Fetching Close prices for {len(universe_stocks)} stocks in {len(batches)} batches...")

    for b_idx, batch in enumerate(batches):
        tickers = [s + '.NS' for s in batch]
        try:
            raw = yf.download(
                tickers, start=start, auto_adjust=True,
                progress=False, group_by='ticker'
            )
            if isinstance(raw.columns, pd.MultiIndex):
                for sym in batch:
                    try:
                        close = raw[sym + '.NS']['Close'].dropna()
                        if len(close) >= RS_P + 5:
                            data[sym] = pd.DataFrame({'Close': close})
                    except:
                        pass
            elif len(batch) == 1 and not raw.empty:
                close = raw['Close'].dropna()
                if len(close) >= RS_P + 5:
                    data[batch[0]] = pd.DataFrame({'Close': close})
            del raw          # Explicitly free batch memory
            gc.collect()     # Force GC between batches
        except Exception as e:
            log.error(f"Batch {b_idx+1} error: {e}")
        log.info(f"  Batch {b_idx+1}/{len(batches)} done | {len(data)} stocks loaded")

    log.info(f"Fetch complete: {len(data)}/{len(universe_stocks)} stocks | Nifty rows: {len(nifty)}")
    return nifty, data

# ── SIGNAL GENERATION ────────────────────────────────────────
def generate_signals(nifty, data):
    candidates = []
    for sym, df in data.items():
        if len(df) < RS_P + 2:
            continue
        latest_close  = df['Close'].iloc[-1]
        rs_close_start = df['Close'].iloc[-RS_P - 1]
        nifty_now   = nifty.iloc[-1]
        nifty_start = nifty.iloc[-RS_P - 1] if len(nifty) > RS_P else None
        if nifty_start is None or nifty_start == 0 or rs_close_start == 0:
            continue
        rs_score = (latest_close / rs_close_start) / (nifty_now / nifty_start) - 1
        if rs_score <= 0:
            continue
        rsi = calc_rsi(df['Close'], RSI_L)
        recent_rsi_min = rsi.dropna().tail(10).min()
        if pd.isna(recent_rsi_min) or recent_rsi_min > RSI_T:
            continue
        tc, pc = df['Close'].iloc[-1], df['Close'].iloc[-2]
        if tc <= pc:
            continue
        if SMA_ENT:
            sma = df['Close'].rolling(SMA_L).mean().iloc[-1]
            if pd.isna(sma) or tc <= sma:
                continue
        candidates.append({
            'symbol':   sym,
            'rs_score': round(rs_score, 4),
            'rsi_min':  round(recent_rsi_min, 2),
            'close':    round(tc, 2),
            'sma15':    round(df['Close'].rolling(SMA_L).mean().iloc[-1], 2)
        })
    candidates.sort(key=lambda x: x['rs_score'], reverse=True)
    return candidates[:TOP_N]

# ── SMA EXIT CHECK ───────────────────────────────────────────
def check_sma_exits(data, holdings):
    exits = []
    for sym, h in holdings.items():
        if sym not in data:
            continue
        df = data[sym]
        cur = df['Close'].iloc[-1]
        sma = df['Close'].rolling(SMA_L).mean().iloc[-1]
        if not pd.isna(sma) and cur < sma:
            pct = (cur / h['entry_price'] - 1) * 100
            exits.append({'symbol': sym, 'price': cur, 'pnl': pct})
    return exits

# ── CORE RUN ─────────────────────────────────────────────────
def run_scan(triggered_by="scheduler", reply_chat_id=None):
    """Main scan. reply_chat_id: send errors here if global CHAT_ID fails."""
    global _state, _last_run_time, _last_run_summary

    def notify(text):
        """Send to global CHAT_ID; also to reply_chat_id if different."""
        tg_send(text)
        if reply_chat_id and str(reply_chat_id) != str(CHAT_ID):
            tg_send(text, chat_id=reply_chat_id)

    try:
        now_ist = datetime.now(IST)
        today   = now_ist.strftime('%Y-%m-%d')
        log.info(f"=== RS+RSI Scan Started | Triggered by: {triggered_by} ===")

        header = (
            f"🤖 *RS+RSI Bot — Scan Started*\n"
            f"🕒 {now_ist.strftime('%d %b %Y, %I:%M %p IST')}\n"
            f"📊 Params: RS={RS_P} | RSI({RSI_L})<{RSI_T} | SMA={SMA_L} | Top{TOP_N}\n"
            f"{'─' * 30}"
        )
        notify(header)

        all_msgs = []

        # ── UPDATE MONGODB (first run: 200 days; after: 7 days) ──
        all_syms = list(set(sum([v['stocks'] for v in UNIVERSES.values()], [])))
        db_manager.init_or_update(all_syms, notify_fn=notify)

        for uname, ucfg in UNIVERSES.items():
            u_state = _state.setdefault(uname, {
                'holdings': {},
                'last_rebalance': '2000-01-01',
                'capital': ucfg['capital']
            })

            notify(f"⏳ Loading *{uname}* ({len(ucfg['stocks'])} stocks) from DB...")
            nifty, data = db_manager.load_universe(ucfg['stocks'], RS_P)
            notify(f"✅ Loaded {len(data)} stocks for *{uname}*. Running signals...")

            msgs = [f"\n📁 *{uname}*\n{'─'*20}"]

            # ── SMA EXIT CHECK ───────────────────────────────────
            if u_state['holdings']:
                exits = check_sma_exits(data, u_state['holdings'])
                if exits:
                    msgs.append("🛑 *SMA Exits:*")
                    for ex in exits:
                        msgs.append(
                            f"  ❌ *{ex['symbol']}* | Price: ₹{ex['price']:.2f} | PnL: {ex['pnl']:+.2f}%"
                        )
                        del u_state['holdings'][ex['symbol']]
                else:
                    msgs.append("✅ No SMA exits triggered.")

            # ── REBALANCE CHECK ──────────────────────────────────
            last_reb   = u_state['last_rebalance']
            days_since = (date.fromisoformat(today) - date.fromisoformat(last_reb)).days
            do_rebal   = days_since >= REBAL_N

            if do_rebal:
                msgs.append(f"\n🔄 *Rebalance Day* (last: {last_reb})")

                if u_state['holdings']:
                    msgs.append("📤 *Closing all positions:*")
                    for sym, h in list(u_state['holdings'].items()):
                        cur = data[sym]['Close'].iloc[-1] if sym in data else h['entry_price']
                        pct = (cur / h['entry_price'] - 1) * 100
                        msgs.append(f"  🛑 SELL *{sym}* | ₹{cur:.2f} | PnL: {pct:+.2f}%")
                    u_state['holdings'] = {}

                signals = generate_signals(nifty, data)
                log.info(f"  -> {len(signals)} signals for {uname}")

                if signals:
                    alloc = u_state['capital'] / TOP_N
                    msgs.append(f"\n🚀 *New Entries* (Alloc/stock: ₹{alloc:,.0f}):")
                    for sig in signals:
                        qty  = max(1, int(alloc / sig['close']))
                        cost = qty * sig['close']
                        msgs.append(
                            f"  ✅ BUY *{sig['symbol']}*\n"
                            f"     RS={sig['rs_score']} | RSI_min={sig['rsi_min']} | "
                            f"₹{sig['close']} | SMA15=₹{sig['sma15']} | Qty={qty} | Cost=₹{cost:,.0f}"
                        )
                        u_state['holdings'][sig['symbol']] = {
                            'entry_price': sig['close'],
                            'qty': qty,
                            'entry_date': today,
                            'rs_score': sig['rs_score']
                        }
                else:
                    msgs.append("⚠️ No qualifying stocks found. Staying in cash.")

                u_state['last_rebalance'] = today

            else:
                next_reb = REBAL_N - days_since
                msgs.append(f"\n📅 Not a rebalance day. Next in *{next_reb} day(s)* (last: {last_reb})")

            # ── PORTFOLIO SUMMARY ────────────────────────────────
            msgs.append(f"\n💼 *Current Holdings ({uname}):*")
            if u_state['holdings']:
                for sym, h in u_state['holdings'].items():
                    cur = data[sym]['Close'].iloc[-1] if sym in data else h['entry_price']
                    pct = (cur / h['entry_price'] - 1) * 100
                    msgs.append(
                        f"  📌 *{sym}* | Entry=₹{h['entry_price']} | CMP=₹{cur:.2f} | "
                        f"Qty={h['qty']} | PnL={pct:+.2f}% | Since={h['entry_date']}"
                    )
            else:
                msgs.append("  💰 All Cash")

            _state[uname] = u_state
            all_msgs.extend(msgs)

        _last_run_time    = now_ist.strftime('%d %b %Y, %I:%M %p IST')
        _last_run_summary = '\n'.join(all_msgs)

        notify(_last_run_summary)
        notify(f"✅ *Scan Complete* | {_last_run_time}")
        log.info("=== Scan Complete ===")

    except Exception as e:
        import traceback
        tb = traceback.format_exc()[-1200:]
        err_msg = f"SCAN ERROR\n\n{tb}"
        log.error(f"run_scan crashed: {e}")
        # Use plain text (no Markdown) so Telegram always accepts it
        for cid in set(filter(None, [CHAT_ID, str(reply_chat_id) if reply_chat_id else None])):
            try:
                requests.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json={"chat_id": cid, "text": err_msg},
                    timeout=10
                )
            except:
                pass


# ── FLASK APP ────────────────────────────────────────────────
app = Flask(__name__)

@app.route('/', methods=['GET'])
def home():
    return jsonify({"status": "RS+RSI Bot is running", "last_run": _last_run_time})

@app.route('/diag', methods=['GET'])
def diag():
    """Synchronous diagnostic — tests MongoDB + yfinance directly."""
    result = {}
    # Test MongoDB
    try:
        col   = db_manager._get_col()
        count = col.count_documents({})
        result['mongodb'] = f"OK - {count} documents"
    except Exception as e:
        result['mongodb'] = f"FAILED: {e}"
    # Test yfinance
    try:
        import yfinance as yf
        h = yf.Ticker("RELIANCE.NS").history(period="3d")
        result['yfinance'] = f"OK - RELIANCE rows: {len(h)}"
    except Exception as e:
        result['yfinance'] = f"FAILED: {e}"
    result['env_mongo_set'] = bool(os.environ.get('MONGODB_URI'))
    result['env_token_set'] = bool(os.environ.get('TELEGRAM_BOT_TOKEN'))
    return jsonify(result)

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"ok": True})

    msg = data.get('message') or data.get('edited_message')
    if not msg:
        return jsonify({"ok": True})

    chat_id = str(msg.get('chat', {}).get('id', ''))
    text    = msg.get('text', '').strip()

    if text.startswith('/start'):
        tg_answer(chat_id,
            "👋 *RS+RSI Bot is live on Render!*\n\n"
            "Commands:\n"
            "• /run — Run scan immediately\n"
            "• /status — Last scan results\n"
            "• /help — Show this menu\n\n"
            f"⏰ Auto-runs daily at *3:15 PM IST* (weekdays)\n"
            f"📊 Universe: *{UNIVERSE}* | Top {TOP_N} stocks"
        )
    elif text.startswith('/run'):
        tg_answer(chat_id, "🔄 Starting scan... First run takes ~5 min (DB init). Later runs ~1 min.")
        threading.Thread(target=run_scan, args=("manual /run", chat_id), daemon=True).start()
    elif text.startswith('/dbtest'):
        def _dbtest(cid):
            def _raw_send(txt):
                try:
                    requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                                  json={"chat_id": cid, "text": txt}, timeout=10)
                except: pass
            _raw_send("🔍 Testing MongoDB connection...")
            try:
                col = db_manager._get_col()
                count = col.count_documents({})
                _raw_send(f"✅ MongoDB OK! Documents in DB: {count}")
            except Exception as e:
                _raw_send(f"❌ MongoDB FAILED: {e}")
            _raw_send("🔍 Testing yfinance (1 stock)...")
            try:
                import yfinance as yf
                t = yf.Ticker("RELIANCE.NS")
                h = t.history(period="5d")
                _raw_send(f"✅ yfinance OK! RELIANCE rows: {len(h)}")
            except Exception as e:
                _raw_send(f"❌ yfinance FAILED: {e}")
        tg_answer(chat_id, "Running DB + yfinance tests...")
        threading.Thread(target=_dbtest, args=(chat_id,), daemon=True).start()
    elif text.startswith('/init'):
        _cid = chat_id
        def _force_init():
            def _raw(txt):
                try:
                    requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                                  json={"chat_id": _cid, "text": txt}, timeout=10)
                except: pass
            try:
                _raw("Thread started. Connecting to MongoDB...")
                all_syms = list(set(sum([v['stocks'] for v in UNIVERSES.values()], [])))
                db_manager._get_col().delete_many({})
                _raw(f"MongoDB cleared. Starting download of {len(all_syms)} stocks...")
                db_manager.init_or_update(all_syms, notify_fn=_raw)
                _raw("✅ DB init complete. Send /run to scan.")
            except Exception as e:
                import traceback
                _raw(f"INIT ERROR: {e}\n{traceback.format_exc()[-500:]}")
        tg_answer(chat_id, "🔄 Starting DB init...")
        threading.Thread(target=_force_init, daemon=True).start()

    elif text.startswith('/status'):
        if _last_run_time:
            tg_answer(chat_id, f"🕒 *Last run:* {_last_run_time}\n\n{_last_run_summary}")
        else:
            tg_answer(chat_id, "⚠️ No scan has run yet. Use /run to trigger one.")
    elif text.startswith('/help'):
        tg_answer(chat_id,
            "📖 *RS+RSI Bot Commands:*\n\n"
            "/run — Trigger a full scan now\n"
            "/status — View last scan results\n"
            "/start — Bot info & status\n"
            "/help — This message\n\n"
            f"⚙️ Params: RS={RS_P} | RSI({RSI_L})<{RSI_T} | SMA={SMA_L} | Top{TOP_N} | Rebal every {REBAL_N}d"
        )

    return jsonify({"ok": True})

@app.route('/ping', methods=['GET'])
def ping():
    return "pong", 200

# ── SELF-PING (keeps Render free tier awake) ─────────────────
def self_ping():
    if not RENDER_URL:
        return
    while True:
        try:
            requests.get(f"{RENDER_URL}/ping", timeout=10)
            log.info("Self-ping OK")
        except:
            pass
        time.sleep(600)  # every 10 min

# ── SCHEDULER: 3:15 PM IST on weekdays ──────────────────────
def scheduled_run():
    now = datetime.now(IST)
    if now.weekday() < 5:  # Mon-Fri
        log.info("Scheduled 3:15 PM IST run triggered.")
        run_scan(triggered_by="scheduler 3:15 PM IST")
    else:
        log.info(f"Skipping scheduled run — today is {now.strftime('%A')}.")

scheduler = BackgroundScheduler(timezone=IST)
scheduler.add_job(scheduled_run, 'cron', hour=15, minute=15, day_of_week='mon-fri')
scheduler.start()

# ── SELF-PING THREAD ─────────────────────────────────────────
threading.Thread(target=self_ping, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
