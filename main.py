"""
RS + RSI Telegram Bot — Render Cloud Edition
- Auto-runs at 3:15 PM IST every weekday
- /run command triggers immediate scan
- All results sent to Telegram
- No state persistence, no Angel One, no Excel
"""
import os, logging, threading, time, requests
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
    "360ONE,ABB,APLAPOLLO,AUBANK,ADANIENSOL,ADANIENT,ADANIGREEN,ADANIPORTS,ADANIPOWER,ATGL,"
    "ABCAPITAL,ALKEM,AMBUJACEM,APOLLOHOSP,ASHOKLEY,ASIANPAINT,ASTRAL,AUROPHARMA,DMART,AXISBANK,"
    "BSE,BAJAJ-AUTO,BAJFINANCE,BAJAJFINSV,BAJAJHLDNG,BANKBARODA,BANKINDIA,BDL,BEL,BHARATFORG,"
    "BHEL,BPCL,BHARTIARTL,BIOCON,BLUESTARCO,BOSCHLTD,BRITANNIA,CGPOWER,CANBK,CHOLAFIN,CIPLA,"
    "COALINDIA,COCHINSHIP,COFORGE,COLPAL,CONCOR,COROMANDEL,CUMMINSIND,DLF,DABUR,DIVISLAB,DIXON,"
    "DRREDDY,EICHERMOT,EXIDEIND,NYKAA,FEDERALBNK,FORTIS,GAIL,GLENMARK,GODFRYPHLP,GODREJCP,"
    "GODREJPROP,GRASIM,HCLTECH,HDFCAMC,HDFCBANK,HDFCLIFE,HAVELLS,HEROMOTOCO,HINDALCO,HAL,"
    "HINDPETRO,HINDUNILVR,HINDZINC,HUDCO,ICICIBANK,ICICIGI,ICICIAMC,IDFCFIRSTB,ITC,INDIANB,"
    "INDHOTEL,IOC,IRCTC,IRFC,IREDA,INDUSTOWER,INDUSINDBK,NAUKRI,INFY,INDIGO,JSWENERGY,JSWSTEEL,"
    "JINDALSTEL,JIOFIN,JUBLFOOD,KEI,KPITTECH,KALYANKJIL,KOTAKBANK,LTF,LICHSGFIN,LTM,LT,"
    "LAURUSLABS,LODHA,LUPIN,MRF,M&MFIN,M&M,MANKIND,MARICO,MARUTI,MFSL,MAXHEALTH,MAZDOCK,"
    "MOTILALOFS,MPHASIS,MCX,MUTHOOTFIN,NHPC,NMDC,NTPC,NATIONALUM,NESTLEIND,OBEROIRLTY,ONGC,"
    "OIL,OFSS,POLICYBZR,PIIND,PAGEIND,PATANJALI,PERSISTENT,PHOENIXLTD,PIDILITIND,POLYCAB,PFC,"
    "POWERGRID,PRESTIGE,PNB,RECLTD,RADICO,RVNL,RELIANCE,SBICARD,SBILIFE,SRF,MOTHERSON,"
    "SHREECEM,SHRIRAMFIN,SIEMENS,SOLARINDS,SBIN,SAIL,SUNPHARMA,SUPREMEIND,SUZLON,TVSMOTOR,"
    "TATACOMM,TCS,TATACONSUM,TATAELXSI,TATAPOWER,TATASTEEL,TECHM,TITAN,TORNTPHARM,TRENT,"
    "TIINDIA,UPL,ULTRACEMCO,UNIONBANK,UNITDSPR,VBL,VEDL,IDEA,VOLTAS,WIPRO,YESBANK,ZYDUSLIFE"
).split(',')

NIFTY500 = (
    "360ONE,3MINDIA,ABB,ACC,ACMESOLAR,AIAENG,APLAPOLLO,AUBANK,AWL,AADHARHFC,AARTIIND,AAVAS,"
    "ABBOTINDIA,ACE,ADANIENSOL,ADANIENT,ADANIGREEN,ADANIPORTS,ADANIPOWER,ATGL,ABCAPITAL,ABFRL,"
    "AFFLE,AJANTPHARM,ALKEM,AMBER,AMBUJACEM,ANANDRATHI,ANANTRAJ,ANGELONE,APARINDS,APOLLOHOSP,"
    "APOLLOTYRE,APTUS,ASHOKLEY,ASIANPAINT,ASTERDM,ASTRAL,ATUL,AUROPHARMA,DMART,AXISBANK,BEML,"
    "BLS,BSE,BAJAJ-AUTO,BAJFINANCE,BAJAJFINSV,BAJAJHLDNG,BAJAJHFL,BALKRISIND,BALRAMCHIN,"
    "BANDHANBNK,BANKBARODA,BANKINDIA,BATAINDIA,BERGEPAINT,BDL,BEL,BHARATFORG,BHEL,BPCL,"
    "BHARTIARTL,BIOCON,BLUESTARCO,BOSCHLTD,BRIGADE,BRITANNIA,CESC,CGPOWER,CRISIL,CANFINHOME,"
    "CANBK,CEATLTD,CDSL,CHOLAFIN,CIPLA,COALINDIA,COCHINSHIP,COFORGE,COLPAL,CAMS,CONCOR,"
    "COROMANDEL,CROMPTON,CUMMINSIND,CYIENT,DLF,DABUR,DALBHARAT,DEEPAKNTR,DELHIVERY,DIVISLAB,"
    "DIXON,LALPATHLAB,DRREDDY,EICHERMOT,EMAMILTD,ENDURANCE,ESCORTS,EXIDEIND,NYKAA,FEDERALBNK,"
    "FORTIS,GAIL,GICRE,GLENMARK,GODFRYPHLP,GODREJCP,GODREJPROP,GRANULES,GRASIM,HCLTECH,"
    "HDFCAMC,HDFCBANK,HDFCLIFE,HFCL,HAVELLS,HEROMOTOCO,HINDALCO,HAL,HINDPETRO,HINDUNILVR,"
    "HINDZINC,ICICIBANK,ICICIGI,ICICIAMC,ICICIPRULI,IDFCFIRSTB,ITC,INDIANB,INDHOTEL,IOC,"
    "IRCTC,IRFC,IREDA,INDUSTOWER,INDUSINDBK,NAUKRI,INFY,INDIGO,IPCALAB,JKCEMENT,JSWENERGY,"
    "JSWSTEEL,JINDALSTEL,JIOFIN,JUBLFOOD,KEI,KPITTECH,KALYANKJIL,KOTAKBANK,LTF,LTTS,"
    "LICHSGFIN,LTM,LT,LAURUSLABS,LICI,LODHA,LUPIN,MRF,MGL,M&MFIN,M&M,MANKIND,MARICO,MARUTI,"
    "MFSL,MAXHEALTH,MAZDOCK,MOTILALOFS,MPHASIS,MCX,MUTHOOTFIN,NBCC,NCC,NHPC,NMDC,NTPC,"
    "NATIONALUM,NESTLEIND,OBEROIRLTY,ONGC,OIL,OFSS,POLICYBZR,PCBL,PIIND,PAGEIND,PATANJALI,"
    "PERSISTENT,PETRONET,PHOENIXLTD,PIDILITIND,POLYCAB,PFC,POWERGRID,PRESTIGE,PNB,RECLTD,"
    "RADICO,RVNL,RELIANCE,SBICARD,SBILIFE,SJVN,SRF,MOTHERSON,SHREECEM,SHRIRAMFIN,SIEMENS,"
    "SOLARINDS,SBIN,SAIL,SUNPHARMA,SUPREMEIND,SUZLON,TVSMOTOR,TATACOMM,TCS,TATACONSUM,"
    "TATAELXSI,TATAPOWER,TATASTEEL,TECHM,TITAN,TORNTPHARM,TRENT,TIINDIA,UPL,ULTRACEMCO,"
    "UNIONBANK,UBL,UNITDSPR,VBL,VEDL,VOLTAS,WIPRO,YESBANK,ZYDUSLIFE,ZYDUSWELL,ECLERX"
).split(',')

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

# ── DATA FETCH ───────────────────────────────────────────────
def fetch_data(universe_stocks):
    lookback_days = max(RS_P * 2, 200)
    start = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
    tickers = [s + '.NS' for s in universe_stocks] + ['^NSEI']
    log.info(f"Fetching {len(tickers)} tickers from {start}...")
    raw = yf.download(tickers, start=start, auto_adjust=True, progress=False, group_by='ticker')
    nifty, data = pd.Series(dtype=float), {}
    try:
        if isinstance(raw.columns, pd.MultiIndex):
            nifty = raw['^NSEI']['Close'].dropna()
            for sym in universe_stocks:
                try:
                    df = raw[sym + '.NS'][['Open', 'High', 'Low', 'Close']].dropna()
                    if len(df) >= RS_P + 5:
                        data[sym] = df
                except:
                    pass
        else:
            nifty = raw['Close'].dropna()
    except Exception as e:
        log.error(f"Data parse error: {e}")
    log.info(f"  -> {len(data)} stocks loaded. Nifty rows: {len(nifty)}")
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
def run_scan(triggered_by="scheduler"):
    global _state, _last_run_time, _last_run_summary
    now_ist = datetime.now(IST)
    today   = now_ist.strftime('%Y-%m-%d')
    log.info(f"=== RS+RSI Scan Started | Triggered by: {triggered_by} ===")

    header = (
        f"🤖 *RS+RSI Bot — Scan Started*\n"
        f"🕒 {now_ist.strftime('%d %b %Y, %I:%M %p IST')}\n"
        f"📊 Params: RS={RS_P} | RSI({RSI_L})<{RSI_T} | SMA={SMA_L} | Top{TOP_N}\n"
        f"{'─' * 30}"
    )
    tg_send(header)

    all_msgs = []

    for uname, ucfg in UNIVERSES.items():
        u_state = _state.setdefault(uname, {
            'holdings': {},
            'last_rebalance': '2000-01-01',
            'capital': ucfg['capital']
        })

        tg_send(f"⏳ Fetching data for *{uname}* ({len(ucfg['stocks'])} stocks)...")
        nifty, data = fetch_data(ucfg['stocks'])

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
        last_reb = u_state['last_rebalance']
        days_since = (date.fromisoformat(today) - date.fromisoformat(last_reb)).days
        do_rebal = days_since >= REBAL_N

        if do_rebal:
            msgs.append(f"\n🔄 *Rebalance Day* (last: {last_reb})")

            # Exit all holdings
            if u_state['holdings']:
                msgs.append("📤 *Closing all positions:*")
                for sym, h in list(u_state['holdings'].items()):
                    cur = data[sym]['Close'].iloc[-1] if sym in data else h['entry_price']
                    pct = (cur / h['entry_price'] - 1) * 100
                    msgs.append(
                        f"  🛑 SELL *{sym}* | ₹{cur:.2f} | PnL: {pct:+.2f}%"
                    )
                u_state['holdings'] = {}

            # Generate new signals
            signals = generate_signals(nifty, data)
            log.info(f"  -> {len(signals)} signals for {uname}")

            if signals:
                alloc = u_state['capital'] / TOP_N
                msgs.append(f"\n🚀 *New Entries* (Alloc/stock: ₹{alloc:,.0f}):*")
                for sig in signals:
                    qty = max(1, int(alloc / sig['close']))
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

    _last_run_time = now_ist.strftime('%d %b %Y, %I:%M %p IST')
    _last_run_summary = '\n'.join(all_msgs)

    tg_send(_last_run_summary)
    tg_send(f"✅ *Scan Complete* | {_last_run_time}")
    log.info("=== Scan Complete ===")


# ── FLASK APP ────────────────────────────────────────────────
app = Flask(__name__)

@app.route('/', methods=['GET'])
def home():
    return jsonify({"status": "RS+RSI Bot is running", "last_run": _last_run_time})

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
        tg_answer(chat_id, "🔄 Starting RS+RSI scan now... Please wait ~2 minutes.")
        threading.Thread(target=run_scan, args=("manual /run",), daemon=True).start()
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
