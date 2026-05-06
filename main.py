"""
RS + RSI Telegram Bot - Render Cloud Edition
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

BOT_TOKEN   = os.environ.get('TELEGRAM_BOT_TOKEN', '')
CHAT_ID     = os.environ.get('TELEGRAM_CHAT_ID', '')
RENDER_URL  = os.environ.get('RENDER_URL', '')

RS_P        = int(os.environ.get('RS_PERIOD', 70))
RSI_L       = int(os.environ.get('RSI_LENGTH', 5))
RSI_T       = float(os.environ.get('RSI_THRESHOLD', 40))
SMA_L       = int(os.environ.get('SMA_LENGTH', 15))
TOP_N       = int(os.environ.get('TOP_N', 5))
SMA_ENT     = os.environ.get('SMA_ENTRY_FILTER', 'true').lower() == 'true'
REBAL_N     = int(os.environ.get('REBAL_DAYS', 5))
CAP_N200    = float(os.environ.get('CAPITAL_N200', 50000))
CAP_N500    = float(os.environ.get('CAPITAL_N500', 50000))
UNIVERSE    = os.environ.get('UNIVERSE', 'BOTH')

IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger('RS_RSI_BOT')

NIFTY200 = "ABB,ADANIENT,ADANIPORTS,APOLLOHOSP,ASIANPAINT,AXISBANK,BAJAJ-AUTO,BAJFINANCE,BAJAJFINSV,BPCL,BHARTIARTL,BRITANNIA,CIPLA,COALINDIA,DIVISLAB,DRREDDY,EICHERMOT,GRASIM,HCLTECH,HDFCBANK,HDFCLIFE,HEROMOTOCO,HINDALCO,HINDUNILVR,ICICIBANK,ITC,INDUSINDBK,INFY,JSWSTEEL,KOTAKBANK,LT,M&M,MARUTI,NESTLEIND,NTPC,ONGC,POWERGRID,RELIANCE,SBILIFE,SBIN,SUNPHARMA,TCS,TATACONSUM,TATAMOTORS,TATASTEEL,TECHM,TITAN,ULTRACEMCO,UPL,WIPRO".split(',')
NIFTY500 = NIFTY200 # Simplified for now to avoid huge string mess

UNIVERSES = {}
if UNIVERSE in ('BOTH', 'N200'):
              UNIVERSES['Nifty200'] = {'stocks': NIFTY200, 'capital': CAP_N200}
          if UNIVERSE in ('BOTH', 'N500'):
                        UNIVERSES['Nifty500'] = {'stocks': NIFTY500, 'capital': CAP_N500}

_state = {}
_last_run_time = None
_last_run_summary = "No scan run yet."

def tg_send(text, chat_id=None):
              cid = chat_id or CHAT_ID
              if not BOT_TOKEN or not cid: return
                            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
                      requests.post(url, json={"chat_id": cid, "text": text, "parse_mode": "Markdown"}, timeout=10)
except Exception as e:
        log.error(f"Telegram error: {e}")

def calc_rsi(s, w):
              d = s.diff()
    g = d.where(d > 0, 0.0).rolling(w).mean()
    l = (-d.where(d < 0, 0.0)).rolling(w).mean()
    rs = g / l.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def fetch_data(universe_stocks):
              lookback = max(RS_P * 2, 200)
    start = (datetime.now() - timedelta(days=lookback)).strftime('%Y-%m-%d')
    tickers = [s + '.NS' for s in universe_stocks] + ['^NSEI']
    raw = yf.download(tickers, start=start, auto_adjust=True, progress=False)
    nifty = raw['Close']['^NSEI'].dropna()
    data = {s: raw['Close'][s + '.NS'].dropna() for s in universe_stocks if s + '.NS' in raw['Close']}
    return nifty, data

def generate_signals(nifty, data):
              candidates = []
    for sym, prices in data.items():
                      if len(prices) < RS_P + 2: continue
                                        rs = (prices.iloc[-1] / prices.iloc[-RS_P-1]) / (nifty.iloc[-1] / nifty.iloc[-RS_P-1]) - 1
        if rs <= 0: continue
                          rsi = calc_rsi(prices, RSI_L).tail(10).min()
        if pd.isna(rsi) or rsi > RSI_T: continue
                          if prices.iloc[-1] <= prices.iloc[-2]: continue
                                            if SMA_ENT:
                                                                  sma = prices.rolling(SMA_L).mean().iloc[-1]
                                                                  if prices.iloc[-1] <= sma: continue
                                                                                    candidates.append({'symbol': sym, 'rs': round(rs, 4), 'rsi': round(rsi, 2), 'price': round(prices.iloc[-1], 2)})
    candidates.sort(key=lambda x: x['rs'], reverse=True)
    return candidates[:TOP_N]

def run_scan(triggered_by="scheduler"):
              global _state, _last_run_time, _last_run_summary
    now = datetime.now(IST)
    log.info(f"Scan Started | {triggered_by}")
    summary = []
    for uname, ucfg in UNIVERSES.items():
                      nifty, data = fetch_data(ucfg['stocks'])
        sigs = generate_signals(nifty, data)
        msg = f"\n{uname} Signals:\n" + "\n".join([f"{s['symbol']}: RS={s['rs']} RSI={s['rsi']} P={s['price']}" for s in sigs])
        summary.append(msg)
    _last_run_time = now.strftime('%Y-%m-%d %H:%M')
    _last_run_summary = "\n".join(summary)
    tg_send(f"Scan Complete at {_last_run_time}\n{_last_run_summary}")

app = Flask(__name__)
@app.route('/')
def home(): return jsonify({"status": "running", "last_run": _last_run_time})
          @app.route('/webhook', methods=['POST'])
def webhook():
              data = request.get_json(silent=True)
    if data and 'message' in data:
                      text = data['message'].get('text', '')
        if text == '/run': threading.Thread(target=run_scan, args=("manual",)).start()
                      return jsonify({"ok": True})

def self_ping():
              while True:
                                if RENDER_URL:
                                                      try: requests.get(f"{RENDER_URL}/ping")
                                                                            except: pass
        time.sleep(600)

scheduler = BackgroundScheduler(timezone=IST)
scheduler.add_job(scheduled_run, 'cron', hour=15, minute=15, day_of_week='mon-fri')
scheduler.start()
threading.Thread(target=self_ping, daemon=True).start()

if __name__ == '__main__':
              app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
