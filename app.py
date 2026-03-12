import os
import time
import logging
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(**name**)
CORS(app)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(**name**)

_cache = {}
CACHE_TTL = 300

def get_cached(key):
if key in _cache:
data, ts = _cache[key]
if time.time() - ts < CACHE_TTL:
return data
return None

def set_cache(key, data):
_cache[key] = (data, time.time())

def find_col(df, names):
for c in df.columns:
if c.lower() in names:
return c
return None

def load_history(symbol, days=200):
from datetime import datetime, timedelta
end = datetime.now().strftime(’%Y-%m-%d’)
start = (datetime.now() - timedelta(days=days)).strftime(’%Y-%m-%d’)
for source in [‘VCI’, ‘TCBS’]:
try:
from vnstock import Vnstock
df = Vnstock().stock(symbol=symbol, source=source).quote.history(
start=start, end=end, interval=‘1D’
)
if df is not None and len(df) >= 30:
logger.info(f”{symbol}/{source} OK rows={len(df)}”)
return df, source
except Exception as e:
logger.warning(f”{symbol}/{source}: {e}”)
return None, None

def compute_indicators(df, price_override=None):
import numpy as np
cc = find_col(df, [‘close’, ‘closeprice’, ‘close_price’])
hc = find_col(df, [‘high’, ‘highprice’, ‘high_price’])
lc = find_col(df, [‘low’, ‘lowprice’, ‘low_price’])
vc = find_col(df, [‘volume’, ‘volume_match’, ‘klgd’])
if cc is None:
nums = df.select_dtypes(include=‘number’).columns
cc = nums[-1] if len(nums) > 0 else None
if cc is None:
return None
closes = df[cc].astype(float).values
if closes.max() < 1000:
closes *= 1000
highs = df[hc].astype(float).values if hc else closes.copy()
if hc and highs.max() < 1000:
highs *= 1000
lows = df[lc].astype(float).values if lc else closes.copy()
if lc and lows.max() < 1000:
lows *= 1000
volumes = df[vc].astype(float).values if vc else np.zeros(len(closes))
price = float(price_override) if price_override else float(closes[-1])
prev_close = float(closes[-2]) if len(closes) > 1 else price

```
def ema_arr(arr, span):
    alpha = 2.0 / (span + 1)
    out = np.zeros(len(arr))
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = alpha * arr[i] + (1 - alpha) * out[i - 1]
    return out

def calc_rsi_arr(arr, p=14):
    out = np.full(len(arr), 50.0)
    for i in range(p, len(arr)):
        d = np.diff(arr[i - p:i + 1])
        g = np.where(d > 0, d, 0.0)
        l = np.where(d < 0, -d, 0.0)
        ag = np.mean(g)
        al = np.mean(l)
        out[i] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
    return np.round(out, 1)

rsi_series = calc_rsi_arr(closes)
rsi_val = float(rsi_series[-1])

def detect_divergence(price_arr, rsi_arr, lookback=20):
    if len(price_arr) < lookback:
        return 'none', ''
    p = price_arr[-lookback:]
    r = rsi_arr[-lookback:]
    bottoms = [i for i in range(1, len(p) - 1) if p[i] < p[i - 1] and p[i] < p[i + 1]]
tops = [i for i in range(1, len(p) - 1) if p[i] > p[i - 1] and p[i] > p[i + 1]]
    if len(bottoms) >= 2:
        b1, b2 = bottoms[-2], bottoms[-1]
        if p[b2] < p[b1] and r[b2] > r[b1] + 2:
            return 'bullish', ('Phan ky tang: Gia day moi (' + f'{p[b2]:,.0f}' + ') thap hon nhung RSI (' + f'{r[b2]:.0f}' + ') cao hon -> Sap dao chieu tang!')
    if len(tops) >= 2:
        t1, t2 = tops[-2], tops[-1]
        if p[t2] > p[t1] and r[t2] < r[t1] - 2:
            return 'bearish', ('Phan ky giam: Gia dinh moi (' + f'{p[t2]:,.0f}' + ') cao hon nhung RSI (' + f'{r[t2]:.0f}' + ') thap hon -> Canh bao dao chieu!')
    return 'none', ''

div_type, div_msg = detect_divergence(closes, rsi_series)

ema12 = ema_arr(closes, 12)
ema26 = ema_arr(closes, 26)
macd_line = ema12 - ema26
sig_line = ema_arr(macd_line, 9)
macd_hist = macd_line - sig_line
macd_val = float(macd_line[-1])
macd_sig = float(sig_line[-1])
macd_h = float(macd_hist[-1])

ma20 = float(np.mean(closes[-20:]))
ma50 = float(np.mean(closes[-min(50, len(closes)):]))
ma20_prev = float(np.mean(closes[-21:-1])) if len(closes) >= 21 else ma20
ma50_prev = float(np.mean(closes[-51:-1])) if len(closes) >= 51 else ma50
golden_cross = ma20_prev < ma50_prev and ma20 > ma50
death_cross = ma20_prev > ma50_prev and ma20 < ma50

bb_mid = float(np.mean(closes[-20:]))
bb_std = float(np.std(closes[-20:]))
bb_upper = bb_mid + 2 * bb_std
bb_lower = bb_mid - 2 * bb_std
bb_pct = (price - bb_lower) / (bb_upper - bb_lower) * 100 if bb_upper != bb_lower else 50

vol_today = float(volumes[-1]) if len(volumes) > 0 else 0
vol_ma20 = float(np.mean(volumes[-20:])) if len(volumes) >= 20 else vol_today
vol_ratio = vol_today / vol_ma20 if vol_ma20 > 0 else 1.0
price_up = price >= prev_close

if vol_ratio >= 1.5 and price_up:
    vol_signal = 'shark_buy'
    vol_msg = 'Vol ' + f'{vol_ratio:.1f}' + 'x TB + gia tang -> Dong tien lon vao! Xac nhan MUA'
elif vol_ratio >= 1.5 and not price_up:
    vol_signal = 'shark_sell'
    vol_msg = 'Vol ' + f'{vol_ratio:.1f}' + 'x TB + gia giam -> Dong tien lon xa! Tin hieu BAN'
elif vol_ratio < 0.7 and price_up:
    vol_signal = 'fake_rally'
    vol_msg = 'Gia tang nhung Vol thap ' + f'{vol_ratio:.1f}' + 'x TB -> Co the keo xa, can than!'
elif vol_ratio >= 1.0 and price_up:
    vol_signal = 'normal_buy'
    vol_msg = 'Vol ' + f'{vol_ratio:.1f}' + 'x TB + gia tang -> Xu huong tang duoc xac nhan'
elif vol_ratio < 0.7 and not price_up:
    vol_signal = 'weak_sell'
    vol_msg = 'Vol thap ' + f'{vol_ratio:.1f}' + 'x TB + gia giam -> Ap luc ban yeu'
else:
    vol_signal = 'normal'
    vol_msg = 'Vol binh thuong ' + f'{vol_ratio:.1f}' + 'x TB'

n = len(closes)
tenkan = (np.max(highs[-9:]) + np.min(lows[-9:])) / 2 if n >= 9 else price
kijun = (np.max(highs[-26:]) + np.min(lows[-26:])) / 2 if n >= 26 else price
span_a = (tenkan + kijun) / 2
span_b = (np.max(highs[-52:]) + np.min(lows[-52:])) / 2 if n >= 52 else price
cloud_top = round(max(float(span_a), float(span_b)), 0)
cloud_bottom = round(min(float(span_a), float(span_b)), 0)
ichi = {
    'tenkan': round(float(tenkan), 0),
    'kijun': round(float(kijun), 0),
    'cloud_top': cloud_top,
    'cloud_bottom': cloud_bottom,
}

def find_sr(h, l, window=5):
    levels = []
    for i in range(window, len(h) - window):
        if h[i] == max(h[i - window:i + window + 1]):
            levels.append(('R', float(h[i])))
        if l[i] == min(l[i - window:i + window + 1]):
            levels.append(('S', float(l[i])))
    merged = []
    levels.sort(key=lambda x: x[1])
    for typ, lvl in levels:
        found = False
        for m in merged:
            if abs(m['price'] - lvl) / lvl < 0.015:
                m['count'] += 1
                found = True
                break
        if not found:
            merged.append({'type': typ, 'price': round(lvl, 0), 'count': 1})
    strong = [m for m in merged if m['count'] >= 2]
    strong.sort(key=lambda x: x['count'], reverse=True)
    sups = sorted([m for m in strong if m['price'] < price], key=lambda x: x['price'], reverse=True)[:3]
    ress = sorted([m for m in strong if m['price'] > price], key=lambda x: x['price'])[:3]
    return sups, ress

supports, resistances = find_sr(highs, lows)

score = 50
signals = []

if vol_signal == 'shark_buy':
    score += 35
    signals.append(('VOL', 'bull', vol_msg))
elif vol_signal == 'shark_sell':
    score -= 35
    signals.append(('VOL', 'bear', vol_msg))
elif vol_signal == 'fake_rally':
    score -= 20
    signals.append(('VOL', 'bear', vol_msg))
elif vol_signal == 'normal_buy':
    score += 12
    signals.append(('VOL', 'bull', vol_msg))
elif vol_signal == 'weak_sell':
    score += 5
    signals.append(('VOL', 'neutral', vol_msg))
else:
    signals.append(('VOL', 'neutral', vol_msg))

if rsi_val < 30:
    score += 15
    signals.append(('RSI', 'bull', 'RSI=' + str(rsi_val) + ' Vung qua ban -> Tim co hoi MUA'))
elif rsi_val < 40:
    score += 7
    signals.append(('RSI', 'bull', 'RSI=' + str(rsi_val) + ' Vung yeu, dang hoi phuc'))
elif rsi_val > 70:
    score -= 15
    signals.append(('RSI', 'bear', 'RSI=' + str(rsi_val) + ' Vung qua mua -> KHONG mua duoi!'))
elif rsi_val > 60:
    score -= 7
    signals.append(('RSI', 'bear', 'RSI=' + str(rsi_val) + ' Vung manh, than trong'))
else:
    signals.append(('RSI', 'neutral', 'RSI=' + str(rsi_val) + ' Vung trung tinh'))

if div_type == 'bullish':
    score += 10
    signals.append(('DIV', 'bull', div_msg))
elif div_type == 'bearish':
    score -= 10
    signals.append(('DIV', 'bear', div_msg))
else:
    signals.append(('DIV', 'neutral', 'Khong phat hien phan ky RSI'))

if golden_cross:
    score += 20
    signals.append(('MA', 'bull', 'GOLDEN CROSS! MA20 cat len MA50 -> Tang manh dai han!'))
elif death_cross:
    score -= 20
    signals.append(('MA', 'bear', 'DEATH CROSS! MA20 cat xuong MA50 -> Giam dai han!'))
elif price > ma20 and ma20 > ma50:
    score += 15
signals.append(('MA', 'bull', 'Gia>MA20(' + f'{ma20:,.0f}' + ')>MA50(' + f'{ma50:,.0f}' + ') -> Tang 2 tang ben vung'))
elif price > ma20:
    score += 10
    signals.append(('MA', 'bull', 'Gia tren MA20 ' + f'{ma20:,.0f}' + ' -> Xu huong ngan han tang'))
elif price < ma20 and ma20 < ma50:
    score -= 15
    signals.append(('MA', 'bear', 'Gia<MA20<MA50 -> Giam 2 tang - KHONG mua duoi!'))
else:
    score -= 10
    signals.append(('MA', 'bear', 'Gia duoi MA20 ' + f'{ma20:,.0f}' + ' - KHONG mua duoi!'))

if macd_val > macd_sig and macd_h > 0:
    score += 3
    signals.append(('MACD', 'bull', 'MACD cat len Signal -> Dong luc tang'))
elif macd_val < macd_sig and macd_h < 0:
    score -= 3
    signals.append(('MACD', 'bear', 'MACD cat xuong Signal -> Dong luc giam'))
else:
    signals.append(('MACD', 'neutral', 'MACD=' + f'{macd_val:+.0f}'))

if supports:
    dist_s = (price - supports[0]['price']) / price * 100
    strength_s = supports[0]['count']
    if dist_s < 1.5:
        pts = min(12, 6 + strength_s * 2)
        score += pts
        signals.append(('SR', 'bull', 'Gia gan HT manh ' + f'{supports[0]["price"]:,.0f}' + ' (cham ' + str(strength_s) + ' lan, cach ' + f'{dist_s:.1f}' + '%)'))
    elif dist_s < 4:
    
