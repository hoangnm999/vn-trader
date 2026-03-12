import os
import time
import logging
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    end = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    for source in ['TCBS', 'VCI']:
        try:
            from vnstock import Vnstock
            df = Vnstock().stock(symbol=symbol, source=source).quote.history(
                start=start, end=end, interval='1D'
            )
            if df is not None and len(df) >= 30:
                logger.info(f"{symbol}/{source} OK rows={len(df)}")
                return df, source
        except Exception as e:
            logger.warning(f"{symbol}/{source}: {e}")
    return None, None

def compute_indicators(df, price_override=None):
    import numpy as np
    cc = find_col(df, ['close','closeprice','close_price'])
    hc = find_col(df, ['high','highprice','high_price'])
    lc = find_col(df, ['low','lowprice','low_price'])
    vc = find_col(df, ['volume','volume_match','klgd'])

    if cc is None:
        nums = df.select_dtypes(include='number').columns
        cc = nums[-1] if len(nums) > 0 else None
    
    if cc is None:
        return None

    closes = df[cc].astype(float).values
    if closes.max() < 1000: closes *= 1000
    
    highs = df[hc].astype(float).values if hc else closes.copy()
    if hc and highs.max() < 1000: highs *= 1000
    
    lows = df[lc].astype(float).values if lc else closes.copy()
    if lc and lows.max() < 1000: lows *= 1000
    
    volumes = df[vc].astype(float).values if vc else np.zeros(len(closes))
    price = float(price_override) if price_override else float(closes[-1])
    prev_close = float(closes[-2]) if len(closes) > 1 else price

    # ── EMA helper ────────────────────────────────────────────────────────────
    def ema_arr(arr, span):
        alpha = 2 / (span + 1)
        out = np.zeros(len(arr))
        out[0] = arr[0]
        for i in range(1, len(arr)):
            out[i] = alpha * arr[i] + (1 - alpha) * out[i-1]
        return out

    # ── RSI series ────────────────────────────────────────────────────────────
    def calc_rsi_arr(arr, p=14):
        out = np.full(len(arr), 50.0)
        for i in range(p, len(arr)):
            d = np.diff(arr[i-p:i+1])
            g = np.where(d > 0, d, 0.)
            l = np.where(d < 0, -d, 0.)
            ag = np.mean(g)
            al = np.mean(l)
            out[i] = 100. if al == 0 else 100 - 100 / (1 + ag / al)
        return np.round(out, 1)

    rsi_series = calc_rsi_arr(closes)
    rsi_val = float(rsi_series[-1])

    # ── RSI Phân kỳ ───────────────────────────────────────────────────────────
    def detect_divergence(price_arr, rsi_arr, lookback=20):
        if len(price_arr) < lookback:
            return 'none', ''
        p = price_arr[-lookback:]
        r = rsi_arr[-lookback:]
        bottoms = [i for i in range(1, len(p)-1) if p[i] < p[i-1] and p[i] < p[i+1]]
        tops = [i for i in range(1, len(p)-1) if p[i] > p[i-1] and p[i] > p[i+1]]
        
        if len(bottoms) >= 2:
            b1, b2 = bottoms[-2], bottoms[-1]
            if p[b2] < p[b1] and r[b2] > r[b1] + 2:
                return 'bullish', (f'Phân kỳ tăng: Giá đáy mới ({p[b2]:,.0f}) thấp hơn '
                                 f'nhưng RSI ({r[b2]:.0f}) cao hơn → Sắp đảo chiều tăng!')
        if len(tops) >= 2:
            t1, t2 = tops[-2], tops[-1]
            if p[t2] > p[t1] and r[t2] < r[t1] - 2:
                return 'bearish', (f'Phân kỳ giảm: Giá đỉnh mới ({p[t2]:,.0f}) cao hơn '
                                  f'nhưng RSI ({r[t2]:.0f}) thấp hơn → Cảnh báo đảo chiều!')
        return 'none', ''

    div_type, div_msg = detect_divergence(closes, rsi_series)

    # ── MACD ──────────────────────────────────────────────────────────────────
    ema12 = ema_arr(closes, 12)
    ema26 = ema_arr(closes, 26)
    macd_line = ema12 - ema26
    sig_line = ema_arr(macd_line, 9)
    macd_hist = macd_line - sig_line
    macd_val = float(macd_line[-1])
    macd_sig = float(sig_line[-1])
    macd_h = float(macd_hist[-1])

    # ── MA20 & MA50 ───────────────────────────────────────────────────────────
    ma20 = float(np.mean(closes[-20:]))
    ma50 = float(np.mean(closes[-min(50, len(closes)):]))
    ma20_prev = float(np.mean(closes[-21:-1])) if len(closes) >= 21 else ma20
    ma50_prev = float(np.mean(closes[-51:-1])) if len(closes) >= 51 else ma50
    golden_cross = ma20_prev < ma50_prev and ma20 > ma50
    death_cross = ma20_prev > ma50_prev and ma20 < ma50

    # ── Bollinger Bands ───────────────────────────────────────────────────────
    bb_mid = float(np.mean(closes[-20:]))
    bb_std = float(np.std(closes[-20:]))
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std
    bb_pct = (price - bb_lower) / (bb_upper - bb_lower) * 100 if bb_upper !=
