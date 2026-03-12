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
    bb_pct = (price - bb_lower) / (bb_upper - bb_lower) * 100 if bb_upper != bb_lower else 0

    # ── Volume thông minh ─────────────────────────────────────────────────────
    vol_today = float(volumes[-1]) if len(volumes) > 0 else 0
    vol_ma20 = float(np.mean(volumes[-20:])) if len(volumes) >= 20 else vol_today
    vol_ratio = vol_today / vol_ma20 if vol_ma20 > 0 else 1.0
    price_up = price >= prev_close

    if vol_ratio >= 1.5 and price_up:
        vol_signal = 'shark_buy'
        vol_msg = f'Dòng tiền lớn vào! Vol {vol_ratio:.1f}x TB + giá tăng → Xác nhận MUA'
    elif vol_ratio >= 1.5 and not price_up:
        vol_signal = 'shark_sell'
        vol_msg = f'Dòng tiền lớn xả! Vol {vol_ratio:.1f}x TB + giá giảm → Tín hiệu BÁN'
    elif vol_ratio < 0.7 and price_up:
        vol_signal = 'fake_rally'
        vol_msg = f'Giá tăng nhưng Vol thấp {vol_ratio:.1f}x TB → Có thể "kéo xả", cẩn trọng'
    elif vol_ratio >= 1.0 and price_up:
        vol_signal = 'normal_buy'
        vol_msg = f'Vol {vol_ratio:.1f}x TB + giá tăng → Xu hướng tăng được xác nhận'
    elif vol_ratio < 0.7 and not price_up:
        vol_signal = 'weak_sell'
        vol_msg = f'Vol thấp {vol_ratio:.1f}x TB + giá giảm → Áp lực bán yếu'
    else:
        vol_signal = 'normal'
        vol_msg = f'Vol bình thường {vol_ratio:.1f}x TB'

    # ── Ichimoku ──────────────────────────────────────────────────────────────
    n = len(closes)
    tenkan = (np.max(highs[-9:]) + np.min(lows[-9:])) / 2 if n >= 9 else price
    kijun = (np.max(highs[-26:]) + np.min(lows[-26:])) / 2 if n >= 26 else price
    span_a = (tenkan + kijun) / 2
    span_b = (np.max(highs[-52:]) + np.min(lows[-52:])) / 2 if n >= 52 else price
    cloud_top = round(max(float(span_a), float(span_b)), 0)
    cloud_bottom = round(min(float(span_a), float(span_b)), 0)
    ichi = {
        'tenkan': round(float(tenkan), 0), 'kijun': round(float(kijun), 0),
        'cloud_top': cloud_top, 'cloud_bottom': cloud_bottom,
    }

    # ── Hỗ trợ & Kháng cự ────────────────────────────────────────────────────
    def find_sr(h, l, window=5):
        levels = []
        for i in range(window, len(h) - window):
            if h[i] == max(h[i-window:i+window+1]): levels.append(('R', float(h[i])))
            if l[i] == min(l[i-window:i+window+1]): levels.append(('S', float(l[i])))
        
        merged = []
        levels.sort(key=lambda x: x[1])
        for typ, lvl in levels:
            found = False
            for m in merged:
                if abs(m['price'] - lvl) / lvl < 0.015:
                    m['count'] += 1; found = True; break
            if not found:
                merged.append({'type': typ, 'price': round(lvl, 0), 'count': 1})
        
        strong = [m for m in merged if m['count'] >= 2]
        strong.sort(key=lambda x: x['count'], reverse=True)
        sups = sorted([m for m in strong if m['price'] < price], key=lambda x: x['price'], reverse=True)
        ress = sorted([m for m in strong if m['price'] > price], key=lambda x: x['price'])[:3]
        return sups, ress

    supports, resistances = find_sr(highs, lows)

    # ── CHẤM ĐIỂM ─────────────────────────────────────────────────────────────
    score = 50 
    signals = []

    # NHÓM 1: VOLUME (35%)
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

    # NHÓM 2: RSI (25%)
    if rsi_val < 30:
        score += 15
        signals.append(('RSI', 'bull', f'RSI={rsi_val} — Vùng quá bán'))
    elif rsi_val > 70:
        score -= 15
        signals.append(('RSI', 'bear', f'RSI={rsi_val} — Vùng quá mua'))
    else:
        signals.append(('RSI', 'neutral', f'RSI={rsi_val}'))

    if div_type == 'bullish':
        score += 10
        signals.append(('DIV', 'bull', div_msg))
    elif div_type == 'bearish':
        score -= 10
        signals.append(('DIV', 'bear', div_msg))

    # NHÓM 3: TREND (20%)
    if golden_cross:
        score += 20
        signals.append(('MA', 'bull', 'GOLDEN CROSS!'))
    elif death_cross:
        score -= 20
        signals.append(('MA', 'bear', 'DEATH CROSS!'))
    elif price > ma20:
        score += 10
        signals.append(('MA', 'bull', 'Giá > MA20'))
    else:
        score -= 10
        signals.append(('MA', 'bear', 'Giá < MA20'))

    if macd_val > macd_sig:
        score += 3
        signals.append(('MACD', 'bull', 'MACD hướng lên'))
    elif macd_val < macd_sig:
        score -= 3
        signals.append(('MACD', 'bear', 'MACD hướng xuống'))

    # NHÓM 4: SR (12%)
    if supports and (price - supports[0]['price']) / price < 0.015:
        score += 12
        signals.append(('SR', 'bull', 'Gần hỗ trợ mạnh'))
    if resistances and (resistances[0]['price'] - price) / price < 0.015:
        score -= 12
        signals.append(('SR', 'bear', 'Gần kháng cự mạnh'))

    # NHÓM 5: ICHI + BB (8%)
    if price > cloud_top: score += 5
    elif price < cloud_bottom: score -= 5
    
    if price <= bb_lower: score += 3
    elif price >= bb_upper: score -= 3

    three_in_one = (price > ma20 and vol_ratio >= 1.5 and price_up and 30 < rsi_val < 70)
    score = max(0, min(100, score))
    
    if score >= 65: action = 'MUA'
    elif score <= 35: action = 'BÁN'
    else: action = 'THEO DÕI'

    return {
        'price': round(price, 0),
        'score': score,
        'action': action,
        'signals': signals,
        'three_in_one': three_in_one,
        'stop_loss': round(price * 0.93, 0),
        'take_profit': round(price * 1.14, 0),
        # ... các trường khác giữ nguyên ...
    }

# ── API ROUTES ───────────────────────────────────────────────────────────

@app.route('/')
def index():
    return jsonify({'status': 'ok', 'version': 'v4'})

@app.route('/api/price/<symbol>')
def api_price(symbol):
    from datetime import datetime, timedelta
    # Logic fetch price lược giản để minh họa cấu trúc
    return jsonify({'symbol': symbol.upper(), 'price': 12345})

@app.route('/api/analyze/<symbol>')
def api_analyze(symbol):
    # Logic thực tế sẽ gọi fetch_analysis
    return jsonify({'symbol': symbol.upper(), 'status': 'analyzed'})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
