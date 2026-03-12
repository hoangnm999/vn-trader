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
    for source in['VCI', 'TCBS']:
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
    cc = find_col(df, ['close', 'closeprice', 'close_price'])
    hc = find_col(df, ['high', 'highprice', 'high_price'])
    lc = find_col(df, ['low', 'lowprice', 'low_price'])
    vc = find_col(df, ['volume', 'volume_match', 'klgd'])
    
    if cc is None:
        nums = df.select_dtypes(include='number').columns
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
        bottoms =[i for i in range(1, len(p) - 1) if p[i] < p[i - 1] and p[i] < p[i + 1]]
        tops =[i for i in range(1, len(p) - 1) if p[i] > p[i - 1] and p[i] > p[i + 1]]
        
        if len(bottoms) >= 2:
            b1, b2 = bottoms[-2], bottoms[-1]
            if p[b2] < p[b1] and r[b2] > r[b1] + 2:
                return 'bullish', ('Phan ky tang: Gia day moi (' + f'{p[b2]:,.0f}' + ') thap')
        
        if len(tops) >= 2:
            t1, t2 = tops[-2], tops[-1]
            if p[t2] > p[t1] and r[t2] < r[t1] - 2:
                return 'bearish', ('Phan ky giam: Gia dinh moi (' + f'{p[t2]:,.0f}' + ') cao')
                
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
        vol_msg = 'Vol ' + f'{vol_ratio:.1f}' + 'x TB + gia tang -> Dong tien lon vao! Xac nhan'
    elif vol_ratio >= 1.5 and not price_up:
        vol_signal = 'shark_sell'
        vol_msg = 'Vol ' + f'{vol_ratio:.1f}' + 'x TB + gia giam -> Dong tien lon xa! Tin hieu'
    elif vol_ratio < 0.7 and price_up:
        vol_signal = 'fake_rally'
        vol_msg = 'Gia tang nhung Vol thap ' + f'{vol_ratio:.1f}' + 'x TB -> Co the keo xa, can'
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
        levels =[]
        for i in range(window, len(h) - window):
            if h[i] == max(h[i - window:i + window + 1]):
                levels.append(('R', float(h[i])))
            if l[i] == min(l[i - window:i + window + 1]):
                levels.append(('S', float(l[i])))
                
        merged =[]
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
        sups = sorted([m for m in strong if m['price'] < price], key=lambda x: x['price'], reverse=True)
        ress = sorted([m for m in strong if m['price'] > price], key=lambda x: x['price'])[:3]
        return sups, ress

    supports, resistances = find_sr(highs, lows)
    score = 50
    signals =[]
    
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
        signals.append(('MA', 'bull', 'Gia>MA20(' + f'{ma20:,.0f}' + ')>MA50(' + f'{ma50:,.0f}' + ')'))
    elif price > ma20:
        score += 10
        signals.append(('MA', 'bull', 'Gia tren MA20 ' + f'{ma20:,.0f}' + ' -> Xu huong ngan'))
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
            signals.append(('SR', 'bull', 'Gia gan HT manh ' + f'{supports[0]["price"]:,.0f}'))
        elif dist_s < 4:
            score += 5
            signals.append(('SR', 'bull', 'HT gan: ' + f'{supports[0]["price"]:,.0f}' + ' (cach)'))
        else:
            signals.append(('SR', 'neutral', 'HT gan nhat: ' + f'{supports[0]["price"]:,.0f}'))

    if resistances:
        dist_r = (resistances[0]['price'] - price) / price * 100
        strength_r = resistances[0]['count']
        if dist_r < 1.5:
            pts = min(12, 6 + strength_r * 2)
            score -= pts
            signals.append(('SR', 'bear', 'Gia gan KC manh ' + f'{resistances[0]["price"]:,.0f}'))
        elif dist_r < 4:
            score -= 5
            signals.append(('SR', 'bear', 'KC gan: ' + f'{resistances[0]["price"]:,.0f}'))
        else:
            signals.append(('SR', 'neutral', 'KC gan nhat: ' + f'{resistances[0]["price"]:,.0f}'))

    if price > cloud_top:
        score += 5
        signals.append(('ICHI', 'bull', 'Gia tren may Ichimoku -> Xu huong tang'))
    elif price < cloud_bottom:
        score -= 5
        signals.append(('ICHI', 'bear', 'Gia duoi may Ichimoku -> Xu huong giam'))
    else:
        signals.append(('ICHI', 'neutral', 'Gia trong may -> Khong ro xu huong'))

    if price <= bb_lower:
        score += 3
        signals.append(('BB', 'bull', 'Gia cham BB duoi ' + f'{bb_lower:,.0f}' + ' -> Ho tro'))
    elif price >= bb_upper:
        score -= 3
        signals.append(('BB', 'bear', 'Gia cham BB tren ' + f'{bb_upper:,.0f}' + ' -> Khang cu'))
    else:
        signals.append(('BB', 'neutral', 'Gia trong BB (' + f'{bb_pct:.0f}' + '% trong dai)'))

    three_in_one = (price > ma20 and vol_ratio >= 1.5 and price_up and 30 < rsi_val < 70)
    score = max(0, min(100, score))
    
    if score >= 65:
        action = 'MUA'
    elif score <= 35:
        action = 'BAN'
    else:
        action = 'THEO DOI'
        
    return {
        'price': round(price, 0),
        'rsi': rsi_val,
        'rsi_divergence': {'type': div_type, 'message': div_msg},
        'macd': round(macd_val, 1),
        'macd_signal': round(macd_sig, 1),
        'macd_hist': round(macd_h, 1),
        'ma20': round(ma20, 0),
        'ma50': round(ma50, 0),
        'golden_cross': golden_cross,
        'death_cross': death_cross,
        'bb_upper': round(bb_upper, 0),
        'bb_lower': round(bb_lower, 0),
        'bb_pct': round(bb_pct, 1),
        'vol_today': int(vol_today),
        'vol_ma20': int(vol_ma20),
        'vol_ratio': round(vol_ratio, 2),
        'vol_signal': vol_signal,
        'ichimoku': ichi,
        'supports': supports,
        'resistances': resistances,
        'score': score,
        'action': action,
        'signals': signals,
        'three_in_one': three_in_one,
        'entry': round(price, 0),
        'stop_loss': round(price * 0.93, 0),
        'take_profit': round(price * 1.14, 0),
    }

def fetch_price(symbol):
    cached = get_cached('price_' + symbol)
    if cached:
        return cached
        
    from datetime import datetime, timedelta
    end = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
    
    for source in ['VCI', 'TCBS']:
        try:
            from vnstock import Vnstock
            df = Vnstock().stock(symbol=symbol, source=source).quote.history(
                start=start, end=end, interval='1D'
            )
            if df is None or df.empty:
                continue
                
            cc = find_col(df,['close', 'closeprice', 'close_price'])
            if cc is None:
                nums = df.select_dtypes(include='number').columns
                cc = nums[-1] if len(nums) > 0 else None
            if cc is None:
                continue
                
            close = float(df.iloc[-1][cc])
            if 0 < close < 1000:
                close *= 1000
            if close <= 0:
                continue
                
            chg = 0
            if len(df) >= 2:
                prev = float(df.iloc[-2][cc])
                if 0 < prev < 1000:
                    prev *= 1000
                if prev > 0:
                    chg = round((close - prev) / prev * 100, 2)
                    
            result = {'symbol': symbol, 'price': round(close, 0), 'change_pct': chg, 'source': source}
            set_cache('price_' + symbol, result)
            return result
        except Exception as e:
            logger.warning(f"{symbol}/{source}: {e}")
            
    return {'symbol': symbol, 'price': 0, 'change_pct': 0, 'source': 'error', 'error': 'Khong co du lieu'}

def fetch_analysis(symbol, price_override=None):
    cache_key = 'analysis_' + symbol + '_' + str(price_override or 'live')
    cached = get_cached(cache_key)
    if cached:
        return cached
        
    df, source = load_history(symbol, days=200)
    if df is None:
        return {'symbol': symbol, 'error': 'Khong tai duoc du lieu'}
        
    try:
        result = compute_indicators(df, price_override)
    except Exception as e:
        logger.error(f"compute {symbol}: {e}")
        return {'symbol': symbol, 'error': str(e)}
        
    if result is None:
        return {'symbol': symbol, 'error': 'Khong tinh duoc chi bao'}
        
    result['symbol'] = symbol
    result['source'] = source
    set_cache(cache_key, result)
    return result

@app.route('/')
def index():
    return jsonify({'status': 'ok', 'message': 'VN Trader API v4', 'weights': 'VOL35 RSI+DIV25'})

@app.route('/api/price/<symbol>')
def api_price(symbol):
    return jsonify(fetch_price(symbol.upper()))

@app.route('/api/analyze/<symbol>')
def api_analyze(symbol):
    return jsonify(fetch_analysis(symbol.upper()))

@app.route('/api/whatif/<symbol>/<int:target_price>')
def api_whatif(symbol, target_price):
    return jsonify(fetch_analysis(symbol.upper(), price_override=target_price))

@app.route('/api/market')
def api_market():
    result = {}
    for sym, name in[('VNINDEX', 'VN-INDEX'), ('HNX30', 'HNX-INDEX'), ('VN30F1M', 'VN30')]:
        d = fetch_price(sym)
        result[sym] = dict(d)
        result[sym]['name'] = name
        time.sleep(0.5)
    return jsonify(result)

WATCHLIST =[
    'VCB', 'BID', 'TCB', 'MBB', 'VPB',
    'VHM', 'VIC', 'NVL', 'PDR',
    'FPT', 'CMG',
    'HPG', 'HSG', 'NKG',
    'SSI', 'VND', 'HCM',
]

_bg_running = False

def start_background_cache():
    global _bg_running
    if _bg_running:
        return
    _bg_running = True
    
    def worker():
        time.sleep(15)
        while True:
            for sym in WATCHLIST:
                try:
                    fetch_analysis(sym)
                    time.sleep(1.5)
                except Exception as e:
                    logger.warning('cache ' + sym + ': ' + str(e))
            time.sleep(300)
            
    threading.Thread(target=worker, daemon=True).start()
    logger.info('Background cache started for ' + str(len(WATCHLIST)) + ' symbols')

@app.route('/api/signals')
def api_signals():
    start_background_cache()
    results =[]
    for sym in WATCHLIST:
        cached = get_cached('analysis_' + sym + '_live')
        if cached and 'score' in cached and 'error' not in cached:
            results.append(cached)
            
    if len(results) < 3:
        for sym in['VCB', 'HPG', 'FPT']:
            if any(r.get('symbol') == sym for r in results):
                continue
            try:
                r = fetch_analysis(sym)
                if r and 'score' in r and 'error' not in r:
                    results.append(r)
            except Exception:
                pass
                
    results.sort(key=lambda x: abs(x.get('score', 50) - 50), reverse=True)
    return jsonify(results[:3])

@app.route('/api/warmup')
def api_warmup():
    start_background_cache()
    return jsonify({'status': 'warming', 'watchlist': WATCHLIST})

@app.route('/api/debug/<symbol>')
def api_debug(symbol):
    try:
        df, source = load_history(symbol.upper(), days=10)
        if df is not None:
            return jsonify({'columns': list(df.columns), 'source': source, 'rows': len(df)})
        return jsonify({'error': 'No data'})
    except Exception as e:
        return jsonify({'error': str(e)})

# Auto-start background cache when Flask loads
threading.Thread(target=lambda: (time.sleep(20), start_background_cache()), daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
