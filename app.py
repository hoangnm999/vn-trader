import os
import time
import logging
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cache đơn giản tránh gọi API quá nhiều
_cache = {}
CACHE_TTL = 60  # giây

def get_cached(key):
    if key in _cache:
        data, ts = _cache[key]
        if time.time() - ts < CACHE_TTL:
            return data
    return None

def set_cache(key, data):
    _cache[key] = (data, time.time())

# ── Lấy giá 1 cổ phiếu ───────────────────────────────────────────────────────
def fetch_price(symbol: str) -> dict:
    cached = get_cached(f"price_{symbol}")
    if cached:
        return cached

    try:
        from vnstock import Vnstock
        stk = Vnstock().stock(symbol=symbol, source='vci')
        df = stk.quote.intraday(symbol=symbol, page_size=10)
        
        if df is not None and not df.empty:
            latest = df.iloc[-1]
            # Thử các tên cột khác nhau của vnstock
            price = 0
            for col in ['price', 'lastPrice', 'close', 'matchPrice']:
                if col in df.columns and latest[col] > 0:
                    price = float(latest[col])
                    break
            
            # Nếu giá nhỏ hơn 1000 → đang ở đơn vị nghìn đồng
            if 0 < price < 1000:
                price = price * 1000

            result = {
                'symbol': symbol,
                'price': price,
                'change_pct': 0,
                'source': 'vci_intraday'
            }
            set_cache(f"price_{symbol}", result)
            return result
    except Exception as e:
        logger.warning(f"Intraday failed for {symbol}: {e}")

    # Fallback: dùng dữ liệu lịch sử ngày hôm nay
    try:
        from vnstock import Vnstock
        from datetime import datetime, timedelta
        stk = Vnstock().stock(symbol=symbol, source='vci')
        end = datetime.now().strftime('%Y-%m-%d')
        start = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
        df = stk.quote.history(start=start, end=end, interval='1D')

        if df is not None and not df.empty:
            latest = df.iloc[-1]
            close = float(latest.get('close', latest.get('Close', 0)))
            if 0 < close < 1000:
                close *= 1000

            prev_close = float(df.iloc[-2].get('close', df.iloc[-2].get('Close', close))) if len(df) > 1 else close
            if 0 < prev_close < 1000:
                prev_close *= 1000

            change_pct = ((close - prev_close) / prev_close * 100) if prev_close > 0 else 0

            result = {
                'symbol': symbol,
                'price': close,
                'change_pct': round(change_pct, 2),
                'source': 'vci_history'
            }
            set_cache(f"price_{symbol}", result)
            return result
    except Exception as e:
        logger.error(f"History also failed for {symbol}: {e}")

    return {'symbol': symbol, 'price': 0, 'change_pct': 0, 'source': 'error', 'error': 'Không lấy được giá'}

# ── Phân tích kỹ thuật ────────────────────────────────────────────────────────
def compute_rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50
    import numpy as np
    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_g = np.mean(gains[-period:])
    avg_l = np.mean(losses[-period:])
    if avg_l == 0:
        return 100
    rs = avg_g / avg_l
    return round(100 - 100 / (1 + rs), 1)

def fetch_analysis(symbol: str) -> dict:
    cached = get_cached(f"analysis_{symbol}")
    if cached:
        return cached

    try:
        from vnstock import Vnstock
        from datetime import datetime, timedelta
        import numpy as np

        stk = Vnstock().stock(symbol=symbol, source='vci')
        end = datetime.now().strftime('%Y-%m-%d')
        start = (datetime.now() - timedelta(days=120)).strftime('%Y-%m-%d')
        df = stk.quote.history(start=start, end=end, interval='1D')

        if df is None or df.empty or len(df) < 30:
            return {'symbol': symbol, 'error': 'Không đủ dữ liệu lịch sử'}

        # Chuẩn hóa tên cột
        col_map = {}
        for c in df.columns:
            cl = c.lower()
            if 'close' in cl: col_map['close'] = c
            elif 'open' in cl: col_map['open'] = c
            elif 'high' in cl: col_map['high'] = c
            elif 'low' in cl: col_map['low'] = c
            elif 'vol' in cl: col_map['volume'] = c

        closes = df[col_map.get('close','close')].astype(float).values
        if closes.max() < 1000:
            closes = closes * 1000

        price = closes[-1]
        rsi = compute_rsi(closes)

        # EMA20, SMA50
        ema20 = float(np.mean(closes[-20:]))  # simplified
        sma50 = float(np.mean(closes[-50:])) if len(closes) >= 50 else float(np.mean(closes))

        # MACD
        ema12 = float(closes[-12:].mean())
        ema26 = float(closes[-26:].mean()) if len(closes) >= 26 else ema12
        macd = ema12 - ema26

        # Bollinger Bands
        bb_mid = float(closes[-20:].mean())
        bb_std = float(closes[-20:].std())
        bb_upper = bb_mid + 2 * bb_std
        bb_lower = bb_mid - 2 * bb_std

        # Tín hiệu
        score = 50
        signals = []
        if rsi < 30:
            score += 20; signals.append('RSI quá bán → MUA')
        elif rsi > 70:
            score -= 20; signals.append('RSI quá mua → BÁN')
        if price > ema20:
            score += 10; signals.append('Giá trên EMA20 → tích cực')
        else:
            score -= 10; signals.append('Giá dưới EMA20 → tiêu cực')
        if macd > 0:
            score += 10; signals.append('MACD dương → xu hướng tăng')
        if price < bb_lower:
            score += 10; signals.append('Giá chạm BB dưới → MUA')
        elif price > bb_upper:
            score -= 10; signals.append('Giá chạm BB trên → BÁN')

        score = max(0, min(100, score))
        if score >= 65:
            action = 'MUA'
        elif score <= 35:
            action = 'BÁN'
        else:
            action = 'THEO DÕI'

        result = {
            'symbol': symbol,
            'price': round(price, 0),
            'rsi': rsi,
            'macd': round(macd, 1),
            'ema20': round(ema20, 0),
            'sma50': round(sma50, 0),
            'bb_upper': round(bb_upper, 0),
            'bb_lower': round(bb_lower, 0),
            'score': score,
            'action': action,
            'signals': signals,
            'entry': round(price, 0),
            'stop_loss': round(price * 0.95, 0),
            'take_profit': round(price * 1.10, 0),
        }
        set_cache(f"analysis_{symbol}", result)
        return result

    except Exception as e:
        logger.error(f"Analysis error {symbol}: {e}")
        return {'symbol': symbol, 'error': str(e)}

# ── Routes ────────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return jsonify({'status': 'ok', 'message': 'VN Trader API đang chạy ✅'})

@app.route('/api/price/<symbol>')
def api_price(symbol):
    return jsonify(fetch_price(symbol.upper()))

@app.route('/api/prices')
def api_prices():
    symbols = ['VCB', 'HPG', 'VHM', 'FPT', 'MWG', 'TCB', 'BID', 'VNM']
    results = {}
    for sym in symbols:
        results[sym] = fetch_price(sym)
        time.sleep(0.3)  # tránh rate limit
    return jsonify(results)

@app.route('/api/analyze/<symbol>')
def api_analyze(symbol):
    return jsonify(fetch_analysis(symbol.upper()))

@app.route('/api/market')
def api_market():
    # Chỉ số thị trường — vnstock 3.2.0
    try:
        from vnstock import Vnstock
        result = {}
        for idx_sym, idx_name in [('VNINDEX','VN-INDEX'), ('HNX','HNX-INDEX'), ('VN30','VN30')]:
            try:
                d = fetch_price(idx_sym)
                result[idx_sym] = {**d, 'name': idx_name}
            except:
                result[idx_sym] = {'name': idx_name, 'price': 0, 'change_pct': 0}
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/signals')
def api_signals():
    watchlist = ['VCB', 'HPG', 'FPT', 'MWG', 'TCB', 'VHM', 'BID', 'VNM', 'GAS', 'CTG']
    signals = []
    for sym in watchlist:
        try:
            r = fetch_analysis(sym)
            if 'score' in r:
                signals.append(r)
            time.sleep(0.5)
        except:
            pass
    signals.sort(key=lambda x: abs(x.get('score', 50) - 50), reverse=True)
    return jsonify(signals[:5])

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
