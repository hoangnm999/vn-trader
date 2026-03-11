bash

cat > /mnt/user-data/outputs/app.py << 'ENDOFFILE'
import os, time, logging
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_cache = {}
CACHE_TTL = 120

def get_cached(key):
    if key in _cache:
        data, ts = _cache[key]
        if time.time() - ts < CACHE_TTL:
            return data
    return None

def set_cache(key, data):
    _cache[key] = (data, time.time())

def fetch_price(symbol: str) -> dict:
    cached = get_cached(f"price_{symbol}")
    if cached: return cached
    from datetime import datetime, timedelta
    end = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
    for source in ['TCBS', 'VCI']:
        try:
            from vnstock import Vnstock
            df = Vnstock().stock(symbol=symbol, source=source).quote.history(start=start, end=end, interval='1D')
            if df is None or df.empty: continue
            close_col = next((c for c in df.columns if c.lower() in ['close','closeprice','close_price']), None)
            if close_col is None:
                nums = df.select_dtypes(include='number').columns
                close_col = nums[-1] if len(nums) > 0 else None
            if close_col is None: continue
            close = float(df.iloc[-1][close_col])
            if 0 < close < 1000: close *= 1000
            if close <= 0: continue
            change_pct = 0
            if len(df) >= 2:
                prev = float(df.iloc[-2][close_col])
                if 0 < prev < 1000: prev *= 1000
                if prev > 0: change_pct = round((close - prev) / prev * 100, 2)
            result = {'symbol': symbol, 'price': round(close, 0), 'change_pct': change_pct, 'source': source}
            set_cache(f"price_{symbol}", result)
            return result
        except Exception as e:
            logger.warning(f"{symbol}/{source}: {e}")
    return {'symbol': symbol, 'price': 0, 'change_pct': 0, 'source': 'error', 'error': 'Không lấy được giá'}

def fetch_analysis(symbol: str) -> dict:
    cached = get_cached(f"analysis_{symbol}")
    if cached: return cached
    from datetime import datetime, timedelta
    import numpy as np
    end = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=150)).strftime('%Y-%m-%d')
    df, used_source = None, None
    for source in ['TCBS', 'VCI']:
        try:
            from vnstock import Vnstock
            _df = Vnstock().stock(symbol=symbol, source=source).quote.history(start=start, end=end, interval='1D')
            if _df is not None and len(_df) >= 20:
                df, used_source = _df, source; break
        except Exception as e:
            logger.warning(f"Analysis {symbol}/{source}: {e}")
    if df is None: return {'symbol': symbol, 'error': 'Không tải được dữ liệu lịch sử'}
    close_col = next((c for c in df.columns if c.lower() in ['close','closeprice','close_price']), None)
    if close_col is None:
        nums = df.select_dtypes(include='number').columns
        close_col = nums[-1] if len(nums) > 0 else None
    if close_col is None: return {'symbol': symbol, 'error': 'Không tìm được cột giá'}
    closes = df[close_col].astype(float).values
    if closes.max() < 1000: closes *= 1000
    price = float(closes[-1])
    def calc_rsi(arr, p=14):
        if len(arr) < p+1: return 50.0
        d=np.diff(arr); g=np.where(d>0,d,0.0); l=np.where(d<0,-d,0.0)
        ag=np.mean(g[-p:]); al=np.mean(l[-p:])
        return 100.0 if al==0 else round(100-100/(1+ag/al),1)
    rsi_val = calc_rsi(closes)
    ema20 = float(np.mean(closes[-20:]))
    sma50 = float(np.mean(closes[-min(50,len(closes)):]))
    ema12 = float(np.mean(closes[-12:]))
    ema26 = float(np.mean(closes[-26:])) if len(closes)>=26 else ema12
    macd = ema12 - ema26
    bb_mid = float(np.mean(closes[-20:]))
    bb_std = float(np.std(closes[-20:]))
    bb_upper = bb_mid + 2*bb_std; bb_lower = bb_mid - 2*bb_std
    score = 50; signals = []
    if rsi_val < 30:   score += 20; signals.append('RSI < 30: Vùng quá bán → MUA')
    elif rsi_val > 70: score -= 20; signals.append('RSI > 70: Vùng quá mua → BÁN')
    else:              signals.append(f'RSI = {rsi_val}: Vùng trung tính')
    if price > ema20:  score += 10; signals.append('Giá trên EMA20 → xu hướng tăng')
    else:              score -= 10; signals.append('Giá dưới EMA20 → xu hướng giảm')
    if macd > 0:       score += 10; signals.append('MACD dương → động lực tăng')
    else:              score -= 5;  signals.append('MACD âm → động lực giảm')
    if price < bb_lower:   score += 10; signals.append('Giá chạm dải BB dưới → MUA')
    elif price > bb_upper: score -= 10; signals.append('Giá chạm dải BB trên → BÁN')
    score = max(0, min(100, score))
    action = 'MUA' if score >= 65 else ('BÁN' if score <= 35 else 'THEO DÕI')
    result = {
        'symbol':symbol,'price':round(price,0),'rsi':rsi_val,'macd':round(macd,1),
        'ema20':round(ema20,0),'sma50':round(sma50,0),
        'bb_upper':round(bb_upper,0),'bb_lower':round(bb_lower,0),
        'score':score,'action':action,'signals':signals,'source':used_source,
        'entry':round(price,0),'stop_loss':round(price*0.95,0),'take_profit':round(price*1.10,0),
    }
    set_cache(f"analysis_{symbol}", result)
    return result

@app.route('/')
def index(): return jsonify({'status':'ok','message':'VN Trader API ✅'})

@app.route('/api/price/<symbol>')
def api_price(symbol): return jsonify(fetch_price(symbol.upper()))

@app.route('/api/prices')
def api_prices():
    results = {}
    for sym in ['VCB','HPG','VHM','FPT','MWG','TCB','BID','VNM']:
        results[sym] = fetch_price(sym); time.sleep(0.8)
    return jsonify(results)

@app.route('/api/analyze/<symbol>')
def api_analyze(symbol): return jsonify(fetch_analysis(symbol.upper()))

@app.route('/api/market')
def api_market():
    result = {}
    for sym, name in [('VNINDEX','VN-INDEX'),('HNX30','HNX-INDEX'),('VN30F1M','VN30')]:
        d = fetch_price(sym); result[sym] = {**d,'name':name}; time.sleep(0.5)
    return jsonify(result)

@app.route('/api/signals')
def api_signals():
    signals = []
    for sym in ['HPG','FPT','MWG','TCB','VHM']:
        try:
            r = fetch_analysis(sym)
            if 'score' in r: signals.append(r)
            time.sleep(1.5)
        except: pass
    signals.sort(key=lambda x: abs(x.get('score',50)-50), reverse=True)
    return jsonify(signals)

@app.route('/api/debug/<symbol>')
def api_debug(symbol):
    try:
        from vnstock import Vnstock
        from datetime import datetime, timedelta
        stk = Vnstock().stock(symbol=symbol.upper(), source='TCBS')
        df = stk.quote.history(start=(datetime.now()-timedelta(days=5)).strftime('%Y-%m-%d'), end=datetime.now().strftime('%Y-%m-%d'), interval='1D')
        if df is not None and not df.empty:
            return jsonify({'columns':list(df.columns),'sample':df.tail(3).to_dict(orient='records'),'rows':len(df)})
        return jsonify({'error':'No data'})
    except Exception as e: return jsonify({'error':str(e)})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
ENDOFFILE
echo "Done"
Output

Done
