import os
import time
import20 else h
        l = l[-120:] if len(l) > 120 logging
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
C else l
        for i in range(window, len(h) - window):
            if hORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)[i] == max(h[i - window:i + window + 1]):
                levels.append(('R

_cache = {}
CACHE_TTL = 300

def get_cached(key):', float(h[i])))
            if l[i] == min(l
    if key in _cache:
        data, ts = _cache[key]
        if time.time()[i - window:i + window + 1]):
                levels.append(('S', float(l[i]))) - ts < CACHE_TTL:
            return data
    return None

def set_cache(key,
                
        merged = []
        levels.sort(key=lambda x: x[1])
        for typ, lvl data):
    _cache[key] = (data, time.time())

def find_col(df, names):
    for c in df.columns:
        if c.lower() in names: in levels:
            found = False
            for m in merged:
                if abs(m['price']
            return c
    return None

def load_history(symbol, days=200):
    from datetime - lvl) / lvl < 0.015:
                    m['count'] += 1 import datetime, timedelta
    end = datetime.now().strftime('%Y-%m-%d')
    start =
                    found = True
                    break
            if not found:
                merged.append({'type': typ, 'price (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    for source': round(lvl, 0), 'count': 1})
                
        strong = in ['VCI', 'TCBS']:
        try:
            from vnstock import Vnstock
            df[m for m in merged if m['count'] >= 2]
        strong.sort(key=lambda x: x = Vnstock().stock(symbol=symbol, source=source).quote.history(
                start=start, end=end, interval='1D'
            )
            if df is not None and len(df)['count'], reverse=True)
        sups = sorted([m for m in strong if m['price'] < price], key=lambda x: x['price'], reverse=True)[:3]
        ress = sorted( >= 30:
                logger.info(f"{symbol}/{source} OK rows={len(df)}")[m for m in strong if m['price'] > price], key=lambda x: x['price'])[:3]
                return df, source
        except Exception as e:
            logger.warning(f"{symbol}/{source}: {e}")
    return None, None

def compute_indicators(df, price_override=None):
        return sups, ress
        
    supports, resistances = find_sr(highs, lows, window=5)
    # If no support found, try smaller window
    if not supports:
        supports, resistances_tmp = find_sr(highs, lows, window=3)
    if not resistances:
    import numpy as np
    
    cc = find_col(df,
        supports_tmp, resistances = find_sr(highs, lows, window=3)['close', 'closeprice', 'close_price'])
    hc = find_col(df,
        
    score = 50
    signals =['high', 'highprice', 'high_price'])
    lc = find_col(df,['low', 'lowprice', 'low_price'])
    vc = find_col(df,['volume', 'volume_match', 'klgd', 'vol', 'trading_volume', 'match_volu'])
    
    if cc is None:
        nums = df.select_dtypes(include='number').columns
        cc = nums[-1] if len(nums) > 0 else None
        
    if cc is None:
        return None
        
    import pandas as pd
    
    def to_float_arr(series):
        # Handle string numbers like "77.7" or "12722400"
        return pd.to_numeric(series, errors='coerce').fillna(0).astype(float).values
        
    closes = to_float_arr(df[cc])
    if closes.max() < 1000:
        closes *= 1000
        
    highs = to_float_arr(df[hc]) if hc else closes.copy()
    if hc and highs.max() < 1000:
        highs *= 1000
        
    lows = to_float_arr(df[lc]) if lc else closes.copy()
    if lc and lows.max() < 1000:
        lows *= 1000
        
    # Robust volume detection - handle string values from VCI
    volumes = np.zeros(len(closes))
    vol_col_found = None
    for try_col in['volume', 'volume_match', 'klgd', 'vol', 'trading_volume',
                    'match_volume', 'total_volume', 'dealVolume', 'matchingVolume']:
        fc = find_col(df, [try_col])
        if fc:
            v = pd.to_numeric(df[fc], errors='coerce').fillna(0).astype(float).values
            if v.max() > 1000:
                volumes = v
                vol_col_found = fc
                logger.info(f"Volume col: {fc} max={v.max():.[]
    
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
        signals.append(('RSI', 'bull', 'RSI=' + str(rsi_val) + ' Vung qua ban -&gt; Tim co ho'))
    elif rsi_val < 40:
        score += 7
        signals.append(('RSI', 'bull', 'RSI=' + str(rsi_val) + ' Vung yeu, dang hoi phuc'))
    elif rsi_val > 70:
        score -= 15
        signals.append(('RSI', 'bear', 'RSI=' + str(rsi_val) + ' Vung qua mua -&gt; KHONG mua'))
    elif rsi_val > 60:
        score -= 7
        signals.append(('RSI', 'bear', 'RSI=' + str(rsi_val) + ' Vung manh, than trong'))
    else:
        signals.append(('RSI', 'neutral', 'RSI=' + str(rsi_val) + ' Vung trung tinh'))
        
    if div_type == 'bullish':
        # RSI qua ban + phan ky tang = tin hieu MUA rat manh
        if rsi_val < 35:
            score += 15
            signals.append(('DIV', 'bull', div_msg + ' [RSI qua ban xac nhan!]'))0f}")
                break
                
    # Fallback: any column with large values
    if vol_
        else:
            score += 10
            signals.append(('DIV', 'bull', div_col_found is None:
        for col in df.columns:
            if col inmsg))
    elif div_type == 'bearish':
        # RSI qua mua + phan ky giam = tin hieu BAN rat manh
        if rsi_val > 65:
            score -=[cc, hc, lc]:
                continue
            v = pd.to_numeric(df[col], errors='coerce'). 15
            signals.append(('DIV', 'bear', div_msg + 'fillna(0).astype(float).values
            if v.max() > 100000[RSI qua mua xac nhan!]'))
        else:
            score -= 10
            signals.append(('DIV:
                volumes = v
                vol_col_found = col
                logger.info(f"Volume', 'bear', div_msg))
    else:
        signals.append(('DIV', 'neutral', ' fallback: {col} max={v.max():.0f}")
                break
                
    if volKhong phat hien phan ky RSI'))
        
    if golden_cross:
        score +=_col_found is None:
        logger.warning(f"No volume col found in: {list( 20
        signals.append(('MA', 'bull', 'GOLDEN CROSS! MA20 cat lendf.columns)}")
        
    price = float(price_override) if price_override else float(closes MA50 -&gt; Tang manh dai ha'))
    elif death_cross:
        score -= 2[-1])
    prev_close = float(closes[-2]) if len(closes) > 1 else0
        signals.append(('MA', 'bear', 'DEATH CROSS! MA20 cat xuong MA price
    
    def ema_arr(arr, span):
        alpha = 2.0 / (50 -&gt; Giam dai han!'))
    elif price > ma20 and ma20 >span + 1)
        out = np.zeros(len(arr))
        out[0] = ma50:
        score += 15
        signals.append(('MA', 'bull', 'G arr[0]
        for i in range(1, len(arr)):
            out[i] =ia&gt;MA20(' + f'{ma20:,.0f}' + ')&gt; alpha * arr[i] + (1 - alpha) * out[i - 1]
        return outMA50(' + f'{ma50:,.0f}' + ')'))
    elif price > ma
        
    def calc_rsi_arr(arr, p=14):
        out = np.20:
        score += 10
        signals.append(('MA', 'bull', 'Giafull(len(arr), 50.0)
        for i in range(p, len(arr tren MA20 ' + f'{ma20:,.0f}' + ' -&gt; Xu huong)):
            d = np.diff(arr[i - p:i + 1])
            g = ng'))
    elif price < ma20 and ma20 < ma50:
        score -= np.where(d > 0, d, 0.0)
            l = np.where( 15
        signals.append(('MA', 'bear', 'Gia&lt;MA20&ltd < 0, -d, 0.0)
            ag = np.mean(g);MA50 -&gt; Giam 2 tang - KHONG mua duoi'))
    else:
            al = np.mean(l)
            out[i] = 100.0 if al
        score -= 10
        signals.append(('MA', 'bear', 'Gia duoi MA2 == 0 else 100 - 100 / (1 + ag / al)
        return0 ' + f'{ma20:,.0f}' + ' - KHONG mua duoi!')) np.round(out, 1)
        
    rsi_series = calc_rsi_arr(closes
        
    if macd_val > macd_sig and macd_h > 0:)
    rsi_val = float(rsi_series[-1])
    
    def detect_divergence
        score += 3
        signals.append(('MACD', 'bull', 'MACD cat len Signal -&gt(price_arr, rsi_arr, lookback=20):
        if len(price_arr; Dong luc tang'))
    elif macd_val < macd_sig and macd_h <) < lookback:
            return 'none', ''
        p = price_arr[-lookback:] 0:
        score -= 3
        signals.append(('MACD', 'bear', 'MACD cat
        r = rsi_arr[-lookback:]
        bottoms = xuong Signal -&gt; Dong luc giam'))
    else:
        signals.append(('MACD',[i for i in range(1, len(p) - 1) if p[i] < p[i - 1] and p 'neutral', 'MACD=' + f'{macd_val:+.0f}'))
        
    if[i] < p[i + 1]]
        tops = supports:
        dist_s = (price - supports[0]['price']) / price * 100[i for i in range(1, len(p) - 1) if p[i] > p[i - 1] and p
        strength_s = supports[0]['count']
        if dist_s < 1.5:[i] > p[i + 1]]
        
        if len(bottoms) >= 2:
            pts = min(12, 6 + strength_s * 2)
            score += pts
            b1, b2 = bottoms[-2], bottoms[-1]
            if p[b2]
            signals.append(('SR', 'bull', 'Gia gan HT manh ' + f'{supports < p[b1] and r[b2] > r[b1] + 2:[0]["price"]:,.0f}'))
        elif dist_s < 4:
            score += 5
                return 'bullish', ('Phan ky tang: Gia day moi (' + f'{p[b2]:,.
            signals.append(('SR', 'bull', 'HT gan: ' + f'{supports[0]0f}' + ') thap')
                
        if len(tops) >= 2:
            t["price"]:,.0f}' + ' (ca'))
        else:
            signals.append(('SR', 'neutral1, t2 = tops[-2], tops[-1]
            if p[t2] > p', 'HT gan nhat: ' + f'{supports[0]["price"]:,.0f}'))[t1] and r[t2] < r[t1] - 2:
                return 'bear
            
    if resistances:
        dist_r = (resistances[0]['price'] - price) / priceish', ('Phan ky giam: Gia dinh moi (' + f'{p[t2]:,.0f * 100
        strength_r = resistances[0]['count']
        if dist_r <}' + ') cao')
                
        return 'none', ''
        
    div_type, div_ 1.5:
            pts = min(12, 6 + strength_r * 2)msg = detect_divergence(closes, rsi_series)
    
    ema12 = ema_
            score -= pts
            signals.append(('SR', 'bear', 'Gia gan KC manh ' +arr(closes, 12)
    ema26 = ema_arr(closes, 26) f'{resistances[0]["price"]:,.0f}'))
        elif dist_r < 4:
    macd_line = ema12 - ema26
    sig_line = ema_arr(macd
            score -= 5
            signals.append(('SR', 'bear', 'KC gan: ' + f_line, 9)
    macd_hist = macd_line - sig_line'{resistances[0]["price"]:,.0f}' + '  '))
        else:
            signals.append(('SR
    
    macd_val = float(macd_line[-1])
    macd_sig = float(sig_line', 'neutral', 'KC gan nhat: ' + f'{resistances[0]["price"]:,.0f[-1])
    macd_h = float(macd_hist[-1])
    
    ma20 =}'))
            
    if price > cloud_top:
        score += 5
        signals.append(('ICHI', 'bull', 'Gia tren may Ichimoku -&gt; Xu huong tang')) float(np.mean(closes[-20:]))
    ma50 = float(np.mean(closes[-min(50, len(closes)):]))
    ma20_prev = float(np
    elif price < cloud_bottom:
        score -= 5
        signals.append(('ICHI', '.mean(closes[-21:-1])) if len(closes) >= 21 else ma20bear', 'Gia duoi may Ichimoku -&gt; Xu huong giam'))
    else:
    ma50_prev = float(np.mean(closes[-51:-1])) if len(closes
        signals.append(('ICHI', 'neutral', 'Gia trong may -&gt; Khong ro xu huong'))
        
    if price <= bb_lower:
        score += 3
        signals.) >= 51 else ma50
    golden_cross = ma20_prev < ma50append(('BB', 'bull', 'Gia cham BB duoi ' + f'{bb_lower:,.0_prev and ma20 > ma50
    death_cross = ma20_prev > ma50_prev and ma20 < ma50
    
    bb_mid = float(np.meanf}' + ' -&gt; Ho t'))
    elif price >= bb_upper:
        score -=(closes[-20:]))
    bb_std = float(np.std(closes[-20:] 3
        signals.append(('BB', 'bear', 'Gia cham BB tren ' + f'{bb_))
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bbupper:,.0f}' + ' -&gt; Khan'))
    else:
        signals.append(('BB', 'neutral', 'Gia trong BB (' + f'{bb_pct:.0f}' + '% trong dai_mid - 2 * bb_std
    bb_pct = (price - bb_lower) / ()'))
        
    three_in_one = (price > ma20 and vol_ratio >=bb_upper - bb_lower) * 100 if bb_upper != bb_lower else 50
    
    vol_today = float(volumes[-1]) if len(volumes) > 0 else 1.5 and price_up and 30 < rsi_val < 70)
    score 0
    vol_ma20 = float(np.mean(volumes[-20:])) if len = max(0, min(100, score))
    
    if score >= 65:(volumes) >= 20 else vol_today
    vol_ratio = vol_today / vol_ma
        action = 'MUA'
    elif score <= 35:
        action = 'BAN'20 if vol_ma20 > 0 else 1.0
    price_up = price >=
    else:
        action = 'THEO DOI'
        
    # SL/TP/Rebuy prev_close
    
    if vol_ratio >= 1.5 and price_up:
        vol theo huong lenh
    # Thi truong VN khong co short selling
    # Tin hieu BAN_signal = 'shark_buy'
        vol_msg = 'Vol ' + f'{vol_ratio:. = nen ban co phieu dang nam + cho mua lai o vung ho tro
    if action == 'M1f}' + 'x TB + gia tang -> Dong tien lon vao! Xac nh'
    elifUA':
        stop_loss = round(price * 0.93, 0) # Cat lo vol_ratio >= 1.5 and not price_up:
        vol_signal = 'shark_sell -7%
        take_profit = round(price * 1.14, 0) # Ch'
        vol_msg = 'Vol ' + f'{vol_ratio:.1f}' + 'x TBot loi +14%
        sl_label = '-7%'
        tp_label = '+14 + gia giam -> Dong tien lon xa! Tin hie'
    elif vol_ratio < 0.%'
        rebuy_zone = None
    elif action == 'BAN':
        stop_loss = price7 and price_up:
        vol_signal = 'fake_rally'
        vol_msg = 'Gia tang nhung Vol thap ' + f'{vol_ratio:.1f}' + 'x TB -> # Gia nen ban = gia hien tai
        # Vung mua lai = vung ho tro gan nhat hoac -10%
        if supports:
            rebuy_zone = supports[0]['price'] Co the keo xa, c'
    elif vol_ratio >= 1.0 and price_up:
        else:
            rebuy_zone = round(price * 0.90, 0)
        vol_signal = 'normal_buy'
        vol_msg = 'Vol ' + f'{vol
        take_profit = rebuy_zone # Dung take_profit luu vung mua lai
        sl_ratio:.1f}' + 'x TB + gia tang -> Xu huong tang duoc xac nh'_label = 'Nen ban ngay'
        tp_label = 'Vung mua lai'
    else
    elif vol_ratio < 0.7 and not price_up:
        vol_signal = ':
        stop_loss = round(price * 0.93, 0)
        take_weak_sell'
        vol_msg = 'Vol thap ' + f'{vol_ratio:.1fprofit = round(price * 1.07, 0)
        rebuy_zone = None}' + 'x TB + gia giam -> Ap luc ban yeu'
    else:
        vol_
        sl_label = '-7% neu da mua'
        tp_label = '+7% tham khao'signal = 'normal'
        vol_msg = 'Vol binh thuong ' + f'{vol_ratio:.
        
    return {
        'price': round(price, 0),
        'rsi': rsi_1f}' + 'x TB'
        
    n = len(closes)
    tenkan = (val,
        'rsi_divergence': {'type': div_type, 'message': div_msg},
        'macd': round(macd_val, 1),
        'macd_signal': round(macdnp.max(highs[-9:]) + np.min(lows[-9:])) / 2 if n >= 9 else price
    kijun = (np.max(highs[-26:]) + np_sig, 1),
        'macd_hist': round(macd_h, 1),.min(lows[-26:])) / 2 if n >= 26 else price
    span
        'ma20': round(ma20, 0),
        'ma50': round(ma_a = (tenkan + kijun) / 2
    span_b = (np.max(50, 0),
        'golden_cross': golden_cross,
        'death_cross':highs[-52:]) + np.min(lows[-52:])) / 2 if n >= death_cross,
        'bb_upper': round(bb_upper, 0),
        'bb 52 else price
    cloud_top = round(max(float(span_a), float(span_lower': round(bb_lower, 0),
        'bb_pct': round(bb_pct_b)), 0)
    cloud_bottom = round(min(float(span_a), float(span_b)), 0)
    
    ichi = {
        'tenkan': round(float(, 1),
        'vol_today': int(vol_today),
        'vol_ma2tenkan), 0),
        'kijun': round(float(kijun), 0),0': int(vol_ma20),
        'vol_ratio': round(vol_ratio,
        'cloud_top': cloud_top,
        'cloud_bottom': cloud_bottom,
    } 2),
        'vol_signal': vol_signal,
        'ichimoku': ichi,
    
    def find_sr(h, l, window=5):
        levels =[]
        # Use last 120 candles for S/R detection
        h = h[-120:] if len
        'supports': supports,
        'resistances': resistances,
        'score': score,
        'action': action,
        'signals': signals,
        'three_in_one': three_in_(h) > 120 else h
        l = l[-120:] if len(lone,
        'entry': round(price, 0),
        'stop_loss': stop_loss) > 120 else l
        for i in range(window, len(h) - window):,
        'take_profit': take_profit,
        'sl_label': sl_label,
            if h[i] == max(h[i - window:i + window + 1]):
        'tp_label': tp_label,
    }

def fetch_price(symbol):
    cached
                levels.append(('R', float(h[i])))
            if l[i] == min(l = get_cached('price_' + symbol)
    if cached:
        return cached
        
    from[i - window:i + window + 1]):
                levels.append(('S', float(l datetime import datetime, timedelta
    end = datetime.now().strftime('%Y-%m-%d')
    start[i])))
                
        merged = []
        levels.sort(key=lambda x: x[1]) = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
        for typ, lvl in levels:
            found = False
            for m in merged:
                if
    for source in ['VCI', 'TCBS']:
        try:
            from vnstock import Vnstock abs(m['price'] - lvl) / lvl < 0.015:
                    m
            df = Vnstock().stock(symbol=symbol, source=source).quote.history(
                start=start, end=end, interval='1D'
            )
            if df is None or df.empty['count'] += 1
                    found = True
                    break
            if not found:
                merged.append({'type': typ, 'price': round(lvl, 0), 'count': 1}):
                continue
            cc = find_col(df, ['close', 'closeprice', 'close_price'])
                
        strong = [m for m in merged if m['count'] >= 2]
        strong.sort(key
            if cc is None:
                nums = df.select_dtypes(include='number').columns=lambda x: x['count'], reverse=True)
        sups = sorted(
                cc = nums[-1] if len(nums) > 0 else None
            if cc is None:[m for m in strong if m['price'] < price], key=lambda x: x['price'], reverse=True)[:3]
                continue
                
            close = float(df.iloc[-1][cc])
            if 0 <
        ress = sorted([m for m in strong if m['price'] > price], key=lambda x: x close < 1000:
                close *= 1000
            if close <= 0['price'])[:3]
        return sups, ress
        
    supports, resistances = find_sr:
                continue
                
            chg = 0
            if len(df) >= 2:(highs, lows, window=5)
    # If no support found, try smaller window
    if
                prev = float(df.iloc[-2][cc])
                if 0 < prev < 1 not supports:
        supports, resistances_tmp = find_sr(highs, lows, window=3)000:
                    prev *= 1000
                if prev > 0:
                    ch
    if not resistances:
        supports_tmp, resistances = find_sr(highs, lows, window=3)
        
    score = 50
    signals =g = round((close - prev) / prev * 100, 2)
                    
            result = {'symbol': symbol, 'price': round(close, 0), 'change_pct': chg, 'source': source}
            set_cache('price_' + symbol, result)
            return result
        except Exception as e:
            logger.warning(f"{symbol}/{source}: {e}")
            
    return {'symbol': symbol, 'price': 0, 'change_pct': 0, 'source': 'error', 'error': 'Khong'}

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
    return jsonify({'status': 'ok', 'message': 'VN Trader API v4', 'weights': 'VOL35 RSI+DIV2'})

@app.route('/api/price/<symbol>')
def api_price(symbol):
    return jsonify(fetch_price(symbol.upper()))

@app.route('/api/analyze/<symbol>')
def api_analyze(symbol):
    return jsonify(fetch_analysis(symbol.upper()))

@app.route('/api/whatif/<symbol>/<int:target_price>')
def api_whatif(symbol, target_price):
    return jsonify(fetch_analysis([]
    
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
        signals.append(('RSI', 'bull', 'RSI=' + str(rsi_val) + ' Vung qua ban -&gt; Tim co ho'))
    elif rsi_val < 40:
        score += 7
        signals.append(('RSI', 'bull', 'RSI=' + str(rsi_val) + ' Vung yeu, dang hoi phuc'))
    elif rsi_val > 70:
        score -= 15
        signals.append(('RSI', 'bear', 'RSI=' + str(rsi_val) + ' Vung qua mua -&gt; KHONG mua'))
    elif rsi_val > 60:
        score -= 7
        signals.append(('RSI', 'bear', 'RSI=' + str(rsi_val) + ' Vung manh, than trong'))
    else:
        signals.append(('RSI', 'neutral', 'RSI=' + str(rsi_val) + ' Vung trung tinh'))
        
    if div_type == 'bullish':
        # RSI qua ban + phan ky tang = tin hieu MUA rat manh
        if rsi_val < 35:
            score += 15
            signals.append(('DIV', 'bull', div_msg + 'symbol.upper(), price_override=target_price))

@app.route('/api/market')
def[RSI qua ban xac nhan!]'))
        else:
            score += 10
            signals.append(('DIV api_market():
    result = {}
    for sym, name in', 'bull', div_msg))
    elif div_type == 'bearish':
        # RSI qua mua + phan ky giam = tin hieu BAN rat manh
        if rsi_val > 6[('VNINDEX', 'VN-INDEX'), ('HNX30', 'HNX-INDEX'), ('VN30F1M', 'VN30')]:
        d = fetch_price(sym)
        result[sym] = dict(d5:
            score -= 15
            signals.append(('DIV', 'bear', div_msg + ' [RSI qua mua xac nhan!]'))
        else:
            score -= 10)
        result[sym]['name'] = name
        time.sleep(0.5)
    return
            signals.append(('DIV', 'bear', div_msg))
    else:
        signals.append(('DIV', 'neutral', 'Khong phat hien phan ky RSI'))
        
    if golden_ jsonify(result)

WATCHLIST =cross:
        score += 20
        signals.append(('MA', 'bull', 'GOLDEN CROSS! MA20 cat len MA50 -&gt; Tang manh dai ha'))
    elif death_cross:[
    'VCB', 'BID', 'TCB', 'MBB', 'VPB',
    'VHM', 'VIC', 'NVL', 'PDR',
    'FPT', 'CMG',
    'HPG', 'HSG', 'NKG',
    'SSI', 'VND', 'HCM',
]

_bg_running = False

def start_
        score -= 20
        signals.append(('MA', 'bear', 'DEATH CROSS! MAbackground_cache():
    global _bg_running
    if _bg_running:
        return20 cat xuong MA50 -&gt; Giam dai han!'))
    elif price > ma2
    _bg_running = True
    import threading
    
    def worker():
        time.sleep(10 and ma20 > ma50:
        score += 15
        signals.append(('MA5)
        while True:
            for sym in WATCHLIST:
                try:
                    fetch_analysis', 'bull', 'Gia&gt;MA20(' + f'{ma20:,.0f(sym)
                    time.sleep(1.5)
                except Exception as e:
                    logger.}' + ')&gt;MA50(' + f'{ma50:,.0f}' + ')'))warning('cache ' + sym + ': ' + str(e))
            time.sleep(300)
    elif price > ma20:
        score += 10
        signals.append(('MA',
            
    threading.Thread(target=worker, daemon=True).start()
    logger.info('Background cache started for ' + str(len(WATCHLIST)) + ' symbols')

@app.route('/api 'bull', 'Gia tren MA20 ' + f'{ma20:,.0f}' + '/signals')
def api_signals():
    start_background_cache()
    results = -&gt; Xu huong ng'))
    elif price < ma20 and ma20 < ma50:
        score -= 15
        signals.append(('MA', 'bear', 'Gia&lt[]
    for sym in WATCHLIST:
        cached = get_cached('analysis_' + sym + '_live')
        if cached and 'score' in cached and 'error' not in cached:
            results.append(cached)
            
    if len(results) < 3:
        for sym in;MA20&lt;MA50 -&gt; Giam 2 tang - KHONG mua duoi'))
    else:
        score -= 10
        signals.append(('MA', 'bear', '['VCB', 'HPG', 'FPT']:
            if any(r.get('symbol') == sym for r in results):Gia duoi MA20 ' + f'{ma20:,.0f}' + ' - KHONG
                continue
            try:
                r = fetch_analysis(sym)
                if r and 'score' in r and 'error' not in r:
                    results.append(r)
            except Exception: mua duoi!'))
        
    if macd_val > macd_sig and macd_h > 0:
        score += 3
        signals.append(('MACD', 'bull', 'MAC
                pass
                
    results.sort(key=lambda x: abs(x.get('score',D cat len Signal -&gt; Dong luc tang'))
    elif macd_val < macd_sig and 50) - 50), reverse=True)
    return jsonify(results[:3])

@ macd_h < 0:
        score -= 3
        signals.append(('MACD', 'app.route('/api/warmup')
def api_warmup():
    start_background_cache()bear', 'MACD cat xuong Signal -&gt; Dong luc giam'))
    else:
        signals
    return jsonify({'status': 'warming', 'watchlist': WATCHLIST})

@app.route('/api/.append(('MACD', 'neutral', 'MACD=' + f'{macd_val:+.0f}'))
        
    if supports:
        dist_s = (price - supports[0]['price']) /clearcache')
def api_clearcache():
    _cache.clear()
    start_background_cache()
    return jsonify({'status': 'cache cleared', 'msg': 'Rebuilding in background...'}) price * 100
        strength_s = supports[0]['count']
        if dist_s

@app.route('/api/debug/<symbol>')
def api_debug(symbol):
    sym = < 1.5:
            pts = min(12, 6 + strength_s * 2 symbol.upper()
    result = {'symbol': sym, 'attempts':)
            score += pts
            signals.append(('SR', 'bull', 'Gia gan HT manh ' + f'{supports[0]["price"]:,.0f}'))
        elif dist_s < 4:
            score += 5
            signals.append(('SR', 'bull', 'HT gan: ' + f'{supports[0]["price"]:,.0f}' + ' (ca'))
        else:
            signals.[]}
    from datetime import datetime, timedelta
    end = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    for source in ['VCI', 'TCBS']:
        try:
            from vnstock import Vnstockappend(('SR', 'neutral', 'HT gan nhat: ' + f'{supports[0]["price"]:,.
            df = Vnstock().stock(symbol=sym, source=source).quote.history(
                start=start0f}'))
            
    if resistances:
        dist_r = (resistances[0]['price'] - price) / price * 100
        strength_r = resistances[0]['count']
        if dist_r < 1.5:
            pts = min(12, 6 + strength_r * 2)
            score -= pts
            signals.append(('SR', 'bear', 'Gia gan KC manh ' + f'{resistances[0]["price"]:,.0f}'))
        elif dist, end=end, interval='1D'
            )
            if df is not None and not df._r < 4:
            score -= 5
            signals.append(('SR', 'bear', 'KC gan: ' + f'{resistances[0]["price"]:,.0f}' + ' '))
        else:
            signals.append(('SR', 'neutral', 'KC gan nhat: ' + f'{resistances[0]["price"]:,.0f}'))
            
    if price > cloud_top:
        score += 5
        signals.append(('ICHI', 'bull', 'Gia tren may Ichimoku -&gtempty:
                row = df.tail(1).to_dict(orient='records')[0]
                result['attempts'].append({
                    'source': source,
                    'status': 'OK',
                    'rows': len(df),
                    'columns': list(df.columns),
                    'last_row': {k: str(v) for k, v in row.items()}
                })
            else:
                result['attempts'].append({'source': source, 'status': 'empty'})
        except Exception as e:
            result['attempts'].append({'source': source, 'status': 'error', 'msg': str(e)[:20]})
            
    return jsonify(result)

# Auto-start background cache when Flask loads
import threading
threading.Thread(target=lambda: (time.sleep(20), start_background_cache()), daemon=True).start()

if __name__ == '__main__':
    port = int(os.; Xu huong tang'))
    elif price < cloud_bottom:
        score -= 5
        signals.append(('ICHI', 'bear', 'Gia duoi may Ichimoku -&gt; Xu huongenviron.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
