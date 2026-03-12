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
 out = np.zeros(len(arr)); out[0] = arr[0]
 for i in range(1, len(arr)):
 out[i] = alpha * arr[i] + (1 - alpha) * out[i-1]
 return out
 # =========================================================================
 # TÍNH TOÁN CHỈ SỐ
 # =========================================================================
 # ── RSI series ────────────────────────────────────────────────────────────
 def calc_rsi_arr(arr, p=14):
 out = np.full(len(arr), 50.0)
 for i in range(p, len(arr)):
 d = np.diff(arr[i-p:i+1])
 g = np.where(d > 0, d, 0.); l = np.where(d < 0, -d, 0.)
 ag = np.mean(g); al = np.mean(l)
 out[i] = 100. if al == 0 else 100 - 100 / (1 + ag / al)
 return np.round(out, 1)
 rsi_series = calc_rsi_arr(closes)
 rsi_val = float(rsi_series[-1])
 # ── RSI Phân kỳ ───────────────────────────────────────────────────────────
 def detect_divergence(price_arr, rsi_arr, lookback=20):
 if len(price_arr) < lookback:
 return 'none', ''
 p = price_arr[-lookback:]; r = rsi_arr[-lookback:]
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
 return 'bearish', (f' Phân kỳ giảm: Giá đỉnh mới ({p[t2]:,.0f}) cao hơn '
 f'nhưng RSI ({r[t2]:.0f}) thấp hơn → Cảnh báo đảo chiều!')
 return 'none', ''
 div_type, div_msg = detect_divergence(closes, rsi_series)
 # ── MACD ──────────────────────────────────────────────────────────────────
 ema12 = ema_arr(closes, 12); ema26 = ema_arr(closes, 26)
 macd_line = ema12 - ema26; sig_line = ema_arr(macd_line, 9)
 macd_hist = macd_line - sig_line
 macd_val = float(macd_line[-1]); macd_sig = float(sig_line[-1]); macd_h = float(macd_his # ── MA20 & MA50 ───────────────────────────────────────────────────────────
 ma20 = float(np.mean(closes[-20:]))
 ma50 = float(np.mean(closes[-min(50, len(closes)):]))
 ma20_prev = float(np.mean(closes[-21:-1])) if len(closes) >= 21 else ma20
 ma50_prev = float(np.mean(closes[-51:-1])) if len(closes) >= 51 else ma50
 golden_cross = ma20_prev < ma50_prev and ma20 > ma50
 death_cross = ma20_prev > ma50_prev and ma20 < ma50
 # ── Bollinger Bands ───────────────────────────────────────────────────────
 bb_mid = float(np.mean(closes[-20:]))
 bb_std = float(np.std(closes[-20:]))
 bb_upper = bb_mid + 2 * bb_std; bb_lower = bb_mid - 2 * bb_std
 bb_pct = (price - bb_lower) / (bb_upper - bb_lower) * 100 if bb_upper != bb_lower else
 # ── Volume thông minh ─────────────────────────────────────────────────────
 vol_today = float(volumes[-1]) if len(volumes) > 0 else 0
 vol_ma20 = float(np.mean(volumes[-20:])) if len(volumes) >= 20 else vol_today
 vol_ratio = vol_today / vol_ma20 if vol_ma20 > 0 else 1.0
 price_up = price >= prev_close
 if vol_ratio >= 1.5 and price_up:
 vol_signal = 'shark_buy'
 vol_msg = f' Dòng tiền lớn vào! Vol {vol_ratio:.1f}x TB + giá tăng → Xác nhận MU elif vol_ratio >= 1.5 and not price_up:
 vol_signal = 'shark_sell'
 vol_msg = f' Dòng tiền lớn xả! Vol {vol_ratio:.1f}x TB + giá giảm → Tín hiệu BÁN elif vol_ratio < 0.7 and price_up:
 vol_signal = 'fake_rally'
 vol_msg = f' Giá tăng nhưng Vol thấp {vol_ratio:.1f}x TB → Có thể "kéo xả", cẩn  elif vol_ratio >= 1.0 and price_up:
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
 sups = sorted([m for m in strong if m['price'] < price], key=lambda x: x['price'], re ress = sorted([m for m in strong if m['price'] > price], key=lambda x: x['price'])[:3 return sups, ress
 supports, resistances = find_sr(highs, lows)
 # =========================================================================
 # CHẤM ĐIỂM THEO TRỌNG SỐ MỚI
 #
 # Tổng 100 điểm phân bổ theo 5 nhóm:
 # Volume thông minh : 35% → max ±35 điểm
 # RSI + RSI Phân kỳ : 25% → max ±25 điểm
 # Trend MA20/MA50 : 20% → max ±20 điểm
 # Hỗ trợ & Kháng cự : 12% → max ±12 điểm
 # Ichimoku + BB : 8% → max ±8 điểm
 # =========================================================================
 score = 50 # điểm khởi đầu trung tính
 signals = []
 # ── NHÓM 1: VOLUME (35%) ─────────────────────────────────────────────────
 # Điểm max: +35 (shark_buy) / -35 (shark_sell)
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
 score += 5 # áp lực bán yếu → hơi tích cực
 signals.append(('VOL', 'neutral', vol_msg))
 else:
 signals.append(('VOL', 'neutral', vol_msg))
 # ── NHÓM 2: RSI + RSI PHÂN KỲ (25%) ─────────────────────────────────────
 # RSI cơ bản: max ±15 | RSI Phân kỳ (bonus/penalty): max ±10
 if rsi_val < 30:
 score += 15
 signals.append(('RSI', 'bull', f'RSI={rsi_val} — Vùng quá bán → Tìm cơ hội MUA'))
 elif rsi_val < 40:
 score += 7
 signals.append(('RSI', 'bull', f'RSI={rsi_val} — Vùng yếu, đang hồi phục'))
 elif rsi_val > 70:
 score -= 15
 signals.append(('RSI', 'bear', f'RSI={rsi_val} — Vùng quá mua → KHÔNG mua đuổi!'))
 elif rsi_val > 60:
 score -= 7
 signals.append(('RSI', 'bear', f'RSI={rsi_val} — Vùng mạnh, thận trọng'))
 else:
 signals.append(('RSI', 'neutral', f'RSI={rsi_val} — Vùng trung tính'))
 # RSI Phân kỳ — bonus/penalty khi xuất hiện
 if div_type == 'bullish':
 score += 10
 signals.append(('DIV', 'bull', div_msg))
 elif div_type == 'bearish':
 score -= 10
 signals.append(('DIV', 'bear', div_msg))
 else:
 signals.append(('DIV', 'neutral', 'Không phát hiện phân kỳ RSI'))
 # ── NHÓM 3: TREND MA20/MA50 (20%) ────────────────────────────────────────
 # Golden/Death Cross: ±20 | Các trường hợp thường: max ±15
 if golden_cross:
 score += 20
 signals.append(('MA', 'bull', f' GOLDEN CROSS! MA20 cắt lên MA50 → Tín hiệu tăng dà elif death_cross:
 score -= 20
 signals.append(('MA', 'bear', f' DEATH CROSS! MA20 cắt xuống MA50 → Tín hiệu giảm d elif price > ma20 and ma20 > ma50:
 score += 15
 signals.append(('MA', 'bull', f'Giá > MA20({ma20:,.0f}) > MA50({ma50:,.0f}) → Xu hướn elif price > ma20:
 score += 10
 signals.append(('MA', 'bull', f'Giá {price:,.0f} trên MA20 {ma20:,.0f} → Xu hướng ngắ elif price < ma20 and ma20 < ma50:
 score -= 15
 signals.append(('MA', 'bear', f'Giá < MA20({ma20:,.0f}) < MA50({ma50:,.0f}) → Giảm 2  else:
 score -= 10
 signals.append(('MA', 'bear', f'Giá {price:,.0f} dưới MA20 {ma20:,.0f} — KHÔNG mua đu # MACD hỗ trợ thêm cho nhóm Trend (không tính trọng số riêng)
 if macd_val > macd_sig and macd_h > 0:
 score += 3
 signals.append(('MACD', 'bull', f'MACD cắt lên Signal → Xác nhận động lực tăng'))
 elif macd_val < macd_sig and macd_h < 0:
 score -= 3
 signals.append(('MACD', 'bear', f'MACD cắt xuống Signal → Xác nhận động lực giảm'))
 else:
 signals.append(('MACD', 'neutral', f'MACD={macd_val:+.0f} (Signal={macd_sig:+.0f})'))
 # ── NHÓM 4: HỖ TRỢ & KHÁNG CỰ (12%) ─────────────────────────────────────
 # Gần vùng HT mạnh: +12 | Gần vùng KC mạnh: -12
 if supports:
 dist_s = (price - supports[0]['price']) / price * 100
 strength_s = supports[0]['count']
 if dist_s < 1.5:
 pts = min(12, 6 + strength_s * 2)
 score += pts
 signals.append(('SR', 'bull', f'Giá rất gần HT mạnh {supports[0]["price"]:,.0f} '
 f'(chạm {strength_s} lần, cách {dist_s:.1f}%) → Rủ elif dist_s < 4:
 score += 5
 signals.append(('SR', 'bull', f'HT gần: {supports[0]["price"]:,.0f} (cách {dist_s else:
 signals.append(('SR', 'neutral', f'HT gần nhất: {supports[0]["price"]:,.0f} (cách if resistances:
 dist_r = (resistances[0]['price'] - price) / price * 100
 strength_r = resistances[0]['count']
 if dist_r < 1.5:
 pts = min(12, 6 + strength_r * 2)
 score -= pts
 signals.append(('SR', 'bear', f'Giá rất gần KC mạnh {resistances[0]["price"]:,.0f f'(chạm {strength_r} lần, cách {dist_r:.1f}%) → Cẩ elif dist_r < 4:
 score -= 5
 signals.append(('SR', 'bear', f'KC gần: {resistances[0]["price"]:,.0f} (cách {dis else:
 signals.append(('SR', 'neutral', f'KC gần nhất: {resistances[0]["price"]:,.0f} (c # ── NHÓM 5: ICHIMOKU + BB (8%) ───────────────────────────────────────────
 # Ichimoku: max ±5 | BB: max ±3
 if price > cloud_top:
 score += 5
 signals.append(('ICHI', 'bull', f'Giá trên mây ({cloud_bottom:,.0f}–{cloud_top:,.0f})
 elif price < cloud_bottom:
 score -= 5
 signals.append(('ICHI', 'bear', f'Giá dưới mây ({cloud_bottom:,.0f}–{cloud_top:,.0f}) else:
 signals.append(('ICHI', 'neutral', f'Giá trong mây → Vùng không rõ xu hướng'))
 if price <= bb_lower:
 score += 3
 signals.append(('BB', 'bull', f'Giá chạm/dưới BB dưới {bb_lower:,.0f} → Hỗ trợ BB'))
 elif price >= bb_upper:
 score -= 3
 signals.append(('BB', 'bear', f'Giá chạm BB trên {bb_upper:,.0f} → Kháng cự BB'))
 else:
 signals.append(('BB', 'neutral', f'Giá trong BB ({bb_pct:.0f}% trong dải)'))
 # ── Điều kiện "3 trong 1" ─────────────────────────────────────────────────
 three_in_one = (
 price > ma20 and # Giá trên MA20
 vol_ratio >= 1.5 and # Volume đột biến (cá mập vào)
 price_up and # Giá đang tăng
 30 < rsi_val < 70 # RSI còn dư địa
 )
 score = max(0, min(100, score))
 if score >= 65: action = 'MUA'
 elif score <= 35: action = 'BÁN'
 else: action = 'THEO DÕI'
 # Stop Loss -7% theo chuyên gia VN, R:R = 1:2
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
 'weight_note': 'VOL 35% | RSI+DIV 25% | MA 20% | SR 12% | ICHI+BB 8%',
 }
def fetch_price(symbol):
 cached = get_cached('price_' + symbol)
 if cached: return cached
 from datetime import datetime, timedelta
 end = datetime.now().strftime('%Y-%m-%d')
 start = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
 for source in ['TCBS', 'VCI']:
 try:
 from vnstock import Vnstock
 df = Vnstock().stock(symbol=symbol, source=source).quote.history(
 start=start, end=end, interval='1D'
 )
 if df is None or df.empty: continue
 cc = find_col(df, ['close','closeprice','close_price'])
 if cc is None:
 nums = df.select_dtypes(include='number').columns
 cc = nums[-1] if len(nums) > 0 else None
 if cc is None: continue
 close = float(df.iloc[-1][cc])
 if 0 < close < 1000: close *= 1000
 if close <= 0: continue
 chg = 0
 if len(df) >= 2:
 prev = float(df.iloc[-2][cc])
 if 0 < prev < 1000: prev *= 1000
 if prev > 0: chg = round((close - prev) / prev * 100, 2)
 result = {'symbol': symbol, 'price': round(close,0), 'change_pct': chg, 'source': set_cache('price_' + symbol, result)
 return result
 except Exception as e:
 logger.warning(f"{symbol}/{source}: {e}")
 return {'symbol': symbol, 'price': 0, 'change_pct': 0, 'source': 'error', 'error': 'Không
def fetch_analysis(symbol, price_override=None):
 cache_key = f'analysis_{symbol}_{price_override or "live"}'
 cached = get_cached(cache_key)
 if cached: return cached
 df, source = load_history(symbol, days=200)
 if df is None:
 return {'symbol': symbol, 'error': 'Không tải được dữ liệu'}
 try:
 result = compute_indicators(df, price_override)
 except Exception as e:
 logger.error(f"compute_indicators {symbol}: {e}")
 return {'symbol': symbol, 'error': f'Lỗi tính chỉ báo: {str(e)}'}
 if result is None:
 return {'symbol': symbol, 'error': 'Không tính được chỉ báo'}
 result['symbol'] = symbol
 result['source'] = source
 set_cache(cache_key, result)
 return result
@app.route('/')
def index():
 return jsonify({
 'status': 'ok',
 'message': 'VN Trader API v4',
 'weights': 'VOL 35% | RSI+DIV 25% | MA 20% | SR 12% | ICHI+BB 8%'
 })
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
 for sym, name in [('VNINDEX','VN-INDEX'),('HNX30','HNX-INDEX'),('VN30F1M','VN30')]:
 d = fetch_price(sym); result[sym] = {**d, 'name': name}; time.sleep(0.5)
 return jsonify(result)
@app.route('/api/signals')
def api_signals():
 watchlist = ['HPG', 'FPT', 'VCB']
 results = []
 for sym in watchlist:
 try:
 # Ưu tiên dùng cache — nếu chưa có cache thì mới gọi vnstock
 cached = get_cached(f'analysis_{sym}_live')
 if cached:
 results.append(cached)
 continue
 r = fetch_analysis(sym)
 if r and 'score' in r and 'error' not in r:
 results.append(r)
 time.sleep(0.2)
 except Exception as e:
 logger.warning(f"Signal skip {sym}: {e}")
 results.sort(key=lambda x: abs(x.get('score', 50) - 50), reverse=True)
 return jsonify(results)
@app.route('/api/warmup')
def api_warmup():
 # Gọi endpoint này khi server khởi động để build cache trước
 import threading
 def _warm():
 for sym in ['HPG', 'FPT', 'VCB']:
 try:
 fetch_analysis(sym)
 time.sleep(0.5)
 except: pass
 threading.Thread(target=_warm, daemon=True).start()
 return jsonify({'status': 'warming up cache...'})
@app.route('/api/debug/<symbol>')
def api_debug(symbol):
 try:
 df, source = load_history(symbol.upper(), days=10)
 if df is not None:
 return jsonify({'columns': list(df.columns), 'source': source,
 'sample': df.tail(3).to_dict(orient='records'), 'rows': len(df)})
 return jsonify({'error': 'No data'})
 except Exception as e:
 return jsonify({'error': str(e)})
if __name__ == '__main__':
 port = int(os.environ.get('PORT', 5000))
 app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
