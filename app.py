import os
import time
import logging
import threading
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── FIX 1: Thread-safe cache dùng Lock ──────────────────────────────────────
_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL_TRADING  = 60      # 60s trong giờ giao dịch — cập nhật thường xuyên
CACHE_TTL_CLOSED   = 3600    # 1h ngoài giờ giao dịch — không cần refresh liên tục

import pytz as _pytz
_VN_TZ = _pytz.timezone('Asia/Ho_Chi_Minh')

def _get_cache_ttl():
    """TTL thông minh: ngắn trong giờ GD, dài ngoài giờ để giảm API call."""
    from datetime import datetime as _dt
    now = _dt.now(_VN_TZ)
    wd  = now.weekday()  # 0=T2, 4=T6
    h, m = now.hour, now.minute
    in_trading = (
        wd < 5 and
        ((h == 9 and m >= 0) or h in (10, 11, 13, 14) or (h == 15 and m == 0)) and
        not (h == 11 and m >= 30) and not (h == 12)
    )
    return CACHE_TTL_TRADING if in_trading else CACHE_TTL_CLOSED

def get_cached(key):
    ttl = _get_cache_ttl()
    with _cache_lock:
        if key in _cache:
            data, ts = _cache[key]
            if time.time() - ts < ttl:
                return data
            else:
                del _cache[key]
    return None

def set_cache(key, data):
    with _cache_lock:
        # FIX 2: Giới hạn cache tối đa 500 entries để tránh memory leak
        if len(_cache) >= 500:
            # Xóa 100 entries cũ nhất
            oldest = sorted(_cache.items(), key=lambda x: x[1][1])[:100]
            for k, _ in oldest:
                del _cache[k]
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

    for source in ['VCI', 'TCBS']:
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


# ── Phân tích cảnh báo khung 1H ──────────────────────────────────────────────
def load_history_1h(symbol, days=30):
    """
    Tải dữ liệu 1H từ vnstock (~150-200 nến 1H với days=30).
    vnstock giới hạn lookback ~60 ngày cho interval='1H'.
    """
    from datetime import datetime, timedelta
    end   = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    for source in ['VCI', 'TCBS']:
        try:
            from vnstock import Vnstock
            df = Vnstock().stock(symbol=symbol, source=source).quote.history(
                start=start, end=end, interval='1H'
            )
            if df is not None and len(df) >= 10:
                logger.info(f"{symbol}/1H/{source} OK rows={len(df)}")
                return df, source
        except Exception as e:
            logger.warning(f"{symbol}/1H/{source}: {e}")
    return None, None


def analyze_1h_warnings(symbol):
    """
    Cảnh báo khung 1H — chỉ giữ Volume Spike, bỏ lower-high và RSI divergence.

    Lý do giản lược:
    - Lower-high 1H: hay phát trong buổi chiều bình thường VN → nhiễu cao
    - RSI divergence 1H: cần 2-3 ngày để hình thành → thông tin đã có trên 1D rồi
    - Volume spike 1H: thông tin duy nhất 1H có mà 1D không có — tổ chức
      đang vào/ra lệnh lớn ngay lúc đó, mất đi khi gộp vào volume cả ngày

    Chỉ cảnh báo khi vol giờ hiện tại >= 2x TB các giờ trước trong ngày.
    Phân biệt rõ hướng: MUA LỚN (giá tăng) hay BÁN LỚN (giá giảm).
    """
    import numpy as np
    import pandas as pd

    df, source = load_history_1h(symbol, days=10)  # Chỉ cần ~50 nến 1H gần nhất
    if df is None or len(df) < 5:
        return []

    cc = find_col(df, ['close', 'closeprice', 'close_price'])
    vc = next((c for c in df.columns if c.lower() in {
        'volume', 'volume_match', 'klgd', 'vol', 'trading_volume',
        'match_volume', 'total_volume', 'dealvolume', 'matchingvolume'}), None)

    if cc is None or vc is None:
        return []

    closes  = pd.to_numeric(df[cc], errors='coerce').fillna(0).values.copy()
    volumes = pd.to_numeric(df[vc], errors='coerce').fillna(0).values.copy()
    if closes.max() < 1000: closes *= 1000

    n = len(closes)
    if n < 3:
        return []

    # Lấy tối đa 8 nến 1H gần nhất (~1 ngày GD VN = 5.5 giờ)
    today_vols = volumes[-8:] if n >= 8 else volumes
    today_vols_nonzero = today_vols[today_vols > 0]
    if len(today_vols_nonzero) < 3:
        return []

    vol_cur         = float(today_vols_nonzero[-1])
    vol_avg_prev_1h = float(np.mean(today_vols_nonzero[:-1]))
    if vol_avg_prev_1h <= 0:
        return []

    spike_ratio = vol_cur / vol_avg_prev_1h
    if spike_ratio < 2.0:
        return []   # Không đủ đột biến, trả về rỗng

    # Xác định hướng dòng tiền
    price_up_1h = closes[-1] >= closes[-2] if n >= 2 else True
    if spike_ratio >= 3.0:
        level     = 'HIGH'
        direction = 'MUA LON' if price_up_1h else 'BAN LON'
        msg = (
            f'Vol gio nay {spike_ratio:.1f}x TB cac gio truoc '
            f'-> To chuc dang {direction} manh '
            f'({"Xac nhan tin hieu MUA" if price_up_1h else "CANH BAO: ap luc ban lon"})'
        )
    else:
        level     = 'MEDIUM'
        direction = 'mua' if price_up_1h else 'ban'
        msg = (
            f'Vol gio nay {spike_ratio:.1f}x TB cac gio truoc '
            f'-> Dong tien {direction} dang tang, theo doi them'
        )

    logger.info(f"{symbol}/1H: vol_spike {spike_ratio:.1f}x ({level})")
    return [{'type': 'VOL_SPIKE_1H', 'level': level, 'message': msg}]


# ── Exchange & Limit Down Detection ─────────────────────────────────────────
# Cache sàn giao dịch để tránh gọi API liên tục
_exchange_cache = {}

def get_exchange(symbol):
    """
    Tự động detect sàn giao dịch (HOSE/HNX/UPCOM) qua vnstock listing.
    Cache kết quả 24h vì sàn của mã không thay đổi thường xuyên.
    Fallback: dùng biên độ rộng nhất (UPCOM 15%) nếu không detect được,
    để tránh false positive khi chặn limit_down.
    """
    if symbol in _exchange_cache:
        exchange, ts = _exchange_cache[symbol]
        if time.time() - ts < 86400:  # 24h cache
            return exchange

    try:
        from vnstock import Vnstock
        listing = Vnstock().stock(symbol=symbol, source='VCI').listing.symbols_by_exchange()
        if listing is not None and not listing.empty:
            # Cột thường là 'exchange' hoặc 'floor'
            exc_col = next((c for c in listing.columns if c.lower() in ('exchange', 'floor', 'san')), None)
            if exc_col:
                row = listing[listing['ticker'] == symbol] if 'ticker' in listing.columns else \
                      listing[listing.index == symbol]
                if not row.empty:
                    exc = str(row.iloc[0][exc_col]).upper()
                    if 'HNX' in exc and 'UPCOM' not in exc:
                        result = 'HNX'
                    elif 'UPCOM' in exc or 'UPC' in exc:
                        result = 'UPCOM'
                    else:
                        result = 'HOSE'
                    _exchange_cache[symbol] = (result, time.time())
                    logger.info(f"{symbol}: exchange detected = {result}")
                    return result
    except Exception as e:
        logger.warning(f"get_exchange {symbol}: {e}")

    # Fallback: không detect được → dùng UPCOM (biên độ rộng nhất 15%)
    # Lý do: thà bỏ lỡ 1 tín hiệu limit_down còn hơn false positive
    _exchange_cache[symbol] = ('UPCOM', time.time())
    logger.warning(f"{symbol}: exchange fallback = UPCOM (broadest band)")
    return 'UPCOM'

EXCHANGE_BANDS = {
    'HOSE':  0.07,   # ±7%
    'HNX':   0.10,   # ±10%
    'UPCOM': 0.15,   # ±15%
}

def detect_limit_down(closes, lows, symbol=''):
    """
    Phát hiện giá sàn (Limit Down) - đặc thù TTCK Việt Nam.

    Điều kiện limit_down = TRUE khi TẤT CẢ 3 điều sau đúng:
      1. Giá đóng cửa <= giá tham chiếu * (1 - biên_độ + tolerance)
      2. Giá thấp nhất ngày cũng chạm vùng sàn (xác nhận dư bán sàn)
      3. Giá giảm so với phiên trước (không phải đứng im)

    Tolerance +0.5%: tránh false positive khi giá gần sàn nhưng chưa chạm.
    Tolerance được cộng thêm 1% nếu không detect được sàn (fallback UPCOM).
    """
    if len(closes) < 2 or len(lows) < 1:
        return False, 0.0, 'HOSE'

    exchange = get_exchange(symbol) if symbol else 'HOSE'
    band = EXCHANGE_BANDS.get(exchange, 0.07)

    prev_close = float(closes[-2])
    curr_close = float(closes[-1])
    curr_low   = float(lows[-1])

    if prev_close <= 0:
        return False, band, exchange

    floor_price = prev_close * (1 - band)
    # Tolerance: 0.5% cho sàn detect được, 1.5% cho fallback UPCOM
    tol = 0.015 if exchange == 'UPCOM' and symbol not in _exchange_cache else 0.005
    threshold = floor_price * (1 + tol)

    is_limit_down = (
        curr_close <= threshold and      # Giá đóng cửa chạm sàn
        curr_low   <= threshold and      # Giá thấp nhất cũng chạm sàn
        curr_close <  prev_close         # Giá có giảm (không phải đứng im)
    )

    logger.info(
        f"limit_down check [{exchange} ±{band*100:.0f}%]: "
        f"prev={prev_close:,.0f} floor={floor_price:,.0f} "
        f"close={curr_close:,.0f} low={curr_low:,.0f} → {is_limit_down}"
    )
    return is_limit_down, band, exchange


def compute_indicators(df, price_override=None, symbol=''):
    import numpy as np
    import pandas as pd

    cc = find_col(df, ['close', 'closeprice', 'close_price'])
    hc = find_col(df, ['high', 'highprice', 'high_price'])
    lc = find_col(df, ['low', 'lowprice', 'low_price'])

    VOLUME_NAMES = {
        'volume', 'volume_match', 'klgd', 'vol', 'trading_volume',
        'match_volume', 'total_volume', 'dealvolume', 'matchingvolume',
        'volumematch', 'vol_match', 'qtgd'
    }

    vc = None
    for c in df.columns:
        if c.lower() in VOLUME_NAMES:
            vc = c
            break

    if cc is None:
        nums = df.select_dtypes(include='number').columns
        cc = nums[-1] if len(nums) > 0 else None

    if cc is None:
        return None

    def to_float_arr(series):
        return pd.to_numeric(series, errors='coerce').fillna(0).astype(float).values.copy()

    closes = to_float_arr(df[cc])
    if closes.max() < 1000:
        closes *= 1000

    highs = to_float_arr(df[hc]) if hc else closes.copy()
    if hc and highs.max() < 1000:
        highs *= 1000

    lows = to_float_arr(df[lc]) if lc else closes.copy()
    if lc and lows.max() < 1000:
        lows *= 1000

    volumes = np.zeros(len(closes))
    vol_col_found = None

    if vc:
        v = pd.to_numeric(df[vc], errors='coerce').fillna(0).astype(float).values
        if v.max() > 1000:
            volumes = v
            vol_col_found = vc
        logger.info(f"Volume col (named): {vc} max={v.max():.0f} mean={v.mean():.0f}")

    if vol_col_found is None:
        best_col = None
        best_max = 0
        for col in df.columns:
            if col in [cc, hc, lc]:
                continue
            v = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(float).values
            if v.max() > best_max and v.max() > 10000:
                best_max = v.max()
                best_col = col
        if best_col:
            volumes = pd.to_numeric(df[best_col], errors='coerce').fillna(0).astype(float).values
            vol_col_found = best_col
            logger.info(f"Volume col (fallback): {best_col} max={volumes.max():.0f}")

    if vol_col_found is None:
        logger.warning(f"No volume col found in: {list(df.columns)}")

    logger.info(f"volumes[-5:] = {volumes[-5:]} | vol_col={vol_col_found}")
    nonzero_count = np.count_nonzero(volumes)
    logger.info(f"volumes nonzero={nonzero_count}/{len(volumes)}")

    # price/prev_close sẽ được tính SAU bước trim nến chưa đóng (fix lỗi prev_close sai)
    # Tạm thời dùng giá trị raw để tính RSI/MACD/divergence — không bị ảnh hưởng bởi trim

    def ema_arr(arr, span):
        alpha = 2.0 / (span + 1)
        out = np.zeros(len(arr))
        out[0] = arr[0]
        for i in range(1, len(arr)):
            out[i] = alpha * arr[i] + (1 - alpha) * out[i - 1]
        return out

    # ── FIX 3: RSI dùng Wilder's Smoothing chuẩn (thay vì SMA) ─────────────
    def calc_rsi_arr(arr, p=14):
        out = np.full(len(arr), 50.0)
        if len(arr) < p + 1:
            return out
        deltas = np.diff(arr)
        gains = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)
        # Seed với SMA của p period đầu
        avg_gain = np.mean(gains[:p])
        avg_loss = np.mean(losses[:p])
        if avg_loss == 0:
            out[p] = 100.0
        else:
            out[p] = 100 - 100 / (1 + avg_gain / avg_loss)
        # Wilder's smoothing cho các period tiếp theo
        for i in range(p, len(deltas)):
            avg_gain = (avg_gain * (p - 1) + gains[i]) / p
            avg_loss = (avg_loss * (p - 1) + losses[i]) / p
            if avg_loss == 0:
                out[i + 1] = 100.0
            else:
                out[i + 1] = 100 - 100 / (1 + avg_gain / avg_loss)
        return np.round(out, 1)

    rsi_series = calc_rsi_arr(closes)
    rsi_val = float(rsi_series[-1])

    def detect_divergence(close_arr, high_arr, low_arr, rsi_arr, lookback=40):
        """
        Dùng highs để tìm đỉnh và lows để tìm đáy — chính xác hơn dùng close.
        RSI được tính tại cùng vị trí với đỉnh/đáy giá.
        """
        if len(close_arr) < lookback:
            return 'none', ''
        # Lấy đoạn lookback
        h = high_arr[-lookback:]
        l = low_arr[-lookback:]
        r = rsi_arr[-lookback:]

        # Tìm đỉnh dùng highs (pivot high: cao hơn 2 nến hai bên)
        tops = [i for i in range(2, len(h) - 2)
                if h[i] >= h[i-1] and h[i] >= h[i-2]
                and h[i] >= h[i+1] and h[i] >= h[i+2]]

        # Tìm đáy dùng lows (pivot low: thấp hơn 2 nến hai bên)
        bottoms = [i for i in range(2, len(l) - 2)
                   if l[i] <= l[i-1] and l[i] <= l[i-2]
                   and l[i] <= l[i+1] and l[i] <= l[i+2]]

        # Bearish divergence: đỉnh giá cao hơn, RSI tại đỉnh thấp hơn
        if len(tops) >= 2:
            t1, t2 = tops[-2], tops[-1]
            if h[t2] > h[t1] and r[t2] < r[t1] - 2:
                return 'bearish', (
                    'Phan ky giam: Gia dinh moi (' + f'{h[t2]:,.0f}'
                    + ') cao hon dinh cu (' + f'{h[t1]:,.0f}'
                    + ') nhung RSI thap hon (' + f'{r[t2]:.0f}'
                    + '<' + f'{r[t1]:.0f}' + ')'
                )

        # Bullish divergence: đáy giá thấp hơn, RSI tại đáy cao hơn
        if len(bottoms) >= 2:
            b1, b2 = bottoms[-2], bottoms[-1]
            if l[b2] < l[b1] and r[b2] > r[b1] + 2:
                return 'bullish', (
                    'Phan ky tang: Gia day moi (' + f'{l[b2]:,.0f}'
                    + ') thap hon day cu (' + f'{l[b1]:,.0f}'
                    + ') nhung RSI cao hon (' + f'{r[b2]:.0f}'
                    + '>' + f'{r[b1]:.0f}' + ')'
                )

        return 'none', ''

    div_type, div_msg = detect_divergence(closes, highs, lows, rsi_series, lookback=40)

    ema12 = ema_arr(closes, 12)
    ema26 = ema_arr(closes, 26)
    macd_line = ema12 - ema26
    sig_line = ema_arr(macd_line, 9)
    macd_hist = macd_line - sig_line
    macd_val = float(macd_line[-1])
    macd_sig = float(sig_line[-1])
    macd_h = float(macd_hist[-1])

    # ── Vol MA20 và trim nến chưa đóng ──────────────────────────────────────
    # MA20/BB/price được tính SAU trim (xem bên dưới sau block này)
    vol_history = volumes[:-1] if len(volumes) > 1 else volumes
    valid_vols = vol_history[vol_history > 0]
    if len(valid_vols) >= 5:
        recent_valid = valid_vols[-20:] if len(valid_vols) >= 20 else valid_vols
        vol_ma20 = float(np.mean(recent_valid))
    else:
        all_valid = volumes[volumes > 0]
        vol_ma20 = float(np.mean(all_valid)) if len(all_valid) > 0 else 0.0

    logger.info(f"vol_ma20={vol_ma20:.0f} (computed from {len(valid_vols)} valid candles)")

    vol_threshold = vol_ma20 * 0.1
    if vol_ma20 > 0 and vol_threshold > 0 and volumes[-1] < vol_threshold and len(volumes) >= 2:
        vol_today = float(volumes[-2])
        closes = closes[:-1]
        highs = highs[:-1]
        lows = lows[:-1]
        volumes = volumes[:-1]
        logger.info(f"Nen cuoi chua dong cua")
    else:
        vol_today = float(volumes[-1])
        logger.info(f"vol_today={vol_today:.0f} (nen cuoi hop le)")

    vol_ratio = vol_today / vol_ma20 if vol_ma20 > 0 else 1.0

    # ── Fix: price, prev_close, BB, MA tính SAU trim để dùng closes đã xác nhận ──
    price      = float(price_override) if price_override else float(closes[-1])
    prev_close = float(closes[-2]) if len(closes) > 1 else price
    price_up   = price >= prev_close

    # MA20/MA50 sau trim
    ma10 = float(np.mean(closes[-10:])) if len(closes) >= 10 else float(np.mean(closes))
    ma10_prev = float(np.mean(closes[-11:-1])) if len(closes) >= 11 else ma10
    ma20 = float(np.mean(closes[-20:]))
    ma50 = float(np.mean(closes[-min(50, len(closes)):]))
    ma20_prev = float(np.mean(closes[-21:-1])) if len(closes) >= 21 else ma20
    ma50_prev = float(np.mean(closes[-51:-1])) if len(closes) >= 51 else ma50
    golden_cross = ma20_prev < ma50_prev and ma20 > ma50
    death_cross  = ma20_prev > ma50_prev and ma20 < ma50

    # ── MA10 cross detection ────────────────────────────────────────────────
    # Ngắn hạn: giá cắt lên MA10 → momentum mới phục hồi
    # Dùng prev_close để xác định cross (hôm qua dưới MA10, hôm nay trên)
    ma10_cross_up   = (prev_close < ma10_prev and price > ma10)   # vừa cắt lên
    ma10_cross_down = (prev_close > ma10_prev and price < ma10)   # vừa cắt xuống
    ma10_slope_up   = ma10 > ma10_prev                             # MA10 đang dốc lên
    above_ma10      = price > ma10
    above_ma50      = price > ma50
    ma50_slope_up   = ma50 > float(np.mean(closes[-53:-3])) if len(closes) >= 53 else False

    # BB sau trim
    bb_mid   = float(np.mean(closes[-20:]))
    bb_std   = float(np.std(closes[-20:]))
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std
    bb_pct   = (price - bb_lower) / (bb_upper - bb_lower) * 100 if bb_upper != bb_lower else 50

    logger.info(f"vol_today={vol_today:.0f} vol_ma20={vol_ma20:.0f} vol_ratio={vol_ratio:.2f}")

    # ── Weekly Trend Filter — tính SAU khi trim nến chưa đóng ────────────────
    # MA10W ≈ MA100D (20 tuần × 5 ngày GD) | MA20W ≈ MA200D (40 tuần × 5 ngày GD)
    # Tính sau trim để dùng closes đã xác nhận (nến đóng hoàn toàn)
    # Cần load_history(days=400) để đủ 200 phiên GD cho MA200
    ma100 = float(np.mean(closes[-100:])) if len(closes) >= 100 else float(np.mean(closes))
    ma200 = float(np.mean(closes[-200:])) if len(closes) >= 200 else ma100

    if price > ma100 and ma100 > ma200:
        weekly_trend    = 'STRONG_UP'
        weekly_trend_vn = 'Tang manh tuan (Gia>MA10W>MA20W)'
    elif price > ma100:
        weekly_trend    = 'UP'
        weekly_trend_vn = 'Tang tuan (Gia>MA10W)'
    elif price > ma200:
        weekly_trend    = 'WEAK_UP'
        weekly_trend_vn = 'Yeu tuan (Gia<MA10W nhung >MA20W)'
    elif ma100 > ma200:
        weekly_trend    = 'PULLBACK'
        weekly_trend_vn = 'Pullback tuan (Gia<MA10W, MA10W van tren MA20W)'
    else:
        weekly_trend    = 'DOWN'
        weekly_trend_vn = 'Downtrend tuan (Gia<MA10W<MA20W)'

    logger.info(f"weekly_trend={weekly_trend} ma100={ma100:.0f} ma200={ma200:.0f}")

    if vol_ratio >= 1.5 and price_up:
        vol_signal = 'shark_buy'
        vol_msg = 'Vol ' + f'{vol_ratio:.1f}' + 'x TB + gia tang -> Dong tien lon vao! Xac nhan'
    elif vol_ratio >= 1.5 and not price_up:
        vol_signal = 'shark_sell'
        vol_msg = 'Vol ' + f'{vol_ratio:.1f}' + 'x TB + gia giam -> Dong tien lon xa! Tin hieu xau'
    elif vol_ratio < 0.7 and price_up:
        vol_signal = 'fake_rally'
        vol_msg = 'Gia tang nhung Vol thap ' + f'{vol_ratio:.1f}' + 'x TB -> Co the keo xa, can than'
    elif vol_ratio >= 1.0 and price_up:
        vol_signal = 'normal_buy'
        vol_msg = 'Vol ' + f'{vol_ratio:.1f}' + 'x TB + gia tang -> Xu huong tang duoc xac nhan'
    elif vol_ratio >= 0.7 and price_up:
        # Fix: vol 0.7-1.0x + giá tăng = xác nhận yếu, không phải normal_buy (+8đ)
        vol_signal = 'weak_buy'
        vol_msg = 'Vol ' + f'{vol_ratio:.1f}' + 'x TB + gia tang -> Xac nhan yeu, can them tin hieu'
    elif vol_ratio < 0.7 and not price_up:
        vol_signal = 'weak_sell'
        vol_msg = 'Vol thap ' + f'{vol_ratio:.1f}' + 'x TB + gia giam -> Ap luc ban yeu'
    elif vol_ratio >= 0.7 and not price_up:
        vol_signal = 'normal_sell'
        vol_msg = 'Vol ' + f'{vol_ratio:.1f}' + 'x TB + gia giam -> Ap luc ban binh thuong'
    else:
        vol_signal = 'normal'
        vol_msg = 'Vol binh thuong ' + f'{vol_ratio:.1f}' + 'x TB'

    # ── Limit Down Detection (đặc thù TTCK Việt Nam) ────────────────────────
    # Phải chạy SAU khi đã xác định closes/lows cuối (sau trim nến chưa đóng)
    is_limit_down, exch_band, exchange = detect_limit_down(closes, lows, symbol)

    if is_limit_down and vol_signal == 'weak_sell':
        # Ghi đè: volume thấp KHÔNG phải kiệt cung mà là mất thanh khoản
        vol_signal = 'shark_sell'
        vol_msg = (
            'CANH BAO GIA SAN [' + exchange + ' +-' + f'{exch_band*100:.0f}' + '%]: '
            'Trang ben mua! Vol thap = mat thanh khoan, KHONG phai kiet cung. '
            'Tuyet doi khong mua!'
        )
        logger.warning(f"[{symbol}] Limit Down detected -> override weak_sell to shark_sell")

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

    def find_sr(h, l, window=6):
        levels = []
        h = h[-120:] if len(h) > 120 else h
        l = l[-120:] if len(l) > 120 else l
        tol = 0.001  # Fix: cho phép sai số 0.1% để bắt pivot đúng hơn
        for i in range(window, len(h) - window):
            local_max = max(h[i - window:i + window + 1])
            local_min = min(l[i - window:i + window + 1])
            if h[i] >= local_max * (1 - tol):
                levels.append(('R', float(h[i])))
            if l[i] <= local_min * (1 + tol):
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
        strong = [m for m in merged if m['count'] >= 3]
        strong.sort(key=lambda x: x['count'], reverse=True)
        sups = sorted([m for m in strong if m['price'] < price], key=lambda x: x['price'], reverse=True)[:3]
        ress = sorted([m for m in strong if m['price'] > price], key=lambda x: x['price'])[:3]
        return sups, ress

    supports, resistances = find_sr(highs, lows, window=6)

    # Fallback window=4 count>=2 nếu không có S/R đủ mạnh
    if not supports or not resistances:
        def find_sr_fallback(h, l, window=4):
            levels = []
            h = h[-120:] if len(h) > 120 else h
            l = l[-120:] if len(l) > 120 else l
            tol = 0.001
            for i in range(window, len(h) - window):
                local_max = max(h[i - window:i + window + 1])
                local_min = min(l[i - window:i + window + 1])
                if h[i] >= local_max * (1 - tol):
                    levels.append(('R', float(h[i])))
                if l[i] <= local_min * (1 + tol):
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
        fb_s, fb_r = find_sr_fallback(highs, lows)
        if not supports: supports = fb_s
        if not resistances: resistances = fb_r

    # ── FIX 4: Cân bằng lại score weights ───────────────────────────────────
    # Trước: VOL=35, RSI=15, MA=20 → VOL quá dominant
    # Sau:   VOL=20, RSI=20, MA=20 → cân bằng hơn, tín hiệu đáng tin hơn
    score = 50
    signals = []

    # VOL: tối đa ±20 (giảm từ ±35)
    if vol_signal == 'shark_buy':
        score += 20
        signals.append(('VOL', 'bull', vol_msg))
    elif vol_signal == 'shark_sell':
        score -= 20
        signals.append(('VOL', 'bear', vol_msg))
    elif vol_signal == 'fake_rally':
        score -= 12
        signals.append(('VOL', 'bear', vol_msg))
    elif vol_signal == 'normal_buy':
        score += 8
        signals.append(('VOL', 'bull', vol_msg))
    elif vol_signal == 'weak_buy':
        # Fix: vol 0.7-1.0x + giá tăng = xác nhận yếu (+3đ thay vì +8đ)
        score += 3
        signals.append(('VOL', 'neutral', vol_msg))
    elif vol_signal == 'weak_sell':
        score += 3
        signals.append(('VOL', 'neutral', vol_msg))
    elif vol_signal == 'normal_sell':
        score -= 5
        signals.append(('VOL', 'bear', vol_msg))
    else:
        signals.append(('VOL', 'neutral', vol_msg))

    # RSI: tối đa ±20 (giữ nguyên, đủ mạnh)
    if rsi_val < 30:
        score += 20
        signals.append(('RSI', 'bull', 'RSI=' + str(rsi_val) + ' Vung qua ban -> Tim co hoi'))
    elif rsi_val < 40:
        score += 10
        signals.append(('RSI', 'bull', 'RSI=' + str(rsi_val) + ' Vung yeu, dang hoi phuc'))
    elif rsi_val > 70:
        score -= 20
        signals.append(('RSI', 'bear', 'RSI=' + str(rsi_val) + ' Vung qua mua -> KHONG mua'))
    elif rsi_val > 60:
        score -= 10
        signals.append(('RSI', 'bear', 'RSI=' + str(rsi_val) + ' Vung manh, than trong'))
    else:
        signals.append(('RSI', 'neutral', 'RSI=' + str(rsi_val) + ' Vung trung tinh'))

    # Divergence: tối đa ±15 (giảm nhẹ từ ±15)
    if div_type == 'bullish':
        if rsi_val < 35:
            score += 15
            signals.append(('DIV', 'bull', div_msg + '[RSI qua ban xac nhan!]'))
        else:
            score += 10
            signals.append(('DIV', 'bull', div_msg))
    elif div_type == 'bearish':
        if rsi_val > 65:
            score -= 15
            signals.append(('DIV', 'bear', div_msg + '[RSI qua mua xac nhan!]'))
        else:
            score -= 10
            signals.append(('DIV', 'bear', div_msg))
    else:
        signals.append(('DIV', 'neutral', 'Khong phat hien phan ky RSI'))

    # MA: tối đa ±20 (giữ nguyên)
    if golden_cross:
        score += 20
        signals.append(('MA', 'bull', 'GOLDEN CROSS! MA20 cat len MA50 -> Tang manh dai han'))
    elif death_cross:
        score -= 20
        signals.append(('MA', 'bear', 'DEATH CROSS! MA20 cat xuong MA50 -> Giam dai han!'))
    elif price > ma20 and ma20 > ma50:
        score += 15
        signals.append(('MA', 'bull', 'Gia>MA20(' + f'{ma20:,.0f}' + ')>MA50(' + f'{ma50:,.0f}' + ')'))
    elif price > ma20:
        score += 10
        signals.append(('MA', 'bull', 'Gia tren MA20 ' + f'{ma20:,.0f}' + ' -> Xu huong ngan han tang'))
    elif price < ma20 and ma20 < ma50:
        score -= 15
        signals.append(('MA', 'bear', 'Gia<MA20<MA50 -> Giam 2 tang - KHONG mua duoi'))
    else:
        score -= 10
        signals.append(('MA', 'bear', 'Gia duoi MA20 ' + f'{ma20:,.0f}' + ' - KHONG mua duoi!'))

    # MACD: tối đa ±8 (tăng từ ±5, thêm momentum histogram)
    if macd_val > macd_sig and macd_h > 0:
        score += 5
        signals.append(('MACD', 'bull', 'MACD cat len Signal -> Dong luc tang'))
        # Histogram đang mở rộng (momentum tăng tốc)
        if len(macd_hist) >= 4 and float(macd_hist[-1]) > float(macd_hist[-3]):
            score += 3
            signals.append(('MACD', 'bull', 'MACD hist mo rong -> Dong luc dang tang toc'))
    elif macd_val < macd_sig and macd_h < 0:
        score -= 5
        signals.append(('MACD', 'bear', 'MACD cat xuong Signal -> Dong luc giam'))
        if len(macd_hist) >= 4 and float(macd_hist[-1]) < float(macd_hist[-3]):
            score -= 3
            signals.append(('MACD', 'bear', 'MACD hist mo rong xuong -> Dong luc giam toc'))
    else:
        signals.append(('MACD', 'neutral', 'MACD=' + f'{macd_val:+.0f}'))

    # S/R: tối đa ±12 (giữ nguyên)
    if supports:
        dist_s = (price - supports[0]['price']) / price * 100
        strength_s = supports[0]['count']
        if dist_s < 1.5:
            pts = min(12, 6 + strength_s * 2)
            score += pts
            signals.append(('SR', 'bull', 'Gia gan HT manh ' + f'{supports[0]["price"]:,.0f}'))
        elif dist_s < 4:
            score += 5
            signals.append(('SR', 'bull', 'HT gan: ' + f'{supports[0]["price"]:,.0f}' + ' (can chu y)'))
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

    # Ichimoku: ±5 vị trí giá vs mây + ±5 TK/KJ cross (tín hiệu ngắn hạn)
    if price > cloud_top:
        score += 5
        signals.append(('ICHI', 'bull', 'Gia tren may Ichimoku -> Xu huong tang'))
    elif price < cloud_bottom:
        score -= 5
        signals.append(('ICHI', 'bear', 'Gia duoi may Ichimoku -> Xu huong giam'))
    else:
        signals.append(('ICHI', 'neutral', 'Gia trong may -> Khong ro xu huong'))

    # Tenkan/Kijun cross — tín hiệu ngắn hạn mạnh, phù hợp TTCK VN
    # Fix: yêu cầu khoảng cách tối thiểu 0.3% để tránh false cross khi TK≈KJ
    if n >= 27:
        tenkan_prev = (np.max(highs[-10:-1]) + np.min(lows[-10:-1])) / 2
        kijun_prev  = (np.max(highs[-27:-1]) + np.min(lows[-27:-1])) / 2
        tk_val      = float(tenkan)
        kj_val      = float(kijun)
        min_cross_gap = kj_val * 0.003   # 0.3% khoảng cách tối thiểu

        tk_cross_bull = (float(tenkan_prev) < float(kijun_prev)
                         and tk_val > kj_val
                         and (tk_val - kj_val) >= min_cross_gap)
        tk_cross_bear = (float(tenkan_prev) > float(kijun_prev)
                         and tk_val < kj_val
                         and (kj_val - tk_val) >= min_cross_gap)
        if tk_cross_bull:
            score += 5
            signals.append(('ICHI', 'bull',
                'TK cat len KJ (' + f'{tk_val:,.0f}' + '>' + f'{kj_val:,.0f}' + ') -> Tin hieu mua ngan han'))
        elif tk_cross_bear:
            score -= 5
            signals.append(('ICHI', 'bear',
                'TK cat xuong KJ (' + f'{tk_val:,.0f}' + '<' + f'{kj_val:,.0f}' + ') -> Tin hieu ban ngan han'))
        elif tk_val > kj_val:
            signals.append(('ICHI', 'bull',
                'TK>KJ (' + f'{tk_val:,.0f}' + '>' + f'{kj_val:,.0f}' + ') -> Xu huong ngan han tang'))
        else:
            signals.append(('ICHI', 'bear',
                'TK<KJ (' + f'{tk_val:,.0f}' + '<' + f'{kj_val:,.0f}' + ') -> Xu huong ngan han giam'))

    # Bollinger Bands: ±3
    if price <= bb_lower:
        score += 3
        signals.append(('BB', 'bull', 'Gia cham BB duoi ' + f'{bb_lower:,.0f}' + ' -> Ho tro'))
    elif price >= bb_upper:
        score -= 3
        signals.append(('BB', 'bear', 'Gia cham BB tren ' + f'{bb_upper:,.0f}' + ' -> Khang cu'))
    else:
        signals.append(('BB', 'neutral', 'Gia trong BB (' + f'{bb_pct:.0f}' + '% trong dai)'))

    # Three-in-one: giá trên MA20, volume đột biến, giá tăng, RSI trung tính
    # Tích hợp vào score (+8) thay vì chỉ tính rồi bỏ đó
    three_in_one = (price > ma20 and vol_ratio >= 1.5 and price_up and 30 < rsi_val < 70)
    if three_in_one:
        score += 8
        signals.append(('3IN1', 'bull', 'Hoi tu 3-trong-1: Gia>MA20 + Vol dot bien + RSI trung tinh'))

    # ── MA10 / MA50 Horizon Signals (tham khảo — không ảnh hưởng score) ──────
    # Ngắn hạn: MA10 cross → momentum mới phục hồi, TP 7-10%
    # Trung hạn: giá trên MA50 bền vững → uptrend trung hạn, TP 25-30%
    # Thiết kế như vol spike: chỉ hiển thị khi có tín hiệu đáng chú ý
    if ma10_cross_up and ma10_slope_up:
        signals.append(('MA10', 'bull',
            f'GIA CAT LEN MA10 ({ma10:,.0f}) hom nay + MA10 doc len '
            f'-> Tin hieu NGAN HAN, TP tham khao 7-10%'))
    elif ma10_cross_down:
        signals.append(('MA10', 'bear',
            f'GIA CAT XUONG MA10 ({ma10:,.0f}) hom nay '
            f'-> Mat dong luc ngan han, theo doi'))
    elif above_ma10 and ma10_slope_up:
        signals.append(('MA10', 'bull',
            f'Gia tren MA10 ({ma10:,.0f}) + MA10 doc len '
            f'-> Dang trong da tang ngan han'))
    elif not above_ma10:
        signals.append(('MA10', 'bear',
            f'Gia duoi MA10 ({ma10:,.0f}) -> Chua co dong luc ngan han'))

    if above_ma50 and ma50_slope_up:
        ma50_dist = (price - ma50) / ma50 * 100
        signals.append(('MA50', 'bull',
            f'Gia tren MA50 ({ma50:,.0f}) +{ma50_dist:.1f}% + MA50 doc len '
            f'-> Uptrend TRUNG HAN xac nhan, TP tham khao 25-30%'))
    elif above_ma50 and not ma50_slope_up:
        signals.append(('MA50', 'neutral',
            f'Gia tren MA50 ({ma50:,.0f}) nhung MA50 phang/giam '
            f'-> Trung han chua ro xu huong'))
    else:
        signals.append(('MA50', 'bear',
            f'Gia duoi MA50 ({ma50:,.0f}) -> Chua vao vung trung han'))

    # ── Weekly Trend Scoring (khung tuần — tối đa ±10) ───────────────────────
    # Dùng MA100D (≈MA10W) và MA200D (≈MA20W) đã tính ở trên
    # Mục đích: lọc lệnh MUA trong downtrend tuần, xác nhận thêm cho uptrend tuần
    if weekly_trend == 'STRONG_UP':
        score += 10
        signals.append(('1W', 'bull', weekly_trend_vn + ' -> Xu huong tuan xac nhan'))
    elif weekly_trend == 'UP':
        score += 5
        signals.append(('1W', 'bull', weekly_trend_vn))
    elif weekly_trend == 'WEAK_UP':
        score += 2
        signals.append(('1W', 'neutral', weekly_trend_vn + ' -> Can theo doi them'))
    elif weekly_trend == 'PULLBACK':
        score -= 5
        signals.append(('1W', 'neutral', weekly_trend_vn + ' -> Pullback trong uptrend tuan'))
    else:  # DOWN
        score -= 10
        signals.append(('1W', 'bear', weekly_trend_vn + ' -> CANH BAO: downtrend tuan'))

    score = max(0, min(100, score))

    # ── Hard Filter: MA20 Trend Gate ────────────────────────────────────────
    # Tầng 1: Downtrend 2 tầng (price < MA20 < MA50) → cap cứng 55
    #         Dù mọi indicator đẹp, không bao giờ ra lệnh MUA
    # Tầng 2: Pullback trong uptrend (price < MA20, MA20 > MA50) → cap 68
    #         Vẫn có thể MUA nhưng cần tín hiệu đặc biệt mạnh
    # Tầng 3: price > MA20 → không giới hạn
    ma20_distance = (ma20 - price) / ma20 if ma20 > 0 else 0.0  # % dưới MA20

    hard_filter_reason = ''
    if price < ma20 and ma20 < ma50:
        # ── DCB Exception: Dead Cat Bounce ──────────────────────────────────
        # Nới lỏng cap 55 → 60 khi hội đủ 3 điều kiện:
        #   1. Giá dưới MA20 >= 15% (oversold cực đoan, không còn là pullback)
        #   2. RSI < 25 (cực kỳ oversold theo Wilder)
        #   3. Volume cạn kiệt (weak_sell) → lực bán đã kiệt, KHÔNG phải limit_down
        dcb_condition = (
            ma20_distance >= 0.15 and
            rsi_val < 25 and
            vol_signal == 'weak_sell' and
            not is_limit_down   # Phân biệt kiệt cung vs mất thanh khoản
            # Lưu ý: breadth thị trường sẽ được kiểm tra thêm ở B-filter
            # DCB chỉ nới lỏng cap 55→60, vẫn cần score_min riêng của mã
        )
        if dcb_condition:
            score = min(score, 60)
            hard_filter_reason = (
                'DCB EXCEPTION: Gia duoi MA20 '
                + f'{ma20_distance*100:.0f}%'
                + ' RSI=' + str(rsi_val)
                + ' Vol can kiet (KHONG phai san trang) -> Co the co nhip hoi ky thuat'
            )
            signals.append(('FILTER', 'neutral', hard_filter_reason))
            logger.info(f"[{symbol}] DCB exception applied, score capped at 60")
        else:
            score = min(score, 55)
            hard_filter_reason = (
                'HARD FILTER: Downtrend 2 tang Gia<MA20<MA50 '
                '-> Score khong vuot 55, khong bao gio ra lenh MUA'
            )
            signals.append(('FILTER', 'bear', hard_filter_reason))
            logger.info(f"[{symbol}] Hard filter (downtrend 2-tier), score capped at 55")

    elif price < ma20:
        # Pullback trong uptrend → cap 68 (cần rất nhiều tín hiệu để MUA)
        score = min(score, 68)
        hard_filter_reason = (
            'HARD FILTER: Gia duoi MA20 (pullback trong uptrend) '
            '-> Score khong vuot 68, can tin hieu rat manh'
        )
        signals.append(('FILTER', 'neutral', hard_filter_reason))
        logger.info(f"[{symbol}] Hard filter (pullback), score capped at 68")

    # Limit down override cuối: dù score bao nhiêu cũng không MUA
    if is_limit_down:
        score = min(score, 30)
        signals.append(('FILTER', 'bear',
            'HARD FILTER: GIA SAN [' + exchange + '] trang ben mua -> '
            'Score bi gioi han 30, KHONG MUA trong moi truong hop'))
        logger.warning(f"[{symbol}] Limit down hard cap applied, score={score}")

    # ── Weekly Downtrend Hard Cap ────────────────────────────────────────────
    # Cap 58 chỉ áp dụng khi chưa có hard filter MA20 mạnh hơn (cap 55)
    # Fix: tránh weekly cap 58 ghi đè hard filter 55 (55 < 58 nên 55 phải thắng)
    if weekly_trend == 'DOWN' and not is_limit_down:
        new_cap = 58
        if score > new_cap:   # Chỉ cap nếu score đang cao hơn 58
            score = new_cap
            if not hard_filter_reason:
                hard_filter_reason = (
                    'WEEKLY FILTER: Downtrend tuan (Gia<MA10W<MA20W) '
                    '-> Score cap 58, can than khi vao lenh MUA'
                )
            signals.append(('FILTER', 'bear',
                'WEEKLY FILTER: ' + weekly_trend_vn + ' -> Score cap 58'))
            logger.info(f"[{symbol}] Weekly downtrend cap applied, score={score}")

    if score >= 65:
        action = 'MUA'
    elif score <= 35:
        action = 'BAN'
    else:
        action = 'THEO DOI'

    # Khởi tạo entry zone mặc định (override trong block MUA bên dưới)
    entry_zone_low  = round(price, 0)
    entry_zone_high = round(price, 0)
    entry_label     = ''

    if action == 'MUA':
        stop_loss = round(price * 0.93, 0)
        take_profit = round(price * 1.14, 0)
        sl_label = '-7%'
        tp_label = '+14%'
        rebuy_zone = None

        # ── Tính điểm vào lệnh tối ưu ─────────────────────────────────────
        # Chiến lược: đặt limit order tại điểm giữa giá hiện tại và hỗ trợ gần nhất
        # Không mua cao hơn giá hiện tại + 0.5% (tránh chase giá)
        entry_max = round(price * 1.005, 0)  # Mua tối đa +0.5% so với giá hiện tại

        if supports:
            sup_price = supports[0]['price']
            dist_to_sup = (price - sup_price) / price
            if dist_to_sup < 0.015:
                # Hỗ trợ rất gần → mua ngay
                entry_opt   = price
                entry_label = 'Mua ngay (HT rat gan)'
            elif dist_to_sup < 0.05:
                # Hỗ trợ trong 1-5% → đặt limit ngay trên hỗ trợ (+0.5%)
                entry_opt   = round(sup_price * 1.005, 0)
                entry_label = 'Limit ngay tren HT ' + f'{sup_price:,.0f}'
            else:
                # Hỗ trợ xa → đặt limit tại -2% so với giá hiện tại
                entry_opt   = round(price * 0.98, 0)
                entry_label = 'Limit -2% cho pullback'
        else:
            # Không có hỗ trợ → dùng BB lower làm tham chiếu
            bb_l = bb_lower if bb_lower > 0 else price * 0.97
            entry_opt = round((price + bb_l) / 2, 0)
            entry_label = 'Limit tai vung BB'

        # Đảm bảo entry_opt không thấp hơn SL
        entry_opt = max(entry_opt, stop_loss + round(price * 0.01, 0))

        entry_zone_low  = entry_opt
        entry_zone_high = entry_max
    elif action == 'BAN':
        stop_loss = price
        if supports:
            rebuy_zone = supports[0]['price']
        else:
            rebuy_zone = round(price * 0.90, 0)
        take_profit = rebuy_zone
        sl_label = 'Nen ban ngay'
        tp_label = 'Vung mua lai'
    else:
        stop_loss = round(price * 0.93, 0)
        take_profit = round(price * 1.07, 0)
        rebuy_zone = None
        sl_label = '-7% neu da mua'
        tp_label = '+7% tham khao'

    return {
        'price': round(price, 0),
        'rsi': rsi_val,
        'rsi_divergence': {'type': div_type, 'message': div_msg},
        'macd': round(macd_val, 1),
        'macd_signal': round(macd_sig, 1),
        'macd_hist': round(macd_h, 1),
        'ma10': round(ma10, 0),
        'ma20': round(ma20, 0),
        'ma50': round(ma50, 0),
        'ma10_cross_up':   ma10_cross_up,
        'ma10_cross_down': ma10_cross_down,
        'ma10_slope_up':   ma10_slope_up,
        'above_ma10':      above_ma10,
        'above_ma50':      above_ma50,
        'ma50_slope_up':   ma50_slope_up,
        'golden_cross': golden_cross,
        'death_cross': death_cross,
        'bb_upper': round(bb_upper, 0),
        'bb_lower': round(bb_lower, 0),
        'bb_pct': round(bb_pct, 1),
        'vol_today': int(vol_today),
        'vol_tb20': int(vol_ma20),
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
        'entry_opt':        round(price if action != 'MUA' else entry_zone_low,  0) if action == 'MUA' else round(price, 0),
        'entry_max':        round(price if action != 'MUA' else entry_zone_high, 0) if action == 'MUA' else round(price, 0),
        'entry_label':      entry_label if action == 'MUA' else '',
        'stop_loss': stop_loss,
        'take_profit': take_profit,
        'sl_label': sl_label,
        'tp_label': tp_label,
        # Meta: Hard Filter & Limit Down info
        'exchange': exchange,
        'is_limit_down': is_limit_down,
        'ma20_distance_pct': round(ma20_distance * 100, 1),
        'hard_filter': hard_filter_reason,
        # Weekly trend (khung tuần)
        'weekly_trend':    weekly_trend,
        'weekly_trend_vn': weekly_trend_vn,
        'ma100': round(ma100, 0),
        'ma200': round(ma200, 0),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# FUNDAMENTAL ANALYSIS — Fair Value Engine
# ═══════════════════════════════════════════════════════════════════════════════
# Cập nhật 1 lần/ngày lúc 8:30 sáng, cache đến hết ngày.
# Không chạy lại mỗi lần /analyze để tiết kiệm API call.
#
# Phân nhóm phương pháp định giá:
#   BANK       : P/B + P/E     (ngân hàng thương mại)
#   SECURITIES : P/B + P/E     (công ty chứng khoán — ROE cao, đòn bẩy thấp hơn bank)
#   CYCLICAL   : EV/EBITDA     (hóa chất, thép, dầu khí — chu kỳ rõ)
#   UTILITY    : EV/EBITDA cao (điện, hạ tầng — dòng tiền ổn, định giá cao hơn)
#   GROWTH     : PEG           (công nghệ, bán lẻ tăng trưởng cao)
#   DEFAULT    : Graham + P/E  (còn lại)
# ═══════════════════════════════════════════════════════════════════════════════

FA_GROUP = {
    'BANK':       {'MBB', 'BID', 'VCB', 'CTG', 'TCB', 'VPB', 'ACB', 'STB', 'LPB'},
    'SECURITIES': {'HCM', 'SSI', 'VND', 'SHS', 'MBS', 'CTS'},
    'CYCLICAL':   {'DCM', 'DGC', 'HPG', 'HSG', 'NKG', 'GAS', 'PVS', 'PVD'},
    # UTILITY: điện (REE, POW, NT2, PC1) và KCN (SZC, BCG, IDC, KBC)
    # KCN có EV/EBITDA cao hơn điện vì biên lợi nhuận cao hơn
    # → dùng chung nhóm UTILITY với ngưỡng EV/EBITDA trung gian
    'UTILITY':    {'REE', 'POW', 'NT2', 'PC1', 'SZC', 'BCG', 'GEG', 'IDC', 'KBC'},
    'GROWTH':     {'FPT', 'CMG', 'MWG', 'FRT'},
}

FA_CACHE_TTL = 8 * 3600   # 8 tiếng — đủ cho cả ngày giao dịch
_fa_cache    = {}
_fa_lock     = threading.Lock()


def _get_fa_group(symbol):
    for grp, syms in FA_GROUP.items():
        if symbol.upper() in syms:
            return grp
    return 'DEFAULT'


def _load_financials(symbol):
    """
    Tải báo cáo tài chính từ vnstock.
    Trả về dict với income_q, balance_q, cashflow_q (quý) và income_yr, balance_yr (năm).
    """
    try:
        from vnstock import Vnstock
        stk = Vnstock().stock(symbol=symbol, source='VCI')

        income_q   = stk.finance.income_statement(period='quarter', lang='en')
        balance_q  = stk.finance.balance_sheet(period='quarter', lang='en')
        cashflow_q = stk.finance.cash_flow(period='quarter', lang='en')
        income_yr  = stk.finance.income_statement(period='year', lang='en')
        balance_yr = stk.finance.balance_sheet(period='year', lang='en')

        # Sort tăng dần theo thời gian (quý cũ → mới) để .tail(4) lấy đúng 4 quý gần nhất
        def _sort_df(df):
            if df is None or df.empty:
                return df
            for col in df.columns:
                if any(k in col.lower() for k in ['year', 'quarter', 'period', 'date', 'nam', 'quy']):
                    try:
                        # Convert sang datetime trước khi sort để xử lý đúng mọi format
                        # vnstock VCI dùng format 'yearReport', 'lengthReport' hoặc datetime
                        dt_col = pd.to_datetime(df[col], errors='coerce')
                        if dt_col.notna().sum() >= len(df) * 0.5:
                            return df.assign(_sort_key=dt_col).sort_values(
                                '_sort_key', ascending=True
                            ).drop(columns='_sort_key').reset_index(drop=True)
                        else:
                            # Fallback: sort as string (vẫn đúng cho '2021','2022','2023')
                            return df.sort_values(col, ascending=True).reset_index(drop=True)
                    except Exception:
                        pass
            return df

        return {
            'income_q':   _sort_df(income_q),
            'balance_q':  _sort_df(balance_q),
            'cashflow_q': _sort_df(cashflow_q),
            'income_yr':  _sort_df(income_yr),
            'balance_yr': _sort_df(balance_yr),
            'ok': True,
        }
    except Exception as e:
        logger.warning(f'FA load_financials {symbol}: {e}')
        return {'ok': False, 'error': str(e)}


def _safe_val(df, col_patterns, row_idx=-1, default=0.0):
    """Lấy giá trị an toàn từ DataFrame — thử nhiều tên cột."""
    if df is None or df.empty:
        return default
    for pat in col_patterns:
        for col in df.columns:
            if pat.lower() in col.lower():
                try:
                    val = float(df.iloc[row_idx][col])
                    if val != 0:
                        return val
                except Exception:
                    pass
    return default


def _ttm(df, col_patterns):
    """
    Tính TTM (Trailing Twelve Months) = tổng 4 quý gần nhất.
    DataFrame phải được sort tăng dần (gọi _sort_df trước).
    Trả về (total, has_negative_quarter) để phát hiện quý lỗ bất thường.
    """
    if df is None or df.empty:
        return 0.0, False
    for pat in col_patterns:
        for col in df.columns:
            if pat.lower() in col.lower():
                try:
                    vals = df[col].tail(4).apply(
                        lambda x: float(x) if x is not None else 0.0
                    ).values
                    total = float(vals.sum())
                    has_neg = bool((vals < 0).any())
                    if total != 0:
                        return total, has_neg
                except Exception:
                    pass
    return 0.0, False


def _normalize_shares(shares_raw):
    """
    Normalize số cổ phiếu về đơn vị cổ phiếu thực tế.

    vnstock VCI thực tế trả về shares theo đơn vị tỷ CP:
      VCB  = ~4.71  (4.71 tỷ CP)
      MBB  = ~4.68  (4.68 tỷ CP)
      DGC  = ~0.12  (120 triệu CP)
      PC1  = ~0.21  (210 triệu CP)

    Heuristic theo range giá trị thực tế TTCK VN:
      < 1e2  → đơn vị tỷ CP  → × 1e9   (VCB=4.71 → 4.71 tỷ)
      < 1e5  → đơn vị triệu  → × 1e6   (VCB=4710 → 4.71 tỷ)
      < 1e8  → đơn vị nghìn  → × 1e3   (VCB=4710000 → 4.71 tỷ)
      else   → đơn vị thực   → giữ nguyên
    """
    if shares_raw <= 0:
        return 1e9  # fallback 1 tỷ CP

    if shares_raw < 1e2:
        return shares_raw * 1e9   # tỷ CP → CP thực
    elif shares_raw < 1e5:
        return shares_raw * 1e6   # triệu CP → CP thực
    elif shares_raw < 1e8:
        return shares_raw * 1e3   # nghìn CP → CP thực
    else:
        return shares_raw         # đã là CP thực


# Ngưỡng tỷ số hợp lý theo ngành TTCK VN (dựa trên dữ liệu lịch sử 2019-2024)
SECTOR_RATIOS = {
    'BANK': {
        'pe_fair': 9.5,  'pe_low': 7.0,  'pe_high': 13.0,
        'pb_fair': 1.4,  'pb_low': 0.9,  'pb_high': 2.2,
        'w_pb': 0.6,     'w_pe': 0.4,    # Trọng số: P/B quan trọng hơn với ngân hàng
    },
    'SECURITIES': {
        # Fix: công ty CK ROE cao hơn ngân hàng, P/B và P/E cao hơn
        'pe_fair': 12.0, 'pe_low': 8.0,  'pe_high': 18.0,
        'pb_fair': 1.8,  'pb_low': 1.0,  'pb_high': 3.0,
        'w_pb': 0.4,     'w_pe': 0.6,    # Trọng số: P/E quan trọng hơn với CK
    },
    'CYCLICAL': {
        # Hóa chất, thép — EV/EBITDA thấp vì chu kỳ và capex cao
        'ev_ebitda_fair': 6.5, 'ev_ebitda_low': 4.0, 'ev_ebitda_high': 9.0,
        'pe_fair': 10.0,       'pe_low': 6.0,         'pe_high': 15.0,
    },
    'UTILITY': {
        # Fix: điện/KCN dòng tiền ổn định → EV/EBITDA cao hơn CYCLICAL
        'ev_ebitda_fair': 9.0, 'ev_ebitda_low': 6.0, 'ev_ebitda_high': 13.0,
        'pe_fair': 13.0,       'pe_low': 9.0,         'pe_high': 18.0,
    },
    'GROWTH': {
        'peg_fair': 1.0, 'pe_fair': 18.0, 'pe_low': 12.0, 'pe_high': 28.0,
    },
    'DEFAULT': {
        'pe_fair': 12.0, 'pe_low': 8.0,  'pe_high': 18.0,
        'pb_fair': 1.5,  'pb_low': 0.8,  'pb_high': 2.5,
        'w_pb': 0.5,     'w_pe': 0.5,
    },
}


def compute_fair_value(symbol, price):
    """
    Tính fair value cho một mã cổ phiếu.
    Trả về dict:
      fair_low, fair_value, fair_high  : vùng giá hợp lý
      valuation  : 'UNDERVALUED' / 'FAIR' / 'OVERVALUED'
      discount   : % chênh lệch giá vs fair_value (âm = đắt, dương = rẻ)
      method     : phương pháp đã dùng
      details    : dict các chỉ số cơ bản tính được
      note       : ghi chú cảnh báo nếu dữ liệu không đủ
    """
    import math

    sym    = symbol.upper()
    group  = _get_fa_group(sym)
    ratios = SECTOR_RATIOS.get(group, SECTOR_RATIOS['DEFAULT'])

    fin = _load_financials(sym)
    if not fin.get('ok'):
        return {'ok': False, 'error': fin.get('error', 'Khong tai duoc BCTC'), 'symbol': sym}

    iq  = fin['income_q']
    bq  = fin['balance_q']
    cq  = fin['cashflow_q']
    iyr = fin['income_yr']

    # Normalize số cổ phiếu
    shares_raw = _safe_val(bq, ['shares', 'so_co_phieu', 'outstanding'], default=0)
    if shares_raw <= 0:
        shares_raw = _safe_val(fin['balance_yr'], ['shares', 'so_co_phieu', 'outstanding'], default=1e9)
    shares = _normalize_shares(shares_raw)
    logger.info(f'FA {sym}: shares_raw={shares_raw} shares_normalized={shares:.0f}')

    details    = {}
    fair_value = 0.0
    method     = ''
    note       = ''
    warnings   = []

    # ── BANK và SECURITIES: P/B + P/E ────────────────────────────────────────
    if group in ('BANK', 'SECURITIES'):
        # equity là số dư cuối kỳ → dùng _safe_val (quý gần nhất), KHÔNG dùng _ttm
        equity = _safe_val(bq, ['equity', 'von_chu_so_huu', 'owner'])
        net_income, has_neg = _ttm(iq, ['net_income', 'loi_nhuan_sau_thue', 'profit_after'])

        # vnstock VCI trả về đơn vị tỷ đồng → nhân 1e9 để về đồng, chia shares ra đ/CP
        bvps = equity * 1e9 / shares     if equity > 0     else 0
        eps  = net_income * 1e9 / shares if net_income > 0 else 0

        pe = price / eps  if eps  > 0 and price > 0 else 0
        pb = price / bvps if bvps > 0 and price > 0 else 0

        details = {
            'EPS_TTM': round(eps, 0),
            'BVPS':    round(bvps, 0),
            'P/E':     round(pe, 1),
            'P/B':     round(pb, 2),
        }

        if has_neg:
            warnings.append('Co quy lo trong TTM — EPS co the bi keo thap bat thuong')

        w_pb = ratios.get('w_pb', 0.5)
        w_pe = ratios.get('w_pe', 0.5)

        if bvps > 0 and eps > 0:
            fv_pb      = bvps * ratios['pb_fair']
            fv_pe      = eps  * ratios['pe_fair']
            fair_value = fv_pb * w_pb + fv_pe * w_pe
            method     = f'P/B {int(w_pb*100)}% + P/E {int(w_pe*100)}%'
        elif bvps > 0:
            fair_value = bvps * ratios['pb_fair']
            method     = 'P/B (thieu EPS)'
            note       = 'EPS am hoac khong co — chi dung P/B'
        else:
            return {'ok': False, 'error': f'Khong du BVPS/EPS cho {group}', 'symbol': sym}

    # ── CYCLICAL và UTILITY: EV/EBITDA ───────────────────────────────────────
    elif group in ('CYCLICAL', 'UTILITY'):
        ebitda, has_neg = _ttm(iq, ['ebitda', 'operating_profit', 'loi_nhuan_tu_hoat_dong'])
        # Fallback: EBIT + Depreciation & Amortization
        if ebitda == 0:
            ebit, _ = _ttm(iq, ['ebit', 'operating'])
            da, _   = _ttm(cq, ['depreciation', 'khau_hao', 'amortization'])
            ebitda  = ebit + da

        # Nợ và tiền — đơn vị tỷ đồng
        total_debt = _safe_val(bq, ['total_debt', 'no_vay', 'long_term_debt', 'short_term_debt'])
        cash       = _safe_val(bq, ['cash', 'tien_va_tuong_duong', 'cash_equivalent'])
        # Fix: cho phép net_debt âm (net cash position)
        # VD DGC: cash=5000ty > debt=1000ty → net_debt=-4000ty → EV thấp → FV cao hơn (đúng)
        # max(0) sẽ cắt mất lợi thế tiền mặt, làm FV thấp hơn thực tế
        net_debt = total_debt - cash

        # Market cap: price (đ) × shares → tỷ đồng
        mkt_cap   = price * shares / 1e9 if price > 0 else 0
        ev        = mkt_cap + net_debt
        ev_ebitda = ev / ebitda if ebitda > 0 and ev > 0 else 0

        net_income, _ = _ttm(iq, ['net_income', 'loi_nhuan_sau_thue'])
        eps = net_income * 1e9 / shares if net_income > 0 else 0

        details = {
            'EBITDA_TTM_ty': round(ebitda, 0),
            'Net_Debt_ty' if net_debt >= 0 else 'Net_Cash_ty': round(abs(net_debt), 0),
            'EV/EBITDA':     round(ev_ebitda, 1),
            'EPS_TTM':       round(eps, 0),
        }

        if has_neg:
            warnings.append('Co quy EBITDA am — nen xem lai BCTC tung quy')

        if ebitda > 0:
            ev_fair     = ebitda * ratios['ev_ebitda_fair']          # tỷ đồng
            equity_fair = (ev_fair - net_debt) * 1e9                  # về đồng
            fair_value  = equity_fair / shares
            method      = f'EV/EBITDA ({group})'
            if ev_ebitda > 0:
                note = (f'EV/EBITDA hien tai: {ev_ebitda:.1f}x | '
                        f'Fair: {ratios["ev_ebitda_fair"]}x')
            # Sanity check: fair_value không được âm
            if fair_value <= 0:
                if eps > 0:
                    fair_value = eps * ratios['pe_fair']
                    method = 'P/E (EV/EBITDA cho ket qua am do no cao)'
                    warnings.append('No rong — EV/EBITDA cho ket qua am, dung P/E thay the')
                else:
                    return {'ok': False, 'error': 'No qua lon, EV/EBITDA va EPS deu cho ket qua bat hop le', 'symbol': sym}
        else:
            if eps > 0:
                fair_value = eps * ratios['pe_fair']
                method     = 'P/E (fallback khi thieu EBITDA)'
                note       = 'Khong co EBITDA — dung P/E thay the'
            else:
                return {'ok': False, 'error': 'Khong du EBITDA/EPS', 'symbol': sym}

    # ── GROWTH: PEG ──────────────────────────────────────────────────────────
    elif group == 'GROWTH':
        net_income, has_neg = _ttm(iq, ['net_income', 'loi_nhuan_sau_thue'])
        eps_ttm = net_income * 1e9 / shares if net_income > 0 else 0

        # Tính CAGR EPS 3 năm từ báo cáo năm
        growth_pct = 15.0   # default
        if iyr is not None and not iyr.empty:
            for col in iyr.columns:
                if 'net_income' in col.lower() or 'loi_nhuan_sau' in col.lower():
                    try:
                        vals = iyr[col].tail(4).dropna().astype(float).values
                        if len(vals) >= 3 and vals[0] > 0 and vals[-1] > 0:
                            cagr = (vals[-1] / vals[0]) ** (1 / (len(vals) - 1)) - 1
                            growth_pct = max(5.0, min(40.0, cagr * 100))
                    except Exception:
                        pass
                    break

        pe_peg = growth_pct * ratios['peg_fair']
        pe_use = min(pe_peg, ratios['pe_high'])
        pe     = price / eps_ttm if eps_ttm > 0 and price > 0 else 0

        details = {
            'EPS_TTM':     round(eps_ttm, 0),
            'Growth_3Y_%': round(growth_pct, 1),
            'P/E_hien_tai': round(pe, 1),
            'PE_PEG':      round(pe_peg, 1),
            'PE_dung':     round(pe_use, 1),
        }

        if has_neg:
            warnings.append('Co quy lo — tang truong co the khong ben vung')

        if eps_ttm > 0:
            fair_value = eps_ttm * pe_use
            method     = f'PEG (CAGR EPS {growth_pct:.0f}%/nam)'
        else:
            return {'ok': False, 'error': 'EPS am — khong dinh gia duoc', 'symbol': sym}

    # ── DEFAULT: Graham Number + P/E ─────────────────────────────────────────
    else:
        net_income, has_neg = _ttm(iq, ['net_income', 'loi_nhuan_sau_thue'])
        equity  = _safe_val(bq, ['equity', 'von_chu_so_huu'])
        eps_ttm = net_income * 1e9 / shares if net_income > 0 else 0
        bvps    = equity * 1e9 / shares     if equity > 0     else 0
        pe      = price / eps_ttm if eps_ttm > 0 and price > 0 else 0
        pb      = price / bvps    if bvps > 0    and price > 0 else 0

        details = {
            'EPS_TTM': round(eps_ttm, 0),
            'BVPS':    round(bvps, 0),
            'P/E':     round(pe, 1),
            'P/B':     round(pb, 2),
        }

        if has_neg:
            warnings.append('Co quy lo trong TTM')

        if eps_ttm > 0 and bvps > 0:
            graham     = math.sqrt(22.5 * eps_ttm * bvps)
            pe_val     = eps_ttm * ratios['pe_fair']
            fair_value = graham * 0.5 + pe_val * 0.5
            method     = 'Graham 50% + P/E 50%'
        elif eps_ttm > 0:
            fair_value = eps_ttm * ratios['pe_fair']
            method     = 'P/E (thieu BVPS)'
            note       = 'Khong co BVPS — chi dung P/E'
        else:
            return {'ok': False, 'error': 'EPS am — khong dinh gia duoc', 'symbol': sym}

    # ── Tính vùng giá và valuation ───────────────────────────────────────────
    if fair_value <= 0:
        return {'ok': False, 'error': 'Fair value tinh ra <= 0 — co the do no qua cao', 'symbol': sym}

    # Margin of safety: rộng hơn khi có cảnh báo dữ liệu
    margin = 0.20 if warnings else 0.15
    fair_low  = round(fair_value * (1 - margin), -2)
    fair_val  = round(fair_value, -2)
    fair_high = round(fair_value * (1 + margin), -2)

    discount = (fair_val - price) / fair_val * 100 if fair_val > 0 else 0

    if price > 0:
        if price < fair_low:
            valuation = 'UNDERVALUED'
        elif price > fair_high:
            valuation = 'OVERVALUED'
        else:
            valuation = 'FAIR'
    else:
        valuation = 'UNKNOWN'

    # Gộp warnings vào note
    if warnings and note:
        note = note + ' | ' + ' | '.join(warnings)
    elif warnings:
        note = ' | '.join(warnings)

    return {
        'ok':         True,
        'symbol':     sym,
        'group':      group,
        'method':     method,
        'fair_low':   int(fair_low),
        'fair_value': int(fair_val),
        'fair_high':  int(fair_high),
        'valuation':  valuation,
        'discount':   round(discount, 1),
        'margin_pct': int(margin * 100),
        'details':    details,
        'note':       note,
    }


def fetch_fair_value(symbol):
    """
    Lấy fair value với cache 8 tiếng.
    Chạy lại tự động khi cache hết hạn (đầu ngày GD tiếp theo).
    """
    sym = symbol.upper()
    key = 'fv_' + sym

    with _fa_lock:
        if key in _fa_cache:
            data, ts = _fa_cache[key]
            if time.time() - ts < FA_CACHE_TTL:
                return data

    logger.info(f'FA: computing fair value for {sym}')

    # Lấy giá hiện tại trước khi tính FV
    price = 0
    try:
        price_data = fetch_price(sym)
        price = price_data.get('price', 0) if price_data else 0
    except Exception as e:
        logger.warning(f'FA: cannot get price for {sym}: {e}')

    result = compute_fair_value(sym, price)

    with _fa_lock:
        _fa_cache[key] = (result, time.time())

    return result


def warmup_fair_values():
    """Khởi động trước khi phiên giao dịch — tính FV cho toàn bộ watchlist."""
    logger.info('FA warmup started for all watchlist symbols')
    for sym in list(WATCHLIST):
        try:
            fetch_fair_value(sym)
            time.sleep(2)   # Tránh rate limit vnstock
        except Exception as e:
            logger.warning(f'FA warmup {sym}: {e}')
    logger.info('FA warmup done')


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
            cc = find_col(df, ['close', 'closeprice', 'close_price'])
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

    return {'symbol': symbol, 'price': 0, 'change_pct': 0, 'source': 'error', 'error': 'Khong the lay gia'}


def fetch_analysis(symbol, price_override=None):
    cache_key = 'analysis_' + symbol + '_' + (str(price_override) if price_override else 'live')
    if not price_override:
        cached = get_cached(cache_key)
        if cached and cached.get('vol_ma20', 0) > 0 and cached.get('vol_tb20', 0) > 0:
            logger.info(f"{symbol}: served from cache vol_ma20={cached['vol_ma20']}")
            return cached

    logger.info(f"{symbol}: computing directly...")
    # Fix: tăng lên 400 ngày calendar (~280 phiên GD) để đủ MA200D cho weekly trend
    df, source = load_history(symbol, days=400)
    if df is None:
        logger.error(f"{symbol}: load_history returned None")
        return {'symbol': symbol, 'error': 'Khong tai duoc du lieu'}

    logger.info(f"{symbol}: df rows={len(df)} cols={list(df.columns)}")
    try:
        result = compute_indicators(df, price_override, symbol=symbol)
    except Exception as e:
        logger.error(f"compute {symbol}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {'symbol': symbol, 'error': str(e)}

    if result is None:
        logger.error(f"{symbol}: compute_indicators returned None")
        return {'symbol': symbol, 'error': 'Khong tinh duoc chi bao'}

    logger.info(f"{symbol}: computed vol_today={result.get('vol_today', 0)} vol_ma20={result.get('vol_ma20', 0)}")
    result['symbol'] = symbol
    result['source'] = source
    if result.get('vol_ma20', 0) > 0:
        set_cache(cache_key, result)

    # Thêm cảnh báo 1H nếu không phải price_override
    if not price_override:
        try:
            result['warnings_1h'] = analyze_1h_warnings(symbol)
        except Exception as e:
            logger.warning(f"{symbol}/1H warnings error: {e}")
            result['warnings_1h'] = []
    else:
        result['warnings_1h'] = []

    # Thêm Fair Value (cache 8h — không tính lại mỗi lần analyze)
    try:
        fv = fetch_fair_value(symbol)
        result['fair_value'] = fv
    except Exception as e:
        logger.warning(f"{symbol}/FA fair_value error: {e}")
        result['fair_value'] = {'ok': False, 'error': str(e)}

    return result


@app.route('/')
def index():
    return jsonify({'status': 'ok', 'message': 'VN Trader API v4.2',
                    'weights': 'VOL20 RSI20 MA20 balanced + FA fair value'})

@app.route('/api/price/<symbol>')
def api_price(symbol):
    return jsonify(fetch_price(symbol.upper()))

@app.route('/api/analyze/<symbol>')
def api_analyze(symbol):
    return jsonify(fetch_analysis(symbol.upper()))

@app.route('/api/fairvalue/<symbol>')
def api_fairvalue(symbol):
    """Endpoint riêng để lấy fair value — dùng khi cần refresh thủ công."""
    sym = symbol.upper()
    # Xóa cache để tính lại
    with _fa_lock:
        _fa_cache.pop('fv_' + sym, None)
    result = fetch_fair_value(sym)
    return jsonify(result)

@app.route('/api/warnings_1h/<symbol>')
def api_warnings_1h(symbol):
    """Endpoint riêng cho cảnh báo 1H — gọi async từ Telegram bot."""
    try:
        warnings = analyze_1h_warnings(symbol.upper())
        return jsonify({'symbol': symbol.upper(), 'warnings_1h': warnings})
    except Exception as e:
        return jsonify({'symbol': symbol.upper(), 'warnings_1h': [], 'error': str(e)})

@app.route('/api/whatif/<symbol>/<int:target_price>')
def api_whatif(symbol, target_price):
    return jsonify(fetch_analysis(symbol.upper(), price_override=target_price))

@app.route('/api/market')
def api_market():
    result = {}
    for sym, name in [('VNINDEX', 'VN-INDEX'), ('HNX30', 'HNX-INDEX'), ('VN30F1M', 'VN30')]:
        d = fetch_price(sym)
        result[sym] = dict(d)
        result[sym]['name'] = name
        time.sleep(0.5)
    return jsonify(result)


# ── WATCHLIST: đồng bộ với WATCHLIST_META trong telegram_bot ─────────────────
# Đọc từ biến môi trường WATCHLIST_SYMBOLS nếu có (để dễ cập nhật không cần deploy)
# Format: "DGC,DCM,MBB,HCM,PC1"
# Fallback về danh sách mặc định nếu không có env var
_wl_env = os.environ.get('WATCHLIST_SYMBOLS', '')
WATCHLIST = (
    [s.strip().upper() for s in _wl_env.split(',') if s.strip()]
    if _wl_env
    else ['DGC', 'DCM', 'MBB', 'HCM', 'PC1']
)

_bg_running = False
_bg_lock = threading.Lock()


def start_background_cache():
    global _bg_running
    with _bg_lock:
        if _bg_running:
            return
        _bg_running = True

    def worker():
        time.sleep(5)
        # FA warmup lần đầu khi khởi động
        try:
            warmup_fair_values()
        except Exception as e:
            logger.warning(f'FA initial warmup error: {e}')

        _last_fa_warmup_day = -1   # Track ngày đã warmup FA

        while True:
            # Warmup FA mỗi ngày lúc 8:30 sáng VN
            try:
                import pytz as _tz
                _now = __import__('datetime').datetime.now(_tz.timezone('Asia/Ho_Chi_Minh'))
                if (_now.weekday() < 5 and _now.hour == 8 and _now.minute >= 30
                        and _last_fa_warmup_day != _now.day):
                    _last_fa_warmup_day = _now.day
                    logger.info('FA daily warmup at 8:30')
                    warmup_fair_values()
            except Exception as e:
                logger.warning(f'FA daily warmup error: {e}')

            for sym in WATCHLIST:
                try:
                    df, source = load_history(sym, days=400)  # Fix: 400 ngày cho MA200
                    if df is not None:
                        result = compute_indicators(df, symbol=sym)
                        if result and result.get('vol_ma20', 0) > 0:
                            result['symbol'] = sym
                            result['source'] = source
                            # Thêm cảnh báo 1H vào cache
                            try:
                                result['warnings_1h'] = analyze_1h_warnings(sym)
                            except Exception:
                                result['warnings_1h'] = []
                            cache_key = 'analysis_' + sym + '_live'
                            set_cache(cache_key, result)
                            logger.info(sym + ' OK vol_ma20=' + str(int(result['vol_ma20'])))
                        else:
                            logger.warning(sym + ': vol_ma20=0, bo qua cache')
                    time.sleep(4)   # 5 mã × 4s = 20s/vòng, tránh rate limit vnstock
                except Exception as e:
                    logger.warning('cache ' + sym + ': ' + str(e))
            time.sleep(60)

    threading.Thread(target=worker, daemon=True).start()
    logger.info('Background cache started for ' + str(len(WATCHLIST)) + ' symbols')


@app.route('/api/signals')
def api_signals():
    start_background_cache()
    results = []
    for sym in WATCHLIST:
        cached = get_cached('analysis_' + sym + '_live')
        if cached and 'score' in cached and 'error' not in cached:
            results.append(cached)

    if len(results) < 3:
        for sym in ['VCB', 'HPG', 'FPT']:
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


@app.route('/api/clearcache')
def api_clearcache():
    with _cache_lock:
        _cache.clear()
    start_background_cache()
    return jsonify({'status': 'cache cleared', 'msg': 'Rebuilding in background...'})


@app.route('/api/debug/<symbol>')
def api_debug(symbol):
    sym = symbol.upper()
    result = {'symbol': sym, 'attempts': []}
    from datetime import datetime, timedelta
    end = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    for source in ['VCI', 'TCBS']:
        try:
            from vnstock import Vnstock
            df = Vnstock().stock(symbol=sym, source=source).quote.history(
                start=start, end=end, interval='1D'
            )
            if df is not None and not df.empty:
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
            result['attempts'].append({'source': source, 'status': 'error', 'msg': str(e)[:200]})

    return jsonify(result)


# Auto-start background cache khi Flask load
threading.Thread(target=lambda: (time.sleep(5), start_background_cache()), daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
