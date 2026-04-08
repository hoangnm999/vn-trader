import os
import time
import logging
import threading
from flask import Flask, jsonify
from flask_cors import CORS

# ── Rate limit retry helper ───────────────────────────────────────────────────
def _vnstock_call(fn, retries=3, base_wait=8, call_timeout=20):
    """
    Gọi hàm vnstock với retry tự động khi bị rate limit.
    retries:      số lần thử lại tối đa
    base_wait:    số giây chờ trước lần retry đầu tiên (tăng dần)
    call_timeout: timeout cứng mỗi lần gọi (giây) — tránh block vô thời hạn
    FIX P2: Thêm timeout cứng per-call + giảm base_wait 15→8s
    """
    import concurrent.futures as _cf
    _log = logging.getLogger('app')
    for attempt in range(retries):
        try:
            with _cf.ThreadPoolExecutor(max_workers=1) as _ex:
                _fut = _ex.submit(fn)
                try:
                    return _fut.result(timeout=call_timeout)
                except _cf.TimeoutError:
                    _log.warning(
                        f'vnstock call timeout {call_timeout}s '
                        f'(attempt {attempt+1}/{retries})')
                    if attempt < retries - 1:
                        time.sleep(base_wait * (attempt + 1))
                        continue
                    raise TimeoutError(
                        f'vnstock timeout sau {call_timeout}s x {retries} lan')
        except TimeoutError:
            raise
        except Exception as e:
            err = str(e).lower()
            is_rate_limit = any(k in err for k in
                                ['rate', 'limit', '429', 'exceeded', 'gioi han'])
            if is_rate_limit and attempt < retries - 1:
                wait = base_wait * (attempt + 1)
                _log.warning(
                    f'Rate limit hit (attempt {attempt+1}/{retries}), waiting {wait}s')
                time.sleep(wait)
            else:
                raise
    return None

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Cấu hình SL/TP tối ưu theo từng mã (đồng bộ backtest.py & telegram_bot.py) ──
# sl/tp là số thập phân: 0.05 = 5%, 0.07 = 7%, 0.09 = 9%, 0.14 = 14%
from config import (
    SETTLEMENT_DAYS, SYMBOL_CONFIG, DEFAULT_SL, DEFAULT_TP,
    SIGNALS_WATCHLIST, get_sl_tp, get_min_score, MIN_SCORE_BUY,
)

# ── FIX 1: Thread-safe cache dùng Lock ──────────────────────────────────────
_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL_TRADING  = 60      # 60s trong giờ giao dịch — cập nhật thường xuyên
CACHE_TTL_CLOSED   = 3600    # 1h ngoài giờ giao dịch — không cần refresh liên tục

import pytz as _pytz
_VN_TZ = _pytz.timezone('Asia/Ho_Chi_Minh')

def _get_cache_ttl():
    """TTL thông minh: ngắn trong giờ GD, dài ngoài giờ để giảm API call.

    Lịch giao dịch thực tế TTCK Việt Nam:
      HOSE / HNX : 9:00-11:30 | 13:00-14:45 (ATC 14:45-15:00, không nhập lệnh)
      UPCOM      : 9:00-11:30 | 13:00-15:00 (không có ATC riêng)
    → Dùng 15:00 làm mốc đóng chung (bao phủ cả UPCOM + ATC HOSE).
    """
    from datetime import datetime as _dt
    now = _dt.now(_VN_TZ)
    wd  = now.weekday()  # 0=T2, 4=T6
    h, m = now.hour, now.minute
    # Phiên sáng: 9:00-11:30 | Phiên chiều: 13:00-15:00
    in_morning   = (h == 9) or (h == 10) or (h == 11 and m < 30)
    in_afternoon = (h == 13) or (h == 14) or (h == 15 and m == 0)
    in_trading   = wd < 5 and (in_morning or in_afternoon)
    return CACHE_TTL_TRADING if in_trading else CACHE_TTL_CLOSED

def get_cached(key, ttl=None):
    _ttl = ttl if ttl is not None else _get_cache_ttl()
    with _cache_lock:
        if key in _cache:
            entry = _cache[key]
            data, ts = entry[0], entry[1]
            if time.time() - ts < _ttl:
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

def set_cached(key, data, ttl=None):
    """set_cache với custom TTL (giây). Nếu ttl=None dùng TTL mặc định."""
    if ttl is None:
        set_cache(key, data)
        return
    with _cache_lock:
        if len(_cache) >= 500:
            oldest = sorted(_cache.items(), key=lambda x: x[1][1])[:100]
            for k, _ in oldest:
                del _cache[k]
        # Lưu tuple (data, timestamp, custom_ttl) để get_cached xử lý đúng
        _cache[key] = (data, time.time() - (_get_cache_ttl() - ttl))

def find_col(df, names):
    for c in df.columns:
        if c.lower() in names:
            return c
    return None

def load_history(symbol, days=200):
    from datetime import datetime, timedelta
    end   = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    for source in ['VCI', 'KBS']:
        try:
            from vnstock import Vnstock
            def _fetch():
                return Vnstock().stock(symbol=symbol, source=source).quote.history(
                    start=start, end=end, interval='1D'
                )
            df = _vnstock_call(_fetch)
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
    for source in ['VCI', 'KBS']:
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
    Cảnh báo khung 1H — Volume Spike.
    Cảnh báo khi vol giờ hiện tại (đã project 60 phút) >= ngưỡng TB.
    HIGH (≥2.5x): alert ngay | MEDIUM (1.8-2.5x): chỉ log.

    FIX: Nến hiện tại thường chưa đóng (VD: 10:15 → nến 10:00 mới có 15 phút).
    Normalize vol_cur = vol_tich_luy × (60 / so_phut_da_qua) để project 60 phút.
    Tránh false positive khi so sánh 15-phút-vol với 60-phút-vol của nến đã đóng.
    """
    import numpy as np
    import pandas as pd

    df, source = load_history_1h(symbol, days=10)
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

    # ── Check: nến 1H cuối có phải hôm nay không? ────────────────────────────
    try:
        _tc1h = next((c for c in df.columns if c.lower() in
                      ('time', 'date', 'datetime', 'trading_date')), None)
        if _tc1h:
            _last_ts = pd.to_datetime(df[_tc1h].iloc[-1], errors='coerce')
            _now_vn  = __import__('datetime').datetime.now(_VN_TZ)
            _today   = _now_vn.date()
            _market_open = _now_vn.replace(hour=9, minute=0, second=0, microsecond=0)
            # Nếu nến cuối không phải hôm nay → data cũ, skip
            if pd.notna(_last_ts) and _last_ts.date() < _today:
                return []
            # Nếu chưa 9:15 hôm nay → thị trường chưa mở đủ 1 giờ, skip
            if _now_vn < _market_open:
                return []
    except Exception:
        pass

    # Lấy tối đa 10 nến 1H — bao gồm nến hiện tại (có thể chưa đóng)
    recent    = volumes[-10:] if n >= 10 else volumes
    prev_vols = recent[:-1]
    prev_nonzero = prev_vols[prev_vols > 0]

    if len(prev_nonzero) < 2:
        return []

    vol_raw      = float(recent[-1])    # Vol tích lũy của nến hiện tại
    vol_avg_prev = float(np.mean(prev_nonzero))

    if vol_avg_prev <= 0 or vol_raw <= 0:
        return []

    # ── Normalize vol nến hiện tại theo số phút đã qua trong giờ ────────────
    now_vn = __import__('datetime').datetime.now(_VN_TZ)
    minutes_into_hour = now_vn.minute   # Đã qua bao nhiêu phút trong giờ hiện tại
    if minutes_into_hour < 5:
        minutes_into_hour = 60   # Gần đầu giờ → coi như nến đã đóng, không project
    projection_factor = 60.0 / minutes_into_hour
    vol_cur = vol_raw * projection_factor   # Vol đã project 60 phút

    spike_ratio = vol_cur / vol_avg_prev

    logger.info(
        f"{symbol}/1H: vol_raw={vol_raw:.0f} min_into_hr={minutes_into_hour} "
        f"proj_factor={projection_factor:.2f} vol_projected={vol_cur:.0f} "
        f"spike={spike_ratio:.2f}x"
    )

    # Ngưỡng: HIGH ≥ 2.5x, MEDIUM 1.8-2.5x
    if spike_ratio < 1.8:
        return []

    # Xác định hướng dòng tiền
    price_up_1h = closes[-1] >= closes[-2] if n >= 2 else True

    if spike_ratio >= 2.5:
        level     = 'HIGH'
        direction = 'MUA LON' if price_up_1h else 'BAN LON'
        msg = (
            f'Vol giờ này {spike_ratio:.1f}x TB cac gio truoc '
            f'(vol project 60p: {vol_cur:,.0f}) '
            f'-> To chuc dang {direction} manh '
            f'({"Xác nhận tín hiệu MUA" if price_up_1h else "CẢNH BÁO: áp lực bán lớn"})'
        )
    else:
        level     = 'MEDIUM'
        direction = 'mua' if price_up_1h else 'ban'
        msg = (
            f'Vol giờ này {spike_ratio:.1f}x TB cac gio truoc '
            f'-> Dòng tiền {direction} dang tang, theo doi them'
        )

    logger.info(f"{symbol}/1H: vol_spike {spike_ratio:.1f}x ({level})")
    return [{'type': 'VOL_SPIKE_1H', 'level': level, 'message': msg}]


# ── Exchange & Limit Down Detection ─────────────────────────────────────────
# Cache sàn giao dịch để tránh gọi API liên tục
_exchange_cache = {}

# Mapping sàn đã biết — tránh gọi API listing, không bao giờ fallback UPCOM sai
# Cập nhật: thêm đầy đủ mã trong BACKTEST_WATCHLIST + EXTENDED
_KNOWN_EXCHANGE = {
    # ── Ngân hàng (HOSE) ─────────────────────────────────────────────────────
    'VCB':'HOSE','BID':'HOSE','CTG':'HOSE','TCB':'HOSE','MBB':'HOSE',
    'VPB':'HOSE','ACB':'HOSE','STB':'HOSE','LPB':'HOSE','HDB':'HOSE',
    'TPB':'HOSE','MSB':'HOSE','SHB':'HOSE','OCB':'HOSE','EIB':'HOSE',
    # ── BĐS / VinGroup (HOSE) ─────────────────────────────────────────────────
    'VHM':'HOSE','VIC':'HOSE','VRE':'HOSE','NVL':'HOSE','PDR':'HOSE',
    'KDH':'HOSE','DXG':'HOSE','NLG':'HOSE','BCM':'HOSE','HDG':'HOSE',
    'DIG':'HOSE','SCR':'HOSE',
    # ── Công nghệ / Bán lẻ (HOSE) ────────────────────────────────────────────
    'FPT':'HOSE','MWG':'HOSE','FRT':'HOSE','CMG':'HOSE','DGW':'HOSE',
    # ── Thực phẩm / Tiêu dùng (HOSE) ─────────────────────────────────────────
    'VNM':'HOSE','MSN':'HOSE','SAB':'HOSE','PNJ':'HOSE','MCH':'HOSE',
    'KDC':'HOSE','QNS':'HNX','BAF':'HOSE',
    # ── Thép (HOSE) ───────────────────────────────────────────────────────────
    'HPG':'HOSE','HSG':'HOSE','NKG':'HOSE','POM':'HOSE',
    # ── Hóa chất / Phân bón (HOSE) ───────────────────────────────────────────
    'DGC':'HOSE','DCM':'HOSE','DPM':'HOSE',
    # ── Chứng khoán (HOSE) ───────────────────────────────────────────────────
    'HCM':'HOSE','SSI':'HOSE','VND':'HOSE','VCI':'HOSE','VIX':'HOSE',
    # ── Chứng khoán (HNX) ────────────────────────────────────────────────────
    'SHS':'HNX','MBS':'HNX','BSI':'HNX','CTS':'HNX',
    # ── Dầu khí / Hóa dầu ────────────────────────────────────────────────────
    'GAS':'HOSE','PLX':'HOSE','PVD':'HOSE','OIL':'UPCOM',
    'PVS':'HNX','BSR':'UPCOM','PVT':'UPCOM','PVC':'UPCOM',
    # ── Điện / Năng lượng ────────────────────────────────────────────────────
    'POW':'HOSE','REE':'HOSE','PC1':'HOSE','NT2':'HOSE',
    'GEX':'HOSE','EVF':'HNX','PPC':'HNX','VSH':'UPCOM',
    # ── KCN / Logistics ──────────────────────────────────────────────────────
    'KBC':'HOSE','SZC':'HOSE','IDC':'HNX',
    'GMD':'HOSE','HAH':'UPCOM','DVP':'UPCOM','VSC':'UPCOM',
    # ── Xây dựng ─────────────────────────────────────────────────────────────
    'CTD':'HOSE','HBC':'HOSE','FCN':'HOSE','CII':'HOSE',
    # ── Hàng không / Vận tải ─────────────────────────────────────────────────
    'VJC':'HOSE','HVN':'HOSE','ACV':'UPCOM','SCS':'UPCOM',
    # ── Thép nhỏ / Vật liệu ──────────────────────────────────────────────────
    'TLH':'UPCOM','SMC':'HOSE','VGC':'HOSE','BMP':'HOSE','NTP':'HNX',
}

def get_exchange(symbol):
    """
    Detect sàn giao dịch. Ưu tiên: known mapping → cache → API → fallback UPCOM.
    _KNOWN_EXCHANGE tránh gọi API và tránh fallback UPCOM sai cho mã đã biết.
    """
    sym = symbol.upper() if symbol else ''

    # 1. Known mapping — không cần API call, không bao giờ sai
    if sym in _KNOWN_EXCHANGE:
        result = _KNOWN_EXCHANGE[sym]
        _exchange_cache[sym] = (result, time.time())
        return result

    # 2. Cache 24h
    if sym in _exchange_cache:
        exchange, ts = _exchange_cache[sym]
        if time.time() - ts < 86400:
            return exchange

    # 3. vnstock API cho mã chưa có trong mapping
    try:
        from vnstock import Vnstock
        listing = Vnstock().stock(symbol=sym, source='VCI').listing.symbols_by_exchange()
        if listing is not None and not listing.empty:
            exc_col = next((c for c in listing.columns
                            if c.lower() in ('exchange', 'floor', 'san')), None)
            if exc_col:
                row = (listing[listing['ticker'] == sym]
                       if 'ticker' in listing.columns
                       else listing[listing.index == sym])
                if not row.empty:
                    exc = str(row.iloc[0][exc_col]).upper()
                    result = ('HNX' if 'HNX' in exc and 'UPCOM' not in exc
                              else 'UPCOM' if ('UPCOM' in exc or 'UPC' in exc)
                              else 'HOSE')
                    _exchange_cache[sym] = (result, time.time())
                    logger.info(f"{sym}: exchange detected = {result}")
                    return result
    except Exception as e:
        logger.warning(f"get_exchange {sym}: {e}")

    # 4. Fallback UPCOM — chỉ xảy ra với mã không có trong known mapping
    _exchange_cache[sym] = ('UPCOM', time.time())
    logger.warning(f"{sym}: exchange fallback = UPCOM (not in known mapping)")
    return 'UPCOM'

EXCHANGE_BANDS = {
    'HOSE':  0.07,   # ±7%
    'HNX':   0.10,   # ±10%
    'UPCOM': 0.15,   # ±15%
}

# Tick size thực tế HoSE/HNX (theo quy định biên độ giá)
_TICK_SIZE_MAP = [
    (50_000, 100),   # >= 50,000đ: bước 100đ
    (10_000,  50),   # 10,000–50,000đ: bước 50đ
    (     0,  10),   # < 10,000đ: bước 10đ
]

def _floor_to_tick(price_raw):
    """Làm tròn giá xuống theo tick size HoSE — khớp cách HoSE tính giá sàn/trần."""
    tick = next(t for lo, t in _TICK_SIZE_MAP if price_raw >= lo)
    return (price_raw // tick) * tick


def detect_limit_down(closes, lows, symbol=''):
    """
    Phát hiện giá sàn (Limit Down) - đặc thù TTCK Việt Nam.

    Điều kiện limit_down = TRUE khi TẤT CẢ 3 điều sau đúng:
      1. Giá đóng cửa <= giá sàn thực (prev_close×(1-band) làm tròn theo tick)
      2. Giá thấp nhất ngày cũng chạm vùng sàn
      3. Giá giảm so với phiên trước

    v2: Dùng tick size thực tế HoSE thay vì tolerance 0.5%
    → Loại bỏ false positive khi giá cách sàn 0.1–0.4%
    → UPCOM vẫn dùng tol nhỏ 0.1% vì tick size khác
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

    raw_floor  = prev_close * (1 - band)
    if exchange == 'UPCOM':
        # UPCOM tick size phức tạp hơn → dùng tolerance nhỏ 0.1%
        floor_price = raw_floor * (1 + 0.001)
    else:
        # HoSE/HNX: làm tròn xuống theo tick size chính xác + không tolerance
        floor_price = _floor_to_tick(raw_floor)

    is_limit_down = (
        curr_close <= floor_price and    # Giá đóng cửa chạm sàn thực
        curr_low   <= floor_price and    # Giá thấp nhất cũng chạm sàn
        curr_close <  prev_close         # Giá có giảm
    )

    logger.info(
        f"limit_down check [{exchange} ±{band*100:.0f}%]: "
        f"prev={prev_close:,.0f} floor={floor_price:,.0f} "
        f"close={curr_close:,.0f} low={curr_low:,.0f} → {is_limit_down}"
    )
    return is_limit_down, band, exchange


def detect_limit_up(closes, highs, symbol=''):
    """
    Phát hiện giá trần (Limit Up) — đặc thù TTCK Việt Nam.

    Điều kiện limit_up = TRUE khi TẤT CẢ 3 điều sau đúng:
      1. Giá đóng cửa >= giá tham chiếu * (1 + biên_độ - tolerance)
      2. Giá cao nhất ngày cũng chạm vùng trần (xác nhận dư mua trần)
      3. Giá tăng so với phiên trước

    Tại sao quan trọng: Khi cổ phiếu kịch trần với vol đột biến, hệ thống
    có thể nhận nhầm là 'shark_buy' và đưa ra tín hiệu MUA. Thực tế đây
    thường là bẫy đội lái — kéo trần để tạo FOMO, sau đó xả hàng.
    → Override vol_signal = 'limit_up_trap' để cảnh báo thay vì khuyến mua.
    """
    if len(closes) < 2 or len(highs) < 1:
        return False, 0.0, 'HOSE'

    exchange = get_exchange(symbol) if symbol else 'HOSE'
    band = EXCHANGE_BANDS.get(exchange, 0.07)

    prev_close = float(closes[-2])
    curr_close = float(closes[-1])
    curr_high  = float(highs[-1])

    if prev_close <= 0:
        return False, band, exchange

    ceil_price = prev_close * (1 + band)
    tol = 0.015 if exchange == 'UPCOM' else 0.005
    threshold  = ceil_price * (1 - tol)

    is_limit_up = (
        curr_close >= threshold and     # Giá đóng cửa chạm trần
        curr_high  >= threshold and     # Giá cao nhất cũng chạm trần
        curr_close >  prev_close        # Giá có tăng
    )

    logger.info(
        f"limit_up check [{exchange} ±{band*100:.0f}%]: "
        f"prev={prev_close:,.0f} ceil={ceil_price:,.0f} "
        f"close={curr_close:,.0f} high={curr_high:,.0f} → {is_limit_up}"
    )
    return is_limit_up, band, exchange


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
                    'Phân kỳ giam: Giá dinh moi (' + f'{h[t2]:,.0f}'
                    + ') cao hon dinh cu (' + f'{h[t1]:,.0f}'
                    + ') nhung RSI thap hon (' + f'{r[t2]:.0f}'
                    + '<' + f'{r[t1]:.0f}' + ')'
                )

        # Bullish divergence: đáy giá thấp hơn, RSI tại đáy cao hơn
        if len(bottoms) >= 2:
            b1, b2 = bottoms[-2], bottoms[-1]
            if l[b2] < l[b1] and r[b2] > r[b1] + 2:
                return 'bullish', (
                    'Phân kỳ tang: Giá day moi (' + f'{l[b2]:,.0f}'
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
        weekly_trend_vn = 'Tăng mạnh tuần (Giá>MA10W>MA20W)'
    elif price > ma100:
        weekly_trend    = 'UP'
        weekly_trend_vn = 'Tang tuan (Giá>MA10W)'
    elif price > ma200:
        weekly_trend    = 'WEAK_UP'
        weekly_trend_vn = 'Yeu tuan (Giá&lt;MA10W nhung &gt;MA20W)'
    elif ma100 > ma200:
        weekly_trend    = 'PULLBACK'
        weekly_trend_vn = 'Pullback tuần (Giá&lt;MA10W, MA10W van tren MA20W)'
    else:
        weekly_trend    = 'DOWN'
        weekly_trend_vn = 'Downtrend tuần (Giá&lt;MA10W&lt;MA20W)'

    logger.info(f"weekly_trend={weekly_trend} ma100={ma100:.0f} ma200={ma200:.0f}")

    # Vol signal classification — ngưỡng chuẩn hóa lại
    # Trước: shark_buy = vol >= 1.5x (quá thấp, xảy ra 25-30% phiên)
    # Sau:   shark_buy = vol >= 2.5x (đột biến thực sự, ~8-10% phiên)
    #
    # Fake rally 2 loại:
    #   (A) Vol thấp + giá tăng = tăng không có xác nhận
    #   (B) Vol cao + upper wick lớn = kéo giá rồi xả trong phiên (làm giá VN)
    #       Điều kiện: close < open + 30% biên độ tăng (open→high)

    # Phát hiện upper wick bất thường (cần open price)
    oc = find_col(df, ['open', 'openprice', 'open_price'])
    _has_wick_data = oc is not None and hc is not None
    if _has_wick_data:
        opens = to_float_arr(df[oc])
        if opens.max() < 1000:
            opens *= 1000
        # Tính sau trim (dùng len(closes) hiện tại)
        _open_cur  = float(opens[len(closes)-1]) if len(closes) <= len(opens) else float(opens[-1])
        _high_cur  = float(highs[-1])
        _close_cur = float(closes[-1])
        _body_up   = _high_cur - _open_cur          # Biên độ tăng từ mở cửa → đỉnh
        # Upper wick pump: giá tăng mạnh trong ngày nhưng đóng cửa gần mở cửa
        # → close < open + 30% biên độ = xả hàng trước đóng cửa
        _is_upper_wick_pump = (
            price_up and
            _body_up > 0 and
            (_close_cur - _open_cur) < _body_up * 0.30 and
            vol_ratio >= 1.5   # Cần vol đủ lớn mới xác nhận
        )
    else:
        _is_upper_wick_pump = False

    if vol_ratio >= 2.5 and price_up and not _is_upper_wick_pump:
        vol_signal = 'shark_buy'
        vol_msg = 'Vol DOT BIEN ' + f'{vol_ratio:.1f}' + 'x TB + gia tang -> Ca map vao! Tín hiệu mạnh'
    elif vol_ratio >= 2.5 and price_up and _is_upper_wick_pump:
        # Vol đột biến + kéo giá trong ngày rồi xả = bẫy tăng điển hình TTCK VN
        vol_signal = 'fake_rally'
        vol_msg = ('Vol đột biến ' + f'{vol_ratio:.1f}' + 'x TB nhung gia dong gan gia mo '
                   '-> Kéo giá roi xa hang! KHONG mua')
    elif vol_ratio >= 2.5 and not price_up:
        vol_signal = 'shark_sell'
        vol_msg = 'Vol DOT BIEN ' + f'{vol_ratio:.1f}' + 'x TB + gia giam -> Ca map xa! Tín hiệu xau'
    elif vol_ratio < 1.0 and price_up:
        # Giá tăng nhưng volume thấp hơn TB = tăng không có xác nhận, nguy hiểm
        vol_signal = 'fake_rally'
        vol_msg = 'Giá tăng nhưng Vol thấp ' + f'{vol_ratio:.1f}' + 'x TB -> Kéo giá, không co dong tien'
    elif _is_upper_wick_pump:
        # Vol vừa phải nhưng vẫn có dấu hiệu xả hàng trong phiên
        vol_signal = 'fake_rally'
        vol_msg = ('Vol ' + f'{vol_ratio:.1f}' + 'x TB + upper wick bat thuong '
                   '-> Co the dang xa hang, than trong')
    elif vol_ratio >= 1.5 and price_up:
        vol_signal = 'normal_buy'
        vol_msg = 'Vol ' + f'{vol_ratio:.1f}' + 'x TB + gia tang -> Dòng tiền vao xac nhan'
    elif vol_ratio >= 1.0 and price_up:
        # vol bằng TB hoặc cao hơn nhẹ + giá tăng = tín hiệu yếu
        vol_signal = 'weak_buy'
        vol_msg = 'Vol ' + f'{vol_ratio:.1f}' + 'x TB + gia tang -> Xác nhận yếu'
    elif vol_ratio < 1.0 and not price_up:
        vol_signal = 'weak_sell'
        vol_msg = 'Vol thap ' + f'{vol_ratio:.1f}' + 'x TB + gia giam -> Ap luc ban yeu'
    elif vol_ratio >= 1.5 and not price_up:
        vol_signal = 'normal_sell'
        vol_msg = 'Vol ' + f'{vol_ratio:.1f}' + 'x TB + gia giam -> Ap luc ban ro rang'
    elif vol_ratio >= 1.0 and not price_up:
        vol_signal = 'normal_sell'
        vol_msg = 'Vol binh thuong ' + f'{vol_ratio:.1f}' + 'x TB + gia giam -> Ap luc ban'
    else:
        vol_signal = 'normal'
        vol_msg = 'Vol binh thuong ' + f'{vol_ratio:.1f}' + 'x TB'

    # ── Limit Down + Limit Up Detection (đặc thù TTCK Việt Nam) ────────────
    # Phải chạy SAU khi đã xác định closes/lows/highs cuối (sau trim nến chưa đóng)
    is_limit_down, exch_band, exchange = detect_limit_down(closes, lows, symbol)
    is_limit_up,   _,          _       = detect_limit_up(closes, highs, symbol)

    if is_limit_down and vol_signal == 'weak_sell':
        # Ghi đè: volume thấp KHÔNG phải kiệt cung mà là mất thanh khoản
        vol_signal = 'shark_sell'
        vol_msg = (
            'CẢNH BÁO GIA SAN [' + exchange + ' +-' + f'{exch_band*100:.0f}' + '%]: '
            'Trang ben mua! Vol thap = mat thanh khoan, KHONG phai kiet cung. '
            'Tuyet doi không mua!'
        )
        logger.warning(f"[{symbol}] Limit Down detected -> override weak_sell to shark_sell")

    if is_limit_up and vol_signal in ('shark_buy', 'normal_buy'):
        # Ghi đè: vol cao + kịch trần = NGUY CƠ BẪY ĐỘI LÁI
        # Đội lái kéo trần tạo FOMO → nhà đầu tư mua đuổi → họ xả hàng ngày hôm sau
        # Không override thành shark_sell (chưa chắc xả) nhưng chặn tín hiệu MUA
        vol_signal = 'limit_up_trap'
        vol_msg = (
            'CẢNH BÁO KICH TRAN [' + exchange + ' +-' + f'{exch_band*100:.0f}' + '%]: '
            'Vol đột biến + gia tran = có thể la bay doi lai. '
            'KHONG mua duoi gia, cho xac nhan ngay hom sau!'
        )
        logger.warning(f"[{symbol}] Limit Up + high vol detected -> override to limit_up_trap")

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
    score_log = [('Baseline', 50)]  # Track score breakdown

    def _log_score(label, delta, new_score):
        if delta != 0:
            score_log.append((label, delta, new_score))


    _snap_Vol_Shark = score
    # VOL scoring — cân xứng hóa sau khi chuẩn hóa ngưỡng
    # shark_buy/sell: ±20 (giữ — đây là tín hiệu mạnh nhất)
    # normal_buy: +10 (tăng từ +8, cân xứng với normal_sell -8)
    # normal_sell: -8 (tăng từ -5, cân xứng với normal_buy)
    # fake_rally: -15 (tăng từ -12 — tăng giá không vol rất nguy hiểm)
    # weak_buy/sell: 0 (loại bỏ noise +3đ)
    if vol_signal == 'shark_buy':
        score += 20
        score_log.append(('Vol', +20, score))
        signals.append(('VOL', 'bull', vol_msg))
    elif vol_signal == 'shark_sell':
        score -= 20
        score_log.append(('Vol', -20, score))
        signals.append(('VOL', 'bear', vol_msg))
    elif vol_signal == 'fake_rally':
        score -= 15
        score_log.append(('Vol', -15, score))
        signals.append(('VOL', 'bear', vol_msg))
    elif vol_signal == 'limit_up_trap':
        # Không trừ điểm mạnh (giá đang tăng thật) nhưng cảnh báo và chặn MUA
        score -= 5
        score_log.append(('Vol', -5, score))
        signals.append(('VOL', 'bear', vol_msg))
    elif vol_signal == 'normal_buy':
        score += 10
        score_log.append(('Vol', +10, score))
        signals.append(('VOL', 'bull', vol_msg))
    elif vol_signal == 'weak_buy':
        # Vol bằng TB + giá tăng = không đủ xác nhận, neutral
        signals.append(('VOL', 'neutral', vol_msg))
    elif vol_signal == 'weak_sell':
        # Vol thấp + giá giảm = áp lực yếu, neutral
        signals.append(('VOL', 'neutral', vol_msg))
    elif vol_signal == 'normal_sell':
        score -= 8
        score_log.append(('Vol', -8, score))
        signals.append(('VOL', 'bear', vol_msg))
    else:
        signals.append(('VOL', 'neutral', vol_msg))


    _snap_RSI = score
    # RSI: tối đa ±20 (giữ nguyên, đủ mạnh)
    if rsi_val < 30:
        score += 20
        score_log.append(('RSI', +20, score))
        signals.append(('RSI', 'bull', 'RSI=' + str(rsi_val) + ' Vùng quá bán -> Tim co hoi'))
    elif rsi_val < 40:
        score += 10
        score_log.append(('RSI', +10, score))
        signals.append(('RSI', 'bull', 'RSI=' + str(rsi_val) + ' Vùng yếu, đang hồi phục'))
    elif rsi_val > 70:
        score -= 20
        score_log.append(('RSI', -20, score))
        signals.append(('RSI', 'bear', 'RSI=' + str(rsi_val) + ' Vùng quá mua -> KHONG mua'))
    elif rsi_val > 60:
        # Giảm từ -10 → -5: RSI 60-70 trong uptrend là bình thường, không nên phạt nặng
        score -= 5
        score_log.append(('RSI', -5, score))
        signals.append(('RSI', 'bear', 'RSI=' + str(rsi_val) + ' Vùng mạnh, than trong'))
    else:
        signals.append(('RSI', 'neutral', 'RSI=' + str(rsi_val) + ' Vùng trung tính'))

    # Divergence: tối đa ±15 (giảm nhẹ từ ±15)
    if div_type == 'bullish':
        if rsi_val < 35:
            score += 15
            score_log.append(('RSI', +15, score))
            signals.append(('DIV', 'bull', div_msg + '[RSI qua ban xac nhan!]'))
        else:
            score += 10
            score_log.append(('RSI', +10, score))
            signals.append(('DIV', 'bull', div_msg))
    elif div_type == 'bearish':
        if rsi_val > 65:
            score -= 15
            score_log.append(('RSI', -15, score))
            signals.append(('DIV', 'bear', div_msg + '[RSI qua mua xac nhan!]'))
        else:
            score -= 10
            score_log.append(('RSI', -10, score))
            signals.append(('DIV', 'bear', div_msg))
    else:
        signals.append(('DIV', 'neutral', 'Không phát hiện phân kỳ RSI'))


    _snap_MA = score
    # MA: tối đa ±20 (giữ nguyên)
    if golden_cross:
        score += 20
        score_log.append(('MA', +20, score))
        signals.append(('MA', 'bull', 'GOLDEN CROSS! MA20 cắt lên MA50 -> Tăng mạnh dai han'))
    elif death_cross:
        score -= 20
        score_log.append(('MA', -20, score))
        signals.append(('MA', 'bear', 'DEATH CROSS! MA20 cat xuong MA50 -> Giám dai han!'))
    elif price > ma20 and ma20 > ma50:
        score += 15
        score_log.append(('MA', +15, score))
        signals.append(('MA', 'bull', 'Giá&gt;MA20(' + f'{ma20:,.0f}' + ')&gt;MA50(' + f'{ma50:,.0f}' + ')'))
    elif price > ma20:
        score += 10
        score_log.append(('MA', +10, score))
        signals.append(('MA', 'bull', 'Giá trên MA20 ' + f'{ma20:,.0f}' + ' -> Xu hướng ngắn hạn tăng'))
    elif price < ma20 and ma20 < ma50:
        score -= 15
        score_log.append(('MA', -15, score))
        signals.append(('MA', 'bear', 'Giá&lt;MA20&lt;MA50 - Giám 2 tang - KHONG mua duoi'))
    else:
        score -= 10
        score_log.append(('MA', -10, score))
        signals.append(('MA', 'bear', 'Giá dưới MA20 ' + f'{ma20:,.0f}' + ' - KHONG mua duoi!'))


    _snap_MACD = score
    # MACD: tối đa ±8 (tăng từ ±5, thêm momentum histogram)
    if macd_val > macd_sig and macd_h > 0:
        score += 5
        score_log.append(('MACD', +5, score))
        signals.append(('MACD', 'bull', 'MACD cắt lên Signal -> Động lực tăng'))
        # Histogram đang mở rộng (momentum tăng tốc)
        if len(macd_hist) >= 4 and float(macd_hist[-1]) > float(macd_hist[-3]):
            score += 3
            score_log.append(('MACD', +3, score))
            signals.append(('MACD', 'bull', 'MACD hist mở rộng -> Động lực đang tăng tốc'))
    elif macd_val < macd_sig and macd_h < 0:
        score -= 5
        score_log.append(('MACD', -5, score))
        signals.append(('MACD', 'bear', 'MACD cắt xuống Signal -> Động lực giảm'))
        if len(macd_hist) >= 4 and float(macd_hist[-1]) < float(macd_hist[-3]):
            score -= 3
            score_log.append(('MACD', -3, score))
            signals.append(('MACD', 'bear', 'MACD hist mở rộng xuong -> Động lực giảm toc'))
    else:
        signals.append(('MACD', 'neutral', 'MACD=' + f'{macd_val:+.0f}'))

    # S/R: tối đa ±12 (giữ nguyên)
    if supports:
        dist_s = (price - supports[0]['price']) / price * 100
        strength_s = supports[0]['count']
        if dist_s < 1.5:
            pts = min(12, 6 + strength_s * 2)
            score += pts
            signals.append(('SR', 'bull', 'Giá gần HT mạnh ' + f'{supports[0]["price"]:,.0f}'))
        elif dist_s < 4:
            score += 5
            score_log.append(('Hỗ trợ/KC', +5, score))
            signals.append(('SR', 'bull', 'HT gan: ' + f'{supports[0]["price"]:,.0f}' + ' (can chu y)'))
        else:
            signals.append(('SR', 'neutral', 'HT gan nhat: ' + f'{supports[0]["price"]:,.0f}'))

    if resistances:
        dist_r = (resistances[0]['price'] - price) / price * 100
        strength_r = resistances[0]['count']
        if dist_r < 1.5:
            pts = min(12, 6 + strength_r * 2)
            score -= pts
            signals.append(('SR', 'bear', 'Giá gan KC manh ' + f'{resistances[0]["price"]:,.0f}'))
        elif dist_r < 4:
            score -= 5
            score_log.append(('Hỗ trợ/KC', -5, score))
            signals.append(('SR', 'bear', 'KC gan: ' + f'{resistances[0]["price"]:,.0f}'))
        else:
            signals.append(('SR', 'neutral', 'KC gan nhat: ' + f'{resistances[0]["price"]:,.0f}'))

    # Ichimoku cloud: KHÔNG tính điểm (MA Weekly đã cover — MA100/200 tương đương cloud)
    # Giữ lại làm display trong signals để user tham khảo
    if price > cloud_top:
        signals.append(('ICHI', 'bull', 'Giá tren may Ichimoku (tham khao)'))
    elif price < cloud_bottom:
        signals.append(('ICHI', 'bear', 'Giá duoi may Ichimoku (tham khao)'))
    else:
        signals.append(('ICHI', 'neutral', 'Giá trong may Ichimoku (tham khao)'))

    # Tenkan/Kijun cross: KHÔNG tính điểm (MACD cross đã cover momentum ngắn hạn)
    # Giữ lại tính toán để display trong signals
    if n >= 27:
        tenkan_prev = (np.max(highs[-10:-1]) + np.min(lows[-10:-1])) / 2
        kijun_prev  = (np.max(highs[-27:-1]) + np.min(lows[-27:-1])) / 2
        tk_val      = float(tenkan)
        kj_val      = float(kijun)
        min_cross_gap = kj_val * 0.003
        tk_cross_bull = (float(tenkan_prev) < float(kijun_prev)
                         and tk_val > kj_val
                         and (tk_val - kj_val) >= min_cross_gap)
        tk_cross_bear = (float(tenkan_prev) > float(kijun_prev)
                         and tk_val < kj_val
                         and (kj_val - tk_val) >= min_cross_gap)
        if tk_cross_bull:
            signals.append(('ICHI', 'bull',
                'TK cat len KJ (' + f'{tk_val:,.0f}' + '>' + f'{kj_val:,.0f}' + ') (tham khao)'))
        elif tk_cross_bear:
            signals.append(('ICHI', 'bear',
                'TK cat xuong KJ (' + f'{tk_val:,.0f}' + '<' + f'{kj_val:,.0f}' + ') (tham khao)'))
        elif tk_val > kj_val:
            signals.append(('ICHI', 'bull',
                'TK>KJ (' + f'{tk_val:,.0f}' + '>' + f'{kj_val:,.0f}' + ') (tham khao)'))
        else:
            signals.append(('ICHI', 'bear',
                'TK<KJ (' + f'{tk_val:,.0f}' + '<' + f'{kj_val:,.0f}' + ') (tham khao)'))

    # Bollinger Bands: KHÔNG tính điểm — S/R pivot đã cover, ±3 quá nhỏ để thay đổi quyết định
    if price <= bb_lower:
        signals.append(('BB', 'bull', 'Giá cham BB duoi ' + f'{bb_lower:,.0f}' + ' (tham khao)'))
    elif price >= bb_upper:
        signals.append(('BB', 'bear', 'Giá cham BB tren ' + f'{bb_upper:,.0f}' + ' (tham khao)'))
    else:
        signals.append(('BB', 'neutral', 'BB: ' + f'{bb_lower:,.0f}' + '-' + f'{bb_upper:,.0f}' + ' | ' + f'{bb_pct:.0f}' + '% (tham khao)'))

    # Three-in-one: KHÔNG cộng điểm thêm (đã double-count từ MA+VOL+RSI)
    # Chỉ giữ lại làm tín hiệu hiển thị để user nhận ra hội tụ
    three_in_one = (price > ma20 and vol_ratio >= 1.5 and price_up and 30 < rsi_val < 70)
    if three_in_one:
        signals.append(('3IN1', 'bull', 'Hoi tu 3-trong-1: Giá&gt;MA20 + Vol đột biến + RSI trung tinh'))

    # ── MA10 / MA50 Horizon Signals (tham khảo — không ảnh hưởng score) ──────
    # Ngắn hạn: MA10 cross → momentum mới phục hồi, TP 7-10%
    # Trung hạn: giá trên MA50 bền vững → uptrend trung hạn, TP 25-30%
    # Thiết kế như vol spike: chỉ hiển thị khi có tín hiệu đáng chú ý
    if ma10_cross_up and ma10_slope_up:
        signals.append(('MA10', 'bull',
            f'GIA CAT LEN MA10 ({ma10:,.0f}) hom nay + MA10 doc len '
            f'-> Tín hiệu NGAN HAN, TP tham khao 7-10%'))
    elif ma10_cross_down:
        signals.append(('MA10', 'bear',
            f'GIA CAT XUONG MA10 ({ma10:,.0f}) hom nay '
            f'-> Mất động lực ngắn hạn, theo dõi'))
    elif above_ma10 and ma10_slope_up:
        signals.append(('MA10', 'bull',
            f'Giá trên MA10 ({ma10:,.0f}) + MA10 doc len '
            f'-> Đang trong đà tăng ngan han'))
    elif not above_ma10:
        signals.append(('MA10', 'bear',
            f'Giá dưới MA10 ({ma10:,.0f}) -> Chưa có động lực ngắn hạn'))

    if above_ma50 and ma50_slope_up:
        ma50_dist = (price - ma50) / ma50 * 100
        signals.append(('MA50', 'bull',
            f'Giá trên MA50 ({ma50:,.0f}) +{ma50_dist:.1f}% + MA50 doc len '
            f'-> Uptrend TRUNG HAN xac nhan, TP tham khao 25-30%'))
    elif above_ma50 and not ma50_slope_up:
        signals.append(('MA50', 'neutral',
            f'Giá trên MA50 ({ma50:,.0f}) nhung MA50 phang/giam '
            f'-> Trung hạn chưa rõ xu hướng'))
    else:
        signals.append(('MA50', 'bear',
            f'Giá dưới MA50 ({ma50:,.0f}) -> Chưa vào vùng trung hạn'))


    _snap_Weekly_Trend = score
    # ── Weekly Trend Scoring (khung tuần — tối đa ±10) ───────────────────────
    # Dùng MA100D (≈MA10W) và MA200D (≈MA20W) đã tính ở trên
    # Mục đích: lọc lệnh MUA trong downtrend tuần, xác nhận thêm cho uptrend tuần
    if weekly_trend == 'STRONG_UP':
        score += 10
        score_log.append(('Xu hướng tuần', +10, score))
        signals.append(('1W', 'bull', weekly_trend_vn + ' -> Xu hướng tuan xac nhan'))
    elif weekly_trend == 'UP':
        score += 5
        score_log.append(('Xu hướng tuần', +5, score))
        signals.append(('1W', 'bull', weekly_trend_vn))
    elif weekly_trend == 'WEAK_UP':
        score += 2
        score_log.append(('Xu hướng tuần', +2, score))
        signals.append(('1W', 'neutral', weekly_trend_vn + ' -> Can theo doi them'))
    elif weekly_trend == 'PULLBACK':
        # Giảm từ -5 → 0: pullback trong uptrend tuần thường là cơ hội mua tốt
        # Không nên trừ điểm, chỉ hiển thị để user chú ý
        signals.append(('1W', 'neutral', weekly_trend_vn + ' -> Pullback trong uptrend tuần (có thể la co hoi)'))
    else:  # DOWN
        score -= 10
        score_log.append(('Xu hướng tuần', -10, score))
        signals.append(('1W', 'bear', weekly_trend_vn + ' -> CẢNH BÁO: downtrend tuan'))

    score = max(0, min(100, score))

    # ── Hard Filter: MA50 Trend Gate (nâng cấp từ MA20 gate) ───────────────
    # MA50 (~2.5 tháng) phân tách uptrend/downtrend trung hạn tốt hơn MA20
    # MA20 quá nhạy → price dưới MA20 vẫn là pullback bình thường nếu > MA50
    #
    # Tầng 1: price < MA20 < MA50 (downtrend 2 tầng) → cap 55
    # Tầng 2: price < MA50 nhưng > MA20 (downtrend trung hạn) → cap 60
    # Tầng 3: MA20 < price < MA50 (pullback bình thường) → cap 72
    # Tầng 4: price > MA20 > MA50 (uptrend hoàn chỉnh) → không cap
    ma20_distance = (ma20 - price) / ma20 if ma20 > 0 else 0.0
    ma50_distance = (ma50 - price) / ma50 if ma50 > 0 else 0.0

    hard_filter_reason = ''
    if price < ma20 and ma20 < ma50:
        # Downtrend 2 tầng: price < MA20 < MA50
        # ── DCB Exception: Dead Cat Bounce ──────────────────────────────────
        dcb_condition = (
            ma20_distance >= 0.15 and
            rsi_val < 25 and
            vol_signal == 'weak_sell' and
            not is_limit_down
        )
        if dcb_condition:
            score = min(score, 60)
            hard_filter_reason = (
                'DCB EXCEPTION: Giá dưới MA20 '
                + f'{ma20_distance*100:.0f}%'
                + ' RSI=' + str(rsi_val)
                + ' Vol cạn kiệt -> Có thể có nhịp hồi kỹ thuật'
            )
            signals.append(('FILTER', 'neutral', hard_filter_reason))
            logger.info(f"[{symbol}] DCB exception applied, score capped at 60")
        else:
            score = min(score, 55)
            hard_filter_reason = 'HARD FILTER: Downtrend 2 tang (Giá<MA20<MA50) -> cap 55'
            signals.append(('FILTER', 'bear', hard_filter_reason))
            logger.info(f"[{symbol}] Hard filter (downtrend 2-tier), score capped at 55")

    elif price < ma50 and price > ma20:
        # Death cross: price trên MA20 nhưng dưới MA50 (downtrend trung hạn)
        score = min(score, 60)
        hard_filter_reason = 'FILTER: Giá dưới MA50 (downtrend trung han) -> cap 60'
        signals.append(('FILTER', 'neutral', hard_filter_reason))
        logger.info(f"[{symbol}] Filter (below MA50), score capped at 60")

    elif price < ma20 and price >= ma50:
        # Pullback bình thường: price giữa MA50 và MA20 — đây là vùng mua tốt
        score = min(score, 72)
        hard_filter_reason = 'FILTER: Pullback (MA50<Giá<MA20) -> cap 72, vùng mua tiem nang'
        signals.append(('FILTER', 'neutral', hard_filter_reason))
        logger.info(f"[{symbol}] Filter (pullback zone MA50-MA20), score capped at 72")

    elif price < ma20:
        # price < MA20, MA50 không xác định → cap 68 an toàn
        score = min(score, 68)
        hard_filter_reason = 'FILTER: Giá dưới MA20 -> cap 68'
        signals.append(('FILTER', 'neutral', hard_filter_reason))
        logger.info(f"[{symbol}] Filter (below MA20), score capped at 68")

    # Limit down override cuối: dù score bao nhiêu cũng không MUA
    if is_limit_down:
        score = min(score, 30)
        signals.append(('FILTER', 'bear',
            'HARD FILTER: GIA SAN [' + exchange + '] trang ben mua -> '
            'Score bi gioi han 30, KHONG MUA trong moi truong hop'))
        logger.warning(f"[{symbol}] Limit down hard cap applied, score={score}")

    # ── Weekly Downtrend Hard Cap ────────────────────────────────────────────
    # Fix: đồng nhất với downtrend 2 tầng — cap 55, không phải 58
    # Lý do: weekly DOWN + bất kỳ cấu trúc nào → không bao giờ ra lệnh MUA (< 65)
    if weekly_trend == 'DOWN' and not is_limit_down:
        score = min(score, 55)
        if not hard_filter_reason:
            hard_filter_reason = (
                'WEEKLY FILTER: Downtrend tuần (Giá<MA10W<MA20W) '
                '-> Score cap 55, KHONG ra lenh MUA'
            )
        signals.append(('FILTER', 'bear',
            'WEEKLY FILTER: ' + weekly_trend_vn + ' -> Score cap 55'))
        logger.info(f"[{symbol}] Weekly downtrend cap applied, score={score}")

    # ── Hard Block: Fake Rally — không bao giờ MUA khi giá tăng không có vol ──
    # Lý do: fake_rally ở TTCK VN thường là kéo giá cuối phiên để xả hàng
    # Hard block sạch hơn trừ điểm vì không bị override bởi indicator khác
    if vol_signal == 'fake_rally' and score >= 65:
        score = min(score, 64)   # Chặn đúng dưới ngưỡng MUA
        signals.append(('FILTER', 'bear',
            'HARD BLOCK: Fake Rally — Giá tăng KHÔNG có Volume ('
            + f'{vol_ratio:.1f}' + 'x < 1.0x TB) -> KHOA LENH MUA'))
        logger.info(f"[{symbol}] Fake rally hard block applied, score capped at 64")

    # ── Hard Block: Limit Up Trap — cảnh báo bẫy trần đội lái ─────────────
    if vol_signal == 'limit_up_trap' and score >= 65:
        score = min(score, 64)   # Chặn MUA — chờ xác nhận ngày hôm sau
        signals.append(('FILTER', 'bear',
            'HARD BLOCK: Kich tran + Vol đột biến -> Co the la bay doi lai. '
            'Doi xac nhan ngay T+1 truoc khi mua'))
        logger.info(f"[{symbol}] Limit up trap hard block applied, score capped at 64")

    if score >= 65:
        action = 'MUA'
    elif score <= 35:
        action = 'BAN'
    else:
        action = 'THEO DOI'


    # Score tracking: done via direct score_log.append() calls above

    # ── SL/TP đọc từ config.py theo mã — single source of truth ────────────
    _sl_pct, _tp_pct = get_sl_tp(symbol)
    _sl_label = f'-{int(_sl_pct*100)}%'
    _tp_label = f'+{int(_tp_pct*100)}%'

    # Khởi tạo entry zone mặc định (override trong block MUA bên dưới)
    entry_zone_low  = round(price, 0)
    entry_zone_high = round(price, 0)
    entry_label     = ''

    if action == 'MUA':
        stop_loss   = round(price * (1 - _sl_pct), 0)
        take_profit = round(price * (1 + _tp_pct), 0)
        sl_label    = _sl_label
        tp_label    = _tp_label
        rebuy_zone  = None

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
        tp_label = 'Vùng mua lại'
    else:
        stop_loss   = round(price * (1 - _sl_pct), 0)
        take_profit = round(price * (1 + _tp_pct / 2), 0)   # TP tham khảo = 1/2 TP thật
        rebuy_zone  = None
        sl_label    = _sl_label + ' neu đã mua'
        tp_label    = f'+{int(_tp_pct/2*100)}% tham khao'



    _snap_Sprint4_ADXOBVROC = score
    # ── Sprint 4: ADX, BB Squeeze, Vol Compression, OBV, ROC, MA50 Slope ────────
    try:
        # ADX (Average Directional Index) — Trend Strength
        _adx_score = 0
        _adx_val   = None
        _squeeze_flag = False
        _vol_compress = False
        _obv_div      = False
        _roc_val      = None
        _ma50_slope   = None

        if len(closes) >= 28:
            # ── ADX ────────────────────────────────────────────────────────────
            _hi  = highs[-28:]
            _lo  = lows[-28:]
            _cl  = closes[-28:]
            _tr  = np.maximum(np.maximum(_hi[1:] - _lo[1:],
                               np.abs(_hi[1:] - _cl[:-1])),
                               np.abs(_lo[1:] - _cl[:-1]))
            _dm_p = np.where((_hi[1:]-_hi[:-1]) > (_lo[:-1]-_lo[1:]),
                              np.maximum(_hi[1:]-_hi[:-1], 0), 0.0)
            _dm_n = np.where((_lo[:-1]-_lo[1:]) > (_hi[1:]-_hi[:-1]),
                              np.maximum(_lo[:-1]-_lo[1:], 0), 0.0)
            _p = 14
            _atr14 = np.convolve(_tr,    np.ones(_p)/_p, mode='valid')[-1]
            _dip14 = np.convolve(_dm_p,  np.ones(_p)/_p, mode='valid')[-1]
            _din14 = np.convolve(_dm_n,  np.ones(_p)/_p, mode='valid')[-1]
            _di_p  = (_dip14 / _atr14 * 100) if _atr14 > 0 else 0
            _di_n  = (_din14 / _atr14 * 100) if _atr14 > 0 else 0
            _dx    = abs(_di_p - _di_n) / (_di_p + _di_n) * 100 if (_di_p + _di_n) > 0 else 0
            _adx_val = round(_dx, 1)
            # ADX > 25 = trending mạnh; > 35 = rất mạnh; < 20 = ranging
            if _adx_val >= 35:
                _adx_score = +5 if price > ma50 else -5   # strong trend bonus/penalty
            elif _adx_val >= 25:
                _adx_score = +3 if price > ma50 else -3
            # ADX < 20 không cộng/trừ — không có trend rõ
            score = max(0, min(100, score + _adx_score))
            if _adx_score != 0: score_log.append(('Sprint4 ADX', _adx_score, score))
            if   score >= MIN_SCORE_BUY:  action = 'MUA'
            elif score <= MAX_SCORE_SELL: action = 'BAN'

        # ATR(14) — Average True Range (dùng cho SL dynamic)
        _atr_val = None
        if len(closes) >= 15:
            _tr2 = np.maximum(np.maximum(highs[-15:][1:] - lows[-15:][1:],
                               np.abs(highs[-15:][1:] - closes[-16:-1])),
                               np.abs(lows[-15:][1:] - closes[-16:-1]))
            _atr_val = round(float(np.mean(_tr2)), 0)

        # ── BB Squeeze ─────────────────────────────────────────────────────────
        if len(closes) >= 40:
            _bb_widths = []
            for _k in range(20, len(closes)):
                _w = float(np.std(closes[_k-20:_k])) * 2 * 2
                _m = float(np.mean(closes[_k-20:_k]))
                _bb_widths.append(_w / _m * 100 if _m > 0 else 0)
            _bw_now  = _bb_widths[-1]
            _bw_pct20 = float(np.percentile(_bb_widths, 20))
            _squeeze_flag = _bw_now < _bw_pct20
            if _squeeze_flag:
                score = max(0, min(100, score + 4))   # squeeze = potential breakout
                score_log.append(('Sprint4 BB Squeeze', +4, score))
                if   score >= MIN_SCORE_BUY:  action = 'MUA'
                elif score <= MAX_SCORE_SELL: action = 'BAN'

        # ── Vol Compression ────────────────────────────────────────────────────
        if len(volumes) >= 15 and vol_ma20 > 0:
            _vol_slope_window = volumes[-10:]
            _vol_nonzero = _vol_slope_window[_vol_slope_window > 0]
            if len(_vol_nonzero) >= 5:
                _xs = np.arange(len(_vol_nonzero), dtype=float)
                _ys = _vol_nonzero / vol_ma20   # normalize
                _slope = np.polyfit(_xs, _ys, 1)[0]
                _vol_compress = bool(_slope < -0.02)   # declining vol = accumulation

        # ── OBV (On Balance Volume) ─────────────────────────────────────────────
        _obv_score = 0
        if len(closes) >= 20 and len(volumes) >= 20:
            _obv = np.zeros(20)
            for _k in range(1, 20):
                _sign = 1 if closes[-20+_k] > closes[-20+_k-1] else (-1 if closes[-20+_k] < closes[-20+_k-1] else 0)
                _obv[_k] = _obv[_k-1] + _sign * volumes[-20+_k]
            # OBV trend: compare first half vs second half
            _obv_first = float(np.mean(_obv[:10]))
            _obv_last  = float(np.mean(_obv[10:]))
            _price_first = float(np.mean(closes[-20:-10]))
            _price_last  = float(np.mean(closes[-10:]))
            # Divergence: price down + OBV up = accumulation (bullish div)
            _obv_div = bool(_price_last < _price_first and _obv_last > _obv_first)
            if _obv_div:
                _obv_score = +5
                score = max(0, min(100, score + _obv_score))
                score_log.append(('Sprint4 OBV Tăng', _obv_score, score))
                if   score >= MIN_SCORE_BUY:  action = 'MUA'
                elif score <= MAX_SCORE_SELL: action = 'BAN'
            elif _price_last > _price_first and _obv_last < _obv_first:
                # Bearish divergence: price up + OBV down
                _obv_score = -4
                score = max(0, min(100, score + _obv_score))
                score_log.append(('Sprint4 OBV Giảm', _obv_score, score))
                if   score >= MIN_SCORE_BUY:  action = 'MUA'
                elif score <= MAX_SCORE_SELL: action = 'BAN'

        # ── ROC (Rate of Change) ────────────────────────────────────────────────
        _roc_score = 0
        if len(closes) >= 11:
            _roc_val = round((closes[-1] / closes[-11] - 1) * 100, 2)
            if _roc_val > 8:    _roc_score = +5
            elif _roc_val > 3:  _roc_score = +3
            elif _roc_val < -8: _roc_score = -4
            elif _roc_val < -3: _roc_score = -2
            if _roc_score != 0:
                score = max(0, min(100, score + _roc_score))
                if _roc_score != 0: score_log.append(('Sprint4 ROC', _roc_score, score))
                if   score >= MIN_SCORE_BUY:  action = 'MUA'
                elif score <= MAX_SCORE_SELL: action = 'BAN'

        # ── MA50 Slope ─────────────────────────────────────────────────────────
        _ma50s_score = 0
        if len(closes) >= 60:
            _ma50_now  = float(np.mean(closes[-50:]))
            _ma50_prev = float(np.mean(closes[-60:-10]))
            _ma50_slope = round((_ma50_now / _ma50_prev - 1) * 100, 2) if _ma50_prev > 0 else 0
            if _ma50_slope > 1.5:    _ma50s_score = +4   # MA50 đang tăng mạnh
            elif _ma50_slope > 0.5:  _ma50s_score = +2
            elif _ma50_slope < -1.5: _ma50s_score = -3   # MA50 đang giảm mạnh
            elif _ma50_slope < -0.5: _ma50s_score = -1
            if _ma50s_score != 0:
                score = max(0, min(100, score + _ma50s_score))
                if _ma50s_score != 0: score_log.append(('Sprint4 MA50', _ma50s_score, score))
                if   score >= MIN_SCORE_BUY:  action = 'MUA'
                elif score <= MAX_SCORE_SELL: action = 'BAN'

    except Exception as _e4:
        _adx_val = None
        _atr_val = None
        _squeeze_flag = False
        _vol_compress = False
        _obv_div = False
        _roc_val = None
        _ma50_slope = None

    # ── Sprint 4 END ──────────────────────────────────────────────────────────


    _snap_RS_Breakout = score
    # ── Relative Strength vs VNINDEX + 52-week Breakout ─────────────────────
    # Cache RS 1 giờ để không gọi VNINDEX API mỗi lần
    rs_data  = {}
    rs_bonus = 0
    try:
        _rs_cache_key = 'rs_' + symbol
        _rs_cached    = get_cached(_rs_cache_key)
        if _rs_cached:
            rs_data = _rs_cached
        else:
            from relative_strength import calc_rs_signals
            rs_data = calc_rs_signals(closes, highs, symbol)
            rs_data = {k: (bool(v) if hasattr(v, 'item') and isinstance(v.item(), bool)
                           else (v.item() if hasattr(v, 'item') else v))
                       for k, v in rs_data.items()}
            set_cached(_rs_cache_key, rs_data, ttl=3600)
        rs_bonus = rs_data.get('total_bonus', 0)
        score    = max(0, min(100, score + rs_bonus))
        if rs_bonus != 0: score_log.append(('RS Bonus', rs_bonus, score))
        if   score >= MIN_SCORE_BUY:    action = 'MUA'
        elif score <= MAX_SCORE_SELL:   action = 'BAN'
        else:                           action = 'THEO_DOI'
    except Exception:
        pass


    # ── Intra-Sector RS ─────────────────────────────────────────────────────
    sector_rs = {'available': False, 'group': '', 'label': '', 'bonus': 0,
                 'rank': None, 'total': 0, 'percentile': None,
                 'symbol_ret': None, 'peer_rets': {}}
    try:
        _sector_key = 'sector_rs_' + symbol
        _sr_cached  = get_cached(_sector_key)
        if _sr_cached:
            sector_rs = _sr_cached
        else:
            from intra_sector_rs import calc_intra_sector_rs
            _sr = calc_intra_sector_rs(symbol, closes, days=20)
            sector_rs = {}
            for _k, _v in _sr.items():
                if _k == 'peer_rets':
                    sector_rs[_k] = {_p: float(_r) for _p, _r in _v.items()}
                elif hasattr(_v, 'item'):
                    sector_rs[_k] = float(_v)
                else:
                    sector_rs[_k] = _v
            _sr_bonus = sector_rs.get('bonus', 0)
            if _sr_bonus != 0:
                score = max(0, min(100, score + _sr_bonus))
                if score >= MIN_SCORE_BUY:
                    action = 'MUA'
                elif score <= MAX_SCORE_SELL:
                    action = 'BAN'
                else:
                    action = 'THEO_DOI'
            set_cache(_sector_key, sector_rs, ttl=1800)
    except Exception:
        pass

    # ── Momentum Leader Signal (2-tier independent system) ───────────────────
    # Tier 1: Core Filter — Price > MA50 AND Vol > MA20 × 1.2
    # Tier 2: Scoring 0-120đ → ≥90 Strong / ≥75 Pass / <75 Ignore
    # Độc lập với Score A — không ảnh hưởng nhau
    momentum_signal = {
        'tier1_pass': False, 'score': 0, 'label': '',
        'grade': '',          # 'STRONG' | 'PASS' | ''
        'components': {},
        'penalties': [],
    }
    try:
        _ms_score = 0
        _ms_comps = {}
        _ms_pens  = []

        # ── Tier 1: Core Filter ─────────────────────────────────────────────
        _t1_price = price > ma50
        _t1_vol   = vol_ratio >= 1.2
        _ms_t1    = _t1_price and _t1_vol
        momentum_signal['tier1_pass'] = _ms_t1

        if _ms_t1:
            n = len(closes)

            # ── Leadership RS ──────────────────────────────────────────────
            # RS vs VNINDEX: dùng rs_20d (% return stock vs VNI 20 ngày)
            _rs_vni = rs_data.get('rs_20d') or 0
            if _rs_vni > 5:
                _ms_score += 20; _ms_comps['rs_vni'] = (20, f'RS vs VNI +{_rs_vni:.1f}% (manh)')
            elif _rs_vni > 0:
                _ms_score += 15; _ms_comps['rs_vni'] = (15, f'RS vs VNI +{_rs_vni:.1f}%')
            else:
                _ms_comps['rs_vni'] = (0, f'RS vs VNI {_rs_vni:.1f}% (yeu)')

            # RS vs Sector: dùng sector_rs symbol_ret vs avg peer
            _rs_sec_sym  = sector_rs.get('symbol_ret') or 0
            _rs_sec_peers = sector_rs.get('peer_rets', {})
            _rs_sec_avg  = float(np.mean(list(_rs_sec_peers.values()))) if _rs_sec_peers else 0
            _rs_sec_diff = _rs_sec_sym - _rs_sec_avg
            if _rs_sec_diff > 3:
                _ms_score += 20; _ms_comps['rs_sector'] = (20, f'RS vs Nganh +{_rs_sec_diff:.1f}% (manh)')
            elif _rs_sec_diff > 0:
                _ms_score += 15; _ms_comps['rs_sector'] = (15, f'RS vs Nganh +{_rs_sec_diff:.1f}%')
            else:
                _ms_comps['rs_sector'] = (0, f'RS vs Nganh {_rs_sec_diff:.1f}% (yeu)')

            # ── Momentum RSI ───────────────────────────────────────────────
            # Level: 45 ≤ RSI ≤ 60
            if 45 <= rsi_val <= 60:
                _ms_score += 10; _ms_comps['rsi_level'] = (10, f'RSI={rsi_val:.0f} (45-60)')
            else:
                _ms_comps['rsi_level'] = (0, f'RSI={rsi_val:.0f} (ngoai range)')

            # Speed 4d: RSI today ≥ RSI_4sessions + 3
            _rsi4 = float(rsi_series[-5]) if len(rsi_series) >= 5 else rsi_val
            if rsi_val >= _rsi4 + 3:
                _ms_score += 10; _ms_comps['rsi_4d'] = (10, f'RSI tang {rsi_val-_rsi4:.1f}d/4phien')
            else:
                _ms_comps['rsi_4d'] = (0, f'RSI 4d: {rsi_val-_rsi4:+.1f}d')

            # Speed 5d: RSI tăng ≥ 5đ trong 5 phiên (intentional overlap với 4d)
            _rsi5 = float(rsi_series[-6]) if len(rsi_series) >= 6 else rsi_val
            if rsi_val >= _rsi5 + 5:
                _ms_score += 10; _ms_comps['rsi_5d'] = (10, f'RSI tang {rsi_val-_rsi5:.1f}d/5phien')
            else:
                _ms_comps['rsi_5d'] = (0, f'RSI 5d: {rsi_val-_rsi5:+.1f}d')

            # ── Price Structure ────────────────────────────────────────────
            # Close ≥ 80% range 20 ngày
            _low20  = float(np.min(lows[-20:]))  if n >= 20 else float(np.min(lows))
            _high20 = float(np.max(highs[-20:])) if n >= 20 else float(np.max(highs))
            _range20 = _high20 - _low20
            if _range20 > 0 and price >= _low20 + 0.8 * _range20:
                _ms_score += 10; _ms_comps['price_range'] = (10, 'Close >= 80% range 20d')
            else:
                _pct_range = (price - _low20) / _range20 * 100 if _range20 > 0 else 0
                _ms_comps['price_range'] = (0, f'Close {_pct_range:.0f}% range 20d')

            # Close ≥ Highest 5 ngày
            _high5 = float(np.max(closes[-5:])) if n >= 5 else price
            if price >= _high5:
                _ms_score += 10; _ms_comps['breakout_5d'] = (10, 'Close >= Highest 5d (breakout)')
            else:
                _ms_comps['breakout_5d'] = (0, f'Close chua vuot Highest5d ({_high5:,.0f})')

            # ── Volume Expansion ───────────────────────────────────────────
            if vol_ratio >= 1.5:
                _ms_score += 10; _ms_comps['vol_expansion'] = (10, f'Vol {vol_ratio:.1f}x (>=1.5x)')
            else:
                _ms_comps['vol_expansion'] = (0, f'Vol {vol_ratio:.1f}x (<1.5x)')

            # ── 52W Proximity ──────────────────────────────────────────────
            _n52 = min(252, n)
            _high52 = float(np.max(highs[-_n52:])) if _n52 >= 20 else float(np.max(highs))
            _pct52  = price / _high52 if _high52 > 0 else 0
            if _pct52 >= 1.0:
                _ms_score += 20; _ms_comps['w52'] = (20, f'Pha vo dinh 52W ({_pct52:.1%})')
            elif _pct52 >= 0.95:
                _ms_score += 15; _ms_comps['w52'] = (15, f'Gan dinh 52W ({_pct52:.1%})')
            elif _pct52 >= 0.90:
                _ms_score += 10; _ms_comps['w52'] = (10, f'Trong vung dinh ({_pct52:.1%})')
            else:
                _ms_comps['w52'] = (0, f'Xa dinh 52W ({_pct52:.1%})')

            # ── Penalties ─────────────────────────────────────────────────
            # Distribution day: Close < Open×0.995 AND Vol > 1.5x
            _open_cur = float(opens[-1]) if opens is not None and len(opens) > 0 else price
            if price < _open_cur * 0.995 and vol_ratio >= 1.5:
                _ms_score -= 10
                _ms_pens.append('Distribution day (-10): Gia giam + Vol cao')

            # Weekly downtrend penalty
            if weekly_trend == 'DOWN':
                _ms_score -= 10
                _ms_pens.append('Weekly DOWN (-10): Downtrend trung han')

            # ── Grade ──────────────────────────────────────────────────────
            _ms_score = max(0, _ms_score)
            if _ms_score >= 90:
                _ms_grade = 'STRONG'
                _ms_label = f'Strong Momentum Leader ({_ms_score}/120)'
            elif _ms_score >= 75:
                _ms_grade = 'PASS'
                _ms_label = f'Momentum Signal ({_ms_score}/120)'
            else:
                _ms_grade = ''
                _ms_label = f'Chua du dieu kien ({_ms_score}/120)'

            momentum_signal = {
                'tier1_pass':  True,
                'score':       _ms_score,
                'label':       _ms_label,
                'grade':       _ms_grade,
                'components':  _ms_comps,
                'penalties':   _ms_pens,
                'rsi4':        round(_rsi4, 1),
                'high52w':     round(_high52, 0),
                'pct52w':      round(_pct52 * 100, 1),
            }
    except Exception as _mse:
        logger.debug(f'Momentum signal {symbol}: {_mse}')

    # ── Market Regime Filter ──────────────────────────────────────────────────
    # Chỉ áp dụng cho mã use_regime=True trong SYMBOL_CONFIG
    # Counter-cyclical (DGC, DCM, NKG...) được exempt
    regime_info   = {'regime': 'UNKNOWN', 'label': '', 'signal': 0,
                     'vni': 0, 'ma50': 0, 'ma200': 0}
    regime_note   = ''
    _sym_use_regime  = SYMBOL_CONFIG.get(symbol.upper(), {}).get('use_regime', True)
    _sym_trigger     = SYMBOL_CONFIG.get(symbol.upper(), {}).get('trigger_mode', 'score_primary')
    _sym_min_score   = SYMBOL_CONFIG.get(symbol.upper(), {}).get('min_score', MIN_SCORE_BUY)
    _tier_min_conv   = 2.0 if _sym_trigger == 'filter_confirm' else (2.5 if _sym_trigger == 'filter_led' else 0)
    try:
        from backtest import get_market_regime, apply_regime_to_score
        regime_info = get_market_regime()
        regime_str  = regime_info.get('regime', 'UNKNOWN')
        if _sym_use_regime and regime_str in ('BULL', 'NEUTRAL', 'BEAR'):
            score, regime_note = apply_regime_to_score(score, regime_str)
            # Cập nhật lại action sau khi áp dụng regime
            if   score >= _sym_min_score:  action = 'MUA'
            elif score <= MAX_SCORE_SELL: action = 'BAN'
            else:                         action = 'THEO_DOI'
    except Exception as _re:
        pass

    # ── VWAP Tuần + Tháng (anchored, cache 4 giờ) ─────────────────────────────
    vwap_info = {'vwap_w': 0, 'vwap_m': 0, 'bonus': 0,
                 'pct_w': 0.0, 'pct_m': 0.0, 'note': ''}
    _sym_use_vwap = SYMBOL_CONFIG.get(symbol.upper(), {}).get('use_vwap', True)
    try:
        _vwap_key    = 'vwap_' + symbol
        _vwap_cached = get_cached(_vwap_key)
        if _vwap_cached:
            vwap_info = _vwap_cached
        elif _sym_use_vwap:
            from backtest import compute_vwap_arrays, apply_vwap_bonus
            _tc = next((c for c in df.columns if c.lower() in
                        ('time','date','datetime','trading_date')), None)
            if _tc:
                _dates_vwap = pd.to_datetime(df[_tc], errors='coerce').reset_index(drop=True)
            elif isinstance(df.index, pd.DatetimeIndex):
                _dates_vwap = pd.Series(df.index)
            else:
                _dates_vwap = pd.Series([pd.NaT] * len(closes))
            _vwap_w_arr, _vwap_m_arr = compute_vwap_arrays(closes, volumes, _dates_vwap)
            _vwap_w = float(_vwap_w_arr[-1]) if len(_vwap_w_arr) else 0
            _vwap_m = float(_vwap_m_arr[-1]) if len(_vwap_m_arr) else 0
            _cur_price = float(closes[-1])
            _, _vbonus, _vnote = apply_vwap_bonus(score, _cur_price, _vwap_w, _vwap_m)
            _pct_w = round((_cur_price/_vwap_w - 1)*100, 2) if _vwap_w > 0 else 0
            _pct_m = round((_cur_price/_vwap_m - 1)*100, 2) if _vwap_m > 0 else 0
            vwap_info = {'vwap_w': round(_vwap_w), 'vwap_m': round(_vwap_m),
                         'bonus': _vbonus, 'pct_w': _pct_w, 'pct_m': _pct_m,
                         'note': _vnote}
            set_cache(_vwap_key, vwap_info, ttl=14400)  # cache 4 giờ
        # Apply VWAP bonus vào score
        _vb = vwap_info.get('bonus', 0)
        if _vb != 0:
            score = max(0, min(100, score + _vb))
            if _vb != 0: score_log.append(('VWAP Bonus', _vb, score))
            if   score >= _sym_min_score:  action = 'MUA'
            elif score <= MAX_SCORE_SELL: action = 'BAN'
            else:                         action = 'THEO_DOI'
    except Exception as _ve:
        pass

    # ── Foreign Flow Info — Display only, không cộng vào Score A ─────────────
    # Lý do không tích hợp vào Score A:
    #   [1] Coverage ~50%: nhiều mã không có data → score không công bằng
    #   [2] Block deals TTCK VN làm méo net_vol
    #   [3] Không backtest được với historical foreign data
    # → Dùng như conviction filter + display trong /analyze và /score
    foreign_info = {
        'available': False, 'mode': 'none',
        'consecutive_buy': 0, 'consecutive_sell': 0,
        'net_5d': 0, 'net_10d': 0, 'net_ratio': 0,
        'bonus': 0, 'label': 'Khong co du lieu',
    }
    try:
        _fkey    = 'foreign_' + symbol
        _fcached = get_cached(_fkey)
        if _fcached:
            foreign_info = _fcached
        else:
            # ── Attempt A: historical flow series ───────────────────────────
            from shark_detector import load_foreign_flow as _lff
            _fdf = _lff(symbol, days=30)
            if _fdf is not None and 'net_vol' in _fdf.columns and len(_fdf) >= 5:
                _fnet  = _fdf['net_vol'].values
                _cbuy  = _csell = 0
                for _v in reversed(_fnet):
                    if _v > 0:
                        if _csell > 0: break
                        _cbuy += 1
                    elif _v < 0:
                        if _cbuy > 0: break
                        _csell += 1
                    else:
                        break
                _net5   = float(sum(_fnet[-5:]))
                _net10  = float(sum(_fnet[-10:]))
                _adtv   = float(vol_ma20) if vol_ma20 > 0 else 1
                _nratio = _net10 / (_adtv * 10) * 100 if _adtv > 0 else 0
                if _cbuy > 0:
                    _flabel = f'Mua rong {_cbuy} phien lien tiep | Net10d={_net10/1e6:.1f}M cp'
                elif _csell > 0:
                    _flabel = f'Ban rong {_csell} phien lien tiep | Net10d={_net10/1e6:.1f}M cp'
                else:
                    _flabel = f'Trung tinh | Net10d={_net10/1e6:.1f}M cp'
                foreign_info = {
                    'available': True, 'mode': 'historical',
                    'consecutive_buy':  int(_cbuy),
                    'consecutive_sell': int(_csell),
                    'net_5d':    round(_net5  / 1e6, 2),
                    'net_10d':   round(_net10 / 1e6, 2),
                    'net_ratio': round(_nratio, 1),
                    'bonus': 0,
                    'label': _flabel,
                }
                set_cached(_fkey, foreign_info, ttl=900)
            else:
                # ── Attempt B: VCI GraphQL snapshot ─────────────────────────
                from shark_detector import load_foreign_snapshot as _lfs
                _snap = _lfs(symbol)
                if _snap and _snap.get('available'):
                    _c_ratio   = _snap['current_ratio']
                    _m_ratio   = _snap['max_ratio']
                    _room_used = _snap['room_used_pct']
                    if _room_used >= 95:
                        _flabel = f'Room gần hết ({_room_used:.1f}%) | Hold {_c_ratio:.1f}%/{_m_ratio:.1f}%'
                    elif _room_used >= 80:
                        _flabel = f'Room cao ({_room_used:.1f}%) | Hold {_c_ratio:.1f}%/{_m_ratio:.1f}%'
                    elif _room_used >= 50:
                        _flabel = f'Room trung bình ({_room_used:.1f}%) | Hold {_c_ratio:.1f}%/{_m_ratio:.1f}%'
                    else:
                        _flabel = f'Room còn nhiều ({_room_used:.1f}%) | Hold {_c_ratio:.1f}%/{_m_ratio:.1f}%'
                    foreign_info = {
                        'available': True, 'mode': 'snapshot',
                        'current_ratio':  _c_ratio,
                        'max_ratio':      _m_ratio,
                        'room_used_pct':  _room_used,
                        'room_left_pct':  _snap.get('room_left_pct', round(_m_ratio - _c_ratio, 1)),
                        'bonus': 0,
                        'label': _flabel,
                        'consecutive_buy': 0, 'consecutive_sell': 0,
                        'net_5d': 0, 'net_10d': 0, 'net_ratio': 0,
                    }
                    set_cached(_fkey, foreign_info, ttl=900)
    except Exception as _fe:
        logger.debug(f'Foreign info {symbol}: {_fe}')

    # ── Shark Accumulation Score v4 (Wyckoff VSA + A/D + Spring + Foreign) ──
    # Cache 2 giờ — Shark dùng daily OHLCV nên không cần refresh liên tục
    shark_score, shark_details = 0, {}
    try:
        _shark_key    = 'shark_' + symbol
        _shark_cached = get_cached(_shark_key)
        if _shark_cached:
            shark_score   = _shark_cached.get('score', 0)
            shark_details = _shark_cached.get('details', {})
        else:
            from shark_detector import calc_shark_score, load_foreign_flow
            foreign_net = None
            try:
                df_fn = load_foreign_flow(symbol, days=60)
                if df_fn is not None and 'net_vol' in df_fn.columns:
                    foreign_net = df_fn['net_vol'].values[-20:].tolist()
            except Exception:
                pass
            shark_score, shark_details = calc_shark_score(
                closes.tolist(), highs.tolist(), lows.tolist(), volumes.tolist(),
                foreign_net=foreign_net, symbol=symbol,
            )
            def _sanitize(obj):
                if isinstance(obj, dict):   return {k: _sanitize(v) for k, v in obj.items()}
                if isinstance(obj, (list, tuple)): return [_sanitize(i) for i in obj]
                if hasattr(obj, 'item'):    return obj.item()
                if isinstance(obj, bool):  return bool(obj)
                return obj
            shark_details = _sanitize(shark_details)
            shark_score   = int(shark_score)
            set_cached(_shark_key, {'score': shark_score, 'details': shark_details}, ttl=7200)
    except Exception:
        pass

    return {
        'price': round(price, 0),
        'prev_close': round(prev_close, 0),
        'price_up':   bool(price_up),
        'change_pct': round((price / prev_close - 1) * 100, 2) if prev_close > 0 else 0,
        'rsi': rsi_val,
        'rsi_divergence': {'type': div_type, 'message': div_msg},
        'macd': round(macd_val, 1),
        'macd_signal': round(macd_sig, 1),
        'macd_hist': round(macd_h, 1),
        'ma10': round(ma10, 0),
        'ma20': round(ma20, 0),
        'ma50': round(ma50, 0),
        'ma10_cross_up':   bool(ma10_cross_up),
        'ma10_cross_down': bool(ma10_cross_down),
        'ma10_slope_up':   bool(ma10_slope_up),
        'above_ma10':      bool(above_ma10),
        'above_ma50':      bool(above_ma50),
        'ma50_slope_up':   bool(ma50_slope_up),
        'golden_cross': bool(golden_cross),
        'death_cross': bool(death_cross),
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
        'market_regime': regime_info.get('regime', 'UNKNOWN'),
        'market_regime_label': regime_info.get('label', ''),
        'market_regime_vni': regime_info.get('vni', 0),
        'market_regime_ma50': regime_info.get('ma50', 0),
        'market_regime_ma200': regime_info.get('ma200', 0),
        'regime_note': regime_note,
        'regime_exempt': not _sym_use_regime,
        'vwap_info': vwap_info,
        'sector_rs':   sector_rs,
        'score_log':   score_log if 'score_log' in locals() else [],
        'adx':         _adx_val,
        'atr':         _atr_val,
        'squeeze':     _squeeze_flag,
        'vol_compress':_vol_compress,
        'obv_div':     _obv_div,
        'roc':         _roc_val,
        'ma50_slope':  _ma50_slope,
        'signals': signals,
        'three_in_one': bool(three_in_one),
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
        'is_limit_up':   is_limit_up,
        'ma20_distance_pct': round(ma20_distance * 100, 1),
        'hard_filter': hard_filter_reason,
        # Weekly trend (khung tuần)
        'weekly_trend':    weekly_trend,
        'weekly_trend_vn': weekly_trend_vn,
        'ma100': round(ma100, 0),
        'ma200': round(ma200, 0),
        # Relative Strength vs VNINDEX
        'rs_20d':       rs_data.get('rs_20d'),
        'rs_5d':        rs_data.get('rs_5d'),
        'rs_60d':       rs_data.get('rs_60d'),
        'rs_bonus':     rs_bonus,
        'breakout_52w': bool(rs_data.get('breakout_52w', False)),
        'breakout_60d': bool(rs_data.get('breakout_60d', False)),
        'rs_label':     rs_data.get('rs_label', ''),
        'rs_emoji':     rs_data.get('rs_emoji', ''),
        # Shark Accumulation Score v4
        'shark_score':   shark_score,
        'shark_details': shark_details,
        'foreign_info':  foreign_info,
        'momentum_signal': momentum_signal,
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
    # Ngân hàng thương mại — định giá bằng P/B + P/E (không dùng Graham/EV/EBITDA)
    # Graham Number không phù hợp: BVPS ngân hàng rất cao → bơm FV lên giả tạo
    # Danh sách đầy đủ các ngân hàng niêm yết VN để tránh fallback DEFAULT sai
    'BANK': {
        'MBB', 'BID', 'VCB', 'CTG', 'TCB', 'VPB', 'ACB', 'STB', 'LPB',
        'HDB', 'OCB', 'TPB', 'MSB', 'NAB', 'BAB', 'ABB', 'VAB', 'BVB',
        'KLB', 'PGB', 'SGB', 'VBB', 'SSB', 'EIB', 'SHB',
    },
    'SECURITIES': {'HCM', 'SSI', 'VND', 'SHS', 'MBS', 'CTS', 'BSI', 'FTS', 'AGR', 'VIX'},
    # CYCLICAL: Thép + Dầu khí — EV/EBITDA 5-9x, chu kỳ giá hàng hóa rõ nét
    'CYCLICAL':   {'DGC', 'HPG', 'HSG', 'NKG', 'GAS', 'PVS', 'PVD', 'PLX', 'BSR'},
    # FERTILIZER: Phân bón (DCM, DPM, BFC...) — tách riêng khỏi CYCLICAL
    # Lý do: margin dày hơn thép (DCM biên EBITDA ~20% vs thép ~5-10%)
    #        ít chu kỳ hơn (nhu cầu nông nghiệp ổn định hơn xây dựng/xe hơi)
    #        thường có net cash lớn (ít capex) → EV/EBITDA fair cao hơn thép
    #        EV/EBITDA fair: 8-10x (thép chỉ 5-8x)
    'FERTILIZER': {'DCM', 'DPM', 'BFC', 'LAS', 'PMB'},
    # UTILITY: điện (REE, POW, NT2, PC1) và KCN (SZC, BCG, IDC, KBC)
    # KCN có EV/EBITDA cao hơn điện vì biên lợi nhuận cao hơn
    # → dùng chung nhóm UTILITY với ngưỡng EV/EBITDA trung gian
    'UTILITY':    {'REE', 'POW', 'NT2', 'PC1'},
    'KCN':        {'KBC', 'SZC', 'BCG', 'IDC', 'GEG'},  # KCN — EV/EBITDA cao hơn điện
    'GROWTH':     {'FPT', 'CMG', 'MWG', 'FRT'},
}

FA_CACHE_TTL   = 8 * 3600   # 8 tiếng — đủ cho cả ngày giao dịch
_fa_cache      = {}
_fa_lock       = threading.Lock()
_fa_computing  = set()   # Track mã đang được tính để tránh duplicate calls

def _get_fa_group(symbol):
    for grp, syms in FA_GROUP.items():
        if symbol.upper() in syms:
            return grp
    return 'DEFAULT'


def _get_shares_outstanding(symbol):
    """
    Lấy số CP lưu hành từ company overview — vì balance_sheet VCI không có cột shares.
    Thử nhiều API của vnstock để tìm giá trị đúng.
    """
    try:
        from vnstock import Vnstock
        stk = Vnstock().stock(symbol=symbol, source='VCI')
        if hasattr(stk, 'company'):
            overview = stk.company.overview()
            if overview is not None and not overview.empty:
                logger.info(f'company.overview {symbol} cols: {list(overview.columns)}')
                for col in overview.columns:
                    if any(k in col.lower() for k in [
                            'share', 'outstanding', 'issued', 'listed',
                            'co_phieu', 'cp_luu', 'so_cp', 'luu_hanh']):
                        try:
                            val = float(overview.iloc[-1][col])
                            if 1e7 < val < 2e10:
                                logger.info(f'{symbol}: shares={val:.0f} from overview[{col}]')
                                return val
                        except Exception:
                            pass
    except Exception as e:
        logger.warning(f'_get_shares_outstanding {symbol}: {e}')
    return None


def _load_financials(symbol):
    """
    Tải báo cáo tài chính từ vnstock VCI.
    Log toàn bộ tên cột để debug đơn vị và tên trường.
    """
    def _sort_df(df):
        """
        Sort DataFrame theo thời gian tăng dần (cũ → mới).
        vnstock VCI: 'yearReport' (int năm) + 'lengthReport' (int quý 1-4).
        Sau sort ascending, tail(4) = 4 quý mới nhất.
        QUAN TRỌNG: validate năm cuối >= 2022 để tránh dùng dữ liệu quá cũ.
        """
        if df is None or df.empty:
            return df
        if 'yearReport' in df.columns and 'lengthReport' in df.columns:
            try:
                sorted_df = df.sort_values(
                    ['yearReport', 'lengthReport'], ascending=[True, True]
                ).reset_index(drop=True)
                # Validate: năm mới nhất sau sort phải >= 2022
                max_year = int(sorted_df['yearReport'].iloc[-1])
                if max_year < 2022:
                    # Thử sort ngược lại — vnstock có thể đã sort mới→cũ
                    sorted_desc = df.sort_values(
                        ['yearReport', 'lengthReport'], ascending=[False, False]
                    ).reset_index(drop=True)
                    # Đảo lại để tail() lấy được hàng mới nhất
                    sorted_df = sorted_desc.iloc[::-1].reset_index(drop=True)
                    max_year2 = int(sorted_df['yearReport'].iloc[-1])
                    logger.info(f'FA sort corrected: max_year {max_year}→{max_year2}')
                return sorted_df
            except Exception as e:
                logger.warning(f'FA _sort_df error: {e}')
        if 'yearReport' in df.columns:
            try:
                return df.sort_values('yearReport', ascending=True).reset_index(drop=True)
            except Exception:
                pass
        return df

    def _validate_freshness(df, symbol, min_year=2022):
        """Trả về (ok, error_msg) — reject nếu dữ liệu quá cũ."""
        if df is None or df.empty:
            return False, 'DataFrame rong'
        if 'yearReport' not in df.columns:
            return True, ''  # Không có cột year, không validate được
        try:
            max_year = int(df['yearReport'].max())
            if max_year < min_year:
                return False, f'Du lieu qua cu: nam moi nhat={max_year} (can >={min_year})'
        except Exception:
            pass
        return True, ''

    def _try_load(source):
        from vnstock import Vnstock
        import concurrent.futures as _cf
        stk = Vnstock().stock(symbol=symbol, source=source)
        if not hasattr(stk, 'finance'):
            raise AttributeError(f'source={source} không ho tro finance API')

        # FIX P1: Gọi 5 API song song thay vì tuần tự
        # Sequential: ~25s (5 x ~5s) → Parallel: ~5-8s
        _calls = {
            'income_q':   lambda: stk.finance.income_statement(period='quarter', lang='en'),
            'balance_q':  lambda: stk.finance.balance_sheet(period='quarter', lang='en'),
            'cashflow_q': lambda: stk.finance.cash_flow(period='quarter', lang='en'),
            'income_yr':  lambda: stk.finance.income_statement(period='year', lang='en'),
            'balance_yr': lambda: stk.finance.balance_sheet(period='year', lang='en'),
        }
        _raw = {}
        with _cf.ThreadPoolExecutor(max_workers=5) as _ex:
            _futs = {key: _ex.submit(_vnstock_call, fn) for key, fn in _calls.items()}
            for key, fut in _futs.items():
                try:
                    _raw[key] = fut.result(timeout=35)
                except Exception as e:
                    logger.warning(f'FA {symbol}/{source}/{key}: {e}')
                    _raw[key] = None

        income_q   = _raw['income_q']
        balance_q  = _raw['balance_q']
        cashflow_q = _raw['cashflow_q']
        income_yr  = _raw['income_yr']
        balance_yr = _raw['balance_yr']

        if balance_q is None or balance_q.empty:
            raise ValueError(f'balance_sheet rong tu source={source}')

        # Sort trước để validate freshness
        bq_sorted  = _sort_df(balance_q)
        iq_sorted  = _sort_df(income_q)
        cq_sorted  = _sort_df(cashflow_q)
        iyr_sorted = _sort_df(income_yr)
        byr_sorted = _sort_df(balance_yr)

        # Validate data freshness — từ chối nếu dữ liệu quá cũ (< 2022)
        ok, err = _validate_freshness(bq_sorted, symbol)
        if not ok:
            raise ValueError(f'Data freshness fail: {err}')

        # Log để debug
        logger.info(f'FA {symbol}/{source} balance_q ALL cols: {list(balance_q.columns)}')
        logger.info(f'FA {symbol}/{source} income_q  ALL cols: {list(income_q.columns) if income_q is not None else []}')
        if bq_sorted is not None and not bq_sorted.empty:
            last = bq_sorted.tail(1).to_dict('records')
            logger.info(f'FA {symbol} balance_q last row (sorted): {last}')

        return {
            'income_q':   iq_sorted,
            'balance_q':  bq_sorted,
            'cashflow_q': cq_sorted,
            'income_yr':  iyr_sorted,
            'balance_yr': byr_sorted,
            'ok':     True,
            'source': source,
        }

    last_err = ''
    for source in ['VCI']:   # Chỉ dùng VCI — TCBS không được support theo log
        try:
            result = _try_load(source)
            # Thử lấy số CP từ company overview
            shares_api = _get_shares_outstanding(symbol)
            result['shares_from_api'] = shares_api
            return result
        except Exception as e:
            last_err = str(e)
            logger.warning(f'FA load_financials {symbol}/{source}: {e}')

    return {'ok': False, 'error': f'Không tai duoc BCTC: {last_err[:150]}'}


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

    Từ log thực tế (Railway):
      VCB shares_raw = 1,000,000,000  (1 tỷ CP — đúng là ~4.7 tỷ, nhưng đây là fallback)
      DGC shares_raw = 220,000,000,000 (220 tỷ — lấy nhầm cột TOTAL ASSETS)

    Thực tế vnstock VCI balance_sheet KHÔNG có cột shares/outstanding trực tiếp.
    Cột 'shares' hoặc 'outstanding' không tồn tại trong response.
    _safe_val fallback về 1e9 (1 tỷ CP) — đây là giá trị default.

    Phạm vi hợp lý TTCK VN:
      Nhỏ nhất: ~50 triệu CP  = 5e7
      Lớn nhất: ~20 tỷ CP     = 2e10
    Nếu shares > 2e10 thì chắc chắn lấy nhầm cột tài sản → dùng fallback.
    """
    if shares_raw <= 0:
        return 1e9  # fallback 1 tỷ CP
    if shares_raw > 2e10:
        # Quá lớn — chắc lấy nhầm cột TOTAL ASSETS (hàng trăm nghìn tỷ)
        logger.warning(f'shares_raw={shares_raw:.0f} qua lon, có thể lay nham cot — dung fallback 1ty CP')
        return 1e9
    # Trong range hợp lý → giữ nguyên
    return shares_raw


# Ngưỡng tỷ số hợp lý theo ngành TTCK VN
# Cơ sở: P/E trung bình thực tế 2020-2024 (nguồn: báo cáo SSI Research, VCSC, VNDIRECT)
# VN-Index P/E trung bình: ~12-15x | Ngân hàng Tier2: 7-11x | CTCK: 12-22x
# Cập nhật: 03/2026 — điều chỉnh sau kiểm tra vs giá thực tế 5 mã watchlist
SECTOR_RATIOS = {
    'BANK': {
        # Ngân hàng TM: P/B vẫn là neo chính nhưng giảm trọng số vì ROE biến động
        # pe_fair: 9.5→10.5 (Tier1 VCB/BID thực tế 12-16x kéo trung bình lên)
        # pb_fair: 1.4→1.5 (P/B bình quân ngành 1.3-1.7x giai đoạn 2020-2024)
        # w_pb: 0.6→0.5 (cân bằng hơn giữa P/B và P/E)
        'pe_fair': 10.5, 'pe_low': 7.0,  'pe_high': 14.0,
        'pb_fair': 1.5,  'pb_low': 0.9,  'pb_high': 2.3,
        'w_pb':    0.5,  'w_pe':   0.5,
    },
    'SECURITIES': {
        # CTCK: giao dịch ở P/E 14-22x bình thường, 12x là đáy bear market
        # pe_fair: 12→16 (phản ánh P/E trung tính, không phải đáy)
        # w_pe: 0.6→0.7 (revenue từ phí giao dịch, không phụ thuộc assets)
        'pe_fair': 16.0, 'pe_low': 10.0, 'pe_high': 24.0,
        'pb_fair': 2.0,  'pb_low': 1.2,  'pb_high': 3.2,
        'w_pb':    0.3,  'w_pe':   0.7,
    },
    'CYCLICAL': {
        # Thép + Dầu khí: EV/EBITDA 5-9x, chu kỳ giá hàng hóa rõ nét
        # THÊM ev_ebitda_trigger: khi EV/EBITDA thực > 30x = EBITDA đáy chu kỳ
        #   → tự động dùng P/E fallback thay vì cho FV âm (NKG đáy 2024: 95x)
        'ev_ebitda_fair':    7.0, 'ev_ebitda_low': 4.5, 'ev_ebitda_high': 10.0,
        'ev_ebitda_trigger': 30.0,   # Trên ngưỡng này = EBITDA đáy, vô nghĩa
        'pe_fair': 10.0, 'pe_low': 6.0, 'pe_high': 15.0,
        'cash_mode': 'strict',   # Chỉ lấy cash thuần, không gộp short-term investments
    },
    'FERTILIZER': {
        # Phân bón (DCM, DPM): margin EBITDA dày ~20%, ít chu kỳ hơn thép
        # Thường có net cash lớn (ít capex mở rộng) → EV/EBITDA fair cao hơn thép
        # DCM thực tế: EV/EBITDA ~5.5x (vì net cash khổng lồ) nhưng ngành fair ~8-9x
        # cash_mode='strict': chỉ dùng cash thuần (không gộp short-term investments)
        #   vì DCM gửi 4,082B tiền gửi ngắn hạn → nếu gộp vào cash thì net_debt âm quá lớn
        #   → EV quá thấp → FV bị kéo xuống giả tạo
        'ev_ebitda_fair':    8.5, 'ev_ebitda_low': 6.0, 'ev_ebitda_high': 12.0,
        'ev_ebitda_trigger': 40.0,   # Phân bón ít chu kỳ hơn thép → trigger cao hơn
        'pe_fair': 11.0, 'pe_low': 7.0, 'pe_high': 16.0,
        'cash_mode': 'strict',   # KEY FIX: chỉ cash thuần, không gộp short-term investments
    },
    'UTILITY': {
        # Điện (PC1/POW/REE): EV/EBITDA 8-12x — dòng tiền ổn định
        'ev_ebitda_fair':    10.0, 'ev_ebitda_low': 7.0, 'ev_ebitda_high': 14.0,
        'ev_ebitda_trigger': 40.0,
        'pe_fair': 14.0, 'pe_low': 9.0, 'pe_high': 20.0,
    },
    'KCN': {
        # KCN (KBC/SZC/IDC/BCG): EV/EBITDA 15-25x — kỳ vọng tăng trưởng lô KCN
        # KBC giao dịch ~19x, SZC ~20x+ → fair=18x, high=28x
        'ev_ebitda_fair':    18.0, 'ev_ebitda_low': 12.0, 'ev_ebitda_high': 28.0,
        'ev_ebitda_trigger': 50.0,
        'pe_fair': 20.0, 'pe_low': 13.0, 'pe_high': 32.0,
    },
    'GROWTH': {
        # FPT giao dịch 20-30x, MWG 15-25x → pe_fair=18 quá thấp
        # pe_high: 28→32 để không cắt FV quá sớm khi tăng trưởng mạnh
        'peg_fair': 1.0, 'pe_fair': 20.0, 'pe_low': 13.0, 'pe_high': 32.0,
    },
    'DEFAULT': {
        # Graham + P/E 50/50: phù hợp mã tiêu dùng (VNM, MSN) và mã chưa phân loại
        # pe_fair: 12→13 (VN-Index P/E trung bình 12-15x, dùng 13 làm trung điểm)
        'pe_fair': 13.0, 'pe_low': 8.0,  'pe_high': 19.0,
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
        return {'ok': False, 'error': fin.get('error', 'Không tai duoc BCTC'), 'symbol': sym}

    iq  = fin['income_q']
    bq  = fin['balance_q']
    cq  = fin['cashflow_q']
    iyr = fin['income_yr']

    # Normalize số cổ phiếu
    shares_raw = _safe_val(bq, ['shares', 'so_co_phieu', 'outstanding'], default=0)
    if shares_raw <= 0:
        shares_raw = _safe_val(fin['balance_yr'], ['shares', 'so_co_phieu', 'outstanding'], default=0)
    shares = _normalize_shares(shares_raw)

    # Ưu tiên dùng số CP từ company API (chính xác hơn)
    shares_api = fin.get('shares_from_api')
    if shares_api and 1e7 < shares_api < 2e10:
        shares = shares_api
        logger.info(f'FA {sym}: dung shares_from_api={shares:.0f}')
    else:
        logger.info(f'FA {sym}: shares_raw={shares_raw} shares_normalized={shares:.0f} (fallback)')

    details    = {}
    fair_value = 0.0
    method     = ''
    note       = ''
    warnings   = []

    # ── BANK và SECURITIES: P/B + P/E ────────────────────────────────────────
    if group in ('BANK', 'SECURITIES'):
        # equity cuối kỳ (không TTM)
        equity     = _safe_val(bq, ["OWNER'S EQUITY(Bn.VND)", 'equity', 'von_chu_so_huu', 'owner'])
        net_income, has_neg = _ttm(iq, ['Attributable to parent company',
                                         'Net Profit For the Year',
                                         'net_income', 'loi_nhuan_sau_thue'])

        # vnstock VCI: tên cột ghi "(Bn. VND)" nhưng thực tế giá trị là đồng (VND)
        # Xác nhận từ log: VCB equity = 42,482,022,000,000đ = ~42.5 nghìn tỷ ✓
        # Không nhân 1e9 — chia thẳng cho shares
        bvps = equity     / shares if equity     > 0 and shares > 0 else 0
        eps  = net_income / shares if net_income > 0 and shares > 0 else 0

        logger.info(f'FA {sym}: equity={equity:.0f} net_income={net_income:.0f} '
                    f'shares={shares:.0f} bvps={bvps:.0f} eps={eps:.0f}')

        pe = price / eps  if eps  > 0 and price > 0 else 0
        pb = price / bvps if bvps > 0 and price > 0 else 0

        details = {
            'EPS_TTM': round(eps, 0),
            'BVPS':    round(bvps, 0),
            'P/E':     round(pe, 1),
            'P/B':     round(pb, 2),
        }

        if has_neg:
            warnings.append('Có quý lỗ trong TTM — EPS có thể bị kéo thấp bất thường')

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
            note       = 'EPS am hoac không co — chi dung P/B'
        else:
            return {'ok': False, 'error': f'Không du BVPS/EPS cho {group}', 'symbol': sym}

    # ── CYCLICAL, FERTILIZER và UTILITY: EV/EBITDA ───────────────────────────
    elif group in ('CYCLICAL', 'FERTILIZER', 'UTILITY', 'KCN'):
        # Cột income_q thực tế: 'Operating Profit/Loss', 'Gross Profit', 'Net Profit For the Year'
        ebitda, has_neg = _ttm(iq, ['Operating Profit/Loss', 'ebitda',
                                     'operating_profit', 'loi_nhuan_tu_hoat_dong'])
        if ebitda == 0:
            ebit, _ = _ttm(iq, ['Operating Profit/Loss', 'ebit', 'operating'])
            da, _   = _ttm(cq, ['depreciation', 'khau_hao', 'amortization'])
            ebitda  = ebit + da

        # Cột balance_q thực tế (đơn vị đồng):
        # Fix: chỉ dùng interest-bearing debt (vay ngắn hạn + vay dài hạn)
        # KHÔNG dùng 'LIABILITIES (Bn. VND)' = total liabilities (bao gồm cả payables, etc.)
        # KHÔNG dùng 'Long-term liabilities' (bao gồm cả deferred revenue, etc.)
        short_debt = _safe_val(bq, ['Short-term borrowings', 'short_term_debt',
                                     'vay_ngan_han', 'short_borrow'])
        long_debt  = _safe_val(bq, ['Long-term borrowings', 'long_term_debt',
                                     'vay_dai_han', 'long_borrow'])
        total_debt = short_debt + long_debt  # chỉ vay có lãi

        # cash_mode kiểm soát cách tính tiền mặt:
        #   'strict' (FERTILIZER): chỉ cash thuần — tránh DCM gộp 4,082B tiền gửi
        #     ngắn hạn vào cash → net_debt âm quá lớn → EV thấp → FV sai
        #   'full'  (CYCLICAL/UTILITY mặc định): cash + short-term investments
        #     Phù hợp với thép/điện — tiền gửi NH thường là dự phòng thanh khoản
        cash_mode = ratios.get('cash_mode', 'full')
        if cash_mode == 'strict':
            # Chỉ lấy tiền mặt và tương đương tiền thuần
            cash = _safe_val(bq, ['Cash and cash equivalents',
                                   'tien_va_tuong_duong', 'cash_equiv',
                                   'tien_mat', 'cash'])
        else:
            # Gộp cả short-term investments (tiền gửi NH, trái phiếu ngắn hạn)
            cash = _safe_val(bq, ['Cash and cash equivalents', 'Short-term investments',
                                   'tien_va_tuong_duong', 'cash_equiv'])

        net_debt   = total_debt - cash   # âm = net cash position
        logger.info(f'FA {sym} [cash_mode={cash_mode}]: '
                    f'short_debt={short_debt/1e9:.1f}ty long_debt={long_debt/1e9:.1f}ty '
                    f'cash={cash/1e9:.1f}ty net_debt={net_debt/1e9:.1f}ty')

        # Market cap: price (đ) × shares = đồng
        # Nếu price=0 (fetch failed), thử lại một lần
        if price <= 0:
            try:
                pd2 = fetch_price(sym)
                price = pd2.get('price', 0) if pd2 else 0
                logger.info(f'FA {sym}: retry fetch_price → {price}')
            except Exception:
                pass
        mkt_cap   = price * shares if price > 0 else 0  # đồng
        ev        = mkt_cap + net_debt                   # đồng
        ev_ebitda = ev / ebitda if ebitda > 0 and ev > 0 else 0

        # Nếu vẫn không lấy được giá, dùng P/E thay thế EV/EBITDA
        if price <= 0:
            logger.warning(f'FA {sym}: price=0, EV/EBITDA không tin cay — fallback P/E')
            net_income2, _ = _ttm(iq, ['Attributable to parent company',
                                        'Net Profit For the Year', 'net_income'])
            eps2 = net_income2 / shares if net_income2 > 0 and shares > 0 else 0
            if eps2 > 0:
                fair_value = eps2 * ratios['pe_fair']
                method = 'P/E (gia=0, không tinh duoc EV/EBITDA)'
                details = {'EPS_TTM': round(eps2, 0), 'EV/EBITDA': 'N/A (gia=0)'}
                warnings.append('Không lay duoc gia hiện tại — dung P/E thay EV/EBITDA')
                note = 'Vui long refresh: /fv ' + sym
                # Skip to valuation section
                mos = ratios.get('mos', 0.25)
                fair_low   = round(fair_value * (1 - mos))
                fair_high  = round(fair_value * (1 + mos))
                fair_value = round(fair_value)
                discount = (fair_value - price) / fair_value * 100 if fair_value > 0 else 0
                valuation = ('UNDERVALUED' if price < fair_low else
                             'OVERVALUED'  if price > fair_high else 'FAIR')
                upside_pct2 = (fair_value - price) / price * 100 if price > 0 else 0  # FIX
                return {
                    'ok': True, 'symbol': sym, 'group': group, 'method': method,
                    'fair_value': fair_value, 'fair_low': fair_low, 'fair_high': fair_high,
                    'valuation': valuation, 'discount': round(upside_pct2, 1),
                    'details': details, 'note': note,
                    'warnings': warnings,
                }

        net_income, _ = _ttm(iq, ['Attributable to parent company',
                                   'Net Profit For the Year', 'net_income'])
        eps = net_income / shares if net_income > 0 and shares > 0 else 0

        logger.info(f'FA {sym}: ebitda={ebitda:.0f} net_debt={net_debt:.0f} '
                    f'mkt_cap={mkt_cap:.0f} ev_ebitda={ev_ebitda:.1f} eps={eps:.0f}')

        details = {
            'EBITDA_TTM':    round(ebitda / 1e9, 1),   # hiển thị tỷ đồng
            'Net_Debt_ty' if net_debt >= 0 else 'Net_Cash_ty': round(abs(net_debt) / 1e9, 1),
            'EV/EBITDA':     round(ev_ebitda, 1),
            'EPS_TTM':       round(eps, 0),
        }

        if has_neg:
            warnings.append('Co quy EBITDA am — nen xem lai BCTC tung quy')

        # ── EV/EBITDA đáy chu kỳ trigger ─────────────────────────────────────
        # Khi EV/EBITDA thực > ev_ebitda_trigger (mặc định 30x): EBITDA đang ở đáy
        # chu kỳ (ví dụ NKG Q4/2024: 95.9x), dùng EV/EBITDA sẽ cho FV sai hoàn toàn
        # → Tự động fallback sang P/E thay vì tiếp tục dùng EV/EBITDA
        ev_trigger = ratios.get('ev_ebitda_trigger', 30.0)
        cycle_bottom = (ev_ebitda > ev_trigger and ev_ebitda > 0)

        if cycle_bottom:
            logger.warning(f'FA {sym}: EV/EBITDA={ev_ebitda:.1f}x > {ev_trigger}x trigger '
                           f'— EBITDA dang o day chu ky, dung P/E fallback')
            warnings.append(
                f'EV/EBITDA={ev_ebitda:.0f}x — EBITDA day chu ky, dung P/E thay the. '
                f'FV chi mang tinh tham khao, can kiem tra EBITDA binh thuong hoa'
            )

        if ebitda > 0 and not cycle_bottom:
            ev_fair     = ebitda * ratios['ev_ebitda_fair']  # đồng
            fair_value  = (ev_fair - net_debt) / shares       # đồng/CP
            method      = f'EV/EBITDA ({group})'
            if ev_ebitda > 0:
                note = (f'EV/EBITDA hiện tại: {ev_ebitda:.1f}x | '
                        f'Fair: {ratios["ev_ebitda_fair"]}x')
            if fair_value <= 0:
                if eps > 0:
                    fair_value = eps * ratios['pe_fair']
                    method = 'P/E (EV/EBITDA am do no cao)'
                    warnings.append('No rong — dung P/E thay the')
                else:
                    return {'ok': False, 'error': 'No qua lon, ca EV/EBITDA va EPS deu am', 'symbol': sym}
        else:
            # EBITDA = 0, âm, hoặc đang ở đáy chu kỳ (ev_ebitda > trigger)
            if eps > 0:
                fair_value = eps * ratios['pe_fair']
                if cycle_bottom:
                    method = f'P/E (EBITDA day chu ky — EV/EBITDA={ev_ebitda:.0f}x vo nghia)'
                else:
                    method = 'P/E (fallback khi thieu EBITDA)'
                    note   = 'Không co EBITDA — dung P/E thay the'
            else:
                return {'ok': False, 'error': 'Không du EBITDA/EPS', 'symbol': sym}

    # ── GROWTH: PEG ──────────────────────────────────────────────────────────
    elif group == 'GROWTH':
        net_income, has_neg = _ttm(iq, ['Attributable to parent company',
                                         'Net Profit For the Year', 'net_income'])
        eps_ttm = net_income / shares if net_income > 0 and shares > 0 else 0

        # CAGR EPS 3 năm từ báo cáo năm
        growth_pct = 15.0
        if iyr is not None and not iyr.empty:
            for col in iyr.columns:
                if any(k in col.lower() for k in ['attributable', 'net profit', 'net_income']):
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
            'EPS_TTM':      round(eps_ttm, 0),
            'Growth_3Y_%':  round(growth_pct, 1),
            'P/E_hien_tai': round(pe, 1),
            'PE_PEG':       round(pe_peg, 1),
            'PE_dung':      round(pe_use, 1),
        }

        if has_neg:
            warnings.append('Co quy lo — tang truong có thể không ben vung')

        if eps_ttm > 0:
            fair_value = eps_ttm * pe_use
            method     = f'PEG (CAGR EPS {growth_pct:.0f}%/nam)'
        else:
            return {'ok': False, 'error': 'EPS am — không dinh gia duoc', 'symbol': sym}

    # ── DEFAULT: Graham Number + P/E ─────────────────────────────────────────
    else:
        net_income, has_neg = _ttm(iq, ['Attributable to parent company',
                                         'Net Profit For the Year', 'net_income'])
        equity  = _safe_val(bq, ["OWNER'S EQUITY(Bn.VND)", 'equity', 'von_chu_so_huu',
                                  'Capital and reserves (Bn. VND)'])
        eps_ttm = net_income / shares if net_income > 0 and shares > 0 else 0
        bvps    = equity     / shares if equity     > 0 and shares > 0 else 0
        pe      = price / eps_ttm if eps_ttm > 0 and price > 0 else 0
        pb      = price / bvps    if bvps    > 0 and price > 0 else 0

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
            note       = 'Không co BVPS — chi dung P/E'
        else:
            return {'ok': False, 'error': 'EPS am — không dinh gia duoc', 'symbol': sym}

    # ── Tính vùng giá và valuation ───────────────────────────────────────────
    if fair_value <= 0:
        return {'ok': False, 'error': 'Fair value tinh ra <= 0 — có thể do no qua cao', 'symbol': sym}

    # Margin of safety: TTCK VN là frontier market, volatility cao (~30-40%/năm)
    # Dùng MoS rộng hơn để tránh false FAIR/OVERVALUED khi data không chắc
    # Có warnings (quý lỗ, thiếu data) → MoS rộng hơn
    margin = 0.30 if warnings else 0.25  # 25-30% thay vì 15-20%
    fair_low  = round(fair_value * (1 - margin), -2)
    fair_val  = round(fair_value, -2)
    fair_high = round(fair_value * (1 + margin), -2)

    # FIX discount formula:
    # Cũ: (fair_val - price) / fair_val  → cho ra % discount so với FV (confusing)
    # Mới: (fair_val - price) / price    → upside/downside % từ giá hiện tại lên FV
    #   Dương = upside tiềm năng (mua được rẻ hơn FV)
    #   Âm    = đang đắt hơn FV
    # Ý nghĩa trực quan hơn: "FV còn +49.8% so với giá hiện tại"
    if price > 0:
        upside_pct = (fair_val - price) / price * 100
    else:
        upside_pct = 0.0

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
        'discount':   round(upside_pct, 1),  # Upside% từ giá hiện tại lên FV (dương=rẻ, âm=đắt)
        'margin_pct': int(margin * 100),
        'details':    details,
        'note':       note,
    }


def fetch_fair_value(symbol):
    """
    Lấy fair value với cache 8 tiếng.
    Fix race condition: nếu đang compute cùng mã thì skip, không chạy duplicate.
    Fix discount: tính lại real-time theo giá hiện tại.
    """
    sym = symbol.upper()
    key = 'fv_' + sym

    # Check cache trước
    with _fa_lock:
        if key in _fa_cache:
            data, ts = _fa_cache[key]
            if time.time() - ts < FA_CACHE_TTL:
                return _update_fv_discount(data)
        # Guard: đang compute rồi thì return None để tránh duplicate
        if sym in _fa_computing:
            logger.info(f'FA {sym}: already computing, skip duplicate')
            return {'ok': False, 'error': 'Đang tính toan, thu lai sau', 'symbol': sym}
        _fa_computing.add(sym)

    try:
        logger.info(f'FA: computing fair value for {sym}')
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
    finally:
        with _fa_lock:
            _fa_computing.discard(sym)


def _update_fv_discount(cached_fv):
    """
    Tính lại upside/downside% với giá thị trường hiện tại.
    fair_value cố định (từ BCTC), discount thay đổi theo giá.
    FIX: dùng (fv - price) / price × 100 — upside% từ giá lên FV.
    """
    if not cached_fv or not cached_fv.get('ok'):
        return cached_fv
    sym = cached_fv.get('symbol', '')
    fair_val  = cached_fv.get('fair_value', 0)
    fair_low  = cached_fv.get('fair_low', 0)
    fair_high = cached_fv.get('fair_high', 0)
    if fair_val <= 0:
        return cached_fv
    try:
        price_data = fetch_price(sym)
        current_price = price_data.get('price', 0) if price_data else 0
        if current_price > 0:
            upside_pct = (fair_val - current_price) / current_price * 100  # FIX: chia cho price
            if current_price < fair_low:
                valuation = 'UNDERVALUED'
            elif current_price > fair_high:
                valuation = 'OVERVALUED'
            else:
                valuation = 'FAIR'
            updated = dict(cached_fv)
            updated['discount']   = round(upside_pct, 1)
            updated['valuation']  = valuation
            updated['price_used'] = current_price
            return updated
    except Exception:
        pass
    return cached_fv


def warmup_fair_values():
    """
    Tính FV cho toàn bộ watchlist — chỉ gọi từ fa_worker lúc 8:00 sáng.
    Sleep 15s/mã: FA cần ~3-5 API calls/mã → 15s buffer an toàn với 60 req/phút limit.
    """
    logger.info('FA warmup started for all watchlist symbols')
    for sym in list(WATCHLIST):
        try:
            fetch_fair_value(sym)
            time.sleep(15)   # Tăng từ 8s → 15s: FA nặng hơn 1D history
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
    for source in ['VCI', 'KBS']:
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

    return {'symbol': symbol, 'price': 0, 'change_pct': 0, 'source': 'error', 'error': 'Không the lay gia'}


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
        return {'symbol': symbol, 'error': 'Không tải được dữ liệu'}

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
        return {'symbol': symbol, 'error': 'Không tinh duoc chi bao'}

    logger.info(f"{symbol}: computed vol_today={result.get('vol_today', 0)} vol_ma20={result.get('vol_ma20', 0)}")
    result['symbol'] = symbol
    result['source'] = source

    # ── score_min và action_effective: tránh nhầm lẫn score 67 = MUA với mã cần 80 ──
    # /analyze hiển thị action dựa trên ngưỡng 65 chung.
    # Nhưng mỗi mã có score_min riêng từ backtest (VCB=80, HCM=75, DGC=65).
    # Thêm 2 field này để Telegram bot và frontend có thể hiển thị đúng.
    _score_min = get_min_score(symbol)
    _score_adj = result.get('score', 50)
    result['score_min']        = _score_min
    result['action_effective'] = (
        'MUA'      if _score_adj >= _score_min else
        'BAN'      if _score_adj <= 35          else
        'THEO DOI'
    )

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

@app.route('/api/vol/<symbol>')
def api_vol(symbol):
    """
    Lightweight endpoint — chỉ trả vol/score từ cache, không gọi vnstock.
    Dùng cho /volscan để quét nhanh nhiều mã mà không tốn rate limit.
    """
    sym    = symbol.upper()
    cached = get_cached('analysis_' + sym + '_live')
    if cached and 'score' in cached:
        return jsonify({k: cached[k] for k in
            ['symbol', 'price', 'vol_today', 'vol_tb20', 'vol_ma20',
             'vol_ratio', 'vol_signal', 'score', 'action',
             'weekly_trend', 'hard_filter', 'rsi',
             'ma10', 'ma50', 'ma10_cross_up', 'ma10_cross_down']
            if k in cached})
    # Cache miss — trả minimal response, không compute
    return jsonify({'error': 'no_cache', 'symbol': sym,
                    'hint': 'Goi /api/analyze/' + sym + ' truoc'})


@app.route('/api/analyze/<symbol>')
def api_analyze(symbol):
    return jsonify(fetch_analysis(symbol.upper()))

@app.route('/api/fairvalue/<symbol>')
def api_fairvalue(symbol):
    """Endpoint riêng để lấy fair value — dùng khi cần refresh thủ công.
    FIX: clear cả _fa_computing guard để tránh bị block khi warmup background đang treo.
    User gọi /fv → luôn tính lại, không bị chặn bởi guard.
    """
    sym = symbol.upper()
    with _fa_lock:
        _fa_cache.pop('fv_' + sym, None)      # Xóa cache cũ
        _fa_computing.discard(sym)             # FIX: clear guard nếu đang treo
    result = fetch_fair_value(sym)
    return jsonify(result)

@app.route('/api/fairvalue/debug/<symbol>')
def api_fairvalue_debug(symbol):
    """Debug endpoint — xem raw BCTC để kiểm tra đơn vị và tên cột."""
    sym = symbol.upper()
    try:
        fin = _load_financials(sym)
        if not fin.get('ok'):
            return jsonify({'ok': False, 'error': fin.get('error')})
        bq = fin['balance_q']
        iq = fin['income_q']
        return jsonify({
            'ok': True,
            'source': fin.get('source'),
            'balance_q_cols': list(bq.columns) if bq is not None else [],
            'balance_q_last': bq.tail(1).to_dict('records') if bq is not None and not bq.empty else [],
            'income_q_cols':  list(iq.columns) if iq is not None else [],
            'income_q_last':  iq.tail(1).to_dict('records') if iq is not None and not iq.empty else [],
        })
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

@app.route('/api/warnings_1h/<symbol>')
def api_warnings_1h(symbol):
    """Endpoint riêng cho cảnh báo 1H — gọi async từ Telegram bot."""
    try:
        warnings = analyze_1h_warnings(symbol.upper())
        return jsonify({'symbol': symbol.upper(), 'warnings_1h': warnings})
    except Exception as e:
        return jsonify({'symbol': symbol.upper(), 'warnings_1h': [], 'error': str(e)})


@app.route('/api/foreign/<symbol>')
def api_foreign(symbol):
    """
    Trả về foreign trading data cho 1 mã.

    Ưu tiên:
      1. Historical flow (30 ngày) từ load_foreign_flow() — nếu có series >= 5 rows
      2. Snapshot hôm nay từ VCI GraphQL — luôn accessible trên Railway (same domain VCI)

    Response schema:
      {symbol, available, mode,
       -- historical mode --
       consecutive_buy, consecutive_sell, net_5d, net_10d, net_ratio, raw_days, dates,
       -- snapshot mode --
       foreign_vol, foreign_room, current_ratio, max_ratio, room_used_pct,
       -- common --
       label, bonus}
    Cache 15 phút.
    """
    sym = symbol.upper()
    try:
        _fkey    = 'foreign_' + sym
        _fcached = get_cached(_fkey)
        if _fcached and _fcached.get('available'):
            return jsonify({'symbol': sym, **_fcached})

        # ── Attempt A: historical flow series ───────────────────────────────
        from shark_detector import load_foreign_flow as _lff
        _fdf = _lff(sym, days=30)
        if _fdf is not None and 'net_vol' in _fdf.columns and len(_fdf) >= 5:
            _fnet  = _fdf['net_vol'].values
            _dates = []
            if 'time' in _fdf.columns:
                _dates = [str(d)[:10] for d in _fdf['time'].values[-10:]]

            _cbuy = _csell = 0
            for _v in reversed(_fnet):
                if _v > 0:
                    if _csell > 0: break
                    _cbuy += 1
                elif _v < 0:
                    if _cbuy > 0: break
                    _csell += 1
                else:
                    break

            _net5   = float(sum(_fnet[-5:]))
            _net10  = float(sum(_fnet[-10:]))
            _acache = get_cached(sym)
            _adtv   = _acache.get('vol_ma20', 1e6) if _acache else 1e6
            _nratio = _net10 / (_adtv * 10) * 100 if _adtv > 0 else 0
            _raw_days = [round(float(v)/1e3, 1) for v in _fnet[-10:]]

            if _cbuy > 0:
                _flabel = f'Mua rong {_cbuy} phien lien tiep | Net10d={_net10/1e6:.2f}M cp'
            elif _csell > 0:
                _flabel = f'Ban rong {_csell} phien lien tiep | Net10d={_net10/1e6:.2f}M cp'
            else:
                _flabel = f'Khong ro xu huong | Net10d={_net10/1e6:.2f}M cp'

            # Bar chart mini: dùng ký tự ▲▼ cho 10 ngày gần nhất
            _bar = ''.join('🟢' if v > 0 else ('🔴' if v < 0 else '⬜') for v in _fnet[-10:])
            _sign5  = '+' if _net5  >= 0 else ''
            _sign10 = '+' if _net10 >= 0 else ''
            _html_lines = [
                f'<b>Khối ngoại — {sym}</b> <i>(30 ngày)</i>',
                f'{_bar}',
                f'Net 5 phiên:  <b>{_sign5}{_net5/1e6:.2f}M cp</b>',
                f'Net 10 phiên: <b>{_sign10}{_net10/1e6:.2f}M cp</b>',
                f'Tỷ lệ ADTV:   <b>{_nratio:+.1f}%</b>',
            ]
            if _cbuy > 0:
                _html_lines.append(f'🟢 Mua ròng <b>{_cbuy} phiên</b> liên tiếp')
            elif _csell > 0:
                _html_lines.append(f'🔴 Bán ròng <b>{_csell} phiên</b> liên tiếp')
            result = {
                'symbol': sym, 'available': True, 'mode': 'historical',
                'consecutive_buy':  int(_cbuy),
                'consecutive_sell': int(_csell),
                'net_5d':    round(_net5  / 1e6, 3),
                'net_10d':   round(_net10 / 1e6, 3),
                'net_ratio': round(_nratio, 1),
                'label':     _flabel,
                'raw_days':  _raw_days,
                'dates':     _dates,
                'bonus': 0,
                'display_html': '\n'.join(_html_lines),
            }
            set_cached(_fkey, result, ttl=900)
            return jsonify(result)

        # ── Attempt B: VCI GraphQL snapshot ─────────────────────────────────
        # Dùng domain trading.vietcap.com.vn — cùng domain VCI quote.history()
        # đang hoạt động trên Railway → luôn accessible
        from shark_detector import load_foreign_snapshot as _lfs
        _snap = _lfs(sym)
        if _snap and _snap.get('available'):
            _c_ratio    = _snap['current_ratio']    # % holding hiện tại
            _m_ratio    = _snap['max_ratio']        # % holding tối đa
            _room_used  = _snap['room_used_pct']    # % room đã dùng
            _room_left  = _snap.get('room_left_pct', round(_m_ratio - _c_ratio, 1))

            # room_used = c_ratio/m_ratio*100 (đã đúng từ shark_detector v4.5)
            _room_left = _snap.get('room_left_pct', round(_m_ratio - _c_ratio, 1))
            if _room_used >= 95:
                _flabel = f'Room gần hết ({_room_used:.1f}%) | Hold {_c_ratio:.1f}%/{_m_ratio:.1f}%'
            elif _room_used >= 80:
                _flabel = f'Room cao ({_room_used:.1f}%) | Hold {_c_ratio:.1f}%/{_m_ratio:.1f}%'
            elif _room_used >= 50:
                _flabel = f'Room trung bình ({_room_used:.1f}%) | Hold {_c_ratio:.1f}%/{_m_ratio:.1f}%'
            else:
                _flabel = f'Room còn nhiều ({_room_used:.1f}%) | Hold {_c_ratio:.1f}%/{_m_ratio:.1f}%'

            # Room bar visual
            _filled  = round(_room_used / 10)
            _room_bar = '█' * _filled + '░' * (10 - _filled)
            if _room_used >= 95:
                _room_emoji = '🔴'
            elif _room_used >= 80:
                _room_emoji = '🟠'
            elif _room_used >= 50:
                _room_emoji = '🟡'
            else:
                _room_emoji = '🟢'
            _html_lines = [
                f'<b>Khối ngoại — {sym}</b> <i>(snapshot hôm nay)</i>',
                f'{_room_emoji} Room: {_room_bar} {_room_used:.1f}% đã dùng',
                f'Còn lại: <b>{_snap.get("room_left_pct", round(_m_ratio-_c_ratio,1)):.1f}%</b> room ({_m_ratio:.1f}% - {_c_ratio:.1f}%)',
                f'Tỷ lệ: <b>{_c_ratio:.1f}%</b> / tối đa <b>{_m_ratio:.1f}%</b>',
            ]
            result = {
                'symbol': sym, 'available': True, 'mode': 'snapshot',
                'current_ratio':  _c_ratio,
                'max_ratio':      _m_ratio,
                'room_used_pct':  _room_used,
                'room_left_pct':  _snap.get('room_left_pct', round(_m_ratio - _c_ratio, 1)),
                'label':         _flabel,
                'bonus': 0,
                # Compat fields
                'consecutive_buy': 0, 'consecutive_sell': 0,
                'net_5d': 0, 'net_10d': 0, 'net_ratio': 0,
                'display_html': '\n'.join(_html_lines),
            }
            set_cached(_fkey, result, ttl=900)
            return jsonify(result)

        # ── Cả 2 đều thất bại ───────────────────────────────────────────────
        return jsonify({'symbol': sym, 'available': False,
                        'label': 'Khong co du lieu foreign', 'bonus': 0})

    except Exception as e:
        logger.error(f'api_foreign {sym}: {e}')
        return jsonify({'symbol': sym, 'available': False, 'error': str(e), 'bonus': 0})


@app.route('/api/whatif/<symbol>/<int:target_price>')
def api_whatif(symbol, target_price):
    return jsonify(fetch_analysis(symbol.upper(), price_override=target_price))

@app.route('/api/regime')
def api_regime():
    """Trả về trạng thái Market Regime hiện tại."""
    try:
        from backtest import get_market_regime
        return jsonify(get_market_regime())
    except Exception as e:
        return jsonify({'regime': 'UNKNOWN', 'error': str(e)})


@app.route('/api/market')
def api_market():
    result = {}
    for sym, name in [('VNINDEX', 'VN-INDEX'), ('HNX30', 'HNX-INDEX'), ('VN30F1M', 'VN30')]:
        d = fetch_price(sym)
        result[sym] = dict(d)
        result[sym]['name'] = name
        time.sleep(0.5)
    return jsonify(result)


# ── WATCHLIST: đọc từ config.py (single source of truth) ─────────────────────
# Override qua env var WATCHLIST_SYMBOLS nếu muốn cập nhật không cần deploy
# Format env: "DGC,DCM,MBB,HCM,PC1,FRT,VND,FPT,NKG"
import os as _os
_wl_env = _os.environ.get('WATCHLIST_SYMBOLS', '')
WATCHLIST = (
    [s.strip().upper() for s in _wl_env.split(',') if s.strip()]
    if _wl_env
    else SIGNALS_WATCHLIST
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
        """
        Vòng lặp cache kỹ thuật (1D + 1H) — KHÔNG tính FA.
        FA được tách riêng vào fa_worker để tránh rate limit burst.

        Budget vnstock community: 60 req/phút.
        Mỗi mã cần: 1 req (1D history) + 1 req (1H history) = 2 req.
        9 mã × 2 req = 18 req/vòng.
        Sleep 12s/mã → 9 × 12s = 108s/vòng → ~10 req/phút → an toàn.
        Vòng tiếp theo sleep 180s → tổng chu kỳ ~5 phút/vòng.
        """
        # Delay 30s sau startup — để FA worker chạy trước (ưu tiên BCTC sáng sớm)
        logger.info('Cache worker: waiting 30s before first cycle...')
        time.sleep(30)

        while True:
            for sym in WATCHLIST:
                try:
                    df, source = load_history(sym, days=400)
                    if df is not None:
                        result = compute_indicators(df, symbol=sym)
                        if result and result.get('vol_ma20', 0) > 0:
                            result['symbol'] = sym
                            result['source'] = source
                            try:
                                result['warnings_1h'] = analyze_1h_warnings(sym)
                            except Exception:
                                result['warnings_1h'] = []
                            # Gắn FV từ cache nếu có (không tính lại — FA worker lo)
                            fv_key = 'fv_' + sym
                            with _fa_lock:
                                if fv_key in _fa_cache:
                                    fv_data, _ = _fa_cache[fv_key]
                                    result['fair_value'] = fv_data
                            cache_key = 'analysis_' + sym + '_live'
                            set_cache(cache_key, result)
                            logger.info(sym + ' OK vol_ma20=' + str(int(result['vol_ma20'])))
                        else:
                            logger.warning(sym + ': vol_ma20=0, bo qua cache')
                    # 12s/mã = 5 req/phút cho 9 mã → tổng ~45 req/phút cả hệ thống
                    time.sleep(12)
                except Exception as e:
                    err_str = str(e)
                    if 'rate' in err_str.lower() or 'limit' in err_str.lower() or '429' in err_str:
                        logger.warning(f'Rate limit hit for {sym}, sleeping 60s')
                        time.sleep(60)   # Chờ rate limit reset đủ 1 phút
                    else:
                        logger.warning('cache ' + sym + ': ' + err_str)
            # 180s giữa các vòng → chu kỳ ~5 phút = cân bằng realtime vs rate limit
            time.sleep(180)

    def fa_worker():
        """
        FA warmup riêng biệt — chỉ chạy 1 lần/ngày lúc 8:00 sáng VN.
        Tách khỏi vòng cache chính để không gây rate limit burst.

        Budget: FA cần ~3-5 API calls/mã (balance_q + income_q + overview).
        9 mã × 4 calls = 36 calls — nếu chạy cùng vòng cache là 54 calls/vòng → dễ chạm 60.
        Tách ra và chạy với sleep 15s/mã → 9 × 15s = 135s → an toàn.
        """
        # Startup warmup: delay 90s để cache worker hoàn thành vòng đầu trước
        logger.info('FA worker: waiting 90s before initial warmup...')
        time.sleep(90)
        try:
            warmup_fair_values()
        except Exception as e:
            logger.warning(f'FA initial warmup error: {e}')

        _last_fa_warmup_day = -1

        while True:
            try:
                import pytz as _tz
                from datetime import datetime as _dt
                _now = _dt.now(_tz.timezone('Asia/Ho_Chi_Minh'))
                # Chạy lúc 8:00-8:05 sáng các ngày giao dịch
                if (_now.weekday() < 5
                        and _now.hour == 8 and _now.minute < 5
                        and _last_fa_warmup_day != _now.day):
                    _last_fa_warmup_day = _now.day
                    logger.info('FA daily warmup at 8:00')
                    warmup_fair_values()
            except Exception as e:
                logger.warning(f'FA daily warmup error: {e}')
            # Check mỗi 5 phút — đủ để bắt đúng 8:00-8:05
            time.sleep(300)

    threading.Thread(target=worker,    daemon=True, name='cache-worker').start()
    threading.Thread(target=fa_worker, daemon=True, name='fa-worker').start()
    logger.info('Background cache + FA worker started for ' + str(len(WATCHLIST)) + ' symbols')


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

    # ── Market Breadth ────────────────────────────────────────────────────────
    n_total   = len(results)
    n_buy     = sum(1 for r in results if r.get('action') == 'MUA')
    n_sell    = sum(1 for r in results if r.get('action') == 'BAN')
    n_watch   = n_total - n_buy - n_sell
    n_above50 = sum(1 for r in results if r.get('score', 0) >= 50)
    breadth   = round(n_buy / n_total * 100) if n_total > 0 else 0
    above50pct= round(n_above50 / n_total * 100) if n_total > 0 else 0

    # Trả toàn bộ watchlist + breadth data
    return jsonify({
        'signals': results,
        'breadth': {
            'total': n_total, 'buy': n_buy, 'sell': n_sell, 'watch': n_watch,
            'buy_pct': breadth, 'above50_pct': above50pct,
            'label': ('BULLISH' if breadth >= 60 else
                      'BEARISH' if breadth <= 25 else 'NEUTRAL'),
        }
    })


@app.route('/health')
@app.route('/api/health')
def api_health():
    """
    Health check endpoint cho Railway — phải trả 200 ngay lập tức.
    Không phụ thuộc cache hay database để tránh Railway kill process.
    """
    cached_syms = sum(1 for sym in WATCHLIST 
                     if get_cached('analysis_' + sym + '_live'))
    return jsonify({
        'status':     'ok',
        'service':    'vn-trader-bot',
        'cache_ready': cached_syms,
        'cache_total': len(WATCHLIST),
        'warmed_up':  cached_syms >= max(3, len(WATCHLIST) // 2),
    })


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
    for source in ['VCI', 'KBS']:
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
threading.Thread(target=lambda: (time.sleep(10), start_background_cache()), daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
