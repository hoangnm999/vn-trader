"""
relative_strength.py — Relative Strength vs VNINDEX + 52-week Breakout
=======================================================================
Tính RS cho bất kỳ mã nào so với VNINDEX.

RS là signal mạnh nhất để tìm "market leader" — cổ phiếu
đang được dòng tiền chọn lọc, tăng mạnh hơn thị trường chung.

Tích hợp vào Score A: +5 đến +20đ (RS dương) / -8đ (RS âm)
Breakout 52-week: +10đ bonus
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Cache VNINDEX data để tránh gọi API nhiều lần
_vnindex_cache = {'data': None, 'ts': None}
_CACHE_TTL_SEC = 3600  # 1 giờ


def _load_ohlcv(symbol, days=300):
    """Load OHLCV từ vnstock — dùng chung load_data nếu có."""
    end   = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    for source in ['VCI', 'TCBS']:
        try:
            from vnstock import Vnstock
            df = Vnstock().stock(symbol=symbol, source=source) \
                          .quote.history(start=start, end=end, interval='1D')
            if df is not None and len(df) >= 20:
                return df
        except Exception:
            continue
    return None


def get_vnindex_returns(days=300):
    """
    Lấy chuỗi returns của VNINDEX.
    Cached để tránh gọi nhiều lần trong cùng session.
    """
    global _vnindex_cache
    now = datetime.now().timestamp()

    if (_vnindex_cache['data'] is not None
            and _vnindex_cache['ts']
            and now - _vnindex_cache['ts'] < _CACHE_TTL_SEC):
        return _vnindex_cache['data']

    df = _load_ohlcv('VNINDEX', days=days)
    if df is None:
        return None

    cc = next((c for c in df.columns if c.lower() in
               ('close', 'closeprice', 'close_price')), None)
    if cc is None:
        return None

    closes = pd.to_numeric(df[cc], errors='coerce').fillna(0).values
    if closes.max() < 100:
        closes = closes * 1000

    _vnindex_cache['data'] = closes
    _vnindex_cache['ts']   = now
    return closes


def calc_rs(stock_closes, vnindex_closes, period):
    """
    Tính Relative Strength so với VNINDEX.
    RS = stock_return% - vnindex_return%
    """
    n_s = len(stock_closes)
    n_v = len(vnindex_closes)
    if n_s < period or n_v < period:
        return None

    stock_ret  = (stock_closes[-1] - stock_closes[-period]) / stock_closes[-period] * 100
    vni_ret    = (vnindex_closes[-1] - vnindex_closes[-period]) / vnindex_closes[-period] * 100
    return round(stock_ret - vni_ret, 2)


def calc_rs_signals(closes, highs, symbol=None):
    """
    Tính đầy đủ RS signals + 52-week breakout cho 1 mã.

    Params:
        closes : numpy array giá đóng cửa của mã
        highs  : numpy array giá cao nhất của mã
        symbol : tên mã (để log)

    Returns:
        dict với các keys:
          rs_5d, rs_20d, rs_60d  : RS theo khung thời gian
          rs_score               : điểm RS cho Score A (-8 đến +20)
          breakout_52w           : True/False
          breakout_60d           : True/False
          breakout_score         : điểm breakout (0/5/10)
          total_bonus            : tổng điểm cộng vào Score A
          rs_label               : mô tả ngắn
          rs_emoji               : emoji trạng thái
    """
    result = {
        'rs_5d': None, 'rs_20d': None, 'rs_60d': None,
        'rs_score': 0, 'breakout_52w': False, 'breakout_60d': False,
        'breakout_score': 0, 'total_bonus': 0,
        'rs_label': 'Khong co du lieu VNINDEX',
        'rs_emoji': '⬜',
        'available': False,
    }

    # Lấy VNINDEX
    vni = get_vnindex_returns(days=300)
    if vni is None:
        return result

    result['available'] = True

    # Align độ dài — dùng min
    min_len = min(len(closes), len(vni))
    c  = closes[-min_len:]
    vi = vni[-min_len:]

    rs_score = 0

    # ── RS theo 3 khung ──────────────────────────────────────────────────────
    rs5  = calc_rs(c, vi, 5)
    rs20 = calc_rs(c, vi, 20)
    rs60 = calc_rs(c, vi, 60)

    result['rs_5d']  = rs5
    result['rs_20d'] = rs20
    result['rs_60d'] = rs60

    # Điểm RS 5d: momentum ngắn hạn đang accelerate
    if rs5 is not None:
        if   rs5 >  5: rs_score += 5
        elif rs5 >  2: rs_score += 3
        elif rs5 < -3: rs_score -= 3

    # Điểm RS 20d: đang dẫn dắt hay lag thị trường
    if rs20 is not None:
        if   rs20 > 10: rs_score += 10
        elif rs20 >  5: rs_score +=  8
        elif rs20 >  2: rs_score +=  5
        elif rs20 > -2: rs_score +=  0
        elif rs20 > -5: rs_score -=  4
        else:           rs_score -=  8

    # Điểm RS 60d: trend trung hạn
    if rs60 is not None:
        if   rs60 > 15: rs_score += 7
        elif rs60 > 10: rs_score += 5
        elif rs60 >  5: rs_score += 3

    rs_score = max(-10, min(22, rs_score))
    result['rs_score'] = rs_score

    # ── 52-week + 60d Breakout ────────────────────────────────────────────────
    breakout_score = 0
    price_now = closes[-1]

    if len(highs) >= 252:
        high_52w = float(np.max(highs[-252:-1]))  # Không tính hôm nay
        if price_now >= high_52w * 0.99:           # Tolerance 1%
            result['breakout_52w'] = True
            breakout_score = 10

    if len(highs) >= 60 and not result['breakout_52w']:
        high_60d = float(np.max(highs[-60:-1]))
        if price_now >= high_60d * 0.99:
            result['breakout_60d'] = True
            breakout_score = 5

    result['breakout_score'] = breakout_score
    result['total_bonus']    = rs_score + breakout_score

    # ── Label + Emoji ─────────────────────────────────────────────────────────
    if rs20 is not None:
        if rs20 > 10:
            emoji, lbl = '🚀', f'Dan dau TT manh (RS20={rs20:+.1f}%)'
        elif rs20 > 5:
            emoji, lbl = '📈', f'Beat VNINDEX (RS20={rs20:+.1f}%)'
        elif rs20 > 0:
            emoji, lbl = '➡️',  f'Nhi thi truong (RS20={rs20:+.1f}%)'
        elif rs20 > -5:
            emoji, lbl = '📉', f'Yeu hon TT (RS20={rs20:+.1f}%)'
        else:
            emoji, lbl = '⬇️',  f'Lag nang (RS20={rs20:+.1f}%)'
    else:
        emoji, lbl = '⬜', 'Khong du du lieu'

    if result['breakout_52w']:
        lbl += ' | PHÁ ĐỈNH 52 TUẦN 🎯'
        emoji = '🎯'
    elif result['breakout_60d']:
        lbl += ' | Breakout 60d'

    result['rs_label'] = lbl
    result['rs_emoji'] = emoji
    return result


def format_rs_msg(rs_data):
    """Format RS signal thành HTML cho Telegram."""
    NL = chr(10)
    if not rs_data.get('available'):
        return '⬜ Khong co du lieu RS (VNINDEX unavailable)'

    rs20 = rs_data.get('rs_20d')
    rs5  = rs_data.get('rs_5d')
    rs60 = rs_data.get('rs_60d')
    em   = rs_data.get('rs_emoji', '')
    lbl  = rs_data.get('rs_label', '')
    bon  = rs_data.get('total_bonus', 0)
    b52  = rs_data.get('breakout_52w', False)
    b60  = rs_data.get('breakout_60d', False)

    lines = [f'{em} <b>Relative Strength vs VNINDEX:</b>']
    if rs5  is not None: lines.append(f'  RS  5 phien: {rs5:+.1f}%')
    if rs20 is not None: lines.append(f'  RS 20 phien: <b>{rs20:+.1f}%</b>')
    if rs60 is not None: lines.append(f'  RS 60 phien: {rs60:+.1f}%')
    if b52:
        lines.append('  🎯 <b>PHÁ ĐỈNH 52 TUẦN — Momentum cực mạnh!</b>')
    elif b60:
        lines.append('  📊 Breakout đỉnh 60 ngày')
    sign = '+' if bon >= 0 else ''
    lines.append(f'  → Dieu chinh Score: <b>{sign}{bon}d</b>')
    return NL.join(lines)
