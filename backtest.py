"""
backtest.py - Kiểm chứng tín hiệu VN Trader Bot — Báo cáo chi tiết theo năm
=============================================================================
Cách dùng:
    python backtest.py VCB              # 1 mã — báo cáo đầy đủ 3 chiều + theo năm
    python backtest.py VCB HPG FPT      # nhiều mã + bảng chéo theo năm
    python backtest.py --all            # toàn bộ 28 mã watchlist

Phân tích 3 chiều:
    [1] Theo năm         -> TP/SL/Expired/WR/PnL từng năm + heatmap ASCII
    [2] Tối ưu ngưỡng    -> ngưỡng MUA tốt nhất (65/68/70/72/75/78/80)
    [3] Kết luận chéo    -> phân biệt "bot tốt" vs "mã tốt" vs "2021 ảo"

Phân tích bổ sung:
    [A] Khoảng tin cậy 95% (Wilson CI)
    [B] Bull Bias Detector — 2021 thổi phồng bao nhiêu %?
    [C] Đặc tính mã — kỹ thuật rõ vs phi kỹ thuật
    [D] Cảnh báo thanh khoản

Cấu hình: 5 năm | SL=-7% | TP=+14% | Giữ tối đa 10 phiên
Quy tắc T+3: mua ngày T, CP về TK ngày T+3, chỉ bán được từ T+3 trở đi.
"""

import sys
import math
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ─── Cấu hình ────────────────────────────────────────────────────────────────
HOLD_DAYS        = 10
SETTLEMENT_DAYS  = 3       # Quy tắc VN: mua T, CP về T+3, chỉ bán từ T+3
STOP_LOSS        = -0.07   # Default toàn hệ thống
TAKE_PROFIT      = 0.14    # Default toàn hệ thống
MIN_SCORE_BUY    = 65
MAX_SCORE_SELL   = 35
LOOKBACK_DAYS    = 2555    # 7 năm (7 x 365)

# ─── Cấu hình SL/TP riêng theo từng mã ───────────────────────────────────────
# Override default khi backtest. Thêm mã mới bất kỳ lúc nào.
# sl/tp là số dương: sl=0.05 = cắt lỗ -5%, tp=0.09 = chốt lời +9%
SYMBOL_CONFIG = {
    # Bộ A — Bluechip/Tăng trưởng (SL=5% TP=9%)
    'VCB': {'sl': 0.05, 'tp': 0.09, 'min_score': 80},
    'FPT': {'sl': 0.05, 'tp': 0.09, 'min_score': 70},
    'BID': {'sl': 0.05, 'tp': 0.09, 'min_score': 80},
    'MBB': {'sl': 0.05, 'tp': 0.09, 'min_score': 70},
    'SSI': {'sl': 0.05, 'tp': 0.09, 'min_score': 70},
    'HCM': {'sl': 0.05, 'tp': 0.09, 'min_score': 75},
    'VND': {'sl': 0.05, 'tp': 0.09, 'min_score': 75},
    # Bộ B — Cyclical/Mid-cap (SL=7% TP=14%)
    'DCM': {'sl': 0.07, 'tp': 0.14, 'min_score': 65},
    'DGC': {'sl': 0.07, 'tp': 0.14, 'min_score': 65},
    'GAS': {'sl': 0.07, 'tp': 0.14, 'min_score': 65},
    'SZC': {'sl': 0.07, 'tp': 0.14, 'min_score': 65},
    'PC1': {'sl': 0.07, 'tp': 0.14, 'min_score': 65},
    'HSG': {'sl': 0.07, 'tp': 0.14, 'min_score': 70},
    'KDH': {'sl': 0.07, 'tp': 0.14, 'min_score': 80},
    'PDR': {'sl': 0.07, 'tp': 0.14, 'min_score': 65},
    'NVL': {'sl': 0.07, 'tp': 0.14, 'min_score': 70},
    'PVS': {'sl': 0.07, 'tp': 0.14, 'min_score': 70},
    'POW': {'sl': 0.07, 'tp': 0.14, 'min_score': 80},
    'NT2': {'sl': 0.05, 'tp': 0.09, 'min_score': 80},
}

SCORE_THRESHOLDS = [60, 65, 68, 70, 72, 75, 78, 80]
MIN_LIQUIDITY_VOL = 500_000  # cp/ngày — dưới mức này tín hiệu volume kém tin cậy

MARKET_PHASES = {
    2020: 'Covid Crash → Phục hồi',
    2021: 'Bull Run lịch sử (+130%)',
    2022: 'Bear Market (-50%)',
    2023: 'Phục hồi sideway',
    2024: 'Tăng trưởng ổn định',
    2025: 'Biến động địa chính trị',
}

# Phân loại đặc tính mã
SYMBOL_PROFILE = {
    'technical_strong': {
        'VCB', 'BID', 'TCB', 'MBB', 'VPB',
        'FPT', 'CMG', 'SSI', 'VND', 'HCM',
        'HPG', 'HSG', 'MWG', 'FRT', 'REE',
    },
    'external_driven': {
        'NVL': 'phap_ly', 'PDR': 'phap_ly', 'KBC': 'phap_ly',
        'GAS': 'gia_dau',  'PVD': 'gia_dau', 'PVS': 'gia_dau',
        'VNM': 'phong_thu', 'MSN': 'phong_thu',
    },
    'mixed': {'VHM', 'VIC', 'NKG', 'POW', 'SZC'},
}

WATCHLIST = [
    'VCB', 'BID', 'TCB', 'MBB', 'VPB',
    'VHM', 'VIC', 'NVL', 'PDR',
    'FPT', 'CMG',
    'HPG', 'HSG', 'NKG',
    'SSI', 'VND', 'HCM',
    'GAS', 'PVD', 'PVS',
    'MWG', 'FRT',
    'VNM', 'MSN',
    'POW', 'REE',
    'KBC', 'SZC',
]


# ─── Chỉ báo kỹ thuật (mirror app.py) ────────────────────────────────────────

def find_col(df, names):
    for c in df.columns:
        if c.lower() in names:
            return c
    return None

def ema_arr(arr, span):
    alpha = 2.0 / (span + 1)
    out = np.zeros(len(arr))
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = alpha * arr[i] + (1 - alpha) * out[i - 1]
    return out

def calc_rsi_wilder(arr, p=14):
    out = np.full(len(arr), 50.0)
    if len(arr) < p + 1:
        return out
    deltas   = np.diff(arr)
    gains    = np.where(deltas > 0, deltas, 0.0)
    losses   = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = np.mean(gains[:p])
    avg_loss = np.mean(losses[:p])
    out[p]   = 100.0 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)
    for i in range(p, len(deltas)):
        avg_gain = (avg_gain * (p - 1) + gains[i]) / p
        avg_loss = (avg_loss * (p - 1) + losses[i]) / p
        out[i + 1] = 100.0 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)
    return np.round(out, 1)

def compute_score_at(closes, highs, lows, volumes, idx):
    # Cần tối thiểu 200 nến để tính MA200 cho weekly trend
    if idx < 200:
        return 50, 'THEO DOI'
    c = closes[:idx + 1]
    h = highs[:idx + 1]
    l = lows[:idx + 1]
    v = volumes[:idx + 1]
    price      = float(c[-1])
    prev_close = float(c[-2]) if len(c) > 1 else price
    rsi_series = calc_rsi_wilder(c)
    rsi_val    = float(rsi_series[-1])
    e12 = ema_arr(c, 12); e26 = ema_arr(c, 26)
    macd_line = e12 - e26; sig_line = ema_arr(macd_line, 9)
    macd_hist_arr = macd_line - sig_line
    macd_h = float(macd_hist_arr[-1])
    macd_v = float(macd_line[-1]); macd_s = float(sig_line[-1])
    ma10      = float(np.mean(c[-10:])) if len(c) >= 10 else float(np.mean(c))
    ma10_prev = float(np.mean(c[-11:-1])) if len(c) >= 11 else ma10
    ma20      = float(np.mean(c[-20:]))
    ma50      = float(np.mean(c[-min(50, len(c)):]))
    ma20_prev = float(np.mean(c[-21:-1])) if len(c) >= 21 else ma20
    ma50_prev = float(np.mean(c[-51:-1])) if len(c) >= 51 else ma50
    golden_cross = ma20_prev < ma50_prev and ma20 > ma50
    death_cross  = ma20_prev > ma50_prev and ma20 < ma50

    # ── Weekly Trend (MA100 ≈ MA10W, MA200 ≈ MA20W) ──────────────────────────
    ma100 = float(np.mean(c[-100:])) if len(c) >= 100 else float(np.mean(c))
    ma200 = float(np.mean(c[-200:])) if len(c) >= 200 else ma100
    if price > ma100 and ma100 > ma200:   weekly_trend = 'STRONG_UP'
    elif price > ma100:                    weekly_trend = 'UP'
    elif price > ma200:                    weekly_trend = 'WEAK_UP'
    elif ma100 > ma200:                    weekly_trend = 'PULLBACK'
    else:                                  weekly_trend = 'DOWN'

    vol_history = v[:-1] if len(v) > 1 else v
    valid_vols  = vol_history[vol_history > 0]
    if len(valid_vols) >= 5:
        vol_ma20 = float(np.mean(valid_vols[-20:] if len(valid_vols) >= 20 else valid_vols))
    else:
        vol_ma20 = float(np.mean(v[v > 0])) if np.any(v > 0) else 0.0
    vol_ratio = float(v[-1]) / vol_ma20 if vol_ma20 > 0 else 1.0
    price_up  = price >= prev_close
    if   vol_ratio >= 1.5 and price_up:      vol_signal = 'shark_buy'
    elif vol_ratio >= 1.5 and not price_up:  vol_signal = 'shark_sell'
    elif vol_ratio < 0.7  and price_up:      vol_signal = 'fake_rally'
    elif vol_ratio >= 1.0 and price_up:      vol_signal = 'normal_buy'
    elif vol_ratio >= 0.7 and price_up:      vol_signal = 'weak_buy'    # Fix: 0.7-1.0x + tăng
    elif vol_ratio < 0.7  and not price_up:  vol_signal = 'weak_sell'
    elif vol_ratio >= 0.7 and not price_up:  vol_signal = 'normal_sell'
    else:                                    vol_signal = 'normal'

    # ── RSI Divergence dùng highs/lows (Fix: không dùng closes) ─────────────
    def detect_div(pc, hc, lc, rc, lookback=40):
        if len(pc) < lookback: return 'none'
        h2 = hc[-lookback:]; l2 = lc[-lookback:]; r2 = rc[-lookback:]
        tops    = [i for i in range(2, len(h2)-2)
                   if h2[i] >= h2[i-1] and h2[i] >= h2[i-2]
                   and h2[i] >= h2[i+1] and h2[i] >= h2[i+2]]
        bottoms = [i for i in range(2, len(l2)-2)
                   if l2[i] <= l2[i-1] and l2[i] <= l2[i-2]
                   and l2[i] <= l2[i+1] and l2[i] <= l2[i+2]]
        if len(tops) >= 2:
            t1, t2 = tops[-2], tops[-1]
            if h2[t2] > h2[t1] and r2[t2] < r2[t1] - 2: return 'bearish'
        if len(bottoms) >= 2:
            b1, b2 = bottoms[-2], bottoms[-1]
            if l2[b2] < l2[b1] and r2[b2] > r2[b1] + 2: return 'bullish'
        return 'none'

    div_type = detect_div(c, h, l, rsi_series)
    tenkan       = (np.max(h[-9:])  + np.min(l[-9:]))  / 2 if len(h) >= 9  else price
    kijun        = (np.max(h[-26:]) + np.min(l[-26:])) / 2 if len(h) >= 26 else price
    span_b       = (np.max(h[-52:]) + np.min(l[-52:])) / 2 if len(h) >= 52 else price
    cloud_top    = max(float((tenkan + kijun) / 2), float(span_b))
    cloud_bottom = min(float((tenkan + kijun) / 2), float(span_b))
    bb_std = float(np.std(c[-20:]))
    bb_mid = float(np.mean(c[-20:]))
    bb_upper = bb_mid + 2 * bb_std; bb_lower = bb_mid - 2 * bb_std

    # ── Support/Resistance ───────────────────────────────────────────────────
    def find_sr_bt(hh, ll, price_now, window=6):
        """S/R với tolerance 0.1% — đồng bộ app.py."""
        def _calc(hw, lw, win, min_count):
            levels = []
            hw2 = hw[-120:] if len(hw) > 120 else hw
            lw2 = lw[-120:] if len(lw) > 120 else lw
            tol = 0.001
            for ii in range(win, len(hw2) - win):
                lmax = max(hw2[ii-win:ii+win+1])
                lmin = min(lw2[ii-win:ii+win+1])
                if hw2[ii] >= lmax * (1 - tol): levels.append(('R', float(hw2[ii])))
                if lw2[ii] <= lmin * (1 + tol): levels.append(('S', float(lw2[ii])))
            merged = []
            levels.sort(key=lambda x: x[1])
            for typ, lvl in levels:
                found = False
                for m in merged:
                    if abs(m['price'] - lvl) / lvl < 0.015:
                        m['count'] += 1; found = True; break
                if not found: merged.append({'type': typ, 'price': lvl, 'count': 1})
            strong = [m for m in merged if m['count'] >= min_count]
            strong.sort(key=lambda x: x['count'], reverse=True)
            sups = sorted([m for m in strong if m['price'] < price_now], key=lambda x: x['price'], reverse=True)[:3]
            ress = sorted([m for m in strong if m['price'] > price_now], key=lambda x: x['price'])[:3]
            return sups, ress
        s, r = _calc(hh, ll, 6, 3)
        if not s or not r:
            s2, r2 = _calc(hh, ll, 4, 2)
            if not s: s = s2
            if not r: r = r2
        return s, r

    supports, resistances = find_sr_bt(h, l, price)

    # ── Tenkan/Kijun cross — với khoảng cách tối thiểu 0.3% ─────────────────
    tenkan_prev = (np.max(h[-10:-1]) + np.min(l[-10:-1])) / 2 if len(h) >= 10 else tenkan
    kijun_prev  = (np.max(h[-27:-1]) + np.min(l[-27:-1])) / 2 if len(h) >= 27 else kijun
    min_cross_gap = float(kijun) * 0.003
    tk_cross_bull = (float(tenkan_prev) < float(kijun_prev)
                     and float(tenkan) > float(kijun)
                     and (float(tenkan) - float(kijun)) >= min_cross_gap)
    tk_cross_bear = (float(tenkan_prev) > float(kijun_prev)
                     and float(tenkan) < float(kijun)
                     and (float(kijun) - float(tenkan)) >= min_cross_gap)

    # ── Tính điểm (đồng bộ app.py) ──────────────────────────────────────────
    score = 50

    # VOL: ±20
    if   vol_signal == 'shark_buy':   score += 20
    elif vol_signal == 'shark_sell':  score -= 20
    elif vol_signal == 'fake_rally':  score -= 12
    elif vol_signal == 'normal_buy':  score += 8
    elif vol_signal == 'weak_buy':    score += 3   # Fix: xác nhận yếu
    elif vol_signal == 'weak_sell':   score += 3
    elif vol_signal == 'normal_sell': score -= 5

    # RSI: ±20
    if   rsi_val < 30: score += 20
    elif rsi_val < 40: score += 10
    elif rsi_val > 70: score -= 20
    elif rsi_val > 60: score -= 10

    # Divergence: ±15
    if div_type == 'bullish':  score += 15 if rsi_val < 35 else 10
    elif div_type == 'bearish': score -= 15 if rsi_val > 65 else 10

    # MA: ±20
    if   golden_cross:                 score += 20
    elif death_cross:                  score -= 20
    elif price > ma20 and ma20 > ma50: score += 15
    elif price > ma20:                 score += 10
    elif price < ma20 and ma20 < ma50: score -= 15
    else:                              score -= 10

    # MACD: ±8 (bao gồm histogram momentum)
    if macd_v > macd_s and macd_h > 0:
        score += 5
        if len(macd_hist_arr) >= 4 and float(macd_hist_arr[-1]) > float(macd_hist_arr[-3]):
            score += 3
    elif macd_v < macd_s and macd_h < 0:
        score -= 5
        if len(macd_hist_arr) >= 4 and float(macd_hist_arr[-1]) < float(macd_hist_arr[-3]):
            score -= 3

    # Ichimoku vị trí vs mây: ±5
    if   price > cloud_top:    score += 5
    elif price < cloud_bottom: score -= 5

    # Tenkan/Kijun cross: ±5
    if len(h) >= 27:
        if   tk_cross_bull: score += 5
        elif tk_cross_bear: score -= 5
        elif float(tenkan) > float(kijun): score += 2
        else:                              score -= 2

    # BB: ±3
    if   price <= bb_lower: score += 3
    elif price >= bb_upper: score -= 3

    # S/R: ±12
    if supports:
        dist_s = (price - supports[0]['price']) / price * 100
        if   dist_s < 1.5: score += min(12, 6 + supports[0]['count'] * 2)
        elif dist_s < 4:   score += 5
    if resistances:
        dist_r = (resistances[0]['price'] - price) / price * 100
        if   dist_r < 1.5: score -= min(12, 6 + resistances[0]['count'] * 2)
        elif dist_r < 4:   score -= 5

    # Three-in-one: +8
    three_in_one = (price > ma20 and vol_ratio >= 1.5 and price_up and 30 < rsi_val < 70)
    if three_in_one: score += 8

    # Weekly trend: ±10
    if   weekly_trend == 'STRONG_UP': score += 10
    elif weekly_trend == 'UP':        score += 5
    elif weekly_trend == 'WEAK_UP':   score += 2
    elif weekly_trend == 'PULLBACK':  score -= 5
    elif weekly_trend == 'DOWN':      score -= 10

    score = max(0, min(100, score))

    # Hard filter MA20 (đồng bộ app.py)
    ma20_dist = (ma20 - price) / ma20 if ma20 > 0 else 0.0
    if price < ma20 and ma20 < ma50:
        dcb = (ma20_dist >= 0.15 and rsi_val < 25 and vol_signal == 'weak_sell')
        score = min(score, 60 if dcb else 55)
    elif price < ma20:
        score = min(score, 68)

    # Weekly downtrend hard cap
    if weekly_trend == 'DOWN':
        score = min(score, 58)

    if   score >= MIN_SCORE_BUY:  action = 'MUA'
    elif score <= MAX_SCORE_SELL: action = 'BAN'
    else:                         action = 'THEO DOI'
    return score, action


# ─── Mô phỏng giao dịch ──────────────────────────────────────────────────────

def simulate_trade(closes, entry_idx, direction='MUA', sl=None, tp=None):
    """
    sl/tp là số dương, vd sl=0.05 = cắt lỗ -5%, tp=0.09 = chốt lời +9%.

    Quy tắc T+3 TTCK Việt Nam:
      - Mua ngày T (entry_idx): tiền trừ ngay, nhưng CP chưa về TK.
      - T+1, T+2: không thể bán (CP chưa về).
      - T+3 (d = SETTLEMENT_DAYS): CP về TK, bắt đầu theo dõi SL/TP.
      - Tổng thời gian nắm giữ thực tế: SETTLEMENT_DAYS + HOLD_DAYS phiên.
    """
    _sl = -(sl if sl is not None else abs(STOP_LOSS))
    _tp =   tp if tp is not None else TAKE_PROFIT
    entry_price = closes[entry_idx]

    # Bắt đầu kiểm tra SL/TP từ T+3 (d=SETTLEMENT_DAYS),
    # kết thúc sau HOLD_DAYS phiên có thể giao dịch.
    total_days = SETTLEMENT_DAYS + HOLD_DAYS
    for d in range(SETTLEMENT_DAYS, total_days + 1):
        if entry_idx + d >= len(closes): break
        current = closes[entry_idx + d]
        pnl = (current - entry_price) / entry_price
        if direction == 'MUA':
            if pnl <= _sl: return round(pnl * 100, 2), 'SL',    d
            if pnl >= _tp: return round(pnl * 100, 2), 'TP',    d
        else:
            if pnl >= 0.07:  return round(-pnl * 100, 2), 'WRONG',  d
            if pnl <= -0.07: return round(-pnl * 100, 2), 'RIGHT',  d

    # Hết kỳ: lấy giá tại ngày cuối cùng có thể bán
    final = closes[min(entry_idx + total_days, len(closes) - 1)]
    pnl   = (final - entry_price) / entry_price
    return (round(pnl * 100, 2), 'EXPIRED', total_days) if direction == 'MUA' \
           else (round(-pnl * 100, 2), 'EXPIRED', total_days)


# ─── Tải dữ liệu ──────────────────────────────────────────────────────────────

def load_data(symbol, days=LOOKBACK_DAYS):
    end   = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    for source in ['VCI', 'TCBS']:
        try:
            from vnstock import Vnstock
            df = Vnstock().stock(symbol=symbol, source=source).quote.history(
                start=start, end=end, interval='1D')
            if df is not None and len(df) >= 120:
                return df, source
        except Exception as e:
            print(f"  [{symbol}/{source}] lỗi: {e}")
    return None, None


# ─── Thống kê cơ bản ──────────────────────────────────────────────────────────

def calc_stats(subset):
    if len(subset) == 0:
        return {}
    wins   = subset[subset['pnl'] > 0]
    losses = subset[subset['pnl'] <= 0]
    wr     = len(wins) / len(subset) * 100
    pf_den = abs(losses['pnl'].sum())
    pf     = abs(wins['pnl'].sum()) / pf_den if pf_den > 0 else float('inf')
    return {
        'total':         len(subset),
        'win_rate':      round(wr, 1),
        'avg_pnl':       round(subset['pnl'].mean(), 2),
        'avg_win':       round(wins['pnl'].mean(),   2) if len(wins)   > 0 else 0.0,
        'avg_loss':      round(losses['pnl'].mean(), 2) if len(losses) > 0 else 0.0,
        'profit_factor': round(pf, 2) if pf != float('inf') else float('inf'),
        'avg_days':      round(subset['days'].mean(), 1),
        'tp':            int(len(subset[subset['reason'] == 'TP'])),
        'sl':            int(len(subset[subset['reason'] == 'SL'])),
        'expired':       int(len(subset[subset['reason'] == 'EXPIRED'])),
        'sum_pnl':       round(subset['pnl'].sum(), 2),
    }


# ─── WR bar mini (heatmap ASCII) ─────────────────────────────────────────────

def wr_bar(wr, width=10):
    """Thanh màu ASCII biểu thị win rate: đỏ < 45, vàng < 55, xanh >= 55."""
    filled = round(wr / 100 * width)
    bar    = '█' * filled + '░' * (width - filled)
    if   wr >= 60: symbol = 'V'
    elif wr >= 55: symbol = '~'
    elif wr >= 45: symbol = '-'
    else:          symbol = 'X'
    return f"{bar} {symbol}"


# ─── PHÂN TÍCH THEO NĂM (CHI TIẾT) ───────────────────────────────────────────

def analyze_by_year_detailed(buy_trades, symbol='', verbose=True):
    """
    Bảng chi tiết từng năm:
    Năm | Lệnh | Win% | Bar | PnL TB | TP | SL | Expired | Tổng PnL | Giai đoạn
    Kèm phát hiện: bull bias, tính nhất quán, năm tốt/xấu.
    """
    if buy_trades.empty:
        return {}

    buy_trades = buy_trades.copy()
    # Parse year an toàn: bỏ qua các giá trị không phải ngày (vd: 'unknown_78')
    def _safe_year(d):
        try:
            return pd.to_datetime(d).year
        except Exception:
            return 0
    buy_trades['year'] = buy_trades['date'].apply(_safe_year)
    buy_trades = buy_trades[buy_trades['year'] > 0]  # loại dòng không parse được
    years = sorted(buy_trades['year'].unique())

    yearly = {}
    for yr in years:
        sub = buy_trades[buy_trades['year'] == yr]
        if len(sub) == 0:
            continue
        st = calc_stats(sub)
        st['phase'] = MARKET_PHASES.get(yr, '---')
        yearly[yr]  = st

    if not verbose or not yearly:
        good = [y for y, d in yearly.items() if d['win_rate'] >= 55 and d['avg_pnl'] > 0]
        bad  = [y for y, d in yearly.items() if d['win_rate'] < 45  or  d['avg_pnl'] < -1]
        return {'yearly': yearly, 'good_years': good, 'bad_years': bad}

    title = f"  CHIỀU 1 — KẾT QUẢ THEO TỪNG NĂM"
    if symbol:
        title += f"  [{symbol}]"
    print(f"\n  {'─'*78}")
    print(title)
    print(f"  {'─'*78}")

    # Header
    print(f"  {'Năm':>4} │ {'Lệnh':>5} │ {'Win%':>5} │ {'WR Bar':^14} │ "
          f"{'PnL TB':>7} │ {'TP':>3} │ {'SL':>3} │ {'HK':>3} │ {'Σ PnL':>7} │ Giai đoạn")
    print(f"  {'─'*4}─┼─{'─'*5}─┼─{'─'*5}─┼─{'─'*14}─┼─"
          f"{'─'*7}─┼─{'─'*3}─┼─{'─'*3}─┼─{'─'*3}─┼─{'─'*7}─┼─{'─'*26}")

    total_tp = total_sl = total_exp = total_lenh = 0
    total_pnl_sum = 0.0

    for yr in years:
        d = yearly[yr]
        bar_str = wr_bar(d['win_rate'])
        # Flag trực quan
        if   d['win_rate'] >= 60 and d['avg_pnl'] >= 2: flag = ' ✓'
        elif d['win_rate'] >= 55:                        flag = ''
        elif d['win_rate'] < 45 or d['avg_pnl'] < -2:   flag = ' ✗'
        else:                                            flag = ' ·'

        print(f"  {yr:>4} │ {d['total']:>5} │ {d['win_rate']:>4.1f}% │ "
              f"{bar_str:<14} │ {d['avg_pnl']:>+6.2f}% │ "
              f"{d['tp']:>3} │ {d['sl']:>3} │ {d['expired']:>3} │ "
              f"{d['sum_pnl']:>+6.1f}% │ {d['phase']}{flag}")

        total_lenh   += d['total']
        total_tp     += d['tp']
        total_sl     += d['sl']
        total_exp    += d['expired']
        total_pnl_sum += d['sum_pnl']

    # Dòng tổng
    all_stats = calc_stats(buy_trades)
    bar_total = wr_bar(all_stats['win_rate'])
    print(f"  {'─'*4}─┼─{'─'*5}─┼─{'─'*5}─┼─{'─'*14}─┼─"
          f"{'─'*7}─┼─{'─'*3}─┼─{'─'*3}─┼─{'─'*3}─┼─{'─'*7}─┼─{'─'*26}")
    print(f"  {'TỔNG':>4} │ {total_lenh:>5} │ {all_stats['win_rate']:>4.1f}% │ "
          f"{bar_total:<14} │ {all_stats['avg_pnl']:>+6.2f}% │ "
          f"{total_tp:>3} │ {total_sl:>3} │ {total_exp:>3} │ "
          f"{total_pnl_sum:>+6.1f}% │")

    print(f"\n  Chú thích cột: TP=chốt lời | SL=cắt lỗ | HK=hết kỳ | Σ PnL=tổng lãi/lỗ")
    print(f"  Bar: ✓ WR≥60%+  ·  ~ WR≥55%  ·  - WR≥45%  ·  ✗ WR<45%")

    # ── Nhận xét phân tích ──
    good_years = [y for y, d in yearly.items() if d['win_rate'] >= 55 and d['avg_pnl'] > 0]
    bad_years  = [y for y, d in yearly.items() if d['win_rate'] < 45  or  d['avg_pnl'] < -1]
    total_yrs  = len(yearly)

    print(f"\n  Năm tốt  ({len(good_years)}/{total_yrs}): "
          f"{', '.join(str(y) for y in good_years) if good_years else 'Không có'}")
    print(f"  Năm xấu  ({len(bad_years)}/{total_yrs}): "
          f"{', '.join(str(y) for y in bad_years)  if bad_years  else 'Không có'}")

    # ── Bull Bias inline ──
    d2021 = yearly.get(2021, {})
    others = {y: d for y, d in yearly.items() if y != 2021}
    if d2021 and others:
        wr_2021      = d2021['win_rate']
        avg_others   = sum(d['win_rate'] for d in others.values()) / len(others)
        gap          = wr_2021 - avg_others
        w2021_pct    = d2021['total'] / total_lenh * 100 if total_lenh > 0 else 0
        print(f"\n  Bull Bias 2021: WR={wr_2021:.1f}% vs TB các năm khác={avg_others:.1f}% "
              f"(chênh {gap:+.1f}%, chiếm {w2021_pct:.0f}% lệnh)")
        if gap > 25 and w2021_pct > 30:
            bias = 'NGHIÊM TRỌNG'
            note = '→ Kết quả tổng thể bị thổi phồng đáng kể bởi bull run lịch sử'
        elif gap > 15:
            bias = 'TRUNG BÌNH'
            note = '→ Xem kết quả 2022-2025 để đánh giá khả năng thực của bot'
        elif gap > 5:
            bias = 'NHẸ'
            note = '→ Chấp nhận được, bull market tự nhiên dễ giao dịch hơn'
        else:
            bias = 'KHÔNG'
            note = '→ Bot nhất quán, 2021 không vượt trội bất thường'
        print(f"  Bull Bias: {bias}  {note}")
    else:
        bias = 'N/A'

    # ── Nhận xét nhất quán ──
    if len(good_years) >= total_yrs * 0.7:
        consistency = 'NHẤT QUÁN — bot hoạt động tốt qua nhiều giai đoạn thị trường'
    elif 2021 in good_years and len(good_years) <= 2:
        consistency = 'CẢNH BÁO — chủ yếu tốt năm 2021, cần kiểm chứng thêm'
    elif len(bad_years) >= total_yrs * 0.5:
        consistency = 'YẾU — nhiều năm thua lỗ, bot chưa ổn định'
    else:
        consistency = 'TRUNG BÌNH — tốt một số năm nhưng chưa nhất quán'
    print(f"  Tính nhất quán: {consistency}")

    # ── Phân tích SL/TP ratio theo năm ──
    print(f"\n  Tỉ lệ TP/SL theo năm (phát hiện giai đoạn bot bị kẹp):")
    print(f"  {'Năm':>4} │ TP:SL │ % Hết kỳ │ Nhận xét")
    print(f"  {'─'*52}")
    for yr in years:
        d = yearly[yr]
        if d['total'] == 0:
            continue
        ratio_str = f"{d['tp']}:{d['sl']}"
        exp_pct   = d['expired'] / d['total'] * 100
        if d['sl'] == 0 and d['tp'] > 0:
            note = '✓ Không bị cắt lỗ'
        elif d['tp'] > 0 and d['sl'] > 0 and d['tp'] / d['sl'] >= 2:
            note = '✓ TP gấp đôi SL'
        elif d['sl'] > d['tp']:
            note = '✗ SL > TP — bot bị sai hướng nhiều'
        elif exp_pct > 50:
            note = '· Nhiều lệnh hết kỳ — tín hiệu yếu, giá không đi rõ'
        else:
            note = ''
        print(f"  {yr:>4} │ {ratio_str:>5} │ {exp_pct:>7.0f}%  │ {note}")

    return {
        'yearly':      yearly,
        'good_years':  good_years,
        'bad_years':   bad_years,
        'consistency': consistency,
        'bull_bias':   bias,
    }


# ─── TỐI ƯU NGƯỠNG SCORE ─────────────────────────────────────────────────────

def optimize_score_threshold(df_trades, verbose=True):
    buy_trades = df_trades[df_trades['action'] == 'MUA'].copy()
    if buy_trades.empty:
        return {}
    if verbose:
        print(f"\n  {'─'*78}")
        print(f"  CHIỀU 2 — TỐI ƯU NGƯỠNG SCORE MUA")
        print(f"  {'─'*78}")
        print(f"  {'Ngưỡng':>8} │ {'Lệnh':>5} │ {'Win%':>5} │ {'PnL TB':>7} │ {'PF':>5} │ Đánh giá")
        print(f"  {'─'*70}")
    results    = {}
    best_thr   = MIN_SCORE_BUY
    best_metric = -999
    for thr in SCORE_THRESHOLDS:
        sub = buy_trades[buy_trades['score'] >= thr]
        if len(sub) < 5:
            if verbose:
                print(f"  score>={thr:>3} │ {len(sub):>5} │ {'--':>5} │ {'--':>7} │ {'--':>5} │ Quá ít lệnh")
            continue
        wins   = sub[sub['pnl'] > 0]
        losses = sub[sub['pnl'] <= 0]
        wr     = len(wins) / len(sub) * 100
        avg_pnl= sub['pnl'].mean()
        pf_den = abs(losses['pnl'].sum())
        pf     = abs(wins['pnl'].sum()) / pf_den if pf_den > 0 else float('inf')
        pf_s   = f'{pf:.2f}' if pf != float('inf') else ' inf'
        penalty= 0.8 if len(sub) < 10 else 1.0
        metric = (wr / 100) * avg_pnl * penalty
        if   wr >= 60 and avg_pnl >= 3 and pf >= 1.8: verdict = '★★★ Rất tốt'
        elif wr >= 55 and avg_pnl >= 1:                verdict = '★★  Tốt'
        elif wr >= 50 and avg_pnl >= 0:                verdict = '★   Chấp nhận'
        else:                                          verdict = '✗   Kém'
        curr_flag = ' ← đang dùng' if thr == MIN_SCORE_BUY else ''
        if verbose:
            print(f"  score>={thr:>3} │ {len(sub):>5} │ {wr:>4.1f}% │ {avg_pnl:>+6.2f}% │ "
                  f"{pf_s:>5} │ {verdict}{curr_flag}")
        results[thr] = {
            'total': len(sub), 'win_rate': round(wr, 1),
            'avg_pnl': round(avg_pnl, 2),
            'profit_factor': round(pf, 2) if pf != float('inf') else float('inf'),
            'metric': round(metric, 3),
        }
        if metric > best_metric and len(sub) >= 8:
            best_metric = metric
            best_thr    = thr
    if verbose:
        curr = results.get(MIN_SCORE_BUY, {})
        best = results.get(best_thr, {})
        print(f"\n  Ngưỡng hiện tại (>={MIN_SCORE_BUY}): "
              f"WR={curr.get('win_rate','?')}%  PnL={curr.get('avg_pnl','?'):+}%")
        print(f"  Ngưỡng tối ưu   (>={best_thr}): "
              f"WR={best.get('win_rate','?')}%  PnL={best.get('avg_pnl','?'):+}%")
        if best_thr != MIN_SCORE_BUY:
            dwr  = best.get('win_rate', 0) - curr.get('win_rate', 0)
            dpnl = best.get('avg_pnl',  0) - curr.get('avg_pnl',  0)
            lost = curr.get('total', 0) - best.get('total', 0)
            print(f"  Cải thiện: WR {dwr:+.1f}%  PnL {dpnl:+.2f}%  "
                  f"(bỏ qua {lost} lệnh score thấp)")
        else:
            print(f"  Ngưỡng {MIN_SCORE_BUY} đang dùng là tối ưu ✓")
    return {'results': results, 'best_threshold': best_thr}


# ─── Khoảng tin cậy 95% (Wilson) ─────────────────────────────────────────────

def analyze_confidence(df_trades, verbose=True):
    buy = df_trades[df_trades['action'] == 'MUA']
    if buy.empty:
        return {}
    n    = len(buy)
    wins = len(buy[buy['pnl'] > 0])
    wr   = wins / n * 100
    z    = 1.96
    p    = wins / n
    denom  = 1 + z**2 / n
    center = (p + z**2 / (2 * n)) / denom
    margin = (z * math.sqrt(p * (1 - p) / n + z**2 / (4 * n**2))) / denom
    lo, hi = round((center - margin) * 100, 1), round((center + margin) * 100, 1)
    if verbose:
        print(f"\n  [A] Khoảng tin cậy 95% — Win Rate thực sự nằm đâu?")
        print(f"  Số lệnh MUA     : {n}")
        print(f"  WR quan sát     : {wr:.1f}%")
        print(f"  CI 95%          : [{lo}% ─── {hi}%]   (độ rộng {hi-lo:.1f}%)")
        width = hi - lo
        if width > 25:
            print(f"  → Khoảng rộng {width:.0f}% — QUÁ ÍT LỆNH, cần ≥ 100 lệnh để kết luận chắc")
        elif width > 15:
            print(f"  → Khoảng {width:.0f}% — Vừa chấp nhận, kết luận cần thận trọng")
        else:
            print(f"  → Khoảng {width:.0f}% — ĐÁNG TIN CẬY thống kê")
        if lo >= 55:
            print(f"  → ✓ Ngay cả trường hợp xấu nhất (CI lower={lo}%), bot VẪN > 55%")
        elif lo >= 45:
            print(f"  → ~ Trường hợp xấu nhất (CI lower={lo}%) vẫn chấp nhận được")
        else:
            print(f"  → ✗ Trường hợp xấu nhất (CI lower={lo}%) rớt xuống dưới 45% — rủi ro cao")
    return {'n': n, 'win_rate': round(wr, 1), 'ci_low': lo, 'ci_high': hi}


# ─── Đặc tính mã ─────────────────────────────────────────────────────────────

def analyze_symbol_profile(symbol, buy_stats, verbose=True):
    tech   = SYMBOL_PROFILE['technical_strong']
    ext    = SYMBOL_PROFILE['external_driven']
    mixed  = SYMBOL_PROFILE['mixed']
    wr     = buy_stats.get('win_rate', 0)
    driver_map = {
        'phap_ly':   'Phụ thuộc tin tức pháp lý / bất động sản',
        'gia_dau':   'Phụ thuộc giá dầu thế giới',
        'phong_thu': 'Cổ phiếu phòng thủ, ít biến động kỹ thuật',
    }
    if symbol in tech:
        profile, desc = 'KY_THUAT_RO', 'Giá phản ứng tốt với RSI/MA/Volume'
    elif symbol in ext:
        driver  = ext[symbol]
        profile = 'PHI_KY_THUAT'
        desc    = driver_map.get(driver, driver)
    elif symbol in mixed:
        profile, desc = 'TRUNG_GIAN', 'Pha trộn kỹ thuật và yếu tố ngành'
    else:
        profile, desc = 'CHUA_PHAN_LOAI', 'Chưa có trong danh sách phân loại'
    if verbose:
        print(f"\n  [C] Đặc tính mã: [{profile}] — {desc}")
        if profile == 'KY_THUAT_RO':
            verdict = '✓ Kết quả backtest PHẢN ÁNH CHÍNH XÁC hiệu quả bot' if wr >= 55 \
                      else '✗ Bot chưa hiệu quả — cân nhắc loại khỏi watchlist'
        elif profile == 'PHI_KY_THUAT':
            verdict = '~ Kết quả tốt nhưng cần thận: có thể do may mắn/đặc thù giai đoạn' if wr >= 55 \
                      else '✗ Kết quả kém — phù hợp dự báo: bot KT thuần không tốt trên mã này'
        else:
            verdict = '· Dùng với thận trọng, không tin hoàn toàn vào bot'
        print(f"      {verdict}")
    return {'symbol': symbol, 'profile': profile, 'desc': desc}


# ─── Cảnh báo thanh khoản ────────────────────────────────────────────────────

def analyze_liquidity(df, symbol, verbose=True):
    vc = next((c for c in df.columns if c.lower() in {
        'volume', 'volume_match', 'klgd', 'vol', 'trading_volume',
        'match_volume', 'total_volume', 'dealvolume', 'matchingvolume',
    }), None)
    if vc is None:
        return {}
    vols        = pd.to_numeric(df[vc], errors='coerce').fillna(0)
    recent_vols = vols.tail(252)
    recent_vols = recent_vols[recent_vols > 0]
    if len(recent_vols) == 0:
        return {}
    avg_vol = float(recent_vols.mean())
    cv      = float(recent_vols.std()) / avg_vol if avg_vol > 0 else 0
    if avg_vol < MIN_LIQUIDITY_VOL:       liq_level = 'low'
    elif avg_vol < MIN_LIQUIDITY_VOL * 3: liq_level = 'medium'
    else:                                 liq_level = 'high'
    if verbose:
        print(f"\n  [D] Thanh khoản: {avg_vol:,.0f} cp/phiên TB (1 năm)")
        if   liq_level == 'low':    print(f"      !! THẤP — tín hiệu Volume kém tin cậy, 1 lệnh tổ chức tạo shark_buy giả")
        elif liq_level == 'medium': print(f"      ~  TRUNG BÌNH — dùng kết quả với mức độ đề phòng")
        else:                       print(f"      ✓  CAO — tín hiệu Volume đáng tin cậy")
        if cv > 2.5:
            print(f"      !! CV={cv:.2f} rất cao — Volume có nhiều đột biến lớn bất thường")
    return {'avg_volume': round(avg_vol), 'cv': round(cv, 2), 'liq_level': liq_level}


# ─── BACKTEST 1 MÃ ────────────────────────────────────────────────────────────

def run_backtest_symbol(symbol, verbose=True, sl=None, tp=None, days=None,
                        entry_mode='T', use_b_filter=False):
    """
    sl/tp         : override SL/TP (số dương). None = dùng SYMBOL_CONFIG rồi mới đến default.
    days          : override số ngày lookback. None = dùng LOOKBACK_DAYS (7 năm).
    entry_mode    : 'T'  = entry tại closes[i]   (backtest chuẩn)
                    'T+1' = entry tại closes[i+1] (thực tế hơn)
    use_b_filter  : True = áp penalty B-filter (Wyckoff/Liquidity) vào score trước khi lọc
                    False = chỉ dùng score kỹ thuật thuần (baseline)
    """
    # Ưu tiên: tham số trực tiếp > SYMBOL_CONFIG > global default
    cfg         = SYMBOL_CONFIG.get(symbol.upper(), {})
    _sl         = sl   if sl   is not None else cfg.get('sl',  abs(STOP_LOSS))
    _tp         = tp   if tp   is not None else cfg.get('tp',  TAKE_PROFIT)
    _min_score  = cfg.get('min_score', MIN_SCORE_BUY)
    _days       = days if days is not None else LOOKBACK_DAYS
    n_years_lbl = round(_days / 365, 1)

    SEP = '═' * 60
    if verbose:
        print(f"\n{SEP}")
        entry_lbl = 'Entry=T+1(thuc te)' if entry_mode == 'T+1' else 'Entry=T(backtest)'
        print(f"  BACKTEST {n_years_lbl:.0f} NĂM: {symbol}  "
              f"[SL=-{_sl*100:.0f}% / TP=+{_tp*100:.0f}% / Score>={_min_score} / {entry_lbl}]")
        print(SEP)
    df, source = load_data(symbol, days=_days)
    if df is None:
        if verbose: print(f"  ✗ Không tải được dữ liệu {symbol}")
        return None

    def to_arr(s):
        return pd.to_numeric(s, errors='coerce').fillna(0).astype(float).values.copy()

    cc = find_col(df, ['close', 'closeprice', 'close_price'])
    hc = find_col(df, ['high',  'highprice',  'high_price'])
    lc = find_col(df, ['low',   'lowprice',   'low_price'])
    vc = next((c for c in df.columns if c.lower() in {
        'volume', 'volume_match', 'klgd', 'vol', 'trading_volume',
        'match_volume', 'total_volume', 'dealvolume', 'matchingvolume',
    }), None)
    if cc is None:
        if verbose: print(f"  ✗ Không tìm được cột close")
        return None

    closes  = to_arr(df[cc]);  closes  = np.where(closes  < 1000, closes  * 1000, closes).copy()
    highs   = to_arr(df[hc]) if hc else closes.copy()
    lows    = to_arr(df[lc]) if lc else closes.copy()
    if hc: highs  = np.where(highs  < 1000, highs  * 1000, highs).copy()
    if lc: lows[lows   < 1000] *= 1000
    volumes = to_arr(df[vc]) if vc else np.zeros(len(closes))

    if verbose:
        n_years = len(closes) // 250
        print(f"  Dữ liệu: {len(closes)} nến (~{n_years} năm) từ {source}")

    # ── Chuẩn bị mảng ngày (vnstock trả về RangeIndex, cần dùng cột 'time') ──
    _time_col = next(
        (c for c in df.columns if c.lower() in ('time', 'date', 'datetime', 'trading_date')),
        None
    )
    if _time_col:
        _dates = pd.to_datetime(df[_time_col], errors='coerce').reset_index(drop=True)
    elif isinstance(df.index, pd.DatetimeIndex):
        _dates = pd.Series(df.index, dtype='datetime64[ns]').reset_index(drop=True)
    else:
        _dates = pd.Series([pd.NaT] * len(df))

    # ── Vòng lặp sinh tín hiệu ──
    trades          = []
    last_signal_idx = -(HOLD_DAYS + SETTLEMENT_DAYS)
    for i in range(200, len(closes) - HOLD_DAYS - SETTLEMENT_DAYS):
        if i - last_signal_idx < HOLD_DAYS + SETTLEMENT_DAYS:
            continue
        score, action = compute_score_at(closes, highs, lows, volumes, i)

        # ── B-filter: điều chỉnh score theo đặc tính thị trường VN ──────────
        b_penalty = 0
        if use_b_filter:
            try:
                # Lấy dữ liệu đến ngày i để tính context (không lookahead)
                df_slice = df.iloc[:i+1].copy()
                import market_context as mc
                ctx = mc.build_market_context(df_slice, symbol,
                          float(closes[i]), 1.0, score)
                wyck = ctx['wyckoff']
                liq  = ctx['liquidity']
                import market_context as _mc_bt
                _bd, _bf, _bds = _mc_bt.calc_b_adjustment(ctx)
                b_penalty = -_bd  # delta âm → penalty dương → trừ điểm; delta dương → cộng điểm
            except Exception:
                pass
            score = max(0, min(100, score - b_penalty))

        # Override ngưỡng MUA theo config per-symbol
        if action == 'MUA' and score < _min_score:
            action = 'THEO DOI'
        if action not in ('MUA', 'BAN'):
            continue
        # entry_mode: 'T' dùng closes[i], 'T+1' dùng closes[i+1] (thực tế hơn)
        entry_idx = i + 1 if entry_mode == 'T+1' and i + 1 < len(closes) else i
        pnl, reason, days = simulate_trade(closes, entry_idx, action, sl=_sl, tp=_tp)
        _ts = _dates.iloc[i] if i < len(_dates) else pd.NaT
        trade_date = _ts.strftime('%Y-%m-%d') if pd.notna(_ts) else f'unknown_{i}'
        trades.append({
            'date':      trade_date,
            'price':     round(closes[entry_idx], 0),
            'score':     score,
            'b_penalty': b_penalty,
            'action':    action,
            'pnl':    pnl,
            'reason': reason,
            'days':   days,
        })
        last_signal_idx = i

    if not trades:
        if verbose: print(f"  Không có tín hiệu nào trong kỳ backtest")
        return None

    df_t    = pd.DataFrame(trades)
    buy_t   = df_t[df_t['action'] == 'MUA']
    sell_t  = df_t[df_t['action'] == 'BAN']
    buy_stats  = calc_stats(buy_t)
    sell_stats = calc_stats(sell_t)

    if verbose and buy_stats:
        bs   = buy_stats
        pf_s = f"{bs['profit_factor']:.2f}" if bs['profit_factor'] != float('inf') else 'inf'
        print(f"\n  Tổng quan lệnh MUA: {bs['total']} lệnh | "
              f"WR={bs['win_rate']}% | PnL={bs['avg_pnl']:+.2f}% | "
              f"PF={pf_s} | TP={bs['tp']} SL={bs['sl']} HK={bs['expired']}")
        print(f"  Cấu hình: SL=-{_sl*100:.0f}%  TP=+{_tp*100:.0f}%  "
              f"Score>={_min_score}  Lookback={n_years_lbl:.0f} năm ({_days} ngày)")

    # ── 3 chiều + 4 phân tích bổ sung ──
    yearly_res  = analyze_by_year_detailed(buy_t, symbol=symbol, verbose=verbose)
    thresh_res  = optimize_score_threshold(df_t, verbose=verbose)

    if verbose:
        # 5 lệnh gần nhất
        print(f"\n  {'─'*60}")
        print(f"  5 LỆNH MUA GẦN NHẤT")
        print(f"  {'─'*60}")
        for _, r in buy_t.tail(5).iterrows():
            icon = '✓' if r['pnl'] > 0 else '✗'
            print(f"  {icon} {r['date']}  @{r['price']:>10,.0f}  "
                  f"Score={r['score']}  PnL={r['pnl']:>+6.1f}%  ({r['reason']}, {r['days']}p)")

    conf     = analyze_confidence(df_t, verbose=verbose)
    profile  = analyze_symbol_profile(symbol, buy_stats, verbose=verbose)
    liq      = analyze_liquidity(df, symbol, verbose=verbose)

    if verbose:
        # ── Tóm tắt cuối ──
        print(f"\n  {'═'*60}")
        print(f"  TÓM TẮT ĐÁNH GIÁ: {symbol}")
        print(f"  {'═'*60}")
        ci_lo = conf.get('ci_low', 0); ci_hi = conf.get('ci_high', 100)
        bias  = yearly_res.get('bull_bias', 'N/A')
        prof  = profile.get('profile', '?')
        liq_l = liq.get('liq_level', '?')
        print(f"  Win Rate   : {buy_stats.get('win_rate',0):.1f}%  "
              f"(CI 95%: {ci_lo}% ─ {ci_hi}%)")
        print(f"  PnL TB     : {buy_stats.get('avg_pnl',0):+.2f}%  "
              f"(TP avg: {buy_stats.get('avg_win',0):+.1f}% | SL avg: {buy_stats.get('avg_loss',0):+.1f}%)")
        print(f"  Bull Bias  : {bias}")
        print(f"  Đặc tính   : {prof}")
        print(f"  Thanh khoản: {liq_l.upper()}")
        # Số red flags
        wr   = buy_stats.get('win_rate', 0)
        rf   = 0
        if ci_lo < 45:                            rf += 1
        if bias in ('NGHIÊM TRỌNG',):             rf += 2
        if bias in ('TRUNG BÌNH',):               rf += 1
        if liq_l == 'low':                        rf += 1
        if prof == 'PHI_KY_THUAT' and wr < 55:   rf += 1
        verdict_map = {
            0: '[✓] TIN CẬY CAO — Tín hiệu đáng tin cậy trên mã này',
            1: '[~] CHẤP NHẬN   — Kết quả khá, nên thận trọng khi giao dịch',
            2: '[!] CẨN THẬN    — Có rủi ro thống kê, dùng vị thế nhỏ',
        }
        verdict = verdict_map.get(rf, '[✗] KHÔNG KHUYẾN DÙNG — Nhiều rủi ro, kết quả có thể không phản ánh thực tế')
        print(f"  Red flags  : {rf}  →  {verdict}")

    return {
        'symbol':   symbol,
        'sl':       _sl,
        'tp':       _tp,
        'min_score': _min_score,
        'days':     _days,
        'entry_mode': entry_mode,
        'buy':      buy_stats,
        'sell':     sell_stats,
        'trades':   df_t,
        'yearly':   yearly_res,
        'thresh':   thresh_res,
        'conf':     conf,
        'profile':  profile,
        'liq':      liq,
    }


# ─── B-FILTER COMPARISON — So sánh Score(A) vs Score(A+B) ──────────────────

def run_b_filter_comparison(symbol, verbose=True):
    """
    Chạy backtest 2 chế độ:
      Mode A  : Chỉ Score kỹ thuật (baseline)
      Mode A+B: Score kỹ thuật + B-filter (Wyckoff/Liquidity penalty)

    Mục đích: kiểm chứng xem B-filter có cải thiện WR/PnL thực sự không.
    """
    if verbose:
        print(f"\n{'═'*60}")
        print(f"  B-FILTER COMPARISON: {symbol}")
        print(f"  Mode A (baseline) vs Mode A+B (with B-filter)")
        print(f"{'═'*60}")

    res_a  = run_backtest_symbol(symbol, verbose=False, use_b_filter=False)
    res_ab = run_backtest_symbol(symbol, verbose=False, use_b_filter=True)

    if not res_a or not res_ab:
        if verbose: print("  Không đủ dữ liệu.")
        return None

    ba  = res_a.get('buy', {})
    bab = res_ab.get('buy', {})
    if not ba or not bab:
        if verbose: print("  Không có lệnh MUA nào.")
        return None

    wr_a   = ba.get('win_rate',      0)
    wr_ab  = bab.get('win_rate',     0)
    pnl_a  = ba.get('avg_pnl',       0)
    pnl_ab = bab.get('avg_pnl',      0)
    pf_a   = ba.get('profit_factor', 0)
    pf_ab  = bab.get('profit_factor',0)
    n_a    = ba.get('total',          0)
    n_ab   = bab.get('total',         0)

    wr_diff  = wr_ab  - wr_a
    pnl_diff = pnl_ab - pnl_a
    n_diff   = n_ab   - n_a  # Số lệnh bị lọc bởi B-filter

    # Đánh giá
    if wr_diff >= 2 and pnl_diff >= 0.3:
        verdict = 'B-FILTER CO GIA TRI — WR va PnL deu cai thien'
        flag    = 'V'
    elif wr_diff >= 0 and pnl_diff >= 0:
        verdict = 'B-FILTER CO ICH NHE — Cai thien nho, chap nhan'
        flag    = '~'
    elif wr_diff >= -2 and n_diff <= -3:
        verdict = 'TRUNG TINH — WR khong doi, chi loc bot lenh nhieu nhieu'
        flag    = '-'
    else:
        verdict = 'B-FILTER CO HAI — WR hoac PnL giam, nen dieu chinh penalty'
        flag    = '!'

    if verbose:
        pf_a_s  = f"{pf_a:.2f}"  if pf_a  != float('inf') else 'inf'
        pf_ab_s = f"{pf_ab:.2f}" if pf_ab != float('inf') else 'inf'

        print(f"\n  {'Mode':>8} | {'Lệnh':>5} | {'WR%':>6} | {'PnL TB':>7} | {'PF':>5}")
        print(f"  {'─'*48}")
        print(f"  {'A (BT)':>8} | {n_a:>5} | {wr_a:>5.1f}% | {pnl_a:>+6.2f}% | {pf_a_s:>5}")
        print(f"  {'A+B':>8} | {n_ab:>5} | {wr_ab:>5.1f}% | {pnl_ab:>+6.2f}% | {pf_ab_s:>5}")
        print(f"  {'─'*48}")
        sign = '+' if wr_diff >= 0 else ''
        print(f"  {'Chenh':>8} | {n_diff:>+5} | {sign}{wr_diff:.1f}% | {pnl_diff:>+6.2f}% |")
        print(f"\n  Lenh bi loc boi B-filter: {abs(n_diff)} ({abs(n_diff)/n_a*100:.1f}% tong lenh)")
        print(f"\n  [{flag}] {verdict}")

    return {
        'symbol':    symbol,
        'mode_A':    {'wr': wr_a,  'pnl': pnl_a,  'pf': pf_a,  'n': n_a},
        'mode_AB':   {'wr': wr_ab, 'pnl': pnl_ab, 'pf': pf_ab, 'n': n_ab},
        'wr_diff':   round(wr_diff,  1),
        'pnl_diff':  round(pnl_diff, 2),
        'n_filtered': abs(n_diff),
        'flag':      flag,
        'verdict':   verdict,
        'res_a':     res_a,
        'res_ab':    res_ab,
    }


def run_b_filter_walkforward(symbol, verbose=True):
    """
    Walk-forward comparison: Mode A vs Mode A+B
    Mỗi cửa sổ IS=2năm OOS=1năm, so sánh WR decay giữa 2 mode.
    """
    if verbose:
        print(f"\n{'═'*60}")
        print(f"  B-FILTER WALK-FORWARD: {symbol}")
        print(f"  IS=2nam OOS=1nam | So sanh decay A vs A+B")
        print(f"{'═'*60}")

    wf_a  = run_walk_forward(symbol, verbose=False)
    # Walk-forward với B-filter: cần inject vào run_walk_forward
    # Hiện tại chạy run_backtest_symbol với use_b_filter=True cho từng window
    wf_ab = _run_walk_forward_with_b_filter(symbol, verbose=False)

    if not wf_a or not wf_ab:
        if verbose: print("  Không đủ dữ liệu walk-forward.")
        return None

    decay_a  = wf_a.get('decay_wr',  0)
    decay_ab = wf_ab.get('decay_wr', 0)
    oos_a    = wf_a.get('avg_oos_wr',  0)
    oos_ab   = wf_ab.get('avg_oos_wr', 0)

    if verbose:
        print(f"\n  {'Mode':>6} | {'IS WR':>6} | {'OOS WR':>7} | {'Decay':>6} | Verdict")
        print(f"  {'─'*52}")
        print(f"  {'A':>6} | {wf_a['avg_is_wr']:>5.1f}% | {oos_a:>6.1f}%  | {decay_a:>+5.1f}% | {wf_a['verdict_txt'][:25]}")
        print(f"  {'A+B':>6} | {wf_ab['avg_is_wr']:>5.1f}% | {oos_ab:>6.1f}%  | {decay_ab:>+5.1f}% | {wf_ab['verdict_txt'][:25]}")

        if decay_ab < decay_a:
            print(f"\n  [V] B-filter GIAM DECAY: {decay_a:.1f}% -> {decay_ab:.1f}% — robustness cao hon")
        elif abs(decay_ab - decay_a) <= 3:
            print(f"\n  [~] B-filter TRUNG TINH: decay tuong duong ({decay_a:.1f}% vs {decay_ab:.1f}%)")
        else:
            print(f"\n  [!] B-filter TANG DECAY: {decay_a:.1f}% -> {decay_ab:.1f}% — nen xem lai penalty")

    return {
        'symbol':   symbol,
        'wf_a':     wf_a,
        'wf_ab':    wf_ab,
        'decay_a':  decay_a,
        'decay_ab': decay_ab,
        'oos_a':    oos_a,
        'oos_ab':   oos_ab,
    }


def _run_walk_forward_with_b_filter(symbol, verbose=False):
    """Walk-forward với B-filter bật — mirror run_walk_forward nhưng thêm penalty."""
    cfg        = SYMBOL_CONFIG.get(symbol.upper(), {})
    _sl        = cfg.get('sl',        abs(STOP_LOSS))
    _tp        = cfg.get('tp',        TAKE_PROFIT)
    _min_score = cfg.get('min_score', MIN_SCORE_BUY)

    df, source = load_data(symbol, days=LOOKBACK_DAYS)
    if df is None:
        return None

    def to_arr(s):
        return pd.to_numeric(s, errors='coerce').fillna(0).astype(float).values.copy()

    cc = find_col(df, ['close','closeprice','close_price'])
    hc = find_col(df, ['high','highprice','high_price'])
    lc = find_col(df, ['low','lowprice','low_price'])
    vc = next((c for c in df.columns if c.lower() in {
        'volume','volume_match','klgd','vol','trading_volume',
        'match_volume','total_volume','dealvolume','matchingvolume'}), None)
    if cc is None:
        return None

    closes  = to_arr(df[cc]);  closes  = np.where(closes  < 1000, closes  * 1000, closes).copy()
    highs   = to_arr(df[hc]) if hc else closes.copy()
    lows    = to_arr(df[lc]) if lc else closes.copy()
    if hc: highs  = np.where(highs  < 1000, highs  * 1000, highs).copy()
    if lc: lows[lows   < 1000] *= 1000
    volumes = to_arr(df[vc]) if vc else np.zeros(len(closes))

    _time_col = next(
        (c for c in df.columns if c.lower() in ('time','date','datetime','trading_date')), None)
    if _time_col:
        dates = pd.to_datetime(df[_time_col], errors='coerce').reset_index(drop=True)
    elif isinstance(df.index, pd.DatetimeIndex):
        dates = pd.Series(df.index, dtype='datetime64[ns]').reset_index(drop=True)
    else:
        dates = pd.Series([pd.NaT] * len(closes))

    n = len(closes)
    window_size = WF_IS_DAYS + WF_OOS_DAYS
    if n < window_size + 200:
        return None

    # Helper: chạy 1 window với B-filter
    def run_window_with_b(start, end, min_score):
        trades = []
        last_sig = start - (HOLD_DAYS + SETTLEMENT_DAYS)
        for i in range(max(start, 200), end - HOLD_DAYS - SETTLEMENT_DAYS):
            if i - last_sig < HOLD_DAYS + SETTLEMENT_DAYS:
                continue
            score, action = compute_score_at(closes, highs, lows, volumes, i)
            # B-filter
            b_pen = 0
            try:
                import market_context as mc
                df_sl = df.iloc[:i+1].copy()
                ctx   = mc.build_market_context(df_sl, symbol, float(closes[i]), 1.0, score)
                wyck  = ctx['wyckoff']
                liq   = ctx['liquidity']
                import market_context as _mc_wf
                _bd2, _, _ = _mc_wf.calc_b_adjustment(ctx)
                b_pen = -_bd2  # delta âm → trừ; delta dương → cộng
            except Exception:
                pass
            score = max(0, min(100, score - b_pen))
            if action == 'MUA' and score < min_score:
                action = 'THEO DOI'
            if action not in ('MUA', 'BAN'):
                continue
            pnl, reason, dh = simulate_trade(closes, i, action, sl=_sl, tp=_tp)
            trades.append({'score': score, 'pnl': pnl, 'reason': reason, 'days': dh})
            last_sig = i
        if not trades:
            return None
        dfb = pd.DataFrame(trades)
        buy = dfb[dfb['action'] == 'MUA'] if 'action' in dfb else dfb
        # Tất cả đều là MUA trong window này
        return calc_stats(dfb)

    def find_best_thr_with_b(start, end):
        best, best_m = _min_score, -999
        for thr in [60,65,68,70,72,75,78,80]:
            st = run_window_with_b(start, end, thr)
            if not st or st['total'] < 5:
                continue
            m = (st['win_rate']/100) * st['avg_pnl'] * (0.8 if st['total'] < 8 else 1.0)
            if m > best_m:
                best_m, best = m, thr
        return best, run_window_with_b(start, end, best)

    windows = []
    for s in range(0, n - window_size, WF_OOS_DAYS):
        is_e = s + WF_IS_DAYS
        oos_e = min(is_e + WF_OOS_DAYS, n)
        if oos_e - is_e < 30:
            continue
        best_thr, is_st = find_best_thr_with_b(s, is_e)
        oos_st = run_window_with_b(is_e, oos_e, best_thr)
        oos_label = str(dates.iloc[is_e].year) if pd.notna(dates.iloc[is_e]) else '?'
        windows.append({
            'best_thr': best_thr,
            'is_wr':    is_st['win_rate'] if is_st else 0,
            'is_pnl':   is_st['avg_pnl']  if is_st else 0,
            'oos_wr':   oos_st['win_rate'] if oos_st else None,
            'oos_pnl':  oos_st['avg_pnl']  if oos_st else None,
            'oos_label': oos_label,
        })

    if not windows:
        return None

    valid = [w for w in windows if w['oos_wr'] is not None]
    avg_is  = sum(w['is_wr']  for w in valid) / len(valid) if valid else 0
    avg_oos = sum(w['oos_wr'] for w in valid) / len(valid) if valid else 0
    avg_ip  = sum(w['is_pnl'] for w in valid) / len(valid) if valid else 0
    avg_op  = sum(w['oos_pnl'] for w in valid) / len(valid) if valid else 0
    decay   = avg_is - avg_oos

    if avg_oos >= 55 and avg_op > 0 and decay <= 10:
        verdict, vf = 'TOT - OOS nhat quan voi IS', 'V'
    elif avg_oos >= 50 and avg_op >= 0 and decay <= 20:
        verdict, vf = 'CHAP NHAN - giam nhe khi ra OOS', '~'
    else:
        verdict, vf = 'YEU - OOS khong xac nhan IS', 'X'

    return {
        'symbol': symbol, 'windows': windows,
        'avg_is_wr': round(avg_is, 1), 'avg_oos_wr': round(avg_oos, 1),
        'avg_is_pnl': round(avg_ip, 2), 'avg_oos_pnl': round(avg_op, 2),
        'decay_wr': round(decay, 1),
        'verdict': vf, 'verdict_txt': verdict,
        'sl': _sl, 'tp': _tp,
    }


def run_b_filter_all(symbols=None, verbose=True):
    """
    Chạy B-filter comparison cho toàn bộ danh sách mã.
    Tổng hợp: bao nhiêu mã được cải thiện, bao nhiêu không.
    """
    if symbols is None:
        symbols = list(SYMBOL_CONFIG.keys()) if SYMBOL_CONFIG else [
            'HCM','PC1','VCB','MBB','NKG','VND','DGC','DCM',
            'BID','TCB','VPB','FPT','HPG','HSG','SSI',
            'VHM','VIC','GAS','PVD','PVS','MWG','VNM',
            'MSN','REE','POW','KBC','IDC','BCM',
        ]

    LINE = '=' * 62
    if verbose:
        print(f"\n{LINE}")
        print(f"  B-FILTER COMPARISON — TOAN BO {len(symbols)} MA")
        print(f"  Mode A (KT thuan) vs Mode A+B (KT + Wyckoff/Liquidity)")
        print(LINE)

    results  = []
    failed   = []

    for i, sym in enumerate(symbols, 1):
        if verbose:
            print(f"  [{i:>2}/{len(symbols)}] {sym}...", end=' ', flush=True)
        res = run_b_filter_comparison(sym, verbose=False)
        if res:
            results.append(res)
            ma  = res['mode_A']
            mab = res['mode_AB']
            if verbose:
                print(f"A: WR={ma['wr']:.1f}% | A+B: WR={mab['wr']:.1f}% "
                      f"({res['wr_diff']:+.1f}%) [{res['flag']}]")
        else:
            failed.append(sym)
            if verbose:
                print("SKIP")

    if not results:
        if verbose: print("  Khong co ket qua nao.")
        return None

    # ── Thống kê tổng hợp ────────────────────────────────────────────────────
    n_improved  = sum(1 for r in results if r['flag'] in ('V','~'))
    n_neutral   = sum(1 for r in results if r['flag'] == '-')
    n_harmful   = sum(1 for r in results if r['flag'] == '!')
    n_total     = len(results)

    avg_wr_a    = sum(r['mode_A']['wr']   for r in results) / n_total
    avg_wr_ab   = sum(r['mode_AB']['wr']  for r in results) / n_total
    avg_pnl_a   = sum(r['mode_A']['pnl']  for r in results) / n_total
    avg_pnl_ab  = sum(r['mode_AB']['pnl'] for r in results) / n_total
    avg_wr_diff = avg_wr_ab  - avg_wr_a
    avg_pnl_diff= avg_pnl_ab - avg_pnl_a
    avg_filtered= sum(r['n_filtered'] for r in results) / n_total

    # Verdict tổng
    if n_improved >= n_total * 0.6 and avg_wr_diff >= 1:
        overall = 'B-FILTER CO GIA TRI THUC SU — nen giu nguyen penalty hien tai'
        overall_flag = 'V'
    elif n_improved >= n_total * 0.4 and avg_wr_diff >= 0:
        overall = 'B-FILTER CO ICH MOT PHAN — xem xet dieu chinh penalty cho ma kem'
        overall_flag = '~'
    elif n_harmful >= n_total * 0.4:
        overall = 'B-FILTER CO HAI — nen giam penalty hoac tat B-filter'
        overall_flag = '!'
    else:
        overall = 'B-FILTER TRUNG TINH — khong co tac dong dang ke'
        overall_flag = '-'

    if verbose:
        HDR  = "  {:<5} | {:>5} | {:>6} | {:>5} | {:>7} | {:>5} | {:>6} | {}".format(
                   "Ma", "N(A)", "WR(A)", "N(AB)", "WR(AB)", "dWR", "dPnL", "Flag")
        SEP2 = "  " + "-" * 68

        print("\n" + LINE)
        print("  TONG KET B-FILTER COMPARISON — " + str(n_total) + " ma")
        print(LINE)
        print("\n" + HDR)
        print(SEP2)

        sorted_res = sorted(results, key=lambda x: x['wr_diff'], reverse=True)
        for r in sorted_res:
            ma  = r['mode_A']
            mab = r['mode_AB']
            row = "  {:>5} | {:>5} | {:>5.1f}% | {:>5} | {:>6.1f}%  | {:>+4.1f}% | {:>+5.2f}% | [{}]".format(
                r['symbol'], ma['n'], ma['wr'], mab['n'], mab['wr'],
                r['wr_diff'], r['pnl_diff'], r['flag'])
            print(row)

        avg_n_a = sum(r['mode_A']['n'] for r in results) // n_total
        tb_row  = "  {:>5} | {:>5} | {:>5.1f}% |       | {:>6.1f}%  | {:>+4.1f}% | {:>+5.2f}% |".format(
                   "TB", avg_n_a, avg_wr_a, avg_wr_ab, avg_wr_diff, avg_pnl_diff)
        print(SEP2)
        print(tb_row)

        print("\n  Ket qua:")
        print("   Co ich (V/~)    : " + str(n_improved) + "/" + str(n_total) + " ma")
        print("   Trung tinh (-)  : " + str(n_neutral)  + "/" + str(n_total) + " ma")
        print("   Co hai (!)      : " + str(n_harmful)  + "/" + str(n_total) + " ma")
        pct_f = avg_filtered / max(avg_wr_a, 1) * 2
        print("   Lenh bi loc TB  : " + str(round(avg_filtered, 1))
              + " lenh/ma (" + str(round(pct_f, 1)) + "%)")
        print("\n  [" + overall_flag + "] " + overall)

        # Top cải thiện và top bị hại
        top_good = [r['symbol'] for r in sorted_res[:5] if r['wr_diff'] > 0]
        top_bad  = [r['symbol'] for r in sorted_res if r['flag'] == '!']
        if top_good:
            print("\n  Ma huong loi nhieu nhat: " + ", ".join(top_good))
        if top_bad:
            print("  Ma bi anh huong xau    : " + ", ".join(top_bad))
            print("  -> Xem xet dieu chinh penalty cho: " + ", ".join(top_bad))

    return {
        'n_total':      n_total,
        'n_improved':   n_improved,
        'n_neutral':    n_neutral,
        'n_harmful':    n_harmful,
        'avg_wr_a':     round(avg_wr_a, 1),
        'avg_wr_ab':    round(avg_wr_ab, 1),
        'avg_pnl_a':    round(avg_pnl_a, 2),
        'avg_pnl_ab':   round(avg_pnl_ab, 2),
        'avg_wr_diff':  round(avg_wr_diff, 1),
        'avg_pnl_diff': round(avg_pnl_diff, 2),
        'avg_filtered': round(avg_filtered, 1),
        'overall_flag': overall_flag,
        'overall':      overall,
        'results':      results,
        'failed':       failed,
    }


# ─── DUAL MODE BACKTEST — So sánh Entry T vs T+1 ──────────────────────────────

def run_backtest_dual(symbol, verbose=True):
    """
    Chạy backtest 2 lần: Entry=T (chuẩn) và Entry=T+1 (thực tế).
    So sánh WR, PnL, PF để đánh giá mức độ entry price bias.
    """
    SEP = '─' * 60
    if verbose:
        print(f"\n{'═'*60}")
        print(f"  DUAL MODE BACKTEST: {symbol}")
        print(f"  So sanh Entry=T (backtest) vs Entry=T+1 (thuc te)")
        print(f"{'═'*60}")

    res_t   = run_backtest_symbol(symbol, verbose=False, entry_mode='T')
    res_t1  = run_backtest_symbol(symbol, verbose=False, entry_mode='T+1')

    if not res_t or not res_t1:
        if verbose: print("  Khong du du lieu.")
        return None

    bt   = res_t.get('buy', {})
    bt1  = res_t1.get('buy', {})

    if not bt or not bt1:
        if verbose: print("  Khong co lenh MUA nao.")
        return None

    # ── So sánh ──────────────────────────────────────────────────────────────
    wr_t    = bt.get('win_rate',      0)
    wr_t1   = bt1.get('win_rate',     0)
    pnl_t   = bt.get('avg_pnl',       0)
    pnl_t1  = bt1.get('avg_pnl',      0)
    pf_t    = bt.get('profit_factor', 0)
    pf_t1   = bt1.get('profit_factor',0)
    n_t     = bt.get('total',         0)
    n_t1    = bt1.get('total',        0)

    wr_diff  = wr_t1  - wr_t
    pnl_diff = pnl_t1 - pnl_t

    # Đánh giá mức độ bias
    if abs(wr_diff) <= 5 and abs(pnl_diff) <= 0.5:
        bias_level = 'NHE - Entry bias khong dang ke (WR chenh nho hon 5%)'
        bias_flag  = 'V'
    elif abs(wr_diff) <= 10 and abs(pnl_diff) <= 1.5:
        bias_level = 'TRUNG BINH - Nen xem xet dung Entry T+1'
        bias_flag  = '~'
    else:
        bias_level = 'NGHIEM TRONG - Entry T+1 thay doi dang ke ket qua'
        bias_flag  = '!'

    # Khuyến nghị
    if wr_t1 >= wr_t - 5 and pnl_t1 >= pnl_t - 0.5:
        recommend = 'Ket qua T+1 on dinh - he thong robust, entry bias khong anh huong lon'
    elif wr_t1 >= wr_t - 10:
        recommend = 'Ket qua T+1 giam nhe - chap nhan duoc, co the dung lam baseline thuc te'
    else:
        recommend = 'T+1 kem hon T dang ke - nen nang nguong score hoac dieu chinh SL/TP'

    if verbose:
        pf_t_s  = f"{pf_t:.2f}"  if pf_t  != float('inf') else 'inf'
        pf_t1_s = f"{pf_t1:.2f}" if pf_t1 != float('inf') else 'inf'
        print(f"\n  {'Mode':>12} | {'Lenh':>5} | {'WR%':>6} | {'PnL TB':>7} | {'PF':>5}")
        print(f"  {'─'*48}")
        print(f"  {'Entry=T (BT)':>12} | {n_t:>5} | {wr_t:>5.1f}% | {pnl_t:>+6.2f}% | {pf_t_s:>5}")
        print(f"  {'Entry=T+1':>12} | {n_t1:>5} | {wr_t1:>5.1f}% | {pnl_t1:>+6.2f}% | {pf_t1_s:>5}")
        print(f"  {'─'*48}")
        print(f"  {'Chenh lech':>12} |       | {wr_diff:>+5.1f}% | {pnl_diff:>+6.2f}% |")
        print(f"\n[{bias_flag}] Entry Bias: {bias_level}")
        print(f"  Khuyen nghi: {recommend}")

    return {
        'symbol':    symbol,
        'mode_T':    {'wr': wr_t,  'pnl': pnl_t,  'pf': pf_t,  'n': n_t},
        'mode_T1':   {'wr': wr_t1, 'pnl': pnl_t1, 'pf': pf_t1, 'n': n_t1},
        'wr_diff':   round(wr_diff,  1),
        'pnl_diff':  round(pnl_diff, 2),
        'bias_flag': bias_flag,
        'bias_level':bias_level,
        'recommend': recommend,
        'res_t':     res_t,
        'res_t1':    res_t1,
    }


def cross_symbol_conclusion(all_results):
    if len(all_results) < 2:
        return
    print(f"\n\n{'═'*78}")
    print(f"  KẾT LUẬN CHÉO — PHÂN TÍCH HỆ THỐNG ({len(all_results)} mã)")
    print(f"{'═'*78}")

    # ── Bảng tổng hợp ──
    rows = []
    for sym, res in all_results.items():
        buy  = res.get('buy', {})
        if not buy: continue
        yr   = res.get('yearly', {})
        conf = res.get('conf', {})
        liq  = res.get('liq', {})
        prof = res.get('profile', {})
        bias = yr.get('bull_bias', 'N/A')
        rows.append({
            'symbol':    sym,
            'total':     buy.get('total', 0),
            'win_rate':  buy.get('win_rate', 0),
            'avg_pnl':   buy.get('avg_pnl', 0),
            'pf':        buy.get('profit_factor', 0),
            'ci_low':    conf.get('ci_low', 0),
            'best_thr':  res.get('thresh', {}).get('best_threshold', MIN_SCORE_BUY),
            'good_yrs':  len(yr.get('good_years', [])),
            'total_yrs': len(yr.get('yearly', {})),
            'bias':      bias,
            'liq':       liq.get('liq_level', '?'),
            'profile':   prof.get('profile', '?'),
        })
    if not rows:
        print("  Không đủ dữ liệu.")
        return

    df = pd.DataFrame(rows).sort_values('win_rate', ascending=False)
    print(f"\n  {'Mã':>5} │ {'Lệnh':>5} │ {'Win%':>5} │ {'CI95':>5} │ "
          f"{'PnL TB':>7} │ {'PF':>5} │ {'YrOK':>5} │ {'Bias':>7} │ {'Liq':>3} │ Ngưỡng")
    print(f"  {'─'*88}")
    for _, r in df.iterrows():
        pf_s  = f"{r['pf']:.2f}" if r['pf'] != float('inf') else ' inf'
        yr_ok = f"{int(r['good_yrs'])}/{int(r['total_yrs'])}"
        flag  = ' ✓' if r['win_rate'] >= 55 and r['avg_pnl'] > 0 else \
                (' ✗' if r['win_rate'] < 45 else ' ·')
        thr_s = f">={int(r['best_thr'])}" + (' ↑' if r['best_thr'] > MIN_SCORE_BUY else ' =')
        bias_s = str(r['bias'])[:7] if r['bias'] else 'N/A'
        print(f"  {r['symbol']:>5} │ {int(r['total']):>5} │ {r['win_rate']:>4.1f}% │ "
              f"{r['ci_low']:>4.1f}% │ {r['avg_pnl']:>+6.2f}% │ {pf_s:>5} │ "
              f"{yr_ok:>5} │ {bias_s:>7} │ {str(r['liq'])[:3].upper():>3} │ {thr_s}{flag}")

    avg_wr  = df['win_rate'].mean()
    avg_pnl = df['avg_pnl'].mean()
    good    = df[(df['win_rate'] >= 55) & (df['avg_pnl'] > 0)]
    weak    = df[(df['win_rate'] < 45)  | (df['avg_pnl'] < -1)]
    consist = df[df['good_yrs'] >= df['total_yrs'] * 0.6]

    print(f"\n  Hệ thống: WR TB={avg_wr:.1f}%  PnL TB={avg_pnl:+.2f}%")
    print(f"  Mã tốt  ({len(good)}): {', '.join(good['symbol'].tolist())}")
    print(f"  Mã yếu  ({len(weak)}): {', '.join(weak['symbol'].tolist())}")
    print(f"  Nhất quán (≥60% năm tốt): {len(consist)}/{len(df)} mã")

    # ── TIME SLICE: Gom tất cả lệnh → phân tích từng năm toàn hệ thống ──
    print(f"\n  {'─'*78}")
    print(f"  TIME SLICE — KẾT QUẢ TOÀN HỆ THỐNG THEO TỪNG NĂM")
    print(f"  Câu hỏi: Bot có đang overfitting bull market 2021 không?")
    print(f"  {'─'*78}")

    # Gom tất cả lệnh MUA từ mọi mã
    all_buy_frames = []
    for sym, res in all_results.items():
        t = res.get('trades')
        if t is not None and len(t) > 0:
            buy_only = t[t['action'] == 'MUA'].copy()
            buy_only['symbol'] = sym
            all_buy_frames.append(buy_only)

    if all_buy_frames:
        all_buys = pd.concat(all_buy_frames, ignore_index=True)
        all_buys['year'] = pd.to_datetime(all_buys['date']).dt.year
        ts_years = sorted(all_buys['year'].unique())

        print(f"\n  {'Năm':>4} │ {'Lệnh':>5} │ {'Mã':>4} │ {'Win%':>5} │ {'WR Bar':^14} │ "
              f"{'PnL TB':>7} │ {'TP':>4} │ {'SL':>4} │ {'HK':>4} │ Giai đoạn + Nhận xét")
        print(f"  {'─'*4}─┼─{'─'*5}─┼─{'─'*4}─┼─{'─'*5}─┼─{'─'*14}─┼─"
              f"{'─'*7}─┼─{'─'*4}─┼─{'─'*4}─┼─{'─'*4}─┼─{'─'*30}")

        ts_data = {}
        for yr in ts_years:
            sub   = all_buys[all_buys['year'] == yr]
            n_sym = sub['symbol'].nunique()
            st    = calc_stats(sub)
            ts_data[yr] = {**st, 'n_sym': n_sym}

            bar   = wr_bar(st['win_rate'])
            phase = MARKET_PHASES.get(yr, '---')
            if   st['win_rate'] >= 60 and st['avg_pnl'] >= 2: note = '✓ Bot hiệu quả'
            elif st['win_rate'] >= 55 and st['avg_pnl'] >= 0: note = '~ Chấp nhận được'
            elif st['win_rate'] >= 45:                         note = '- Trung bình'
            else:                                              note = '✗ Bot gặp khó'

            print(f"  {yr:>4} │ {st['total']:>5} │ {n_sym:>4} │ {st['win_rate']:>4.1f}% │ "
                  f"{bar:<14} │ {st['avg_pnl']:>+6.2f}% │ {st['tp']:>4} │ {st['sl']:>4} │ "
                  f"{st['expired']:>4} │ {phase} — {note}")

        # Dòng tổng
        total_all_st = calc_stats(all_buys)
        bar_all = wr_bar(total_all_st['win_rate'])
        n_sym_all = all_buys['symbol'].nunique()
        print(f"  {'─'*4}─┼─{'─'*5}─┼─{'─'*4}─┼─{'─'*5}─┼─{'─'*14}─┼─"
              f"{'─'*7}─┼─{'─'*4}─┼─{'─'*4}─┼─{'─'*4}─┼─{'─'*30}")
        print(f"  {'TỔNG':>4} │ {total_all_st['total']:>5} │ {n_sym_all:>4} │ "
              f"{total_all_st['win_rate']:>4.1f}% │ {bar_all:<14} │ "
              f"{total_all_st['avg_pnl']:>+6.2f}% │ {total_all_st['tp']:>4} │ "
              f"{total_all_st['sl']:>4} │ {total_all_st['expired']:>4} │")

        # ── Phân tích overfitting bull 2021 ──
        print(f"\n  Phân tích Bull Bias toàn hệ thống:")
        d2021  = ts_data.get(2021, {})
        others = {y: d for y, d in ts_data.items() if y != 2021 and d.get('total', 0) >= 5}
        if d2021 and others:
            wr_2021    = d2021['win_rate']
            avg_others = sum(d['win_rate'] for d in others.values()) / len(others)
            gap        = wr_2021 - avg_others
            pnl_2021   = d2021['avg_pnl']
            pnl_others = sum(d['avg_pnl'] for d in others.values()) / len(others)

            print(f"  Năm 2021 : WR={wr_2021:.1f}%  PnL={pnl_2021:+.2f}%  ({d2021.get('total',0)} lệnh)")
            print(f"  Các năm khác: WR TB={avg_others:.1f}%  PnL TB={pnl_others:+.2f}%")
            print(f"  Chênh lệch WR 2021 vs TB: {gap:+.1f}%")

            if gap > 20:
                print(f"\n  !! CẢNH BÁO OVERFITTING BULL MARKET:")
                print(f"     2021 cao hơn {gap:.0f}% so với TB các năm — kết quả tổng bị thổi phồng")
                print(f"     → Dùng WR {avg_others:.1f}% (loại trừ 2021) làm kỳ vọng thực tế")
            elif gap > 10:
                print(f"\n  ~ Chênh lệch vừa ({gap:.0f}%) — bull market tự nhiên dễ hơn, chấp nhận được")
                print(f"     → Kỳ vọng thực tế nằm giữa: {avg_others:.1f}% – {wr_2021:.1f}%")
            else:
                print(f"\n  ✓ Không có overfitting đáng kể — bot nhất quán qua cả bull lẫn bear")

            # So sánh bear 2022 vs bull 2021 — test kép
            d2022 = ts_data.get(2022, {})
            if d2022 and d2022.get('total', 0) >= 5:
                gap_22 = wr_2021 - d2022['win_rate']
                print(f"\n  Test kép 2021 (bull) vs 2022 (bear):")
                print(f"  WR 2021={wr_2021:.1f}%  vs  WR 2022={d2022['win_rate']:.1f}%  "
                      f"(chênh {gap_22:+.1f}%)")
                if gap_22 > 25:
                    print(f"  !! Chênh lệch lớn — bot phụ thuộc nhiều vào xu hướng thị trường chung")
                    print(f"     → Cân nhắc thêm bộ lọc xu hướng VN-Index trước khi vào lệnh")
                elif gap_22 > 10:
                    print(f"  ~ Chênh lệch vừa — bình thường, bear market khó hơn")
                else:
                    print(f"  ✓ Bot ổn định qua cả hai giai đoạn đối lập")

        # ── Phân phối lệnh theo năm (phát hiện lệch tỉ trọng) ──
        print(f"\n  Phân phối lệnh theo năm (kiểm tra tỉ trọng):")
        total_lenh = total_all_st['total']
        for yr in ts_years:
            d   = ts_data[yr]
            pct = d['total'] / total_lenh * 100 if total_lenh > 0 else 0
            bar = '█' * int(pct / 5) + '░' * (20 - int(pct / 5))
            warn = ' ← tỉ trọng cao, ảnh hưởng lớn đến WR tổng' if pct > 30 else ''
            print(f"  {yr}: {bar} {pct:>4.0f}%  ({d['total']} lệnh){warn}")
    else:
        print(f"  Không đủ dữ liệu lệnh để phân tích time slice.")

    # ── Bảng win rate toàn bộ mã theo từng năm (heatmap chéo) ──
    print(f"\n  {'─'*78}")
    print(f"  HEAT MAP — WIN RATE TỪNG MÃ × TỪNG NĂM")
    print(f"  {'─'*78}")
    all_years = sorted({yr for res in all_results.values()
                        for yr in res.get('yearly', {}).get('yearly', {}).keys()})
    header = f"  {'Mã':>5} │"
    for yr in all_years:
        header += f" {yr} │"
    header += " TB"
    print(header)
    print(f"  {'─'*5}─┼─" + "─┼─".join(["─────"] * len(all_years)) + "─┼─────")
    for sym, res in all_results.items():
        buy  = res.get('buy', {})
        yr_d = res.get('yearly', {}).get('yearly', {})
        row  = f"  {sym:>5} │"
        wrs  = []
        for yr in all_years:
            d = yr_d.get(yr)
            if d:
                wr  = d['win_rate']
                wrs.append(wr)
                if   wr >= 60: cell = f" {wr:>3.0f}✓│"
                elif wr >= 55: cell = f" {wr:>3.0f}~│"
                elif wr >= 45: cell = f" {wr:>3.0f}-│"
                else:          cell = f" {wr:>3.0f}✗│"
            else:
                cell = "  -- │"
            row += cell
        avg_wr_sym = buy.get('win_rate', 0)
        row += f" {avg_wr_sym:>3.0f}%"
        print(row)

    # ── Hàng trung bình theo năm ──
    row_avg = f"  {'TB':>5} │"
    for yr in all_years:
        yr_wrs = [
            res['yearly']['yearly'][yr]['win_rate']
            for res in all_results.values()
            if yr in res.get('yearly', {}).get('yearly', {})
        ]
        if yr_wrs:
            avg  = sum(yr_wrs) / len(yr_wrs)
            cell = f" {avg:>3.0f}%│"
        else:
            cell = "  -- │"
        row_avg += cell
    row_avg += f" {avg_wr:.0f}%"
    print(f"  {'─'*5}─┼─" + "─┼─".join(["─────"] * len(all_years)) + "─┼─────")
    print(row_avg)

    print(f"\n  ✓=WR≥60%  ~=WR≥55%  -=WR≥45%  ✗=WR<45%")

    # ── Kết luận hệ thống ──
    print(f"\n  {'═'*78}")
    print(f"  KẾT LUẬN HỆ THỐNG:")
    if avg_wr >= 58 and avg_pnl >= 2:
        print(f"  [✓] Bot HOẠT ĐỘNG TỐT — WR={avg_wr:.1f}% PnL={avg_pnl:+.2f}% trên {len(df)} mã")
    elif avg_wr >= 52 and avg_pnl >= 0:
        print(f"  [~] TRUNG BÌNH — WR={avg_wr:.1f}% chấp nhận, PnL={avg_pnl:+.2f}% cần cải thiện")
    else:
        print(f"  [✗] CHƯA HIỆU QUẢ — WR={avg_wr:.1f}% PnL={avg_pnl:+.2f}% dưới ngưỡng")
    pct_good = len(good) / len(df) * 100
    if pct_good >= 70:
        print(f"  [✓] Nhất quán trên {pct_good:.0f}% mã — tín hiệu đáng tin cậy hệ thống")
    elif pct_good >= 40:
        print(f"  [~] Tốt trên {pct_good:.0f}% mã — nên chọn lọc")
        print(f"      Ưu tiên: {', '.join(good['symbol'].tolist())}")
        if len(weak) > 0:
            print(f"      Tránh  : {', '.join(weak['symbol'].tolist())}")
    else:
        print(f"  [✗] Chỉ tốt {pct_good:.0f}% mã — kết quả phụ thuộc mã, không phải bot")
    # Khuyến nghị ngưỡng theo đa số
    thresh_votes = df['best_thr'].value_counts()
    best_common  = int(thresh_votes.index[0])
    if best_common != MIN_SCORE_BUY:
        print(f"\n  [→] KHUYẾN NGHỊ nâng ngưỡng MUA: {MIN_SCORE_BUY} → {best_common}")
        print(f"      {thresh_votes[best_common]} mã cho kết quả tốt hơn ở ngưỡng này")
    else:
        print(f"\n  [✓] Ngưỡng MUA hiện tại ({MIN_SCORE_BUY}) là tối ưu")
    print(f"\n  Lưu ý: Backtest chưa tính phí giao dịch (~0.15-0.25%/lệnh).")
    print(f"  Kết quả quá khứ không đảm bảo tương lai.\n")


# ─── ENTRY POINT ──────────────────────────────────────────────────────────────

if __name__ == '__main__':
    args = sys.argv[1:]
    if not args:
        print("Chạy backtest mẫu 3 mã: VCB HPG FPT")
        print("Dùng: python backtest.py VCB HPG  hoặc  python backtest.py --all")
        print("      python backtest.py --ma MBB    (chiến lược MA10/MA50 cho 1 mã)")
        print("      python backtest.py --ma --all  (so sánh MA cho toàn bộ watchlist)\n")
        symbols = ['VCB', 'HPG', 'FPT']
    elif args[0] == '--ma':
        # Chế độ MA strategy backtest
        if len(args) >= 2 and args[1] == '--all':
            run_ma_strategy_all(WATCHLIST, verbose=True)
        elif len(args) >= 2:
            sym = args[1].upper()
            print(f"\n{'═'*60}")
            print(f"  MA STRATEGY BACKTEST: {sym}")
            print(f"{'═'*60}")
            run_ma_strategy_backtest(sym, 'MA10',     tp_pct=0.08, sl_pct=0.05, hold_days=15)
            run_ma_strategy_backtest(sym, 'MA50',     tp_pct=0.25, sl_pct=0.08, hold_days=40)
            run_ma_strategy_backtest(sym, 'COMBINED', tp_pct=0.10, sl_pct=0.05, hold_days=15)
        else:
            print("Dùng: python backtest.py --ma MBB  hoặc  python backtest.py --ma --all")
        sys.exit(0)
    elif args[0] == '--all':
        print(f"Chạy backtest toàn bộ {len(WATCHLIST)} mã watchlist...\n")
        symbols = WATCHLIST
    else:
        symbols = [s.upper() for s in args]

    if len(symbols) == 1:
        run_backtest_symbol(symbols[0], verbose=True)
    else:
        all_results = {}
        for sym in symbols:
            res = run_backtest_symbol(sym, verbose=True)
            if res and res.get('buy'):
                all_results[sym] = res
        if len(all_results) >= 2:
            cross_symbol_conclusion(all_results)
        elif len(all_results) == 1:
            print("\n(Chỉ có 1 mã thành công — bỏ qua bảng kết luận chéo)")


# ═══════════════════════════════════════════════════════════════════════════════
# WALK-FORWARD ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════
# Thiết kế:
#   - Chia dữ liệu thành các cửa sổ cuộn: IS (2 năm) → OOS (1 năm)
#   - IS : tìm ngưỡng score tối ưu → ngưỡng nào cho WR×PnL cao nhất
#   - OOS: chạy backtest thực tế với ngưỡng vừa tìm được
#   - So sánh IS vs OOS để phát hiện overfitting
#
# Ví dụ 7 năm (2019-2025), IS=2yr OOS=1yr:
#   W1: IS=2019-2020 → OOS=2021
#   W2: IS=2020-2021 → OOS=2022
#   W3: IS=2021-2022 → OOS=2023
#   W4: IS=2022-2023 → OOS=2024
#   W5: IS=2023-2024 → OOS=2025

WF_IS_YEARS  = 2   # Cửa sổ In-Sample (năm)
WF_OOS_YEARS = 1   # Cửa sổ Out-of-Sample (năm)
# TTCK VN thực tế ~245-252 phiên/năm (bỏ T7, CN, nghỉ lễ ~10 ngày)
WF_IS_DAYS   = WF_IS_YEARS  * 252
WF_OOS_DAYS  = WF_OOS_YEARS * 252


def _run_window(closes, highs, lows, volumes, dates,
                start_idx, end_idx, _sl, _tp, _min_score):
    """Chạy backtest trên 1 đoạn [start_idx, end_idx), trả về stats.
    Đồng bộ với run_backtest_symbol: cooldown HOLD+SETTLEMENT, start>=200."""
    trades = []
    last_signal = start_idx - (HOLD_DAYS + SETTLEMENT_DAYS)
    # Cần tối thiểu 200 nến để tính MA200 — khởi đầu từ max(start, 200)
    for i in range(max(start_idx, 200), end_idx - HOLD_DAYS - SETTLEMENT_DAYS):
        if i - last_signal < HOLD_DAYS + SETTLEMENT_DAYS:
            continue
        score, action = compute_score_at(closes, highs, lows, volumes, i)
        if action == 'MUA' and score < _min_score:
            action = 'THEO DOI'
        if action not in ('MUA', 'BAN'):
            continue
        pnl, reason, days_held = simulate_trade(closes, i, action, sl=_sl, tp=_tp)
        ts = dates.iloc[i] if i < len(dates) else pd.NaT
        trade_date = ts.strftime('%Y-%m-%d') if pd.notna(ts) else f'idx_{i}'
        trades.append({
            'date': trade_date, 'score': score,
            'action': action,   'pnl': pnl,
            'reason': reason,   'days': days_held,
        })
        last_signal = i

    if not trades:
        return None
    df = pd.DataFrame(trades)
    buy = df[df['action'] == 'MUA']
    if buy.empty:
        return None
    return calc_stats(buy)


def _find_best_threshold(closes, highs, lows, volumes, dates,
                         start_idx, end_idx, _sl, _tp,
                         thresholds=None):
    """
    Tìm ngưỡng score tối ưu trong cửa sổ IS.
    Tiêu chí: WR × PnL TB (có phạt nếu < 8 lệnh).
    Đồng bộ cooldown và start_idx với _run_window.
    """
    if thresholds is None:
        thresholds = [60, 65, 68, 70, 72, 75, 78, 80]
    best_thr    = 65
    best_metric = -999
    results     = {}
    for thr in thresholds:
        trades = []
        last_signal = start_idx - (HOLD_DAYS + SETTLEMENT_DAYS)
        for i in range(max(start_idx, 200), end_idx - HOLD_DAYS - SETTLEMENT_DAYS):
            if i - last_signal < HOLD_DAYS + SETTLEMENT_DAYS:
                continue
            score, action = compute_score_at(closes, highs, lows, volumes, i)
            if action == 'MUA' and score < thr:
                action = 'THEO DOI'
            if action != 'MUA':
                continue
            pnl, reason, dh = simulate_trade(closes, i, 'MUA', sl=_sl, tp=_tp)
            trades.append({'score': score, 'pnl': pnl, 'reason': reason, 'days': dh})
            last_signal = i
        if len(trades) < 5:
            continue
        df_t = pd.DataFrame(trades)
        wins = len(df_t[df_t['pnl'] > 0])
        wr   = wins / len(df_t) * 100
        avg  = df_t['pnl'].mean()
        pen  = 0.8 if len(df_t) < 8 else 1.0
        m    = (wr / 100) * avg * pen
        results[thr] = {'total': len(df_t), 'win_rate': round(wr, 1),
                        'avg_pnl': round(avg, 2), 'metric': round(m, 3)}
        if m > best_metric:
            best_metric = m
            best_thr    = thr
    return best_thr, results


def run_walk_forward(symbol, verbose=True):
    """
    Walk-Forward Analysis cho 1 mã.
    Trả về dict kết quả hoặc None nếu không đủ dữ liệu.
    """
    cfg        = SYMBOL_CONFIG.get(symbol.upper(), {})
    _sl        = cfg.get('sl',        abs(STOP_LOSS))
    _tp        = cfg.get('tp',        TAKE_PROFIT)
    _min_score = cfg.get('min_score', MIN_SCORE_BUY)

    df, source = load_data(symbol, days=LOOKBACK_DAYS)
    if df is None:
        if verbose: print(f"  ✗ Không tải được dữ liệu {symbol}")
        return None

    # Chuẩn bị arrays
    def to_arr(s):
        return pd.to_numeric(s, errors='coerce').fillna(0).astype(float).values.copy()

    cc = find_col(df, ['close', 'closeprice', 'close_price'])
    hc = find_col(df, ['high',  'highprice',  'high_price'])
    lc = find_col(df, ['low',   'lowprice',   'low_price'])
    vc = next((c for c in df.columns if c.lower() in {
        'volume','volume_match','klgd','vol','trading_volume',
        'match_volume','total_volume','dealvolume','matchingvolume'}), None)
    if cc is None:
        return None

    closes  = to_arr(df[cc]);  closes  = np.where(closes  < 1000, closes  * 1000, closes).copy()
    highs   = to_arr(df[hc]) if hc else closes.copy()
    lows    = to_arr(df[lc]) if lc else closes.copy()
    if hc: highs  = np.where(highs  < 1000, highs  * 1000, highs).copy()
    if lc: lows[lows   < 1000] *= 1000
    volumes = to_arr(df[vc]) if vc else np.zeros(len(closes))

    _time_col = next(
        (c for c in df.columns if c.lower() in ('time','date','datetime','trading_date')), None)
    if _time_col:
        dates = pd.to_datetime(df[_time_col], errors='coerce').reset_index(drop=True)
    elif isinstance(df.index, pd.DatetimeIndex):
        dates = pd.Series(df.index, dtype='datetime64[ns]').reset_index(drop=True)
    else:
        dates = pd.Series([pd.NaT] * len(closes))

    n = len(closes)
    window_size = WF_IS_DAYS + WF_OOS_DAYS
    # +200 vì compute_score_at cần tối thiểu 200 nến để tính MA200/weekly trend
    if n < window_size + 200:
        if verbose: print(f"  ✗ Không đủ dữ liệu walk-forward (cần ~{window_size+200} nến)")
        return None

    SEP = '─' * 72
    if verbose:
        print(f"\n{'═'*72}")
        print(f"  WALK-FORWARD ANALYSIS: {symbol}")
        print(f"  IS={WF_IS_YEARS}năm → OOS={WF_OOS_YEARS}năm | "
              f"SL={_sl*100:.0f}% TP={_tp*100:.0f}% | {n} nến từ {source}")
        print(f"{'═'*72}")

    windows     = []
    oos_trades  = []  # Gom tất cả lệnh OOS để tính tổng kết

    # Tạo cửa sổ cuộn: bước 1 năm (250 nến)
    step   = WF_OOS_DAYS
    starts = range(0, n - window_size, step)

    for w_idx, s in enumerate(starts, 1):
        is_start  = s
        is_end    = s + WF_IS_DAYS
        oos_start = is_end
        oos_end   = min(is_end + WF_OOS_DAYS, n)

        if oos_end - oos_start < 30:
            continue

        # Lấy label năm cho IS và OOS
        is_date_start  = dates.iloc[is_start]  if pd.notna(dates.iloc[is_start])  else None
        oos_date_start = dates.iloc[oos_start] if pd.notna(dates.iloc[oos_start]) else None
        oos_date_end   = dates.iloc[oos_end-1] if pd.notna(dates.iloc[oos_end-1]) else None

        is_label  = (f"{is_date_start.year}"
                     f"–{dates.iloc[is_end-1].year}"
                     if is_date_start is not None else f"W{w_idx}-IS")
        oos_label = (f"{oos_date_start.year}"
                     if oos_date_start is not None else f"W{w_idx}-OOS")

        # ── IS: tìm ngưỡng tối ưu ──────────────────────────────────────────
        best_thr, is_results = _find_best_threshold(
            closes, highs, lows, volumes, dates,
            is_start, is_end, _sl, _tp
        )
        is_data = is_results.get(best_thr, {})

        # ── OOS: chạy với ngưỡng vừa tìm ─────────────────────────────────
        oos_stats = _run_window(
            closes, highs, lows, volumes, dates,
            oos_start, oos_end, _sl, _tp, best_thr
        )

        win_data = {
            'window':    w_idx,
            'is_label':  is_label,
            'oos_label': oos_label,
            'best_thr':  best_thr,
            'is_n':      is_data.get('total', 0),
            'is_wr':     is_data.get('win_rate', 0),
            'is_pnl':    is_data.get('avg_pnl', 0),
            'oos_n':     oos_stats['total']    if oos_stats else 0,
            'oos_wr':    oos_stats['win_rate'] if oos_stats else None,
            'oos_pnl':   oos_stats['avg_pnl']  if oos_stats else None,
            'oos_pf':    oos_stats['profit_factor'] if oos_stats else None,
        }
        windows.append(win_data)

        # Gom lệnh OOS để tính equity curve
        if oos_stats and oos_stats['total'] > 0:
            oos_trades.append(oos_stats)

        if verbose:
            oos_wr_s  = f"{win_data['oos_wr']:.1f}%" if win_data['oos_wr']  is not None else "  --  "
            oos_pnl_s = f"{win_data['oos_pnl']:+.2f}%" if win_data['oos_pnl'] is not None else "  --  "
            decay = ""
            if win_data['oos_wr'] is not None:
                d = win_data['is_wr'] - win_data['oos_wr']
                decay = f"  decay={d:+.1f}%"

            print(f"\n  W{w_idx} | IS: {is_label:10s} | OOS: {oos_label}")
            print(f"     IS  → Nguong toi uu: >={best_thr}  "
                  f"WR={win_data['is_wr']:.1f}%  PnL={win_data['is_pnl']:+.2f}%  "
                  f"({win_data['is_n']}L)")
            print(f"     OOS → Thuc te:        "
                  f"WR={oos_wr_s}  PnL={oos_pnl_s}  "
                  f"({win_data['oos_n']}L){decay}")

    if not windows:
        if verbose: print("  Không đủ cửa sổ để phân tích walk-forward.")
        return None

    # ── Tổng kết ──────────────────────────────────────────────────────────────
    valid_w   = [w for w in windows if w['oos_wr'] is not None]
    avg_is_wr = sum(w['is_wr']  for w in valid_w) / len(valid_w) if valid_w else 0
    avg_oo_wr = sum(w['oos_wr'] for w in valid_w) / len(valid_w) if valid_w else 0
    avg_is_pn = sum(w['is_pnl'] for w in valid_w) / len(valid_w) if valid_w else 0
    avg_oo_pn = sum(w['oos_pnl'] for w in valid_w) / len(valid_w) if valid_w else 0
    decay_wr  = avg_is_wr - avg_oo_wr

    # Kiểm tra tính nhất quán ngưỡng score
    thresholds_used = [w['best_thr'] for w in windows]
    thr_stable = max(thresholds_used) - min(thresholds_used) <= 10

    # Kết luận
    if avg_oo_wr >= 55 and avg_oo_pn > 0 and decay_wr <= 10:
        verdict = 'TOT — He thong robustness cao, OOS nhat quan voi IS'
        verdict_flag = 'V'
    elif avg_oo_wr >= 50 and avg_oo_pn >= 0 and decay_wr <= 20:
        verdict = 'CHAP NHAN — Hieu qua giam nhe khi ra OOS, van chap nhan'
        verdict_flag = '~'
    elif decay_wr > 25:
        verdict = 'CANH BAO — Gap lon IS vs OOS, co the overfitting'
        verdict_flag = '!'
    else:
        verdict = 'YEU — OOS khong xac nhan duoc ket qua IS'
        verdict_flag = 'X'

    if verbose:
        print(f"\n\n  {'─'*72}")
        print(f"  TỔNG KẾT WALK-FORWARD: {symbol}")
        print(f"  {'─'*72}")
        print(f"  {'Cửa sổ':>5} │ {'IS Thr':>6} │ {'IS WR':>6} │ {'IS PnL':>7} │ "
              f"{'OOS WR':>6} │ {'OOS PnL':>7} │ {'Decay':>6} │ Đánh giá")
        print(f"  {'─'*72}")
        for w in windows:
            if w['oos_wr'] is None:
                continue
            decay_w = w['is_wr'] - w['oos_wr']
            if   decay_w <= 5:  ev = 'V Nhat quan'
            elif decay_w <= 15: ev = '~ Giam nhe'
            elif decay_w <= 25: ev = '! Giam nhieu'
            else:               ev = 'X Overfit'
            w_label = f"W{w['window']} {w['oos_label']}"
            print(f"  {w_label:>8} │ "
                  f"  >={w['best_thr']:>2} │ {w['is_wr']:>5.1f}% │ "
                  f"{w['is_pnl']:>+6.2f}% │ {w['oos_wr']:>5.1f}% │ "
                  f"{w['oos_pnl']:>+6.2f}% │ {decay_w:>+5.1f}% │ {ev}")

        print(f"\n  Trung bình IS : WR={avg_is_wr:.1f}%  PnL={avg_is_pn:+.2f}%")
        print(f"  Trung bình OOS: WR={avg_oo_wr:.1f}%  PnL={avg_oo_pn:+.2f}%")
        print(f"  Decay WR TB   : {decay_wr:+.1f}%")
        print(f"  Ngưỡng score  : {thresholds_used} → "
              f"{'ổn định' if thr_stable else 'BIẾN ĐỘNG — ngưỡng không nhất quán'}")
        print(f"\n  [{verdict_flag}] {verdict}")

    return {
        'symbol':      symbol,
        'windows':     windows,
        'avg_is_wr':   round(avg_is_wr, 1),
        'avg_oos_wr':  round(avg_oo_wr, 1),
        'avg_is_pnl':  round(avg_is_pn, 2),
        'avg_oos_pnl': round(avg_oo_pn, 2),
        'decay_wr':    round(decay_wr, 1),
        'thr_stable':  thr_stable,
        'thresholds':  thresholds_used,
        'verdict':     verdict_flag,
        'verdict_txt': verdict,
        'sl':          _sl,
        'tp':          _tp,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# LOOKAHEAD BIAS CHECK
# ═══════════════════════════════════════════════════════════════════════════════
# Kiểm tra 3 dạng lookahead bias phổ biến trong backtest:
#
#   [1] SIGNAL BIAS: compute_score_at(idx) có dùng closes[idx+1..] không?
#       → Test: thay closes[idx] bằng giá bất thường, score phải thay đổi
#       → Nếu score không đổi khi thay closes[idx] → hàm không dùng closes[idx] → bug
#
#   [2] ENTRY PRICE BIAS: mua tại closes[i] (giá đóng cửa ngày signal)
#       → Thực tế: signal phát cuối ngày i, chỉ mua được ngày i+1
#       → closes[i] đã biết khi xử lý, nhưng là "future" so với lúc quyết định
#       → Đây là structural bias — tính chênh lệch PnL(entry=i) vs PnL(entry=i+1)
#
#   [3] FORWARD-LOOK trong indicators: MA, RSI tính trên close[i]
#       → Close[i] là giá CUỐI phiên, chỉ biết sau khi phiên kết thúc → OK
#       → Không có bias nếu chỉ dùng close, không dùng intraday data

BIAS_LOOKAHEAD_N = 10


def run_lookahead_check(symbol, verbose=True):
    """
    Kiểm tra 3 dạng lookahead bias. Trả về dict kết quả.
    """
    cfg        = SYMBOL_CONFIG.get(symbol.upper(), {})
    _sl        = cfg.get('sl',        abs(STOP_LOSS))
    _tp        = cfg.get('tp',        TAKE_PROFIT)
    _min_score = cfg.get('min_score', MIN_SCORE_BUY)

    df, source = load_data(symbol, days=LOOKBACK_DAYS)
    if df is None:
        if verbose: print(f"  ✗ Không tải được dữ liệu {symbol}")
        return None

    def to_arr(s):
        return pd.to_numeric(s, errors='coerce').fillna(0).astype(float).values.copy()

    cc = find_col(df, ['close','closeprice','close_price'])
    hc = find_col(df, ['high', 'highprice', 'high_price'])
    lc = find_col(df, ['low',  'lowprice',  'low_price'])
    vc = next((c for c in df.columns if c.lower() in {
        'volume','volume_match','klgd','vol','trading_volume',
        'match_volume','total_volume','dealvolume','matchingvolume'}), None)
    if cc is None:
        return None

    closes  = to_arr(df[cc]);  closes  = np.where(closes  < 1000, closes  * 1000, closes).copy()
    highs   = to_arr(df[hc]) if hc else closes.copy()
    lows    = to_arr(df[lc]) if lc else closes.copy()
    if hc: highs  = np.where(highs  < 1000, highs  * 1000, highs).copy()
    if lc: lows[lows   < 1000] *= 1000
    volumes = to_arr(df[vc]) if vc else np.zeros(len(closes))
    n = len(closes)

    if verbose:
        print(f"\n{'═'*60}")
        print(f"  LOOKAHEAD BIAS CHECK: {symbol}")
        print(f"{'═'*60}")

    # ── Lấy 150 điểm mẫu trải đều ───────────────────────────────────────────
    sample_idxs = list(range(60, n - HOLD_DAYS - SETTLEMENT_DAYS - 5,
                              max(1, (n - 60) // 150)))

    # ═══ CHECK 1: SIGNAL BIAS ════════════════════════════════════════════════
    # Kiểm tra compute_score_at(idx) có thực sự dùng closes[idx] không
    # Nếu thay closes[idx] bằng giá cực đoan → score PHẢI thay đổi (hàm đúng)
    # Nếu score KHÔNG đổi → hàm bỏ qua closes[idx] → signal không phụ thuộc ngày đó
    signal_bias_count = 0
    signal_total      = 0

    for idx in sample_idxs:
        original_score, _ = compute_score_at(closes, highs, lows, volumes, idx)

        # Thay closes[idx] bằng giá cực đoan (x10 và x0.1)
        c_mod = closes.copy()
        c_mod[idx] = closes[idx] * 10   # Giá tăng 10x bất thường
        score_up, _ = compute_score_at(c_mod, highs, lows, volumes, idx)

        c_mod[idx] = closes[idx] * 0.1  # Giá giảm 90% bất thường
        score_dn, _ = compute_score_at(c_mod, highs, lows, volumes, idx)

        signal_total += 1
        # Nếu score không thay đổi khi giá thay đổi cực đoan → KHÔNG dùng closes[idx]
        if score_up == original_score and score_dn == original_score:
            signal_bias_count += 1

    signal_ok_rate = (signal_total - signal_bias_count) / signal_total * 100 if signal_total > 0 else 0

    # ═══ CHECK 2: ENTRY PRICE BIAS ═══════════════════════════════════════════
    # So sánh PnL khi entry = closes[i] (hiện tại) vs closes[i+1] (thực tế)
    entry_diffs = []
    entry_action_flips = 0
    entry_total = 0

    for idx in sample_idxs:
        if idx + 1 >= n - HOLD_DAYS - SETTLEMENT_DAYS:
            continue
        score, action = compute_score_at(closes, highs, lows, volumes, idx)
        if action == 'MUA' and score < _min_score:
            action = 'THEO DOI'
        if action != 'MUA':
            continue

        entry_total += 1
        # PnL với entry = closes[i] (backtest hiện tại — có thể bias)
        pnl_current, _, _ = simulate_trade(closes, idx,   'MUA', sl=_sl, tp=_tp)
        # PnL với entry = closes[i+1] (thực tế — mua ngày hôm sau)
        pnl_next,    _, _ = simulate_trade(closes, idx+1, 'MUA', sl=_sl, tp=_tp)

        diff = abs(pnl_current - pnl_next)
        entry_diffs.append(diff)

        # Kiểm tra flip: entry[i] thắng nhưng entry[i+1] thua (hoặc ngược lại)
        if (pnl_current > 0) != (pnl_next > 0):
            entry_action_flips += 1

    avg_entry_diff  = float(np.mean(entry_diffs)) if entry_diffs else 0
    entry_flip_rate = entry_action_flips / entry_total * 100 if entry_total > 0 else 0

    # ═══ CHECK 3: FORWARD-LOOK trong indicators ════════════════════════════
    # Kiểm tra xem EMA/MA có dùng data tương lai không
    # Tính EMA tại idx với array đầy đủ vs array cắt tại idx
    indicator_bias_count = 0
    indicator_total      = 0

    for idx in sample_idxs[::3]:  # Kiểm tra 1/3 sample (đủ nhanh)
        if idx < 60:
            continue
        # EMA tính trên full array vs array cắt — nếu đúng thì giống nhau
        c_full = closes
        c_cut  = closes[:idx + 1]
        ema_full = ema_arr(c_full, 12)
        ema_cut  = ema_arr(c_cut,  12)
        indicator_total += 1
        # EMA[idx] trên full array vs EMA[-1] trên array cắt
        if abs(ema_full[idx] - ema_cut[-1]) > 0.1:
            indicator_bias_count += 1

    indicator_ok_rate = (indicator_total - indicator_bias_count) / indicator_total * 100 if indicator_total > 0 else 0

    # ═══ Kết luận tổng hợp ═══════════════════════════════════════════════════
    issues = []
    if signal_bias_count > signal_total * 0.1:
        issues.append(f'SIGNAL: {signal_bias_count} diem score khong phu thuoc closes[idx]')
    if entry_flip_rate > 20:
        issues.append(f'ENTRY: {entry_flip_rate:.1f}% lenh flip win/loss khi doi entry sang ngay+1')
    if indicator_bias_count > 0:
        issues.append(f'INDICATOR: EMA/MA co sai lech khi dung full array vs cut array')

    if not issues:
        verdict     = 'SACH — Khong phat hien lookahead bias'
        verdict_flag = 'V'
    elif len(issues) == 1 and entry_flip_rate <= 30:
        verdict      = 'CANH BAO NHE — Co structural entry bias (mua T vs T+1)'
        verdict_flag = '~'
    else:
        verdict      = 'CO VAN DE — Can kiem tra lai logic backtest'
        verdict_flag = '!'

    if verbose:
        print(f"\n  [1] SIGNAL BIAS (score phu thuoc closes[idx]?):")
        print(f"      Kiem tra : {signal_total} diem")
        print(f"      Score phu thuoc closes[idx]: {signal_total - signal_bias_count}/{signal_total} ({signal_ok_rate:.1f}%)")
        if signal_bias_count > 0:
            print(f"      !! {signal_bias_count} diem score KHONG doi khi gia thay doi cuc doan")
        else:
            print(f"      V  Score luon thay doi khi gia thay doi → KHONG co signal bias")

        print(f"\n  [2] ENTRY PRICE BIAS (mua T vs T+1):")
        print(f"      So lenh MUA kiem tra: {entry_total}")
        if entry_total > 0:
            print(f"      Chenh lech PnL TB   : {avg_entry_diff:.2f}%")
            print(f"      Lenh flip win/loss  : {entry_action_flips}/{entry_total} ({entry_flip_rate:.1f}%)")
            if entry_flip_rate > 20:
                print(f"      !! Nhieu lenh doi ket qua khi doi entry → entry price co anh huong lon")
            else:
                print(f"      ~ Structural bias nhe — binh thuong trong backtest EOD (< 20%)")

        print(f"\n  [3] INDICATOR BIAS (EMA/MA tren full vs cut array):")
        print(f"      Kiem tra : {indicator_total} diem")
        if indicator_bias_count == 0:
            print(f"      V  EMA nhat quan — KHONG co indicator lookahead bias")
        else:
            print(f"      !! {indicator_bias_count} diem EMA sai lech → co indicator bias")

        print(f"\n  Van de phat hien: {len(issues)}")
        for iss in issues:
            print(f"    - {iss}")
        print(f"\n  [{verdict_flag}] {verdict}")

    return {
        'symbol':           symbol,
        'signal_total':     signal_total,
        'signal_bias':      signal_bias_count,
        'signal_ok_rate':   round(signal_ok_rate, 1),
        'entry_total':      entry_total,
        'entry_flip_rate':  round(entry_flip_rate, 1),
        'avg_entry_diff':   round(avg_entry_diff, 2),
        'indicator_bias':   indicator_bias_count,
        'indicator_total':  indicator_total,
        'issues':           issues,
        'verdict':          verdict_flag,
        'verdict_txt':      verdict,
    }

# ═══════════════════════════════════════════════════════════════════════════════
# MA STRATEGY BACKTEST — Chiến lược MA10 cross và MA50 uptrend độc lập
# ═══════════════════════════════════════════════════════════════════════════════
# Mục đích: test thuần túy 2 chiến lược MA TRƯỚC khi quyết định tích hợp vào
# scoring tổng hợp. Không ảnh hưởng compute_score_at hay kết quả backtest hiện tại.
#
# Chiến lược A (MA10 cross): Mua khi giá cắt lên MA10 + MA10 dốc lên, TP 7-10%
# Chiến lược B (MA50 trend): Mua khi giá trên MA50 + MA50 dốc lên, TP 25-30%


def _calc_ma_signals(closes, highs, lows, idx):
    """
    Tính MA10/MA50 signals tại điểm idx.
    Trả về dict với đầy đủ thông tin để quyết định vào lệnh.
    """
    c = closes[:idx + 1]
    if len(c) < 50:
        return None

    ma10      = float(np.mean(c[-10:]))
    ma10_prev = float(np.mean(c[-11:-1])) if len(c) >= 11 else ma10
    ma50      = float(np.mean(c[-50:]))   if len(c) >= 50 else float(np.mean(c))
    ma50_prev = float(np.mean(c[-53:-3])) if len(c) >= 53 else ma50
    price     = float(c[-1])
    prev      = float(c[-2]) if len(c) >= 2 else price

    return {
        'price':          price,
        'ma10':           ma10,
        'ma50':           ma50,
        'ma10_cross_up':  prev < ma10_prev and price > ma10,
        'ma10_cross_down':prev > ma10_prev and price < ma10,
        'ma10_slope_up':  ma10 > ma10_prev,
        'above_ma10':     price > ma10,
        'above_ma50':     price > ma50,
        'ma50_slope_up':  ma50 > ma50_prev,
    }


def run_ma_strategy_backtest(symbol, strategy='MA10',
                             tp_pct=0.08, sl_pct=0.05,
                             hold_days=15, verbose=True):
    """
    Backtest chiến lược MA độc lập.

    strategy: 'MA10' — mua khi giá cắt lên MA10 + MA10 dốc lên
              'MA50' — mua khi giá lần đầu lên trên MA50 + MA50 dốc lên
              'COMBINED' — MA10 cross up VÀ đang trên MA50

    tp_pct  : take profit % (default 8% cho MA10, 25% cho MA50)
    sl_pct  : stop loss %   (default 5%)
    hold_days: số phiên nắm giữ tối đa
    """
    df, source = load_data(symbol, days=LOOKBACK_DAYS)
    if df is None:
        if verbose: print(f"  ✗ Không tải được dữ liệu {symbol}")
        return None

    def to_arr(s):
        return pd.to_numeric(s, errors='coerce').fillna(0).astype(float).values.copy()

    cc = find_col(df, ['close', 'closeprice', 'close_price'])
    hc = find_col(df, ['high',  'highprice',  'high_price'])
    lc = find_col(df, ['low',   'lowprice',   'low_price'])
    if cc is None: return None

    closes = to_arr(df[cc]); closes = np.where(closes < 1000, closes * 1000, closes).copy()
    highs  = to_arr(df[hc]) if hc else closes.copy()
    lows   = to_arr(df[lc]) if lc else closes.copy()
    if hc: highs = np.where(highs < 1000, highs * 1000, highs).copy()
    if lc: lows  = np.where(lows  < 1000, lows  * 1000, lows).copy()

    _time_col = next(
        (c for c in df.columns if c.lower() in ('time','date','datetime','trading_date')), None)
    if _time_col:
        dates = pd.to_datetime(df[_time_col], errors='coerce').reset_index(drop=True)
    elif isinstance(df.index, pd.DatetimeIndex):
        dates = pd.Series(df.index, dtype='datetime64[ns]').reset_index(drop=True)
    else:
        dates = pd.Series([pd.NaT] * len(closes))

    n = len(closes)
    total_days = SETTLEMENT_DAYS + hold_days
    trades = []
    last_signal = -(hold_days + SETTLEMENT_DAYS)
    prev_above_ma50 = False  # Theo dõi trạng thái MA50 để phát hiện cross

    for i in range(55, n - total_days):
        if i - last_signal < hold_days + SETTLEMENT_DAYS:
            continue

        sig = _calc_ma_signals(closes, highs, lows, i)
        if sig is None:
            continue

        entry_signal = False

        if strategy == 'MA10':
            # Mua khi: vừa cắt lên MA10 VÀ MA10 đang dốc lên
            entry_signal = sig['ma10_cross_up'] and sig['ma10_slope_up']

        elif strategy == 'MA50':
            # Mua khi: lần đầu lên trên MA50 (cross up) VÀ MA50 dốc lên
            ma50_cross_up = not prev_above_ma50 and sig['above_ma50']
            entry_signal  = ma50_cross_up and sig['ma50_slope_up']
            prev_above_ma50 = sig['above_ma50']

        elif strategy == 'COMBINED':
            # Mua khi: MA10 cross up VÀ đang trên MA50 (xu hướng trung hạn tốt)
            entry_signal = (sig['ma10_cross_up'] and sig['ma10_slope_up']
                            and sig['above_ma50'] and sig['ma50_slope_up'])

        if not entry_signal:
            if strategy == 'MA50':
                prev_above_ma50 = sig['above_ma50']
            continue

        # Vào lệnh MUA — T+3 settlement
        entry_idx = i + 1 if i + 1 < n else i
        pnl, reason, days_held = simulate_trade(
            closes, entry_idx, 'MUA', sl=sl_pct, tp=tp_pct
        )
        ts = dates.iloc[i] if i < len(dates) else pd.NaT
        trade_date = ts.strftime('%Y-%m-%d') if pd.notna(ts) else f'idx_{i}'

        trades.append({
            'date':   trade_date,
            'price':  round(sig['price'], 0),
            'ma10':   round(sig['ma10'], 0),
            'ma50':   round(sig['ma50'], 0),
            'pnl':    pnl,
            'reason': reason,
            'days':   days_held,
        })
        last_signal = i

        if strategy == 'MA50':
            prev_above_ma50 = sig['above_ma50']

    if not trades:
        if verbose: print(f"  {symbol}/{strategy}: Không có tín hiệu nào")
        return None

    df_t = pd.DataFrame(trades)
    stats = calc_stats(df_t)

    if verbose:
        strat_label = {
            'MA10':     f'MA10 Cross Up (TP={tp_pct*100:.0f}% SL={sl_pct*100:.0f}%)',
            'MA50':     f'MA50 Uptrend  (TP={tp_pct*100:.0f}% SL={sl_pct*100:.0f}%)',
            'COMBINED': f'MA10+MA50 Kết hợp (TP={tp_pct*100:.0f}% SL={sl_pct*100:.0f}%)',
        }.get(strategy, strategy)

        pf_s = f"{stats['profit_factor']:.2f}" if stats['profit_factor'] != float('inf') else 'inf'
        print(f"\n  {'─'*60}")
        print(f"  MA STRATEGY: {symbol} — {strat_label}")
        print(f"  {'─'*60}")
        print(f"  Lệnh: {stats['total']} | WR: {stats['win_rate']:.1f}% | "
              f"PnL TB: {stats['avg_pnl']:+.2f}% | PF: {pf_s}")
        print(f"  TP: {stats['tp']} | SL: {stats['sl']} | HK: {stats['expired']}")

        # Phân tích theo năm
        df_t['year'] = pd.to_datetime(df_t['date'], errors='coerce').dt.year
        for yr in sorted(df_t['year'].dropna().unique()):
            yr_df = df_t[df_t['year'] == yr]
            yr_st = calc_stats(yr_df)
            phase = MARKET_PHASES.get(int(yr), '---')
            flag  = '✓' if yr_st['win_rate'] >= 55 else ('✗' if yr_st['win_rate'] < 45 else '·')
            print(f"  {flag} {int(yr)}: WR={yr_st['win_rate']:>5.1f}% "
                  f"PnL={yr_st['avg_pnl']:>+6.2f}% ({yr_st['total']}L) — {phase}")

        # 5 lệnh gần nhất
        print(f"\n  5 lệnh gần nhất:")
        for _, r in df_t.tail(5).iterrows():
            icon = '✓' if r['pnl'] > 0 else '✗'
            print(f"  {icon} {r['date']}  @{r['price']:>9,.0f}  "
                  f"MA10={r['ma10']:>9,.0f}  PnL={r['pnl']:>+6.1f}%  ({r['reason']})")

    return {
        'symbol':   symbol,
        'strategy': strategy,
        'tp':       tp_pct,
        'sl':       sl_pct,
        'stats':    stats,
        'trades':   df_t,
    }


def run_ma_strategy_all(symbols=None, verbose=True):
    """
    Chạy cả 3 chiến lược MA cho toàn bộ danh sách mã.
    So sánh MA10, MA50, COMBINED và hệ thống score tổng hợp.
    """
    if symbols is None:
        symbols = WATCHLIST

    print(f"\n{'═'*72}")
    print(f"  MA STRATEGY COMPARISON — {len(symbols)} mã")
    print(f"  MA10: TP=8% SL=5%  |  MA50: TP=25% SL=8%  |  COMBINED: TP=10% SL=5%")
    print(f"{'═'*72}")

    rows = []
    for sym in symbols:
        r10  = run_ma_strategy_backtest(sym, 'MA10',     tp_pct=0.08, sl_pct=0.05,
                                        hold_days=15, verbose=False)
        r50  = run_ma_strategy_backtest(sym, 'MA50',     tp_pct=0.25, sl_pct=0.08,
                                        hold_days=40, verbose=False)
        rcmb = run_ma_strategy_backtest(sym, 'COMBINED', tp_pct=0.10, sl_pct=0.05,
                                        hold_days=15, verbose=False)
        # So sánh với score tổng hợp hiện tại
        rbase = run_backtest_symbol(sym, verbose=False)
        base_wr = rbase['buy']['win_rate'] if rbase and rbase.get('buy') else 0

        row = {
            'symbol':   sym,
            'ma10_wr':  r10['stats']['win_rate']  if r10  else 0,
            'ma10_n':   r10['stats']['total']      if r10  else 0,
            'ma50_wr':  r50['stats']['win_rate']   if r50  else 0,
            'ma50_n':   r50['stats']['total']      if r50  else 0,
            'cmb_wr':   rcmb['stats']['win_rate']  if rcmb else 0,
            'cmb_n':    rcmb['stats']['total']     if rcmb else 0,
            'base_wr':  base_wr,
        }
        rows.append(row)

        if verbose:
            ma10_s  = f"{row['ma10_wr']:>4.1f}%({row['ma10_n']}L)" if r10  else "  N/A  "
            ma50_s  = f"{row['ma50_wr']:>4.1f}%({row['ma50_n']}L)" if r50  else "  N/A  "
            cmb_s   = f"{row['cmb_wr']:>4.1f}%({row['cmb_n']}L)"  if rcmb else "  N/A  "
            base_s  = f"{base_wr:>4.1f}%"                          if rbase else " N/A "
            best = max(row['ma10_wr'], row['ma50_wr'], row['cmb_wr'], base_wr)
            flag = ' ◄' if row['cmb_wr'] == best else ''
            print(f"  {sym:>5} | MA10:{ma10_s} | MA50:{ma50_s} | CMB:{cmb_s} | Score:{base_s}{flag}")

    if rows and verbose:
        avg10  = sum(r['ma10_wr']  for r in rows if r['ma10_wr']  > 0) / max(1, sum(1 for r in rows if r['ma10_wr']  > 0))
        avg50  = sum(r['ma50_wr']  for r in rows if r['ma50_wr']  > 0) / max(1, sum(1 for r in rows if r['ma50_wr']  > 0))
        avgcmb = sum(r['cmb_wr']   for r in rows if r['cmb_wr']   > 0) / max(1, sum(1 for r in rows if r['cmb_wr']   > 0))
        avgbas = sum(r['base_wr']  for r in rows if r['base_wr']  > 0) / max(1, sum(1 for r in rows if r['base_wr']  > 0))
        print(f"\n  {'─'*60}")
        print(f"  TB WR: MA10={avg10:.1f}% | MA50={avg50:.1f}% | COMBINED={avgcmb:.1f}% | Score={avgbas:.1f}%")

        winner = max([('MA10', avg10), ('MA50', avg50), ('COMBINED', avgcmb), ('Score', avgbas)],
                     key=lambda x: x[1])
        print(f"\n  ► Chiến lược hiệu quả nhất TB: {winner[0]} ({winner[1]:.1f}%)")
        print(f"  Lưu ý: Cần xem xét số lệnh, PF và nhất quán theo năm trước khi kết luận.")

    return rows
