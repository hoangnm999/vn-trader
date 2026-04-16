"""
backtest.py - Backtest engine cho VN Trader Bot
================================================
Functions chính:
  load_data(symbol, days)             — Load OHLCV từ vnstock
  compute_score_at(closes,highs,lows,volumes,i) — Score kỹ thuật tại candle i
  simulate_trade(closes,entry_idx,action,sl,tp,highs,lows) — Simulate 1 trade
  calc_stats(df_trades)               — Tính WR, PnL, PF từ trade list
  run_backtest_symbol(symbol, ...)    — Backtest full per-symbol
  run_walk_forward(symbol)            — Walk-Forward validation
  run_b_filter_comparison(symbol)     — So sánh có/không B-filter
  run_backtest_dual(symbol)           — Entry T vs T+1 comparison
  get_market_regime()                 — BULL/NEUTRAL/BEAR từ VNINDEX
  apply_regime_to_score(score,regime) — Áp regime penalty/cap
  compute_vwap_arrays(closes,volumes,dates) — VWAP tuần + tháng
  apply_vwap_bonus(score,price,vwap_w,vwap_m) — VWAP bonus/penalty
  run_optimize_symbol(symbol)         — Grid search SL/TP/Hold/Score
  get_wf_summary(symbol)              — WF summary có cache 24h
"""

import sys, math, warnings
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
warnings.filterwarnings('ignore')

from config import (
    SYMBOL_CONFIG, BACKTEST_WATCHLIST, ML_CONFIRMED_WATCHLIST,
    SETTLEMENT_DAYS, HOLD_DAYS, STOP_LOSS, TAKE_PROFIT,
    MIN_SCORE_BUY, MAX_SCORE_SELL, LOOKBACK_DAYS, COMMISSION,
)

# ─── MARKET PHASES (lịch sử thị trường VN) ────────────────────────────────────
MARKET_PHASES = {
    2017: 'BULL',
    2018: 'BEAR',
    2019: 'BULL',
    2020: 'VOLATILE',
    2021: 'BULL',
    2022: 'BEAR',
    2023: 'RECOVERY',
    2024: 'NEUTRAL',
    2025: 'BULL',
    2026: 'VOLATILE',
}

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def find_col(df, names):
    for c in df.columns:
        if c.lower() in names:
            return c
    return None

def load_data(symbol, days=LOOKBACK_DAYS):
    """Load dữ liệu daily OHLCV từ vnstock (VCI fallback KBS)."""
    from datetime import datetime, timedelta
    end   = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    for source in ['VCI', 'KBS']:
        try:
            from vnstock import Vnstock
            df = Vnstock().stock(symbol=symbol, source=source).quote.history(
                start=start, end=end, interval='1D'
            )
            if df is not None and len(df) >= 50:
                return df, source
        except Exception as e:
            print(f"  [{symbol}/{source}] lỗi: {e}")
    return None, None

def to_arr(series):
    return pd.to_numeric(series, errors='coerce').fillna(0).astype(float).values.copy()

# ─── TECHNICAL INDICATORS ─────────────────────────────────────────────────────

def _ema(arr, span):
    alpha = 2.0 / (span + 1)
    out = np.zeros(len(arr))
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = alpha * arr[i] + (1 - alpha) * out[i-1]
    return out

def _rsi_wilder(arr, p=14):
    """RSI với Wilder's Smoothing."""
    out = np.full(len(arr), 50.0)
    if len(arr) < p + 1:
        return out
    deltas = np.diff(arr)
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_g  = np.mean(gains[:p])
    avg_l  = np.mean(losses[:p])
    out[p] = 100.0 if avg_l == 0 else 100 - 100 / (1 + avg_g / avg_l)
    for i in range(p, len(deltas)):
        avg_g = (avg_g * (p-1) + gains[i]) / p
        avg_l = (avg_l * (p-1) + losses[i]) / p
        out[i+1] = 100.0 if avg_l == 0 else 100 - 100 / (1 + avg_g / avg_l)
    return out

def _macd(arr, fast=12, slow=26, signal=9):
    ema_f = _ema(arr, fast)
    ema_s = _ema(arr, slow)
    macd_line = ema_f - ema_s
    signal_line = _ema(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

def _bollinger(arr, period=20, std_mult=2.0):
    ma = pd.Series(arr).rolling(period).mean().values
    std = pd.Series(arr).rolling(period).std().values
    upper = ma + std_mult * std
    lower = ma - std_mult * std
    return upper, ma, lower

# ─── COMPUTE SCORE AT CANDLE i ────────────────────────────────────────────────

def compute_score_at(closes, highs, lows, volumes, i, opens=None):
    """
    Tính Score A tại candle i (0-100) và action MUA/BAN/THEO_DOI.
    Dùng dữ liệu closes[:i+1] — không look-ahead.
    """
    if i < 50:
        return 50, 'THEO_DOI'

    c = closes[:i+1]
    h = highs[:i+1]
    l = lows[:i+1]
    v = volumes[:i+1]
    price = c[-1]

    score = 50  # baseline

    # ── MA ───────────────────────────────────────────────────────────────────
    ma10  = np.mean(c[-10:])  if len(c) >= 10  else price
    ma20  = np.mean(c[-20:])  if len(c) >= 20  else price
    ma50  = np.mean(c[-50:])  if len(c) >= 50  else price
    ma200 = np.mean(c[-200:]) if len(c) >= 200 else price

    # MA score
    if price > ma10:  score += 3
    else:             score -= 3
    if price > ma20:  score += 4
    else:             score -= 4
    if price > ma50:  score += 5
    else:             score -= 5
    if ma20 > ma50:   score += 3
    else:             score -= 3
    # Golden/Death cross
    if len(c) >= 51:
        ma20_prev = np.mean(c[-21:-1])
        ma50_prev = np.mean(c[-51:-1])
        if ma20 > ma50 and ma20_prev <= ma50_prev:
            score += 8   # Golden cross
        elif ma20 < ma50 and ma20_prev >= ma50_prev:
            score -= 8   # Death cross

    # ── RSI ──────────────────────────────────────────────────────────────────
    rsi_arr = _rsi_wilder(c)
    rsi = rsi_arr[-1]
    if 55 <= rsi <= 70:   score += 8
    elif 45 <= rsi < 55:  score += 4
    elif 35 <= rsi < 45:  score += 1
    elif rsi < 35:        score -= 3
    elif rsi > 75:        score -= 2  # overbought

    # ── MACD ─────────────────────────────────────────────────────────────────
    if len(c) >= 35:
        _, sig_arr, hist_arr = _macd(c)
        if hist_arr[-1] > 0 and hist_arr[-2] <= 0:
            score += 8   # MACD cross up
        elif hist_arr[-1] < 0 and hist_arr[-2] >= 0:
            score -= 6   # MACD cross down
        elif hist_arr[-1] > 0:
            score += 4
        else:
            score -= 3

    # ── Volume ───────────────────────────────────────────────────────────────
    # SA-1 FIX: vol_ma20 dùng 20 phiên TRƯỚC hôm nay (loại v[-1]=hôm nay)
    # Trước: v[-20:] gồm hôm nay → vol spike tự inflate baseline → ratio bị pha loãng
    if len(v) >= 21:
        vol_ma20 = np.mean(v[-21:-1])   # 20 phiên trước, không gồm hôm nay
        vol_today = v[-1]
        vol_ratio = vol_today / vol_ma20 if vol_ma20 > 0 else 1.0
        if vol_ratio >= 2.0 and price > c[-2]:
            score += 8   # volume spike up
        elif vol_ratio >= 1.5 and price > c[-2]:
            score += 5
        elif vol_ratio < 0.5:
            score -= 2

    # ── Bollinger ────────────────────────────────────────────────────────────
    # SA-5 FIX: tính BB trên 20 phiên TRƯỚC (loại c[-1]=hôm nay tránh self-reference)
    # Sau đó so price hiện tại vs band đã tính từ lịch sử
    if len(c) >= 21:
        bb_up, bb_mid, bb_low = _bollinger(c[:-1])   # 20 phiên trước (không có hôm nay)
        bb_pos = (price - bb_low[-1]) / (bb_up[-1] - bb_low[-1]) if (bb_up[-1] - bb_low[-1]) > 0 else 0.5
        if bb_pos > 0.8:   score += 3
        elif bb_pos > 0.5: score += 1
        elif bb_pos < 0.2: score -= 2

    # ── Ichimoku (simplified) ────────────────────────────────────────────────
    if len(c) >= 52:
        tenkan = (np.max(h[-9:]) + np.min(l[-9:])) / 2
        kijun  = (np.max(h[-26:]) + np.min(l[-26:])) / 2
        # Senkou span A & B (cloud)
        span_a = (tenkan + kijun) / 2
        span_b = (np.max(h[-52:]) + np.min(l[-52:])) / 2
        cloud_top = max(span_a, span_b)
        cloud_bot = min(span_a, span_b)
        if price > cloud_top:  score += 4
        elif price < cloud_bot: score -= 4
        if tenkan > kijun:     score += 2
        else:                  score -= 2

    # ── Support/Resistance (pivot) ───────────────────────────────────────────
    if len(c) >= 20:
        recent_low  = np.min(l[-20:])
        recent_high = np.max(h[-20:])
        dist_to_support = (price - recent_low) / price if price > 0 else 0
        if dist_to_support < 0.03:  score += 3  # near support

    score = max(0, min(100, score))
    if   score >= MIN_SCORE_BUY:  action = 'MUA'
    elif score <= MAX_SCORE_SELL: action = 'BAN'
    else:                         action = 'THEO_DOI'

    # ── Sprint 4: ADX, BB Squeeze, OBV, ROC, MA50 Slope ─────────────────────
    try:
        # ADX(14)
        if len(closes) >= 28:
            _hi = highs[max(0,i-27):i+1]
            _lo = lows[max(0,i-27):i+1]
            _cl = closes[max(0,i-27):i+1]
            if len(_hi) >= 15:
                _tr  = np.maximum(np.maximum(_hi[1:]-_lo[1:],
                           np.abs(_hi[1:]-_cl[:-1])), np.abs(_lo[1:]-_cl[:-1]))
                _dmp = np.where((_hi[1:]-_hi[:-1])>(_lo[:-1]-_lo[1:]),
                                np.maximum(_hi[1:]-_hi[:-1],0),0.0)
                _dmn = np.where((_lo[:-1]-_lo[1:])>(_hi[1:]-_hi[:-1]),
                                np.maximum(_lo[:-1]-_lo[1:],0),0.0)
                _p   = min(14, len(_tr))
                _atr = float(np.mean(_tr[-_p:]))
                _dip = float(np.mean(_dmp[-_p:])) / _atr * 100 if _atr > 0 else 0
                _din = float(np.mean(_dmn[-_p:])) / _atr * 100 if _atr > 0 else 0
                _adx = abs(_dip-_din)/(_dip+_din)*100 if (_dip+_din)>0 else 0
                if _adx >= 35:
                    _ds = +5 if closes[i] > float(np.mean(closes[max(0,i-50):i+1])) else -5
                elif _adx >= 25:
                    _ds = +3 if closes[i] > float(np.mean(closes[max(0,i-50):i+1])) else -3
                else:
                    _ds = 0
                score = max(0, min(100, score + _ds))

        # BB Squeeze
        # FIX-3: tính BB width lịch sử trên i-1 (không gồm hôm nay)
        # tránh self-reference: BB width hôm nay nằm trong chuỗi tính percentile
        if i >= 40:
            _bws = []
            for _k in range(max(0,i-20), i):  # đến i-1, không gồm i
                _w = float(np.std(closes[max(0,_k-19):_k+1])) * 4
                _m = float(np.mean(closes[max(0,_k-19):_k+1]))
                _bws.append(_w/_m*100 if _m>0 else 0)
            # BB width hiện tại tính riêng, so với percentile của lịch sử
            _w_now = float(np.std(closes[max(0,i-19):i+1])) * 4
            _m_now = float(np.mean(closes[max(0,i-19):i+1]))
            _bw_now = _w_now/_m_now*100 if _m_now>0 else 0
            if len(_bws) >= 20:
                _sq = _bw_now < float(np.percentile(_bws, 20))
                if _sq:
                    score = max(0, min(100, score + 4))

        # OBV Divergence (20 phiên)
        if i >= 20:
            _obv = np.zeros(20)
            for _k in range(1, 20):
                _s = 1 if closes[i-20+_k]>closes[i-21+_k] else (-1 if closes[i-20+_k]<closes[i-21+_k] else 0)
                _obv[_k] = _obv[_k-1] + _s * volumes[i-20+_k]
            _of = float(np.mean(_obv[:10]))
            _ol = float(np.mean(_obv[10:]))
            _pf = float(np.mean(closes[i-20:i-10]))
            _pl = float(np.mean(closes[i-10:i+1]))
            if _pl < _pf and _ol > _of:
                score = max(0, min(100, score + 5))   # bullish div
            elif _pl > _pf and _ol < _of:
                score = max(0, min(100, score - 4))   # bearish div

        # ROC(10)
        if i >= 10:
            _roc = (closes[i]/closes[i-10]-1)*100
            if _roc > 8:      score = max(0, min(100, score + 5))
            elif _roc > 3:    score = max(0, min(100, score + 3))
            elif _roc < -8:   score = max(0, min(100, score - 4))
            elif _roc < -3:   score = max(0, min(100, score - 2))

        # MA50 Slope
        # FIX-4: dùng 2 window MA10 không overlap để đo slope thực
        # MA10 hiện tại (10 bars gần nhất) vs MA10 cách đây 10 bars
        # → slope thật, không bị dilute bởi 40 bars overlap
        if i >= 20:
            _ma10n = float(np.mean(closes[i-9:i+1]))    # MA10 hiện tại
            _ma10p = float(np.mean(closes[i-19:i-9]))   # MA10 cách đây 10 bars
            _sl = (_ma10n/_ma10p-1)*100 if _ma10p>0 else 0
            if _sl > 1.5:    score = max(0, min(100, score + 4))
            elif _sl > 0.5:  score = max(0, min(100, score + 2))
            elif _sl < -1.5: score = max(0, min(100, score - 3))
            elif _sl < -0.5: score = max(0, min(100, score - 1))

        # Re-compute action
        if   score >= MIN_SCORE_BUY:  action = 'MUA'
        elif score <= MAX_SCORE_SELL: action = 'BAN'
        else:                         action = 'THEO_DOI'

    except Exception:
        pass
    # ── Sprint 4 END — đảm bảo action luôn đồng bộ với score cuối ────────────
    # (phòng trường hợp exception xảy ra giữa chừng trước khi Re-compute action)
    if   score >= MIN_SCORE_BUY:  action = 'MUA'
    elif score <= MAX_SCORE_SELL: action = 'BAN'
    else:                         action = 'THEO_DOI'
    return score, action

# ─── TRADE SIMULATION ─────────────────────────────────────────────────────────

def simulate_trade(closes, entry_idx, direction='MUA', sl=None, tp=None,
                   highs=None, lows=None, hold_days=None,
                   trailing_stop=False):
    """
    Simulate 1 trade với T+2 settlement.
    sl/tp là số dương: sl=0.07 = cắt lỗ -7%, tp=0.14 = chốt +14%.
    hold_days: số ngày giữ lệnh (mặc định dùng HOLD_DAYS từ config).

    TRAILING STOP 1R/3R (trailing_stop=True):
      - Activate khi profit >= 1R (= sl%)
      - Trail = 0.5R từ đỉnh (tức sl*0.5)
      - TP cứng tại 3R (= sl*3) — override tp param
      - hold_days vẫn là hard cap
    Returns: (pnl_pct, reason, days_held)
    """
    _sl   = -(sl if sl is not None else abs(STOP_LOSS))
    _tp   =   tp if tp is not None else TAKE_PROFIT
    # SA-3 FIX: dùng hold_days param thay vì global HOLD_DAYS
    _hold = hold_days if hold_days is not None else HOLD_DAYS

    entry_price = closes[entry_idx]
    if entry_price <= 0:
        return 0.0, 'invalid', 0

    # Trailing stop params — tính từ sl
    _sl_abs       = abs(_sl)                  # 0.07
    _trail_activate = _sl_abs * 1.0           # activate @ +1R (e.g. +7%)
    _trail_pct      = _sl_abs * 0.5           # trail 0.5R từ đỉnh (e.g. 3.5%)
    _trail_tp       = _sl_abs * 3.0           # hard TP @ 3R (e.g. +21%)

    total_days   = SETTLEMENT_DAYS + _hold
    n            = len(closes)
    peak_price   = entry_price   # đỉnh cao nhất kể từ khi vào lệnh
    trail_active = False

    for d in range(SETTLEMENT_DAYS, total_days + 1):
        idx = entry_idx + d
        if idx >= n:
            exit_price = closes[-1]
            pnl = (exit_price / entry_price - 1) * 100
            return round(pnl, 2), 'expired', d

        hi = highs[idx]  if highs  is not None else closes[idx]
        lo = lows[idx]   if lows   is not None else closes[idx]
        cl = closes[idx]

        # Check SL (worst case intraday) — luôn áp dụng
        pnl_lo = (lo / entry_price - 1)
        if pnl_lo <= _sl:
            return round(_sl * 100, 2), 'sl', d

        if trailing_stop:
            # Cập nhật đỉnh
            if hi > peak_price:
                peak_price = hi

            pnl_hi = (hi / entry_price - 1)

            # Hard TP @ 3R
            if pnl_hi >= _trail_tp:
                return round(_trail_tp * 100, 2), 'tp', d

            # Activate trailing khi lời >= 1R
            if pnl_hi >= _trail_activate:
                trail_active = True

            # Trailing stop: exit khi price rớt > 0.5R từ đỉnh
            if trail_active:
                trail_floor = peak_price * (1 - _trail_pct)
                if lo <= trail_floor:
                    pnl_trail = (trail_floor / entry_price - 1) * 100
                    return round(pnl_trail, 2), 'trail', d

        else:
            # Chế độ cũ: TP cứng
            pnl_hi = (hi / entry_price - 1)
            if pnl_hi >= _tp:
                return round(_tp * 100, 2), 'tp', d

        # Cuối HOLD_DAYS — close out
        if d == total_days:
            pnl = (cl / entry_price - 1) * 100
            return round(pnl, 2), 'expired', d

    return 0.0, 'expired', _hold

# ─── STATISTICS ───────────────────────────────────────────────────────────────

def calc_stats(df_trades, direction='MUA'):
    """Tính WR, avg PnL, PF từ DataFrame trades."""
    subset = df_trades[df_trades['action'] == direction].copy() if 'action' in df_trades.columns else df_trades.copy()
    if len(subset) == 0:
        return {'total': 0, 'win_rate': 0, 'avg_pnl': 0,
                'avg_win': 0, 'avg_loss': 0, 'profit_factor': 0,
                'avg_days': 0, 'tp': 0, 'sl': 0, 'expired': 0}

    wins   = subset[subset['pnl'] > 0]
    losses = subset[subset['pnl'] <= 0]
    wr     = len(wins) / len(subset) * 100

    gross_profit = wins['pnl'].sum() if len(wins) > 0 else 0
    gross_loss   = abs(losses['pnl'].sum()) if len(losses) > 0 else 0
    pf = gross_profit / gross_loss if gross_loss > 0 else float('inf')

    # Wilson CI 95%
    n_total = len(subset)
    z = 1.96
    p = wr / 100
    if n_total > 0:
        center = (p + z*z/(2*n_total)) / (1 + z*z/n_total)
        margin = (z * math.sqrt(p*(1-p)/n_total + z*z/(4*n_total*n_total))) / (1 + z*z/n_total)
        ci_lo = round(max(0, center - margin) * 100, 1)
        ci_hi = round(min(1, center + margin) * 100, 1)
    else:
        ci_lo, ci_hi = 0.0, 100.0

    return {
        'total':         n_total,
        'win_rate':      round(wr, 1),
        'avg_pnl':       round(subset['pnl'].mean(), 2),
        'avg_win':       round(wins['pnl'].mean(),   2) if len(wins)   > 0 else 0.0,
        'avg_loss':      round(losses['pnl'].mean(), 2) if len(losses) > 0 else 0.0,
        'profit_factor': round(pf, 2) if pf != float('inf') else float('inf'),
        'avg_days':      round(subset['days'].mean(), 1) if 'days' in subset.columns else 0,
        'tp':            int(len(subset[subset['reason'] == 'tp'])),
        'sl':            int(len(subset[subset['reason'] == 'sl'])),
        'expired':       int(len(subset[subset['reason'] == 'expired'])),
        'trail':         int(len(subset[subset['reason'] == 'trail'])) if 'reason' in subset.columns else 0,
        'ci_low':        ci_lo,
        'ci_high':       ci_hi,
    }

# ─── MARKET REGIME ────────────────────────────────────────────────────────────

_regime_cache = {}

def get_market_regime():
    """Tính BULL/NEUTRAL/BEAR từ VNINDEX. Cache 1 giờ."""
    import time
    now = time.time()
    if 'data' in _regime_cache and now - _regime_cache['ts'] < 3600:
        return _regime_cache['data']

    result = {'regime': 'UNKNOWN', 'vni': 0, 'ma50': 0, 'ma200': 0, 'label': ''}
    try:
        df_vni, _ = load_data('VNINDEX', days=300)
        if df_vni is None or len(df_vni) < 50:
            return result
        cc = find_col(df_vni, ['close', 'closeprice', 'close_price'])
        if cc is None:
            return result
        arr = to_arr(df_vni[cc])
        if arr.max() < 1000:
            arr *= 1000
        vni   = float(arr[-1])
        ma50  = float(np.mean(arr[-50:]))
        ma200 = float(np.mean(arr[-200:])) if len(arr) >= 200 else ma50

        if vni > ma50 > ma200:
            regime = 'BULL'
        elif vni > ma200:
            regime = 'NEUTRAL'
        else:
            regime = 'BEAR'

        result = {'regime': regime, 'vni': round(vni), 'ma50': round(ma50),
                  'ma200': round(ma200), 'label': f'VNI={vni:.0f} MA50={ma50:.0f}'}
        _regime_cache['data'] = result
        _regime_cache['ts']   = now
    except Exception:
        pass
    return result

def apply_regime_to_score(score, regime):
    """Áp Market Regime penalty/bonus vào score."""
    note = ''
    if regime == 'BULL':
        pass  # không thay đổi
    elif regime == 'NEUTRAL':
        if score > 72:
            score = 72
            note = 'Regime NEUTRAL: cap 72'
    elif regime == 'BEAR':
        if score >= MIN_SCORE_BUY:
            score = MIN_SCORE_BUY - 7  # = 58, dưới ngưỡng MUA
            note = 'Regime BEAR: cap 58'
    return score, note

# ─── VWAP ─────────────────────────────────────────────────────────────────────

def compute_vwap_arrays(closes, volumes, dates):
    """
    Tính VWAP tuần (reset T2) và VWAP tháng (reset ngày 1).
    dates: pd.Series of datetime64.
    Returns: (vwap_weekly_arr, vwap_monthly_arr)
    """
    n = len(closes)
    vwap_w = np.zeros(n)
    vwap_m = np.zeros(n)

    pv_w, vol_w = 0.0, 0.0
    pv_m, vol_m = 0.0, 0.0
    last_week_start = None
    last_month = None

    for i in range(n):
        try:
            dt = pd.Timestamp(dates.iloc[i])
        except Exception:
            dt = None

        # Weekly reset: Monday (weekday=0)
        if dt is not None:
            week_start = dt - timedelta(days=dt.weekday())
            if last_week_start is None or week_start > last_week_start:
                pv_w, vol_w = 0.0, 0.0
                last_week_start = week_start
            # Monthly reset: day 1
            month_key = (dt.year, dt.month)
            if last_month is None or month_key != last_month:
                pv_m, vol_m = 0.0, 0.0
                last_month = month_key

        pv_w += closes[i] * volumes[i]
        vol_w += volumes[i]
        pv_m += closes[i] * volumes[i]
        vol_m += volumes[i]

        vwap_w[i] = pv_w / vol_w if vol_w > 0 else closes[i]
        vwap_m[i] = pv_m / vol_m if vol_m > 0 else closes[i]

    return vwap_w, vwap_m

def apply_vwap_bonus(score, price, vwap_w, vwap_m):
    """Tính VWAP bonus/penalty (-4 đến +5)."""
    bonus = 0
    note  = ''
    if vwap_w > 0 and vwap_m > 0:
        if price > vwap_w:
            bonus += 2
        else:
            bonus -= 2
        if price > vwap_m:
            bonus += 2
        else:
            bonus -= 2
        if vwap_w > vwap_m:
            bonus += 1
    bonus = max(-4, min(5, bonus))
    new_score = max(0, min(100, score + bonus))
    return new_score, bonus, note

# ─── MAIN BACKTEST ─────────────────────────────────────────────────────────────

def run_backtest_symbol(symbol, verbose=True, sl=None, tp=None, days=None,
                        entry_mode='T', use_b_filter=False, use_regime=None,
                        use_vwap=True, _df_cache=None, min_conviction=0,
                        trigger_mode='score_primary', trigger_score=None,
                        hold_days=None, min_score=None, _vni_cache=None,
                        trailing_stop=False):
    """
    Backtest per-symbol với đầy đủ tham số.
    Returns dict với keys: buy, sell, trades, yearly, sl, tp, min_score, symbol
    """
    cfg        = SYMBOL_CONFIG.get(symbol.upper(), {})
    _sl        = sl        if sl        is not None else cfg.get('sl',  abs(STOP_LOSS))
    _tp        = tp        if tp        is not None else cfg.get('tp',  TAKE_PROFIT)
    _hold_days = hold_days if hold_days is not None else cfg.get('hold_days', HOLD_DAYS)
    _days      = days      if days      is not None else LOOKBACK_DAYS

    # trigger_mode + min_score
    _trig_mode = cfg.get('trigger_mode', 'score_primary')
    _tier_min  = {'score_primary': MIN_SCORE_BUY,
                  'filter_confirm': 55, 'filter_led': 45}
    _min_score = (min_score if min_score is not None
                  else cfg.get('min_score', _tier_min.get(_trig_mode, MIN_SCORE_BUY)))

    if use_regime is None:
        use_regime = cfg.get('use_regime', True)
    if use_vwap is None:
        use_vwap = cfg.get('use_vwap', True)

    n_years = round(_days / 365, 1)
    if verbose:
        print(f"\n{'═'*60}")
        print(f"  BACKTEST {n_years:.0f}Y: {symbol} | SL={_sl*100:.0f}% TP={_tp*100:.0f}% Hold={_hold_days}p Score>={_min_score}")
        print('═'*60)

    # Load data
    if _df_cache is not None:
        df, source = _df_cache, 'cache'
    else:
        df, source = load_data(symbol, days=_days)
    if df is None:
        if verbose: print(f"  ✗ Không tải được dữ liệu {symbol}")
        return None

    cc = find_col(df, ['close', 'closeprice', 'close_price'])
    hc = find_col(df, ['high',  'highprice',  'high_price'])
    lc = find_col(df, ['low',   'lowprice',   'low_price'])
    vc = next((c for c in df.columns if c.lower() in
               ('volume','volume_match','klgd','vol','trading_volume',
                'match_volume','total_volume')), None)
    oc = find_col(df, ['open', 'openprice', 'open_price'])

    if cc is None:
        return None

    closes  = to_arr(df[cc])
    highs   = to_arr(df[hc]) if hc else closes.copy()
    lows    = to_arr(df[lc]) if lc else closes.copy()
    opens   = to_arr(df[oc]) if oc else closes.copy()
    volumes = to_arr(df[vc]) if vc else np.ones(len(closes))

    # Fix scale
    for arr in [closes, highs, lows, opens]:
        if arr.max() < 1000:
            arr *= 1000

    # Dates
    _tc = next((c for c in df.columns if c.lower() in
                ('time','date','datetime','trading_date')), None)
    if _tc:
        _dates = pd.to_datetime(df[_tc], errors='coerce').reset_index(drop=True)
    elif isinstance(df.index, pd.DatetimeIndex):
        _dates = pd.Series(df.index)
    else:
        _dates = pd.Series([pd.NaT] * len(df))

    # Load VNI for regime
    _vni_closes = None
    # Load VNI cho cả regime filter lẫn trade analytics (vni_slope/vni_ma20_dist)
    # FIX-2: align VNI array với stock array theo length
    # Nếu VNI có nhiều rows hơn (load thêm 60 ngày), trim về cùng độ dài
    # Nếu VNI ít hơn, pad đầu bằng giá trị đầu tiên (conservative)
    try:
        df_vni = _vni_cache if _vni_cache is not None else load_data('VNINDEX', days=_days+60)[0]
        if df_vni is not None:
            cc_vni = find_col(df_vni, ['close', 'closeprice', 'close_price'])
            if cc_vni:
                _vni_raw = to_arr(df_vni[cc_vni])
                _vni_raw = np.where(_vni_raw < 1000, _vni_raw * 1000, _vni_raw)
                n_stock = len(closes)
                if len(_vni_raw) >= n_stock:
                    # Trim: lấy n_stock rows cuối (align về ngày gần nhất)
                    _vni_closes = _vni_raw[-n_stock:]
                else:
                    # Pad đầu bằng giá trị đầu tiên
                    pad = np.full(n_stock - len(_vni_raw), _vni_raw[0])
                    _vni_closes = np.concatenate([pad, _vni_raw])
    except Exception:
        pass

    # Pre-compute VWAP
    try:
        _vwap_w, _vwap_m = compute_vwap_arrays(closes, volumes, _dates)
    except Exception:
        _vwap_w = _vwap_m = None

    # ── Main backtest loop ─────────────────────────────────────────────────
    trades          = []
    # FIX-1: dùng _hold_days (đã resolve từ config/param) thay vì global HOLD_DAYS
    # Tránh skip lệnh đầu tiên sớm hơn cần thiết khi hold_days override
    last_signal_idx = -(_hold_days + SETTLEMENT_DAYS)

    for i in range(200, len(closes) - _hold_days - SETTLEMENT_DAYS):
        if i - last_signal_idx < _hold_days + SETTLEMENT_DAYS:
            continue

        score, action = compute_score_at(closes, highs, lows, volumes, i, opens=opens)

        # Regime filter
        _regime_at_i = 'UNKNOWN'
        if use_regime and _vni_closes is not None:
            try:
                vni_i = _vni_closes[min(i, len(_vni_closes)-1)]
                ma50_i  = np.mean(_vni_closes[max(0,i-50):i+1])
                ma200_i = np.mean(_vni_closes[max(0,i-200):i+1])
                if vni_i > ma50_i > ma200_i:
                    _regime_at_i = 'BULL'
                elif vni_i > ma200_i:
                    _regime_at_i = 'NEUTRAL'
                else:
                    _regime_at_i = 'BEAR'
                if _regime_at_i in ('BULL', 'NEUTRAL', 'BEAR'):
                    score, _ = apply_regime_to_score(score, _regime_at_i)
            except Exception:
                pass

        # VWAP bonus
        _vwap_bonus = 0
        if use_vwap and _vwap_w is not None and i < len(_vwap_w) and _vwap_w[i] > 0:
            score, _vwap_bonus, _ = apply_vwap_bonus(score, closes[i], _vwap_w[i], _vwap_m[i])

        # Re-compute action after adjustments
        if   score >= MIN_SCORE_BUY:  action = 'MUA'
        elif score <= MAX_SCORE_SELL: action = 'BAN'
        else:                         action = 'THEO_DOI'

        # ── Trigger mode ──────────────────────────────────────────────────
        _trig_score = trigger_score if trigger_score is not None else _min_score
        _tm = trigger_mode if trigger_mode != 'score_primary' else _trig_mode

        if _tm == 'score_primary':
            if action == 'MUA' and score < _min_score:
                action = 'THEO_DOI'
        elif _tm == 'filter_confirm':
            if action == 'MUA' and score < _trig_score:
                action = 'THEO_DOI'
        elif _tm == 'filter_led':
            if score < _trig_score:
                action = 'THEO_DOI'
            elif score < _min_score:
                _regime_ok = (not use_regime) or (_regime_at_i == 'BULL')
                _vwap_ok   = True
                if _vwap_w is not None and i < len(_vwap_w) and _vwap_w[i] > 0:
                    _vwap_ok = (closes[i] > _vwap_w[i]) or (closes[i] > _vwap_m[i])
                if not (_regime_ok and _vwap_ok):
                    action = 'THEO_DOI'

        if action not in ('MUA', 'BAN'):
            continue

        # ── Conviction Filter ─────────────────────────────────────────────
        if min_conviction > 0 and action == 'MUA':
            _conv = 1  # [1] Score A always pass
            if not use_regime:
                _conv += 0.5
            elif _regime_at_i == 'BULL':
                _conv += 1
            elif _regime_at_i == 'NEUTRAL':
                _conv += 0.5
            # VWAP
            if _vwap_w is not None and i < len(_vwap_w) and _vwap_w[i] > 0:
                p_vs_w = closes[i] - _vwap_w[i]
                p_vs_m = closes[i] - _vwap_m[i]
                if p_vs_w > 0 and p_vs_m > 0:   _conv += 1
                elif p_vs_w > 0 or p_vs_m > 0:  _conv += 0.5
            else:
                _conv += 0.5
            _conv += 0.5  # Shark neutral in offline BT
            if _conv < min_conviction:
                continue

        # Entry
        entry_idx = i + 1 if entry_mode == 'T+1' and i + 1 < len(closes) else i
        pnl, reason, days_held = simulate_trade(
            closes, entry_idx, action, sl=_sl, tp=_tp,
            highs=highs, lows=lows, hold_days=_hold_days,  # SA-3 FIX: truyền hold_days
            trailing_stop=trailing_stop,
        )
        pnl = round(pnl - COMMISSION * 100, 2)

        _ts = _dates.iloc[i] if i < len(_dates) else pd.NaT
        trade_date = _ts.strftime('%Y-%m-%d') if pd.notna(_ts) else f'idx_{i}'

        # ── Rich context fields for trade analytics ─────────────────────
        _ep   = closes[entry_idx]

        # Entry vs MA20 / MA50
        _ma20 = float(np.mean(closes[max(0,i-20):i])) if i >= 20 else _ep
        _ma50 = float(np.mean(closes[max(0,i-50):i])) if i >= 50 else _ep
        _ma20_dist = (_ep / _ma20 - 1) * 100 if _ma20 > 0 else 0.0
        _ma50_dist = (_ep / _ma50 - 1) * 100 if _ma50 > 0 else 0.0

        # MA20 slope: % change of MA20 over last 5 bars
        _ma20_prev = float(np.mean(closes[max(0,i-25):max(1,i-5)])) if i >= 25 else _ma20
        _ma20_slope = (_ma20 / _ma20_prev - 1) * 100 if _ma20_prev > 0 else 0.0

        # Rate of change: close 5 bars ago vs now
        _roc5 = (closes[i] / closes[max(0,i-5)] - 1) * 100 if i >= 5 else 0.0

        # Structure: distance from 20-bar high (HH20) — how extended past breakout
        # FIX-6: dùng highs thay vì closes để đo đúng mức kháng cự intraday
        _hh20 = float(np.max(highs[max(0,i-20):i])) if i >= 5 else _ep
        _hh20_dist = (_ep / _hh20 - 1) * 100 if _hh20 > 0 else 0.0

        # Volume ratio: entry vol / avg20
        _vol_entry = float(volumes[entry_idx]) if entry_idx < len(volumes) else 0
        _vol_avg20 = float(np.mean(volumes[max(0,i-20):i])) if i >= 5 else 1
        _vol_ratio = _vol_entry / _vol_avg20 if _vol_avg20 > 0 else 1.0

        # Vol context: is volume spike at extended price? (climax) or near base?
        _vol_structure = 0.0  # positive = vol spike near base, negative = vol spike extended
        if _vol_ratio >= 1.5:
            _vol_structure = _vol_ratio * (1 if _hh20_dist >= -2 and _ma20_dist < 5 else -1)

        # VNI context at entry
        _vni_slope_val  = 0.0
        _vni_ma20_dist  = 0.0
        _vni_atr_ratio  = 0.0   # ATR(14)/price — volatility regime
        _vni_vol_ratio  = 0.0   # VNI volume vs MA20 — liquidity regime
        if _vni_closes is not None and len(_vni_closes) > 0:
            try:
                _vi = min(i, len(_vni_closes) - 1)
                # Slope
                if _vi >= 10:
                    _vni_slope_val = float(
                        (_vni_closes[_vi] / _vni_closes[max(0,_vi-10)] - 1) * 100
                    )
                # VNI vs MA20
                if _vi >= 20:
                    _vni_ma20v = float(np.mean(_vni_closes[max(0,_vi-20):_vi]))
                    _vni_ma20_dist = float(
                        (_vni_closes[_vi] / _vni_ma20v - 1) * 100
                    ) if _vni_ma20v > 0 else 0.0
                # ATR(14) / price — volatility regime proxy (true range: abs diff closes)
                if _vi >= 15:
                    _atr_v = float(np.mean(np.abs(np.diff(_vni_closes[max(0,_vi-14):_vi+1]))))
                    _vni_atr_ratio = _atr_v / _vni_closes[_vi] * 100 if _vni_closes[_vi] > 0 else 0.0
            except Exception:
                pass

        # VNI volume regime — need vni volumes; fallback 0 if not available
        # (vni volume not always available from vnstock, graceful fallback)

        # MFE — Max Favorable Excursion: highest profit during trade (all reasons)
        # MAE — Max Adverse Excursion: deepest drawdown during trade (all reasons)
        _mfe = 0.0
        _mae = 0.0
        try:
            _n_path = len(closes)
            _peak_h = _ep
            _trough_l = _ep
            for _d in range(SETTLEMENT_DAYS, days_held + 1):
                _pi = entry_idx + _d
                if _pi >= _n_path:
                    break
                if highs[_pi]  > _peak_h:   _peak_h   = highs[_pi]
                if lows[_pi]   < _trough_l: _trough_l = lows[_pi]
            _mfe = round((_peak_h   / _ep - 1) * 100, 2)
            _mae = round((_trough_l / _ep - 1) * 100, 2)
        except Exception:
            pass

        # max_pnl_path: keep for backward compat (= MFE for SL trades)
        _max_pnl_path = _mfe if reason == 'sl' else 0.0

        # VNI % change DURING hold — phân biệt "SL do market" vs "SL do signal"
        # Nếu VNI cũng rớt mạnh trong khi lệnh đang hold → market risk
        # Nếu VNI flat/tăng mà lệnh vẫn SL → signal/stock-specific risk
        _vni_during_hold = 0.0
        if _vni_closes is not None and len(_vni_closes) > 0:
            try:
                _vi_entry = min(entry_idx, len(_vni_closes) - 1)
                _vi_exit  = min(entry_idx + days_held, len(_vni_closes) - 1)
                if _vi_entry != _vi_exit and _vni_closes[_vi_entry] > 0:
                    _vni_during_hold = round(
                        (_vni_closes[_vi_exit] / _vni_closes[_vi_entry] - 1) * 100, 2
                    )
            except Exception:
                pass

        trades.append({
            'date': trade_date, 'price': round(_ep, 0),
            'score': score, 'regime': _regime_at_i,
            'action': action, 'pnl': pnl, 'reason': reason, 'days': days_held,
            # Entry quality
            'ma20_dist':    round(_ma20_dist,   2),
            'ma50_dist':    round(_ma50_dist,   2),
            'ma20_slope':   round(_ma20_slope,  2),
            'roc5':         round(_roc5,        2),
            'hh20_dist':    round(_hh20_dist,   2),
            'vol_ratio':    round(_vol_ratio,   2),
            'vol_structure': round(_vol_structure, 2),
            # Market context
            'vni_slope':     round(_vni_slope_val, 2),
            'vni_ma20_dist': round(_vni_ma20_dist, 2),
            'vni_atr_ratio': round(_vni_atr_ratio, 2),
            'vni_during_hold': _vni_during_hold,
            # MFE / MAE
            'mfe':          _mfe,
            'mae':          _mae,
            'max_pnl_path': _max_pnl_path,   # backward compat
        })
        last_signal_idx = i

    if not trades:
        return None

    df_t = pd.DataFrame(trades)
    buy_stats  = calc_stats(df_t, 'MUA')
    sell_stats = calc_stats(df_t, 'BAN')

    # Yearly breakdown
    df_t['year'] = pd.to_datetime(df_t['date'], errors='coerce').dt.year
    yearly_res = {}
    for yr, grp in df_t.groupby('year'):
        yearly_res[int(yr)] = calc_stats(grp, 'MUA')

    # Optimize threshold (for verbose output)
    thresh_res = {}
    for thr in [55, 60, 65, 70]:
        sub = df_t[(df_t['action'] == 'MUA') & (df_t['score'] >= thr)]
        if len(sub) >= 5:
            wr = len(sub[sub['pnl'] > 0]) / len(sub) * 100
            thresh_res[thr] = {'n': len(sub), 'wr': round(wr, 1),
                                'pnl': round(sub['pnl'].mean(), 2)}

    if verbose:
        b = buy_stats
        pf_s = f"{b['profit_factor']:.2f}" if b['profit_factor'] != float('inf') else '∞'
        print(f"\n  MUA: {b['total']}L | WR={b['win_rate']}% | AvgPnL={b['avg_pnl']:+.2f}% | PF={pf_s}")
        print(f"       TP={b['tp']}L SL={b['sl']}L Hết={b['expired']}L Trail={b.get('trail',0)}L")

    # Extract CI for top-level conf key (backward compat)
    _conf = {
        'ci_low':  buy_stats.get('ci_low',  0),
        'ci_high': buy_stats.get('ci_high', 100),
    }

    return {
        'symbol': symbol, 'sl': _sl, 'tp': _tp,
        'min_score': _min_score, 'days': _hold_days,
        'entry_mode': entry_mode,
        'buy': buy_stats, 'sell': sell_stats,
        'trades': df_t,
        'yearly': {'yearly': yearly_res},  # wrapped for bot compatibility
        'thresh': thresh_res,
        'conf': _conf,
    }

# ─── COMPARE B-FILTER ─────────────────────────────────────────────────────────

def run_b_filter_comparison(symbol, verbose=True):
    """So sánh Score A vs Score A+B (với B-filter) cho 1 mã."""
    cfg = SYMBOL_CONFIG.get(symbol.upper(), {})
    use_regime = cfg.get('use_regime', True)
    use_vwap   = cfg.get('use_vwap', True)
    df, _ = load_data(symbol)
    if df is None:
        return None

    r_no  = run_backtest_symbol(symbol, verbose=False,
                                 use_b_filter=False, use_regime=use_regime,
                                 use_vwap=use_vwap, _df_cache=df)
    r_yes = run_backtest_symbol(symbol, verbose=False,
                                 use_b_filter=True, use_regime=use_regime,
                                 use_vwap=use_vwap, _df_cache=df)
    if not r_no or not r_yes:
        return None

    st_no  = r_no['buy']
    st_yes = r_yes['buy']
    dwr    = round(st_yes['win_rate'] - st_no['win_rate'], 1)
    dpnl   = round(st_yes['avg_pnl'] - st_no['avg_pnl'], 2)

    if verbose:
        print(f"\n  B-filter comparison {symbol}:")
        print(f"  No  B: {st_no['total']}L WR={st_no['win_rate']}% PnL={st_no['avg_pnl']:+.2f}%")
        print(f"  With B: {st_yes['total']}L WR={st_yes['win_rate']}% PnL={st_yes['avg_pnl']:+.2f}%")
        print(f"  Delta: dWR={dwr:+.1f}% dPnL={dpnl:+.2f}%")

    n_no  = st_no.get('total', 0)
    n_yes = st_yes.get('total', 0)
    n_filtered = max(0, n_no - n_yes)

    # Verdict + flag
    if dwr >= 3:
        flag, bverdict = 'V', f'B-filter TOT: +{dwr:.1f}% WR, loc {n_filtered} lenh nhieu → NEN BAT'
    elif dwr >= 0.5:
        flag, bverdict = '~', f'B-filter HUU ICH nhe: +{dwr:.1f}% WR → THU NGHIEM'
    elif dwr >= -1:
        flag, bverdict = '-', f'B-filter TRUNG TINH: {dwr:+.1f}% WR → khong ro tac dong'
    else:
        flag, bverdict = '!', f'B-filter PHAN TAC DUNG: {dwr:.1f}% WR → NEN TAT'

    mode_a  = {'wr': st_no.get('win_rate', 0),  'pnl': st_no.get('avg_pnl', 0),  'n': n_no}
    mode_ab = {'wr': st_yes.get('win_rate', 0), 'pnl': st_yes.get('avg_pnl', 0), 'n': n_yes}

    return {
        'mode_A':    mode_a,
        'mode_AB':   mode_ab,
        'wr_diff':   dwr,
        'pnl_diff':  dpnl,
        'n_filtered':n_filtered,
        'flag':      flag,
        'verdict':   bverdict,
        'no':        r_no,
        'yes':       r_yes,
        'dwr':       dwr,
        'dpnl':      dpnl,
        'no_filter': r_no,
        'b_filter':  r_yes,
    }

# ─── DUAL MODE (Entry T vs T+1) ───────────────────────────────────────────────

def run_backtest_dual(symbol, verbose=True):
    """So sánh Entry=T vs Entry=T+1 cho 1 mã."""
    cfg = SYMBOL_CONFIG.get(symbol.upper(), {})
    use_regime = cfg.get('use_regime', True)
    use_vwap   = cfg.get('use_vwap', True)
    df, _ = load_data(symbol)
    if df is None:
        return None

    r_t   = run_backtest_symbol(symbol, verbose=False, entry_mode='T',
                                 use_regime=use_regime, use_vwap=use_vwap, _df_cache=df)
    r_t1  = run_backtest_symbol(symbol, verbose=False, entry_mode='T+1',
                                 use_regime=use_regime, use_vwap=use_vwap, _df_cache=df)
    if not r_t or not r_t1:
        return None

    if verbose:
        bt  = r_t['buy'];  bt1 = r_t1['buy']
        print(f"\n  Entry T:   {bt['total']}L WR={bt['win_rate']}% PnL={bt['avg_pnl']:+.2f}%")
        print(f"  Entry T+1: {bt1['total']}L WR={bt1['win_rate']}% PnL={bt1['avg_pnl']:+.2f}%")

    st_t  = r_t['buy']  if r_t  else {'total': 0, 'win_rate': 0, 'avg_pnl': 0}
    st_t1 = r_t1['buy'] if r_t1 else {'total': 0, 'win_rate': 0, 'avg_pnl': 0}

    wr_diff  = round(st_t1['win_rate'] - st_t['win_rate'], 1)
    pnl_diff = round(st_t1['avg_pnl']  - st_t['avg_pnl'],  2)

    if wr_diff >= 5:
        bias_flag  = 'V'
        recommend  = f'Entry T+1 TOT HON: +{wr_diff:.1f}% WR — nen dung T+1'
    elif wr_diff >= 2:
        bias_flag  = '~'
        recommend  = f'Entry T+1 nhi hon: +{wr_diff:.1f}% WR — tham khao'
    elif wr_diff <= -5:
        bias_flag  = 'V'
        recommend  = f'Entry T TOT HON: +{-wr_diff:.1f}% WR — nen dung T'
    elif wr_diff <= -2:
        bias_flag  = '~'
        recommend  = f'Entry T nhi hon: +{-wr_diff:.1f}% WR — tham khao'
    else:
        bias_flag  = '-'
        recommend  = f'Khong co chenh lech ro: {wr_diff:+.1f}% WR — tuy chon'

    mode_T  = {'wr': st_t['win_rate'],  'pnl': st_t['avg_pnl'],  'n': st_t['total']}
    mode_T1 = {'wr': st_t1['win_rate'], 'pnl': st_t1['avg_pnl'], 'n': st_t1['total']}

    return {
        'T':         r_t,
        'T+1':       r_t1,
        'mode_T':    mode_T,
        'mode_T1':   mode_T1,
        'wr_diff':   wr_diff,
        'pnl_diff':  pnl_diff,
        'bias_flag': bias_flag,
        'recommend': recommend,
    }

# ─── WALK-FORWARD ─────────────────────────────────────────────────────────────

def run_walk_forward(symbol, verbose=True, _df_cache=None,
                     sl=None, tp=None, min_score=None, hold_days=None,
                     trailing_stop=False):
    """
    Walk-Forward validation: chia data thành 4 windows liên tiếp.
    Mỗi window: IS=3 năm, OOS=1 năm.
    Returns dict với avg_oos_wr, avg_oos_pnl, verdict.

    SA-5 FIX: thêm override params (sl/tp/min_score/hold_days)
    để IS và WF dùng cùng bộ rules — quan trọng cho sascreen
    với mã chưa có trong SYMBOL_CONFIG.
    trailing_stop: nếu True, dùng chế độ 1R activate / 0.5R trail / 3R TP
    """
    cfg        = SYMBOL_CONFIG.get(symbol.upper(), {})
    use_regime = cfg.get('use_regime', False)   # SA-5: screening default off
    use_vwap   = cfg.get('use_vwap',   False)   # SA-5: screening default off
    _sl        = sl        if sl        is not None else cfg.get('sl',  abs(STOP_LOSS))
    _tp        = tp        if tp        is not None else cfg.get('tp',  TAKE_PROFIT)
    _hold_ovr  = hold_days if hold_days is not None else cfg.get('hold_days', HOLD_DAYS)

    if _df_cache is not None:
        df, source = _df_cache, 'cache'
    else:
        df, source = load_data(symbol, days=LOOKBACK_DAYS)
    if df is None:
        return None

    total = len(df)

    # Adaptive window size theo data available
    if total >= 1500:
        # 7+ năm: IS=3 năm, OOS=1 năm, 4 windows
        is_size, oos_size = 756, 252
    elif total >= 800:
        # 3-7 năm: IS=2 năm, OOS=6 tháng
        is_size, oos_size = 504, 126
    elif total >= 400:
        # 1.5-3 năm: IS=1 năm, OOS=4 tháng, 1-2 windows
        is_size, oos_size = 252, 88
    else:
        return None   # Dưới 1.5 năm: không đủ data WF

    step = oos_size

    windows = []
    start = 0
    # SA-5 FIX: dùng override min_score nếu có (từ sascreen), fallback config
    # SA-4 FIX: floor ở 50 để đảm bảo đủ lệnh trong window nhỏ
    _cfg_score = SYMBOL_CONFIG.get(symbol.upper(), {}).get('min_score', MIN_SCORE_BUY)
    _wf_min_score = max(50, min_score if min_score is not None else _cfg_score)

    while start + is_size + oos_size <= total:
        df_is  = df.iloc[start:start+is_size].copy()
        df_oos = df.iloc[start+is_size:start+is_size+oos_size].copy()

        # SA-2 FIX: entry_mode='T+1' cho cả IS lẫn OOS trong walk-forward
        # Trước: không truyền entry_mode → mặc định 'T' → lookahead (signal ở close[i],
        #        entry cũng close[i] — chỉ biết sau 15:00, không thể mua cùng phiên)
        r_is = run_backtest_symbol(symbol, verbose=False,
                                    sl=_sl, tp=_tp, use_regime=use_regime,
                                    use_vwap=use_vwap, _df_cache=df_is,
                                    hold_days=_hold_ovr,
                                    min_score=_wf_min_score,
                                    trailing_stop=trailing_stop,
                                    entry_mode='T+1')        # SA-2 FIX
        r_oos = run_backtest_symbol(symbol, verbose=False,
                                     sl=_sl, tp=_tp, use_regime=use_regime,
                                     use_vwap=use_vwap, _df_cache=df_oos,
                                     hold_days=_hold_ovr,
                                     min_score=_wf_min_score,
                                     trailing_stop=trailing_stop,
                                     entry_mode='T+1')       # SA-2 FIX  SA-5: hold_days override

        st_is  = r_is['buy']  if r_is  else {'total': 0, 'win_rate': 0, 'avg_pnl': 0}
        st_oos = r_oos['buy'] if r_oos else {'total': 0, 'win_rate': 0, 'avg_pnl': 0}

        # Derive OOS label from date column if available
        try:
            tc = find_col(df_oos, ['time', 'date', 'tradingdate', 'trading_date'])
            if tc is not None:
                oos_label = str(pd.to_datetime(df_oos[tc].iloc[0]).year)
            else:
                oos_label = str(len(windows) + 1)
        except Exception:
            oos_label = str(len(windows) + 1)

        windows.append({
            'is_n': st_is['total'], 'is_wr': st_is['win_rate'], 'is_pnl': st_is['avg_pnl'],
            'oos_n': st_oos['total'], 'oos_wr': st_oos['win_rate'], 'oos_pnl': st_oos['avg_pnl'],
            'oos_label': oos_label,
            'best_thr':  MIN_SCORE_BUY,
        })
        start += step

    if not windows:
        return None

    # Avg metrics
    valid_oos = [w for w in windows if w['oos_n'] >= 5]
    # Fallback: nếu không window nào có >= 5 lệnh, dùng tất cả windows có lệnh
    if not valid_oos:
        valid_oos = [w for w in windows if w['oos_n'] >= 1]
    if not valid_oos:
        return None

    avg_is_wr  = np.mean([w['is_wr']  for w in windows])
    avg_oo_wr  = np.mean([w['oos_wr'] for w in valid_oos])
    avg_is_pn  = np.mean([w['is_pnl'] for w in windows])
    avg_oo_pn  = np.mean([w['oos_pnl'] for w in valid_oos])
    decay_wr   = avg_is_wr - avg_oo_wr

    # Stability: các windows có threshold nhất quán không
    thresholds_used = [_wf_min_score] * len(windows)
    thr_stable = True  # simplified

    # Verdict
    if avg_oo_wr >= 55 and decay_wr < 10 and avg_oo_pn >= 0.5:
        verdict = 'V'        # TOT / Robust
        verdict_txt = 'Robust — OOS on dinh'
    elif avg_oo_wr >= 48 or avg_oo_pn >= 0:
        verdict = '~'        # TRUNG_BINH
        verdict_txt = 'Trung binh — OOS chap nhan duoc'
    else:
        verdict = '!'        # YEU
        verdict_txt = 'Yeu — He thong khong on dinh OOS'

    if verbose:
        print(f"\n  Walk-Forward {symbol} ({len(windows)} windows):")
        print(f"  Avg IS:  WR={avg_is_wr:.1f}% PnL={avg_is_pn:+.2f}%")
        print(f"  Avg OOS: WR={avg_oo_wr:.1f}% PnL={avg_oo_pn:+.2f}% (decay={decay_wr:+.1f}%)")
        print(f"  [{verdict}] {verdict_txt}")

    return {
        'symbol': symbol, 'windows': windows,
        'avg_is_wr': round(avg_is_wr, 1), 'avg_oos_wr': round(avg_oo_wr, 1),
        'avg_is_pnl': round(avg_is_pn, 2), 'avg_oos_pnl': round(avg_oo_pn, 2),
        'decay_wr': round(decay_wr, 1), 'thr_stable': thr_stable,
        'thresholds': thresholds_used, 'verdict': verdict, 'verdict_txt': verdict_txt,
        'sl': _sl, 'tp': _tp,
    }

# ─── WF CACHE SUMMARY ─────────────────────────────────────────────────────────

_wf_summary_cache = {}

def get_wf_summary(symbol, max_age_hours=24):
    """WF summary với cache 24h — dùng trong /score."""
    import time
    now = time.time()
    sym = symbol.upper()
    if sym in _wf_summary_cache:
        ts, cached = _wf_summary_cache[sym]
        if now - ts < max_age_hours * 3600:
            return cached
    try:
        r = run_walk_forward(sym, verbose=False)
        if r is None:
            result = None
        else:
            oos_wr  = r.get('avg_oos_wr', 0)
            oos_pnl = r.get('avg_oos_pnl', 0)
            verdict = r.get('verdict', '?')
            if verdict == 'V':
                icon, stability = '&#x2705;', 'Tot'
            elif verdict == '~':
                icon, stability = '&#x1F7E1;', 'Trung binh'
            else:
                icon, stability = '&#x274C;', 'Yeu'
            result = {
                'oos_wr': oos_wr, 'oos_pnl': oos_pnl,
                'verdict': verdict, 'stability': stability, 'icon': icon,
                'label': f'{icon} WF-OOS: {oos_wr:.0f}% WR | PnL {oos_pnl:+.2f}% | {stability}',
            }
        _wf_summary_cache[sym] = (now, result)
        return result
    except Exception:
        return None

# ─── OPTIMIZE ─────────────────────────────────────────────────────────────────

def run_optimize_symbol(symbol, verbose=True, _df_cache=None):
    """
    Grid search SL×TP×Hold×Score để tìm combo tốt nhất.
    Dùng Adaptive Walk-Forward: split theo số lệnh khi ít data.
    """
    import itertools
    cfg        = SYMBOL_CONFIG.get(symbol.upper(), {})
    use_regime = cfg.get('use_regime', True)
    use_vwap   = cfg.get('use_vwap',   True)
    cur_sl     = cfg.get('sl',  abs(STOP_LOSS)) * 100
    cur_tp     = cfg.get('tp',  TAKE_PROFIT)    * 100
    cur_hold   = cfg.get('hold_days', HOLD_DAYS)
    cur_score  = cfg.get('min_score', MIN_SCORE_BUY)

    if _df_cache is not None:
        df, source = _df_cache, 'cache'
    else:
        df, source = load_data(symbol, days=LOOKBACK_DAYS)
    if df is None:
        return None
    total_rows = len(df)
    if total_rows < 300:
        return None

    # Load VNI once
    vni_cache = None
    if use_regime:
        try:
            vni_cache, _ = load_data('VNINDEX', days=LOOKBACK_DAYS + 60)
        except Exception:
            pass

    # Pre-scan: count total trades
    r_scan = run_backtest_symbol(symbol, verbose=False,
                                  use_regime=use_regime, use_vwap=use_vwap,
                                  _df_cache=df, _vni_cache=vni_cache)
    n_total = r_scan.get('buy', {}).get('total', 0) if r_scan else 0

    # Adaptive split
    if n_total < 30:
        df_is, df_oos = df, None
        split_mode = 'no_wf'
    elif n_total < 60:
        split_row  = int(total_rows * 0.70)
        df_is  = df.iloc[:split_row].copy()
        df_oos = df.iloc[split_row:].copy()
        split_mode = 'by_count'
    else:
        is_rows = min(1825, int(total_rows * 0.72))
        df_is   = df.iloc[:is_rows].copy()
        df_oos  = df.iloc[is_rows:].copy()
        split_mode = 'by_time'

    scores    = [60, 65, 70, 75]          # bỏ 55 (quá thấp, nhiễu nhiều)
    sl_vals   = [5, 7, 10]
    tp_vals   = [10, 14, 20]              # bỏ 25 (ít khi đạt được, kéo dài chờ)
    hold_vals = [5, 10]                   # bỏ 7 (không khác biệt nhiều vs 5 và 10)

    all_results = []
    for score_thr, sl, tp, hold in itertools.product(scores, sl_vals, tp_vals, hold_vals):
        if tp < sl * 1.3:
            continue

        # FIX-5: thêm entry_mode='T+1' để nhất quán với run_walk_forward
        # Trước: default 'T' → IS params lạc quan, mismatch với WF verdict
        r_is = run_backtest_symbol(symbol, verbose=False,
                                    sl=sl/100, tp=tp/100,
                                    use_regime=use_regime, use_vwap=use_vwap,
                                    _df_cache=df_is, min_score=score_thr,
                                    hold_days=hold, _vni_cache=vni_cache,
                                    entry_mode='T+1')
        if not r_is:
            continue
        st_is  = r_is.get('buy', {})
        n_is, wr_is, pnl_is = st_is.get('total',0), st_is.get('win_rate',0), st_is.get('avg_pnl',0)
        pf_is  = st_is.get('profit_factor', 0)
        if n_is < 12:
            continue

        n_oos, wr_oos, pnl_oos, pf_oos = 0, 0.0, 0.0, 0.0
        oos_status = 'NO_WF'

        if df_oos is not None and len(df_oos) >= 100:
            r_oos = run_backtest_symbol(symbol, verbose=False,
                                         sl=sl/100, tp=tp/100,
                                         use_regime=use_regime, use_vwap=use_vwap,
                                         _df_cache=df_oos, min_score=score_thr,
                                         hold_days=hold, _vni_cache=vni_cache,
                                         entry_mode='T+1')
            if r_oos:
                st_oos = r_oos.get('buy', {})
                n_oos   = st_oos.get('total', 0)
                wr_oos  = st_oos.get('win_rate', 0)
                pnl_oos = st_oos.get('avg_pnl', 0)
                pf_oos  = st_oos.get('profit_factor', 0)

            if n_oos < 12:
                # < 12 lệnh OOS: không đủ thống kê để kết luận
                oos_status = 'THIN'
            elif n_oos < 20 and wr_oos >= wr_is - 20:
                # 12-19 lệnh: threshold nới rộng hơn (±20% chấp nhận)
                oos_status = 'ROBUST' if pnl_oos >= -1.0 else 'THIN'
            elif wr_oos >= wr_is - 15 and pnl_oos >= -0.5:
                oos_status = 'ROBUST'
            else:
                oos_status = 'OVERFIT'

        if oos_status == 'ROBUST':
            combined = wr_oos*0.5 + pnl_oos*10*0.3 + (pf_oos-1)*5*0.2
        elif oos_status in ('THIN', 'NO_WF'):
            overfit_sus = max(0, wr_is - 65) * 0.2
            combined = wr_is*0.4 + pnl_is*10*0.3 + (pf_is-1)*5*0.1 - overfit_sus
        else:
            combined = wr_oos*0.3 + pnl_oos*5*0.2 - max(0, wr_is-wr_oos)*0.5

        all_results.append({
            'score': score_thr, 'sl': sl, 'tp': tp, 'hold': hold,
            'n_is': n_is, 'wr_is': round(wr_is,1), 'pnl_is': round(pnl_is,2), 'pf_is': round(pf_is,2),
            'n_oos': n_oos, 'wr_oos': round(wr_oos,1), 'pnl_oos': round(pnl_oos,2),
            'oos_status': oos_status, 'combined': round(combined,3),
        })

    if not all_results:
        return None

    order = {'ROBUST': 0, 'NO_WF': 1, 'THIN': 2, 'OVERFIT': 3}
    all_results.sort(key=lambda x: (order.get(x['oos_status'],9), -x['combined']))

    # Best per score level
    best_per_score = {}
    for r in all_results:
        s = r['score']
        if s not in best_per_score:
            best_per_score[s] = r
        else:
            cur = best_per_score[s]
            if (order.get(r['oos_status'],9) < order.get(cur['oos_status'],9)
                    or (r['oos_status'] == cur['oos_status'] and r['combined'] > cur['combined'])):
                best_per_score[s] = r

    per_score_rows = sorted(best_per_score.values(), key=lambda x: x['score'])
    overall_best   = all_results[0]

    return {'per_score': per_score_rows, 'overall': overall_best, 'all': all_results[:10]}


# ─── ANALYZE BY YEAR ──────────────────────────────────────────────────────────

def compute_momentum_score_at(closes, highs, lows, volumes, i,
                               opens=None, vni_closes=None,
                               vol_time_pct=0.75,
                               min_liquidity_bil=3.0):
    """
    Tính Momentum Leader score tại candle i — không look-ahead.

    DESIGN: Intraday-aware detection
    ─────────────────────────────────────────────────────────────────────────
    Hệ thống phát hiện signal TRONG phiên giao dịch (không chờ close):
    - price = giá hiện tại trong phiên (backtest dùng close[i] = worst-case)
    - vol_today = vol tích lũy đến thời điểm detect
    - vol_time_pct: tỷ lệ phiên đã qua khi detect (default 0.75 = ~14:00)
      → vol_threshold = vol_ma20_eod × vol_time_pct × 1.2
      → So sánh công bằng: vol tích lũy vs baseline tích lũy cùng thời điểm
      VD detect 14:00 (~75% phiên): ngưỡng = vol_ma20 × 0.75 × 1.2 = 0.90x EOD

    Parameters:
        closes, highs, lows, volumes: toàn bộ array (dùng [:i+1])
        i: index hiện tại
        opens: optional, dùng cho distribution day penalty
        vni_closes: optional VNINDEX closes array để tính RS vs VNI
        vol_time_pct: % phiên đã qua khi detect (0.5-1.0), default 0.75
        min_liquidity_bil: Thanh khoản tối thiểu trung bình 20 phiên (tỷ đồng).
                           = vol_ma20 × giá ÷ 1_000_000_000
                           Default 3.0 tỷ. Dùng 0 để tắt filter.
                           Loại mã nhỏ như PXS (~0.3T), CNG (~0.5T)
                           trước khi chạy Tier 1+2 → tiết kiệm thời gian,
                           tránh signal kém thanh khoản khó vào/thoát thực tế.

    Returns:
        (score, grade, tier1_pass, components)
        score: 0-120 (clip về 0 sau penalty)
        grade: 'STRONG' (>=90) | 'PASS' (>=75) | '' (<75)
        tier1_pass: bool
        components: dict {name: (pts, label)}
    """
    if i < 55:  # cần ít nhất 55 candles cho MA50 + buffer
        return 0, '', False, {}

    c = closes[:i+1]
    h = highs[:i+1]
    l = lows[:i+1]
    v = volumes[:i+1]
    n = len(c)
    price = float(c[-1])

    # ── LIQUIDITY GATE (trước Tier 1) ────────────────────────────────────────
    # Thanh khoản = vol_ma20 × giá ÷ 1e9 (tỷ đồng/ngày trung bình 20 phiên)
    # Dùng v[-21:-1] (20 phiên trước, không gồm hôm nay) — nhất quán với SA-1 fix
    # Loại sớm mã thanh khoản thấp trước khi tính Tier 1+2:
    #   - Tránh signal kém thanh khoản khó vào/thoát thực tế
    #   - Nhất quán giữa backtest và live trading
    if min_liquidity_bil > 0 and n >= 22:
        _vol_ma20_liq = float(np.mean(v[-21:-1]))   # 20 phiên trước, không gồm hôm nay
        _liquidity    = _vol_ma20_liq * price / 1e9  # tỷ đồng/ngày
        if _liquidity < min_liquidity_bil:
            return 0, '', False, {
                'liquidity_gate': (0,
                    f'Thanh khoan {_liquidity:.1f} ty/ngay < {min_liquidity_bil:.0f} ty (loai)')
            }

    # ── Tier 1: Gate ─────────────────────────────────────────────────────────
    ma50     = float(np.mean(c[-50:]))
    # SA-1 consistency: dùng 20 phiên TRƯỚC (không gồm hôm nay) — nhất quán
    # với Score A vol_ma20 fix và Liquidity Gate bên trên
    vol_ma20  = float(np.mean(v[-21:-1])) if n >= 21 else float(np.mean(v[:-1]) if n > 1 else v[0])
    vol_today = float(v[-1])

    # BUG-1 FIX (intraday-aware): so sánh vol tích lũy vs baseline tích lũy
    # vol_threshold = vol_ma20_eod × vol_time_pct × 1.2
    # → cùng tỷ lệ phiên đã qua, ngưỡng scale theo thời điểm detect
    vol_threshold = vol_ma20 * vol_time_pct * 1.2
    # vol_ratio dùng cho tier2 scoring: adjusted theo thời điểm detect
    vol_ratio_adj = vol_today / (vol_ma20 * vol_time_pct) if vol_ma20 > 0 else 1.0
    # vol_ratio raw (EOD-based) dùng cho penalty distribution day
    vol_ratio_raw = vol_today / vol_ma20 if vol_ma20 > 0 else 1.0

    tier1_pass = (price > ma50) and (vol_today >= vol_threshold)
    if not tier1_pass:
        return 0, '', False, {}

    # ── Tier 2: Scoring ───────────────────────────────────────────────────────
    score = 0
    comps = {}

    # ── 1. RS vs VNINDEX (0/15/20đ) ──────────────────────────────────────────
    rs_vni = 0.0
    if vni_closes is not None and i < len(vni_closes) and i >= 20:
        n20   = 20
        s_ret = (c[-1] / c[-n20] - 1) * 100 if c[-n20] > 0 else 0
        v_ret = (vni_closes[i] / vni_closes[max(0, i - n20)] - 1) * 100 \
                if vni_closes[max(0, i - n20)] > 0 else 0
        rs_vni = s_ret - v_ret
    else:
        # Fallback: dùng raw return 20d (không có VNI)
        if n >= 21:
            rs_vni = (c[-1] / c[-21] - 1) * 100 if c[-21] > 0 else 0

    if rs_vni > 5:
        score += 20; comps['rs_vni'] = (20, f'RS vs VNI +{rs_vni:.1f}% (manh)')
    elif rs_vni > 0:
        score += 15; comps['rs_vni'] = (15, f'RS vs VNI +{rs_vni:.1f}%')
    else:
        comps['rs_vni'] = (0, f'RS vs VNI {rs_vni:.1f}% (yeu)')

    # ── 2. RS vs Sector (0/15/20đ) — BUG-3 FIX ──────────────────────────────
    # Trước: ROC10 absolute = double-counting với rs_vni (đo cùng 1 thứ)
    # Sau:   RS relative = ROC10_stock - ROC10_vni = outperformance thực sự
    # Nếu không có VNI data: bỏ component (0đ), tránh double-count
    if vni_closes is not None and i >= 10 and i < len(vni_closes):
        roc10_stock = (c[-1] / c[-11] - 1) * 100 if n >= 11 and c[-11] > 0 else 0
        roc10_vni   = (vni_closes[i] / vni_closes[max(0, i - 10)] - 1) * 100 \
                      if vni_closes[max(0, i - 10)] > 0 else 0
        rs_sector   = roc10_stock - roc10_vni  # outperformance vs market 10d
        if rs_sector > 5:
            score += 20
            comps['rs_sector'] = (20, f'RS 10d vs VNI +{rs_sector:.1f}% (outperform manh)')
        elif rs_sector > 2:
            score += 15
            comps['rs_sector'] = (15, f'RS 10d vs VNI +{rs_sector:.1f}% (outperform)')
        else:
            comps['rs_sector'] = (0, f'RS 10d vs VNI {rs_sector:.1f}% (khong outperform)')
    else:
        # Không có VNI → bỏ qua hoàn toàn, không double-count với rs_vni
        comps['rs_sector'] = (0, 'RS sector: can VNI data')

    # ── 3-5. RSI Momentum ─────────────────────────────────────────────────────
    rsi_arr = _rsi_wilder(c)
    rsi_val = float(rsi_arr[-1])

    # 3. RSI Level: 45-70 (nới từ 45-60 — momentum thường RSI 60-70)
    if 45 <= rsi_val <= 70:
        score += 10; comps['rsi_level'] = (10, f'RSI={rsi_val:.0f} (45-70 momentum zone)')
    else:
        comps['rsi_level'] = (0, f'RSI={rsi_val:.0f} (ngoai momentum zone)')

    # 4. RSI Speed 4d
    rsi4 = float(rsi_arr[-5]) if len(rsi_arr) >= 5 else rsi_val
    if rsi_val >= rsi4 + 3:
        score += 10; comps['rsi_4d'] = (10, f'RSI tang {rsi_val - rsi4:.1f}d/4phien')
    else:
        comps['rsi_4d'] = (0, f'RSI 4d: {rsi_val - rsi4:+.1f}d')

    # 5. RSI Speed 5d
    rsi5 = float(rsi_arr[-6]) if len(rsi_arr) >= 6 else rsi_val
    if rsi_val >= rsi5 + 5:
        score += 10; comps['rsi_5d'] = (10, f'RSI tang {rsi_val - rsi5:.1f}d/5phien')
    else:
        comps['rsi_5d'] = (0, f'RSI 5d: {rsi_val - rsi5:+.1f}d')

    # ── 6. Price Structure: Close >= 80% range 20d ───────────────────────────
    low20   = float(np.min(l[-20:])) if n >= 20 else float(np.min(l))
    high20  = float(np.max(h[-20:])) if n >= 20 else float(np.max(h))
    range20 = high20 - low20
    if range20 > 0 and price >= low20 + 0.8 * range20:
        score += 10; comps['price_range'] = (10, 'Close >= 80% range 20d')
    else:
        pct_r = (price - low20) / range20 * 100 if range20 > 0 else 0
        comps['price_range'] = (0, f'Close {pct_r:.0f}% range 20d')

    # ── 7. Breakout 5d — BUG-4 FIX ──────────────────────────────────────────
    # Trước: np.max(c[-5:]) — window gồm c[-1]=hôm nay, tự so với chính mình
    # Sau:   np.max(c[-6:-1]) — 5 phiên TRƯỚC, không gồm hôm nay
    if n >= 6:
        high5_prev = float(np.max(c[-6:-1]))
        if price >= high5_prev:
            score += 10
            comps['breakout_5d'] = (10, f'Vuot high 5 phien truoc ({high5_prev:,.0f})')
        else:
            comps['breakout_5d'] = (0, f'Chua vuot high 5 phien truoc ({high5_prev:,.0f})')
    else:
        comps['breakout_5d'] = (0, 'Chua du data 5 phien')

    # ── 8. Volume Expansion: vol_ratio_adj >= 1.5 ────────────────────────────
    # Dùng vol_ratio_adj (adjusted theo thời điểm detect) để so sánh công bằng
    if vol_ratio_adj >= 1.5:
        score += 10
        comps['vol_expansion'] = (10, f'Vol {vol_ratio_adj:.1f}x adjusted (>=1.5x)')
    else:
        comps['vol_expansion'] = (0, f'Vol {vol_ratio_adj:.1f}x adjusted (<1.5x)')

    # ── 9. 52W Proximity ─────────────────────────────────────────────────────
    n52    = min(252, n)
    high52 = float(np.max(h[-n52:])) if n52 >= 20 else float(np.max(h))
    pct52  = price / high52 if high52 > 0 else 0
    if pct52 >= 1.0:
        score += 20; comps['w52'] = (20, f'Pha vo dinh 52W ({pct52:.1%})')
    elif pct52 >= 0.95:
        score += 15; comps['w52'] = (15, f'Gan dinh 52W ({pct52:.1%})')
    elif pct52 >= 0.90:
        score += 10; comps['w52'] = (10, f'Trong vung dinh 52W ({pct52:.1%})')
    else:
        comps['w52'] = (0, f'Xa dinh 52W ({pct52:.1%})')

    # ── Penalties ─────────────────────────────────────────────────────────────
    # Distribution day: giá giảm so với open VÀ vol cao
    # Dùng vol_ratio_raw (EOD-based) vì penalty cần nhìn toàn phiên
    open_cur = float(opens[i]) if opens is not None and i < len(opens) else price
    if price < open_cur * 0.995 and vol_ratio_raw >= 1.5:
        score -= 10
        comps['dist_day'] = (-10, 'Distribution day: gia giam + vol cao')

    # Weekly downtrend proxy: MA50d < MA100d
    if n >= 100:
        ma50d  = float(np.mean(c[-50:]))
        ma100d = float(np.mean(c[-100:]))
        if ma50d < ma100d:
            score -= 10
            comps['weekly_down'] = (-10, 'Weekly DOWN proxy: MA50<MA100')

    score = max(0, score)

    # ── Grade ─────────────────────────────────────────────────────────────────
    if score >= 90:
        grade = 'STRONG'
    elif score >= 75:
        grade = 'PASS'
    else:
        grade = ''

    return score, grade, True, comps


def run_backtest_momentum(symbol, sl=0.06, tp=0.17, hold_days=18,
                          min_ml_score=75, days=None, verbose=True,
                          _df_cache=None, _vni_cache=None,
                          vol_time_pct=0.75,
                          min_liquidity_bil=3.0):
    """
    Backtest Momentum Leader signal cho 1 mã.

    DESIGN: Intraday-aware detection
    ─────────────────────────────────────────────────────────────────────────
    Signal phát hiện TRONG phiên giao dịch (~14:00), không chờ close:
    - entry_idx = i: hợp lệ — signal detect trong phiên i, mua ngay lúc đó
      (worst-case: mua ATC, entry_price ≈ close[i])
    - vol so sánh theo tỷ lệ thời gian (vol_time_pct), không phải EOD full-day

    BUG-2 FIX: ML_COOLDOWN = hold_days + SETTLEMENT_DAYS
    → Không mở lệnh mới khi lệnh cũ chưa đóng (realistic capital usage)

    Parameters:
        vol_time_pct: % phiên đã qua khi detect (default 0.75 = ~14:00 HOSE)
                      Dùng 1.0 cho EOD/conservative mode
        min_liquidity_bil: Thanh khoản tối thiểu (tỷ đồng/ngày TB 20 phiên).
                           Default 3.0. Dùng 0 để tắt.
    Returns dict: symbol, sl, tp, hold_days, min_ml_score, vol_time_pct,
                  min_liquidity_bil, buy (stats), trades (DataFrame),
                  yearly, grade_stats, conf
    """
    _days = days if days is not None else LOOKBACK_DAYS

    if verbose:
        print(f"\n{'═'*60}")
        print(f"  BACKTEST ML {round(_days/365,0):.0f}Y: {symbol} | "
              f"SL={sl*100:.0f}% TP={tp*100:.0f}% Hold={hold_days}d "
              f"Score>={min_ml_score} vol_time={vol_time_pct:.0%}")
        print('═'*60)

    # ── Load data ─────────────────────────────────────────────────────────────
    if _df_cache is not None:
        df, source = _df_cache, 'cache'
    else:
        df, source = load_data(symbol, days=_days)
    if df is None:
        if verbose: print(f"  ✗ Không tải được dữ liệu {symbol}")
        return None

    cc = find_col(df, ['close', 'closeprice', 'close_price'])
    hc = find_col(df, ['high',  'highprice',  'high_price'])
    lc = find_col(df, ['low',   'lowprice',   'low_price'])
    vc = next((c for c in df.columns if c.lower() in
               ('volume','volume_match','klgd','vol','trading_volume',
                'match_volume','total_volume')), None)
    oc = find_col(df, ['open', 'openprice', 'open_price'])
    _tc = next((c for c in df.columns if c.lower() in
                ('time','date','datetime','trading_date')), None)

    if cc is None:
        return None

    closes  = to_arr(df[cc])
    highs   = to_arr(df[hc]) if hc else closes.copy()
    lows    = to_arr(df[lc]) if lc else closes.copy()
    opens   = to_arr(df[oc]) if oc else closes.copy()
    volumes = to_arr(df[vc]) if vc else np.ones(len(closes))

    for arr in [closes, highs, lows, opens]:
        if arr.max() < 1000:
            arr *= 1000

    if _tc:
        _dates = pd.to_datetime(df[_tc], errors='coerce').reset_index(drop=True)
    elif isinstance(df.index, pd.DatetimeIndex):
        _dates = pd.Series(df.index)
    else:
        _dates = pd.Series([pd.NaT] * len(df))

    # ── Load VNINDEX ──────────────────────────────────────────────────────────
    vni_closes = None
    try:
        df_vni = _vni_cache if _vni_cache is not None else load_data('VNINDEX', days=_days+60)[0]
        if df_vni is not None:
            cc_vni = find_col(df_vni, ['close', 'closeprice', 'close_price'])
            if cc_vni:
                _raw    = to_arr(df_vni[cc_vni])
                vni_raw = np.where(_raw < 1000, _raw * 1000, _raw)
                if len(vni_raw) >= len(closes):
                    vni_closes = vni_raw[-len(closes):]
                else:
                    pad = np.full(len(closes) - len(vni_raw), vni_raw[0])
                    vni_closes = np.concatenate([pad, vni_raw])
    except Exception:
        pass

    # ── Main loop ─────────────────────────────────────────────────────────────
    # BUG-2 FIX: cooldown = hold_days + SETTLEMENT_DAYS
    # → lệnh mới chỉ mở sau khi lệnh cũ chắc chắn đã đóng (realistic capital)
    ML_COOLDOWN     = hold_days + SETTLEMENT_DAYS
    trades          = []
    last_signal_idx = -ML_COOLDOWN

    for i in range(55, len(closes) - hold_days - SETTLEMENT_DAYS):
        # Cooldown: không mở lệnh khi lệnh cũ chưa đóng
        if i - last_signal_idx < ML_COOLDOWN:
            continue

        ml_score, grade, tier1, comps = compute_momentum_score_at(
            closes, highs, lows, volumes, i,
            opens=opens, vni_closes=vni_closes,
            vol_time_pct=vol_time_pct,
            min_liquidity_bil=min_liquidity_bil,
        )

        if not tier1 or ml_score < min_ml_score:
            continue

        # ── Entry: intraday detect → mua TRONG phiên i ───────────────────
        # Signal phát hiện lúc ~14:00 → đặt lệnh ngay trong phiên
        # entry_price = closes[i] là worst-case (mua ATC cuối phiên)
        entry_idx = i

        pnl, reason, days_held = simulate_trade(
            closes, entry_idx, 'MUA', sl=sl, tp=tp,
            highs=highs, lows=lows, hold_days=hold_days
        )
        pnl = round(pnl - COMMISSION * 100, 2)

        _ts        = _dates.iloc[i] if i < len(_dates) else pd.NaT
        trade_date = _ts.strftime('%Y-%m-%d') if pd.notna(_ts) else f'idx_{i}'

        trades.append({
            'date':       trade_date,
            'price':      round(closes[entry_idx], 0),
            'ml_score':   ml_score,
            'grade':      grade,
            'action':     'MUA',
            'pnl':        pnl,
            'reason':     reason,
            'days':       days_held,
            'rs_vni_pts': comps.get('rs_vni',     (0,))[0],
            'w52_pts':    comps.get('w52',         (0,))[0],
            'rsi_pts':    (comps.get('rsi_level', (0,))[0] +
                           comps.get('rsi_4d',    (0,))[0] +
                           comps.get('rsi_5d',    (0,))[0]),
        })
        last_signal_idx = i

    if not trades:
        if verbose: print(f"  Không có tín hiệu ML nào trong {_days} ngày dữ liệu.")
        return None

    df_t  = pd.DataFrame(trades)
    stats = calc_stats(df_t, 'MUA')

    # Yearly breakdown
    df_t['year'] = pd.to_datetime(df_t['date'], errors='coerce').dt.year
    yearly_res   = {}
    for yr, grp in df_t.groupby('year'):
        yearly_res[int(yr)] = calc_stats(grp, 'MUA')

    # Grade breakdown: STRONG vs PASS
    grade_res = {}
    for g in ['STRONG', 'PASS']:
        sub = df_t[df_t['grade'] == g]
        if len(sub) >= 3:
            grade_res[g] = calc_stats(sub, 'MUA')

    if verbose:
        b    = stats
        pf_s = f"{b['profit_factor']:.2f}" if b['profit_factor'] != float('inf') else '∞'
        print(f"\n  ML MUA: {b['total']}L | WR={b['win_rate']}% [{b['ci_low']}-{b['ci_high']}%] | "
              f"AvgPnL={b['avg_pnl']:+.2f}% | PF={pf_s}")
        print(f"         TP={b['tp']}L  SL={b['sl']}L  Hết={b['expired']}L  "
              f"AvgHold={b['avg_days']:.0f}d")
        print(f"         Cooldown={ML_COOLDOWN}d | vol_time={vol_time_pct:.0%} | liq_min={min_liquidity_bil:.0f}T")
        if grade_res:
            for g, gs in grade_res.items():
                pf2 = f"{gs['profit_factor']:.2f}" if gs['profit_factor'] != float('inf') else '∞'
                print(f"         [{g}]: {gs['total']}L WR={gs['win_rate']}% "
                      f"PnL={gs['avg_pnl']:+.2f}% PF={pf2}")
        print(f"\n  Yearly:")
        for yr, ys in sorted(yearly_res.items()):
            if ys['total'] > 0:
                phase = MARKET_PHASES.get(yr, '?')
                print(f"    {yr} [{phase:8s}]: {ys['total']:2d}L WR={ys['win_rate']:5.1f}% "
                      f"PnL={ys['avg_pnl']:+.2f}%")

    return {
        'symbol':           symbol,
        'sl':               sl,
        'tp':               tp,
        'hold_days':        hold_days,
        'min_ml_score':     min_ml_score,
        'vol_time_pct':     vol_time_pct,
        'min_liquidity_bil':min_liquidity_bil,
        'buy':              stats,
        'trades':           df_t,
        'yearly':           {'yearly': yearly_res},
        'grade_stats':      grade_res,
        'conf': {'ci_low': stats.get('ci_low', 0), 'ci_high': stats.get('ci_high', 100)},
    }


def run_walk_forward_momentum(symbol, sl=0.06, tp=0.17, hold_days=18,
                               min_ml_score=75, verbose=True,
                               vol_time_pct=0.75,
                               min_liquidity_bil=3.0,
                               _df_cache=None, _vni_cache=None):
    """
    Walk-Forward validation cho Momentum Leader.
    Cùng cấu trúc window với run_walk_forward() (Score A):
      >= 1500 rows: IS=756d OOS=252d
      >= 800 rows:  IS=504d OOS=126d
      >= 400 rows:  IS=252d OOS=88d

    vol_time_pct được truyền xuống run_backtest_momentum cho mỗi window
    → BUG-2 cooldown fix được áp dụng tự động qua hold_days param
    min_liquidity_bil: truyền xuống compute_momentum_score_at qua run_backtest_momentum.
                       Nhất quán giữa WF validation và live trading.
    _df_cache: DataFrame đã load sẵn — tránh gọi vnstock thêm lần nữa.
    _vni_cache: DataFrame VNINDEX đã load sẵn.
    """
    if _df_cache is not None:
        df = _df_cache
    else:
        df, _ = load_data(symbol, days=LOOKBACK_DAYS)
    if df is None:
        return None

    total = len(df)
    if total >= 1500:
        is_size, oos_size = 756, 252
    elif total >= 800:
        is_size, oos_size = 504, 126
    elif total >= 400:
        is_size, oos_size = 252, 88
    else:
        return None

    # Load VNI 1 lần cho toàn bộ WF — dùng cache nếu có
    try:
        df_vni = _vni_cache if _vni_cache is not None else load_data('VNINDEX', days=LOOKBACK_DAYS + 60)[0]
    except Exception:
        df_vni = None

    windows = []
    start   = 0

    while start + is_size + oos_size <= total:
        df_is  = df.iloc[start : start + is_size].copy()
        df_oos = df.iloc[start + is_size : start + is_size + oos_size].copy()

        r_is = run_backtest_momentum(
            symbol, sl=sl, tp=tp, hold_days=hold_days,
            min_ml_score=min_ml_score, verbose=False,
            _df_cache=df_is, _vni_cache=df_vni,
            vol_time_pct=vol_time_pct,
            min_liquidity_bil=min_liquidity_bil,
        )
        r_oos = run_backtest_momentum(
            symbol, sl=sl, tp=tp, hold_days=hold_days,
            min_ml_score=min_ml_score, verbose=False,
            _df_cache=df_oos, _vni_cache=df_vni,
            vol_time_pct=vol_time_pct,
            min_liquidity_bil=min_liquidity_bil,
        )

        st_is  = r_is['buy']  if r_is  else {'total': 0, 'win_rate': 0, 'avg_pnl': 0}
        st_oos = r_oos['buy'] if r_oos else {'total': 0, 'win_rate': 0, 'avg_pnl': 0}

        try:
            tc        = find_col(df_oos, ['time', 'date', 'tradingdate', 'trading_date'])
            oos_label = str(pd.to_datetime(df_oos[tc].iloc[0]).year) if tc else str(len(windows)+1)
        except Exception:
            oos_label = str(len(windows) + 1)

        windows.append({
            'is_n':   st_is['total'],  'is_wr':  st_is['win_rate'],  'is_pnl':  st_is['avg_pnl'],
            'oos_n':  st_oos['total'], 'oos_wr': st_oos['win_rate'], 'oos_pnl': st_oos['avg_pnl'],
            'oos_label': oos_label,
        })
        start += oos_size

    if not windows:
        return None

    valid_oos = [w for w in windows if w['oos_n'] >= 3]
    if not valid_oos:
        valid_oos = [w for w in windows if w['oos_n'] >= 1]
    if not valid_oos:
        return None

    avg_is_wr  = np.mean([w['is_wr']  for w in windows])
    avg_oos_wr = np.mean([w['oos_wr'] for w in valid_oos])
    avg_is_pnl = np.mean([w['is_pnl'] for w in windows])
    avg_oos_pnl= np.mean([w['oos_pnl'] for w in valid_oos])
    decay_wr   = avg_is_wr - avg_oos_wr

    if avg_oos_wr >= 55 and decay_wr < 10 and avg_oos_pnl >= 0.5:
        verdict = 'V'; verdict_txt = 'Robust — OOS on dinh'
    elif avg_oos_wr >= 48 or avg_oos_pnl >= 0:
        verdict = '~'; verdict_txt = 'Trung binh — OOS chap nhan duoc'
    else:
        verdict = '!'; verdict_txt = 'Yeu — He thong khong on dinh OOS'

    if verbose:
        print(f"\n  Walk-Forward ML {symbol} ({len(windows)} windows, "
              f"SL={sl*100:.0f}% TP={tp*100:.0f}% Hold={hold_days}d "
              f"vol_time={vol_time_pct:.0%}):")
        print(f"  Avg IS:  WR={avg_is_wr:.1f}% PnL={avg_is_pnl:+.2f}%")
        print(f"  Avg OOS: WR={avg_oos_wr:.1f}% PnL={avg_oos_pnl:+.2f}% (decay={decay_wr:+.1f}%)")
        for w in windows:
            print(f"    OOS {w['oos_label']}: {w['oos_n']:2d}L IS_WR={w['is_wr']:.0f}% "
                  f"→ OOS_WR={w['oos_wr']:.0f}% PnL={w['oos_pnl']:+.2f}%")
        print(f"  [{verdict}] {verdict_txt}")

    return {
        'symbol': symbol, 'windows': windows,
        'avg_is_wr':        round(avg_is_wr,   1), 'avg_oos_wr':  round(avg_oos_wr,  1),
        'avg_is_pnl':       round(avg_is_pnl,  2), 'avg_oos_pnl': round(avg_oos_pnl, 2),
        'decay_wr':         round(decay_wr, 1),
        'verdict':          verdict, 'verdict_txt': verdict_txt,
        'sl': sl, 'tp': tp, 'hold_days': hold_days,
        'vol_time_pct':     vol_time_pct,
        'min_liquidity_bil':min_liquidity_bil,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ML v3 — POOLED EVENT STUDY (Pattern-Type Driven, không phân ngành)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Thiết kế lại từ đầu dựa trên kết quả event study pooled 7 mã (2018-2025):
#
# MOMENTUM_V3 — pool: NKG(33ev), POW(18ev), FRT(31ev), DGW(38ev), MBS(32ev)
#   Pattern chung: giá/vol đang tăng TRƯỚC khi bứt phá lớn
#   ✅ trend_10d:    NKG ratio=6.31x p=0.0004 | POW ratio=6.4x p=0.007
#   ✅ vol_trend_10d: NKG p=0.007 | FRT p=0.047 | DGW p=0.019
#   ✅ vol_spike_3d:  FRT p=0.036 | DGW p=0.034
#   ✅ rs_vni_5d âm:  DGW p=0.016 | MBS p=0.0045 (yếu hơn thị trường ngắn hạn)
#
# COUNTER_V3 — pool: MWG(16ev), PNJ(14ev)
#   Pattern chung: bị bán quá tay TRƯỚC khi bounce mạnh
#   ✅ near_52w_low:  MWG ratio=4.3x p=0.010 | PNJ ratio=4.4x p=0.008
#   ✅ rsi_oversold:  MWG ratio=2.4x p=0.013 | PNJ ratio=3.2x p=0.0002
#   ✅ rs_vni_5d âm:  MWG p=0.023 | PNJ p=0.004
#
# Phân loại:
#   COUNTER_V3_SYMS — retail consumer bị bán theo sentiment
#   Tất cả mã còn lại — dùng MOMENTUM_V3
#
# Ưu điểm so với symbol-specific v3 cũ:
#   - Chạy được bất kỳ mã nào, không cần event study riêng
#   - Pattern được validate cross-symbol (không overfit 1 mã)
#   - Đơn giản: 2 mode thay vì 7 mode
# ═══════════════════════════════════════════════════════════════════════════════

# Mã dùng COUNTER_V3 — retail consumer, bứt phá khi oversold
# Evidence: MWG + PNJ cùng pattern near_52w_low + rsi_oversold
# Mở rộng sau khi có thêm cross-validation evidence
COUNTER_V3_SYMS = {'MWG', 'PNJ'}


def compute_momentum_score_v3(closes, highs, lows, volumes, i,
                               opens=None, vni_closes=None,
                               vol_time_pct=0.75,
                               min_liquidity_bil=3.0,
                               sector_mode=None):
    """
    ML v3 — Pooled Event Study Pattern.

    2 mode dựa trên pattern type, không phân ngành:
        'momentum_v3' — NKG/POW/FRT/DGW/MBS và tất cả mã còn lại:
                        trend_10d + vol_trend_10d + vol_spike_3d + rs_vni_5d_neg
        'counter_v3'  — MWG/PNJ và retail consumer oversold:
                        near_52w_low + rsi_oversold + rs_vni_5d_neg

    sector_mode: nếu None sẽ tự detect từ symbol (truyền vào qua caller).
                 Nếu truyền 'counter_v3' hoặc 'momentum_v3' thì dùng luôn.

    Returns: (score, grade, tier1_pass, components)
    """
    # Normalize mode
    _mode = (sector_mode or 'momentum_v3').lower()
    if _mode not in ('momentum_v3', 'counter_v3'):
        _mode = 'momentum_v3'   # safe default

    if i < 55:
        return 0, '', False, {}

    c = closes[:i+1]
    h = highs[:i+1]
    l = lows[:i+1]
    v = volumes[:i+1]
    n = len(c)
    price = float(c[-1])

    # ── LIQUIDITY GATE ────────────────────────────────────────────────────────
    if min_liquidity_bil > 0 and n >= 22:
        _vol_ma20_liq = float(np.mean(v[-21:-1]))
        _liquidity    = _vol_ma20_liq * price / 1e9
        if _liquidity < min_liquidity_bil:
            return 0, '', False, {
                'liquidity_gate': (0,
                    f'Thanh khoan {_liquidity:.1f}ty < {min_liquidity_bil:.0f}ty (loai)')
            }

    # ── COMMON PREP ───────────────────────────────────────────────────────────
    ma50     = float(np.mean(c[-50:])) if n >= 50 else float(np.mean(c))
    # FIX (S16): nhất quán với SA-1 fix trong v1 — dùng v[-21:-1] (20 phiên trước,
    # không gồm hôm nay) để tránh vol spike hôm nay inflate baseline → ratio bị dilute.
    vol_ma20  = float(np.mean(v[-21:-1])) if n >= 21 else float(np.mean(v[:-1]) if n > 1 else v[0])
    vol_today = float(v[-1])
    vol_threshold = vol_ma20 * vol_time_pct * 0.9   # gate ×0.9 (tích lũy âm thầm)
    vol_ratio_adj = vol_today / (vol_ma20 * vol_time_pct) if vol_ma20 > 0 else 1.0
    vol_ratio_raw = vol_today / vol_ma20 if vol_ma20 > 0 else 1.0

    # ═════════════════════════════════════════════════════════════════════════
    # COUNTER_V3: MWG, PNJ — bứt phá từ vùng oversold/đáy 52W
    # ═════════════════════════════════════════════════════════════════════════
    if _mode == 'counter_v3':

        # ── TIER 1: GATE — oversold OR gần đáy + vol ─────────────────────────
        n52       = min(252, n)
        low52     = float(np.min(l[-n52:])) if n52 >= 20 else float(np.min(l))
        high52    = float(np.max(h[-n52:])) if n52 >= 20 else float(np.max(h))
        range52   = high52 - low52
        pct_low   = (price - low52) / range52 if range52 > 0 else 1.0
        near_low  = pct_low <= 0.40             # trong 40% vùng đáy 52W

        rsi_arr_g = _rsi_wilder(c)
        rsi_g     = float(rsi_arr_g[-1])
        oversold  = rsi_g <= 48                 # RSI thấp (mean event MWG=41.7, PNJ=31.6)

        if not ((near_low or oversold) and vol_today >= vol_threshold):
            return 0, '', False, {}

        # ── TIER 2: SCORING ───────────────────────────────────────────────────
        score = 0
        comps = {}

        # 1. NEAR_52W_LOW bonus — pooled: MWG 4.3x + PNJ 4.4x
        if pct_low <= 0.20:
            score += 20; comps['near_52w_low'] = (20, f'Day 52W ({pct_low:.0%}) — ES pool 4.4x')
        elif pct_low <= 0.30:
            score += 15; comps['near_52w_low'] = (15, f'Gan day 52W ({pct_low:.0%}) — ES pool 4.4x')
        elif pct_low <= 0.40:
            score += 10; comps['near_52w_low'] = (10, f'Vung day 52W ({pct_low:.0%})')
        else:
            comps['near_52w_low'] = (0, f'Xa day 52W ({pct_low:.0%})')

        # 2. RSI_OVERSOLD bonus — pooled: MWG 2.4x + PNJ 3.2x
        rsi_val = float(_rsi_wilder(c)[-1])
        if rsi_val <= 30:
            score += 20; comps['rsi_oversold'] = (20, f'RSI={rsi_val:.0f} deep oversold — ES pool 3.2x')
        elif rsi_val <= 40:
            score += 15; comps['rsi_oversold'] = (15, f'RSI={rsi_val:.0f} oversold — ES pool 2.4x')
        elif rsi_val <= 48:
            score += 10; comps['rsi_oversold'] = (10, f'RSI={rsi_val:.0f} yeu')
        else:
            comps['rsi_oversold'] = (0, f'RSI={rsi_val:.0f} (chua oversold)')

        # 3. RS_VNI_5D âm — cross-symbol: MWG p=0.023, PNJ p=0.004
        if vni_closes is not None and i >= 5 and i < len(vni_closes):
            rs5_s = (c[-1] / c[-6] - 1) if n >= 6 and c[-6] > 0 else 0
            rs5_v = (vni_closes[i] / vni_closes[max(0, i-5)] - 1) \
                    if vni_closes[max(0, i-5)] > 0 else 0
            rs5   = rs5_s - rs5_v
            if rs5 < -0.015:
                score += 15; comps['rs_vni_5d'] = (15, f'RS 5d={rs5*100:+.1f}% (yeu ro hon VNI — ES pool)')
            elif rs5 < -0.005:
                score += 10; comps['rs_vni_5d'] = (10, f'RS 5d={rs5*100:+.1f}% (yeu hon VNI)')
            else:
                comps['rs_vni_5d'] = (0, f'RS 5d={rs5*100:+.1f}%')
        else:
            comps['rs_vni_5d'] = (0, 'Can VNI data')

        # 4. RS vs VNINDEX 20d
        if vni_closes is not None and i >= 20 and i < len(vni_closes):
            s_ret  = (c[-1] / c[-20] - 1) * 100 if c[-20] > 0 else 0
            v_ret  = (vni_closes[i] / vni_closes[max(0, i-20)] - 1) * 100 \
                     if vni_closes[max(0, i-20)] > 0 else 0
            rs_vni = s_ret - v_ret
        elif n >= 21:
            rs_vni = (c[-1] / c[-21] - 1) * 100 if c[-21] > 0 else 0
        else:
            rs_vni = 0
        if rs_vni > 3:
            score += 10; comps['rs_vni_20d'] = (10, f'RS 20d +{rs_vni:.1f}% (hoi phuc tot)')
        else:
            comps['rs_vni_20d'] = (0, f'RS 20d {rs_vni:.1f}%')

        # 5. Vol expansion (xác nhận có dòng tiền vào)
        if vol_ratio_adj >= 1.3:
            score += 10; comps['vol_expansion'] = (10, f'Vol {vol_ratio_adj:.1f}x — dong tien vao')
        else:
            comps['vol_expansion'] = (0, f'Vol {vol_ratio_adj:.1f}x')

        # 6. Price structure: đang hồi phục (giá > open)
        open_c = float(opens[i]) if opens is not None and i < len(opens) else price
        if price >= open_c * 1.005:
            score += 10; comps['price_action'] = (10, 'Nen xanh — bên mua kiểm soát')
        else:
            comps['price_action'] = (0, 'Nen do/doji')

        # ── PENALTY ───────────────────────────────────────────────────────────
        if n >= 100:
            ma100 = float(np.mean(c[-100:]))
            if ma50 < ma100:
                score -= 10
                comps['weekly_down'] = (-10, 'Weekly DOWN: MA50<MA100')

        score = max(0, score)
        if score >= 90:   grade = 'STRONG'
        elif score >= 75: grade = 'PASS'
        else:             grade = ''
        return score, grade, True, comps

    # ═════════════════════════════════════════════════════════════════════════
    # MOMENTUM_V3: tất cả mã còn lại — bứt phá từ momentum đang hình thành
    # Pool: NKG(33ev) + POW(18ev) + FRT(31ev) + DGW(38ev) + MBS(32ev) = 152 ev
    # ═════════════════════════════════════════════════════════════════════════

    # ── TIER 1: GATE — price > MA50 + vol ────────────────────────────────────
    if not (price > ma50 and vol_today >= vol_threshold):
        return 0, '', False, {}

    score = 0
    comps = {}

    # ── 1. RS vs VNINDEX 20d (0/10/15đ) ──────────────────────────────────────
    if vni_closes is not None and i < len(vni_closes) and i >= 20:
        s_ret  = (c[-1] / c[-20] - 1) * 100 if c[-20] > 0 else 0
        v_ret  = (vni_closes[i] / vni_closes[max(0, i-20)] - 1) * 100 \
                 if vni_closes[max(0, i-20)] > 0 else 0
        rs_vni = s_ret - v_ret
    elif n >= 21:
        rs_vni = (c[-1] / c[-21] - 1) * 100 if c[-21] > 0 else 0
    else:
        rs_vni = 0

    if rs_vni > 5:
        score += 15; comps['rs_vni'] = (15, f'RS vs VNI +{rs_vni:.1f}% (manh)')
    elif rs_vni > 0:
        score += 10; comps['rs_vni'] = (10, f'RS vs VNI +{rs_vni:.1f}%')
    else:
        comps['rs_vni'] = (0, f'RS vs VNI {rs_vni:.1f}% (yeu)')

    # ── 2. RS vs Sector 10d (0/15/20đ) ───────────────────────────────────────
    if vni_closes is not None and i >= 10 and i < len(vni_closes):
        roc10_s = (c[-1] / c[-11] - 1) * 100 if n >= 11 and c[-11] > 0 else 0
        roc10_v = (vni_closes[i] / vni_closes[max(0, i-10)] - 1) * 100 \
                  if vni_closes[max(0, i-10)] > 0 else 0
        rs10    = roc10_s - roc10_v
        if rs10 > 5:
            score += 20; comps['rs_sector'] = (20, f'RS 10d +{rs10:.1f}% (outperform manh)')
        elif rs10 > 2:
            score += 15; comps['rs_sector'] = (15, f'RS 10d +{rs10:.1f}%')
        else:
            comps['rs_sector'] = (0, f'RS 10d {rs10:.1f}%')
    else:
        comps['rs_sector'] = (0, 'RS sector: can VNI data')

    # ── 3-5. RSI (giữ nguyên v1 — zone 45-70, tang 4d/5d) ───────────────────
    rsi_arr = _rsi_wilder(c)
    rsi_val = float(rsi_arr[-1])

    if 45 <= rsi_val <= 70:
        score += 10; comps['rsi_level'] = (10, f'RSI={rsi_val:.0f} (zone 45-70)')
    else:
        comps['rsi_level'] = (0, f'RSI={rsi_val:.0f} (ngoai zone)')

    rsi4 = float(rsi_arr[-5]) if len(rsi_arr) >= 5 else rsi_val
    if rsi_val >= rsi4 + 3:
        score += 10; comps['rsi_4d'] = (10, f'RSI tang {rsi_val-rsi4:.1f}/4phien')
    else:
        comps['rsi_4d'] = (0, f'RSI 4d: {rsi_val-rsi4:+.1f}')

    rsi5 = float(rsi_arr[-6]) if len(rsi_arr) >= 6 else rsi_val
    if rsi_val >= rsi5 + 5:
        score += 10; comps['rsi_5d'] = (10, f'RSI tang {rsi_val-rsi5:.1f}/5phien')
    else:
        comps['rsi_5d'] = (0, f'RSI 5d: {rsi_val-rsi5:+.1f}')

    # ── 6. Price structure: Close >= 80% range 20d ────────────────────────────
    low20  = float(np.min(l[-20:])) if n >= 20 else float(np.min(l))
    high20 = float(np.max(h[-20:])) if n >= 20 else float(np.max(h))
    range20 = high20 - low20
    if range20 > 0 and price >= low20 + 0.8 * range20:
        score += 10; comps['price_range'] = (10, 'Close >= 80% range 20d')
    else:
        pct_r = (price - low20) / range20 * 100 if range20 > 0 else 0
        comps['price_range'] = (0, f'Close {pct_r:.0f}% range 20d')

    # ── 7. Breakout 5d ────────────────────────────────────────────────────────
    if n >= 6:
        high5 = float(np.max(c[-6:-1]))
        if price >= high5:
            score += 10; comps['breakout'] = (10, f'Vuot high 5d ({high5:,.0f})')
        else:
            comps['breakout'] = (0, f'Chua vuot high 5d ({high5:,.0f})')

    # ── 8. Vol Expansion >= 1.5x ──────────────────────────────────────────────
    if vol_ratio_adj >= 1.5:
        score += 10; comps['vol_expansion'] = (10, f'Vol {vol_ratio_adj:.1f}x (>=1.5x)')
    else:
        comps['vol_expansion'] = (0, f'Vol {vol_ratio_adj:.1f}x')

    # ── 9. 52W Proximity ──────────────────────────────────────────────────────
    n52w   = min(252, n)
    high52 = float(np.max(h[-n52w:])) if n52w >= 20 else float(np.max(h))
    pct52  = price / high52 if high52 > 0 else 0
    if pct52 >= 1.0:
        score += 20; comps['w52'] = (20, f'Pha vo dinh 52W')
    elif pct52 >= 0.95:
        score += 15; comps['w52'] = (15, f'Gan dinh 52W ({pct52:.1%})')
    elif pct52 >= 0.90:
        score += 10; comps['w52'] = (10, f'Trong vung dinh 52W ({pct52:.1%})')
    else:
        comps['w52'] = (0, f'Xa dinh 52W ({pct52:.1%})')

    # ── 10. TREND_10D BONUS — pooled: NKG 6.31x + POW 6.4x ──────────────────
    # Evidence: 2 mã commodity cùng confirm, p<0.01 → áp cho tất cả momentum mã
    # Logic: giá đã tăng 10d liên tục → momentum đang hình thành
    if n >= 11:
        trend_10d = float(c[-1]) > float(c[-11])
        if trend_10d:
            chg = (c[-1] / c[-11] - 1) * 100
            score += 15; comps['trend_10d'] = (15, f'Gia tang {chg:+.1f}% trong 10d — ES pool 6.4x')
        else:
            chg = (c[-1] / c[-11] - 1) * 100
            comps['trend_10d'] = (0, f'Gia {chg:+.1f}% trong 10d (chua co momentum)')

    # ── 11. VOL_TREND_10D BONUS — pooled: NKG p=0.007, FRT p=0.047, DGW p=0.019
    # Logic: vol 5d gần tăng so với 5d trước → dòng tiền đang vào
    if n >= 11:
        vol_recent = float(np.mean(v[-5:]))
        vol_prev   = float(np.mean(v[-10:-5]))
        if vol_recent > vol_prev:
            ratio_vt = vol_recent / vol_prev if vol_prev > 0 else 1.0
            score += 10
            comps['vol_trend_10d'] = (10,
                f'Vol trend tang {(ratio_vt-1)*100:.0f}% — ES pool p<0.05')
        else:
            comps['vol_trend_10d'] = (0, f'Vol trend giam')

    # ── 12. VOL_SPIKE_3D BONUS — pooled: FRT p=0.036, DGW p=0.034 ───────────
    # Logic: vol 3d gần đây > 1.2× vol MA20 → có đợt mua lớn ngắn hạn
    if n >= 4:
        vol_3d = float(np.mean(v[-3:]))
        vspike = vol_3d / vol_ma20 if vol_ma20 > 0 else 1.0
        if vspike >= 1.3:
            score += 10; comps['vol_spike_3d'] = (10, f'Vol spike 3d={vspike:.2f}x — ES pool p<0.04')
        elif vspike >= 1.1:
            score += 5;  comps['vol_spike_3d'] = (5,  f'Vol spike 3d={vspike:.2f}x (nhe)')
        else:
            comps['vol_spike_3d'] = (0, f'Vol spike 3d={vspike:.2f}x')

    # ── 13. RS_VNI_5D ÂM BONUS — pooled: DGW p=0.016, MBS p=0.0045 ──────────
    # Counter-intuitive nhưng confirmed: mã bứt phá sau khi underperform VNI 5d
    # Logic: bị bỏ qua ngắn hạn → khi dòng tiền quay lại bứt phá mạnh hơn
    if vni_closes is not None and i >= 5 and i < len(vni_closes):
        rs5_s = (c[-1] / c[-6] - 1) if n >= 6 and c[-6] > 0 else 0
        rs5_v = (vni_closes[i] / vni_closes[max(0, i-5)] - 1) \
                if vni_closes[max(0, i-5)] > 0 else 0
        rs5   = rs5_s - rs5_v
        if rs5 < -0.015:
            score += 10; comps['rs_vni_5d_neg'] = (10, f'RS 5d={rs5*100:+.1f}% (bi bo qua — ES pool)')
        elif rs5 < -0.005:
            score += 5;  comps['rs_vni_5d_neg'] = (5,  f'RS 5d={rs5*100:+.1f}% (yeu nhe hon VNI)')
        else:
            comps['rs_vni_5d_neg'] = (0, f'RS 5d={rs5*100:+.1f}%')

    # ── PENALTIES ─────────────────────────────────────────────────────────────
    open_c = float(opens[i]) if opens is not None and i < len(opens) else price
    if price < open_c * 0.995 and vol_ratio_raw >= 1.5:
        score -= 10
        comps['dist_day'] = (-10, 'Distribution day: gia giam + vol cao')

    if n >= 100:
        ma100 = float(np.mean(c[-100:]))
        if ma50 < ma100:
            score -= 10
            comps['weekly_down'] = (-10, 'Weekly DOWN: MA50<MA100')

    score = max(0, score)
    if score >= 90:   grade = 'STRONG'
    elif score >= 75: grade = 'PASS'
    else:             grade = ''
    return score, grade, True, comps


def _get_v3_mode(symbol):
    """Trả về pattern mode cho symbol: 'counter_v3' hoặc 'momentum_v3'."""
    return 'counter_v3' if symbol.upper() in COUNTER_V3_SYMS else 'momentum_v3'

def run_backtest_momentum_v3(symbol, sl=0.06, tp=0.17, hold_days=18,
                              min_ml_score=75, days=None, verbose=True,
                              _df_cache=None, _vni_cache=None,
                              vol_time_pct=0.75,
                              min_liquidity_bil=3.0,
                              sector_mode=None):
    """
    Backtest ML v3 — Pooled Event Study (momentum_v3 / counter_v3).
    Tự detect mode từ symbol qua _get_v3_mode().
    Chạy được bất kỳ mã nào, không cần whitelist.
    """
    _days   = days if days is not None else LOOKBACK_DAYS
    _sector = sector_mode if sector_mode else _get_v3_mode(symbol)

    if verbose:
        print(f"\n{'═'*60}")
        print(f"  BACKTEST ML-v3 [{_sector.upper()}] {round(_days/365,0):.0f}Y: {symbol} | "
              f"SL={sl*100:.0f}% TP={tp*100:.0f}% Hold={hold_days}d "
              f"Score>={min_ml_score}")
        print('═'*60)

    if _df_cache is not None:
        df, source = _df_cache, 'cache'
    else:
        df, source = load_data(symbol, days=_days)
    if df is None:
        if verbose: print(f"  ✗ Không tải được dữ liệu {symbol}")
        return None

    cc = find_col(df, ['close', 'closeprice', 'close_price'])
    hc = find_col(df, ['high',  'highprice',  'high_price'])
    lc = find_col(df, ['low',   'lowprice',   'low_price'])
    vc = next((col for col in df.columns if col.lower() in
               ('volume','volume_match','klgd','vol','trading_volume',
                'match_volume','total_volume')), None)
    oc = find_col(df, ['open', 'openprice', 'open_price'])
    _tc = next((col for col in df.columns if col.lower() in
                ('time','date','datetime','trading_date')), None)

    if cc is None:
        return None

    closes  = to_arr(df[cc])
    highs   = to_arr(df[hc]) if hc else closes.copy()
    lows    = to_arr(df[lc]) if lc else closes.copy()
    opens   = to_arr(df[oc]) if oc else closes.copy()
    volumes = to_arr(df[vc]) if vc else np.ones(len(closes))

    for arr in [closes, highs, lows, opens]:
        if arr.max() < 1000:
            arr *= 1000

    if _tc:
        _dates = pd.to_datetime(df[_tc], errors='coerce').reset_index(drop=True)
    elif isinstance(df.index, pd.DatetimeIndex):
        _dates = pd.Series(df.index)
    else:
        _dates = pd.Series([pd.NaT] * len(df))

    # Load VNINDEX
    vni_closes = None
    try:
        df_vni = _vni_cache if _vni_cache is not None else load_data('VNINDEX', days=_days+60)[0]
        if df_vni is not None:
            cc_vni = find_col(df_vni, ['close', 'closeprice', 'close_price'])
            if cc_vni:
                _raw    = to_arr(df_vni[cc_vni])
                vni_raw = np.where(_raw < 1000, _raw * 1000, _raw)
                if len(vni_raw) >= len(closes):
                    vni_closes = vni_raw[-len(closes):]
                else:
                    pad = np.full(len(closes) - len(vni_raw), vni_raw[0])
                    vni_closes = np.concatenate([pad, vni_raw])
    except Exception:
        pass

    ML_COOLDOWN     = hold_days + SETTLEMENT_DAYS
    trades          = []
    last_signal_idx = -ML_COOLDOWN

    for i in range(55, len(closes) - hold_days - SETTLEMENT_DAYS):
        if i - last_signal_idx < ML_COOLDOWN:
            continue

        ml_score, grade, tier1, comps = compute_momentum_score_v3(
            closes, highs, lows, volumes, i,
            opens=opens, vni_closes=vni_closes,
            vol_time_pct=vol_time_pct,
            min_liquidity_bil=min_liquidity_bil,
            sector_mode=_sector,
        )

        if not tier1 or ml_score < min_ml_score:
            continue

        entry_idx = i
        pnl, reason, days_held = simulate_trade(
            closes, entry_idx, 'MUA', sl=sl, tp=tp,
            highs=highs, lows=lows, hold_days=hold_days
        )
        pnl = round(pnl - COMMISSION * 100, 2)

        _ts        = _dates.iloc[i] if i < len(_dates) else pd.NaT
        trade_date = _ts.strftime('%Y-%m-%d') if pd.notna(_ts) else f'idx_{i}'

        trades.append({
            'date':        trade_date,
            'price':       round(closes[entry_idx], 0),
            'ml_score':    ml_score,
            'grade':       grade,
            'action':      'MUA',
            'pnl':         pnl,
            'reason':      reason,
            'days':        days_held,
            'sector_mode': _sector,
            'rs_vni_pts':  comps.get('rs_vni',    (0,))[0],
            'w52_pts':     comps.get('w52',        (0,))[0],
            'rsi_pts':     (comps.get('rsi_level', (0,))[0] +
                            comps.get('rsi_4d',    (0,))[0] +
                            comps.get('rsi_5d',    (0,))[0]),
            'vol_trend_pts': comps.get('vol_trend_10d', (0,))[0],
            'trend10d_pts':  comps.get('trend_10d',     (0,))[0],
        })
        last_signal_idx = i

    if not trades:
        if verbose:
            print(f"  Không có tín hiệu ML-v3 [{_sector}] nào trong {_days} ngày dữ liệu.")
        return None

    df_t  = pd.DataFrame(trades)
    stats = calc_stats(df_t, 'MUA')

    df_t['year'] = pd.to_datetime(df_t['date'], errors='coerce').dt.year
    yearly_res   = {}
    for yr, grp in df_t.groupby('year'):
        yearly_res[int(yr)] = calc_stats(grp, 'MUA')

    grade_res = {}
    for g in ['STRONG', 'PASS']:
        sub = df_t[df_t['grade'] == g]
        if len(sub) >= 3:
            grade_res[g] = calc_stats(sub, 'MUA')

    if verbose:
        b    = stats
        pf_s = f"{b['profit_factor']:.2f}" if b['profit_factor'] != float('inf') else '∞'
        print(f"\n  ML-v3 [{_sector}] MUA: {b['total']}L | WR={b['win_rate']}% "
              f"[{b['ci_low']}-{b['ci_high']}%] | "
              f"AvgPnL={b['avg_pnl']:+.2f}% | PF={pf_s}")
        print(f"         TP={b['tp']}L  SL={b['sl']}L  Hết={b['expired']}L  "
              f"AvgHold={b['avg_days']:.0f}d")
        if grade_res:
            for g, gs in grade_res.items():
                pf2 = f"{gs['profit_factor']:.2f}" if gs['profit_factor'] != float('inf') else '∞'
                print(f"         [{g}]: {gs['total']}L WR={gs['win_rate']}% "
                      f"PnL={gs['avg_pnl']:+.2f}% PF={pf2}")
        print(f"\n  Yearly:")
        for yr, ys in sorted(yearly_res.items()):
            if ys['total'] > 0:
                phase = MARKET_PHASES.get(yr, '?')
                print(f"    {yr} [{phase:8s}]: {ys['total']:2d}L WR={ys['win_rate']:5.1f}% "
                      f"PnL={ys['avg_pnl']:+.2f}%")

    return {
        'symbol':            symbol,
        'sl':                sl,
        'tp':                tp,
        'hold_days':         hold_days,
        'min_ml_score':      min_ml_score,
        'sector_mode':       _sector,
        'vol_time_pct':      vol_time_pct,
        'min_liquidity_bil': min_liquidity_bil,
        'buy':               stats,
        'trades':            df_t,
        'yearly':            {'yearly': yearly_res},
        'grade_stats':       grade_res,
        'conf': {'ci_low': stats.get('ci_low', 0), 'ci_high': stats.get('ci_high', 100)},
    }


def run_walk_forward_momentum_v3(symbol, sl=0.06, tp=0.17, hold_days=18,
                                  min_ml_score=75, verbose=True,
                                  vol_time_pct=0.75,
                                  min_liquidity_bil=3.0,
                                  sector_mode=None,
                                  _df_cache=None, _vni_cache=None):
    """
    Walk-Forward cho ML v3. Cùng window structure với v1/v2.
    _df_cache: DataFrame đã load sẵn — tránh gọi vnstock thêm.
    """
    _sector = sector_mode if sector_mode else _get_v3_mode(symbol)
    _days   = LOOKBACK_DAYS

    if _df_cache is not None:
        df, source = _df_cache, 'cache'
    else:
        df, source = load_data(symbol, days=_days)
    if df is None:
        return None

    cc = find_col(df, ['close', 'closeprice', 'close_price'])
    if cc is None:
        return None
    closes = to_arr(df[cc])
    if closes.max() < 1000:
        closes *= 1000

    n_rows = len(closes)
    if n_rows < 1500:
        if verbose:
            print(f"  {symbol}: Không đủ data WF-v3 ({n_rows} rows < 1500)")
        return None

    # Load VNINDEX 1 lần
    try:
        df_vni = _vni_cache if _vni_cache is not None else load_data('VNINDEX', days=_days+60)[0]
    except Exception:
        df_vni = None

    IS_DAYS  = 756
    OOS_DAYS = 252
    windows  = []
    start    = 0

    while start + IS_DAYS + OOS_DAYS <= n_rows:
        is_end  = start + IS_DAYS
        oos_end = min(is_end + OOS_DAYS, n_rows)

        df_is  = df.iloc[start:is_end].reset_index(drop=True)
        df_oos = df.iloc[is_end:oos_end].reset_index(drop=True)

        r_is  = run_backtest_momentum_v3(symbol, sl=sl, tp=tp, hold_days=hold_days,
                                          min_ml_score=min_ml_score, verbose=False,
                                          _df_cache=df_is, _vni_cache=df_vni,
                                          vol_time_pct=vol_time_pct,
                                          min_liquidity_bil=min_liquidity_bil,
                                          sector_mode=_sector)
        r_oos = run_backtest_momentum_v3(symbol, sl=sl, tp=tp, hold_days=hold_days,
                                          min_ml_score=min_ml_score, verbose=False,
                                          _df_cache=df_oos, _vni_cache=df_vni,
                                          vol_time_pct=vol_time_pct,
                                          min_liquidity_bil=min_liquidity_bil,
                                          sector_mode=_sector)

        is_wr   = r_is['buy']['win_rate']   if r_is  else 0
        is_pnl  = r_is['buy']['avg_pnl']    if r_is  else 0
        oos_wr  = r_oos['buy']['win_rate']  if r_oos else 0
        oos_pnl = r_oos['buy']['avg_pnl']  if r_oos else 0
        oos_n   = r_oos['buy']['total']     if r_oos else 0

        try:
            tc        = find_col(df_oos, ['time', 'date', 'tradingdate', 'trading_date'])
            oos_label = str(pd.to_datetime(df_oos[tc].iloc[0]).year) if tc else str(len(windows)+1)
        except Exception:
            oos_label = str(len(windows) + 1)

        windows.append({
            'is_n':   r_is['buy']['total']  if r_is  else 0,
            'is_wr':  is_wr,  'is_pnl':  is_pnl,
            'oos_n':  oos_n,  'oos_wr':  oos_wr,  'oos_pnl': oos_pnl,
            'oos_label': oos_label,
        })
        start += OOS_DAYS

    if not windows:
        return None

    valid_oos = [w for w in windows if w['oos_n'] >= 3]
    if not valid_oos:
        valid_oos = [w for w in windows if w['oos_n'] >= 1]
    if not valid_oos:
        return None

    avg_is_wr   = float(np.mean([w['is_wr']   for w in windows]))
    avg_oos_wr  = float(np.mean([w['oos_wr']  for w in valid_oos]))
    avg_is_pnl  = float(np.mean([w['is_pnl']  for w in windows]))
    avg_oos_pnl = float(np.mean([w['oos_pnl'] for w in valid_oos]))
    decay_wr    = avg_is_wr - avg_oos_wr

    if avg_oos_wr >= 55 and decay_wr < 10 and avg_oos_pnl >= 0.5:
        verdict, verdict_txt = 'Robust', 'Robust — OOS on dinh'
    elif avg_oos_wr >= 48 or avg_oos_pnl >= 0:
        verdict, verdict_txt = 'Chap nhan', 'OOS chap nhan duoc'
    else:
        verdict, verdict_txt = 'Yeu', 'He thong khong on dinh OOS'

    if verbose:
        print(f"\n  Walk-Forward ML-v3 [{_sector}] {symbol} ({len(windows)} windows, "
              f"SL={sl*100:.0f}% TP={tp*100:.0f}% Hold={hold_days}d):")
        print(f"  Avg IS:  WR={avg_is_wr:.1f}% PnL={avg_is_pnl:+.2f}%")
        print(f"  Avg OOS: WR={avg_oos_wr:.1f}% PnL={avg_oos_pnl:+.2f}% (decay={decay_wr:+.1f}%)")
        for w in windows:
            print(f"    OOS {w['oos_label']}: {w['oos_n']:2d}L IS_WR={w['is_wr']:.0f}% "
                  f"→ OOS_WR={w['oos_wr']:.0f}% PnL={w['oos_pnl']:+.2f}%")
        print(f"  [{verdict}] {verdict_txt}")

    return {
        'symbol': symbol, 'sector_mode': _sector, 'windows': windows,
        'avg_is_wr':   round(avg_is_wr,  1), 'avg_oos_wr':  round(avg_oos_wr,  1),
        'avg_is_pnl':  round(avg_is_pnl, 2), 'avg_oos_pnl': round(avg_oos_pnl, 2),
        'decay_wr':    round(decay_wr, 1),
        'verdict':     verdict, 'verdict_txt': verdict_txt,
        'sl': sl, 'tp': tp, 'hold_days': hold_days,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# EVENT STUDY — P3
# Mục tiêu: tìm pattern trước big moves (+15%/18d) để build ML v3
# Data scope: 2018-2025 | Dedup: skip 18d sau mỗi event (option B)
# Anti-hindsight: tất cả features chỉ dùng data tại T-1 trở về trước
# ═══════════════════════════════════════════════════════════════════════════════

from concurrent.futures import ThreadPoolExecutor

# ── Constants ─────────────────────────────────────────────────────────────────
EVENT_BIG_MOVE_PCT   = 0.15   # +15% forward return
EVENT_WINDOW_DAYS    = 18     # trong 18 ngày
MIN_PRECISION_RATIO  = 2.0    # pattern phải precision > 2× base rate
MIN_EVENT_COUNT      = 5      # bỏ qua mã nếu < 5 big moves
RANDOM_SAMPLE_SIZE   = 500
RANDOM_SEED          = 42

EVENT_STUDY_START    = '2018-01-01'
EVENT_STUDY_END      = '2025-12-31'

# Mã đã có event study confirm (pooled pattern)
# Momentum: NKG, POW, FRT, DGW, MBS | Counter: MWG, PNJ
EVENT_STUDY_SYMBOLS = list(COUNTER_V3_SYMS) + ['NKG', 'POW', 'FRT', 'DGW', 'MBS']

# ── Feature lists ─────────────────────────────────────────────────────────────
_ES_BOOL_FEATURES = [
    'vol_dry_up_5d',
    'price_above_ma20', 'price_above_ma50', 'price_above_ma200',
    'near_52w_high', 'near_52w_low',
    'inside_bar_3d',
    'trend_5d', 'trend_10d',
    'rsi_zone_oversold', 'rsi_zone_neutral', 'rsi_zone_overbought',
    'rsi_rising_5d',
    'macd_cross',
    'rs_improving', 'rs_outperform_both',
]
_ES_NUMERIC_FEATURES = [
    'vol_spike_3d',
    'vol_trend_10d',
    'rs_vs_vni_5d',
    'rs_vs_vni_20d',
    'rsi_val',
]


# ── STEP 1 — Find big moves (dedup skip 18d) ──────────────────────────────────



# ─── SECTOR ROTATION BACKTEST ─────────────────────────────────────────────────

def run_sector_backtest(sector_map, sl=0.07, tp=0.15, hold_days=10,
                        scr_avg_threshold=55, days=LOOKBACK_DAYS,
                        bear_skip=True, shock_skip_pct=-3.0,
                        verbose=False):
    """
    Backtest chiến lược Sector Rotation — Hướng C:
      - Mỗi ngày: tìm ngành tốt nhất (ScR_avg cao nhất, >= scr_avg_threshold)
      - Entry: top1 mã của ngành đó (ScR cao nhất)
      - Skip nếu: BEAR regime | VNI shock (5d < shock_skip_pct) | đang giữ mã đó
      - Exit: TP / SL / HK 10 ngày

    Parameters:
        sector_map         : dict {sector_name: [sym, ...]}
        sl                 : stop loss (0.07 = 7%)
        tp                 : take profit (0.15 = 15%)
        hold_days          : số ngày giữ lệnh
        scr_avg_threshold  : ngưỡng ScR_avg ngành để vào (default 55)
        days               : số ngày lịch sử load
        bear_skip          : bỏ qua khi VNI BEAR regime
        shock_skip_pct     : bỏ qua khi VNI 5d ret < ngưỡng (default -3%)

    Returns:
        dict với keys: trades, stats, yearly, sector_stats, params
    """
    import numpy as np, pandas as pd

    # ── 1. Load VNINDEX ───────────────────────────────────────────────────────
    if verbose:
        print("Loading VNINDEX...")
    df_vni, _ = load_data('VNINDEX', days=days + 60)
    if df_vni is None or len(df_vni) < 200:
        return {'error': 'Không load được VNINDEX'}

    cc_vni = find_col(df_vni, ['close', 'closeprice', 'close_price'])
    tc_vni = next((c for c in df_vni.columns
                   if c.lower() in ('time', 'date', 'datetime', 'trading_date')), None)
    if cc_vni is None:
        return {'error': 'VNINDEX không có cột close'}

    vni_closes = to_arr(df_vni[cc_vni])
    if vni_closes.max() < 1000:
        vni_closes *= 1000
    vni_dates = pd.to_datetime(df_vni[tc_vni], errors='coerce') if tc_vni else None

    # ── 2. Load OHLCV tất cả mã trong sector_map ─────────────────────────────
    # Dedup mã, giữ mapping sym → sector
    sym_sector = {}
    all_syms   = []
    for sec, syms in sector_map.items():
        for s in syms:
            if s not in sym_sector:
                sym_sector[s] = sec
                all_syms.append(s)

    if verbose:
        print(f"Loading {len(all_syms)} mã...")

    sym_data = {}  # sym → {'closes','highs','lows','volumes','dates','ma50_arr'}
    for sym in all_syms:
        try:
            df, _ = load_data(sym, days=days + 60)
            if df is None or len(df) < 200:
                continue
            cc = find_col(df, ['close', 'closeprice', 'close_price'])
            hc = find_col(df, ['high',  'highprice',  'high_price'])
            lc = find_col(df, ['low',   'lowprice',   'low_price'])
            vc = next((c for c in df.columns if c.lower() in
                       ('volume','volume_match','klgd','vol')), None)
            tc = next((c for c in df.columns if c.lower() in
                       ('time','date','datetime','trading_date')), None)
            if cc is None:
                continue

            cl = to_arr(df[cc]); hi = to_arr(df[hc]) if hc else cl.copy()
            lo = to_arr(df[lc]) if lc else cl.copy()
            vo = to_arr(df[vc]) if vc else np.ones(len(cl))
            if cl.max() < 1000:
                cl *= 1000; hi *= 1000; lo *= 1000

            dates = pd.to_datetime(df[tc], errors='coerce') if tc else None

            # Pre-compute rolling MA50 (pandas rolling, nhanh)
            cl_s    = pd.Series(cl)
            ma50_arr= cl_s.rolling(50, min_periods=10).mean().values

            sym_data[sym] = {
                'closes': cl, 'highs': hi, 'lows': lo,
                'volumes': vo, 'dates': dates, 'ma50_arr': ma50_arr,
                'n': len(cl),
            }
        except Exception:
            pass

    if verbose:
        print(f"Loaded {len(sym_data)}/{len(all_syms)} mã OK")

    # ── 3. Build date-aligned index ───────────────────────────────────────────
    # Dùng VNI dates làm calendar chính
    n_vni = len(vni_closes)

    # ── 4. Main backtest loop ─────────────────────────────────────────────────
    trades       = []
    open_trades  = {}   # sym → entry_idx_in_sym_array (để skip double entry)

    # Lookback cần ít nhất 200 ngày để tính MA200
    START_IDX = 220

    for i in range(START_IDX, n_vni - hold_days - 3):

        # ── Filter 0A: VNI Regime ─────────────────────────────────────────────
        if bear_skip:
            vni_i   = vni_closes[i]
            ma50_i  = float(np.mean(vni_closes[max(0, i-50):i+1]))
            ma200_i = float(np.mean(vni_closes[max(0, i-200):i+1]))
            if vni_i <= ma200_i:
                continue   # BEAR — đứng ngoài

        # ── Filter 0B: VNI 5d shock ───────────────────────────────────────────
        if i >= 5:
            vni_5d = (vni_closes[i] / vni_closes[i-5] - 1) * 100
            if vni_5d < shock_skip_pct:
                continue   # Cú sốc ngắn hạn — đứng ngoài

        # ── Tính ScR cho từng ngành tại ngày i ───────────────────────────────
        best_sector    = None
        best_scr_avg   = -1
        best_sym       = None
        best_sym_scr   = -1

        for sec, sec_syms in sector_map.items():
            # Thu thập indicators cho tất cả mã trong ngành tại ngày i
            peer_data = []
            for sym in sec_syms:
                if sym not in sym_data:
                    continue
                sd = sym_data[sym]
                n_sym = sd['n']

                # Align index: VNI[i] ~ sym[i_sym]
                # Dùng ratio vì mỗi mã có thể load khác số ngày
                ratio   = n_sym / n_vni
                i_sym   = min(int(i * ratio), n_sym - 1)
                if i_sym < 55:   # cần ít nhất 55 nến để tính đủ
                    continue

                cl  = sd['closes']
                vo  = sd['volumes']
                ma50= sd['ma50_arr']

                # rs_5d
                rs5 = float((cl[i_sym] / cl[i_sym-5] - 1) * 100) if i_sym >= 5 else 0.0
                # vol_ratio
                vol_ma20 = float(np.mean(vo[max(0,i_sym-20):i_sym])) if i_sym >= 20 else float(vo[i_sym])
                vol_ratio = float(vo[i_sym] / vol_ma20) if vol_ma20 > 0 else 1.0
                # ma50_slope proxy (5 ngày)
                ma50_slope = float((ma50[i_sym] / ma50[i_sym-5] - 1) * 100) \
                             if (i_sym >= 5 and ma50[i_sym-5] > 0
                                 and not np.isnan(ma50[i_sym])
                                 and not np.isnan(ma50[i_sym-5])) else 0.0

                peer_data.append({
                    'sym': sym, 'rs_5d': rs5, 'vol_ratio': vol_ratio,
                    'ma50_slope': ma50_slope, 'i_sym': i_sym,
                })

            if len(peer_data) < 2:
                continue

            # vni_5d_ret tại ngày i
            vni_5d_ret = float((vni_closes[i] / vni_closes[i-5] - 1) * 100) \
                         if i >= 5 else 0.0

            # Tính ScR cho từng mã
            sec_avg_rs5   = float(np.mean([p['rs_5d']       for p in peer_data]))
            sec_avg_vr    = float(np.mean([p['vol_ratio']    for p in peer_data]))
            sec_avg_slope = float(np.mean([p['ma50_slope']   for p in peer_data]))

            scr_list = []
            for p in peer_data:
                # C1: RS 5d vs TB ngành (30đ)
                diff_rs5 = p['rs_5d'] - sec_avg_rs5
                if   diff_rs5 >= 4.0: c1 = 30
                elif diff_rs5 >= 2.0: c1 = 22
                elif diff_rs5 >= 0.5: c1 = 15
                elif diff_rs5 >= -1.0: c1 = 7
                else: c1 = 0

                # C2: Vol vs TB ngành (25đ)
                if p['vol_ratio'] < 1.2:
                    c2 = 0
                else:
                    vr_diff = p['vol_ratio'] - sec_avg_vr
                    if   vr_diff >= 0.8: c2 = 25
                    elif vr_diff >= 0.4: c2 = 18
                    elif vr_diff >= 0.0: c2 = 12
                    else: c2 = 5

                # C3: MA50 slope (20đ)
                slope_diff = p['ma50_slope'] - sec_avg_slope
                if   p['ma50_slope'] >= 1.0 and slope_diff >= 0.3: c3 = 20
                elif p['ma50_slope'] >= 0.5: c3 = 14
                elif p['ma50_slope'] >= 0.0: c3 = 7
                else: c3 = 0

                # C4: RS 5d vs VNI (25đ)
                diff_vni = p['rs_5d'] - vni_5d_ret
                if   diff_vni >= 3.0: c4 = 25
                elif diff_vni >= 1.0: c4 = 18
                elif diff_vni >= 0.0: c4 = 12
                elif diff_vni >= -1.0: c4 = 5
                else: c4 = 0

                scr = min(100, c1 + c2 + c3 + c4)
                scr_list.append((scr, p['sym'], p['i_sym']))

            # ScR_avg ngành = TB tất cả mã
            scr_avg = float(np.mean([s[0] for s in scr_list]))
            if scr_avg < scr_avg_threshold:
                continue

            # Top1 mã trong ngành
            scr_list.sort(key=lambda x: -x[0])
            top_scr, top_sym, top_i_sym = scr_list[0]

            # So sánh ngành này với best hiện tại
            if scr_avg > best_scr_avg:
                best_scr_avg  = scr_avg
                best_sector   = sec
                best_sym      = top_sym
                best_sym_scr  = top_scr

        # Không có ngành đạt ngưỡng
        if best_sym is None:
            continue

        # Skip nếu đang có lệnh mở cho mã này
        if best_sym in open_trades:
            continue

        # ── Entry ─────────────────────────────────────────────────────────────
        sd     = sym_data[best_sym]
        n_sym  = sd['n']
        ratio  = n_sym / n_vni
        i_sym  = min(int(i * ratio), n_sym - 1)

        # Entry T+1: dùng close ngày i làm entry price (proxy T+1 open)
        entry_i   = min(i_sym + 1, n_sym - 1)
        entry_px  = sd['closes'][entry_i]
        if entry_px <= 0:
            continue

        # Simulate trade từ entry_i
        if entry_i + hold_days + 3 >= n_sym:
            continue   # Không đủ data để simulate

        pnl, reason, days_held = simulate_trade(
            sd['closes'], entry_i,
            direction='MUA', sl=sl, tp=tp,
            highs=sd['highs'], lows=sd['lows'],
            hold_days=hold_days
        )

        # Lấy date để ghi trade log
        trade_date = ''
        if vni_dates is not None and i < len(vni_dates):
            td = vni_dates.iloc[i]
            trade_date = str(td.date()) if pd.notna(td) else ''

        trades.append({
            'date':       trade_date,
            'sym':        best_sym,
            'sector':     best_sector,
            'scr_sym':    round(best_sym_scr, 1),
            'scr_avg':    round(best_scr_avg, 1),
            'entry_px':   round(entry_px, 0),
            'pnl':        pnl,
            'reason':     reason,
            'days':       days_held,
            'action':     'MUA',
            'vni_i':      i,
        })

        # Mark open — sẽ clear sau hold_days (simple: dùng vni index)
        open_trades[best_sym] = i + hold_days + 2

        # Cleanup open_trades (remove expired)
        open_trades = {s: exp for s, exp in open_trades.items() if exp > i}

    if not trades:
        return {'error': 'Không có lệnh nào được sinh ra', 'trades': []}

    df_t = pd.DataFrame(trades)

    # ── Stats tổng thể ────────────────────────────────────────────────────────
    stats = calc_stats(df_t)

    # ── Yearly breakdown ──────────────────────────────────────────────────────
    yearly = {}
    if 'date' in df_t.columns and df_t['date'].str.len().gt(3).any():
        df_t['year'] = df_t['date'].str[:4]
        for yr, grp in df_t.groupby('year'):
            s = calc_stats(grp)
            yearly[yr] = {
                'n': s['total'], 'wr': s['win_rate'],
                'avg_pnl': s['avg_pnl'], 'pf': s['profit_factor'],
            }

    # ── Per-sector stats ──────────────────────────────────────────────────────
    sector_stats = {}
    for sec, grp in df_t.groupby('sector'):
        s = calc_stats(grp)
        sector_stats[sec] = {
            'n': s['total'], 'wr': s['win_rate'],
            'avg_pnl': s['avg_pnl'], 'pf': s['profit_factor'],
            'tp': s['tp'], 'sl': s['sl'], 'expired': s['expired'],
        }

    return {
        'trades':       df_t,
        'stats':        stats,
        'yearly':       yearly,
        'sector_stats': sector_stats,
        'params': {
            'sl': sl, 'tp': tp, 'hold_days': hold_days,
            'scr_avg_threshold': scr_avg_threshold,
            'bear_skip': bear_skip, 'shock_skip_pct': shock_skip_pct,
            'n_syms_loaded': len(sym_data),
        },
    }
