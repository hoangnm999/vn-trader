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
    SYMBOL_CONFIG, BACKTEST_WATCHLIST,
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
    if len(v) >= 20:
        vol_ma20 = np.mean(v[-20:])
        vol_today = v[-1]
        vol_ratio = vol_today / vol_ma20 if vol_ma20 > 0 else 1.0
        if vol_ratio >= 2.0 and price > c[-2]:
            score += 8   # volume spike up
        elif vol_ratio >= 1.5 and price > c[-2]:
            score += 5
        elif vol_ratio < 0.5:
            score -= 2

    # ── Bollinger ────────────────────────────────────────────────────────────
    if len(c) >= 20:
        bb_up, bb_mid, bb_low = _bollinger(c)
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
        if i >= 40:
            _bws = []
            for _k in range(max(0,i-19), i+1):
                _w = float(np.std(closes[max(0,_k-20):_k+1])) * 4
                _m = float(np.mean(closes[max(0,_k-20):_k+1]))
                _bws.append(_w/_m*100 if _m>0 else 0)
            if len(_bws) >= 20:
                _sq = _bws[-1] < float(np.percentile(_bws, 20))
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
        if i >= 60:
            _ma50n = float(np.mean(closes[i-49:i+1]))
            _ma50p = float(np.mean(closes[i-59:i-9]))
            _sl = (_ma50n/_ma50p-1)*100 if _ma50p>0 else 0
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
                   highs=None, lows=None):
    """
    Simulate 1 trade với T+2 settlement.
    sl/tp là số dương: sl=0.07 = cắt lỗ -7%, tp=0.14 = chốt +14%.
    Returns: (pnl_pct, reason, days_held)
    """
    _sl = -(sl if sl is not None else abs(STOP_LOSS))
    _tp =   tp if tp is not None else TAKE_PROFIT

    entry_price = closes[entry_idx]
    if entry_price <= 0:
        return 0.0, 'invalid', 0

    total_days = SETTLEMENT_DAYS + HOLD_DAYS
    n = len(closes)

    for d in range(SETTLEMENT_DAYS, total_days + 1):
        idx = entry_idx + d
        if idx >= n:
            # Hết data — tính theo close cuối
            exit_price = closes[-1]
            pnl = (exit_price / entry_price - 1) * 100
            return round(pnl, 2), 'expired', d

        hi = highs[idx]  if highs  is not None else closes[idx]
        lo = lows[idx]   if lows   is not None else closes[idx]
        cl = closes[idx]

        # Check SL (worst case intraday)
        pnl_lo = (lo / entry_price - 1)
        if pnl_lo <= _sl:
            exit_price = entry_price * (1 + _sl)
            return round(_sl * 100, 2), 'sl', d

        # Check TP
        pnl_hi = (hi / entry_price - 1)
        if pnl_hi >= _tp:
            exit_price = entry_price * (1 + _tp)
            return round(_tp * 100, 2), 'tp', d

        # Cuối HOLD_DAYS — close out
        if d == total_days:
            pnl = (cl / entry_price - 1) * 100
            return round(pnl, 2), 'expired', d

    return 0.0, 'expired', HOLD_DAYS

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
        'avg_days':      round(subset['days'].mean(), 1),
        'tp':            int(len(subset[subset['reason'] == 'tp'])),
        'sl':            int(len(subset[subset['reason'] == 'sl'])),
        'expired':       int(len(subset[subset['reason'] == 'expired'])),
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
                        hold_days=None, min_score=None, _vni_cache=None):
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
    if use_regime:
        try:
            df_vni = _vni_cache if _vni_cache is not None else load_data('VNINDEX', days=_days+60)[0]
            if df_vni is not None:
                cc_vni = find_col(df_vni, ['close', 'closeprice', 'close_price'])
                if cc_vni:
                    _vni_raw = to_arr(df_vni[cc_vni])
                    _vni_closes = np.where(_vni_raw < 1000, _vni_raw * 1000, _vni_raw)
        except Exception:
            pass

    # Pre-compute VWAP
    try:
        _vwap_w, _vwap_m = compute_vwap_arrays(closes, volumes, _dates)
    except Exception:
        _vwap_w = _vwap_m = None

    # ── Main backtest loop ─────────────────────────────────────────────────
    trades          = []
    last_signal_idx = -(HOLD_DAYS + SETTLEMENT_DAYS)

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
            closes, entry_idx, action, sl=_sl, tp=_tp, highs=highs, lows=lows
        )
        pnl = round(pnl - COMMISSION * 100, 2)

        _ts = _dates.iloc[i] if i < len(_dates) else pd.NaT
        trade_date = _ts.strftime('%Y-%m-%d') if pd.notna(_ts) else f'idx_{i}'

        trades.append({
            'date': trade_date, 'price': round(closes[entry_idx], 0),
            'score': score, 'regime': _regime_at_i,
            'action': action, 'pnl': pnl, 'reason': reason, 'days': days_held,
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
        print(f"       TP={b['tp']}L SL={b['sl']}L Hết={b['expired']}L")

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

def run_walk_forward(symbol, verbose=True, _df_cache=None):
    """
    Walk-Forward validation: chia data thành 4 windows liên tiếp.
    Mỗi window: IS=3 năm, OOS=1 năm.
    Returns dict với avg_oos_wr, avg_oos_pnl, verdict.
    """
    cfg        = SYMBOL_CONFIG.get(symbol.upper(), {})
    use_regime = cfg.get('use_regime', True)
    use_vwap   = cfg.get('use_vwap', True)
    _sl        = cfg.get('sl',  abs(STOP_LOSS))
    _tp        = cfg.get('tp',  TAKE_PROFIT)

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
    # Dùng min_score từ config nhưng không thấp hơn 50 và không cao hơn 65
    # để đảm bảo đủ lệnh trong mỗi window WF
    _wf_min_score = max(50, min(65, SYMBOL_CONFIG.get(symbol.upper(), {}).get('min_score', MIN_SCORE_BUY)))

    while start + is_size + oos_size <= total:
        df_is  = df.iloc[start:start+is_size].copy()
        df_oos = df.iloc[start+is_size:start+is_size+oos_size].copy()

        r_is = run_backtest_symbol(symbol, verbose=False,
                                    sl=_sl, tp=_tp, use_regime=use_regime,
                                    use_vwap=use_vwap, _df_cache=df_is,
                                    min_score=_wf_min_score)
        r_oos = run_backtest_symbol(symbol, verbose=False,
                                     sl=_sl, tp=_tp, use_regime=use_regime,
                                     use_vwap=use_vwap, _df_cache=df_oos,
                                     min_score=_wf_min_score)

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

        r_is = run_backtest_symbol(symbol, verbose=False,
                                    sl=sl/100, tp=tp/100,
                                    use_regime=use_regime, use_vwap=use_vwap,
                                    _df_cache=df_is, min_score=score_thr,
                                    hold_days=hold, _vni_cache=vni_cache)
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
                                         hold_days=hold, _vni_cache=vni_cache)
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

def analyze_by_year_detailed(result):
    """Tóm tắt kết quả backtest theo năm."""
    if not result or 'yearly' not in result:
        return ''
    # result['yearly'] = {'yearly': {2017: stats, 2018: stats, ...}}
    yearly_outer = result['yearly']
    yearly = yearly_outer.get('yearly', yearly_outer) if isinstance(yearly_outer, dict) else yearly_outer
    lines  = []
    for yr in sorted(yearly.keys()):
        st = yearly[yr]
        if not isinstance(st, dict) or st.get('total', 0) == 0:
            continue
        pf_s = f"{st['profit_factor']:.2f}" if st['profit_factor'] != float('inf') else '∞'
        lines.append(f"  {yr}: {st['total']}L WR={st['win_rate']:.0f}% PnL={st['avg_pnl']:+.2f}% PF={pf_s}")
    return '\n'.join(lines)




# ─── MOMENTUM LEADER BACKTEST ─────────────────────────────────────────────────
# Hệ thống độc lập với Score A.
# Tier 1 (gate): price > MA50 AND vol > vol_MA20 × 1.2
# Tier 2 (9 components, max 120đ): RS + RSI momentum + Price structure + Vol + 52W
# Signal: tier1_pass AND score >= min_ml_score (default 75)
# SL/TP/Hold: tối ưu hóa riêng (context doc: SL=6-7%, TP=15-18%, Hold=15-20 ngày)

def compute_momentum_score_at(closes, highs, lows, volumes, i,
                               opens=None, vni_closes=None,
                               vol_time_pct=0.75):
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

    # ── Tier 1: Gate ─────────────────────────────────────────────────────────
    ma50     = float(np.mean(c[-50:]))
    vol_ma20  = float(np.mean(v[-20:])) if n >= 20 else float(np.mean(v))
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
                          vol_time_pct=0.75):
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
    Returns dict: symbol, sl, tp, hold_days, min_ml_score, vol_time_pct,
                  buy (stats), trades (DataFrame), yearly, grade_stats, conf
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
        )

        if not tier1 or ml_score < min_ml_score:
            continue

        # ── Entry: intraday detect → mua TRONG phiên i ───────────────────
        # Signal phát hiện lúc ~14:00 → đặt lệnh ngay trong phiên
        # entry_price = closes[i] là worst-case (mua ATC cuối phiên)
        entry_idx = i

        pnl, reason, days_held = simulate_trade(
            closes, entry_idx, 'MUA', sl=sl, tp=tp,
            highs=highs, lows=lows
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
        print(f"         Cooldown={ML_COOLDOWN}d | vol_time={vol_time_pct:.0%}")
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
        'symbol':       symbol,
        'sl':           sl,
        'tp':           tp,
        'hold_days':    hold_days,
        'min_ml_score': min_ml_score,
        'vol_time_pct': vol_time_pct,
        'buy':          stats,
        'trades':       df_t,
        'yearly':       {'yearly': yearly_res},
        'grade_stats':  grade_res,
        'conf': {'ci_low': stats.get('ci_low', 0), 'ci_high': stats.get('ci_high', 100)},
    }


def run_walk_forward_momentum(symbol, sl=0.06, tp=0.17, hold_days=18,
                               min_ml_score=75, verbose=True,
                               vol_time_pct=0.75):
    """
    Walk-Forward validation cho Momentum Leader.
    Cùng cấu trúc window với run_walk_forward() (Score A):
      >= 1500 rows: IS=756d OOS=252d
      >= 800 rows:  IS=504d OOS=126d
      >= 400 rows:  IS=252d OOS=88d

    vol_time_pct được truyền xuống run_backtest_momentum cho mỗi window
    → BUG-2 cooldown fix được áp dụng tự động qua hold_days param
    """
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

    # Load VNI 1 lần cho toàn bộ WF
    try:
        df_vni, _ = load_data('VNINDEX', days=LOOKBACK_DAYS + 60)
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
        )
        r_oos = run_backtest_momentum(
            symbol, sl=sl, tp=tp, hold_days=hold_days,
            min_ml_score=min_ml_score, verbose=False,
            _df_cache=df_oos, _vni_cache=df_vni,
            vol_time_pct=vol_time_pct,
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
        'avg_is_wr':   round(avg_is_wr,   1), 'avg_oos_wr':  round(avg_oos_wr,  1),
        'avg_is_pnl':  round(avg_is_pnl,  2), 'avg_oos_pnl': round(avg_oos_pnl, 2),
        'decay_wr':    round(decay_wr, 1),
        'verdict':     verdict, 'verdict_txt': verdict_txt,
        'sl': sl, 'tp': tp, 'hold_days': hold_days, 'vol_time_pct': vol_time_pct,
    }


# ─── CLI ENTRY POINT ──────────────────────────────────────────────────────────

if __name__ == '__main__':
    syms = sys.argv[1:] if len(sys.argv) > 1 else ['VCB']
    if '--all' in syms:
        syms = BACKTEST_WATCHLIST

    results = {}
    for sym in syms:
        r = run_backtest_symbol(sym.upper(), verbose=True)
        if r:
            results[sym.upper()] = r
            print(analyze_by_year_detailed(r))
