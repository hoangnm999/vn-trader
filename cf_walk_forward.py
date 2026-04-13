"""
cf_walk_forward.py — Context Filter Walk-Forward Validation
============================================================
VN Trader Bot V6 | Session 7

Mục đích:
  Validate 3 CF rules bằng Walk-Forward giống hệt run_walk_forward()
  của Score A — tránh in-sample bias khi đánh giá Context Filter.

Methodology:
  - Cùng window structure: IS=3 năm (756 rows), OOS=1 năm (252 rows)
  - Mỗi window: backtest Score A → lấy trades df → apply CF → so sánh
  - Kết quả: avg OOS dExp, avg OOS dWR, stability qua từng window
  - Verdict: V (robust) / ~ (trung bình) / ! (yếu/overfit)

3 CF Rules:
  CF1: vni_ma20_dist < +3.0%   (VNI không OVERBOUGHT)
  CF2: vni_atr_ratio < 1.3×med (NORMAL/LOW volatility)
  CF3: ma20_dist ∉ [2,5%)      (không OK zone, trừ score>=85)

Chạy:
  # WF cho 10 mã Score A watchlist
  python cf_walk_forward.py --group score_a

  # WF cho từng mã riêng
  python cf_walk_forward.py --symbols NKG,CTS,STB

  # WF + sensitivity test (thay đổi ngưỡng CF1/CF2/CF3)
  python cf_walk_forward.py --group score_a --sensitivity

  # WF + per-window detail
  python cf_walk_forward.py --symbols NKG --detail
"""

import sys, os, argparse
import numpy as np
import pandas as pd
from datetime import datetime

bot_dir = os.path.dirname(os.path.abspath(__file__))
if bot_dir not in sys.path:
    sys.path.insert(0, bot_dir)

import backtest as bt
from config import SYMBOL_CONFIG, SIGNALS_WATCHLIST

# ══════════════════════════════════════════════════════════════════════════════
# CF RULE ENGINE
# ══════════════════════════════════════════════════════════════════════════════

DEFAULT_CF = {
    'cf1_ob_max':       3.0,   # vni_ma20_dist < +3% (VNI không OB)
    'cf2_vol_mult':     1.30,  # vni_atr_ratio < 1.3×median (NORMAL/LOW vol)
    'cf3_ok_min':       2.0,   # ma20_dist không trong [2,5%) = OK zone
    'cf3_ok_max':       5.0,
    'cf3_exempt_score': 85,    # score >= 85 → CF3 không áp dụng
    'cf4_slope_min':    0.3,   # ma20_slope > 0.3 → EXTENDED là trend cont, không phải exhaustion
    'cf5_score_min':    85,    # score >= 85 → tier reliable
}

# ── Tất cả combinations cho ablation study ───────────────────────────────────
# Mỗi entry: (combo_id, rules_active, label mô tả)
ALL_RULE_NAMES = ['CF1','CF2','CF3','CF4','CF5']

ABLATION_COMBOS = [
    # Baseline
    ('BASELINE', [],                              'Baseline (không filter)'),
    # Single rules
    ('CF1',      ['CF1'],                         'CF1 only: VNI không OB'),
    ('CF2',      ['CF2'],                         'CF2 only: Vol regime NORMAL/LOW'),
    ('CF3',      ['CF3'],                         'CF3 only: MA20 không OK zone'),
    ('CF4',      ['CF4'],                         'CF4 only: EXTENDED+slope (không exhaustion)'),
    ('CF5',      ['CF5'],                         'CF5 only: Score >= 85'),
    # Pairs — test interaction giữa các cặp quan trọng
    ('CF1+CF2',  ['CF1','CF2'],                   'CF1+CF2: VNI OB + Vol regime'),
    ('CF1+CF3',  ['CF1','CF3'],                   'CF1+CF3: VNI OB + MA20 zone'),
    ('CF1+CF4',  ['CF1','CF4'],                   'CF1+CF4: VNI OB + EXTENDED slope'),
    ('CF1+CF5',  ['CF1','CF5'],                   'CF1+CF5: VNI OB + Score bucket'),
    ('CF2+CF3',  ['CF2','CF3'],                   'CF2+CF3: Vol regime + MA20 zone'),
    ('CF2+CF5',  ['CF2','CF5'],                   'CF2+CF5: Vol regime + Score bucket'),
    ('CF3+CF4',  ['CF3','CF4'],                   'CF3+CF4: MA20 zone + EXTENDED slope'),
    ('CF3+CF5',  ['CF3','CF5'],                   'CF3+CF5: MA20 zone + Score bucket'),
    ('CF4+CF5',  ['CF4','CF5'],                   'CF4+CF5: EXTENDED slope + Score bucket'),
    # Triples
    ('CF1+CF2+CF3', ['CF1','CF2','CF3'],          'CF1+CF2+CF3: Original 3-rule combo'),
    ('CF1+CF2+CF5', ['CF1','CF2','CF5'],          'CF1+CF2+CF5: OB+Vol+Score'),
    ('CF1+CF3+CF4', ['CF1','CF3','CF4'],          'CF1+CF3+CF4: OB+Zone+Slope'),
    ('CF1+CF3+CF5', ['CF1','CF3','CF5'],          'CF1+CF3+CF5: OB+Zone+Score'),
    ('CF1+CF4+CF5', ['CF1','CF4','CF5'],          'CF1+CF4+CF5: OB+Slope+Score'),
    ('CF2+CF3+CF5', ['CF2','CF3','CF5'],          'CF2+CF3+CF5: Vol+Zone+Score'),
    # Quads
    ('CF1+CF2+CF3+CF4', ['CF1','CF2','CF3','CF4'],'CF1-4: All except Score'),
    ('CF1+CF2+CF3+CF5', ['CF1','CF2','CF3','CF5'],'CF1-3+CF5: Original+Score'),
    ('CF1+CF2+CF4+CF5', ['CF1','CF2','CF4','CF5'],'CF1+CF2+CF4+CF5'),
    # Full
    ('ALL',      ['CF1','CF2','CF3','CF4','CF5'], 'ALL: Tất cả 5 rules'),
]


def apply_cf(df: pd.DataFrame,
             cfg: dict = None,
             median: float = None,
             active_rules: list = None) -> pd.DataFrame:
    """
    Áp CF rules lên trades DataFrame.

    active_rules: list các rule cần bật, ví dụ ['CF1','CF3'].
                  None hoặc [] = baseline (không filter gì).
                  ['CF1','CF2','CF3'] = original 3-rule combo.

    Trả về df với cột cf1..cf5 và cf_pass.
    """
    c      = cfg or DEFAULT_CF
    df     = df.copy()
    active = set(active_rules) if active_rules else set()

    # Tự tính median VNI ATR từ data (tránh hardcode)
    if median is None:
        vals   = df['vni_atr_ratio'].replace(0, np.nan).dropna()
        median = float(vals.median()) if len(vals) >= 5 else 0.80

    # ── Tính từng rule ────────────────────────────────────────────────────────
    # CF1: VNI không OVERBOUGHT
    df['cf1'] = df['vni_ma20_dist'] < c['cf1_ob_max']

    # CF2: VNI volatility NORMAL hoặc LOW
    df['cf2'] = df['vni_atr_ratio'] < median * c['cf2_vol_mult']

    # CF3: MA20 zone không phải OK(2-5%), trừ score >= exempt
    exempt    = df['score'] >= c['cf3_exempt_score']
    in_ok     = ((df['ma20_dist'] >= c['cf3_ok_min']) &
                 (df['ma20_dist'] <  c['cf3_ok_max']))
    df['cf3'] = exempt | (~in_ok)

    # CF4: Nếu EXTENDED (>5%), slope phải đang rising — tránh exhaustion
    # Nếu không EXTENDED → CF4 tự động pass (rule chỉ áp cho zone EXTENDED)
    is_extended   = df['ma20_dist'] >= c['cf3_ok_max']           # > 5%
    slope_ok      = df['ma20_slope'] > c['cf4_slope_min']         # slope đang tăng
    df['cf4']     = (~is_extended) | (is_extended & slope_ok)     # không ext OR ext+rising

    # CF5: Score bucket reliable (>= 85)
    df['cf5']     = df['score'] >= c['cf5_score_min']

    # ── cf_pass: AND của tất cả rules đang active ─────────────────────────────
    if not active:
        # Baseline: không filter gì → tất cả pass
        df['cf_pass'] = True
    else:
        mask = pd.Series([True] * len(df), index=df.index)
        if 'CF1' in active: mask = mask & df['cf1']
        if 'CF2' in active: mask = mask & df['cf2']
        if 'CF3' in active: mask = mask & df['cf3']
        if 'CF4' in active: mask = mask & df['cf4']
        if 'CF5' in active: mask = mask & df['cf5']
        df['cf_pass'] = mask

    return df

def stats_from_df(df: pd.DataFrame) -> dict:
    """Tính WR/Exp/PF từ trades df (đã filter action=='MUA')."""
    if len(df) == 0:
        return dict(n=0, wr=0.0, exp=0.0, pf=0.0, aw=0.0, al=0.0)
    wins   = df[df['pnl'] > 0]
    losses = df[df['pnl'] <= 0]
    wr     = len(wins) / len(df) * 100
    gp     = wins['pnl'].sum()       if len(wins)   > 0 else 0.0
    gl     = abs(losses['pnl'].sum()) if len(losses) > 0 else 0.0
    pf     = gp / gl if gl > 0 else 99.0
    return dict(
        n   = len(df),
        wr  = round(wr, 1),
        exp = round(df['pnl'].mean(), 2),
        pf  = round(min(pf, 99.0), 2),
        aw  = round(wins['pnl'].mean(),   2) if len(wins)   > 0 else 0.0,
        al  = round(losses['pnl'].mean(), 2) if len(losses) > 0 else 0.0,
    )

# ══════════════════════════════════════════════════════════════════════════════
# WALK-FORWARD CF
# ══════════════════════════════════════════════════════════════════════════════

def run_cf_walk_forward(symbol: str,
                        cf_cfg: dict = None,
                        verbose: bool = True,
                        detail: bool  = False) -> dict | None:
    """
    Walk-Forward Validation của Context Filter cho 1 symbol.

    Window structure (đồng nhất với run_walk_forward Score A):
      >= 1500 rows: IS=756d OOS=252d  → 4-5 windows
      >= 800  rows: IS=504d OOS=126d  → 2-3 windows
      >= 400  rows: IS=252d OOS=88d   → 1-2 windows

    Logic mỗi window:
      1. Backtest Score A trên TOÀN BỘ window (IS+OOS) → lấy trades df
         (không split lại — chỉ tách sau khi có trades)
      2. Filter trades theo date: IS trades vs OOS trades
      3. Tính median VNI ATR từ IS trades (tránh data leak từ OOS)
      4. Apply CF lên cả IS và OOS trades dùng cùng median
      5. So sánh: OOS before CF vs OOS after CF

    Tại sao không backtest IS/OOS riêng biệt:
      run_backtest_symbol cần ít nhất 200 candles warmup.
      Nếu split data rồi chạy riêng, OOS window 252 rows sẽ không đủ
      warmup cho indicators. Thay vào đó: chạy backtest trên toàn window,
      rồi split trades theo date — methodology đúng và nhất quán.
    """
    cfg = cf_cfg or DEFAULT_CF

    # Load data
    df_raw, source = bt.load_data(symbol, days=bt.LOOKBACK_DAYS)
    if df_raw is None:
        if verbose: print(f"  {symbol}: không load được data")
        return None

    total_rows = len(df_raw)

    # Window sizing (đồng nhất với Score A WF)
    if total_rows >= 1500:
        is_size, oos_size = 756, 252
    elif total_rows >= 800:
        is_size, oos_size = 504, 126
    elif total_rows >= 400:
        is_size, oos_size = 252, 88
    else:
        if verbose: print(f"  {symbol}: không đủ data ({total_rows} rows)")
        return None

    # SYMBOL_CONFIG params
    sym_cfg   = SYMBOL_CONFIG.get(symbol.upper(), {})
    _sl       = sym_cfg.get('sl',  0.07)
    _tp       = sym_cfg.get('tp',  0.14)
    _hd       = sym_cfg.get('hold_days', 15)
    _ms       = sym_cfg.get('min_score', 65)

    # Lấy date column
    _tc = next((c for c in df_raw.columns
                if c.lower() in ('time','date','datetime','trading_date')), None)
    if _tc:
        all_dates = pd.to_datetime(df_raw[_tc], errors='coerce').reset_index(drop=True)
    else:
        all_dates = pd.Series([pd.NaT] * len(df_raw))

    windows     = []
    start       = 0
    n_windows   = 0

    while start + is_size + oos_size <= total_rows:
        n_windows += 1
        is_end  = start + is_size
        oos_end = is_end + oos_size

        # Date boundaries cho window này
        is_start_date  = all_dates.iloc[start]   if pd.notna(all_dates.iloc[start])  else None
        is_end_date    = all_dates.iloc[is_end-1] if pd.notna(all_dates.iloc[is_end-1]) else None
        oos_start_date = all_dates.iloc[is_end]   if pd.notna(all_dates.iloc[is_end])  else None
        oos_end_date   = all_dates.iloc[oos_end-1]if pd.notna(all_dates.iloc[oos_end-1])else None

        oos_label = str(oos_start_date.year) if oos_start_date is not None else str(n_windows)

        # Backtest trên TOÀN window (IS+OOS) để có đủ warmup
        df_window = df_raw.iloc[start:oos_end].copy()
        result = bt.run_backtest_symbol(
            symbol, verbose=False,
            sl=_sl, tp=_tp, hold_days=_hd, min_score=_ms,
            use_regime=False,  # tắt để CF tự lọc
            use_vwap=False,
            _df_cache=df_window,
            entry_mode='T+1',
        )

        if result is None or result.get('trades') is None:
            start += oos_size
            continue

        df_trades = result['trades'].copy()
        if len(df_trades) < 5:
            start += oos_size
            continue

        # Kiểm tra có đủ rich fields
        required = ['vni_ma20_dist', 'vni_atr_ratio', 'ma20_dist', 'score', 'ma20_slope']
        missing  = [c for c in required if c not in df_trades.columns]
        if missing:
            if verbose: print(f"  {symbol} window {oos_label}: thiếu fields {missing}")
            start += oos_size
            continue

        # Convert date và split IS / OOS
        df_trades['_date_ts'] = pd.to_datetime(df_trades['date'], errors='coerce')
        df_mua = df_trades[df_trades['action'] == 'MUA'].copy()

        # IS trades: date < oos_start_date
        # OOS trades: date >= oos_start_date
        if oos_start_date is not None:
            df_is_trades  = df_mua[df_mua['_date_ts'] <  oos_start_date]
            df_oos_trades = df_mua[df_mua['_date_ts'] >= oos_start_date]
        else:
            # Fallback: split theo số lượng
            split_n = int(len(df_mua) * (is_size / (is_size + oos_size)))
            df_is_trades  = df_mua.iloc[:split_n]
            df_oos_trades = df_mua.iloc[split_n:]

        if len(df_oos_trades) < 3:
            start += oos_size
            continue

        # Tính median VNI ATR từ IS trades (không dùng OOS để tránh data leak)
        is_atrs = df_is_trades['vni_atr_ratio'].replace(0, np.nan).dropna()
        if len(is_atrs) >= 5:
            vni_atr_median = float(is_atrs.median())
        else:
            vni_atr_median = 0.80  # fallback

        # Apply CF trên IS và OOS dùng cùng median từ IS
        # active_rules=None ở đây vì run_cf_walk_forward dùng full combo
        # Ablation study dùng run_ablation_walk_forward riêng
        df_is_cf  = apply_cf(df_is_trades,  cfg, median=vni_atr_median,
                             active_rules=['CF1','CF2','CF3'])
        df_oos_cf = apply_cf(df_oos_trades, cfg, median=vni_atr_median,
                             active_rules=['CF1','CF2','CF3'])

        # Stats: IS before/after CF
        is_bef  = stats_from_df(df_is_trades)
        is_aft  = stats_from_df(df_is_cf[df_is_cf['cf_pass']])

        # Stats: OOS before/after CF  ← đây là số quan trọng nhất
        oos_bef = stats_from_df(df_oos_trades)
        oos_aft = stats_from_df(df_oos_cf[df_oos_cf['cf_pass']])

        # CF rule breakdown (trên OOS)
        n_fail_cf1 = int((~df_oos_cf['cf1']).sum())
        n_fail_cf2 = int((~df_oos_cf['cf2']).sum())
        n_fail_cf3 = int((~df_oos_cf['cf3']).sum())
        pass_rate  = round(df_oos_cf['cf_pass'].mean() * 100, 1)

        w = {
            'window':      n_windows,
            'oos_label':   oos_label,
            'is_date':     f"{is_start_date:%Y-%m}" if is_start_date else '?',
            'oos_date':    f"{oos_start_date:%Y-%m}" if oos_start_date else '?',
            'vni_atr_med': round(vni_atr_median, 3),

            # IS metrics
            'is_n_bef':   is_bef['n'],
            'is_n_aft':   is_aft['n'],
            'is_wr_bef':  is_bef['wr'],
            'is_wr_aft':  is_aft['wr'],
            'is_exp_bef': is_bef['exp'],
            'is_exp_aft': is_aft['exp'],

            # OOS metrics ← primary validation
            'oos_n_bef':   oos_bef['n'],
            'oos_n_aft':   oos_aft['n'],
            'oos_wr_bef':  oos_bef['wr'],
            'oos_wr_aft':  oos_aft['wr'],
            'oos_exp_bef': oos_bef['exp'],
            'oos_exp_aft': oos_aft['exp'],
            'oos_pf_bef':  oos_bef['pf'],
            'oos_pf_aft':  oos_aft['pf'],

            # Deltas
            'oos_dwr':     round(oos_aft['wr']  - oos_bef['wr'],  1),
            'oos_dexp':    round(oos_aft['exp'] - oos_bef['exp'], 2),

            # CF breakdown (OOS)
            'pass_rate':  pass_rate,
            'n_fail_cf1': n_fail_cf1,
            'n_fail_cf2': n_fail_cf2,
            'n_fail_cf3': n_fail_cf3,

            # IS dexp (để so sánh IS vs OOS delta)
            'is_dexp':    round(is_aft['exp'] - is_bef['exp'], 2),
        }
        windows.append(w)
        start += oos_size

    if not windows:
        if verbose: print(f"  {symbol}: không có window hợp lệ")
        return None

    # ── Aggregate metrics ────────────────────────────────────────────────────
    # Chỉ dùng windows có oos_n_aft >= 5 (đủ thống kê)
    valid = [w for w in windows if w['oos_n_aft'] >= 5]
    thin  = [w for w in windows if 0 < w['oos_n_aft'] < 5]

    if not valid:
        # Fallback: dùng tất cả windows có lệnh
        valid = [w for w in windows if w['oos_n_bef'] > 0]
    if not valid:
        return None

    avg_oos_dexp = float(np.mean([w['oos_dexp'] for w in valid]))
    avg_oos_dwr  = float(np.mean([w['oos_dwr']  for w in valid]))
    avg_is_dexp  = float(np.mean([w['is_dexp']  for w in valid]))

    avg_oos_exp_bef = float(np.mean([w['oos_exp_bef'] for w in valid]))
    avg_oos_exp_aft = float(np.mean([w['oos_exp_aft'] for w in valid]))
    avg_pass_rate   = float(np.mean([w['pass_rate']   for w in valid]))

    # IS vs OOS delta comparison
    # Nếu IS dexp >> OOS dexp → overfit
    # Nếu gần bằng nhau → genuine improvement
    is_oos_gap = avg_is_dexp - avg_oos_dexp

    # Stability: bao nhiêu windows OOS dexp > 0?
    n_positive_windows = sum(1 for w in valid if w['oos_dexp'] > 0)
    stability_pct      = round(n_positive_windows / len(valid) * 100)

    # ── Verdict (đồng nhất tiêu chí với run_walk_forward Score A) ────────────
    if (avg_oos_dexp >= 0.30
            and stability_pct >= 60
            and is_oos_gap < 0.40
            and avg_oos_exp_aft > avg_oos_exp_bef):
        verdict    = 'V'
        verdict_vi = 'Robust — CF co edge OOS that su'
        verdict_color = 'GREEN'
    elif (avg_oos_dexp >= 0.10
              and stability_pct >= 50
              and avg_oos_exp_aft >= avg_oos_exp_bef - 0.1):
        verdict    = '~'
        verdict_vi = 'Trung binh — CF co ich nhe, giu nguyen'
        verdict_color = 'YELLOW'
    elif avg_oos_dexp >= -0.10:
        verdict    = '-'
        verdict_vi = 'Trung tinh — CF khong giup them'
        verdict_color = 'YELLOW'
    else:
        verdict    = '!'
        verdict_vi = 'Yeu — CF lam xau OOS, can xem lai rules'
        verdict_color = 'RED'

    # Overfit check
    overfit_flag = is_oos_gap > 0.50

    if verbose:
        _print_symbol_result(symbol, windows, valid,
                             avg_oos_dexp, avg_oos_dwr,
                             avg_oos_exp_bef, avg_oos_exp_aft,
                             avg_pass_rate, stability_pct,
                             is_oos_gap, verdict, verdict_vi,
                             overfit_flag, detail)

    return {
        'symbol':          symbol,
        'n_windows':       len(windows),
        'n_valid':         len(valid),
        'n_thin':          len(thin),
        'windows':         windows,
        'avg_oos_dexp':    round(avg_oos_dexp, 2),
        'avg_oos_dwr':     round(avg_oos_dwr,  1),
        'avg_is_dexp':     round(avg_is_dexp,  2),
        'is_oos_gap':      round(is_oos_gap,   2),
        'avg_oos_exp_bef': round(avg_oos_exp_bef, 2),
        'avg_oos_exp_aft': round(avg_oos_exp_aft, 2),
        'avg_pass_rate':   round(avg_pass_rate, 1),
        'stability_pct':   stability_pct,
        'n_positive_windows': n_positive_windows,
        'verdict':         verdict,
        'verdict_vi':      verdict_vi,
        'overfit_flag':    overfit_flag,
        'cf_cfg':          cfg,
    }


def _print_symbol_result(symbol, windows, valid,
                          avg_oos_dexp, avg_oos_dwr,
                          avg_exp_bef, avg_exp_aft,
                          pass_rate, stability_pct,
                          is_oos_gap, verdict, verdict_vi,
                          overfit_flag, detail):
    """Print kết quả WF cho 1 symbol."""
    ov_flag = '  ⚠ OVERFIT SUSPECT (IS delta >> OOS delta)' if overfit_flag else ''

    print(f"\n  ── {symbol} ({len(windows)} windows, {len(valid)} valid) ──")
    print(f"  OOS Exp: {avg_exp_bef:+.2f}% → {avg_exp_aft:+.2f}%"
          f"  (dExp={avg_oos_dexp:+.2f}%  dWR={avg_oos_dwr:+.1f}%)")
    print(f"  Pass rate: {pass_rate:.0f}% | Stable windows: {stability_pct:.0f}%"
          f" | IS↔OOS gap: {is_oos_gap:+.2f}%{ov_flag}")
    print(f"  [{verdict}] {verdict_vi}")

    if detail and windows:
        print(f"\n  {'Window':<8} {'OOS Year':<10} {'n_bef':>6} {'n_aft':>6} "
              f"{'Exp_bef':>8} {'Exp_aft':>8} {'dExp':>7} "
              f"{'WR_bef':>7} {'WR_aft':>7} {'Pass%':>6} "
              f"{'CF1F':>5} {'CF2F':>5} {'CF3F':>5}")
        print(f"  {'-'*110}")
        for w in windows:
            suf = '*' if w['oos_n_aft'] >= 5 else (' ' if w['oos_n_bef'] > 0 else '✗')
            print(
                f"  {w['window']:<8} {w['oos_label']:<10}"
                f" {w['oos_n_bef']:>6} {w['oos_n_aft']:>6}"
                f" {w['oos_exp_bef']:>+8.2f}% {w['oos_exp_aft']:>+8.2f}%"
                f" {w['oos_dexp']:>+7.2f}%"
                f" {w['oos_wr_bef']:>6.1f}% {w['oos_wr_aft']:>6.1f}%"
                f" {w['pass_rate']:>5.0f}%"
                f" {w['n_fail_cf1']:>5} {w['n_fail_cf2']:>5} {w['n_fail_cf3']:>5}"
                f" {suf}"
            )
        print(f"  (* = n_aft>=5 đủ thống kê | ✗ = không có lệnh OOS)")


# ══════════════════════════════════════════════════════════════════════════════
# ABLATION STUDY — Per-Rule và Per-Combo Walk-Forward
# ══════════════════════════════════════════════════════════════════════════════

def _cache_symbol_windows(symbol: str,
                           df_raw: pd.DataFrame,
                           all_dates: pd.Series,
                           is_size: int, oos_size: int,
                           sym_cfg: dict) -> list | None:
    """
    Chạy backtest 1 lần duy nhất cho 1 symbol, cache toàn bộ windows.
    Trả về list of window dicts chứa sẵn df_is_trades, df_oos_trades, median.

    KEY OPTIMIZATION: Thay vì chạy backtest 26 lần (1 per combo),
    chỉ chạy 1 lần và cache trades df.
    26 combos sau đó chỉ cần apply pandas filter ~ms mỗi cái.

    Reduction: 26 backtests → 1 backtest per symbol
    Time: 26 × 4 phút → 4 phút per symbol
    """
    total_rows = len(df_raw)
    _sl = sym_cfg.get('sl',  0.07)
    _tp = sym_cfg.get('tp',  0.14)
    _hd = sym_cfg.get('hold_days', 15)
    _ms = sym_cfg.get('min_score', 65)

    cached_windows = []
    start = 0

    while start + is_size + oos_size <= total_rows:
        is_end  = start + is_size
        oos_end = is_end + oos_size

        oos_start_date = (all_dates.iloc[is_end]
                          if pd.notna(all_dates.iloc[is_end]) else None)
        oos_label = str(oos_start_date.year) if oos_start_date is not None else str(len(cached_windows)+1)

        # ── BACKTEST 1 LẦN DUY NHẤT cho window này ────────────────────────
        df_window = df_raw.iloc[start:oos_end].copy()
        result = bt.run_backtest_symbol(
            symbol, verbose=False,
            sl=_sl, tp=_tp, hold_days=_hd, min_score=_ms,
            use_regime=False, use_vwap=False,
            _df_cache=df_window, entry_mode='T+1',
        )

        if result is None or result.get('trades') is None:
            start += oos_size
            continue

        df_trades = result['trades'].copy()
        required  = ['vni_ma20_dist','vni_atr_ratio','ma20_dist','score','ma20_slope']
        if any(c not in df_trades.columns for c in required):
            start += oos_size
            continue

        df_trades['_date_ts'] = pd.to_datetime(df_trades['date'], errors='coerce')
        df_mua = df_trades[df_trades['action'] == 'MUA'].copy()

        # Split IS / OOS
        if oos_start_date is not None:
            df_is_t  = df_mua[df_mua['_date_ts'] <  oos_start_date]
            df_oos_t = df_mua[df_mua['_date_ts'] >= oos_start_date]
        else:
            sn = int(len(df_mua) * (is_size / (is_size + oos_size)))
            df_is_t  = df_mua.iloc[:sn]
            df_oos_t = df_mua.iloc[sn:]

        if len(df_oos_t) < 3:
            start += oos_size
            continue

        # Median VNI ATR từ IS (tránh data leak)
        is_atrs = df_is_t['vni_atr_ratio'].replace(0, np.nan).dropna()
        med = float(is_atrs.median()) if len(is_atrs) >= 5 else 0.80

        # Pre-compute ALL CF rule columns 1 lần
        # (apply_cf với active_rules=None tính cf1..cf5 nhưng cf_pass=True)
        df_oos_precalc = apply_cf(df_oos_t, DEFAULT_CF,
                                   median=med, active_rules=None)

        cached_windows.append({
            'oos_label':     oos_label,
            'df_oos':        df_oos_precalc,   # đã có sẵn cf1..cf5
            'median':        med,
            'oos_n_total':   len(df_oos_t),
        })
        start += oos_size

    return cached_windows if cached_windows else None


def _apply_combo_on_cache(cached_windows: list,
                           active_rules: list) -> dict | None:
    """
    Apply 1 combo rules lên cached windows (đã có cf1..cf5).
    Chỉ cần tính cf_pass mới — không cần backtest lại.
    Tốc độ: ~1ms per window thay vì 4 phút.
    """
    window_results = []

    for w in cached_windows:
        df = w['df_oos'].copy()

        # Tính cf_pass cho combo này (reuse cf1..cf5 đã tính sẵn)
        if not active_rules:
            df['cf_pass'] = True
        else:
            mask = pd.Series([True] * len(df), index=df.index)
            if 'CF1' in active_rules: mask = mask & df['cf1']
            if 'CF2' in active_rules: mask = mask & df['cf2']
            if 'CF3' in active_rules: mask = mask & df['cf3']
            if 'CF4' in active_rules: mask = mask & df['cf4']
            if 'CF5' in active_rules: mask = mask & df['cf5']
            df['cf_pass'] = mask

        bef = stats_from_df(df)
        aft = stats_from_df(df[df['cf_pass']])

        if aft['n'] < 5:   # minimum n_aft
            continue

        window_results.append({
            'oos_n_bef':  bef['n'],
            'oos_n_aft':  aft['n'],
            'oos_exp_bef':bef['exp'],
            'oos_exp_aft':aft['exp'],
            'oos_dexp':   round(aft['exp'] - bef['exp'], 2),
            'pass_rate':  round(df['cf_pass'].mean() * 100, 1),
        })

    if not window_results:
        return None

    valid = [w for w in window_results if w['oos_n_aft'] >= 5]
    if not valid:
        valid = window_results

    avg_dexp      = float(np.mean([w['oos_dexp']    for w in valid]))
    avg_exp_bef   = float(np.mean([w['oos_exp_bef'] for w in valid]))
    avg_exp_aft   = float(np.mean([w['oos_exp_aft'] for w in valid]))
    avg_pass_rate = float(np.mean([w['pass_rate']   for w in valid]))
    n_pos         = sum(1 for w in valid if w['oos_dexp'] > 0)

    return {
        'avg_dexp':      round(avg_dexp, 2),
        'avg_exp_bef':   round(avg_exp_bef, 2),
        'avg_exp_aft':   round(avg_exp_aft, 2),
        'avg_pass_rate': round(avg_pass_rate, 1),
        'stability_pct': round(n_pos / len(valid) * 100),
        'n_valid':       len(valid),
    }


def run_ablation_study(symbols: list,
                       cfg: dict = None,
                       verbose: bool = True) -> dict:
    """
    Ablation Study: WF từng rule riêng lẻ + tất cả combinations.

    OPTIMIZATION: Backtest mỗi symbol CHỈ 1 LẦN,
    cache trades df, sau đó apply 26 combos bằng pandas filter.

    Thời gian thực tế:
      Trước: 26 combos × 10 mã × ~4 phút = ~17 giờ (không khả thi)
      Sau:   10 mã × ~4 phút + 260 pandas ops = ~40-50 phút ✅
    """
    cfg       = cfg or DEFAULT_CF
    n_symbols = len(symbols)
    n_combos  = len(ABLATION_COMBOS)

    print(f"\n{'╔'+'═'*68+'╗'}")
    print(f"║  ABLATION STUDY — Per-Rule Walk-Forward{' '*29}║")
    print(f"║  {n_symbols} mã | {n_combos} combos | {datetime.now():%d/%m/%Y %H:%M}{' '*20}║")
    print(f"║  Optimized: backtest 1 lần/mã, {n_combos} combos dùng cache{' '*20}║")
    print(f"{'╚'+'═'*68+'╝'}")

    # ── PHASE 1: Load data + Backtest 1 lần/symbol ───────────────────────────
    print(f"\n  PHASE 1: Caching backtest results ({n_symbols} mã)...")
    symbol_cache = {}   # sym → {cached_windows, is_size, oos_size, sym_cfg}

    for sym in symbols:
        df_raw, _ = bt.load_data(sym, days=bt.LOOKBACK_DAYS)
        if df_raw is None:
            print(f"  {sym}: skip (no data)")
            continue

        total_rows = len(df_raw)
        if total_rows >= 1500:   is_size, oos_size = 756, 252
        elif total_rows >= 800:  is_size, oos_size = 504, 126
        elif total_rows >= 400:  is_size, oos_size = 252, 88
        else:                    continue

        _tc = next((c for c in df_raw.columns
                    if c.lower() in ('time','date','datetime','trading_date')), None)
        all_dates = (pd.to_datetime(df_raw[_tc], errors='coerce').reset_index(drop=True)
                     if _tc else pd.Series([pd.NaT]*len(df_raw)))

        sym_cfg = SYMBOL_CONFIG.get(sym.upper(), {})

        print(f"  {sym}: backtest {total_rows} rows...", end='', flush=True)
        windows = _cache_symbol_windows(
            sym, df_raw, all_dates, is_size, oos_size, sym_cfg
        )
        if windows:
            symbol_cache[sym] = windows
            n_trades_total = sum(w['oos_n_total'] for w in windows)
            print(f" OK ({len(windows)} windows, {n_trades_total} OOS trades)")
        else:
            print(f" skip (no valid windows)")

    if not symbol_cache:
        print("  Không có data.")
        return {}

    # ── PHASE 2: Apply 26 combos lên cached data ─────────────────────────────
    print(f"\n  PHASE 2: Applying {n_combos} combos × {len(symbol_cache)} mã...")
    combo_results = {}

    for idx, (combo_id, active_rules, label) in enumerate(ABLATION_COMBOS):
        combo_results[combo_id] = {}
        sym_dexps = []

        for sym, windows in symbol_cache.items():
            r = _apply_combo_on_cache(windows, active_rules)
            if r:
                combo_results[combo_id][sym] = r
                sym_dexps.append(r['avg_dexp'])

        avg = float(np.mean(sym_dexps)) if sym_dexps else 0.0
        if verbose:
            bar = '█' * max(0, int((avg + 0.5) * 10))
            print(f"  [{idx+1:>2}/{n_combos}] {combo_id:<22} "
                  f"dExp={avg:+.3f}%  n={len(sym_dexps)}  {bar}")

    # ── Aggregate (phần này không thay đổi) ──────────────────────────────────
    combo_summary = []
    for combo_id, active_rules, label in ABLATION_COMBOS:
        sym_results = combo_results.get(combo_id, {})
        if not sym_results:
            continue

        dexps      = [r['avg_dexp']      for r in sym_results.values()]
        pass_rates = [r['avg_pass_rate'] for r in sym_results.values()]
        stabs      = [r['stability_pct'] for r in sym_results.values()]

        avg_dexp     = float(np.mean(dexps))
        avg_pass     = float(np.mean(pass_rates))
        avg_stab     = float(np.mean(stabs))
        std_dexp     = float(np.std(dexps)) if len(dexps) > 1 else 0.0
        n_pos_sym    = sum(1 for d in dexps if d > 0)
        n_valid      = len(dexps)
        threshold    = avg_dexp - std_dexp
        n_consistent = sum(1 for d in dexps if d >= threshold and d > 0)

        combo_summary.append({
            'combo_id':     combo_id,
            'rules':        active_rules,
            'label':        label,
            'n_rules':      len(active_rules),
            'avg_dexp':     round(avg_dexp, 3),
            'std_dexp':     round(std_dexp, 3),
            'avg_pass':     round(avg_pass, 1),
            'avg_stab':     round(avg_stab, 1),
            'n_pos_sym':    n_pos_sym,
            'n_consistent': n_consistent,
            'n_valid':      n_valid,
            'pct_pos_sym':  round(n_pos_sym/n_valid*100) if n_valid else 0,
        })

    combo_summary.sort(key=lambda x: x['avg_dexp'], reverse=True)

    # Baseline
    baseline      = next((c for c in combo_summary if c['combo_id'] == 'BASELINE'), None)
    baseline_dexp = baseline['avg_dexp'] if baseline else 0.0

    # Marginal contribution — paired comparison
    combo_by_id = {c['combo_id']: c for c in combo_summary}
    marginal = {}
    for rule in ALL_RULE_NAMES:
        paired_diffs = []
        for combo_id, active_rules, _ in ABLATION_COMBOS:
            if rule not in active_rules:
                continue
            without    = [r for r in active_rules if r != rule]
            without_id = '+'.join(without) if without else 'BASELINE'
            if without_id in combo_by_id and combo_id in combo_by_id:
                diff = (combo_by_id[combo_id]['avg_dexp']
                        - combo_by_id[without_id]['avg_dexp'])
                paired_diffs.append(diff)
        marginal[rule] = round(float(np.mean(paired_diffs))
                               if paired_diffs else 0.0, 3)

    # Optimal combo với parsimony + pass rate floor
    def parsimony_score(c):
        return c['avg_dexp'] - c['n_rules'] * 0.03

    candidates = [c for c in combo_summary
                  if c['n_rules'] <= 3
                  and c['pct_pos_sym'] >= 60
                  and c['avg_pass'] >= 30.0
                  and c['n_valid'] >= len(symbol_cache) * 0.7]

    optimal = max(candidates, key=parsimony_score) if candidates else combo_summary[0]

    # Suggested weights — rules âm → weight 0
    pos_marginals = {k: v for k, v in marginal.items() if v > 0}
    neg_rules     = {k for k, v in marginal.items() if v <= 0}
    suggested_weights = {}
    if pos_marginals:
        total_m = sum(pos_marginals.values())
        for rule, v in pos_marginals.items():
            raw_w = v / total_m * 12
            suggested_weights[rule] = (4 if raw_w >= 3.5 else
                                       3 if raw_w >= 2.5 else
                                       2 if raw_w >= 1.5 else 1)
    for rule in neg_rules:
        suggested_weights[rule] = 0

    _print_ablation_results(combo_summary, marginal, optimal,
                             suggested_weights, baseline_dexp)

    print(f"\n  Thời gian phase 1 (backtest): ~{len(symbol_cache)*4} phút")
    print(f"  Thời gian phase 2 (combos):   ~{len(ABLATION_COMBOS)*len(symbol_cache)//60 + 1} phút")

    return {
        'combo_summary':      combo_summary,
        'combo_results':      combo_results,
        'marginal':           marginal,
        'optimal_combo':      optimal,
        'suggested_weights':  suggested_weights,
        'baseline_dexp':      baseline_dexp,
    }


def _print_ablation_results(combo_summary, marginal, optimal,
                              suggested_weights, baseline_dexp):
    """Print ablation results đẹp ra console."""

    print(f"\n{'═'*85}")
    print(f"  ABLATION RESULTS — Ranking tất cả combos theo OOS dExp")
    print(f"  (Paired marginal contribution | Pass rate floor ≥30% | Parsimony penalty -0.03%/rule)")
    print(f"{'═'*85}")
    print(f"  {'Rank':<5} {'Combo':<22} {'#R':>3} {'dExp':>8} {'±Std':>6} "
          f"{'Pass%':>7} {'Stable%':>8} {'Sym+':>5} {'Consist':>8} {'Verdict'}")
    print(f"  {'-'*85}")

    for rank, c in enumerate(combo_summary, 1):
        is_opt  = ' ← OPTIMAL' if c['combo_id'] == optimal['combo_id'] else ''
        is_base = ' (baseline)' if c['combo_id'] == 'BASELINE' else ''
        vd      = ('✅ Positive' if c['avg_dexp'] > 0.15
                   else ('🟡 Marginal' if c['avg_dexp'] > 0
                         else '❌ Negative'))
        consist = c.get('n_consistent', 0)
        std     = c.get('std_dexp', 0)
        print(f"  {rank:<5} {c['combo_id']:<22} {c['n_rules']:>3} "
              f"{c['avg_dexp']:>+7.3f}% {std:>5.3f}  {c['avg_pass']:>6.1f}% "
              f"{c['avg_stab']:>7.1f}%  {c['n_pos_sym']:>2}/{c['n_valid']}"
              f"  {consist:>2}/{c['n_valid']}"
              f"  {vd}{is_opt}{is_base}")

    # Marginal contribution
    print(f"\n{'═'*75}")
    print(f"  MARGINAL CONTRIBUTION — Mỗi rule đóng góp bao nhiêu?")
    print(f"{'═'*75}")
    print(f"  (Marginal = avg dExp của combo CÓ rule - avg dExp của combo KHÔNG CÓ rule)")
    print()
    sorted_rules = sorted(marginal.items(), key=lambda x: x[1], reverse=True)
    for rule, m in sorted_rules:
        w    = suggested_weights.get(rule, 0)
        if w == 0:
            keep = '❌ BỎ  (marginal âm hoặc = 0 → không đưa vào scorecard)'
        elif w >= 3:
            keep = '✅ QUAN TRỌNG'
        elif w >= 2:
            keep = '✅ CÓ ÍCH'
        else:
            keep = '🟡 YẾU — cân nhắc bỏ'
        bar  = '█' * max(0, int(m * 20 + 10))
        print(f"  {rule:<6} marginal={m:+.3f}%  weight={w}pt  {keep}  {bar}")

    # Optimal combo
    print(f"\n{'═'*75}")
    print(f"  RECOMMENDED OPTIMAL COMBO")
    print(f"{'═'*75}")
    print(f"  Combo:  {optimal['combo_id']}")
    print(f"  Rules:  {optimal['rules'] or ['(none — baseline)']}")
    print(f"  Label:  {optimal['label']}")
    print(f"  dExp:   {optimal['avg_dexp']:+.3f}%")
    print(f"  Pass%:  {optimal['avg_pass']:.1f}%  |  Stable: {optimal['avg_stab']:.0f}%")
    print()

    # Suggested weights
    print(f"  SUGGESTED SCORECARD WEIGHTS (data-driven từ marginal contribution):")
    total_w = sum(suggested_weights.values())
    for rule, w in sorted(suggested_weights.items(),
                           key=lambda x: x[1], reverse=True):
        pct = round(w/total_w*100)
        print(f"    {rule}: {w}pt / {total_w}pt  ({pct}%)")

    print(f"\n  NOTE: Weights chỉ valid sau khi có đủ n OOS trades.")
    print(f"  Re-run ablation sau mỗi 6 tháng để check stability.")


# ══════════════════════════════════════════════════════════════════════════════
# SENSITIVITY TEST — thay đổi ngưỡng CF rules
# ══════════════════════════════════════════════════════════════════════════════

SENSITIVITY_GRID = {
    'cf1_ob_max':    [2.0, 2.5, 3.0, 3.5, 4.0],  # default 3.0
    'cf2_vol_mult':  [1.15, 1.20, 1.30, 1.40],    # default 1.30
    'cf3_ok_min':    [1.5, 2.0, 2.5],              # default 2.0
    'cf3_ok_max':    [4.0, 5.0, 6.0],              # default 5.0
}

def run_sensitivity_test(symbols: list[str], verbose: bool = True) -> dict:
    """
    Test CF với nhiều bộ params khác nhau.
    Mục tiêu: xác nhận default params là robust, không phải cherry-picked.

    Chỉ thay đổi 1 param tại một thời điểm (one-at-a-time sensitivity).
    """
    print(f"\n{'═'*60}")
    print(f"  SENSITIVITY TEST — Thay đổi ngưỡng CF rules")
    print(f"  ({len(symbols)} mã, one-at-a-time)")
    print('═'*60)

    # Baseline: default CF
    print(f"\n  Đang tính baseline (default CF)...")
    baseline_results = {}
    for sym in symbols:
        r = run_cf_walk_forward(sym, cf_cfg=DEFAULT_CF, verbose=False)
        if r:
            baseline_results[sym] = r['avg_oos_dexp']

    if not baseline_results:
        print("  Không có kết quả baseline")
        return {}

    baseline_avg = float(np.mean(list(baseline_results.values())))
    print(f"  Baseline avg dExp OOS = {baseline_avg:+.2f}%")

    sensitivity_summary = {}

    # Test từng param
    for param_name, values in SENSITIVITY_GRID.items():
        print(f"\n  Testing {param_name}: {values}")
        param_results = {}
        for val in values:
            cf_test = dict(DEFAULT_CF)
            cf_test[param_name] = val

            dexps = []
            for sym in symbols:
                r = run_cf_walk_forward(sym, cf_cfg=cf_test, verbose=False)
                if r:
                    dexps.append(r['avg_oos_dexp'])

            avg_dexp = float(np.mean(dexps)) if dexps else 0.0
            delta_vs_baseline = avg_dexp - baseline_avg
            marker = ' ← default' if val == DEFAULT_CF[param_name] else ''
            print(f"    {param_name}={val:<6}: avg dExp={avg_dexp:+.2f}%"
                  f"  (vs baseline: {delta_vs_baseline:+.2f}%){marker}")
            param_results[val] = avg_dexp

        sensitivity_summary[param_name] = param_results

        # Stability check: variance của results
        all_vals = list(param_results.values())
        variance = float(np.std(all_vals))
        if variance < 0.10:
            print(f"    ✅ Robust: variance={variance:.3f} (ngưỡng ít ảnh hưởng)")
        elif variance < 0.20:
            print(f"    🟡 Chấp nhận: variance={variance:.3f}")
        else:
            print(f"    ⚠ Sensitive: variance={variance:.3f} — ngưỡng này quan trọng")

    # Overall verdict
    all_variances = []
    for param_name, vals in sensitivity_summary.items():
        all_variances.append(float(np.std(list(vals.values()))))

    avg_var = float(np.mean(all_variances))
    print(f"\n  Sensitivity summary: avg variance = {avg_var:.3f}")
    if avg_var < 0.12:
        print(f"  ✅ CF rules ROBUST — kết quả ít thay đổi khi điều chỉnh ngưỡng")
    elif avg_var < 0.20:
        print(f"  🟡 CF rules khá robust — một vài ngưỡng sensitive nhẹ")
    else:
        print(f"  ⚠ CF rules SENSITIVE — kết quả phụ thuộc nhiều vào ngưỡng → overfit risk")

    return sensitivity_summary


# ══════════════════════════════════════════════════════════════════════════════
# GROUP WALK-FORWARD
# ══════════════════════════════════════════════════════════════════════════════

def run_group_wf(symbols: list[str],
                 group_name: str = 'GROUP',
                 detail: bool    = False,
                 verbose: bool   = True) -> dict:
    """Chạy CF WF cho toàn bộ 1 nhóm symbols."""
    print(f"\n{'╔'+'═'*58+'╗'}")
    print(f"║  CF WALK-FORWARD: {group_name:<40}║")
    print(f"║  {len(symbols)} mã | {datetime.now():%d/%m/%Y %H:%M}{' '*28}║")
    print(f"{'╚'+'═'*58+'╝'}")

    all_results = []
    for sym in symbols:
        r = run_cf_walk_forward(sym, verbose=verbose, detail=detail)
        if r:
            all_results.append(r)

    if not all_results:
        print(f"\n  Không có kết quả cho {group_name}")
        return {}

    # ── Group summary ─────────────────────────────────────────────────────────
    valid = [r for r in all_results if r['n_valid'] > 0]

    avg_dexp       = float(np.mean([r['avg_oos_dexp'] for r in valid]))
    avg_dwr        = float(np.mean([r['avg_oos_dwr']  for r in valid]))
    avg_pass_rate  = float(np.mean([r['avg_pass_rate']for r in valid]))
    avg_is_oos_gap = float(np.mean([r['is_oos_gap']   for r in valid]))

    n_V  = sum(1 for r in valid if r['verdict'] == 'V')
    n_ok = sum(1 for r in valid if r['verdict'] == '~')
    n_nt = sum(1 for r in valid if r['verdict'] == '-')
    n_bad= sum(1 for r in valid if r['verdict'] == '!')
    n_ov = sum(1 for r in valid if r['overfit_flag'])

    # Group verdict
    if avg_dexp >= 0.30 and n_V + n_ok >= len(valid) * 0.70:
        grp_verdict    = 'V'
        grp_verdict_vi = 'CF RULES ROBUST TREN NHOM NAY'
    elif avg_dexp >= 0.10 and n_bad <= len(valid) * 0.20:
        grp_verdict    = '~'
        grp_verdict_vi = 'CF RULES CO ICH NHE'
    elif avg_dexp >= -0.05:
        grp_verdict    = '-'
        grp_verdict_vi = 'CF RULES TRUNG TINH'
    else:
        grp_verdict    = '!'
        grp_verdict_vi = 'CF RULES KHONG HIEU QUA OOS'

    print(f"\n{'═'*70}")
    print(f"  TỔNG KẾT: {group_name}")
    print(f"{'═'*70}")
    print(f"  Mã có kết quả: {len(valid)}/{len(symbols)}")
    print(f"  Avg OOS dExp:  {avg_dexp:+.2f}%  |  Avg OOS dWR: {avg_dwr:+.1f}%")
    print(f"  Avg pass rate: {avg_pass_rate:.0f}%  |  IS↔OOS gap: {avg_is_oos_gap:+.2f}%")
    print(f"  Verdict: V={n_V} ~={n_ok} -={n_nt} !={n_bad}  |  Overfit suspect: {n_ov}")
    print(f"\n  [{grp_verdict}] {grp_verdict_vi}")

    if n_ov > 0:
        ov_syms = [r['symbol'] for r in valid if r['overfit_flag']]
        print(f"  ⚠ Overfit suspect: {', '.join(ov_syms)}")

    # ── Per-symbol summary table ──────────────────────────────────────────────
    print(f"\n  {'Mã':<6} {'Win':>4} {'Val':>4} "
          f"{'dExp OOS':>9} {'dWR OOS':>8} {'Exp_bef':>8} {'Exp_aft':>8} "
          f"{'Pass%':>6} {'Stable%':>8} {'Ver':>4} {'Overfit':>8}")
    print(f"  {'-'*90}")
    for r in sorted(all_results, key=lambda x: x['avg_oos_dexp'], reverse=True):
        ov = '⚠' if r['overfit_flag'] else ''
        print(
            f"  {r['symbol']:<6} {r['n_windows']:>4} {r['n_valid']:>4}"
            f" {r['avg_oos_dexp']:>+8.2f}% {r['avg_oos_dwr']:>+7.1f}%"
            f" {r['avg_oos_exp_bef']:>+7.2f}% {r['avg_oos_exp_aft']:>+7.2f}%"
            f" {r['avg_pass_rate']:>5.0f}%"
            f" {r['stability_pct']:>7.0f}%"
            f"  {r['verdict']:>2}"
            f"  {ov}"
        )

    # ── Interpretation guide ──────────────────────────────────────────────────
    print(f"\n  DIỄN GIẢI:")
    print(f"  dExp OOS  >= +0.30% trên >= 70% mã → CF có edge thật")
    print(f"  IS↔OOS gap < 0.40%               → không overfit")
    print(f"  Stable windows >= 60%              → nhất quán qua thời gian")
    print(f"  Pass rate ~45-55%                  → filter không quá chặt/lỏng")

    return {
        'group':           group_name,
        'results':         all_results,
        'avg_oos_dexp':    round(avg_dexp, 2),
        'avg_oos_dwr':     round(avg_dwr,  1),
        'avg_pass_rate':   round(avg_pass_rate, 1),
        'avg_is_oos_gap':  round(avg_is_oos_gap, 2),
        'n_V': n_V, 'n_ok': n_ok, 'n_nt': n_nt, 'n_bad': n_bad,
        'n_overfit':       n_ov,
        'verdict':         grp_verdict,
        'verdict_vi':      grp_verdict_vi,
    }


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

SCORE_A_WATCHLIST = [
    'MCH','DGC','SSI','HCM','NKG','FRT','HAH','PC1','STB','CTS'
]

def main():
    parser = argparse.ArgumentParser(
        description='CF Walk-Forward Validation — VN Trader Bot V6'
    )
    parser.add_argument('--group', choices=['score_a','all'],
                        help='Nhóm symbols định sẵn')
    parser.add_argument('--symbols', type=str, default='',
                        help='Danh sách mã, ngăn cách bằng phẩy')
    parser.add_argument('--detail',       action='store_true',
                        help='In chi tiết từng window')
    parser.add_argument('--sensitivity',  action='store_true',
                        help='Chạy sensitivity test ngưỡng CF')
    parser.add_argument('--ablation',     action='store_true',
                        help='Chạy ablation study: WF từng rule riêng lẻ + combinations')
    parser.add_argument('--ablation-only',action='store_true',
                        help='Chỉ chạy ablation, bỏ qua full WF')
    parser.add_argument('--quiet',        action='store_true',
                        help='Chỉ in summary, không in per-symbol')
    args = parser.parse_args()

    verbose = not args.quiet

    # Xác định symbols
    if args.symbols:
        symbols    = [s.strip().upper() for s in args.symbols.split(',') if s.strip()]
        group_name = 'CUSTOM'
    elif args.group == 'score_a':
        symbols    = SCORE_A_WATCHLIST
        group_name = 'SCORE_A_WATCHLIST'
    elif args.group == 'all':
        symbols    = list(set(SCORE_A_WATCHLIST + list(SIGNALS_WATCHLIST)))
        group_name = 'ALL_WATCHLIST'
    else:
        symbols    = SCORE_A_WATCHLIST
        group_name = 'SCORE_A_WATCHLIST'

    # Full WF (trừ khi --ablation-only)
    group_result = None
    if not args.ablation_only:
        group_result = run_group_wf(
            symbols,
            group_name = group_name,
            detail     = args.detail,
            verbose    = verbose,
        )

    # Sensitivity test
    if args.sensitivity and group_result:
        run_sensitivity_test(symbols, verbose=True)

    # Ablation study — chạy sau full WF để có context
    if args.ablation or args.ablation_only:
        ablation_result = run_ablation_study(symbols, verbose=True)

        # Nếu có cả full WF và ablation → in kết luận tổng hợp
        if group_result and ablation_result:
            _print_combined_conclusion(group_result, ablation_result)

    # Action summary
    if group_result and not args.ablation_only:
        v = group_result.get('verdict', '?')
        print(f"\n{'═'*70}")
        print(f"  ACTION DỰA TRÊN KẾT QUẢ:")
        print(f"{'═'*70}")
        if v == 'V':
            print(f"  ✅ CF rules pass WF. Scorecard justified.")
            if args.ablation:
                print(f"  → Cập nhật weights scorecard theo suggested_weights từ ablation.")
            print(f"  → Tiếp tục: CF test ML/sascreen group.")
        elif v == '~':
            print(f"  🟡 CF rules có ích nhẹ. Dùng ablation để tìm optimal combo.")
            if not args.ablation:
                print(f"  → Chạy lại với --ablation để tìm combination tốt nhất.")
        elif v == '-':
            print(f"  🟡 CF trung tính. Chỉ giữ rule có marginal contribution dương.")
            print(f"  → Bắt buộc chạy --ablation để identify rules đáng giữ.")
        else:
            print(f"  ❌ CF yếu. Chạy --ablation để hiểu rule nào gây ra vấn đề.")
        print()


def _print_combined_conclusion(group_result, ablation_result):
    """In kết luận tổng hợp từ cả Full WF và Ablation."""
    print(f"\n{'═'*70}")
    print(f"  KẾT LUẬN TỔNG HỢP: Full WF + Ablation Study")
    print(f"{'═'*70}")

    v       = group_result.get('verdict', '?')
    opt     = ablation_result.get('optimal_combo', {})
    weights = ablation_result.get('suggested_weights', {})
    margin  = ablation_result.get('marginal', {})

    print(f"  Full WF verdict:     [{v}] {group_result.get('verdict_vi','')}")
    print(f"  Optimal combo:       {opt.get('combo_id','')} "
          f"(dExp={opt.get('avg_dexp',0):+.3f}%)")
    print()
    print(f"  Rules xếp hạng theo marginal contribution:")
    for rule, m in sorted(margin.items(), key=lambda x: x[1], reverse=True):
        w    = weights.get(rule, 1)
        keep = '✅ GIỮ' if m > 0.05 else ('🟡 OPTIONAL' if m > -0.05 else '❌ BỎ')
        print(f"    {rule}: marginal={m:+.3f}%  weight={w}pt  → {keep}")

    print()
    print(f"  ACTION:")
    # Rules có marginal > 0 → giữ; < 0 → bỏ; ≈ 0 → optional
    keep_rules = [r for r,m in margin.items() if m > 0.05]
    drop_rules = [r for r,m in margin.items() if m < -0.05]
    opt_rules  = [r for r,m in margin.items() if -0.05 <= m <= 0.05]

    if keep_rules:
        print(f"  ✅ Giữ:     {', '.join(keep_rules)}")
    if opt_rules:
        print(f"  🟡 Optional: {', '.join(opt_rules)} (test thêm khi có live data)")
    if drop_rules:
        print(f"  ❌ Xem xét bỏ: {', '.join(drop_rules)} (làm giảm OOS Exp)")
    print(f"  → Rebuild context_scorecard.py với weights: {weights}")


if __name__ == '__main__':
    main()
