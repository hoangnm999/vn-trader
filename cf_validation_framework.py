"""
cf_validation_framework.py
===========================
Framework kiểm chứng Context Filter (CF) trên 2 nhóm ngoài Score A watchlist:
  - Nhóm 1: ML_CONFIRMED_WATCHLIST (12 mã, PF 1.7-2.2, edge đã biết)
  - Nhóm 2: Sascreen candidates (mã chưa trong watchlist, PF chưa biết)

Mục tiêu: Xác định CF là "universal timing filter" hay chỉ hoạt động
trên mã đã có edge mạnh sẵn.

Cách chạy:
  python cf_validation_framework.py --group ml
  python cf_validation_framework.py --group sascreen --symbols VCB,HPG,VHM,...
  python cf_validation_framework.py --group both
"""

import sys, os, argparse, json
import numpy as np
import pandas as pd
from datetime import datetime

# ── Thêm bot dir vào path ─────────────────────────────────────────────────────
bot_dir = os.path.dirname(os.path.abspath(__file__))
if bot_dir not in sys.path:
    sys.path.insert(0, bot_dir)

import backtest as bt
from config import SYMBOL_CONFIG, ML_CONFIRMED_WATCHLIST

# ══════════════════════════════════════════════════════════════════════════════
# CF RULE DEFINITIONS (đồng bộ với context_scorecard.py)
# ══════════════════════════════════════════════════════════════════════════════

# FIX: Import apply_cf từ cf_walk_forward để đảm bảo đồng bộ rules
# Không duplicate rule definitions — single source of truth
try:
    from cf_walk_forward import apply_cf as _apply_cf_wf, DEFAULT_CF
    _CF_IMPORT_OK = True
except ImportError:
    _CF_IMPORT_OK = False
    DEFAULT_CF = {
        'cf1_ob_max': 3.0, 'cf2_vol_mult': 1.30,
        'cf3_ok_min': 2.0, 'cf3_ok_max': 5.0,
        'cf3_exempt_score': 85, 'cf4_slope_min': 0.3, 'cf5_score_min': 85,
    }


def apply_cf_rules(df_trades: pd.DataFrame,
                   vni_atr_median: float = None,
                   active_rules: list = None) -> pd.DataFrame:
    """
    Wrapper đồng bộ với cf_walk_forward.apply_cf.
    active_rules: None = dùng optimal combo từ ablation (default CF1+CF2+CF3)
    Trả về df với cột cf_pass (đồng bộ naming).
    """
    _rules = active_rules or ['CF1','CF2','CF3']
    if _CF_IMPORT_OK:
        df = _apply_cf_wf(df_trades, DEFAULT_CF,
                          median=vni_atr_median, active_rules=_rules)
    else:
        # Fallback nếu không import được
        df = df_trades.copy()
        vals = df['vni_atr_ratio'].replace(0, float('nan')).dropna()
        med  = float(vals.median()) if len(vals)>=5 else (vni_atr_median or 0.80)
        df['cf1'] = df['vni_ma20_dist'] < DEFAULT_CF['cf1_ob_max']
        df['cf2'] = df['vni_atr_ratio'] < med * DEFAULT_CF['cf2_vol_mult']
        exempt = df['score'] >= DEFAULT_CF['cf3_exempt_score']
        in_ok  = ((df['ma20_dist'] >= DEFAULT_CF['cf3_ok_min']) &
                  (df['ma20_dist'] <  DEFAULT_CF['cf3_ok_max']))
        df['cf3'] = exempt | (~in_ok)
        mask = pd.Series([True]*len(df), index=df.index)
        if 'CF1' in _rules: mask = mask & df['cf1']
        if 'CF2' in _rules: mask = mask & df['cf2']
        if 'CF3' in _rules: mask = mask & df['cf3']
        df['cf_pass'] = mask
    # Alias for backward compatibility
    df['cf_all_pass'] = df['cf_pass']
    return df


def compute_stats(df: pd.DataFrame, label: str = '') -> dict:
    """Tính WR, Exp, PF từ df trades."""
    if len(df) == 0:
        return {'label': label, 'n': 0, 'wr': 0, 'exp': 0, 'pf': 0,
                'avg_win': 0, 'avg_loss': 0}
    wins   = df[df['pnl'] > 0]
    losses = df[df['pnl'] <= 0]
    wr     = len(wins) / len(df) * 100
    gp     = wins['pnl'].sum()   if len(wins)   > 0 else 0
    gl     = abs(losses['pnl'].sum()) if len(losses) > 0 else 0
    pf     = gp / gl if gl > 0 else float('inf')
    return {
        'label':    label,
        'n':        len(df),
        'wr':       round(wr, 1),
        'exp':      round(df['pnl'].mean(), 2),
        'pf':       round(pf, 2) if pf != float('inf') else 99.0,
        'avg_win':  round(wins['pnl'].mean(), 2) if len(wins) > 0 else 0,
        'avg_loss': round(losses['pnl'].mean(), 2) if len(losses) > 0 else 0,
        'n_pass':   int(df['cf_all_pass'].sum()) if 'cf_all_pass' in df.columns else 0,
    }


# ══════════════════════════════════════════════════════════════════════════════
# PER-SYMBOL ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════

def analyze_symbol_cf(symbol: str,
                      sl: float = 0.07,
                      tp: float = 0.14,
                      hold_days: int = 15,
                      min_score: int = 65,
                      verbose: bool = True) -> dict | None:
    """
    Chạy backtest cho 1 symbol, áp CF rules, trả về comparison stats.
    """
    if verbose:
        print(f"\n  Đang chạy {symbol}...", end='', flush=True)

    # Lấy config từ SYMBOL_CONFIG nếu có
    cfg = SYMBOL_CONFIG.get(symbol.upper(), {})
    _sl  = cfg.get('sl',  sl)
    _tp  = cfg.get('tp',  tp)
    _hd  = cfg.get('hold_days', hold_days)
    _ms  = cfg.get('min_score', min_score)

    result = bt.run_backtest_symbol(
        symbol, verbose=False,
        sl=_sl, tp=_tp, hold_days=_hd, min_score=_ms,
        use_regime=False,  # tắt regime để giữ all trades, CF sẽ tự lọc
        use_vwap=False,
    )

    if result is None or result.get('trades') is None:
        if verbose: print(" ✗ không có data")
        return None

    df = result['trades'].copy()
    if len(df) < 10:
        if verbose: print(f" ⚠ quá ít lệnh ({len(df)})")
        return None

    # Tính VNI ATR median từ data
    vni_atr_median = 0.80
    if 'vni_atr_ratio' in df.columns:
        vals = df['vni_atr_ratio'].replace(0, np.nan).dropna()
        if len(vals) >= 10:
            vni_atr_median = float(vals.median())

    # Kiểm tra có đủ rich fields không
    required = ['vni_ma20_dist', 'vni_atr_ratio', 'ma20_dist', 'score']
    missing = [c for c in required if c not in df.columns]
    if missing:
        if verbose: print(f" ⚠ thiếu fields: {missing}")
        return None

    # Áp CF rules
    df = apply_cf_rules(df, vni_atr_median=vni_atr_median)

    df_all  = df[df['action'] == 'MUA'].copy()
    df_pass = df_all[df_all['cf_all_pass']].copy()

    if len(df_all) < 5:
        if verbose: print(f" ⚠ quá ít lệnh MUA ({len(df_all)})")
        return None

    stats_before = compute_stats(df_all,  label='Truoc CF')
    stats_after  = compute_stats(df_pass, label='Sau CF')

    # CF1/CF2/CF3 breakdown
    n_fail_cf1 = int((~df_all['cf1_pass']).sum())
    n_fail_cf2 = int((~df_all['cf2_pass']).sum())
    n_fail_cf3 = int((~df_all['cf3_pass']).sum())
    n_pass     = int(df_all['cf_all_pass'].sum())
    pass_rate  = round(n_pass / len(df_all) * 100, 1) if len(df_all) > 0 else 0

    dwr  = round(stats_after['wr']  - stats_before['wr'],  1)
    dexp = round(stats_after['exp'] - stats_before['exp'], 2)
    dpf  = round(stats_after['pf']  - stats_before['pf'],  2)

    # Verdict
    if len(df_pass) < 8:
        verdict = 'INSUFFICIENT_N'
        verdict_vi = 'Khong du mau (n_pass<8)'
    elif dexp > 0.5 and dwr > 3:
        verdict = 'STRONG_IMPROVEMENT'
        verdict_vi = 'Cai thien ro rang'
    elif dexp > 0.2:
        verdict = 'MODERATE_IMPROVEMENT'
        verdict_vi = 'Cai thien nhe'
    elif dexp > -0.2:
        verdict = 'NEUTRAL'
        verdict_vi = 'Trung tinh'
    else:
        verdict = 'DEGRADED'
        verdict_vi = 'Lam toi hon'

    if verbose:
        pf_str = f'{stats_before["pf"]:.2f}→{stats_after["pf"]:.2f}'
        print(f" OK | n={len(df_all)}→{n_pass} ({pass_rate:.0f}%) | "
              f"WR={stats_before['wr']:.1f}→{stats_after['wr']:.1f}% "
              f"({dwr:+.1f}) | Exp={stats_before['exp']:+.2f}→{stats_after['exp']:+.2f}% "
              f"({dexp:+.2f}) | [{verdict_vi}]")

    return {
        'symbol':        symbol,
        'sl':            _sl, 'tp': _tp, 'hold_days': _hd, 'min_score': _ms,
        'before':        stats_before,
        'after':         stats_after,
        'dwr':           dwr,
        'dexp':          dexp,
        'dpf':           dpf,
        'pass_rate':     pass_rate,
        'n_total':       len(df_all),
        'n_pass':        n_pass,
        'n_fail_cf1':    n_fail_cf1,
        'n_fail_cf2':    n_fail_cf2,
        'n_fail_cf3':    n_fail_cf3,
        'vni_atr_median':round(vni_atr_median, 3),
        'verdict':       verdict,
        'verdict_vi':    verdict_vi,
        'sufficient_n':  len(df_pass) >= 15,
    }


# ══════════════════════════════════════════════════════════════════════════════
# GROUP ANALYSIS + COMPARISON
# ══════════════════════════════════════════════════════════════════════════════

def analyze_group(symbols: list[str],
                  group_name: str,
                  sl: float = 0.07,
                  tp: float = 0.14,
                  hold_days: int = 15,
                  min_score: int = 65) -> dict:
    """Chạy CF analysis cho toàn bộ 1 nhóm."""

    print(f"\n{'═'*60}")
    print(f"  GROUP: {group_name} ({len(symbols)} mã)")
    print('═'*60)

    results = []
    for sym in symbols:
        r = analyze_symbol_cf(sym, sl=sl, tp=tp,
                               hold_days=hold_days, min_score=min_score)
        if r:
            results.append(r)

    if not results:
        print("  ✗ Không có kết quả")
        return {'group': group_name, 'results': [], 'summary': None}

    # Summary stats
    valid = [r for r in results if r['verdict'] != 'INSUFFICIENT_N']
    suff  = [r for r in results if r['sufficient_n']]

    avg_dexp_all   = np.mean([r['dexp'] for r in valid]) if valid else 0
    avg_dwr_all    = np.mean([r['dwr']  for r in valid]) if valid else 0
    avg_pass_rate  = np.mean([r['pass_rate'] for r in valid]) if valid else 0

    n_improved  = sum(1 for r in valid if r['dexp'] > 0.2)
    n_neutral   = sum(1 for r in valid if -0.2 <= r['dexp'] <= 0.2)
    n_degraded  = sum(1 for r in valid if r['dexp'] < -0.2)
    n_insuff    = sum(1 for r in results if r['verdict'] == 'INSUFFICIENT_N')

    summary = {
        'group':          group_name,
        'n_symbols':      len(symbols),
        'n_valid':        len(valid),
        'n_sufficient_n': len(suff),
        'avg_dexp':       round(avg_dexp_all, 2),
        'avg_dwr':        round(avg_dwr_all, 1),
        'avg_pass_rate':  round(avg_pass_rate, 1),
        'n_improved':     n_improved,
        'n_neutral':      n_neutral,
        'n_degraded':     n_degraded,
        'n_insuff_n':     n_insuff,
        'pct_improved':   round(n_improved / len(valid) * 100) if valid else 0,
    }

    # Print group summary
    print(f"\n  ── GROUP SUMMARY: {group_name} ──")
    print(f"  Mã valid: {len(valid)}/{len(symbols)} | "
          f"n_pass>=15: {len(suff)}")
    print(f"  Avg dExp: {avg_dexp_all:+.2f}%  Avg dWR: {avg_dwr_all:+.1f}%  "
          f"Pass rate: {avg_pass_rate:.0f}%")
    print(f"  Cải thiện: {n_improved} | Trung tính: {n_neutral} | "
          f"Tệ hơn: {n_degraded} | Thiếu n: {n_insuff}")

    return {'group': group_name, 'results': results, 'summary': summary}


def compare_groups(group1: dict, group2: dict):
    """
    So sánh kết quả CF giữa 2 nhóm.
    Đây là phần quan trọng nhất — xác định CF là universal hay không.
    """
    s1 = group1['summary']
    s2 = group2['summary']

    if not s1 or not s2:
        print("Không đủ data để so sánh")
        return

    print(f"\n{'═'*70}")
    print("  SO SÁNH 2 NHÓM — CF UNIVERSAL HAY KHÔNG?")
    print('═'*70)

    print(f"\n  {'Metric':<30} {s1['group']:<20} {s2['group']:<20}")
    print(f"  {'-'*70}")

    metrics = [
        ('Mã có kết quả', 'n_valid', ''),
        ('n_pass >= 15 (đủ tin)', 'n_sufficient_n', ''),
        ('Avg dExp (CF delta)', 'avg_dexp', '%'),
        ('Avg dWR', 'avg_dwr', '%'),
        ('Pass rate (% lệnh pass)', 'avg_pass_rate', '%'),
        ('% mã cải thiện', 'pct_improved', '%'),
        ('Số mã cải thiện', 'n_improved', ''),
        ('Số mã trung tính', 'n_neutral', ''),
        ('Số mã tệ hơn', 'n_degraded', ''),
    ]
    for label, key, unit in metrics:
        v1 = s1.get(key, 0)
        v2 = s2.get(key, 0)
        arrow = ''
        if isinstance(v1, float) and isinstance(v2, float):
            arrow = ' ↑' if v2 > v1 else (' ↓' if v2 < v1 else ' =')
        print(f"  {label:<30} {str(v1)+unit:<20} {str(v2)+unit+arrow:<20}")

    # Interpretation
    print(f"\n  ── DIỄN GIẢI ──")

    avg_dexp_diff = s2['avg_dexp'] - s1['avg_dexp']
    pct_improved_diff = s2['pct_improved'] - s1['pct_improved']

    if abs(avg_dexp_diff) < 0.15 and abs(pct_improved_diff) < 10:
        interp = ("UNIVERSAL: CF hoạt động tương tự trên cả 2 nhóm. "
                  "→ Có thể áp dụng CF cho mọi mã pass sascreen.")
        action = "Mở rộng CF cho sascreen candidates."
    elif s1['avg_dexp'] > s2['avg_dexp'] + 0.2:
        interp = ("EDGE-DEPENDENT: CF cải thiện tốt hơn trên mã có edge sẵn. "
                  "→ CF không tạo edge, chỉ tối ưu timing cho mã đã proven.")
        action = "Chỉ dùng CF trên watchlist đã validated. Không expand sang sascreen."
    elif s2['avg_dexp'] > s1['avg_dexp'] + 0.2:
        interp = ("DISCOVERY: CF cải thiện nhiều hơn trên mã mới. "
                  "→ CF có thể giúp qualify mã vào watchlist!")
        action = "Thử dùng CF như tiêu chí bổ sung khi sascreen mã mới."
    else:
        interp = "KẾT QUẢ MIXED — cần thêm data hoặc điều chỉnh ngưỡng CF."
        action = "Chạy thêm mã, kiểm tra n_pass."

    print(f"  {interp}")
    print(f"\n  ACTION: {action}")


def format_results_table(results: list[dict], group_name: str):
    """In bảng kết quả đẹp ra console."""
    print(f"\n  ── BẢNG CHI TIẾT: {group_name} ──")
    print(f"  {'Mã':<6} {'n':>4} {'n_pass':>6} {'Pass%':>6} "
          f"{'WR_bef':>7} {'WR_aft':>7} {'dWR':>5} "
          f"{'Exp_bef':>8} {'Exp_aft':>8} {'dExp':>6} "
          f"{'PF_bef':>7} {'PF_aft':>7} "
          f"{'CF1F':>5} {'CF2F':>5} {'CF3F':>5} "
          f"{'Verdict'}")
    print(f"  {'-'*130}")

    for r in sorted(results, key=lambda x: x['dexp'], reverse=True):
        b = r['before']
        a = r['after']
        suf = '*' if r['sufficient_n'] else ' '
        pf_aft = f"{a['pf']:.2f}" if a['pf'] < 90 else "inf"
        pf_bef = f"{b['pf']:.2f}" if b['pf'] < 90 else "inf"
        print(
            f"  {r['symbol']:<6}{suf} {b['n']:>4} {r['n_pass']:>6} "
            f"{r['pass_rate']:>5.0f}% "
            f"{b['wr']:>6.1f}% {a['wr']:>6.1f}% {r['dwr']:>+5.1f}% "
            f"{b['exp']:>+7.2f}% {a['exp']:>+7.2f}% {r['dexp']:>+5.2f}% "
            f"{pf_bef:>7} {pf_aft:>7} "
            f"{r['n_fail_cf1']:>5} {r['n_fail_cf2']:>5} {r['n_fail_cf3']:>5} "
            f"  [{r['verdict_vi']}]"
        )
    print(f"  (* = n_pass >= 15, đủ thống kê)")


def save_results(all_results: dict, path: str = 'cf_validation_results.json'):
    """Lưu kết quả ra JSON để xem lại."""
    # Convert numpy types
    def convert(obj):
        if isinstance(obj, (np.integer, np.int64)): return int(obj)
        if isinstance(obj, (np.floating, np.float64)): return float(obj)
        if isinstance(obj, np.ndarray): return obj.tolist()
        return obj

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2, default=convert)
    print(f"\n  Đã lưu kết quả: {path}")


# ══════════════════════════════════════════════════════════════════════════════
# ML GROUP CONFIG
# ══════════════════════════════════════════════════════════════════════════════

# ML_CONFIRMED params (từ SESSION_PROMPT V11)
ML_PARAMS = {
    # sym: (sl, tp, hold_days, min_score)
    'HCM': (0.05, 0.15, 18, 75),
    'FRT': (0.05, 0.15, 18, 75),
    'VCI': (0.05, 0.15, 18, 75),
    'LPB': (0.05, 0.15, 18, 75),
    'DGC': (0.06, 0.17, 18, 75),
    'NKG': (0.06, 0.17, 18, 75),
    'SSI': (0.06, 0.17, 18, 75),
    'MWG': (0.06, 0.17, 18, 75),
    'VIX': (0.06, 0.17, 18, 75),
    'BSI': (0.06, 0.17, 18, 75),
    'ORS': (0.06, 0.17, 18, 75),
    'HDB': (0.06, 0.17, 18, 75),
}


def run_ml_group(verbose: bool = True) -> dict:
    """Chạy CF analysis cho ML_CONFIRMED_WATCHLIST."""
    symbols = list(ML_PARAMS.keys())
    results = []

    print(f"\n{'═'*60}")
    print(f"  NHÓM 1: ML_CONFIRMED ({len(symbols)} mã)")
    print(f"  Params: SL=5-6% TP=15-17% Hold=18d Score>=75")
    print('═'*60)

    for sym in symbols:
        sl, tp, hd, ms = ML_PARAMS[sym]
        r = analyze_symbol_cf(sym, sl=sl, tp=tp,
                               hold_days=hd, min_score=ms,
                               verbose=verbose)
        if r:
            results.append(r)

    if not results:
        return {'group': 'ML_CONFIRMED', 'results': [], 'summary': None}

    valid = [r for r in results if r['verdict'] != 'INSUFFICIENT_N']
    suff  = [r for r in results if r['sufficient_n']]

    summary = {
        'group':          'ML_CONFIRMED',
        'n_symbols':      len(symbols),
        'n_valid':        len(valid),
        'n_sufficient_n': len(suff),
        'avg_dexp':       round(np.mean([r['dexp'] for r in valid]), 2) if valid else 0,
        'avg_dwr':        round(np.mean([r['dwr']  for r in valid]), 1) if valid else 0,
        'avg_pass_rate':  round(np.mean([r['pass_rate'] for r in valid]), 1) if valid else 0,
        'n_improved':     sum(1 for r in valid if r['dexp'] > 0.2),
        'n_neutral':      sum(1 for r in valid if -0.2 <= r['dexp'] <= 0.2),
        'n_degraded':     sum(1 for r in valid if r['dexp'] < -0.2),
        'n_insuff_n':     sum(1 for r in results if r['verdict'] == 'INSUFFICIENT_N'),
        'pct_improved':   round(sum(1 for r in valid if r['dexp']>0.2)/len(valid)*100) if valid else 0,
    }

    format_results_table(results, 'ML_CONFIRMED')
    return {'group': 'ML_CONFIRMED', 'results': results, 'summary': summary}


def run_sascreen_group(symbols: list[str],
                       sl: float = 0.07,
                       tp: float = 0.14,
                       hold_days: int = 15,
                       min_score: int = 65,
                       verbose: bool = True) -> dict:
    """
    Chạy CF analysis cho sascreen candidates.
    symbols: danh sách mã từ /sascreen output
    """
    print(f"\n{'═'*60}")
    print(f"  NHÓM 2: SASCREEN CANDIDATES ({len(symbols)} mã)")
    print(f"  Params: SL={sl*100:.0f}% TP={tp*100:.0f}% "
          f"Hold={hold_days}d Score>={min_score}")
    print('═'*60)

    results = []
    for sym in symbols:
        # Check SYMBOL_CONFIG override
        cfg = SYMBOL_CONFIG.get(sym.upper(), {})
        _sl = cfg.get('sl', sl)
        _tp = cfg.get('tp', tp)
        _hd = cfg.get('hold_days', hold_days)
        _ms = cfg.get('min_score', min_score)
        r = analyze_symbol_cf(sym, sl=_sl, tp=_tp,
                               hold_days=_hd, min_score=_ms,
                               verbose=verbose)
        if r:
            results.append(r)

    if not results:
        return {'group': 'SASCREEN', 'results': [], 'summary': None}

    valid = [r for r in results if r['verdict'] != 'INSUFFICIENT_N']
    suff  = [r for r in results if r['sufficient_n']]

    summary = {
        'group':          'SASCREEN',
        'n_symbols':      len(symbols),
        'n_valid':        len(valid),
        'n_sufficient_n': len(suff),
        'avg_dexp':       round(np.mean([r['dexp'] for r in valid]), 2) if valid else 0,
        'avg_dwr':        round(np.mean([r['dwr']  for r in valid]), 1) if valid else 0,
        'avg_pass_rate':  round(np.mean([r['pass_rate'] for r in valid]), 1) if valid else 0,
        'n_improved':     sum(1 for r in valid if r['dexp'] > 0.2),
        'n_neutral':      sum(1 for r in valid if -0.2 <= r['dexp'] <= 0.2),
        'n_degraded':     sum(1 for r in valid if r['dexp'] < -0.2),
        'n_insuff_n':     sum(1 for r in results if r['verdict'] == 'INSUFFICIENT_N'),
        'pct_improved':   round(sum(1 for r in valid if r['dexp']>0.2)/len(valid)*100) if valid else 0,
    }

    format_results_table(results, 'SASCREEN')
    return {'group': 'SASCREEN', 'results': results, 'summary': summary}


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='CF Validation Framework')
    parser.add_argument('--group', choices=['ml','sascreen','both'],
                        default='both', help='Nhóm cần test')
    parser.add_argument('--symbols', type=str, default='',
                        help='Danh sách mã sascreen, ngăn cách bằng dấu phẩy')
    parser.add_argument('--sl',   type=float, default=0.07)
    parser.add_argument('--tp',   type=float, default=0.14)
    parser.add_argument('--hold', type=int,   default=15)
    parser.add_argument('--score',type=int,   default=65)
    parser.add_argument('--save', action='store_true',
                        help='Lưu kết quả ra JSON')
    parser.add_argument('--quiet', action='store_true')
    args = parser.parse_args()

    verbose = not args.quiet
    all_results = {}
    ts = datetime.now().strftime('%Y%m%d_%H%M')

    print(f"\n{'╔'+'═'*58+'╗'}")
    print(f"║  CF VALIDATION FRAMEWORK — VN Trader Bot V6{' '*13}║")
    print(f"║  {datetime.now():%d/%m/%Y %H:%M}  |  3 CF Rules{' '*27}║")
    print(f"{'╚'+'═'*58+'╝'}")

    group1 = group2 = None

    if args.group in ('ml', 'both'):
        group1 = run_ml_group(verbose=verbose)
        all_results['ml'] = group1

    if args.group in ('sascreen', 'both'):
        if not args.symbols:
            # Default: dùng một số mã HOSE phổ biến làm example
            # Thay bằng output thực từ /sascreen
            default_sascreen = [
                'VCB','BID','CTG','MBB','TCB',   # banking
                'HPG','HSG','TLH',                # steel
                'VHM','VIC','DXG',                # real estate
                'MWG','PNJ',                      # retail
                'GAS','PLX',                      # energy
            ]
            print(f"\n  Không có --symbols → dùng default ({len(default_sascreen)} mã)")
            sascreen_syms = default_sascreen
        else:
            sascreen_syms = [s.strip().upper() for s in args.symbols.split(',') if s.strip()]

        group2 = run_sascreen_group(
            sascreen_syms,
            sl=args.sl, tp=args.tp,
            hold_days=args.hold, min_score=args.score,
            verbose=verbose
        )
        all_results['sascreen'] = group2

    # So sánh nếu có cả 2 nhóm
    if group1 and group2 and group1['summary'] and group2['summary']:
        compare_groups(group1, group2)

    # Lưu kết quả
    if args.save:
        out_path = f'cf_validation_{ts}.json'
        save_results(all_results, out_path)

    print(f"\n  Hoàn thành.\n")


if __name__ == '__main__':
    main()
