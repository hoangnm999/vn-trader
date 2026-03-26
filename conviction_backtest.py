#!/usr/bin/env python3
"""
conviction_backtest.py — Tìm ngưỡng conviction tối ưu per-symbol

Conviction = số tầng xác nhận đồng thuận:
  [1] Score A >= min_score  (luôn = 1 vì đây là trigger)
  [2] Regime: BULL=1, NEUTRAL=0.5, BEAR=0, exempt=0.5
  [3] VWAP:   P>W&M=1, 1 trong 2=0.5, cả 2 âm=0
  [4] Shark:  offline backtest không có data → 0.5 neutral

Max conviction = 4 (khi cả 4 tầng đồng thuận)

Test K = 1.0, 1.5, 2.0, 2.5, 3.0 → tìm K tốt nhất per-symbol

Usage:
  python3 conviction_backtest.py --syms DGC SSI HCM POW
  python3 conviction_backtest.py  (toàn 28 mã)
"""
import sys, argparse
sys.path.insert(0, '.')
from backtest import run_backtest_symbol, SYMBOL_CONFIG

ALL_SYMS = [
    'DGC','DCM','SSI','NKG','MBB','HSG','FRT','VND','HCM','PDR',
    'NVL','VIC','BID','KBC','FPT','SZC','KDH','GAS','PVS','POW',
    'HPG','TCB','VPB','VCB','MWG','CMG','PVD','REE',
]

K_LEVELS = [1.0, 1.5, 2.0, 2.5, 3.0]


def run_conviction_backtest(symbols):
    summary = []

    for sym in symbols:
        print(f"\n{'='*55}")
        print(f"  CONVICTION BACKTEST: {sym}")
        print('='*55)

        cfg = SYMBOL_CONFIG.get(sym.upper(), {})
        use_regime = cfg.get('use_regime', True)
        use_vwap   = cfg.get('use_vwap', True)
        wf         = cfg.get('wf_verdict', '?')

        # Load data 1 lần
        from backtest import load_data, LOOKBACK_DAYS
        _days = cfg.get('days', LOOKBACK_DAYS)
        df, _ = load_data(sym, days=_days)
        if df is None:
            print(f"  {sym}: Khong tai duoc data")
            continue

        results_k = {}
        for k in K_LEVELS:
            r = run_backtest_symbol(
                sym, verbose=False,
                use_regime=use_regime,
                use_vwap=use_vwap,
                _df_cache=df,
                min_conviction=k
            )
            if r:
                st = r.get('buy', {})
                results_k[k] = {
                    'n':   st.get('total', 0),
                    'wr':  st.get('win_rate', 0),
                    'pnl': st.get('avg_pnl', 0),
                    'pf':  st.get('profit_factor', 0),
                }
            else:
                results_k[k] = {'n': 0, 'wr': 0, 'pnl': 0, 'pf': 0}

        # Baseline (K=1.0 = chỉ cần Score A)
        base = results_k.get(1.0, {})
        base_wr  = base.get('wr', 0)
        base_pnl = base.get('pnl', 0)

        print(f"  WF={wf} | Regime={'True ' if use_regime else 'False'} | VWAP={'True ' if use_vwap else 'False'}")
        print(f"  {'K':6} {'N':5} {'WR':7} {'PnL':8} {'PF':6} {'dWR':7} {'dPnL':8} Note")
        print(f"  {'-'*62}")

        best_k = 1.0
        best_score = 0
        for k in K_LEVELS:
            r = results_k[k]
            n, wr, pnl, pf = r['n'], r['wr'], r['pnl'], r['pf']
            dwr  = round(wr  - base_wr,  1)
            dpnl = round(pnl - base_pnl, 2)

            if n >= 30:   stat = '✅'
            elif n >= 20: stat = '🟡'
            else:         stat = '❌ ít'

            note = stat
            if k == 1.0: note += ' baseline'

            # Score tổng hợp để chọn best K
            if n >= 20:
                score_val = dwr * 0.6 + dpnl * 10 * 0.4
                if score_val > best_score:
                    best_score = score_val
                    best_k = k

            print(f"  K={k:.1f}  {n:4}L {wr:5.1f}%  {pnl:+6.2f}%  {pf:5.2f}  {dwr:+5.1f}%  {dpnl:+6.2f}%  {note}")

        best = results_k.get(best_k, {})
        print(f"\n  → Best K={best_k}: {best.get('n',0)}L WR={best.get('wr',0):.1f}% PnL={best.get('pnl',0):+.2f}%")

        summary.append({
            'sym': sym, 'wf': wf,
            'use_regime': use_regime, 'use_vwap': use_vwap,
            'best_k': best_k,
            'base_n': base.get('n', 0) if k==1.0 else results_k.get(1.0,{}).get('n',0),
            'best_n': best.get('n', 0),
            'best_wr': best.get('wr', 0),
            'best_pnl': best.get('pnl', 0),
            'base_wr': base_wr, 'base_pnl': base_pnl,
        })

    # Tổng kết
    if len(summary) > 1:
        print(f"\n{'='*65}")
        print("TỔNG KẾT CONVICTION BACKTEST")
        print('='*65)
        print(f"  {'Ma':5} {'WF':5} {'Best K':7} {'Base':14} {'Best':14} {'dWR':7} {'dPnL':7}")
        print(f"  {'-'*60}")
        for r in summary:
            base_str = f"{r['base_n']}L WR={r['base_wr']:.0f}%"
            best_str = f"{r['best_n']}L WR={r['best_wr']:.0f}%"
            dwr  = round(r['best_wr']  - r['base_wr'],  1)
            dpnl = round(r['best_pnl'] - r['base_pnl'], 2)
            icon = '✅' if dwr >= 1 and dpnl >= 0 else ('🟡' if dwr >= 0 else '❌')
            print(f"  {r['sym']:5} {r['wf']:5} K={r['best_k']:.1f}  {base_str:14} {best_str:14} {dwr:+5.1f}%  {dpnl:+5.2f}% {icon}")

        # Distribution of best K
        from collections import Counter
        k_dist = Counter(r['best_k'] for r in summary)
        print(f"\n  Best K distribution:")
        for k in sorted(k_dist):
            syms_k = [r['sym'] for r in summary if r['best_k'] == k]
            print(f"    K={k:.1f}: {k_dist[k]} mã → {syms_k}")

    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--syms', nargs='+', default=ALL_SYMS)
    args = parser.parse_args()
    run_conviction_backtest([s.upper() for s in args.syms])
