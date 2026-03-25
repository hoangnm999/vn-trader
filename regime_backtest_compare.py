#!/usr/bin/env python3
"""
regime_backtest_compare.py
So sanh ket qua backtest Score A + Shark voi va khong co Market Regime Filter.
Chay offline (can vnstock + internet).

Usage:
  python3 regime_backtest_compare.py
  python3 regime_backtest_compare.py --syms DGC DCM MBB
"""
import sys, argparse
import pandas as pd
import numpy as np

sys.path.insert(0, '.')
from backtest import run_backtest_symbol, calc_stats
from backtest_shark import run_shark_report

# 28 ma watchlist
ALL_SYMS = [
    'DGC','DCM','SSI','NKG','MBB','HSG','FRT','VND','HCM','PDR',
    'NVL','VIC','BID','KBC','FPT','SZC','KDH','GAS','PVS','POW',
    'HPG','TCB','VPB','VCB','MWG','CMG','PVD','REE',
]

def run_compare(symbols):
    results = []
    for sym in symbols:
        print(f"\n{'='*55}")
        print(f"  {sym}")
        print('='*55)

        # ── Score A: WITHOUT regime ──
        r_no  = run_backtest_symbol(sym, verbose=False, use_regime=False)
        # ── Score A: WITH regime ──
        r_yes = run_backtest_symbol(sym, verbose=False, use_regime=True)

        st_no  = r_no['stats']  if r_no  else {}
        st_yes = r_yes['stats'] if r_yes else {}

        wr_no  = st_no.get('win_rate', 0)
        wr_yes = st_yes.get('win_rate', 0)
        pnl_no  = st_no.get('avg_pnl', 0)
        pnl_yes = st_yes.get('avg_pnl', 0)
        n_no  = st_no.get('total', 0)
        n_yes = st_yes.get('total', 0)

        dwr  = round(wr_yes  - wr_no,  1)
        dpnl = round(pnl_yes - pnl_no, 2)
        dn   = n_yes - n_no  # lenh bi block (am = da block lenh xau)

        flag = ('✅' if dwr >= 1.0 and dpnl >= 0 else
                '〰' if abs(dwr) < 1.0 else
                '❌')

        print(f"  Score A:")
        print(f"    No  regime: {n_no}L  WR={wr_no:.1f}%  PnL={pnl_no:+.2f}%")
        print(f"    With regime:{n_yes}L  WR={wr_yes:.1f}%  PnL={pnl_yes:+.2f}%")
        print(f"    Delta: {flag} dWR={dwr:+.1f}%  dPnL={dpnl:+.2f}%  block={-dn}L")

        # ── Shark best mode: WITH regime ──
        rsh_yes = run_shark_report(sym, verbose=False, use_regime=True)
        rsh_no  = run_shark_report(sym, verbose=False, use_regime=False)

        best_yes = rsh_yes.get('best', {}) if rsh_yes else {}
        best_no  = rsh_no.get('best',  {}) if rsh_no  else {}

        for mode in ['S', 'AS']:
            if mode in best_yes and mode in best_no:
                thr_y, _, st_y = best_yes[mode]
                thr_n, _, st_n = best_no[mode]
                wr_sy = st_y.get('win_rate', 0); pnl_sy = st_y.get('avg_pnl', 0)
                wr_sn = st_n.get('win_rate', 0); pnl_sn = st_n.get('avg_pnl', 0)
                d = round(wr_sy - wr_sn, 1)
                print(f"  Shark {mode}>=: No={wr_sn:.1f}% → With={wr_sy:.1f}% (dWR={d:+.1f}%)")

        results.append({
            'sym':    sym,
            'wr_no':  wr_no,   'wr_yes':  wr_yes,  'dwr':  dwr,
            'pnl_no': pnl_no,  'pnl_yes': pnl_yes, 'dpnl': dpnl,
            'n_no':   n_no,    'n_yes':   n_yes,    'block': -dn,
            'flag':   flag,
        })

    # Tong ket
    df = pd.DataFrame(results)
    print(f"\n{'='*55}")
    print("TONG KET — Market Regime Filter Impact")
    print('='*55)
    print(f"{'Ma':5} {'WR no':7} {'WR yes':7} {'dWR':6} {'PnL no':8} {'PnL yes':8} {'dPnL':7} {'Block':6} {''}")
    print('-'*65)
    for _, row in df.iterrows():
        print(f"{row['sym']:5} {row['wr_no']:6.1f}% {row['wr_yes']:6.1f}% "
              f"{row['dwr']:+5.1f}% {row['pnl_no']:+7.2f}% {row['pnl_yes']:+7.2f}% "
              f"{row['dpnl']:+6.2f}% {row['block']:5.0f}L  {row['flag']}")

    improved = (df['dwr'] >= 1.0).sum()
    neutral  = (df['dwr'].abs() < 1.0).sum()
    worse    = (df['dwr'] < -1.0).sum()
    print(f"\nKet qua: {improved} ma cai thien | {neutral} trung tinh | {worse} te hon")
    print(f"Trung binh: dWR={df['dwr'].mean():+.2f}% | dPnL={df['dpnl'].mean():+.2f}%")
    print(f"Lenh bi block trung binh: {df['block'].mean():.1f}L/ma")

    df.to_csv('regime_compare_results.csv', index=False)
    print("\n✅ Da luu: regime_compare_results.csv")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--syms', nargs='+', default=ALL_SYMS,
                        help='Danh sach ma can test (default: tat ca 28 ma)')
    args = parser.parse_args()
    run_compare(args.syms)
