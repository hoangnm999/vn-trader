#!/usr/bin/env python3
"""
vwap_backtest_compare.py
So sanh ket qua backtest Score A co va khong co VWAP bonus.
Chay offline: python3 vwap_backtest_compare.py
              python3 vwap_backtest_compare.py --syms DGC POW HCM
"""
import sys, argparse
sys.path.insert(0, '.')
from backtest import run_backtest_symbol, SYMBOL_CONFIG

ALL_SYMS = [
    'DGC','DCM','SSI','NKG','MBB','HSG','FRT','VND','HCM','PDR',
    'NVL','VIC','BID','KBC','FPT','SZC','KDH','GAS','PVS','POW',
    'HPG','TCB','VPB','VCB','MWG','CMG','PVD','REE',
]

def run_compare(symbols):
    results = []
    for sym in symbols:
        print(f"\n{'='*55}")
        print(f"  VWAP BACKTEST: {sym}")
        print('='*55)

        cfg = SYMBOL_CONFIG.get(sym.upper(), {})
        use_regime = cfg.get('use_regime', True)

        # Baseline: Score A + Regime (như hiện tại), KHÔNG VWAP
        r_no = run_backtest_symbol(sym, verbose=False,
                                   use_regime=use_regime, use_vwap=False)
        # Có VWAP: Score A + Regime + VWAP bonus
        r_yes = run_backtest_symbol(sym, verbose=False,
                                    use_regime=use_regime, use_vwap=True)

        st_no  = r_no['stats']  if r_no  else {}
        st_yes = r_yes['stats'] if r_yes else {}

        wr_no   = st_no.get('win_rate', 0)
        wr_yes  = st_yes.get('win_rate', 0)
        pnl_no  = st_no.get('avg_pnl', 0)
        pnl_yes = st_yes.get('avg_pnl', 0)
        pf_no   = st_no.get('profit_factor', 0)
        pf_yes  = st_yes.get('profit_factor', 0)
        n_no    = st_no.get('total', 0)
        n_yes   = st_yes.get('total', 0)

        dwr  = round(wr_yes  - wr_no,  1)
        dpnl = round(pnl_yes - pnl_no, 2)
        dn   = n_yes - n_no  # lệnh bị block (âm = block thêm)

        if dwr >= 1.0 and dpnl >= 0:
            flag = '✅ CO ICH'
        elif abs(dwr) < 1.0 and abs(dpnl) < 0.1:
            flag = '〰 TRUNG TINH'
        elif dwr >= 0 or dpnl >= 0:
            flag = '🟡 CO ICH NHE'
        else:
            flag = '❌ CO HAI'

        print(f"  Regime: {'True' if use_regime else 'False'}")
        print(f"  Khong VWAP: {n_no}L  WR={wr_no:.1f}%  PnL={pnl_no:+.2f}%  PF={pf_no:.2f}")
        print(f"  Co VWAP:    {n_yes}L  WR={wr_yes:.1f}%  PnL={pnl_yes:+.2f}%  PF={pf_yes:.2f}")
        print(f"  Delta: {flag}  dWR={dwr:+.1f}%  dPnL={dpnl:+.2f}%  block={-dn}L")

        results.append({
            'sym': sym, 'use_regime': use_regime,
            'n_no': n_no, 'wr_no': wr_no, 'pnl_no': pnl_no,
            'n_yes': n_yes, 'wr_yes': wr_yes, 'pnl_yes': pnl_yes,
            'dwr': dwr, 'dpnl': dpnl, 'dn': dn, 'flag': flag,
        })

    # ── Tổng kết ──
    if len(results) > 1:
        print(f"\n{'='*60}")
        print("TỔNG KẾT VWAP BACKTEST")
        print('='*60)
        print(f"{'Ma':6} {'Regime':7} {'No-VWAP':18} {'Co-VWAP':18} {'dWR':7} {'dPnL':8} Ket qua")
        print("-"*80)
        for r in results:
            reg = 'True ' if r['use_regime'] else 'False'
            no_str  = f"{r['n_no']:2}L WR={r['wr_no']:.0f}% P={r['pnl_no']:+.2f}%"
            yes_str = f"{r['n_yes']:2}L WR={r['wr_yes']:.0f}% P={r['pnl_yes']:+.2f}%"
            print(f"  {r['sym']:5} {reg:7} {no_str:18} {yes_str:18} {r['dwr']:+5.1f}%  {r['dpnl']:+5.2f}%  {r['flag']}")

        pos = [r for r in results if r['dwr'] >= 1.0 and r['dpnl'] >= 0]
        neg = [r for r in results if r['dwr'] < -1.0 and r['dpnl'] < 0]
        neu = [r for r in results if r not in pos and r not in neg]

        avg_dwr  = sum(r['dwr']  for r in results) / len(results)
        avg_dpnl = sum(r['dpnl'] for r in results) / len(results)

        print(f"\n  CO ICH   : {len(pos)} ma — {[r['sym'] for r in pos]}")
        print(f"  TRUNG TINH: {len(neu)} ma")
        print(f"  CO HAI   : {len(neg)} ma — {[r['sym'] for r in neg]}")
        print(f"\n  Avg dWR  = {avg_dwr:+.1f}%")
        print(f"  Avg dPnL = {avg_dpnl:+.2f}%")
        print()
        if avg_dwr >= 1.0:
            print("  → VWAP CO ICH TOAN WATCHLIST — nen giu use_vwap=True")
        elif avg_dwr >= 0:
            print("  → VWAP TRUNG TINH — co the giu nhung xem xet per-symbol")
        else:
            print("  → VWAP CO HAI TOAN WATCHLIST — nen xem xet tat vwap per-symbol")

    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--syms', nargs='+', default=ALL_SYMS)
    args = parser.parse_args()
    run_compare([s.upper() for s in args.syms])
