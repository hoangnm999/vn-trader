#!/usr/bin/env python3
"""
trigger_backtest.py — So sánh 3 trigger mode per-symbol

Mode 1 (score_primary) : Score >= 65           [mặc định hiện tại]
Mode 2 (filter_confirm): Score >= 55 + conv>=2 [Thép, NH, KCN]
Mode 3 (filter_led)    : Score >= 45 + Regime=BULL + VWAP=UP [Dầu khí, BDS]

Câu hỏi cần trả lời:
  "Có case Score A < 65 nhưng Regime+VWAP tốt mà vẫn PnL dương không?"

Usage:
  python3 trigger_backtest.py --syms GAS HPG POW PDR TCB
  python3 trigger_backtest.py  (toàn 28 mã)
"""
import sys, argparse
sys.path.insert(0, '.')
from backtest import run_backtest_symbol, SYMBOL_CONFIG, load_data, LOOKBACK_DAYS

ALL_SYMS = [
    'DGC','DCM','SSI','NKG','MBB','HSG','FRT','VND','HCM','PDR',
    'NVL','VIC','BID','KBC','FPT','SZC','KDH','GAS','PVS','POW',
    'HPG','TCB','VPB','VCB','MWG','CMG','PVD','REE',
]

# Nhóm mã theo đặc thù — gợi ý trigger_mode ban đầu
TRIGGER_HINTS = {
    'score_primary' : ['DGC','DCM','SSI','NKG','FRT','FPT','CMG','MWG','REE'],
    'filter_confirm': ['HSG','HPG','MBB','TCB','VPB','KBC','SZC','BID','VCB'],
    'filter_led'    : ['GAS','POW','PVS','PVD','PDR','NVL','VIC','VND','HCM','KDH'],
}


def run_trigger_backtest(symbols):
    results = []

    for sym in symbols:
        print(f"\n{'='*60}")
        print(f"  TRIGGER BACKTEST: {sym}")
        print('='*60)

        cfg        = SYMBOL_CONFIG.get(sym.upper(), {})
        use_regime = cfg.get('use_regime', True)
        use_vwap   = cfg.get('use_vwap', True)
        wf         = cfg.get('wf_verdict', '?')

        # Gợi ý mode từ sector
        hint = 'score_primary'
        for mode, syms in TRIGGER_HINTS.items():
            if sym in syms:
                hint = mode

        # Load data 1 lần
        _days = cfg.get('days', LOOKBACK_DAYS)
        df, _ = load_data(sym, days=_days)
        if df is None:
            print(f"  ✗ Không tải được data {sym}")
            continue

        modes = [
            ('score_primary',  65, 0,   'Score>=65 (hien tai)'),
            ('filter_confirm', 55, 2.0, 'Score>=55 + conv>=2'),
            ('filter_confirm', 55, 1.5, 'Score>=55 + conv>=1.5'),
            ('filter_led',     45, 0,   'Score>=45 + Regime+VWAP'),
            ('filter_led',     50, 0,   'Score>=50 + Regime+VWAP'),
        ]

        print(f"  WF={wf} | Regime={use_regime} | VWAP={use_vwap} | Hint={hint}")
        print(f"\n  {'Mode':<35} {'N':>4} {'WR':>6} {'PnL':>7} {'PF':>5}  Stat")
        print(f"  {'-'*65}")

        mode_results = {}
        for tmode, tscore, tconv, label in modes:
            r = run_backtest_symbol(
                sym, verbose=False,
                use_regime=use_regime, use_vwap=use_vwap,
                _df_cache=df,
                trigger_mode=tmode,
                trigger_score=tscore,
                min_conviction=tconv,
            )
            st = r.get('buy', {}) if r else {}
            n   = st.get('total', 0)
            wr  = st.get('win_rate', 0)
            pnl = st.get('avg_pnl', 0)
            pf  = st.get('profit_factor', 0)

            stat = ('✅' if n >= 30 else ('🟡' if n >= 20 else '❌'))
            quality = ('🔥' if wr >= 60 and pnl >= 2.0 else
                       '✅' if wr >= 55 and pnl >= 1.0 else
                       '🟡' if wr >= 50 and pnl >= 0   else '❌')

            key = (tmode, tscore, tconv)
            mode_results[key] = {'n': n, 'wr': wr, 'pnl': pnl, 'pf': pf}

            hint_mark = ' ◀ sector hint' if tmode == hint and tscore == (65 if tmode == 'score_primary' else tscore) else ''
            print(f"  {label:<35} {n:>4}L {wr:>5.1f}%  {pnl:>+5.2f}%  {pf:>4.2f}  {stat}{quality}{hint_mark}")

        # So sánh: mode nào tốt hơn baseline?
        baseline = mode_results.get(('score_primary', 65, 0), {})
        b_wr, b_pnl, b_n = baseline.get('wr',0), baseline.get('pnl',0), baseline.get('n',0)

        print(f"\n  Delta vs baseline (Score>=65):")
        best_mode, best_val = None, -99
        for tmode, tscore, tconv, label in modes[1:]:
            r = mode_results.get((tmode, tscore, tconv), {})
            if r.get('n', 0) >= 20:
                dwr  = round(r['wr']  - b_wr,  1)
                dpnl = round(r['pnl'] - b_pnl, 2)
                dn   = r['n'] - b_n
                flag = ('✅' if dwr >= 1 and dpnl >= 0 else
                        '🟡' if dwr >= 0 or dpnl >= 0 else '❌')
                print(f"    {label:<35} dWR={dwr:+.1f}%  dPnL={dpnl:+.2f}%  +{dn}L  {flag}")
                # Score tổng hợp
                val = dwr * 0.5 + dpnl * 10 * 0.3 + (r['n'] - b_n) * 0.01
                if val > best_val:
                    best_val  = val
                    best_mode = (tmode, tscore, tconv, label)

        if best_mode and best_val > 0:
            bm = mode_results.get(best_mode[:3], {})
            print(f"\n  🎯 Best mode: {best_mode[3]}")
            print(f"     {bm.get('n',0)}L WR={bm.get('wr',0):.1f}% PnL={bm.get('pnl',0):+.2f}%")
            rec_mode = best_mode[0]
        else:
            print(f"\n  → Score>=65 vẫn là tốt nhất cho {sym}")
            rec_mode = 'score_primary'

        results.append({
            'sym': sym, 'wf': wf, 'hint': hint, 'rec': rec_mode,
            'b_n': b_n, 'b_wr': b_wr, 'b_pnl': b_pnl,
            'modes': mode_results,
        })

    # Tổng kết
    if len(results) > 1:
        print(f"\n{'='*65}")
        print("TỔNG KẾT — REC TRIGGER MODE PER-SYMBOL")
        print('='*65)

        by_mode = {'score_primary': [], 'filter_confirm': [], 'filter_led': []}
        for r in results:
            by_mode[r['rec']].append(r['sym'])

        for mode, syms in by_mode.items():
            if syms:
                print(f"\n  {mode} ({len(syms)} mã): {syms}")

        print()
        print("  Gợi ý cập nhật SYMBOL_CONFIG:")
        print("  'trigger_mode': 'score_primary'   # default, không cần ghi")
        for r in results:
            if r['rec'] != 'score_primary':
                print(f"  '{r['sym']}': {{'trigger_mode': '{r['rec']}'}}  # WF={r['wf']}")

    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--syms', nargs='+', default=ALL_SYMS)
    args = parser.parse_args()
    run_trigger_backtest([s.upper() for s in args.syms])
