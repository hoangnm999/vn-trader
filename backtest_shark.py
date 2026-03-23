"""
backtest_shark.py v2.0 — Kiem chung Shark Score (OHLCV) vs Shark v2 (+ Foreign Flow)
=======================================================================================
4 che do so sanh:

  Mode S    : Shark OHLCV doc lap (khong Score A, khong Foreign)
  Mode SF   : Shark OHLCV + Foreign Flow doc lap
  Mode A    : Chi Score A ky thuat (baseline hien tai)
  Mode A+S  : Score A + Shark OHLCV ket hop
  Mode A+SF : Score A + Shark v2 (co Foreign Flow) ket hop

Cach dung:
    python backtest_shark.py DGC
    python backtest_shark.py DGC MBB NKG SSI
    python backtest_shark.py --all
"""

import sys, math, warnings
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
warnings.filterwarnings('ignore')

sys.path.insert(0, '.')
from backtest import (
    load_data, compute_score_at, simulate_trade,
    calc_stats, HOLD_DAYS, STOP_LOSS, TAKE_PROFIT,
    MIN_SCORE_BUY, SYMBOL_CONFIG, LOOKBACK_DAYS, MARKET_PHASES, find_col,
)
# v3.0: Wyckoff VSA + Chaikin A/D + Spring Detection + Supply Exhaustion fix
from shark_detector import calc_shark_score, load_foreign_flow

SHARK_THRESHOLDS = [40, 50, 60, 70, 80]
SHARK_LOOKBACK   = 20

WATCHLIST_SHARK = [
    'DGC', 'DCM', 'MBB', 'SSI', 'NKG', 'VND',
    'HSG', 'PDR', 'HCM', 'BID', 'NVL',
]

SEP  = '=' * 68
SEP2 = '-' * 68


_arrays_cache = {}   # Cache trong session: symbol → arrays dict

def _load_arrays(symbol):
    """Load OHLCV 7 năm cho symbol. Cache trong session để các mode dùng chung."""
    sym = symbol.upper()
    if sym in _arrays_cache:
        return _arrays_cache[sym]   # Dùng lại — không gọi Vnstock nữa

    df, source = load_data(sym, days=LOOKBACK_DAYS)
    if df is None: return None

    def to_arr(s):
        return pd.to_numeric(s, errors='coerce').fillna(0).astype(float).values.copy()

    cc = find_col(df, ['close','closeprice','close_price'])
    hc = find_col(df, ['high','highprice','high_price'])
    lc = find_col(df, ['low','lowprice','low_price'])
    vc = next((c for c in df.columns if c.lower() in {
        'volume','volume_match','klgd','vol','trading_volume',
        'match_volume','total_volume','dealvolume','matchingvolume'}), None)
    if cc is None: return None

    closes  = to_arr(df[cc]); closes  = np.where(closes  <1000, closes  *1000, closes ).copy()
    highs   = to_arr(df[hc]) if hc else closes.copy()
    lows    = to_arr(df[lc]) if lc else closes.copy()
    if hc: highs = np.where(highs<1000, highs*1000, highs).copy()
    if lc: lows  = np.where(lows <1000, lows *1000, lows ).copy()
    volumes = to_arr(df[vc]) if vc else np.zeros(len(closes))

    tc = next((c for c in df.columns if c.lower() in
               ('time','date','datetime','trading_date')), None)
    dates = (pd.to_datetime(df[tc], errors='coerce').reset_index(drop=True)
             if tc else pd.Series([pd.NaT]*len(closes)))

    result = {'closes':closes,'highs':highs,'lows':lows,
              'volumes':volumes,'dates':dates,'source':source}
    _arrays_cache[sym] = result   # Lưu cache
    return result


def shark_at(arr, idx, foreign_arr=None, lookback=SHARK_LOOKBACK):
    """Tinh Shark Score tai idx, co the dung foreign data."""
    if idx < lookback: return 0
    s = max(0, idx-lookback+1)
    c = arr['closes'][s:idx+1]; h = arr['highs'][s:idx+1]
    l = arr['lows'][s:idx+1];   v = arr['volumes'][s:idx+1]
    fn = foreign_arr[s:idx+1] if foreign_arr is not None else None
    score, _ = calc_shark_score(c.tolist(), h.tolist(), l.tolist(), v.tolist(),
                                 lookback, foreign_net=fn)
    return score


def _run_mode(symbol, mode, shark_threshold=60, foreign_arr=None):
    """
    Chay backtest cho 1 mode.
    mode: 'S' | 'SF' | 'A' | 'AS' | 'ASF'
    """
    cfg    = SYMBOL_CONFIG.get(symbol.upper(), {})
    _sl    = cfg.get('sl', abs(STOP_LOSS))
    _tp    = cfg.get('tp', TAKE_PROFIT)
    _min_a = cfg.get('min_score', MIN_SCORE_BUY)

    arr = _load_arrays(symbol)
    if arr is None: return None

    closes = arr['closes']; highs = arr['highs']
    lows   = arr['lows'];   vols  = arr['volumes']; dates = arr['dates']

    use_score_a  = mode in ('A', 'AS', 'ASF')
    use_shark    = mode in ('S', 'SF', 'AS', 'ASF')
    use_foreign  = mode in ('SF', 'ASF') and foreign_arr is not None

    trades = []
    last_entry = -HOLD_DAYS

    start_i = max(60, SHARK_LOOKBACK + 5)
    for i in range(start_i, len(closes) - HOLD_DAYS):
        if i - last_entry < HOLD_DAYS: continue

        # Dieu kien Score A
        if use_score_a:
            score_a, action = compute_score_at(closes, highs, lows, vols, i)
            if action != 'MUA' or score_a < _min_a: continue
        else:
            score_a = 0

        # Dieu kien Shark
        if use_shark:
            fn = foreign_arr if use_foreign else None
            shark = shark_at(arr, i, foreign_arr=fn)
            if shark < shark_threshold: continue
        else:
            shark = 0

        pnl, reason, days_held = simulate_trade(closes, i, 'MUA', sl=_sl, tp=_tp)
        ts = dates.iloc[i] if i < len(dates) else pd.NaT
        dt = ts.strftime('%Y-%m-%d') if pd.notna(ts) else f'idx_{i}'

        trades.append({
            'date':    dt,
            'price':   round(closes[i], 0),
            'score_a': score_a,
            'shark':   shark,
            'pnl':     pnl,
            'reason':  reason,
            'days':    days_held,
            'mode':    mode,
        })
        last_entry = i

    if not trades: return None
    df_t = pd.DataFrame(trades)
    return {'symbol':symbol,'mode':mode,'threshold':shark_threshold,
            'trades':df_t,'stats':calc_stats(df_t),'sl':_sl,'tp':_tp}


def _yearly(df_trades):
    """Dict {year: stats}."""
    if df_trades is None or df_trades.empty: return {}
    df = df_trades.copy()
    df['year'] = df['date'].apply(lambda d: pd.to_datetime(d, errors='coerce').year
                                   if d else 0)
    df = df[df['year'] > 0]
    return {yr: calc_stats(df[df['year']==yr]) for yr in sorted(df['year'].unique())}


def run_shark_report(symbol, verbose=True):
    """Bao cao day du 1 ma: S | SF | A | A+S | A+SF."""
    if verbose:
        print(f'\n{SEP}')
        print(f'  SHARK BACKTEST v2: {symbol}')
        print(f'{SEP}')

    # Reset cache cho mỗi lần gọi run_shark_report (fresh data)
    _arrays_cache.clear()

    # Load foreign data (1 lần duy nhất, dùng chung cho tất cả modes)
    if verbose: print('\n  Dang tai foreign flow data...')
    foreign_arr = load_foreign_flow(symbol, days=LOOKBACK_DAYS + 30)
    has_f = foreign_arr is not None and len(foreign_arr) >= 20
    if verbose:
        print(f'  Foreign data: {"CO (%d phien)" % len(foreign_arr) if has_f else "KHONG CO — skip mode SF/ASF"}')

    # ── Baseline A ────────────────────────────────────────────────────────────
    res_a = _run_mode(symbol, 'A')
    st_a  = res_a['stats'] if res_a else {}
    wr_a  = st_a.get('win_rate', 0); pnl_a = st_a.get('avg_pnl', 0)
    pf_a  = st_a.get('profit_factor', 0); n_a = st_a.get('total', 0)

    if verbose:
        print(f'\n  BASELINE — Score A >= {SYMBOL_CONFIG.get(symbol,{}).get("min_score",65)}')
        print(f'  {SEP2}')
        pf_s = f'{pf_a:.2f}' if pf_a != float('inf') else 'inf'
        print(f'  {n_a}L | WR={wr_a:.1f}% | PnL={pnl_a:+.2f}% | PF={pf_s}')

    # ── Tong hop tat ca mode theo nguong ─────────────────────────────────────
    all_rows = []  # (mode_label, thr, n, wr, pnl, pf, dwr, dpnl)

    modes_to_test = [('S', False), ('AS', False)]
    if has_f:
        modes_to_test += [('SF', True), ('ASF', True)]

    best = {}  # mode -> best threshold result

    for mode, use_f in modes_to_test:
        fn_arr = foreign_arr if (use_f and has_f) else None
        best_m = None; best_metric = -999

        for thr in SHARK_THRESHOLDS:
            res = _run_mode(symbol, mode, shark_threshold=thr, foreign_arr=fn_arr)
            if res is None: continue
            st = res['stats']
            n  = st.get('total', 0)
            if n < 3: continue
            wr  = st.get('win_rate', 0)
            pnl = st.get('avg_pnl', 0)
            pf  = st.get('profit_factor', 0)
            dwr  = wr  - wr_a
            dpnl = pnl - pnl_a
            metric = (wr/100) * pnl * (0.8 if n < 8 else 1.0)
            all_rows.append((mode, thr, n, wr, pnl, pf, dwr, dpnl))
            if metric > best_metric and n >= 3:
                best_metric = metric; best_m = (thr, res, st)
        if best_m: best[mode] = best_m

    # ── In bang ket qua ──────────────────────────────────────────────────────
    if verbose:
        print(f'\n  KET QUA THEO NGUONG (so voi A baseline)')
        print(f'  {"Mode":>5} | {"Thr":>3} | {"L":>4} | {"WR%":>5} | {"PnL":>6} | {"PF":>5} | {"dWR":>5} | Danh gia')
        print(f'  {SEP2}')
        for mode, thr, n, wr, pnl, pf, dwr, dpnl in all_rows:
            pf_s = f'{pf:.2f}' if pf != float('inf') else ' inf'
            if   wr >= wr_a+3 and pnl >= pnl_a:  verdict = 'Tot hon A'
            elif wr >= wr_a-1:                     verdict = 'Tuong duong'
            else:                                  verdict = 'Kem hon A'
            print(f'  {mode:>5} | {thr:>3} | {n:>4} | {wr:>4.1f}% | {pnl:>+5.2f}% | '
                  f'{pf_s:>5} | {dwr:>+4.1f}% | {verdict}')

    # ── Phan tich theo nam cho mode tot nhat ─────────────────────────────────
    best_mode = 'ASF' if 'ASF' in best else ('AS' if 'AS' in best else 'S')
    best_data = best.get(best_mode)

    if verbose and best_data:
        thr_b, res_b, st_b = best_data
        print(f'\n  THEO NAM: A vs {best_mode}>={thr_b}')
        print(f'  {"Nam":>4} | {"A Lenh":>6} | {"A WR%":>6} | {"A PnL":>6} | '
              f'{best_mode+" L":>7} | {best_mode+" WR":>7} | {best_mode+" PnL":>8} | dWR')
        print(f'  {SEP2}')
        yr_a  = _yearly(res_a['trades'] if res_a else None)
        yr_b  = _yearly(res_b['trades'])
        for yr in sorted(set(list(yr_a.keys()) + list(yr_b.keys()))):
            da = yr_a.get(yr, {}); db = yr_b.get(yr, {})
            phase = MARKET_PHASES.get(yr, '---')[:18]
            flag  = ('v' if db.get('win_rate',0) >= da.get('win_rate',0)+2
                     else ('~' if db.get('win_rate',0) >= da.get('win_rate',0)-1 else 'x'))
            print(f'  {yr:>4} | {da.get("total",0):>6} | {da.get("win_rate",0):>5.1f}% | '
                  f'{da.get("avg_pnl",0):>+5.2f}% | '
                  f'{db.get("total",0):>7} | {db.get("win_rate",0):>6.1f}% | '
                  f'{db.get("avg_pnl",0):>+7.2f}% | {db.get("win_rate",0)-da.get("win_rate",0):>+.1f}% {flag}  {phase}')

    # ── Ket luan ─────────────────────────────────────────────────────────────
    results = {'symbol': symbol, 'has_foreign': has_f,
               'score_a': {'wr':wr_a,'pnl':pnl_a,'pf':pf_a,'n':n_a},
               'best': best, 'all_rows': all_rows}

    verdicts = {}
    for mode, (thr_m, res_m, st_m) in best.items():
        wr_m = st_m.get('win_rate',0); pnl_m = st_m.get('avg_pnl',0); n_m = st_m.get('total',0)
        dwr_m = wr_m - wr_a
        if   dwr_m >= 3 and pnl_m >= pnl_a: v = f'TOT HON A (+{dwr_m:.1f}% WR)'
        elif dwr_m >= 0:                     v = f'TUONG DUONG A ({dwr_m:+.1f}% WR)'
        else:                                v = f'KEM HON A ({dwr_m:+.1f}% WR, {n_m}L < {n_a}L A)'
        verdicts[mode] = v
        results[f'verdict_{mode}'] = v

    if verbose:
        print(f'\n{SEP}')
        print(f'  KET LUAN: {symbol}')
        print(f'{SEP}')
        print(f'  Score A:  {n_a}L | WR={wr_a:.1f}% | PnL={pnl_a:+.2f}%')
        for mode, v in verdicts.items():
            thr_m = best[mode][0]; st_m = best[mode][2]
            print(f'  {mode:>5}>={thr_m}: {st_m.get("total",0)}L | '
                  f'WR={st_m.get("win_rate",0):.1f}% | PnL={st_m.get("avg_pnl",0):+.2f}%'
                  f'  → {v}')

    return results


def run_all(symbols):
    all_res = []
    for i, sym in enumerate(symbols, 1):
        print(f'\n[{i}/{len(symbols)}] {sym}...', flush=True)
        try:
            r = run_shark_report(sym, verbose=True)
            if r: all_res.append(r)
        except Exception as e:
            print(f'  SKIP {sym}: {e}')

    if len(all_res) < 2: return

    print(f'\n\n{"="*72}')
    print(f'  TONG KET SHARK v2 — {len(all_res)} ma')
    print(f'{"="*72}')
    print(f'  {"Ma":>5} | {"A WR":>5} | {"Best Mode":>10} | {"Best WR":>7} | {"dWR":>5} | Ket luan')
    print(f'  {"-"*65}')
    cnt_better = 0
    for r in all_res:
        sym  = r['symbol']; sa = r['score_a']
        best = r.get('best', {})
        if not best: continue
        # Chon mode tot nhat
        bm = sorted(best.keys(), key=lambda m: best[m][2].get('win_rate',0), reverse=True)[0]
        thr_b, _, st_b = best[bm]
        wr_b = st_b.get('win_rate',0); dwr = wr_b - sa['wr']
        if dwr >= 2: cnt_better += 1
        flag = 'TOT HON' if dwr>=2 else ('TUONG DUONG' if dwr>=-1 else 'KEM HON')
        print(f'  {sym:>5} | {sa["wr"]:>4.1f}% | {bm:>4}>={thr_b:<3} | '
              f'{wr_b:>6.1f}% | {dwr:>+4.1f}% | {flag}')

    print(f'\n  Shark giup ich: {cnt_better}/{len(all_res)} ma')
    if   cnt_better >= len(all_res)*0.6: print('  => SHARK SCORE CO GIA TRI TREN TTCK VN')
    elif cnt_better >= len(all_res)*0.3: print('  => SHARK SCORE CO ICH MOT PHAN')
    else:                                print('  => SHARK SCORE CHUA DU TIN CAY TREN DAILY DATA')


if __name__ == '__main__':
    args = sys.argv[1:]
    if not args:
        print('Usage: python backtest_shark.py DGC | DGC MBB | --all')
        symbols = ['DGC', 'MBB']
    elif args[0] == '--all':
        symbols = WATCHLIST_SHARK
    else:
        symbols = [s.upper() for s in args]

    if len(symbols) == 1: run_shark_report(symbols[0], verbose=True)
    else:                  run_all(symbols)
