"""
analyze_score_a_bt.py — Chạy trực tiếp trên Railway server
============================================================
Việc 1: B-filter simulation — So sánh Score A vs Score A+context filters
Việc 2: Score A breakdown theo context (VNI / Vol / MA20 / Score bucket)

Chạy: python3 analyze_score_a_bt.py
Output: in ra terminal (có thể pipe vào file)
"""

import sys, os
import numpy as np
import pandas as pd

# Railway có thư mục bot, import trực tiếp
try:
    import backtest as bt
    import config as cfg_mod
    print("✅ Import OK")
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Chạy script này trong cùng thư mục với backtest.py và config.py")
    sys.exit(1)

# ── Tham số ───────────────────────────────────────────────────────────────────
WATCHLIST = [
    'BSR','BSI','ORS','PDR','CTS','PC1','CNG','GAS','DPM','DVP','FPT','HMC',
    'MCH','HBC','VIC','AGG','VCB','CSV','CTG','FRT','VTP','DCM','PHP','DXS'
]
MIN_N = 15  # sample tối thiểu

# ── Classify helpers ──────────────────────────────────────────────────────────
def classify_vni(vni_slope):
    """VNI_slope = % change 10 bars tại thời điểm entry."""
    if vni_slope >= 1.0:  return 'UP'
    if vni_slope <= -2.0: return 'DOWN'
    return 'FLAT'

def classify_vol(vol_ratio):
    if vol_ratio >= 2.0:  return 'HIGH'
    if vol_ratio >= 1.2:  return 'MED'
    if vol_ratio >= 0.8:  return 'NORMAL'
    return 'LOW'

def classify_ma20(ma20_dist):
    if ma20_dist < 0:    return 'BELOW'
    if ma20_dist < 2:    return 'NEAR'
    if ma20_dist < 10:   return 'OPT'
    if ma20_dist < 20:   return 'EXT'
    return 'FAR'

def classify_score(score):
    if score < 75:  return '65-74'
    if score < 85:  return '75-84'
    if score < 95:  return '85-94'
    return '95+'

def stats(df):
    if len(df) == 0: return None
    wr  = (df['pnl'] > 0).mean() * 100
    exp = df['pnl'].mean()
    wins  = df[df['pnl'] > 0]['pnl']
    losss = df[df['pnl'] < 0]['pnl']
    pf = (wins.sum() / abs(losss.sum())) if len(losss) > 0 and losss.sum() != 0 else 99.0
    return {'n': len(df), 'wr': round(wr,1), 'exp': round(exp,3), 'pf': round(min(pf,99),2)}

def fmt(s, baseline_exp=None):
    if s is None: return f'{"—":>5} {"—":>6} {"—":>8} {"—":>6}  —'
    if s['n'] < MIN_N:
        return f'{s["n"]:>5} {"—":>6} {"—":>8} {"—":>6}  ⚠ n<{MIN_N}'
    icon = '✅' if s['exp'] >= 0.5 else ('❌' if s['exp'] < -0.5 else ('🟡' if s['exp'] < -0.15 else '➡'))
    delta = f' (Δ{s["exp"]-baseline_exp:+.3f}%)' if baseline_exp is not None else ''
    return f'{s["n"]:>5} {s["wr"]:>5.1f}% {s["exp"]:>+7.3f}% {s["pf"]:>6.2f}  {icon}{delta}'

# ── Load data ─────────────────────────────────────────────────────────────────
print('\nLoading backtest data (có thể mất 3-5 phút)...')
all_trades = []
failed = []

for sym in WATCHLIST:
    try:
        cfg = bt.SYMBOL_CONFIG.get(sym, {})
        result = bt.run_backtest_symbol(
            sym, verbose=False,
            entry_mode='T+1',
            use_regime=cfg.get('use_regime', True),
            use_vwap=True,
        )
        if result is None:
            failed.append(sym); continue
        df_t = pd.DataFrame(result['trades'])
        buy_df = df_t[df_t['action'] == 'MUA'].copy()
        if buy_df.empty:
            failed.append(sym); continue
        buy_df['sym'] = sym
        all_trades.append(buy_df)
        s = stats(buy_df)
        print(f'  ✅ {sym}: {s["n"]}L WR={s["wr"]}% Exp={s["exp"]:+.3f}%')
    except Exception as e:
        failed.append(sym)
        print(f'  ❌ {sym}: {e}')

if not all_trades:
    print('Không load được data.'); sys.exit(1)

df = pd.concat(all_trades, ignore_index=True)
if failed: print(f'\nFailed: {failed}')
print(f'\nTotal: {len(df)}L | {len(all_trades)} mã')

# Classify
df['vni_ctx']   = df['vni_slope'].apply(classify_vni)
df['vol_ctx']   = df['vol_ratio'].apply(classify_vol)
df['ma20_ctx']  = df['ma20_dist'].apply(classify_ma20)
df['score_ctx'] = df['score'].apply(classify_score)

base = stats(df)
print(f'\nBaseline: n={base["n"]}L WR={base["wr"]}% Exp={base["exp"]:+.3f}% PF={base["pf"]}')

# ─────────────────────────────────────────────────────────────────────────────
# VIỆC 2: CONTEXT BREAKDOWN
# ─────────────────────────────────────────────────────────────────────────────
print('\n' + '='*72)
print('SCORE A CONTEXT BREAKDOWN')
print('='*72)

sections = [
    ('C1 — VNI Context (vni_slope %change 10 bars)', 'vni_ctx',  ['UP','FLAT','DOWN']),
    ('C2 — Volume Pattern',                           'vol_ctx',  ['HIGH','MED','NORMAL','LOW']),
    ('C3 — Score Bucket',                             'score_ctx',['65-74','75-84','85-94','95+']),
    ('C4 — MA20 Distance',                            'ma20_ctx', ['BELOW','NEAR','OPT','EXT','FAR']),
]

bad_buckets = []

for title, col, order in sections:
    print(f'\n{"─"*72}')
    print(f'  {title}')
    print(f'  {"Bucket":<10} {"n":>5} {"WR%":>6} {"Exp%":>9} {"PF":>6}  Note')
    print(f'  {"─"*62}')
    for bucket in order:
        sub = df[df[col] == bucket]
        s = stats(sub)
        if s is None: print(f'  {bucket:<10}  (no data)'); continue
        row = fmt(s, base['exp'])
        print(f'  {bucket:<10} {row}')
        if s['n'] >= MIN_N and s['exp'] < -0.5:
            bad_buckets.append((col, bucket, s))

# ─────────────────────────────────────────────────────────────────────────────
# VIỆC 1: B-FILTER SIMULATION
# ─────────────────────────────────────────────────────────────────────────────
print('\n' + '='*72)
print('VIỆC 1 — B-FILTER SIMULATION')
print('So sánh performance khi loại dần các context xấu')
print('='*72)

# Xác định tất cả buckets âm rõ từ analysis trên
bad_vni  = df['vni_ctx'] == 'DOWN'
bad_vol  = df['vol_ctx'] == 'LOW'
bad_ma20 = df['ma20_ctx'].isin(['EXT', 'FAR'])
bad_scr  = df['score_ctx'] == '65-74'

filters = [
    ('Baseline (không filter)',           pd.Series([True]*len(df),  index=df.index)),
    ('Loại VNI DOWN',                     ~bad_vni),
    ('Loại VNI DOWN + Vol LOW',           ~bad_vni & ~bad_vol),
    ('Loại DOWN + LOW + MA20 EXT/FAR',    ~bad_vni & ~bad_vol & ~bad_ma20),
    ('Loại DOWN + LOW + EXT + Score<75',  ~bad_vni & ~bad_vol & ~bad_ma20 & ~bad_scr),
    ('Loại tất cả bucket Exp<-0.5%',      ~(bad_vni | bad_vol | bad_ma20 | bad_scr)),
]

print(f'\n  {"Filter":<40} {"n":>5} {"WR%":>6} {"Exp%":>9} {"PF":>6}  {"ΔExp":>7}  {"ΔN"}')
print(f'  {"─"*80}')

n_base = len(df)
for label, mask in filters:
    sub = df[mask]
    s = stats(sub)
    if s is None: continue
    d_exp  = s['exp'] - base['exp']
    d_n    = len(sub) - n_base
    icon   = '✅' if d_exp >= 0.3 else ('🟡' if d_exp >= 0 else '❌')
    delta_n_str = f'{d_n:+d}L'
    print(f'  {label:<40} {s["n"]:>5} {s["wr"]:>5.1f}% {s["exp"]:>+7.3f}% '
          f'{s["pf"]:>6.2f}  {d_exp:>+6.3f}%  {icon}  {delta_n_str}')

# ─────────────────────────────────────────────────────────────────────────────
# CROSS MATRIX: VNI × Vol
# ─────────────────────────────────────────────────────────────────────────────
print(f'\n{"─"*72}')
print('  CROSS: VNI × Vol (Exp%)')
print(f'  {"VNI\\Vol":<8}', end='')
for v in ['HIGH','MED','NORMAL','LOW']:
    print(f'  {v:>10}', end='')
print()
print(f'  {"─"*55}')
for vni in ['UP','FLAT','DOWN']:
    print(f'  {vni:<8}', end='')
    for vol in ['HIGH','MED','NORMAL','LOW']:
        sub = df[(df['vni_ctx']==vni) & (df['vol_ctx']==vol)]
        s = stats(sub)
        if s is None or s['n'] < MIN_N:
            print(f'  {"n<15":>10}', end='')
        else:
            icon = '✅' if s['exp'] >= 0.5 else ('❌' if s['exp'] < -0.5 else '➡')
            print(f'  {icon}{s["exp"]:>+5.2f}%({s["n"]})', end='')
    print()

# ─────────────────────────────────────────────────────────────────────────────
# TÓM TẮT
# ─────────────────────────────────────────────────────────────────────────────
print(f'\n{"="*72}')
print('TÓM TẮT')
print(f'{"="*72}')
if bad_buckets:
    print('\nBuckets âm rõ (Exp < -0.5%, n≥15L) — ứng viên cho B-filter mới:')
    for col, bucket, s in bad_buckets:
        print(f'  ❌ {col}={bucket}: Exp={s["exp"]:+.3f}% WR={s["wr"]}% n={s["n"]}L')
else:
    print('\n✅ Không có bucket nào âm rõ ở level aggregate.')

# Best filter version
best = max(
    [(label, stats(df[mask])) for label, mask in filters[1:]],
    key=lambda x: x[1]['exp'] if x[1] else -99
)
print(f'\nFilter hiệu quả nhất: "{best[0]}"')
b2 = best[1]
print(f'  Exp={b2["exp"]:+.3f}% (+{b2["exp"]-base["exp"]:.3f}%) | WR={b2["wr"]}% | n={b2["n"]}L')

verdict_delta = b2['exp'] - base['exp']
if verdict_delta >= 0.5:
    print('\n→ ✅ B-filter CÓ GIÁ TRỊ RÕ RÀNG — Nên implement vào backtest')
elif verdict_delta >= 0.2:
    print('\n→ 🟡 B-filter có tác dụng nhẹ — Xem xét implement')
else:
    print('\n→ ❌ B-filter không cải thiện đáng kể — Review lại logic filter')

print('\nDone.')
