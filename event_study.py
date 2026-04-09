"""
event_study.py — Standalone Event Study Script (P3)
=====================================================
Mục tiêu: Tìm pattern kỹ thuật xuất hiện TRƯỚC big moves (+15%/18d)
          để làm cơ sở build ML v3 signal rules.

Flow:
  1. Chạy script này LOCAL (không cần Railway/Telegram)
  2. Đọc kết quả CSV + console output
  3. Chọn pattern có ý nghĩa → viết rule cho compute_momentum_score_v3()
  4. Backtest v3 trên bot: /mlbtv3 <symbol>

Cách chạy:
  python event_study.py                    # toàn bộ 10 mã SECTOR_MODE_MAP
  python event_study.py NKG DGC            # chỉ mã chỉ định
  python event_study.py --fail             # mã v2 fail (cập nhật V2_FAIL_SYMBOLS)
  python event_study.py NKG --plot         # thêm chart phân phối features

Anti-hindsight:
  - find_big_moves()             : fwd_return chỉ dùng để LABEL event
  - compute_pre_event_features() : chỉ dùng data df.iloc[:idx+1] (T-1 trở về)
  - compare_event_vs_random()    : contamination zone ±18d quanh event
  - Dual filter                  : p < 0.05 AND ratio >= 2× (bool features)

Output:
  event_study_results/
    summary.csv          — tất cả features, tất cả mã
    <symbol>_events.csv  — danh sách big moves từng mã
    <symbol>_features.csv — feature stats chi tiết từng mã
"""

import sys
import os
import warnings
import numpy as np
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from scipy import stats as _scipy_stats

warnings.filterwarnings('ignore')

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG — chỉnh tại đây trước khi chạy
# ══════════════════════════════════════════════════════════════════════════════

EVENT_BIG_MOVE_PCT  = 0.15   # +15% forward return = "big move"
EVENT_WINDOW_DAYS   = 18     # trong 18 ngày
MIN_PRECISION_RATIO = 2.0    # pattern phải event_rate > 2× random_rate
MIN_EVENT_COUNT     = 5      # bỏ qua mã nếu < 5 big moves
RANDOM_SAMPLE_SIZE  = 500    # số random windows để so sánh
RANDOM_SEED         = 42

DATA_START = '2018-01-01'    # 7 năm: bull + bear + sideways + COVID
DATA_END   = '2025-12-31'

OUTPUT_DIR = 'event_study_results'

# Mã chạy mặc định — 10 mã SECTOR_MODE_MAP
DEFAULT_SYMBOLS = [
    'NKG', 'DGC', 'HPG', 'POW', 'PHR', 'DPR',   # commodity
    'FRT', 'MWG', 'PNJ', 'DGW',                   # retail
]

# Cập nhật sau khi có kết quả /mlbtv2 all
# Ví dụ: V2_FAIL_SYMBOLS = ['NKG', 'HPG', 'DGW']
V2_FAIL_SYMBOLS = ['NKG', 'FRT', 'PNJ']

# ══════════════════════════════════════════════════════════════════════════════
# DATA FETCHER — vnstock
# ══════════════════════════════════════════════════════════════════════════════

def fetch_data(symbol: str, start: str, end: str) -> pd.DataFrame | None:
    """
    Fetch OHLCV từ vnstock (VCI → KBS fallback).
    Normalize tên cột thành: date, open, high, low, close, volume.
    """
    for source in ['VCI', 'KBS']:
        try:
            from vnstock import Vnstock
            df = Vnstock().stock(symbol=symbol, source=source).quote.history(
                start=start, end=end, interval='1D'
            )
            if df is None or len(df) < 100:
                continue

            df = df.copy()
            df.columns = [c.lower() for c in df.columns]

            # Normalize close
            for cname in ('close', 'closeprice', 'close_price'):
                if cname in df.columns:
                    df['close'] = pd.to_numeric(df[cname], errors='coerce').fillna(0)
                    break
            # Normalize volume
            for cname in ('volume', 'volume_match', 'klgd', 'vol'):
                if cname in df.columns:
                    df['volume'] = pd.to_numeric(df[cname], errors='coerce').fillna(0)
                    break
            # Normalize high/low
            for cname in ('high', 'highprice', 'high_price'):
                if cname in df.columns:
                    df['high'] = pd.to_numeric(df[cname], errors='coerce').fillna(0)
                    break
            for cname in ('low', 'lowprice', 'low_price'):
                if cname in df.columns:
                    df['low'] = pd.to_numeric(df[cname], errors='coerce').fillna(0)
                    break
            # Normalize date
            for cname in ('time', 'date', 'datetime', 'trading_date'):
                if cname in df.columns:
                    df['date'] = pd.to_datetime(df[cname])
                    break

            # Giá đơn vị nghìn đồng → đồng
            if df['close'].max() < 1000 and df['close'].max() > 0:
                df['close'] = df['close'] * 1000
                if 'high' in df.columns:
                    df['high'] = df['high'] * 1000
                if 'low' in df.columns:
                    df['low'] = df['low'] * 1000

            required = {'date', 'close', 'volume'}
            if not required.issubset(df.columns):
                continue

            df = df.sort_values('date').reset_index(drop=True)
            print(f'  [{symbol}/{source}] {len(df)} phiên '
                  f'({df["date"].iloc[0].date()} → {df["date"].iloc[-1].date()})')
            return df

        except Exception as e:
            print(f'  [{symbol}/{source}] lỗi: {e}')

    return None


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — FIND BIG MOVES
# ══════════════════════════════════════════════════════════════════════════════

def find_big_moves(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tìm tất cả ngày T mà forward return 18d >= +15%.

    Dedup (option B): sau khi tìm event tại T, skip 18d tiếp theo.
    → Tránh overlapping windows + autocorrelation trong chi-square.
    → "First-win": nếu T=0 và T=3 đều valid, chỉ giữ T=0.

    ANTI-HINDSIGHT: fwd_return chỉ dùng để LABEL event.
                    Không bao giờ đưa vào feature computation.
    """
    df = df.copy().reset_index(drop=True)
    events = []
    i = 0

    while i < len(df) - EVENT_WINDOW_DAYS:
        p0  = float(df.loc[i, 'close'])
        p18 = float(df.loc[i + EVENT_WINDOW_DAYS, 'close'])
        fwd = (p18 - p0) / p0 if p0 > 0 else 0

        if fwd >= EVENT_BIG_MOVE_PCT:
            events.append({
                'date':       df.loc[i, 'date'],
                'idx':        i,
                'close_t0':   round(p0, 0),
                'close_t18':  round(p18, 0),
                'fwd_return': round(fwd, 4),
            })
            i += EVENT_WINDOW_DAYS   # skip — first-win dedup
        else:
            i += 1

    return pd.DataFrame(events) if events else pd.DataFrame(
        columns=['date', 'idx', 'close_t0', 'close_t18', 'fwd_return']
    )


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — COMPUTE PRE-EVENT FEATURES
# ══════════════════════════════════════════════════════════════════════════════

def _rsi(prices: np.ndarray, period: int = 14) -> float:
    if len(prices) < period + 1:
        return np.nan
    d  = np.diff(prices[-(period + 1):])
    ag = np.where(d > 0, d, 0.0).mean()
    al = np.where(d < 0, -d, 0.0).mean()
    if al == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + ag / al)


def _ema(arr: np.ndarray, n: int) -> float:
    if len(arr) < n:
        return np.nan
    k = 2.0 / (n + 1)
    v = float(arr[:n].mean())
    for p in arr[n:]:
        v = float(p) * k + v * (1.0 - k)
    return v


def _slope(y: np.ndarray) -> float:
    """Normalized linear slope — comparable cross-symbol."""
    if len(y) < 2 or np.mean(y) == 0:
        return np.nan
    x = np.arange(len(y), dtype=float)
    s, *_ = _scipy_stats.linregress(x, y / np.mean(y))
    return float(s)


def compute_pre_event_features(df: pd.DataFrame, idx: int,
                                vni_df: pd.DataFrame | None = None) -> dict:
    """
    Tính 17 features tại T-1 (idx), nhìn lại tối đa 252 phiên.

    STRICT NO-LOOKAHEAD: chỉ dùng df.iloc[:idx+1].
    vni_df: DataFrame(date, close) của VNINDEX — đã được align bên ngoài.

    Returns dict features, hoặc {} nếu không đủ data.
    """
    if idx < 22:
        return {}

    window = df.iloc[max(0, idx - 252): idx + 1].copy()
    close  = window['close'].values.astype(float)
    volume = window['volume'].values.astype(float)
    n      = len(close)
    price  = float(close[-1])

    feat = {'idx': idx, 'date': df.loc[idx, 'date']}

    # ── Volume ────────────────────────────────────────────────────────────────
    vol_avg20 = float(volume[-20:].mean()) if n >= 20 else np.nan

    feat['vol_spike_3d'] = (
        float(volume[-3:].mean()) / vol_avg20
        if n >= 3 and vol_avg20 > 0 else np.nan
    )
    feat['vol_trend_10d'] = _slope(volume[-10:]) if n >= 10 else np.nan
    feat['vol_dry_up_5d'] = bool(
        float(volume[-6:-1].mean()) / vol_avg20 < 0.8
        if n >= 6 and vol_avg20 > 0 else False
    )

    # ── Moving Averages ───────────────────────────────────────────────────────
    ma20  = float(close[-20:].mean())  if n >= 20  else np.nan
    ma50  = float(close[-50:].mean())  if n >= 50  else np.nan
    ma200 = float(close[-200:].mean()) if n >= 200 else np.nan

    feat['price_above_ma20']  = bool(price > ma20)  if not np.isnan(ma20)  else False
    feat['price_above_ma50']  = bool(price > ma50)  if not np.isnan(ma50)  else False
    feat['price_above_ma200'] = bool(price > ma200) if not np.isnan(ma200) else False

    # ── 52W High / Low ────────────────────────────────────────────────────────
    high252 = float(close[-252:].max()) if n >= 252 else float(close.max())
    low252  = float(close[-252:].min()) if n >= 252 else float(close.min())
    feat['near_52w_high'] = bool(price >= high252 * 0.95)
    feat['near_52w_low']  = bool(price <= low252  * 1.10)

    # ── Trend consistency ─────────────────────────────────────────────────────
    feat['trend_5d']  = (
        bool(sum(close[-5+i+1] > close[-5+i] for i in range(4)) >= 3)
        if n >= 5 else False
    )
    feat['trend_10d'] = (
        bool(sum(close[-10+i+1] > close[-10+i] for i in range(9)) >= 7)
        if n >= 10 else False
    )

    # ── Inside bar (vol accumulation signal) ──────────────────────────────────
    if 'high' in window.columns and 'low' in window.columns:
        ranges    = (window['high'].values - window['low'].values).astype(float)
        avg_range = float(ranges[-20:].mean()) if len(ranges) >= 20 else float(ranges.mean())
        feat['inside_bar_3d'] = bool(avg_range > 0 and float(ranges[-3:].mean()) < avg_range * 0.7)
    else:
        feat['inside_bar_3d'] = False

    # ── RSI ───────────────────────────────────────────────────────────────────
    rsi_val = _rsi(close)
    feat['rsi_val']            = rsi_val
    feat['rsi_zone_oversold']  = bool(rsi_val < 40)       if not np.isnan(rsi_val) else False
    feat['rsi_zone_neutral']   = bool(40 <= rsi_val <= 60) if not np.isnan(rsi_val) else False
    feat['rsi_zone_overbought']= bool(rsi_val > 60)        if not np.isnan(rsi_val) else False

    # RSI rising 5d — tính RSI tại 5 điểm gần nhất
    if n >= 20:
        rsi_pts = [_rsi(close[:n - 4 + j]) for j in range(5)]
        valid   = [r for r in rsi_pts if not np.isnan(r)]
        feat['rsi_rising_5d'] = bool(
            len(valid) == 5 and all(valid[k + 1] > valid[k] for k in range(4))
        )
    else:
        feat['rsi_rising_5d'] = False

    # ── MACD cross up (trong 3 phiên qua) ────────────────────────────────────
    if n >= 35:
        def _macd_above(c_arr):
            m = _ema(c_arr, 12) - _ema(c_arr, 26)
            s = _ema(c_arr[-9:], 9) if len(c_arr) >= 9 else np.nan
            return m, s

        m0, s0 = _macd_above(close)
        m2, s2 = _macd_above(close[:-2])
        feat['macd_cross'] = bool(
            not any(np.isnan(v) for v in [m0, s0, m2, s2]) and
            m0 > s0 and m2 < s2   # cross up trong 3 phiên
        )
    else:
        feat['macd_cross'] = False

    # ── RS vs VNINDEX ─────────────────────────────────────────────────────────
    _rs_null = {
        'rs_vs_vni_5d': np.nan, 'rs_vs_vni_20d': np.nan,
        'rs_improving': False,  'rs_outperform_both': False,
    }

    if vni_df is not None and len(vni_df) > 0:
        cur_date = pd.to_datetime(df.loc[idx, 'date'])
        vni_sub  = vni_df[pd.to_datetime(vni_df['date']) <= cur_date]

        if len(vni_sub) >= 21:
            vc = vni_sub['close'].values.astype(float)

            stk5  = (close[-1] / close[-6]  - 1) if n >= 6  else np.nan
            vni5  = (vc[-1]   / vc[-6]      - 1) if len(vc) >= 6  else np.nan
            stk20 = (close[-1] / close[-21] - 1) if n >= 21 else np.nan
            vni20 = (vc[-1]   / vc[-21]     - 1) if len(vc) >= 21 else np.nan

            rs5  = stk5  - vni5  if not (np.isnan(stk5)  or np.isnan(vni5))  else np.nan
            rs20 = stk20 - vni20 if not (np.isnan(stk20) or np.isnan(vni20)) else np.nan

            feat['rs_vs_vni_5d']  = rs5
            feat['rs_vs_vni_20d'] = rs20

            # RS improving: rs5 hôm nay > rs5 cách đây 5 phiên
            if n >= 11 and len(vc) >= 11:
                rs5p = (close[-6] / close[-11] - 1) - (vc[-6] / vc[-11] - 1)
                feat['rs_improving'] = bool(
                    not np.isnan(rs5) and not np.isnan(rs5p) and rs5 > rs5p
                )
            else:
                feat['rs_improving'] = False

            feat['rs_outperform_both'] = bool(
                not np.isnan(rs5) and rs5 > 0 and
                not np.isnan(rs20) and rs20 > 0
            )
        else:
            feat.update(_rs_null)
    else:
        feat.update(_rs_null)

    return feat


# Danh sách features phân loại (để dùng trong compare và summary)
BOOL_FEATURES = [
    'vol_dry_up_5d',
    'price_above_ma20', 'price_above_ma50', 'price_above_ma200',
    'near_52w_high', 'near_52w_low',
    'inside_bar_3d',
    'trend_5d', 'trend_10d',
    'rsi_zone_oversold', 'rsi_zone_neutral', 'rsi_zone_overbought',
    'rsi_rising_5d', 'macd_cross',
    'rs_improving', 'rs_outperform_both',
]
NUMERIC_FEATURES = [
    'vol_spike_3d', 'vol_trend_10d',
    'rs_vs_vni_5d', 'rs_vs_vni_20d',
    'rsi_val',
]


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — COMPARE EVENT VS RANDOM
# ══════════════════════════════════════════════════════════════════════════════

def compare_event_vs_random(df: pd.DataFrame, events: pd.DataFrame,
                             vni_df: pd.DataFrame | None = None,
                             n_random: int = RANDOM_SAMPLE_SIZE,
                             seed: int = RANDOM_SEED) -> pd.DataFrame:
    """
    So sánh feature distribution:
      EVENT   → T-1 của mỗi big move
      RANDOM  → ngày ngẫu nhiên, loại event ± 18d (contamination zone)

    Dual filter chống multiple testing false positives:
      Boolean  → chi2_contingency p < 0.05  AND  ratio >= MIN_PRECISION_RATIO
      Numeric  → Mann-Whitney U p < 0.05    AND  direction rõ ràng (event > random)

    Trả về DataFrame đã sort: valid=True trước, ratio giảm dần.
    """
    np.random.seed(seed)

    valid_range = list(range(252, len(df) - EVENT_WINDOW_DAYS))
    event_idxs  = set(events['idx'].tolist())

    # Contamination zone ± EVENT_WINDOW_DAYS quanh mỗi event
    contaminated = set()
    for ei in event_idxs:
        for off in range(-EVENT_WINDOW_DAYS, EVENT_WINDOW_DAYS + 1):
            contaminated.add(ei + off)

    non_event = [i for i in valid_range if i not in contaminated]
    sample_n  = min(n_random, len(non_event))

    if sample_n == 0:
        print('  WARNING: Không đủ random windows!')
        return pd.DataFrame()

    rand_idxs = np.random.choice(non_event, size=sample_n, replace=False).tolist()

    # Compute features cho event và random windows
    ev_feats  = [compute_pre_event_features(df, i, vni_df) for i in events['idx']]
    rnd_feats = [compute_pre_event_features(df, i, vni_df) for i in rand_idxs]

    ev_df  = pd.DataFrame([f for f in ev_feats  if f])
    rnd_df = pd.DataFrame([f for f in rnd_feats if f])

    results = []

    # ── Boolean features — chi-square test ───────────────────────────────────
    for feat in BOOL_FEATURES:
        if feat not in ev_df.columns:
            continue

        e_col = ev_df[feat].fillna(False).astype(bool)
        r_col = (rnd_df[feat].fillna(False).astype(bool)
                 if feat in rnd_df.columns
                 else pd.Series([False] * len(rnd_df)))

        e_rate = float(e_col.mean())
        r_rate = float(r_col.mean())
        ratio  = e_rate / r_rate if r_rate > 1e-9 else np.nan

        try:
            ct = np.array([
                [int(e_col.sum()), len(e_col) - int(e_col.sum())],
                [int(r_col.sum()), len(r_col) - int(r_col.sum())],
            ])
            _, pval, _, _ = _scipy_stats.chi2_contingency(ct)
        except Exception:
            pval = np.nan

        valid = (
            not np.isnan(ratio) and ratio >= MIN_PRECISION_RATIO and
            not np.isnan(pval)  and pval < 0.05
        )
        results.append({
            'feature':         feat,
            'type':            'bool',
            'n_event':         len(e_col),
            'n_random':        len(r_col),
            'event_rate':      round(e_rate, 3),
            'random_rate':     round(r_rate, 3),
            'event_mean':      None,
            'random_mean':     None,
            'precision_ratio': round(ratio, 2) if not np.isnan(ratio) else None,
            'pvalue':          round(pval, 4)  if not np.isnan(pval)  else None,
            'valid':           valid,
            'note':            '✅ VALID' if valid else '',
        })

    # ── Numeric features — Mann-Whitney U ────────────────────────────────────
    for feat in NUMERIC_FEATURES:
        if feat not in ev_df.columns:
            continue

        e_vals = ev_df[feat].dropna()
        r_vals = (rnd_df[feat].dropna() if feat in rnd_df.columns
                  else pd.Series(dtype=float))

        if len(e_vals) < 3 or len(r_vals) < 3:
            continue

        _, pval = _scipy_stats.mannwhitneyu(e_vals, r_vals, alternative='two-sided')
        e_mean  = float(e_vals.mean())
        r_mean  = float(r_vals.mean())

        results.append({
            'feature':         feat,
            'type':            'numeric',
            'n_event':         len(e_vals),
            'n_random':        len(r_vals),
            'event_rate':      None,
            'random_rate':     None,
            'event_mean':      round(e_mean, 3),
            'random_mean':     round(r_mean, 3),
            'precision_ratio': None,
            'pvalue':          round(float(pval), 4),
            'valid':           pval < 0.05,
            'note':            '✅ VALID' if pval < 0.05 else '',
        })

    out = pd.DataFrame(results)
    if len(out) > 0:
        out = out.sort_values(
            ['valid', 'precision_ratio'], ascending=[False, False]
        ).reset_index(drop=True)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — PIPELINE PER MÃ
# ══════════════════════════════════════════════════════════════════════════════

def analyze_symbol(symbol: str, vni_df: pd.DataFrame | None = None) -> dict:
    """
    Pipeline hoàn chỉnh cho 1 mã:
      fetch → find_big_moves → compute features → compare vs random → summary
    """
    print(f'\n{"="*60}')
    print(f'  {symbol} — fetch data {DATA_START[:4]}–{DATA_END[:4]}')
    print(f'{"="*60}')

    df = fetch_data(symbol, DATA_START, DATA_END)
    if df is None:
        return {'symbol': symbol, 'error': 'Không fetch được data'}

    events    = find_big_moves(df)
    n_total   = max(1, len(df) - EVENT_WINDOW_DAYS - 252)
    n_events  = len(events)
    base_rate = n_events / n_total

    print(f'  Tổng phiên: {len(df)} | Big moves tìm được: {n_events} '
          f'| Base rate: {base_rate:.1%}')

    if n_events > 0:
        print(f'  Avg fwd return: {events["fwd_return"].mean():.1%} '
              f'| Max: {events["fwd_return"].max():.1%}')

    if n_events < MIN_EVENT_COUNT:
        msg = f'Chỉ có {n_events} big moves — không đủ (min={MIN_EVENT_COUNT})'
        print(f'  ⚠ {msg}')
        return {
            'symbol': symbol, 'n_events': n_events,
            'base_rate': base_rate, 'warning': msg,
            'events': events, 'stats': pd.DataFrame(),
        }

    stats_df       = compare_event_vs_random(df, events, vni_df)
    valid_patterns = stats_df[stats_df['valid']].to_dict('records') if len(stats_df) > 0 else []

    print(f'\n  Pattern hợp lệ (ratio≥{MIN_PRECISION_RATIO}× & p<0.05): '
          f'{len(valid_patterns)}/{len(stats_df)}')

    if valid_patterns:
        print(f'  {"Feature":<25} {"Type":<8} {"EventRate/Mean":>14} '
              f'{"RndRate/Mean":>12} {"Ratio":>7} {"p-value":>8}')
        print(f'  {"-"*75}')
        for p in valid_patterns:
            if p['type'] == 'bool':
                print(f'  ✅ {p["feature"]:<23} bool  '
                      f'{p["event_rate"]:>13.1%} {p["random_rate"]:>11.1%} '
                      f'{p["precision_ratio"]:>6.1f}× {p["pvalue"]:>8.4f}')
            else:
                print(f'  ✅ {p["feature"]:<23} num   '
                      f'{p["event_mean"]:>13.3f} {p["random_mean"]:>11.3f} '
                      f'{"—":>7} {p["pvalue"]:>8.4f}')
    else:
        print('  ⬜ Không tìm được pattern có ý nghĩa thống kê')

    return {
        'symbol':         symbol,
        'n_events':       n_events,
        'base_rate':      base_rate,
        'avg_fwd_return': float(events['fwd_return'].mean()),
        'max_fwd_return': float(events['fwd_return'].max()),
        'events':         events,
        'stats':          stats_df,
        'valid_patterns': valid_patterns,
    }


# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — RUN ALL + SAVE OUTPUT
# ══════════════════════════════════════════════════════════════════════════════

def run_all(symbols: list) -> dict:
    """
    Chạy event study cho list mã.
    VNI fetch 1 lần duy nhất → truyền vào tất cả analyze_symbol().
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Fetch VNI 1 lần
    print('\n>>> Fetching VNINDEX...')
    vni_df = None
    try:
        vni_raw = fetch_data('VNINDEX', DATA_START, DATA_END)
        if vni_raw is not None:
            vni_df = vni_raw[['date', 'close']].copy()
            vni_df['date'] = pd.to_datetime(vni_df['date'])
            vni_df = vni_df.sort_values('date').reset_index(drop=True)
            print(f'  VNINDEX: {len(vni_df)} phiên OK')
        else:
            print('  WARNING: Không lấy được VNINDEX — RS features sẽ bị skip')
    except Exception as e:
        print(f'  WARNING: VNINDEX fetch failed: {e}')

    # Phân tích từng mã (sequential để dễ debug + tránh rate limit)
    all_results = {}
    for sym in symbols:
        result = analyze_symbol(sym, vni_df)
        all_results[sym] = result

    # ── Save output ───────────────────────────────────────────────────────────
    save_outputs(all_results)

    return all_results


def save_outputs(results: dict):
    """Lưu kết quả ra CSV để phân tích thủ công."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M')

    # 1. summary.csv — tổng hợp tất cả mã × tất cả features
    summary_rows = []
    for sym, r in results.items():
        if 'error' in r or 'warning' in r or 'stats' not in r or len(r['stats']) == 0:
            continue
        df_stats = r['stats'].copy()
        df_stats.insert(0, 'symbol', sym)
        df_stats.insert(1, 'n_events', r['n_events'])
        df_stats.insert(2, 'base_rate', round(r['base_rate'], 4))
        df_stats.insert(3, 'avg_fwd_return', round(r.get('avg_fwd_return', 0), 4))
        summary_rows.append(df_stats)

    if summary_rows:
        summary_df = pd.concat(summary_rows, ignore_index=True)
        summary_path = os.path.join(OUTPUT_DIR, f'summary_{ts}.csv')
        summary_df.to_csv(summary_path, index=False, encoding='utf-8-sig')
        print(f'\n>>> Saved: {summary_path}')

    # 2. Per-mã: events + feature stats
    for sym, r in results.items():
        if 'error' in r or 'events' not in r:
            continue

        # events CSV
        if len(r['events']) > 0:
            ev_path = os.path.join(OUTPUT_DIR, f'{sym}_events_{ts}.csv')
            r['events'].to_csv(ev_path, index=False, encoding='utf-8-sig')

        # feature stats CSV
        if 'stats' in r and len(r['stats']) > 0:
            ft_path = os.path.join(OUTPUT_DIR, f'{sym}_features_{ts}.csv')
            r['stats'].to_csv(ft_path, index=False, encoding='utf-8-sig')

    print(f'>>> Output directory: {os.path.abspath(OUTPUT_DIR)}/')


# ══════════════════════════════════════════════════════════════════════════════
# STEP 6 — PRINT FINAL SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

def print_summary(results: dict):
    """
    In bảng tóm tắt cuối:
      - Mã nào có pattern hợp lệ
      - Pattern nào xuất hiện nhiều nhất (cross-symbol)
      - Gợi ý rule cho ML v3
    """
    print(f'\n{"="*70}')
    print('  FINAL SUMMARY — EVENT STUDY')
    print(f'  Big move: +{EVENT_BIG_MOVE_PCT:.0%}/{EVENT_WINDOW_DAYS}d | '
          f'Data: {DATA_START[:4]}–{DATA_END[:4]}')
    print(f'  Threshold: ratio ≥ {MIN_PRECISION_RATIO}× AND p < 0.05')
    print(f'{"="*70}')

    pattern_counter = {}   # feature → count mã có pattern hợp lệ

    for sym, r in results.items():
        if 'error' in r:
            print(f'  ❌ {sym}: {r["error"]}')
            continue
        if 'warning' in r:
            print(f'  ⚠  {sym}: {r["warning"]} (base={r["base_rate"]:.1%})')
            continue

        n_valid = len(r.get('valid_patterns', []))
        avg_fwd = r.get('avg_fwd_return', 0)
        status  = '✅' if n_valid > 0 else '⬜'

        print(f'  {status} {sym:<6} | {r["n_events"]:2d} events '
              f'| base={r["base_rate"]:.1%} '
              f'| avg_fwd={avg_fwd:.1%} '
              f'| {n_valid} pattern(s) valid')

        for p in r.get('valid_patterns', []):
            feat = p['feature']
            pattern_counter[feat] = pattern_counter.get(feat, 0) + 1

    # Cross-symbol patterns (xuất hiện ở ≥ 2 mã)
    cross = sorted(
        [(cnt, feat) for feat, cnt in pattern_counter.items() if cnt >= 2],
        reverse=True
    )
    if cross:
        print(f'\n  ── Cross-symbol patterns (≥ 2 mã) ──────────────────────────')
        for cnt, feat in cross:
            syms_with = [
                s for s, r in results.items()
                if any(p['feature'] == feat for p in r.get('valid_patterns', []))
            ]
            print(f'  {cnt}× {feat:<30} → {", ".join(syms_with)}')

    # Gợi ý bước tiếp theo
    print(f'\n  ── Bước tiếp theo ───────────────────────────────────────────────')
    print('  1. Đọc CSV trong event_study_results/ để xem full stats')
    print('  2. Chọn pattern có ý nghĩa kinh tế rõ ràng (tránh spurious)')
    print('  3. Viết compute_momentum_score_v3() trong backtest.py')
    print('  4. Backtest: /mlbtv3 <symbol> trên bot')
    print(f'{"="*70}\n')


# ══════════════════════════════════════════════════════════════════════════════
# OPTIONAL — PLOT (yêu cầu matplotlib)
# ══════════════════════════════════════════════════════════════════════════════

def plot_feature_distributions(symbol: str, result: dict):
    """
    Vẽ histogram so sánh feature distribution: event vs random.
    Chỉ vẽ numeric features (vol_spike_3d, rs_vs_vni_5d, rsi_val...).
    Yêu cầu: pip install matplotlib
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print('matplotlib chưa cài — bỏ qua plot. pip install matplotlib')
        return

    if 'stats' not in result or len(result['stats']) == 0:
        return

    numeric_valid = result['stats'][
        (result['stats']['type'] == 'numeric') &
        (result['stats']['valid'])
    ]['feature'].tolist()

    if not numeric_valid:
        print(f'  [{symbol}] Không có numeric feature hợp lệ để plot')
        return

    fig, axes = plt.subplots(1, len(numeric_valid),
                              figsize=(5 * len(numeric_valid), 4))
    if len(numeric_valid) == 1:
        axes = [axes]

    fig.suptitle(f'{symbol} — Feature Distribution: Event vs Random', fontsize=13)

    for ax, feat in zip(axes, numeric_valid):
        # Recompute distributions từ events và random (không lưu lại trong result)
        # Đây là approximation dùng stats đã tính
        row = result['stats'][result['stats']['feature'] == feat].iloc[0]
        ax.bar(['Event', 'Random'],
               [row['event_mean'], row['random_mean']],
               color=['#2ecc71', '#95a5a6'])
        ax.set_title(f'{feat}\np={row["pvalue"]:.3f}')
        ax.set_ylabel('Mean value')

    plt.tight_layout()
    out_path = os.path.join(OUTPUT_DIR, f'{symbol}_plot.png')
    plt.savefig(out_path, dpi=120, bbox_inches='tight')
    plt.close()
    print(f'  Plot saved: {out_path}')


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    args   = sys.argv[1:]
    do_plot = '--plot' in args
    args    = [a for a in args if a != '--plot']

    # Xác định danh sách mã
    if '--fail' in args:
        if not V2_FAIL_SYMBOLS:
            print('⚠ V2_FAIL_SYMBOLS chưa được điền.')
            print('  Cập nhật V2_FAIL_SYMBOLS trong script sau khi có kết quả /mlbtv2 all')
            sys.exit(1)
        symbols = V2_FAIL_SYMBOLS
        print(f'>>> Chế độ: mã v2 fail — {symbols}')
    elif args:
        symbols = [a.upper() for a in args if not a.startswith('--')]
        print(f'>>> Chế độ: mã chỉ định — {symbols}')
    else:
        symbols = DEFAULT_SYMBOLS
        print(f'>>> Chế độ: toàn bộ {len(symbols)} mã mặc định')

    print(f'>>> Big move: +{EVENT_BIG_MOVE_PCT:.0%} / {EVENT_WINDOW_DAYS}d')
    print(f'>>> Data: {DATA_START} → {DATA_END}')
    print(f'>>> Output: {os.path.abspath(OUTPUT_DIR)}/')

    results = run_all(symbols)
    print_summary(results)

    # Optional plot
    if do_plot:
        for sym, r in results.items():
            if 'valid_patterns' in r and r['valid_patterns']:
                plot_feature_distributions(sym, r)
