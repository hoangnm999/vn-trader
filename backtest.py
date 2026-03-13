"""
backtest.py - Công cụ kiểm chứng tín hiệu VN Trader Bot
=========================================================
Chạy 1 lần để ra kết quả toàn bộ 28 mã watchlist:
    python backtest.py

Hoặc test riêng lẻ:
    python backtest.py VCB
    python backtest.py VCB HPG FPT
    python backtest.py --detail VCB HPG   (in chi tiết từng mã)

Phân tích 3 chiều:
  Chiều 1 — Breadth  : Toàn bộ 28 mã, xem hệ thống hoạt động tốt không
  Chiều 2 — Time     : Tách kết quả theo từng năm, phát hiện overfitting bull market
  Chiều 3 — Threshold: Tìm ngưỡng score MUA tối ưu (65 / 70 / 75 / 80)

Cấu hình: 5 năm | SL=-7% | TP=+14% | Giữ tối đa 10 phiên
"""

import sys
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ═══════════════════════════════════════════════════════════════════════════════
# CẤU HÌNH
# ═══════════════════════════════════════════════════════════════════════════════
HOLD_DAYS      = 10      # Giữ tối đa N phiên
STOP_LOSS      = -0.07   # Cắt lỗ -7%
TAKE_PROFIT    = 0.14    # Chốt lời +14%
MIN_SCORE_BUY  = 65      # Ngưỡng MUA mặc định
MAX_SCORE_SELL = 35      # Ngưỡng BAN
LOOKBACK_DAYS  = 1825    # 5 năm (5 x 365)

# 28 mã watchlist
WATCHLIST = [
    'VCB', 'BID', 'TCB', 'MBB', 'VPB',   # Ngân hàng
    'VHM', 'VIC', 'NVL', 'PDR',           # Bất động sản
    'FPT', 'CMG',                          # Công nghệ
    'HPG', 'HSG', 'NKG',                   # Thép
    'SSI', 'VND', 'HCM',                   # Chứng khoán
    'GAS', 'PVD', 'PVS',                   # Dầu khí
    'MWG', 'FRT',                          # Bán lẻ
    'VNM', 'MSN',                          # Thực phẩm
    'POW', 'REE',                          # Điện
    'KBC', 'SZC',                          # BĐS Khu công nghiệp
]

SECTOR_MAP = {
    'VCB':'Ngan hang','BID':'Ngan hang','TCB':'Ngan hang',
    'MBB':'Ngan hang','VPB':'Ngan hang',
    'VHM':'Bat dong san','VIC':'Bat dong san',
    'NVL':'Bat dong san','PDR':'Bat dong san',
    'FPT':'Cong nghe','CMG':'Cong nghe',
    'HPG':'Thep','HSG':'Thep','NKG':'Thep',
    'SSI':'Chung khoan','VND':'Chung khoan','HCM':'Chung khoan',
    'GAS':'Dau khi','PVD':'Dau khi','PVS':'Dau khi',
    'MWG':'Ban le','FRT':'Ban le',
    'VNM':'Thuc pham','MSN':'Thuc pham',
    'POW':'Dien','REE':'Dien',
    'KBC':'KCN','SZC':'KCN',
}


# ═══════════════════════════════════════════════════════════════════════════════
# CHI SO KY THUAT (mirror tu app.py)
# ═══════════════════════════════════════════════════════════════════════════════

def find_col(df, names):
    for c in df.columns:
        if c.lower() in names:
            return c
    return None


def ema_arr(arr, span):
    alpha = 2.0 / (span + 1)
    out = np.zeros(len(arr))
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = alpha * arr[i] + (1 - alpha) * out[i - 1]
    return out


def calc_rsi_wilder(arr, p=14):
    out = np.full(len(arr), 50.0)
    if len(arr) < p + 1:
        return out
    deltas = np.diff(arr)
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = np.mean(gains[:p])
    avg_loss = np.mean(losses[:p])
    out[p] = 100.0 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)
    for i in range(p, len(deltas)):
        avg_gain = (avg_gain * (p - 1) + gains[i]) / p
        avg_loss = (avg_loss * (p - 1) + losses[i]) / p
        out[i + 1] = 100.0 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)
    return np.round(out, 1)


def compute_score_at(closes, highs, lows, volumes, idx):
    """Tinh score tai idx — khong nhin truoc du lieu tuong lai."""
    if idx < 52:
        return 50, 'THEO DOI'

    c = closes[:idx + 1]
    h = highs[:idx + 1]
    l = lows[:idx + 1]
    v = volumes[:idx + 1]

    price      = float(c[-1])
    prev_close = float(c[-2]) if len(c) > 1 else price

    rsi_series = calc_rsi_wilder(c)
    rsi_val    = float(rsi_series[-1])

    e12       = ema_arr(c, 12)
    e26       = ema_arr(c, 26)
    macd_line = e12 - e26
    sig_line  = ema_arr(macd_line, 9)
    macd_h    = float((macd_line - sig_line)[-1])
    macd_v    = float(macd_line[-1])
    macd_s    = float(sig_line[-1])

    ma20      = float(np.mean(c[-20:]))
    ma50      = float(np.mean(c[-min(50, len(c)):]))
    ma20_prev = float(np.mean(c[-21:-1])) if len(c) >= 21 else ma20
    ma50_prev = float(np.mean(c[-51:-1])) if len(c) >= 51 else ma50
    golden_cross = ma20_prev < ma50_prev and ma20 > ma50
    death_cross  = ma20_prev > ma50_prev and ma20 < ma50

    vol_history = v[:-1] if len(v) > 1 else v
    valid_vols  = vol_history[vol_history > 0]
    vol_ma20    = float(np.mean(valid_vols[-20:] if len(valid_vols) >= 20 else valid_vols)) \
                  if len(valid_vols) >= 5 else \
                  (float(np.mean(v[v > 0])) if np.any(v > 0) else 0.0)
    vol_today   = float(v[-1])
    vol_ratio   = vol_today / vol_ma20 if vol_ma20 > 0 else 1.0
    price_up    = price >= prev_close

    if   vol_ratio >= 1.5 and price_up:     vol_signal = 'shark_buy'
    elif vol_ratio >= 1.5 and not price_up: vol_signal = 'shark_sell'
    elif vol_ratio < 0.7  and price_up:     vol_signal = 'fake_rally'
    elif vol_ratio >= 1.0 and price_up:     vol_signal = 'normal_buy'
    elif vol_ratio < 0.7  and not price_up: vol_signal = 'weak_sell'
    else:                                   vol_signal = 'normal'

    def detect_div(pc, rc, lookback=20):
        if len(pc) < lookback:
            return 'none'
        p2 = pc[-lookback:]; r2 = rc[-lookback:]
        bottoms = [i for i in range(1, len(p2)-1) if p2[i] < p2[i-1] and p2[i] < p2[i+1]]
        tops    = [i for i in range(1, len(p2)-1) if p2[i] > p2[i-1] and p2[i] > p2[i+1]]
        if len(bottoms) >= 2:
            b1, b2 = bottoms[-2], bottoms[-1]
            if p2[b2] < p2[b1] and r2[b2] > r2[b1] + 2:
                return 'bullish'
        if len(tops) >= 2:
            t1, t2 = tops[-2], tops[-1]
            if p2[t2] > p2[t1] and r2[t2] < r2[t1] - 2:
                return 'bearish'
        return 'none'

    div_type = detect_div(c, rsi_series)

    tenkan    = (np.max(h[-9:])  + np.min(l[-9:]))  / 2 if len(h) >= 9  else price
    kijun     = (np.max(h[-26:]) + np.min(l[-26:])) / 2 if len(h) >= 26 else price
    span_a    = (tenkan + kijun) / 2
    span_b    = (np.max(h[-52:]) + np.min(l[-52:])) / 2 if len(h) >= 52 else price
    cloud_top = max(float(span_a), float(span_b))
    cloud_bot = min(float(span_a), float(span_b))

    bb_mid   = float(np.mean(c[-20:]))
    bb_std   = float(np.std(c[-20:]))
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std

    # Score weights can bang v4.1
    score = 50

    if   vol_signal == 'shark_buy':  score += 20
    elif vol_signal == 'shark_sell': score -= 20
    elif vol_signal == 'fake_rally': score -= 12
    elif vol_signal == 'normal_buy': score += 8
    elif vol_signal == 'weak_sell':  score += 3

    if   rsi_val < 30: score += 20
    elif rsi_val < 40: score += 10
    elif rsi_val > 70: score -= 20
    elif rsi_val > 60: score -= 10

    if   div_type == 'bullish': score += (15 if rsi_val < 35 else 10)
    elif div_type == 'bearish': score -= (15 if rsi_val > 65 else 10)

    if   golden_cross:                 score += 20
    elif death_cross:                  score -= 20
    elif price > ma20 and ma20 > ma50: score += 15
    elif price > ma20:                 score += 10
    elif price < ma20 and ma20 < ma50: score -= 15
    else:                              score -= 10

    # Hard Filter MA20
    ma20_dist = (ma20 - price) / ma20 if ma20 > 0 else 0.0
    if price < ma20 and ma20 < ma50:
        dcb = (ma20_dist >= 0.15 and rsi_val < 25 and vol_signal == 'weak_sell')
        score = min(score, 60 if dcb else 55)
    elif price < ma20:
        score = min(score, 68)

    if   macd_v > macd_s and macd_h > 0: score += 5
    elif macd_v < macd_s and macd_h < 0: score -= 5

    if   price > cloud_top: score += 5
    elif price < cloud_bot: score -= 5

    if   price <= bb_lower: score += 3
    elif price >= bb_upper: score -= 3

    score = max(0, min(100, score))

    if   score >= MIN_SCORE_BUY:  action = 'MUA'
    elif score <= MAX_SCORE_SELL: action = 'BAN'
    else:                         action = 'THEO DOI'

    return score, action


# ═══════════════════════════════════════════════════════════════════════════════
# MO PHONG GIAO DICH
# ═══════════════════════════════════════════════════════════════════════════════

def simulate_trade(closes, entry_idx, direction='MUA'):
    entry_price = closes[entry_idx]
    for d in range(1, HOLD_DAYS + 1):
        if entry_idx + d >= len(closes):
            break
        current = closes[entry_idx + d]
        pnl = (current - entry_price) / entry_price
        if direction == 'MUA':
            if pnl <= STOP_LOSS:   return round(pnl * 100, 2), 'SL', d
            if pnl >= TAKE_PROFIT: return round(pnl * 100, 2), 'TP', d
        else:
            if pnl >= 0.07:  return round(-pnl * 100, 2), 'WRONG', d
            if pnl <= -0.07: return round(-pnl * 100, 2), 'RIGHT', d
    final = closes[min(entry_idx + HOLD_DAYS, len(closes) - 1)]
    pnl   = (final - entry_price) / entry_price
    return (round(pnl * 100, 2), 'EXPIRED', HOLD_DAYS) if direction == 'MUA' \
           else (round(-pnl * 100, 2), 'EXPIRED', HOLD_DAYS)


# ═══════════════════════════════════════════════════════════════════════════════
# TAI DU LIEU
# ═══════════════════════════════════════════════════════════════════════════════

def load_data(symbol, days=LOOKBACK_DAYS):
    end   = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    for source in ['VCI', 'TCBS']:
        try:
            from vnstock import Vnstock
            df = Vnstock().stock(symbol=symbol, source=source).quote.history(
                start=start, end=end, interval='1D'
            )
            if df is not None and len(df) >= 120:
                return df, source
        except Exception as e:
            print(f"    [{symbol}/{source}] {e}")
    return None, None


# ═══════════════════════════════════════════════════════════════════════════════
# BACKTEST 1 MA
# ═══════════════════════════════════════════════════════════════════════════════

def calc_stats(subset):
    if len(subset) == 0:
        return {}
    wins   = subset[subset['pnl'] > 0]
    losses = subset[subset['pnl'] <= 0]
    pf_d   = abs(losses['pnl'].sum())
    return {
        'total':    len(subset),
        'win_rate': round(len(wins) / len(subset) * 100, 1),
        'avg_pnl':  round(subset['pnl'].mean(), 2),
        'avg_win':  round(wins['pnl'].mean(), 2)   if len(wins)   > 0 else 0.0,
        'avg_loss': round(losses['pnl'].mean(), 2) if len(losses) > 0 else 0.0,
        'profit_factor': round(abs(wins['pnl'].sum()) / pf_d, 2) if pf_d > 0 else float('inf'),
        'avg_days': round(subset['days'].mean(), 1),
        'tp':       int(len(subset[subset['reason'] == 'TP'])),
        'sl':       int(len(subset[subset['reason'] == 'SL'])),
        'expired':  int(len(subset[subset['reason'] == 'EXPIRED'])),
    }


def run_backtest_symbol(symbol, verbose=True):
    if verbose:
        print(f"\n{'─'*58}")
        print(f"  Backtest: {symbol}  [{SECTOR_MAP.get(symbol, 'Khac')}]")
        print(f"{'─'*58}")

    df, source = load_data(symbol)
    if df is None:
        if verbose: print(f"  X Khong tai duoc du lieu")
        return None

    def to_arr(s):
        return pd.to_numeric(s, errors='coerce').fillna(0).astype(float).values

    cc = find_col(df, ['close','closeprice','close_price'])
    hc = find_col(df, ['high','highprice','high_price'])
    lc = find_col(df, ['low','lowprice','low_price'])
    vc = next((c for c in df.columns if c.lower() in {
        'volume','volume_match','klgd','vol','trading_volume',
        'match_volume','total_volume','dealvolume','matchingvolume'}), None)

    if cc is None:
        if verbose: print(f"  X Khong tim duoc cot close")
        return None

    closes  = to_arr(df[cc]);  closes[closes   < 1000] *= 1000
    highs   = to_arr(df[hc]) if hc else closes.copy()
    if hc:  highs[highs < 1000] *= 1000
    lows    = to_arr(df[lc]) if lc else closes.copy()
    if lc:  lows[lows   < 1000] *= 1000
    volumes = to_arr(df[vc]) if vc else np.zeros(len(closes))

    # Nam cua tung nen
    try:
        years = pd.to_datetime(df.index).year.values
    except Exception:
        years = np.zeros(len(closes), dtype=int)

    if verbose:
        print(f"  Du lieu: {len(closes)} nen (~{len(closes)//250} nam) tu {source}")

    # Vong lap sinh tin hieu
    trades          = []
    last_signal_idx = -HOLD_DAYS

    for i in range(60, len(closes) - HOLD_DAYS):
        if i - last_signal_idx < HOLD_DAYS:
            continue
        score, action = compute_score_at(closes, highs, lows, volumes, i)
        if action not in ('MUA', 'BAN'):
            continue
        pnl, reason, days = simulate_trade(closes, i, action)
        yr = int(years[i]) if years[i] != 0 else 0
        trades.append({
            'date':   str(df.index[i])[:10],
            'year':   yr,
            'price':  round(closes[i], 0),
            'score':  score,
            'action': action,
            'pnl':    pnl,
            'reason': reason,
            'days':   days,
        })
        last_signal_idx = i

    if not trades:
        if verbose: print(f"  Khong co tin hieu nao")
        return None

    df_t        = pd.DataFrame(trades)
    buy_trades  = df_t[df_t['action'] == 'MUA']
    sell_trades = df_t[df_t['action'] == 'BAN']
    buy_stats   = calc_stats(buy_trades)
    sell_stats  = calc_stats(sell_trades)

    # Theo nam
    yearly = {}
    for yr, grp in buy_trades.groupby('year'):
        if yr == 0: continue
        yearly[int(yr)] = calc_stats(grp)

    # Theo nguong score
    thresholds = {}
    for thr in [65, 70, 75, 80]:
        thresholds[thr] = calc_stats(buy_trades[buy_trades['score'] >= thr])

    # Score bucket
    buckets = {}
    for lo, hi in [(65,72),(72,80),(80,101)]:
        lbl = f'{lo}-{hi-1}'
        buckets[lbl] = calc_stats(buy_trades[
            (buy_trades['score'] >= lo) & (buy_trades['score'] < hi)])

    if verbose and buy_stats:
        bs   = buy_stats
        pf_s = f"{bs['profit_factor']:.2f}" if bs['profit_factor'] != float('inf') else 'inf'
        print(f"\n  [MUA] {bs['total']} lenh | WR={bs['win_rate']}% | "
              f"PnL={bs['avg_pnl']:+.2f}% | PF={pf_s} | "
              f"TP={bs['tp']} SL={bs['sl']}")

        if yearly:
            print(f"\n  [Theo nam]")
            print(f"   {'Nam':>5} | {'Lenh':>5} | {'WR%':>6} | {'PnL TB':>8} | Ghi chu")
            print(f"   {'─'*52}")
            for yr in sorted(yearly.keys()):
                y    = yearly[yr]
                note = ('← Tot' if y['win_rate'] >= 65 else
                        '← Yeu' if y['win_rate'] < 45 else
                        '← Canh bao lo' if y['avg_pnl'] < -1 else '')
                print(f"   {yr:>5} | {y['total']:>5} | "
                      f"{y['win_rate']:>5.1f}% | {y['avg_pnl']:>+7.2f}% | {note}")

        print(f"\n  [Toi uu nguong MUA]")
        print(f"   {'Nguong':>8} | {'Lenh':>5} | {'WR%':>6} | {'PnL TB':>8} | {'PF':>5}")
        print(f"   {'─'*48}")
        for thr, st in thresholds.items():
            if not st: continue
            pf_t  = f"{st['profit_factor']:.2f}" if st['profit_factor'] != float('inf') else 'inf'
            flag  = ' <- TOT NHAT' if (st['win_rate'] >= 60 and st['avg_pnl'] >= 3) else ''
            print(f"   score>={thr:>3} | {st['total']:>5} | "
                  f"{st['win_rate']:>5.1f}% | {st['avg_pnl']:>+7.2f}% | {pf_t:>5}{flag}")

        print(f"\n  [5 lenh MUA gan nhat]")
        for _, row in buy_trades.tail(5).iterrows():
            icon = 'V' if row['pnl'] > 0 else 'X'
            print(f"   {icon} {row['date']}  @{row['price']:>10,.0f}  "
                  f"Score={row['score']}  PnL={row['pnl']:>+6.1f}%  "
                  f"({row['reason']}, {row['days']}p)")

    return {
        'symbol':     symbol,
        'sector':     SECTOR_MAP.get(symbol, 'Khac'),
        'total':      len(df_t),
        'buy':        buy_stats,
        'sell':       sell_stats,
        'yearly':     yearly,
        'thresholds': thresholds,
        'buckets':    buckets,
        'trades':     df_t,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# BAO CAO TONG HOP 28 MA — 3 CHIEU PHAN TICH
# ═══════════════════════════════════════════════════════════════════════════════

def run_backtest_all(symbols=None, verbose_each=False):
    if symbols is None:
        symbols = WATCHLIST

    LINE = '=' * 62
    print(f"\n{LINE}")
    print(f"  BACKTEST TOAN BO {len(symbols)} MA WATCHLIST — 5 NAM")
    print(f"  SL=-7% | TP=+14% | Giu toi da {HOLD_DAYS} phien")
    print(f"  Bat dau: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"{LINE}\n")

    all_results = []
    failed      = []

    for i, sym in enumerate(symbols, 1):
        print(f"  [{i:>2}/{len(symbols)}] Dang tai {sym}...", end=' ', flush=True)
        result = run_backtest_symbol(sym, verbose=verbose_each)
        if result and result.get('buy'):
            all_results.append(result)
            bs   = result['buy']
            pf_s = f"{bs['profit_factor']:.2f}" if bs['profit_factor'] != float('inf') else 'inf'
            print(f"OK  WR={bs['win_rate']}%  PnL={bs['avg_pnl']:+.2f}%  PF={pf_s}")
        else:
            failed.append(sym)
            print(f"X  Khong co du lieu/tin hieu")

    if not all_results:
        print("\nKhong co ket qua nao.")
        return

    df_sum = pd.DataFrame([{
        'symbol':   r['symbol'],
        'sector':   r['sector'],
        'lenh':     r['buy']['total'],
        'wr':       r['buy']['win_rate'],
        'pnl':      r['buy']['avg_pnl'],
        'pf':       r['buy']['profit_factor'],
        'avg_win':  r['buy']['avg_win'],
        'avg_loss': r['buy']['avg_loss'],
        'tp':       r['buy']['tp'],
        'sl':       r['buy']['sl'],
    } for r in all_results])

    # ─────────────────────────────────────────────────────────────────────────
    # CHIEU 1: BANG TUNG MA
    # ─────────────────────────────────────────────────────────────────────────
    print(f"\n\n{LINE}")
    print(f"  CHIEU 1 — BREADTH: KET QUA TUNG MA (sap xep theo Win Rate)")
    print(f"{LINE}")
    print(f"  {'Ma':>5} | {'Nganh':>15} | {'Lenh':>5} | {'WR%':>6} | "
          f"{'PnL TB':>8} | {'PF':>5} | {'TP/SL':>6} | Danh gia")
    print(f"  {'─'*80}")

    df_sorted = df_sum.sort_values('wr', ascending=False)
    for _, row in df_sorted.iterrows():
        pf_s = f"{row['pf']:.2f}" if row['pf'] != float('inf') else '  inf'
        if   row['wr'] >= 60 and row['pnl'] >= 3: rating = '[TOT]'
        elif row['wr'] >= 55 and row['pnl'] >= 0: rating = '[ON ]'
        elif row['wr'] >= 50 and row['pnl'] >= 0: rating = '[TB ]'
        else:                                       rating = '[YEU]'
        print(f"  {row['symbol']:>5} | {row['sector']:>15} | {int(row['lenh']):>5} | "
              f"{row['wr']:>5.1f}% | {row['pnl']:>+7.2f}% | {pf_s:>5} | "
              f"{int(row['tp']):>3}/{int(row['sl']):<3} | {rating}")

    avg_wr  = df_sum['wr'].mean()
    avg_pnl = df_sum['pnl'].mean()
    n_good  = len(df_sum[(df_sum['wr'] >= 55) & (df_sum['pnl'] > 0)])
    n_bad   = len(df_sum[(df_sum['wr'] < 50)  | (df_sum['pnl'] < 0)])

    print(f"\n  Tong ket Chieu 1:")
    print(f"   Trung binh Win Rate  : {avg_wr:.1f}%  (muc tieu >=55%)")
    print(f"   Trung binh PnL       : {avg_pnl:+.2f}%  (muc tieu >0%)")
    print(f"   Ma tot (WR>=55%,PnL>0): {n_good}/{len(all_results)} ({n_good/len(all_results)*100:.0f}%)")
    print(f"   Ma yeu (WR<50% hoac PnL<0): {n_bad}/{len(all_results)}")

    # ─────────────────────────────────────────────────────────────────────────
    # CHIEU 2: THEO NAM
    # ─────────────────────────────────────────────────────────────────────────
    print(f"\n\n{LINE}")
    print(f"  CHIEU 2 — TIME SLICE: KET QUA THEO NAM (gop toan bo ma)")
    print(f"  Phat hien: bot co dang overfitting bull market 2021 khong?")
    print(f"{LINE}")

    all_trades_list = []
    for r in all_results:
        t = r['trades'].copy()
        t['symbol'] = r['symbol']
        all_trades_list.append(t)
    df_all = pd.concat(all_trades_list, ignore_index=True)
    buy_all = df_all[df_all['action'] == 'MUA']

    print(f"\n  {'Nam':>5} | {'Lenh':>5} | {'WR%':>6} | {'PnL TB':>8} | "
          f"{'TP':>4} | {'SL':>4} | Nhan xet")
    print(f"  {'─'*68}")

    yearly_global = {}
    for yr, grp in buy_all.groupby('year'):
        if yr == 0: continue
        s    = calc_stats(grp)
        wr   = s['win_rate']
        avg  = s['avg_pnl']
        yearly_global[int(yr)] = s

        if wr >= 60 and avg >= 3:
            note = 'V Bot hoat dong hieu qua'
        elif wr >= 50 and avg >= 0:
            note = '-> Trung binh'
        elif int(yr) == 2021 and wr >= 58:
            note = '! Canh bao — co the huong loi bull market'
        else:
            note = 'X Bot gap kho khan giai doan nay'

        print(f"  {int(yr):>5} | {s['total']:>5} | {wr:>5.1f}% | {avg:>+7.2f}% | "
              f"{s['tp']:>4} | {s['sl']:>4} | {note}")

    # Kiem tra overfitting
    if 2021 in yearly_global and 2022 in yearly_global:
        wr_2021 = yearly_global[2021]['win_rate']
        wr_2022 = yearly_global[2022]['win_rate']
        gap     = wr_2021 - wr_2022
        print(f"\n  Chenh lech WR 2021 vs 2022: {gap:+.1f}%")
        if gap > 20:
            print(f"  ! CANH BAO: Chenh lech lon ({gap:.0f}%) — ket qua 2021 bi inflate boi bull market")
            print(f"    Ket qua 2022 (bear market) moi phan anh kha nang thuc su cua bot")
        elif gap > 10:
            print(f"  -> Chenh lech vua ({gap:.0f}%) — binh thuong, bull market luon de hon")
        else:
            print(f"  V Chenh lech nho ({gap:.0f}%) — bot on dinh qua ca bull lan bear")

    # ─────────────────────────────────────────────────────────────────────────
    # CHIEU 3: TOI UU NGUONG SCORE
    # ─────────────────────────────────────────────────────────────────────────
    print(f"\n\n{LINE}")
    print(f"  CHIEU 3 — THRESHOLD: TIM NGUONG SCORE MUA TOI UU")
    print(f"  Cau hoi: Nen dat nguong MUA o 65, 70, 75, hay 80?")
    print(f"{LINE}")
    print(f"\n  {'Nguong':>10} | {'Lenh':>5} | {'WR%':>6} | {'PnL TB':>8} | "
          f"{'PF':>5} | {'%Ma':>5} | Khuyen nghi")
    print(f"  {'─'*70}")

    best_thr   = MIN_SCORE_BUY
    best_score = 0
    thr_results = {}

    for thr in [65, 70, 75, 80]:
        sub = buy_all[buy_all['score'] >= thr]
        if len(sub) == 0:
            continue
        st    = calc_stats(sub)
        pf_s  = f"{st['profit_factor']:.2f}" if st['profit_factor'] != float('inf') else 'inf'
        n_ma  = sub['symbol'].nunique()
        pct   = n_ma / len(all_results) * 100

        composite = (st['win_rate'] * 0.4
                     + st['avg_pnl'] * 2.0
                     + (min(st['profit_factor'], 5.0)) * 5.0)
        thr_results[thr] = st

        if composite > best_score:
            best_score = composite
            best_thr   = thr

        flag = ' <- TOI UU' if thr == best_thr else ''
        print(f"  score>={thr:>3}   | {st['total']:>5} | {st['win_rate']:>5.1f}% | "
              f"{st['avg_pnl']:>+7.2f}% | {pf_s:>5} | {pct:>4.0f}% | {flag}")

    print(f"\n  => Nguong MUA toi uu de xuat: score >= {best_thr}")
    curr = thr_results.get(MIN_SCORE_BUY, {})
    opt  = thr_results.get(best_thr, {})
    if best_thr != MIN_SCORE_BUY and curr and opt:
        delta_wr  = opt['win_rate']  - curr['win_rate']
        delta_pnl = opt['avg_pnl']   - curr['avg_pnl']
        lost_pct  = (curr['total'] - opt['total']) / curr['total'] * 100
        print(f"     Hien tai (>={MIN_SCORE_BUY}): WR={curr['win_rate']:.1f}%  PnL={curr['avg_pnl']:+.2f}%")
        print(f"     Toi uu  (>={best_thr}): WR={opt['win_rate']:.1f}%  PnL={opt['avg_pnl']:+.2f}%")
        print(f"     Cai thien: WR {delta_wr:+.1f}%  |  PnL {delta_pnl:+.2f}%")
        print(f"     Danh doi : bo qua {curr['total']-opt['total']} lenh score thap ({lost_pct:.0f}% tong)")
    else:
        print(f"     Nguong hien tai (>={MIN_SCORE_BUY}) dang la toi uu.")

    # ─────────────────────────────────────────────────────────────────────────
    # PHAN TICH THEO NGANH
    # ─────────────────────────────────────────────────────────────────────────
    print(f"\n\n{LINE}")
    print(f"  PHAN TICH THEO NGANH")
    print(f"{LINE}")
    print(f"\n  {'Nganh':>18} | {'So ma':>5} | {'Lenh TB':>7} | "
          f"{'WR% TB':>7} | {'PnL TB':>8} | Danh gia")
    print(f"  {'─'*68}")

    sector_agg = df_sum.groupby('sector').agg(
        n_ma=('symbol','count'),
        avg_lenh=('lenh','mean'),
        avg_wr=('wr','mean'),
        avg_pnl=('pnl','mean'),
    ).sort_values('avg_wr', ascending=False)

    for sector, row in sector_agg.iterrows():
        if   row['avg_wr'] >= 60 and row['avg_pnl'] >= 2: sg = '[TOT] Ky thuat ro rang'
        elif row['avg_wr'] >= 55 and row['avg_pnl'] >= 0: sg = '[ON ] Chap nhan duoc'
        else:                                               sg = '[YEU] Phi ky thuat cao'
        print(f"  {sector:>18} | {int(row['n_ma']):>5} | {row['avg_lenh']:>7.1f} | "
              f"{row['avg_wr']:>6.1f}% | {row['avg_pnl']:>+7.2f}% | {sg}")

    # ─────────────────────────────────────────────────────────────────────────
    # KET LUAN TONG THE
    # ─────────────────────────────────────────────────────────────────────────
    print(f"\n\n{'=' * 62}")
    print(f"  KET LUAN VA KHUYEN NGHI HANH DONG")
    print(f"{'=' * 62}")

    if avg_wr >= 58 and avg_pnl >= 2:
        verdict  = "[V] HE THONG HOAT DONG TOT"
        v_detail = "Tin hieu dang tin cay tren phan lon cac ma."
    elif avg_wr >= 53 and avg_pnl >= 0:
        verdict  = "[~] HE THONG DAT MUC CHAP NHAN"
        v_detail = "Tin hieu co gia tri tham khao, nen ket hop phan tich tay."
    else:
        verdict  = "[X] HE THONG CAN CAI THIEN"
        v_detail = "Weights hoac nguong score chua phu hop, can calibrate lai."

    print(f"\n  {verdict}")
    print(f"  {v_detail}")
    print(f"\n  Tom tat so lieu:")
    print(f"   {len(all_results)}/{len(symbols)} ma co du lieu day du")
    print(f"   Win Rate trung binh  : {avg_wr:.1f}%  (muc tieu >=55%)")
    print(f"   PnL trung binh       : {avg_pnl:+.2f}%  (muc tieu >0%)")
    print(f"   Ma dat chuan tot     : {n_good}/{len(all_results)}")
    print(f"   Nguong score toi uu  : >={best_thr}  (hien tai >={MIN_SCORE_BUY})")

    good_syms = df_sorted[df_sorted['wr'] >= 58]['symbol'].tolist()[:6]
    bad_syms  = df_sorted[df_sorted['wr'] <  50]['symbol'].tolist()

    print(f"\n  Khuyen nghi hanh dong:")
    if best_thr != MIN_SCORE_BUY:
        print(f"   1. Nang nguong MUA tu {MIN_SCORE_BUY} -> {best_thr} de loc tot hon")
    else:
        print(f"   1. Giu nguong MUA hien tai ({MIN_SCORE_BUY}) — dang toi uu")
    if good_syms:
        print(f"   2. Uu tien theo doi tin hieu: {', '.join(good_syms)}")
    if bad_syms:
        print(f"   3. Can than voi: {', '.join(bad_syms)}")
        print(f"      (Bot kem hieu qua — ma bi chi phoi boi yeu to phi ky thuat)")

    print(f"\n  Luu y quan trong:")
    print(f"   - Phi giao dich thuc te ~0.3%/khu hoi — tru them vao PnL")
    print(f"   - Du lieu 5 nam bao gom ca bull (2021) lan bear (2022)")
    print(f"   - Ket qua qua khu khong dam bao tuong lai")
    print(f"   - Khong phai tu van dau tu\n")

    if failed:
        print(f"  Ma khong tai duoc: {', '.join(failed)}\n")

    return all_results


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    args = sys.argv[1:]

    if not args:
        # Mac dinh: chay toan bo 28 ma watchlist
        print("=> Chay toan bo 28 ma watchlist (5 nam du lieu)...")
        print("   De test rieng: python backtest.py VCB")
        print("   De xem chi tiet: python backtest.py --detail VCB HPG\n")
        run_backtest_all(WATCHLIST, verbose_each=False)

    elif args[0] == '--detail':
        syms = [s.upper() for s in args[1:]] if len(args) > 1 else WATCHLIST
        run_backtest_all(syms, verbose_each=True)

    elif len(args) == 1:
        run_backtest_symbol(args[0].upper(), verbose=True)

    else:
        syms = [s.upper() for s in args]
        run_backtest_all(syms, verbose_each=False)
