"""
backtest.py - Module kiểm tra độ tin cậy tín hiệu VN Trader Bot
=================================================================
Cách dùng:
    python backtest.py VCB          # backtest 1 mã
    python backtest.py VCB HPG FPT  # backtest nhiều mã
    python backtest.py --all        # backtest toàn bộ watchlist

Cấu hình: 5 năm dữ liệu | SL=-7% | TP=+14% | Giữ tối đa 10 phiên
"""

import sys
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ── Cấu hình backtest ───────────────────────────────────────────────────────
HOLD_DAYS    = 10    # Giữ tối đa 10 phiên sau tín hiệu
STOP_LOSS    = -0.07 # Cắt lỗ -7%
TAKE_PROFIT  = 0.14  # Chốt lời +14%
MIN_SCORE_BUY  = 65  # Ngưỡng MUA
MAX_SCORE_SELL = 35  # Ngưỡng BAN
LOOKBACK_DAYS  = 1825 # Lấy 5 năm dữ liệu để backtest (5 * 365)

WATCHLIST = [
    'VCB', 'BID', 'TCB', 'MBB', 'VPB',
    'VHM', 'VIC', 'NVL', 'PDR',
    'FPT', 'CMG',
    'HPG', 'HSG', 'NKG',
    'SSI', 'VND', 'HCM',
    'GAS', 'PVD', 'PVS',
    'MWG', 'FRT',
    'VNM', 'MSN',
    'POW', 'REE',
    'KBC', 'SZC',
]


# ── Tái sử dụng logic tính toán từ app.py ───────────────────────────────────

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
    """RSI dùng Wilder's Smoothing - chuẩn nhất."""
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
    """
    Tính score tại điểm idx (chỉ dùng dữ liệu đến idx để không lookahead).
    Trả về score (0-100) và action string.
    """
    if idx < 52:  # Cần ít nhất 52 nến cho Ichimoku
        return 50, 'THEO DOI'

    c = closes[:idx + 1]
    h = highs[:idx + 1]
    l = lows[:idx + 1]
    v = volumes[:idx + 1]

    price = float(c[-1])
    prev_close = float(c[-2]) if len(c) > 1 else price

    # RSI
    rsi_series = calc_rsi_wilder(c)
    rsi_val = float(rsi_series[-1])

    # MACD
    e12 = ema_arr(c, 12)
    e26 = ema_arr(c, 26)
    macd_line = e12 - e26
    sig_line  = ema_arr(macd_line, 9)
    macd_h    = float((macd_line - sig_line)[-1])
    macd_v    = float(macd_line[-1])
    macd_s    = float(sig_line[-1])

    # MA
    ma20 = float(np.mean(c[-20:]))
    ma50 = float(np.mean(c[-min(50, len(c)):]))
    ma20_prev = float(np.mean(c[-21:-1])) if len(c) >= 21 else ma20
    ma50_prev = float(np.mean(c[-51:-1])) if len(c) >= 51 else ma50
    golden_cross = ma20_prev < ma50_prev and ma20 > ma50
    death_cross  = ma20_prev > ma50_prev and ma20 < ma50

    # Volume
    vol_history = v[:-1] if len(v) > 1 else v
    valid_vols  = vol_history[vol_history > 0]
    if len(valid_vols) >= 5:
        vol_ma20 = float(np.mean(valid_vols[-20:] if len(valid_vols) >= 20 else valid_vols))
    else:
        vol_ma20 = float(np.mean(v[v > 0])) if np.any(v > 0) else 0.0

    vol_today = float(v[-1])
    vol_ratio = vol_today / vol_ma20 if vol_ma20 > 0 else 1.0
    price_up  = price >= prev_close

    if   vol_ratio >= 1.5 and price_up:      vol_signal = 'shark_buy'
    elif vol_ratio >= 1.5 and not price_up:  vol_signal = 'shark_sell'
    elif vol_ratio < 0.7  and price_up:      vol_signal = 'fake_rally'
    elif vol_ratio >= 1.0 and price_up:      vol_signal = 'normal_buy'
    elif vol_ratio < 0.7  and not price_up:  vol_signal = 'weak_sell'
    else:                                    vol_signal = 'normal'

    # Divergence RSI
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

    # Ichimoku
    tenkan   = (np.max(h[-9:])  + np.min(l[-9:]))  / 2 if len(h) >= 9  else price
    kijun    = (np.max(h[-26:]) + np.min(l[-26:])) / 2 if len(h) >= 26 else price
    span_a   = (tenkan + kijun) / 2
    span_b   = (np.max(h[-52:]) + np.min(l[-52:])) / 2 if len(h) >= 52 else price
    cloud_top    = max(float(span_a), float(span_b))
    cloud_bottom = min(float(span_a), float(span_b))

    # BB
    bb_mid = float(np.mean(c[-20:]))
    bb_std = float(np.std(c[-20:]))
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std

    # ── Tính score (weights cân bằng) ───────────────────────────────────────
    score = 50

    # VOL ±20
    if   vol_signal == 'shark_buy':   score += 20
    elif vol_signal == 'shark_sell':  score -= 20
    elif vol_signal == 'fake_rally':  score -= 12
    elif vol_signal == 'normal_buy':  score += 8
    elif vol_signal == 'weak_sell':   score += 3

    # RSI ±20
    if   rsi_val < 30:  score += 20
    elif rsi_val < 40:  score += 10
    elif rsi_val > 70:  score -= 20
    elif rsi_val > 60:  score -= 10

    # Divergence ±15
    if div_type == 'bullish':
        score += 15 if rsi_val < 35 else 10
    elif div_type == 'bearish':
        score -= 15 if rsi_val > 65 else 10

    # MA ±20
    if   golden_cross:                  score += 20
    elif death_cross:                   score -= 20
    elif price > ma20 and ma20 > ma50:  score += 15
    elif price > ma20:                  score += 10
    elif price < ma20 and ma20 < ma50:  score -= 15
    else:                               score -= 10

    # MACD ±5
    if   macd_v > macd_s and macd_h > 0:  score += 5
    elif macd_v < macd_s and macd_h < 0:  score -= 5

    # Ichimoku ±5
    if   price > cloud_top:    score += 5
    elif price < cloud_bottom: score -= 5

    # BB ±3
    if   price <= bb_lower:  score += 3
    elif price >= bb_upper:  score -= 3

    score = max(0, min(100, score))

    if   score >= MIN_SCORE_BUY:   action = 'MUA'
    elif score <= MAX_SCORE_SELL:  action = 'BAN'
    else:                          action = 'THEO DOI'

    return score, action


def simulate_trade(closes, entry_idx, direction='MUA'):
    """
    Mô phỏng giao dịch từ entry_idx.
    Trả về: (pnl_pct, exit_reason, days_held)
    """
    entry_price = closes[entry_idx]
    for d in range(1, HOLD_DAYS + 1):
        if entry_idx + d >= len(closes):
            break
        current = closes[entry_idx + d]
        pnl = (current - entry_price) / entry_price

        if direction == 'MUA':
            if pnl <= STOP_LOSS:
                return round(pnl * 100, 2), 'SL', d
            if pnl >= TAKE_PROFIT:
                return round(pnl * 100, 2), 'TP', d
        else:  # BAN (short hoặc tránh mua)
            # Nếu BAN mà giá tăng = sai; giá giảm = đúng
            if pnl >= 0.07:
                return round(-pnl * 100, 2), 'WRONG', d
            if pnl <= -0.07:
                return round(-pnl * 100, 2), 'RIGHT', d

    # Kết thúc kỳ giữ
    final = closes[min(entry_idx + HOLD_DAYS, len(closes) - 1)]
    pnl = (final - entry_price) / entry_price
    if direction == 'MUA':
        return round(pnl * 100, 2), 'EXPIRED', HOLD_DAYS
    else:
        return round(-pnl * 100, 2), 'EXPIRED', HOLD_DAYS


def load_data(symbol, days=LOOKBACK_DAYS):
    """Tải dữ liệu lịch sử từ vnstock."""
    end   = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    for source in ['VCI', 'TCBS']:
        try:
            from vnstock import Vnstock
            df = Vnstock().stock(symbol=symbol, source=source).quote.history(
                start=start, end=end, interval='1D'
            )
            if df is not None and len(df) >= 120:  # Cần ít nhất 120 phiên (~6 tháng)
                return df, source
        except Exception as e:
            print(f"  [{symbol}/{source}] lỗi: {e}")
    return None, None


def run_backtest_symbol(symbol):
    """
    Chạy backtest cho 1 mã.
    Trả về dict kết quả hoặc None nếu không có dữ liệu.
    """
    print(f"\n{'='*50}")
    print(f"  Backtest: {symbol}")
    print(f"{'='*50}")

    df, source = load_data(symbol)
    if df is None:
        print(f"  ✗ Không tải được dữ liệu {symbol}")
        return None

    # Chuẩn hóa cột
    def to_float_arr(series):
        return pd.to_numeric(series, errors='coerce').fillna(0).astype(float).values

    cc = find_col(df, ['close', 'closeprice', 'close_price'])
    hc = find_col(df, ['high',  'highprice',  'high_price'])
    lc = find_col(df, ['low',   'lowprice',   'low_price'])

    VOLUME_NAMES = {
        'volume', 'volume_match', 'klgd', 'vol', 'trading_volume',
        'match_volume', 'total_volume', 'dealvolume', 'matchingvolume',
    }
    vc = next((c for c in df.columns if c.lower() in VOLUME_NAMES), None)

    if cc is None:
        print(f"  ✗ Không tìm được cột close")
        return None

    closes  = to_float_arr(df[cc])
    highs   = to_float_arr(df[hc]) if hc else closes.copy()
    lows    = to_float_arr(df[lc]) if lc else closes.copy()
    volumes = to_float_arr(df[vc]) if vc else np.zeros(len(closes))

    # Scale giá nếu cần
    if closes.max() < 1000:  closes  *= 1000
    if highs.max()  < 1000:  highs   *= 1000
    if lows.max()   < 1000:  lows    *= 1000

    print(f"  Dữ liệu: {len(closes)} nến (~{len(closes)//250} năm) từ {source}")

    # ── Vòng lặp backtest ───────────────────────────────────────────────────
    trades = []
    last_signal_idx = -HOLD_DAYS  # tránh chồng tín hiệu

    for i in range(60, len(closes) - HOLD_DAYS):
        # Bỏ qua nếu vừa vào lệnh gần đây
        if i - last_signal_idx < HOLD_DAYS:
            continue

        score, action = compute_score_at(closes, highs, lows, volumes, i)

        if action not in ('MUA', 'BAN'):
            continue

        pnl, reason, days = simulate_trade(closes, i, action)
        trade_date = df.index[i] if hasattr(df.index, '__getitem__') else i

        trades.append({
            'idx':    i,
            'date':   str(trade_date)[:10],
            'price':  round(closes[i], 0),
            'score':  score,
            'action': action,
            'pnl':    pnl,
            'reason': reason,
            'days':   days,
        })
        last_signal_idx = i

    if not trades:
        print(f"  Không có tín hiệu nào trong kỳ backtest")
        return None

    df_trades = pd.DataFrame(trades)

    # ── Thống kê ────────────────────────────────────────────────────────────
    buy_trades  = df_trades[df_trades['action'] == 'MUA']
    sell_trades = df_trades[df_trades['action'] == 'BAN']

    def stats(subset, label):
        if len(subset) == 0:
            return {}
        wins     = subset[subset['pnl'] > 0]
        losses   = subset[subset['pnl'] <= 0]
        win_rate = len(wins) / len(subset) * 100
        avg_pnl  = subset['pnl'].mean()
        avg_win  = wins['pnl'].mean()   if len(wins) > 0  else 0
        avg_loss = losses['pnl'].mean() if len(losses) > 0 else 0
        profit_factor = abs(wins['pnl'].sum() / losses['pnl'].sum()) if losses['pnl'].sum() != 0 else float('inf')
        avg_days = subset['days'].mean()

        print(f"\n  [{label}] Tổng: {len(subset)} lệnh")
        print(f"   Tỉ lệ thắng  : {win_rate:.1f}%")
        print(f"   PnL trung bình: {avg_pnl:+.2f}%")
        print(f"   TB lời        : {avg_win:+.2f}%  |  TB lỗ: {avg_loss:+.2f}%")
        print(f"   Profit Factor : {profit_factor:.2f}")
        print(f"   Giữ TB        : {avg_days:.1f} phiên")

        tp_count  = len(subset[subset['reason'] == 'TP'])
        sl_count  = len(subset[subset['reason'] == 'SL'])
        exp_count = len(subset[subset['reason'] == 'EXPIRED'])
        print(f"   Kết quả: TP={tp_count} | SL={sl_count} | Hết kỳ={exp_count}")

        return {
            'total': len(subset), 'win_rate': round(win_rate, 1),
            'avg_pnl': round(avg_pnl, 2), 'avg_win': round(avg_win, 2),
            'avg_loss': round(avg_loss, 2), 'profit_factor': round(profit_factor, 2),
            'avg_days': round(avg_days, 1),
        }

    buy_stats  = stats(buy_trades,  'MUA')
    sell_stats = stats(sell_trades, 'BAN')

    # ── Phân tích theo score bucket ─────────────────────────────────────────
    print(f"\n  [Phân tích theo score]")
    print(f"   {'Score':>10} | {'Lệnh':>5} | {'Win%':>6} | {'PnL TB':>8}")
    print(f"   {'-'*38}")
    for lo, hi in [(65,72), (72,80), (80,100), (0,28), (28,35)]:
        bucket = df_trades[(df_trades['score'] >= lo) & (df_trades['score'] < hi)]
        if len(bucket) > 0:
            wr  = len(bucket[bucket['pnl'] > 0]) / len(bucket) * 100
            avg = bucket['pnl'].mean()
            print(f"   {lo:>4}-{hi:<4}    | {len(bucket):>5} | {wr:>5.1f}% | {avg:>+7.2f}%")

    # ── 5 lệnh gần nhất ─────────────────────────────────────────────────────
    print(f"\n  [5 lệnh gần nhất]")
    for _, row in df_trades.tail(5).iterrows():
        icon = '✓' if row['pnl'] > 0 else '✗'
        print(f"   {icon} {row['date']} {row['action']} @{row['price']:,.0f} | "
              f"Score={row['score']} | PnL={row['pnl']:+.1f}% ({row['reason']}, {row['days']}d)")

    return {
        'symbol':     symbol,
        'total_trades': len(df_trades),
        'buy':        buy_stats,
        'sell':       sell_stats,
        'trades':     df_trades,
    }


def run_backtest_all(symbols):
    """Chạy backtest toàn bộ danh sách và in báo cáo tổng hợp."""
    summary = []
    for sym in symbols:
        result = run_backtest_symbol(sym)
        if result and result.get('buy'):
            summary.append({
                'symbol':        result['symbol'],
                'total':         result['total_trades'],
                'buy_winrate':   result['buy'].get('win_rate', 0),
                'buy_pnl':       result['buy'].get('avg_pnl', 0),
                'profit_factor': result['buy'].get('profit_factor', 0),
            })

    if not summary:
        print("\nKhông có kết quả backtest nào.")
        return

    print(f"\n\n{'='*60}")
    print(f"  BÁO CÁO TỔNG HỢP BACKTEST - {len(summary)} mã")
    print(f"{'='*60}")
    print(f"{'Mã':>6} | {'Lệnh':>5} | {'Win%':>6} | {'PnL TB':>8} | {'PF':>5}")
    print(f"{'-'*45}")

    df_summary = pd.DataFrame(summary).sort_values('buy_winrate', ascending=False)
    for _, row in df_summary.iterrows():
        pf_str = f"{row['profit_factor']:.2f}" if row['profit_factor'] != float('inf') else "∞"
        flag = ' ✓' if row['buy_winrate'] >= 55 and row['buy_pnl'] > 0 else ''
        print(f"{row['symbol']:>6} | {int(row['total']):>5} | "
              f"{row['buy_winrate']:>5.1f}% | {row['buy_pnl']:>+7.2f}% | {pf_str:>5}{flag}")

    # Tổng kết
    avg_wr  = df_summary['buy_winrate'].mean()
    avg_pnl = df_summary['buy_pnl'].mean()
    good    = len(df_summary[(df_summary['buy_winrate'] >= 55) & (df_summary['buy_pnl'] > 0)])
    print(f"\n  Trung bình win rate: {avg_wr:.1f}%")
    print(f"  Trung bình PnL     : {avg_pnl:+.2f}%")
    print(f"  Mã tốt (WR>=55% và PnL>0): {good}/{len(summary)}")

    # Nhận xét
    print(f"\n{'='*60}")
    print(f"  NHẬN XÉT:")
    if avg_wr >= 55:
        print(f"  ✓ Win rate tổng thể {avg_wr:.1f}% - CHẤP NHẬN ĐƯỢC")
    else:
        print(f"  ✗ Win rate tổng thể {avg_wr:.1f}% - CẦN CẢI THIỆN (mục tiêu >= 55%)")

    if avg_pnl > 0:
        print(f"  ✓ PnL trung bình dương {avg_pnl:+.2f}% - TỐT")
    else:
        print(f"  ✗ PnL trung bình âm {avg_pnl:+.2f}% - XEM LẠI WEIGHTS")

    print(f"\n  Lưu ý: Backtest không tính phí giao dịch (~0.15-0.25%/lệnh)")
    print(f"  Dữ liệu 5 năm — kết quả thống kê đáng tin cậy hơn nhưng")
    print(f"  quá khứ vẫn không đảm bảo tương lai.")


# ── Entry point ──────────────────────────────────────────────────────────────
if __name__ == '__main__':
    args = sys.argv[1:]

    if not args:
        # Default: test 3 mã tiêu biểu
        symbols = ['VCB', 'HPG', 'FPT']
        print(f"Chạy backtest mẫu cho: {symbols}")
        print("Dùng: python backtest.py VCB HPG FPT  hoặc  python backtest.py --all")
        run_backtest_all(symbols)

    elif args[0] == '--all':
        print(f"Chạy backtest toàn bộ {len(WATCHLIST)} mã watchlist...")
        run_backtest_all(WATCHLIST)

    else:
        symbols = [s.upper() for s in args]
        if len(symbols) == 1:
            run_backtest_symbol(symbols[0])
        else:
            run_backtest_all(symbols)
