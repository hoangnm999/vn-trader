"""
walk_forward.py — Walk-Forward Validation cho VN Trader Bot
==============================================================
Tích hợp trực tiếp với backtest.py hiện tại.
Dùng lại toàn bộ: load_data(), compute_score_at(), simulate_trade(), calc_stats()

Cách dùng:
    python walk_forward.py DCM              # 1 mã — báo cáo đầy đủ
    python walk_forward.py DCM DGC GAS BID  # nhiều mã
    python walk_forward.py --top            # chạy top 9 mã đã chọn lọc

Cấu hình mặc định (Rolling Window):
    Train window : 3 năm
    Test window  : 1 năm
    Bước trượt   : 1 năm
    Score thresh : [65, 70, 75, 80]
"""

import sys
import warnings
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from itertools import product

warnings.filterwarnings('ignore')

# ── Import từ backtest.py hiện tại ──────────────────────────────────────────
# Đảm bảo backtest.py nằm cùng thư mục
try:
    from backtest import (
        load_data, compute_score_at, simulate_trade, calc_stats,
        HOLD_DAYS, STOP_LOSS, TAKE_PROFIT, SYMBOL_CONFIG,
        SCORE_THRESHOLDS, MARKET_PHASES, find_col,
    )
except ImportError as e:
    print(f"[LỖI] Không import được backtest.py: {e}")
    print("      Đảm bảo walk_forward.py nằm cùng thư mục với backtest.py")
    sys.exit(1)


# ─── Cấu hình Walk-Forward ───────────────────────────────────────────────────

WF_CONFIG = {
    'train_years': 3,       # Số năm Train mỗi window
    'test_years':  1,       # Số năm Test mỗi window
    'step_years':  1,       # Bước trượt (Rolling)
    'min_trades':  8,       # Số lệnh tối thiểu để cửa sổ hợp lệ
    'mode':        'rolling', # 'rolling' hoặc 'anchored'
}

# Tham số sẽ tối ưu trong Train
PARAM_GRID = {
    'score_threshold': [65, 70, 75, 80],
    # Thêm SL/TP vào đây nếu muốn tối ưu (tốn thêm thời gian)
    # 'sl': [0.05, 0.07],
    # 'tp': [0.09, 0.12, 0.14],
}

# Top 9 mã đã phân tích
TOP_SYMBOLS = ['DCM', 'DGC', 'GAS', 'MBB', 'BID', 'KDH', 'FPT', 'VCB', 'NVL']


# ─── Hàm phụ trợ ─────────────────────────────────────────────────────────────

def _run_trades_on_slice(closes, highs, lows, volumes, dates_series,
                          score_thr, sl, tp):
    """
    Chạy vòng lặp sinh tín hiệu + simulate trên một slice dữ liệu.
    Trả về DataFrame trades giống backtest.py
    """
    trades = []
    last_signal_idx = -HOLD_DAYS

    for i in range(60, len(closes) - HOLD_DAYS):
        if i - last_signal_idx < HOLD_DAYS:
            continue
        score, action = compute_score_at(closes, highs, lows, volumes, i)
        if action != 'MUA':
            continue
        if score < score_thr:
            continue

        pnl, reason, days_held = simulate_trade(closes, i, 'MUA', sl=sl, tp=tp)
        _ts = dates_series.iloc[i] if i < len(dates_series) else pd.NaT
        trade_date = _ts.strftime('%Y-%m-%d') if pd.notna(_ts) else f'idx_{i}'

        trades.append({
            'date':   trade_date,
            'price':  round(closes[i], 0),
            'score':  score,
            'action': 'MUA',
            'pnl':    pnl,
            'reason': reason,
            'days':   days_held,
        })
        last_signal_idx = i

    return pd.DataFrame(trades) if trades else pd.DataFrame()


def _slice_data(closes, highs, lows, volumes, dates_series, start_date, end_date):
    """Cắt dữ liệu theo khoảng ngày, trả về slice."""
    mask = (dates_series >= pd.Timestamp(start_date)) & \
           (dates_series <  pd.Timestamp(end_date))
    idx = mask[mask].index.tolist()
    if not idx:
        return None, None, None, None, None
    i0, i1 = idx[0], idx[-1] + 1
    return (closes[i0:i1], highs[i0:i1], lows[i0:i1],
            volumes[i0:i1], dates_series.iloc[i0:i1].reset_index(drop=True))


def _metric(stats):
    """
    Metric tổng hợp để chọn tham số tốt nhất trong Train.
    Kết hợp WR, PnL, PF — có penalty nếu ít lệnh.
    """
    if not stats or stats.get('total', 0) < WF_CONFIG['min_trades']:
        return -999.0
    wr  = stats.get('win_rate',      0) / 100
    pnl = stats.get('avg_pnl',       0)
    pf  = min(stats.get('profit_factor', 0), 5.0)  # cap ở 5 tránh inf
    n   = stats.get('total',         0)
    penalty = 0.8 if n < 15 else 1.0
    return wr * pnl * pf * penalty


# ─── CORE: Walk-Forward cho 1 mã ─────────────────────────────────────────────

def walk_forward_symbol(symbol, verbose=True):
    """
    Chạy Walk-Forward Validation Rolling cho 1 mã.
    Trả về dict kết quả đầy đủ.
    """
    SEP = '═' * 70

    cfg = SYMBOL_CONFIG.get(symbol.upper(), {})
    _sl = cfg.get('sl', abs(STOP_LOSS))
    _tp = cfg.get('tp', TAKE_PROFIT)

    if verbose:
        print(f"\n{SEP}")
        print(f"  WALK-FORWARD VALIDATION: {symbol}")
        print(f"  Mode: {WF_CONFIG['mode'].upper()} | "
              f"Train={WF_CONFIG['train_years']}Y | Test={WF_CONFIG['test_years']}Y | "
              f"SL={_sl*100:.0f}% TP={_tp*100:.0f}%")
        print(SEP)

    # ── Tải dữ liệu toàn bộ (7 năm) ─────────────────────────────────────────
    df_raw, source = load_data(symbol, days=2555)
    if df_raw is None or len(df_raw) < 400:
        print(f"  ✗ Không đủ dữ liệu cho {symbol}")
        return None

    def to_arr(s):
        return pd.to_numeric(s, errors='coerce').fillna(0).astype(float).values

    cc = find_col(df_raw, ['close', 'closeprice', 'close_price'])
    hc = find_col(df_raw, ['high',  'highprice',  'high_price'])
    lc = find_col(df_raw, ['low',   'lowprice',   'low_price'])
    vc = next((c for c in df_raw.columns if c.lower() in {
        'volume', 'volume_match', 'klgd', 'vol', 'trading_volume',
        'match_volume', 'total_volume', 'dealvolume', 'matchingvolume',
    }), None)

    if cc is None:
        print(f"  ✗ Không tìm được cột close cho {symbol}")
        return None

    closes  = to_arr(df_raw[cc]);  closes[closes < 1000] *= 1000
    highs   = to_arr(df_raw[hc]) if hc else closes.copy()
    lows    = to_arr(df_raw[lc]) if lc else closes.copy()
    if hc: highs[highs < 1000] *= 1000
    if lc: lows[lows   < 1000] *= 1000
    volumes = to_arr(df_raw[vc]) if vc else np.zeros(len(closes))

    _time_col = next(
        (c for c in df_raw.columns
         if c.lower() in ('time', 'date', 'datetime', 'trading_date')), None)
    if _time_col:
        dates_series = pd.to_datetime(df_raw[_time_col], errors='coerce').reset_index(drop=True)
    elif isinstance(df_raw.index, pd.DatetimeIndex):
        dates_series = pd.Series(df_raw.index, dtype='datetime64[ns]').reset_index(drop=True)
    else:
        print(f"  ✗ Không tìm được cột ngày cho {symbol}")
        return None

    # Lọc NaT
    valid_mask   = dates_series.notna()
    dates_series = dates_series[valid_mask].reset_index(drop=True)
    closes  = closes[valid_mask.values]
    highs   = highs[valid_mask.values]
    lows    = lows[valid_mask.values]
    volumes = volumes[valid_mask.values]

    first_date = dates_series.min()
    last_date  = dates_series.max()

    if verbose:
        print(f"  Dữ liệu: {len(closes)} nến | "
              f"{first_date.strftime('%Y-%m-%d')} → {last_date.strftime('%Y-%m-%d')} | "
              f"Nguồn: {source}")

    # ── Xây dựng danh sách cửa sổ ────────────────────────────────────────────
    train_y = WF_CONFIG['train_years']
    test_y  = WF_CONFIG['test_years']
    step_y  = WF_CONFIG['step_years']

    windows = []
    anchor  = first_date + pd.DateOffset(years=1)  # skip 1 năm đầu warmup
    cursor  = anchor

    while True:
        if WF_CONFIG['mode'] == 'rolling':
            train_start = cursor
            train_end   = cursor + pd.DateOffset(years=train_y)
        else:  # anchored
            train_start = anchor
            train_end   = cursor + pd.DateOffset(years=train_y)

        test_start = train_end
        test_end   = test_start + pd.DateOffset(years=test_y)

        if test_end > last_date + pd.DateOffset(months=6):
            break

        windows.append({
            'train_start': train_start,
            'train_end':   train_end,
            'test_start':  test_start,
            'test_end':    test_end,
        })
        cursor += pd.DateOffset(years=step_y)

    if not windows:
        print(f"  ✗ Không đủ dữ liệu để tạo cửa sổ Walk-Forward")
        return None

    if verbose:
        print(f"\n  Số cửa sổ Walk-Forward: {len(windows)}")
        print(f"  {'─'*70}")

    # ── Chạy từng cửa sổ ─────────────────────────────────────────────────────
    window_results = []

    for w_idx, win in enumerate(windows):
        # Slice Train
        tr_c, tr_h, tr_l, tr_v, tr_d = _slice_data(
            closes, highs, lows, volumes, dates_series,
            win['train_start'], win['train_end'])

        # Slice Test
        te_c, te_h, te_l, te_v, te_d = _slice_data(
            closes, highs, lows, volumes, dates_series,
            win['test_start'], win['test_end'])

        if tr_c is None or len(tr_c) < 100 or te_c is None or len(te_c) < 30:
            if verbose:
                print(f"  Cửa sổ {w_idx+1}: Không đủ dữ liệu — bỏ qua")
            continue

        # ── Bước 1: Tìm tham số tốt nhất trên Train ──────────────────────────
        best_params = {'score_threshold': 65}
        best_metric_val = -999.0

        for score_thr in PARAM_GRID['score_threshold']:
            tr_trades = _run_trades_on_slice(
                tr_c, tr_h, tr_l, tr_v, tr_d, score_thr, _sl, _tp)
            if tr_trades.empty:
                continue
            buy_only = tr_trades[tr_trades['action'] == 'MUA']
            if len(buy_only) < WF_CONFIG['min_trades']:
                continue
            st = calc_stats(buy_only)
            m  = _metric(st)
            if m > best_metric_val:
                best_metric_val = m
                best_params = {'score_threshold': score_thr}
                best_train_stats = st

        # ── Bước 2: Áp dụng tham số tốt nhất lên Test ────────────────────────
        te_trades = _run_trades_on_slice(
            te_c, te_h, te_l, te_v, te_d,
            best_params['score_threshold'], _sl, _tp)

        if te_trades.empty:
            te_buy    = pd.DataFrame()
            te_stats  = {}
        else:
            te_buy   = te_trades[te_trades['action'] == 'MUA']
            te_stats = calc_stats(te_buy) if not te_buy.empty else {}

        # Ghi kết quả cửa sổ
        window_results.append({
            'window':          w_idx + 1,
            'train_start':     win['train_start'].strftime('%Y'),
            'train_end':       (win['train_end'] - pd.DateOffset(days=1)).strftime('%Y'),
            'test_year':       win['test_start'].strftime('%Y'),
            'best_score_thr':  best_params['score_threshold'],
            'train_pf':        best_train_stats.get('profit_factor', 0) if 'best_train_stats' in dir() else 0,
            'train_wr':        best_train_stats.get('win_rate', 0) if 'best_train_stats' in dir() else 0,
            'train_n':         best_train_stats.get('total', 0) if 'best_train_stats' in dir() else 0,
            'test_n':          te_stats.get('total', 0),
            'test_wr':         te_stats.get('win_rate', 0),
            'test_pnl':        te_stats.get('avg_pnl', 0),
            'test_pf':         te_stats.get('profit_factor', 0),
            'test_pass':       te_stats.get('profit_factor', 0) > 1.0 and te_stats.get('total', 0) >= 3,
        })

        if verbose:
            ts = te_stats
            pf_s = f"{ts.get('profit_factor',0):.2f}" if ts.get('profit_factor',0) != float('inf') else ' inf'
            status = '✅' if window_results[-1]['test_pass'] else '❌'
            phase  = MARKET_PHASES.get(int(win['test_start'].strftime('%Y')), '---')
            print(f"  Cửa sổ {w_idx+1} │ "
                  f"Train: {win['train_start'].strftime('%Y')}–{(win['train_end']-pd.DateOffset(days=1)).strftime('%Y')} │ "
                  f"Test: {win['test_start'].strftime('%Y')} │ "
                  f"Score≥{best_params['score_threshold']} │ "
                  f"N={ts.get('total',0):>2} WR={ts.get('win_rate',0):>4.1f}% "
                  f"PF={pf_s} {status}")
            if phase:
                print(f"           ({phase})")

    if not window_results:
        print(f"  ✗ Không có cửa sổ hợp lệ")
        return None

    # ── Tổng hợp kết quả ─────────────────────────────────────────────────────
    df_wf    = pd.DataFrame(window_results)
    n_pass   = int(df_wf['test_pass'].sum())
    n_total  = len(df_wf)
    pass_rate = n_pass / n_total * 100

    avg_test_wr  = df_wf['test_wr'].mean()
    avg_test_pf  = df_wf[df_wf['test_n'] >= 3]['test_pf'].mean()
    avg_test_pnl = df_wf['test_pnl'].mean()

    # Score threshold được chọn nhiều nhất
    if not df_wf.empty:
        best_score_consensus = int(df_wf['best_score_thr'].mode()[0])
    else:
        best_score_consensus = 65

    # Đánh giá tổng thể
    if pass_rate >= 75 and avg_test_pf >= 1.3:
        verdict = '✅ TIN CẬY CAO — Signal có edge thực sự, không phải overfitting'
        tier    = 'HIGH'
    elif pass_rate >= 50 and avg_test_pf >= 1.0:
        verdict = '🟡 CHẤP NHẬN — Signal có edge, nhưng không nhất quán 100%'
        tier    = 'MEDIUM'
    elif pass_rate >= 50:
        verdict = '⚠️  THẬN TRỌNG — Pass rate ổn nhưng PF thấp, cần theo dõi thêm'
        tier    = 'LOW'
    else:
        verdict = '❌ OVERFITTING — Kết quả backtest không giữ được ngoài mẫu'
        tier    = 'FAIL'

    if verbose:
        print(f"\n  {'═'*70}")
        print(f"  KẾT QUẢ WALK-FORWARD: {symbol}")
        print(f"  {'═'*70}")
        print(f"  Số cửa sổ hợp lệ  : {n_total}")
        print(f"  Cửa sổ Test PASS   : {n_pass}/{n_total} ({pass_rate:.0f}%)")
        print(f"  WR trung bình Test : {avg_test_wr:.1f}%")
        print(f"  PF trung bình Test : {avg_test_pf:.2f}")
        print(f"  PnL trung bình Test: {avg_test_pnl:+.2f}%")
        print(f"  Score tối ưu nhất  : ≥{best_score_consensus} (được chọn nhiều nhất)")
        print(f"\n  Đánh giá: {verdict}")

        # Bảng chi tiết từng cửa sổ
        print(f"\n  Chi tiết từng cửa sổ:")
        print(f"  {'CW':>3} │ {'Train':>9} │ {'Test':>4} │ {'Thr':>3} │ "
              f"{'N':>3} │ {'WR':>5} │ {'PF':>5} │ {'PnL':>6} │ Kết quả")
        print(f"  {'─'*65}")
        for _, row in df_wf.iterrows():
            pf_s   = f"{row['test_pf']:.2f}" if row['test_pf'] != float('inf') else ' inf'
            status = '✅ PASS' if row['test_pass'] else '❌ FAIL'
            phase  = MARKET_PHASES.get(int(row['test_year']), '')[:20]
            print(f"  {int(row['window']):>3} │ "
                  f"{row['train_start']}–{row['train_end']} │ "
                  f"{row['test_year']:>4} │ "
                  f"≥{int(row['best_score_thr']):>2} │ "
                  f"{int(row['test_n']):>3} │ "
                  f"{row['test_wr']:>4.1f}% │ "
                  f"{pf_s:>5} │ "
                  f"{row['test_pnl']:>+5.1f}% │ "
                  f"{status} ({phase})")

        # Hướng dẫn sử dụng kết quả
        print(f"\n  {'─'*70}")
        print(f"  ĐỀ XUẤT ÁP DỤNG:")
        if tier in ('HIGH', 'MEDIUM'):
            print(f"  → Dùng Score≥{best_score_consensus} cho {symbol} trong live trading")
            print(f"  → Kỳ vọng thực tế: WR≈{avg_test_wr:.0f}% | PF≈{avg_test_pf:.2f}")
            fail_years = df_wf[~df_wf['test_pass']]['test_year'].tolist()
            if fail_years:
                print(f"  → Chú ý: Signal yếu trong năm {', '.join(fail_years)}")
                for fy in fail_years:
                    phase = MARKET_PHASES.get(int(fy), '')
                    if phase:
                        print(f"     {fy}: {phase}")
                print(f"  → Cân nhắc tắt tín hiệu khi thị trường ở giai đoạn tương tự")
        else:
            print(f"  → KHÔNG khuyến dùng {symbol} trong live trading với signal hiện tại")
            print(f"  → Cần xem xét lại bộ chỉ số hoặc loại mã này khỏi watchlist")

    return {
        'symbol':           symbol,
        'n_windows':        n_total,
        'n_pass':           n_pass,
        'pass_rate':        round(pass_rate, 1),
        'avg_test_wr':      round(avg_test_wr, 1),
        'avg_test_pf':      round(avg_test_pf, 2),
        'avg_test_pnl':     round(avg_test_pnl, 2),
        'best_score':       best_score_consensus,
        'tier':             tier,
        'verdict':          verdict,
        'window_results':   df_wf,
    }


# ─── Chạy nhiều mã + bảng tổng hợp ──────────────────────────────────────────

def walk_forward_multi(symbols):
    """Chạy Walk-Forward cho nhiều mã và in bảng so sánh."""
    all_results = {}

    for sym in symbols:
        res = walk_forward_symbol(sym, verbose=True)
        if res:
            all_results[sym] = res

    if len(all_results) < 2:
        return all_results

    # Bảng tổng hợp
    print(f"\n\n{'═'*78}")
    print(f"  BẢNG TỔNG HỢP WALK-FORWARD — {len(all_results)} MÃ")
    print(f"{'═'*78}")
    print(f"  {'Mã':>5} │ {'Pass':>6} │ {'WR TB':>5} │ {'PF TB':>5} │ "
          f"{'PnL TB':>6} │ {'Score':>5} │ Đánh giá")
    print(f"  {'─'*70}")

    # Sắp xếp theo pass_rate + avg_test_pf
    sorted_res = sorted(all_results.items(),
                        key=lambda x: (x[1]['pass_rate'], x[1]['avg_test_pf']),
                        reverse=True)

    tier_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2, 'FAIL': 3}
    for sym, res in sorted_res:
        tier_icon = {'HIGH': '✅', 'MEDIUM': '🟡', 'LOW': '⚠️ ', 'FAIL': '❌'}
        icon = tier_icon.get(res['tier'], '?')
        print(f"  {sym:>5} │ "
              f"{res['pass_rate']:>4.0f}%  │ "
              f"{res['avg_test_wr']:>4.1f}% │ "
              f"{res['avg_test_pf']:>5.2f} │ "
              f"{res['avg_test_pnl']:>+5.1f}% │ "
              f"≥{res['best_score']:>3}  │ "
              f"{icon} {res['tier']}")

    # Khuyến nghị cuối
    high  = [s for s, r in all_results.items() if r['tier'] == 'HIGH']
    mid   = [s for s, r in all_results.items() if r['tier'] == 'MEDIUM']
    fail  = [s for s, r in all_results.items() if r['tier'] == 'FAIL']

    print(f"\n  {'─'*70}")
    print(f"  KẾT LUẬN HỆ THỐNG:")
    if high:
        print(f"  ✅ Tin cậy cao  : {', '.join(high)}")
    if mid:
        print(f"  🟡 Chấp nhận   : {', '.join(mid)}")
    if fail:
        print(f"  ❌ Loại bỏ     : {', '.join(fail)}")
    print(f"\n  → Chỉ phát tín hiệu LIVE trên: "
          f"{', '.join(high + mid) if (high or mid) else 'Chưa có mã đạt yêu cầu'}")

    return all_results


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────

if __name__ == '__main__':
    args = sys.argv[1:]

    if not args:
        print("Walk-Forward Validation — VN Trader Bot")
        print("Dùng:")
        print("  python walk_forward.py DCM              # 1 mã")
        print("  python walk_forward.py DCM DGC GAS      # nhiều mã")
        print("  python walk_forward.py --top            # top 9 mã đã chọn lọc")
        print("\nChạy demo với DCM...\n")
        walk_forward_symbol('DCM', verbose=True)

    elif args[0] == '--top':
        print(f"Chạy Walk-Forward toàn bộ {len(TOP_SYMBOLS)} mã top...\n")
        walk_forward_multi(TOP_SYMBOLS)

    elif len(args) == 1:
        walk_forward_symbol(args[0].upper(), verbose=True)

    else:
        symbols = [s.upper() for s in args]
        walk_forward_multi(symbols)
