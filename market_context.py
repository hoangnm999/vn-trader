"""
market_context.py — Phân tích đặc tính thị trường Việt Nam (Giai đoạn 1)
=========================================================================
Dựa trên Blueprint "VN Trading Signal" — 4 checks nhanh:

  [1] Liquidity Tier   : ADTV so với ngưỡng VN30/VNMID/Ngoài index
  [2] Wick Filter      : Phát hiện wash trading / làm giá qua râu nến bất thường
  [3] Weekend Rule     : Cảnh báo vào lệnh cuối tuần (T4, T5 nguy hiểm nhất)
  [4] Wyckoff Phase    : Phát hiện UTAD (bẫy tăng trước phân phối)

Chạy nhanh — không cần download thêm dữ liệu, dùng lại data từ vnstock.
"""

import numpy as np
import pandas as pd
from datetime import datetime
import pytz

VN_TZ = pytz.timezone('Asia/Ho_Chi_Minh')

# ── Ngưỡng thanh khoản (Blueprint Section 3) ─────────────────────────────────
LIQUIDITY_TIERS = {
    'tier1': 20_000_000_000,   # >= 20 tỷ/phiên — VN30, an toàn tuyệt đối
    'tier2': 10_000_000_000,   # 10-20 tỷ/phiên — VNMID, cẩn thận
    'tier3':  5_000_000_000,   # 5-10 tỷ/phiên  — Ngoài index, mặc định tắt
}

# ── Wick filter config (Blueprint Section 3) ─────────────────────────────────
WICK_RATIO_THRESHOLD = 3.0    # Wick/body > 3.0 = bất thường
WICK_MAX_DAYS        = 4      # Tối đa 4 ngày wick bất thường trong 20 phiên


def analyze_liquidity_tier(df, symbol=''):
    """
    [1] Xác định Liquidity Tier từ ADTV 20 phiên gần nhất.
    Trả về dict: tier (1/2/3/0), adtv, label, emoji, warning
    """
    # Tìm cột volume và close
    vc = next((c for c in df.columns if c.lower() in {
        'volume','volume_match','klgd','vol','trading_volume',
        'match_volume','total_volume','dealvolume','matchingvolume'}), None)
    cc = next((c for c in df.columns if c.lower() in
               ['close','closeprice','close_price']), None)

    if vc is None or cc is None:
        return {'tier': -1, 'adtv': 0, 'label': 'Khong xac dinh',
                'emoji': '&#x2753;', 'warning': ''}

    vols   = pd.to_numeric(df[vc], errors='coerce').fillna(0).values.copy()
    closes = pd.to_numeric(df[cc], errors='coerce').fillna(0).values.copy()
    if closes.max() < 1000:
        closes = closes * 1000

    recent = min(20, len(vols))
    avg_vol   = float(np.mean(vols[-recent:][vols[-recent:] > 0])) if np.any(vols[-recent:] > 0) else 0
    avg_price = float(np.mean(closes[-recent:][closes[-recent:] > 0])) if np.any(closes[-recent:] > 0) else 0
    adtv = avg_vol * avg_price  # Giá trị giao dịch TB (VND)

    if adtv >= LIQUIDITY_TIERS['tier1']:
        return {
            'tier': 1, 'adtv': adtv,
            'label': 'Tier 1 - VN30 (An toan tuyet doi)',
            'emoji': '&#x1F7E2;',
            'warning': '',
            'max_pos_pct': 5,   # Max 5% ADTV
        }
    elif adtv >= LIQUIDITY_TIERS['tier2']:
        return {
            'tier': 2, 'adtv': adtv,
            'label': 'Tier 2 - VNMID (Can than)',
            'emoji': '&#x1F7E1;',
            'warning': 'Stop buffer nen rong hon 1.5x binh thuong',
            'max_pos_pct': 3,
        }
    elif adtv >= LIQUIDITY_TIERS['tier3']:
        return {
            'tier': 3, 'adtv': adtv,
            'label': 'Tier 3 - Ngoai index (Rui ro)',
            'emoji': '&#x1F7E0;',
            'warning': 'Thanh khoan thap - kho thoat khi can. Stop 2x binh thuong',
            'max_pos_pct': 2,
        }
    else:
        return {
            'tier': 0, 'adtv': adtv,
            'label': 'TRAI - Duoi 5 ty/phien',
            'emoji': '&#x274C;',
            'warning': 'KHONG TRADE - Liquidity Illusion nguy hiem cao',
            'max_pos_pct': 0,
        }


def analyze_wick_filter(df, symbol=''):
    """
    [2] Phát hiện wash trading / làm giá qua râu nến bất thường.
    Wick/body > 3.0 trong > 4/20 phiên = dấu hiệu quay tay.
    """
    oc = next((c for c in df.columns if c.lower() in ['open','openprice','open_price']), None)
    hc = next((c for c in df.columns if c.lower() in ['high','highprice','high_price']), None)
    lc = next((c for c in df.columns if c.lower() in ['low','lowprice','low_price']), None)
    cc = next((c for c in df.columns if c.lower() in ['close','closeprice','close_price']), None)

    if not all([oc, hc, lc, cc]):
        return {'clean': True, 'abnormal_days': 0, 'total_checked': 0,
                'emoji': '&#x2705;', 'label': 'Khong du du lieu', 'warning': ''}

    opens  = pd.to_numeric(df[oc], errors='coerce').fillna(0).values[-20:].copy().copy()
    highs  = pd.to_numeric(df[hc], errors='coerce').fillna(0).values[-20:].copy().copy()
    lows   = pd.to_numeric(df[lc], errors='coerce').fillna(0).values[-20:].copy().copy()
    closes = pd.to_numeric(df[cc], errors='coerce').fillna(0).values.copy()[-20:]

    abnormal = 0
    for i in range(len(closes)):
        body = abs(closes[i] - opens[i])
        if body < 1:
            continue  # Doji — bỏ qua
        upper_wick = highs[i] - max(opens[i], closes[i])
        lower_wick = min(opens[i], closes[i]) - lows[i]
        total_wick = upper_wick + lower_wick
        wick_ratio = total_wick / body
        if wick_ratio > WICK_RATIO_THRESHOLD:
            abnormal += 1

    checked = len(closes)
    is_clean = abnormal <= WICK_MAX_DAYS

    if abnormal == 0:
        emoji = '&#x2705;'
        label = f'0/{checked} nen bat thuong - Sach'
        warning = ''
    elif is_clean:
        emoji = '&#x1F7E1;'
        label = f'{abnormal}/{checked} nen co rau bat thuong - Chap nhan'
        warning = 'Co mot so phien rau lon, theo doi them'
    else:
        emoji = '&#x274C;'
        label = f'{abnormal}/{checked} nen rau bat thuong - NGHI NGO LAM GIA'
        warning = 'Nhieu phien rau bat thuong - co the la wash trading / quay tay'

    return {
        'clean': is_clean,
        'abnormal_days': abnormal,
        'total_checked': checked,
        'emoji': emoji,
        'label': label,
        'warning': warning,
    }


def analyze_weekend_rule(symbol=''):
    """
    [3] Weekend Rule — Cảnh báo vào lệnh cuối tuần.
    Regulatory shock VN thường xảy ra cuối tuần.

    Phân cấp theo weekday (0=T2 … 4=T6):
      T6 (weekday=4) : NGUY HIỂM — không mở vị thế mới
      T5 (weekday=3) : CẢNH BÁO — cẩn thận trước cuối tuần
      T4 cuối phiên  : THEO DÕI — chú ý tin tức tối T4/sáng T5
        (weekday=2 AND hour >= 14)
      Còn lại        : AN TOÀN

    FIX: Bug cũ có 2 elif weekday==3 → elif thứ 2 (T4 cuối phiên) không bao giờ
    chạy vì bị bắt bởi elif đầu tiên. Sửa: tách T4 cuối phiên = weekday==2 (T4).
    """
    now     = datetime.now(VN_TZ)
    weekday = now.weekday()   # 0=T2, 1=T3, 2=T4, 3=T5, 4=T6
    hour    = now.hour

    # Sau 14:00 = gần đóng cửa / ATC
    near_close = hour >= 14

    if weekday == 4:  # Thứ 6
        return {
            'safe':    False,
            'level':   'NGUY HIEM',
            'emoji':   '&#x1F534;',
            'label':   'Thu 6 - Weekend Rule: KHONG nen mo vi the moi',
            'warning': (
                'Regulatory shock VN thuong xay ra cuoi tuan. '
                'ATO sang thu 2 co the la san trang neu co tin xau. '
                'Neu bat buoc vao lenh: giam size xuong con 50%'
            ),
        }
    elif weekday == 3:  # Thứ 5
        return {
            'safe':    False,
            'level':   'CANH BAO',
            'emoji':   '&#x26A0;',
            'label':   'Thu 5 - Canh bao: Can than truoc cuoi tuan',
            'warning': 'Neu mo vi the, dam bao co "dem" loi nhuan it nhat 10% truoc thu 6',
        }
    elif weekday == 2 and near_close:  # Thứ 4 cuối phiên (FIX: weekday==2, không phải 3)
        return {
            'safe':    True,    # Vẫn an toàn nhưng cần chú ý
            'level':   'THEO DOI',
            'emoji':   '&#x1F7E1;',
            'label':   'Cuoi phien Thu 4 - Theo doi tin tuc toi nay / sang Thu 5',
            'warning': 'Neu co tin xau toi T4: ATO sang T5 co the gap gia thap hon',
        }
    else:
        day_names = ['Thu 2', 'Thu 3', 'Thu 4', 'Thu 5', 'Thu 6', 'Thu 7', 'CN']
        return {
            'safe':    True,
            'level':   'AN TOAN',
            'emoji':   '&#x2705;',
            'label':   f'{day_names[weekday]} - OK vao lenh',
            'warning': '',
        }


# ── Wyckoff config — chỉnh tại đây ──────────────────────────────────────────
# Tăng lookback để phân loại phase chính xác hơn (Wyckoff chuẩn cần 60-120 nến)
WYCKOFF_LOOKBACK      = 90    # Phiên lookback chính (phase detection, vol MA)
WYCKOFF_SIDEWAY_WIN   = 60    # Cửa sổ sideway (trước: 20 → giờ 60)
WYCKOFF_HIGH_WIN      = 60    # Cửa sổ tìm đỉnh để detect breakout (trước: 30 → giờ 60)
WYCKOFF_TREND_WIN     = 40    # Cửa sổ tính price trend (trước: 20 → giờ 40)
WYCKOFF_SHORT_WIN     = 10    # Cửa sổ ngắn hạn (momentum 5-10 nến gần nhất)
WYCKOFF_VOL_MA_WIN    = 40    # Volume MA window (trước: 20 → giờ 40, ổn định hơn)

# Ngưỡng sideway — TTCK VN biên ±7%/phiên nên cần ngưỡng cao hơn thị trường khác
# Trước: 8% trong 20 phiên → quá thấp → nhiều mã bị nhận nhầm là sideway
# Sau: 15% trong 60 phiên → chỉ sideway thực sự mới pass
WYCKOFF_SIDEWAY_THR   = 0.15  # Biến động < 15% trong 60 phiên = sideway thật

# Ngưỡng breakout — loại bỏ false breakout do noise
# Trước: 99.8% đỉnh → trigger quá dễ
# Sau: giá phải vượt đỉnh thực sự (>= 100%)
WYCKOFF_BREAKOUT_THR  = 1.000 # Phải phá hoặc bằng đỉnh thực (bỏ 0.2% buffer nhỏ)

# Ngưỡng volume cho UTAD — tăng để tránh false positive
# Trước: 1.8x → dễ trigger quá
# Sau: 2.5x → chỉ volume đột biến thực sự mới cảnh báo
WYCKOFF_UTAD_VOL_THR  = 2.5   # Trước: 1.8x

# Ngưỡng đóng cửa yếu (close_pct) — giữ nguyên 0.6 vì logic này đúng
WYCKOFF_CLOSE_WEAK    = 0.6

# Ngưỡng volume cho Accumulation/Distribution — tăng nhẹ để tin cậy hơn
WYCKOFF_ACC_VOL_THR   = 1.5   # Trước: 1.3x
WYCKOFF_DIST_VOL_THR  = 1.5   # Trước: 1.3x

# Ngưỡng trend để detect Markup/Markdown — tính trên cửa sổ dài hơn
WYCKOFF_MARKUP_THR    = 0.08  # Trước: 5% trong 20 phiên → giờ 8% trong 40 phiên
WYCKOFF_MARKDOWN_THR  = -0.08 # Trước: -5% → giờ -8%
# ─────────────────────────────────────────────────────────────────────────────


def analyze_wyckoff_phase(df, symbol=''): 
    """
    [4] Phát hiện Wyckoff phase — dùng 60-120 nến, ngưỡng sideway thực tế cho TTCK VN.

    Cải tiến so với phiên bản cũ:
    - Lookback 90 nến (từ 30) → phân loại phase chính xác hơn
    - Sideway window 60 nến, ngưỡng 15% (từ 20 nến, 8%) → phù hợp biên ±7%/phiên của VN
    - UTAD volume threshold 2.5x (từ 1.8x) → giảm false positive
    - Breakout phải phá đỉnh thực sự (từ 99.8%) → loại noise
    - Tất cả thông số cấu hình qua constants ở trên

    UTAD = Upthrust After Distribution:
    - Giá phá đỉnh 60 phiên NHƯNG đóng cửa yếu (< 60% range ngày)
    - Volume đột biến >= 2.5x MA40
    - Xảy ra sau sideway thực sự >= 60 phiên, biến động < 15%
    """
    cc = next((c for c in df.columns if c.lower() in ['close','closeprice','close_price']), None)
    vc = next((c for c in df.columns if c.lower() in {
        'volume','volume_match','klgd','vol','trading_volume',
        'match_volume','total_volume','dealvolume','matchingvolume'}), None)
    hc = next((c for c in df.columns if c.lower() in ['high','highprice','high_price']), None)

    if cc is None or vc is None:
        return {'phase': 'UNKNOWN', 'emoji': '&#x2753;',
                'label': 'Khong du du lieu', 'warning': '', 'utad_risk': False}

    closes  = pd.to_numeric(df[cc], errors='coerce').fillna(0).values.copy()
    volumes = pd.to_numeric(df[vc], errors='coerce').fillna(0).values.copy()
    highs   = pd.to_numeric(df[hc], errors='coerce').fillna(0).values if hc else closes.copy()

    if closes.max() < 1000: closes = (closes * 1000).copy()
    if highs.max()  < 1000: highs  = (highs  * 1000).copy()

    n = len(closes)

    # Cần tối thiểu WYCKOFF_LOOKBACK nến để phân loại đáng tin cậy
    if n < WYCKOFF_LOOKBACK:
        if n < 30:
            return {'phase': 'UNKNOWN', 'emoji': '&#x2753;',
                    'label': f'Khong du du lieu (can >= {WYCKOFF_LOOKBACK} phien, co {n})',
                    'warning': '', 'utad_risk': False}
        # Có đủ dữ liệu cơ bản nhưng ít hơn ideal → giảm độ tin cậy
        confidence = 'THAP'
    else:
        confidence = 'CAO'

    # ── Trích xuất các cửa sổ dữ liệu ───────────────────────────────────────
    sw  = min(WYCKOFF_SIDEWAY_WIN, n)   # sideway window
    hw  = min(WYCKOFF_HIGH_WIN,   n)   # high window
    tw  = min(WYCKOFF_TREND_WIN,  n)   # trend window
    shw = min(WYCKOFF_SHORT_WIN,  n)   # short window
    vw  = min(WYCKOFF_VOL_MA_WIN, n)   # vol MA window

    c_sw  = closes[-sw:]
    c_hw  = closes[-hw:]; h_hw = highs[-hw:]
    c_tw  = closes[-tw:]
    c_sh  = closes[-shw:]
    v_vw  = volumes[-vw:]

    # ── Volume MA (dùng cửa sổ dài hơn → ổn định hơn) ───────────────────────
    v_valid  = v_vw[v_vw > 0]
    vol_ma   = float(np.mean(v_valid)) if len(v_valid) > 0 else 1.0
    vol_today = float(volumes[-1])
    vol_ratio = vol_today / vol_ma if vol_ma > 0 else 1.0

    # ── Sideway detection — dùng 60 nến, ngưỡng 15% ──────────────────────────
    # Lý do: TTCK VN biên ±7%/phiên, cần 60 nến mới thấy pattern sideway thực
    # 20 nến × 7% noise = 140% range lý thuyết → ngưỡng 8% quá thấp
    c_sw_mean = c_sw.mean()
    price_range_sw = (c_sw.max() - c_sw.min()) / c_sw_mean if c_sw_mean > 0 else 0
    is_sideways = price_range_sw < WYCKOFF_SIDEWAY_THR

    # ── Trend detection — dùng 40 nến ────────────────────────────────────────
    price_trend_long  = (c_tw[-1] - c_tw[0]) / c_tw[0] if c_tw[0] > 0 else 0
    price_trend_short = (c_sh[-1] - c_sh[0]) / c_sh[0] if c_sh[0] > 0 else 0

    # ── Breakout detection — phải phá đỉnh thực của 60 phiên ─────────────────
    high_hw       = float(h_hw.max())
    cur_high      = float(highs[-1])
    is_new_high_breakout = cur_high >= high_hw * WYCKOFF_BREAKOUT_THR

    # ── Phát hiện UTAD — điều kiện chặt hơn ──────────────────────────────────
    utad_risk   = False
    utad_detail = ''

    if is_sideways and is_new_high_breakout and vol_ratio >= WYCKOFF_UTAD_VOL_THR:
        # Giá phá đỉnh 60 phiên sau sideway thực + volume đột biến mạnh
        last_close = float(closes[-1])
        last_high  = float(highs[-1])
        recent_low = float(closes[-shw:].min())
        close_range = last_high - recent_low
        close_pct   = (last_close - recent_low) / close_range if close_range > 0 else 1.0
        if close_pct < WYCKOFF_CLOSE_WEAK:
            utad_risk   = True
            utad_detail = (
                f'Pha dinh {hw}p + Vol {vol_ratio:.1f}x MA{vw} + Nen yeu ({close_pct*100:.0f}% range) = UTAD'
            )

    # ── Phân loại Wyckoff phase ───────────────────────────────────────────────
    conf_note = '' if confidence == 'CAO' else f' (du lieu it: {n}/{WYCKOFF_LOOKBACK}p)'

    if utad_risk:
        phase   = 'UTAD'
        emoji   = '&#x1F6A8;'
        label   = f'UTAD - Bay tang! Pha dinh {hw}p tren vol {vol_ratio:.1f}x{conf_note}'
        warning = (
            'CANH BAO WYCKOFF: Co the la Upthrust After Distribution.\n'
            'Gia pha dinh gia tao + Vol dot bien + Nen yeu = dau hieu xa hang.\n'
            'KHOA LENH MUA cho den khi co xac nhan xu huong that su.'
        )
    elif is_sideways and vol_ratio >= WYCKOFF_ACC_VOL_THR and price_trend_short >= -0.02:
        phase   = 'ACCUMULATION'
        emoji   = '&#x1F4C8;'
        label   = f'Tich luy (Acc) - Sideway {sw}p + Vol {vol_ratio:.1f}x{conf_note}'
        warning = 'Gia sideway dai + Volume cao = co the dang gom. Cho breakout xac nhan.'
    elif not is_sideways and price_trend_long > WYCKOFF_MARKUP_THR and vol_ratio >= 0.8:
        phase   = 'MARKUP'
        emoji   = '&#x1F680;'
        label   = f'Tang gia (Markup) - Trend {price_trend_long*100:+.1f}% / {tw}p{conf_note}'
        warning = ''
    elif is_sideways and vol_ratio >= WYCKOFF_DIST_VOL_THR and price_trend_short < -0.01:
        phase   = 'DISTRIBUTION'
        emoji   = '&#x26A0;'
        label   = f'Phan phoi (Dist) - Sideway {sw}p + giam nhe + Vol {vol_ratio:.1f}x{conf_note}'
        warning = 'Gia sideway + xu huong giam nhe + Vol cao = co the dang phan phoi.'
    elif price_trend_long < WYCKOFF_MARKDOWN_THR:
        phase   = 'MARKDOWN'
        emoji   = '&#x1F4C9;'
        label   = f'Giam gia (Markdown) - Trend {price_trend_long*100:+.1f}% / {tw}p{conf_note}'
        warning = 'Tranh mua trong xu huong giam. Cho tin hieu dao chieu.'
    else:
        phase   = 'NEUTRAL'
        emoji   = '&#x27A1;'
        label   = f'Trung tinh - Chua ro Wyckoff phase{conf_note}'
        warning = ''

    return {
        'phase':             phase,
        'emoji':             emoji,
        'label':             label,
        'warning':           warning,
        'utad_risk':         utad_risk,
        'utad_detail':       utad_detail,
        'is_sideways':       is_sideways,
        'vol_ratio':         round(vol_ratio, 2),
        'price_range_pct':   round(price_range_sw * 100, 1),
        'price_trend_long':  round(price_trend_long * 100, 1),
        'sideways_window':   sw,
        'confidence':        confidence,
    }


def build_market_context(df, symbol='', price=0, vol_ratio=1.0, score=50):
    """
    Tổng hợp 4 checks thành 1 dict hoàn chỉnh.
    Trả về dict với tất cả phân tích + overall_flag.
    """
    liq     = analyze_liquidity_tier(df, symbol)
    wick    = analyze_wick_filter(df, symbol)
    weekend = analyze_weekend_rule(symbol)
    wyckoff = analyze_wyckoff_phase(df, symbol)

    # ── Overall flag ──────────────────────────────────────────────────────────
    red_flags = []
    if liq['tier'] == 0:
        red_flags.append('Thanh khoan TRAI - khong trade')
    if liq['tier'] == 3:
        red_flags.append('Thanh khoan thap (Tier 3)')
    if not wick['clean']:
        red_flags.append(f'Wick bat thuong {wick["abnormal_days"]}/20 phien')
    if not weekend['safe']:
        red_flags.append(f'Weekend Rule: {weekend["level"]}')
    if wyckoff['utad_risk']:
        red_flags.append('UTAD - Bay tang nguy hiem!')
    elif wyckoff['phase'] == 'DISTRIBUTION':
        red_flags.append('Wyckoff: Phan phoi')
    elif wyckoff['phase'] == 'MARKDOWN':
        red_flags.append('Wyckoff: Xu huong giam')

    n_flags = len(red_flags)
    if n_flags == 0:
        overall = 'THUAN LOI'
        overall_emoji = '&#x2705;'
    elif n_flags == 1 and not wyckoff['utad_risk'] and liq['tier'] > 0:
        overall = 'CHAP NHAN'
        overall_emoji = '&#x1F7E1;'
    elif wyckoff['utad_risk'] or liq['tier'] == 0:
        overall = 'NGUY HIEM'
        overall_emoji = '&#x1F534;'
    else:
        overall = 'CAN THAN'
        overall_emoji = '&#x26A0;'

    return {
        'liquidity':  liq,
        'wick':       wick,
        'weekend':    weekend,
        'wyckoff':    wyckoff,
        'red_flags':  red_flags,
        'overall':    overall,
        'overall_emoji': overall_emoji,
    }


def format_market_context_msg(ctx, symbol=''):
    """Format dict tu build_market_context thanh HTML cho Telegram.
    FIX: Hard block (Liquidity TRAI, UTAD) được highlight lên đầu để trader
    không bỏ qua khi đọc nhanh. Wyckoff chỉ hiện nếu có warning thực sự.
    """
    liq  = ctx['liquidity']
    wick = ctx['wick']
    wknd = ctx['weekend']
    wyck = ctx['wyckoff']

    adtv_b = liq['adtv'] / 1_000_000_000
    adtv_s = (str(round(adtv_b, 1)) + ' ty/phien'
              if adtv_b >= 1 else
              str(round(liq['adtv'] / 1_000_000)) + ' trieu/phien')

    lines = []

    # ── Hard block banner — hiện đầu tiên nếu có ────────────────────────────
    # Lý do: trader đọc nhanh có thể bỏ qua [1] Liquidity ở giữa message
    if liq['tier'] == 0:
        lines.append(
            '🔴 <b>⚠ HARD BLOCK: Thanh khoản TRAI (' + adtv_s + ')</b>' + chr(10)
            + '   Không trade dù score kỹ thuật cao — Liquidity Illusion nguy hiểm'
        )
    elif wyck.get('utad_risk'):
        lines.append(
            '🔴 <b>⚠ HARD BLOCK: UTAD phát hiện</b>' + chr(10)
            + '   ' + wyck.get('utad_detail', 'Bay tang nguy hiem — tranh vao lenh')
        )

    # [1] Liquidity — label rút gọn nếu đã có banner
    liq_txt = (liq['emoji'] + ' <b>[1] Thanh khoan:</b> ' + liq['label']
               + chr(10) + '   ADTV 20 phien: ' + adtv_s)
    if liq['warning'] and liq['tier'] != 0:  # Tier 0 đã có banner trên
        liq_txt += chr(10) + '   &#x26A0; ' + liq['warning']
    lines.append(liq_txt)

    # [2] Wick filter
    wick_txt = wick['emoji'] + ' <b>[2] Wick Filter:</b> ' + wick['label']
    if wick['warning']:
        wick_txt += chr(10) + '   &#x26A0; ' + wick['warning']
    lines.append(wick_txt)

    # [3] Weekend rule — highlight nếu NGUY HIEM
    wknd_txt = wknd['emoji'] + ' <b>[3] Weekend Rule:</b> ' + wknd['label']
    if wknd['warning']:
        wknd_txt += chr(10) + '   &#x26A0; ' + wknd['warning']
    lines.append(wknd_txt)

    # [4] Wyckoff — chỉ hiện chi tiết nếu có warning (NEUTRAL im lặng)
    wyck_txt = wyck['emoji'] + ' <b>[4] Wyckoff Phase:</b> ' + wyck['label']
    if wyck.get('utad_risk') and liq['tier'] != 0:  # UTAD đã có banner, chỉ thêm detail nếu chưa có
        wyck_txt += chr(10) + '   &#x1F6A8; ' + wyck.get('utad_detail', '')
    elif wyck.get('warning') and not wyck.get('utad_risk'):
        wyck_txt += chr(10) + '   &#x1F4CC; ' + wyck['warning']
    lines.append(wyck_txt)

    # Summary
    flags = ctx['red_flags']
    if flags:
        lines.append(chr(10) + ctx['overall_emoji'] + ' <b>Canh bao:</b> ' + ' | '.join(flags))
    else:
        lines.append(chr(10) + ctx['overall_emoji']
                     + ' <b>Dac tinh thi truong VN: ' + ctx['overall'] + '</b>')

    return chr(10).join(lines)
def analyze_macro_risk(vnindex_data=None, watchlist_scores=None):
    """
    Tính Systemic Risk Score từ:
      [A] VNINDEX trend vs MA20 (35 điểm)
      [B] Market breadth — % mã tăng/giảm (30 điểm)
      [C] VNINDEX volatility — độ dao động bất thường (20 điểm)
      [D] Weekend amplifier — cuối tuần nhân hệ số (15 điểm)

    vnindex_data   : dict từ /api/market (price, change_pct, ma20, ...)
    watchlist_scores: list dict {'symbol':..., 'score':..., 'action':...} từ cache
    """
    risk_score = 0
    components = {}

    # ── [A] VNINDEX vs MA20 ────────────────────────────────────────────────
    a_score = 0
    if vnindex_data:
        vn_price  = vnindex_data.get('price', 0)
        vn_ma20   = vnindex_data.get('ma20', 0)
        vn_chg    = vnindex_data.get('change_pct', 0)  # % thay đổi hôm nay

        if vn_price > 0 and vn_ma20 > 0:
            dist_ma20 = (vn_price - vn_ma20) / vn_ma20 * 100  # % trên/dưới MA20
            if dist_ma20 >= 3:
                a_score = 0    # Tốt — xa trên MA20
            elif dist_ma20 >= 0:
                a_score = 10   # OK — trên MA20 nhưng gần
            elif dist_ma20 >= -3:
                a_score = 20   # Cảnh báo — dưới MA20 chút
            elif dist_ma20 >= -7:
                a_score = 28   # Nguy hiểm — dưới MA20 đáng kể
            else:
                a_score = 35   # Rất nguy hiểm — xa dưới MA20

        # Bonus: hôm nay giảm mạnh
        if vn_chg <= -2:
            a_score = min(35, a_score + 10)
        elif vn_chg <= -1:
            a_score = min(35, a_score + 5)

        components['vnindex'] = {
            'price': vn_price, 'ma20': vn_ma20,
            'dist_pct': round((vn_price - vn_ma20) / vn_ma20 * 100, 1) if vn_ma20 > 0 else 0,
            'change_pct': vn_chg, 'score': a_score
        }
    else:
        a_score = 15  # Không có data → mặc định cảnh báo vừa
        components['vnindex'] = {'score': a_score, 'note': 'Khong co du lieu'}

    risk_score += a_score

    # ── [B] Market Breadth ─────────────────────────────────────────────────
    b_score = 0
    if watchlist_scores and len(watchlist_scores) >= 5:
        n_total   = len(watchlist_scores)
        n_buy     = sum(1 for x in watchlist_scores if x.get('action') == 'MUA')
        n_sell    = sum(1 for x in watchlist_scores if x.get('action') == 'BAN')
        n_watch   = n_total - n_buy - n_sell
        breadth   = n_buy / n_total * 100  # % mã có tín hiệu MUA

        avg_score = sum(x.get('score', 50) for x in watchlist_scores) / n_total

        if breadth >= 40:
            b_score = 0   # Thị trường rộng khỏe
        elif breadth >= 25:
            b_score = 8
        elif breadth >= 10:
            b_score = 18
        else:
            b_score = 25  # Rất ít mã tăng

        # Nếu avg score thấp → thị trường yếu
        if avg_score < 40:
            b_score = min(30, b_score + 8)

        components['breadth'] = {
            'n_buy': n_buy, 'n_sell': n_sell, 'n_watch': n_watch,
            'breadth_pct': round(breadth, 1),
            'avg_score': round(avg_score, 1), 'score': b_score
        }
    else:
        b_score = 10
        components['breadth'] = {'score': b_score, 'note': 'Khong du du lieu'}

    risk_score += b_score

    # ── [C] Volatility Check ───────────────────────────────────────────────
    c_score = 0
    if vnindex_data:
        chg = abs(vnindex_data.get('change_pct', 0))
        if chg >= 3:
            c_score = 20   # Biến động cực lớn
        elif chg >= 2:
            c_score = 12
        elif chg >= 1.5:
            c_score = 6
        else:
            c_score = 0
        components['volatility'] = {'change_abs': round(chg, 2), 'score': c_score}

    risk_score += c_score

    # ── [D] Weekend Amplifier ──────────────────────────────────────────────
    now = datetime.now(VN_TZ)
    weekday = now.weekday()
    d_score = 0
    if weekday == 4:    # Thứ 6
        d_score = 15
    elif weekday == 3:  # Thứ 5
        d_score = 7
    components['weekend'] = {'weekday': weekday, 'score': d_score}
    risk_score += d_score

    # ── Kết luận ──────────────────────────────────────────────────────────
    risk_score = min(100, max(0, risk_score))

    if risk_score < 30:
        status = 'XANH'
        emoji  = '&#x1F7E2;'
        action = 'Trade binh thuong theo he thong'
        size_pct = 100
    elif risk_score < 50:
        status = 'VANG'
        emoji  = '&#x1F7E1;'
        action = 'Giam size 30%, tang cash'
        size_pct = 70
    elif risk_score < 70:
        status = 'CAM'
        emoji  = '&#x1F7E0;'
        action = 'Giam size 60%, chi Tier 1, khong margin'
        size_pct = 40
    elif risk_score < 85:
        status = 'DO'
        emoji  = '&#x1F534;'
        action = 'Dong 70% vi the, chi nam cash + ETF lon'
        size_pct = 15
    else:
        status = 'DO THAM'
        emoji  = '&#x1F198;'
        action = 'THOAT HET — chuan bi watchlist mua tai day'
        size_pct = 0

    return {
        'score':       risk_score,
        'status':      status,
        'emoji':       emoji,
        'action':      action,
        'size_pct':    size_pct,
        'components':  components,
    }


def format_macro_risk_msg(macro):
    """Format Macro Risk result thành HTML cho Telegram."""
    score   = macro['score']
    status  = macro['status']
    emoji   = macro['emoji']
    action  = macro['action']
    size    = macro['size_pct']
    comp    = macro['components']

    # Progress bar
    filled = round(score / 10)
    bar    = '&#x2588;' * filled + '&#x2591;' * (10 - filled)

    vn = comp.get('vnindex', {})
    br = comp.get('breadth', {})

    vn_line = ''
    if 'price' in vn:
        dist_s = f'{vn["dist_pct"]:+.1f}%'
        chg_s  = f'{vn["change_pct"]:+.2f}%'
        vn_line = f'VN-INDEX: {vn["price"]:,.2f} ({chg_s}) | MA20 {dist_s}'
    else:
        vn_line = 'VN-INDEX: Khong co du lieu'

    br_line = ''
    if 'breadth_pct' in br:
        br_line = (f'Breadth: {br["n_buy"]}MUA / {br["n_sell"]}BAN / {br["n_watch"]}TD '
                   f'({br["breadth_pct"]:.0f}% ma MUA) | Score TB: {br["avg_score"]:.0f}')
    else:
        br_line = 'Breadth: Khong du du lieu'

    return (
        f'{emoji} <b>MACRO RISK: {status}</b> ({score}/100)\\n'
        f'{bar} {score}%\\n\\n'
        f'{vn_line}\\n'
        f'{br_line}\\n\\n'
        f'&#x1F3AF; <b>Hanh dong:</b> {action}\\n'
        f'&#x1F4CF; <b>Size de xuat:</b> {size}% vi the binh thuong'
    )


# ═══════════════════════════════════════════════════════════════════════════════
# HÀM CHUNG — Tính B-adjustment (cộng/trừ điểm)
# Dùng chung cho: build_analysis_msg / handle_signals / backtest
# ═══════════════════════════════════════════════════════════════════════════════

B_ADJUSTMENTS = {
    # Trừ điểm — rủi ro
    'UTAD':         {'delta': -25, 'icon': '&#x1F534;', 'label': 'UTAD (bay tang)'},
    'DISTRIBUTION': {'delta': -15, 'icon': '&#x1F534;', 'label': 'Distribution (xa hang)'},
    'MARKDOWN':     {'delta': -10, 'icon': '&#x26A0;',  'label': 'Markdown (giam gia)'},
    'TIER0':        {'delta': -20, 'icon': '&#x1F534;', 'label': 'Thanh khoan TRAI'},
    'TIER3':        {'delta':  -8, 'icon': '&#x26A0;',  'label': 'Thanh khoan Tier3'},
    'WICK':         {'delta':  -5, 'icon': '&#x1F7E1;', 'label': 'Wick bat thuong'},
    # Cộng điểm — cơ hội
    'ACCUMULATION': {'delta': +8,  'icon': '&#x1F4C8;', 'label': 'Tich luy (Accumulation)'},
    'MARKUP':       {'delta': +5,  'icon': '&#x1F680;', 'label': 'Tang gia (Markup)'},
    'TIER1_CLEAN':  {'delta': +3,  'icon': '&#x2705;',  'label': 'Tier1 + Wick sach'},
}


def calc_b_adjustment(ctx):
    """
    Tính tổng điều chỉnh B từ market context.
    Trả về:
      delta     : số điểm điều chỉnh (âm = trừ, dương = cộng)
      flags     : list các flag được kích hoạt (key từ B_ADJUSTMENTS)
      details   : list dict {'key', 'delta', 'icon', 'label'} để hiển thị
    """
    if not ctx:
        return 0, [], []

    wyck = ctx.get('wyckoff', {})
    liq  = ctx.get('liquidity', {})
    wick = ctx.get('wick', {})

    delta   = 0
    flags   = []
    details = []

    def _add(key):
        nonlocal delta
        adj = B_ADJUSTMENTS[key]
        delta += adj['delta']
        flags.append(key)
        details.append({
            'key':   key,
            'delta': adj['delta'],
            'icon':  adj['icon'],
            'label': adj['label'],
        })

    # ── Trừ điểm (rủi ro) — ưu tiên cao nhất ────────────────────────────────
    if wyck.get('utad_risk'):
        _add('UTAD')
    elif wyck.get('phase') == 'DISTRIBUTION':
        _add('DISTRIBUTION')
    elif wyck.get('phase') == 'MARKDOWN':
        _add('MARKDOWN')

    if liq.get('tier') == 0:
        _add('TIER0')
    elif liq.get('tier') == 3:
        _add('TIER3')

    if not wick.get('clean', True):
        _add('WICK')

    # ── Cộng điểm: ĐÃ TẮT dựa trên kết quả backtest 21 mã ─────────────────
    # Backtest cho thấy Accumulation/Markup/Tier1_Clean tạo thêm lệnh MUA
    # nhưng các lệnh đó lại thua → WR giảm đáng kể (VCB -15%, VND -12%)
    # Kết luận: B chỉ hiệu quả khi TRỪ điểm (bảo vệ), không hiệu quả khi CỘNG
    # → B = bộ lọc bảo vệ, KHÔNG phải bộ tạo signal thêm

    return delta, flags, details


def format_b_adjustment(delta, details, score_a, score_ab):
    """
    Format kết quả B-adjustment thành HTML ngắn gọn cho Telegram.
    """
    if not details:
        return ''

    lines = []
    for d in details:
        sign = '+' if d['delta'] > 0 else ''
        lines.append(
            d['icon'] + ' ' + d['label']
            + ' (' + sign + str(d['delta']) + 'd)'
        )

    sign_total = '+' if delta > 0 else ''
    summary    = (
        'Dieu chinh B: ' + sign_total + str(delta) + 'd'
        + ' (' + str(score_a) + ' &#x2192; <b>' + str(score_ab) + '</b>)'
    )

    return '\\n'.join(lines) + '\\n' + summary
