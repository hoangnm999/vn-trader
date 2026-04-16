"""
context_scorecard.py — VN Trader Bot V6
========================================
Context Scorecard (Layer 2) — đánh giá thời điểm mua dựa trên
cross-validation analytics từ 10 mã Score A (7 năm, ~860 lệnh).

Chỉ dùng data realtime tại thời điểm signal:
  - VNI context (slope, vs MA20, ATR vol)
  - Entry quality (MA20 dist, vol context, HH20 dist, ROC5, score bucket)
  - Per-symbol adjustments (NKG ngoại lệ VNI UP, STB/PC1 HK rate cao, v.v.)

Output:
  evaluate_signal(symbol, score, ma20_dist, hh20_dist, vol_ratio,
                  vol_structure, roc5, ma20_slope,
                  vni_slope, vni_ma20_dist, vni_atr_ratio,
                  vni_atr_median)
  → dict: checks, points, max_points, grade, sizing, summary_line

Dùng trong telegram_bot.py:
  from context_scorecard import evaluate_signal, format_scorecard_msg
"""

import numpy as np
from typing import Optional

# ─── Thresholds từ cross-validation (10 mã / 7 năm) ──────────────────────────
# Chỉ áp dụng rules có evidence ≥ 7/10 mã

# VNI vs MA20 — OVERBOUGHT threshold
VNI_OB_THRESHOLD   = 3.0    # > +3% → Exp âm trên 7/10 mã
VNI_OS_THRESHOLD   = -3.0   # < -3% → bonus (OVERSOLD)

# VNI Slope 10 ngày
VNI_UP_THRESHOLD   = 2.0    # > +2%
VNI_DOWN_THRESHOLD = -2.0   # < -2%

# VNI ATR/price — volatility regime
# HIGH_VOL nếu > 1.3x median; LOW_VOL nếu < 0.7x median
VOL_HIGH_MULT = 1.30
VOL_LOW_MULT  = 0.70

# MA20 distance zones
MA20_EXTENDED_MIN  = 5.0    # > 5% → strong edge (8/10 mã Exp > 1.5%)
MA20_OK_MAX        = 5.0    # 2-5% → danger zone (6/10 mã Exp < 1.0%)
MA20_OK_MIN        = 2.0
MA20_BELOW         = 0.0    # dưới MA20 → nói chung dương (nhưng per-symbol)

# HH20 distance
HH20_AT_BREAKOUT   = 0.0    # = 0% (AT_BREAKOUT)
HH20_NEAR_MAX      = -1.0   # -1% đến -5% (NEAR — kém hơn AT_BREAKOUT)
HH20_NEAR_MIN      = -5.0
HH20_EXTENDED      = -5.0   # < -5% (EXTENDED — mixed, phụ thuộc mã)

# Volume context
VOL_CLIMAX_RATIO   = 1.5    # vol_ratio ≥ 1.5 AND hh20_dist ≥ -2% AND ma20_dist ≥ 5%
VOL_BREAKOUT_RATIO = 1.5    # vol_ratio ≥ 1.5 near base (ma20_dist < 5%)
VOL_HIGH_RATIO     = 1.5    # vol cao nhưng không rõ context
VOL_LOW_RATIO      = 0.80   # vol thấp

# ROC5
ROC5_STRONG = 3.0
ROC5_WEAK   = 0.0

# Score buckets
SCORE_STRONG  = 95
SCORE_GOOD    = 85
SCORE_MID     = 75
SCORE_BASE    = 65

# ─── Per-symbol overrides từ cross-validation ────────────────────────────────
# Các mã có đặc điểm đặc biệt làm lệch rule phổ quát

SYMBOL_OVERRIDES = {
    # NKG: VNI UP là tốt nhất (Exp = +3.94) — ngược với phần lớn
    'NKG': {
        'vni_up_ok': True,      # Không penalize VNI UP
        'vni_up_bonus': True,   # Thêm bonus khi VNI UP
    },
    # STB: HK rate 68%, WR tốt nhưng Exp thấp — cần context chặt hơn
    'STB': {
        'need_strong_context': True,
    },
    # PC1: FALLING MA20 slope lại tốt (+6.03) — counter-intuitive
    'PC1': {
        'falling_slope_ok': True,
    },
    # DGC: MaxDD -67%, đang suy yếu — giảm sizing
    'DGC': {
        'max_dd_caution': True,
        'size_cap': '50%',
    },
    # SSI: PF thấp nhất (1.32), xu hướng xấu đi — cần context rất tốt
    'SSI': {
        'need_strong_context': True,
        'min_checks_for_buy': 6,   # cần pass ít nhất 6/8 checks
    },
    # MCH: 83% SL là false breakout — phải có volume confirmation
    'MCH': {
        'need_volume_confirm': True,
    },
    # CTS: CLIMAX_VOL cực mạnh (+6.09) — boost khi có
    'CTS': {
        'climax_boost': True,
    },
    # HAH: NORMAL_VOL với NORMAL vol_context rất tốt
    'HAH': {
        'normal_vol_boost': True,
    },
}

# ─── Sizing rules từ analytics ───────────────────────────────────────────────
def _get_base_sizing(score: int) -> str:
    """Sizing cơ bản theo score bucket (từ 4A cross-validation)."""
    if score >= SCORE_STRONG:
        return 'FULL SIZE'
    elif score >= SCORE_GOOD:
        return '70-80%'
    elif score >= SCORE_MID:
        return '50%'
    else:
        return '25-50%'

# ─── Classify helpers ────────────────────────────────────────────────────────

def _classify_vni_trend(vni_slope: float) -> str:
    if vni_slope > VNI_UP_THRESHOLD:
        return 'UP'
    elif vni_slope < VNI_DOWN_THRESHOLD:
        return 'DOWN'
    return 'FLAT'

def _classify_vni_ob(vni_ma20_dist: float) -> str:
    if vni_ma20_dist > VNI_OB_THRESHOLD:
        return 'OVERBOUGHT'
    elif vni_ma20_dist < VNI_OS_THRESHOLD:
        return 'OVERSOLD'
    return 'NEUTRAL'

def _classify_vol_regime(vni_atr_ratio: float, vni_atr_median: float) -> str:
    if vni_atr_median <= 0:
        return 'NORMAL_VOL'
    if vni_atr_ratio > vni_atr_median * VOL_HIGH_MULT:
        return 'HIGH_VOL'
    elif vni_atr_ratio < vni_atr_median * VOL_LOW_MULT:
        return 'LOW_VOL'
    return 'NORMAL_VOL'

def _classify_ma20_zone(ma20_dist: float) -> str:
    if ma20_dist < 0:
        return 'BELOW'
    elif ma20_dist < MA20_OK_MIN:
        return 'NEAR'
    elif ma20_dist < MA20_OK_MAX:
        return 'OK'
    return 'EXTENDED'

def _classify_vol_context(vol_ratio: float, vol_structure: float,
                           hh20_dist: float, ma20_dist: float,
                           ma20_slope: float = 0.0) -> str:
    """
    Phân loại volume context dựa trên vol_ratio + vol_structure + slope.

    FIX (reviewer điểm 1+2): CLIMAX_VOL được tách thành 2 loại:
      - CLIMAX_ACCUM: volume spike ở vùng extended + slope RISING → accumulation thật
      - CLIMAX_BLOWOFF: volume spike ở vùng extended + slope FLAT/FALLING → blow-off/distribution

    Lý do: "Exp dương 10/10 mã" không đủ để kết luận CLIMAX = tốt.
    DGC (+0.65%), STB (+1.44%) cho thấy phần lớn là blow-off không cải thiện.
    Chỉ CLIMAX_ACCUM (slope rising, trend continuation) mới có edge thật.
    """
    if vol_ratio >= VOL_CLIMAX_RATIO:
        if vol_structure < 0 or ma20_dist >= MA20_EXTENDED_MIN:
            # Vol spike ở vùng extended — phân biệt accumulation vs blow-off bằng slope
            if ma20_slope > 0.3:
                return 'CLIMAX_ACCUM'    # Trend continuation: slope vẫn đang tăng
            else:
                return 'CLIMAX_BLOWOFF'  # Exhaustion: slope phẳng/giảm → rủi ro cao
        elif hh20_dist >= HH20_NEAR_MAX:
            return 'BREAKOUT_VOL'        # Spike gần base — breakout thật
        else:
            return 'HIGH_VOL'            # Spike không rõ context
    elif vol_ratio < VOL_LOW_RATIO:
        return 'LOW_VOL'
    return 'NORMAL'

def _classify_hh20(hh20_dist: float) -> str:
    if hh20_dist >= HH20_AT_BREAKOUT:
        return 'AT_BREAKOUT'
    elif hh20_dist >= HH20_NEAR_MIN:
        return 'NEAR'
    return 'EXTENDED'

def _classify_roc5(roc5: float) -> str:
    if roc5 > ROC5_STRONG:
        return 'STRONG'
    elif roc5 > ROC5_WEAK:
        return 'OK'
    return 'WEAK'

def _classify_ma20_slope(slope: float) -> str:
    if slope > 0.5:
        return 'RISING'
    elif slope < -0.5:
        return 'FALLING'
    return 'FLAT'

# ─── MAIN EVALUATE FUNCTION ──────────────────────────────────────────────────

def evaluate_signal(
    symbol: str,
    score: int,
    ma20_dist: float,
    hh20_dist: float,
    vol_ratio: float,
    vol_structure: float,
    roc5: float,
    ma20_slope: float,
    vni_slope: float,
    vni_ma20_dist: float,
    vni_atr_ratio: float,
    vni_atr_median: float = 0.80,   # median từ dữ liệu lịch sử (~0.74-0.88%)
) -> dict:
    """
    Đánh giá context của 1 signal MUA bằng scorecard từ cross-validation.

    Parameters
    ----------
    symbol        : mã cổ phiếu (VD: 'NKG')
    score         : Score A tại thời điểm signal (0-100)
    ma20_dist     : % giá so với MA20 (dương = trên MA20)
    hh20_dist     : % giá so với đỉnh 20 phiên (âm = dưới đỉnh)
    vol_ratio     : khối lượng hôm nay / avg20 (1.0 = bình thường)
    vol_structure : dương = spike gần base, âm = spike ở vùng extended
    roc5          : % change 5 phiên trước entry
    ma20_slope    : % slope của MA20 trong 5 bar gần nhất
    vni_slope     : % VNINDEX change trong 10 ngày
    vni_ma20_dist : % VNINDEX so với MA20 của nó (dương = trên MA20)
    vni_atr_ratio : ATR(14)/price của VNINDEX (%)
    vni_atr_median: ngưỡng median ATR để phân loại LOW/NORMAL/HIGH_VOL

    Returns
    -------
    dict với keys:
      checks       : list of dict {name, label, result, points, emoji, detail, n_evidence}
      total_points : tổng điểm đạt được
      max_points   : tổng điểm tối đa
      grade        : 'STRONG' / 'MODERATE' / 'WEAK' / 'SKIP'
      sizing       : '25%' / '50%' / '70-80%' / 'FULL SIZE'
      summary_line : 1 dòng tóm tắt
      classifications: các nhãn phân loại đã tính
      override_notes: ghi chú per-symbol
    """
    sym = symbol.upper()
    overrides = SYMBOL_OVERRIDES.get(sym, {})

    # ── Phân loại tất cả dimensions ─────────────────────────────────────────
    vni_trend   = _classify_vni_trend(vni_slope)
    vni_ob      = _classify_vni_ob(vni_ma20_dist)
    vol_regime  = _classify_vol_regime(vni_atr_ratio, vni_atr_median)
    ma20_zone   = _classify_ma20_zone(ma20_dist)
    vol_ctx     = _classify_vol_context(vol_ratio, vol_structure,
                                         hh20_dist, ma20_dist, ma20_slope)  # FIX: thêm slope
    hh20_zone   = _classify_hh20(hh20_dist)
    roc5_zone   = _classify_roc5(roc5)
    slope_zone  = _classify_ma20_slope(ma20_slope)

    checks = []
    override_notes = []

    # ══════════════════════════════════════════════════════════════════════════
    # CHECK 1: VNI KHÔNG OVERBOUGHT  [CF-1]
    # Evidence: 7/10 mã Exp âm hoặc rất thấp khi VNI OVERBOUGHT
    # ══════════════════════════════════════════════════════════════════════════
    c1_pts = 0
    if vni_ob == 'NEUTRAL':
        c1_pts = 2
        c1_emoji = '✅'
        c1_detail = f'VNI vs MA20 = {vni_ma20_dist:+.1f}% (NEUTRAL) — an toàn'
    elif vni_ob == 'OVERSOLD':
        c1_pts = 3  # bonus: OVERSOLD thường là setup tốt (DGC Exp=+3.87, CTS +5.44)
        c1_emoji = '🟢'
        c1_detail = f'VNI vs MA20 = {vni_ma20_dist:+.1f}% (OVERSOLD) — setup rất tốt'
    else:
        # OVERBOUGHT — nhưng NKG ngoại lệ
        if overrides.get('vni_up_ok') and vni_trend == 'UP':
            c1_pts = 1
            c1_emoji = '🟡'
            c1_detail = f'VNI OVERBOUGHT ({vni_ma20_dist:+.1f}%) nhưng {sym} hoạt động tốt khi VNI UP'
            override_notes.append(f'{sym}: VNI UP không penalize (NKG đặc tính chu kỳ)')
        else:
            c1_pts = -1
            c1_emoji = '❌'
            c1_detail = f'VNI vs MA20 = {vni_ma20_dist:+.1f}% (OVERBOUGHT) — risk cao, 7/10 mã Exp âm'

    checks.append({
        'id': 'CF1_VNI_OB',
        'name': 'VNI Overbought Filter',
        'label': f'VNI vs MA20: {vni_ob}',
        'result': vni_ob != 'OVERBOUGHT' or overrides.get('vni_up_ok'),
        'points': c1_pts,
        'max_points': 3,
        'emoji': c1_emoji,
        'detail': c1_detail,
        'n_evidence': '7/10 mã',
        'weight': 'HIGH',
    })

    # ══════════════════════════════════════════════════════════════════════════
    # CHECK 2: VNI VOLATILITY REGIME  [CF-2]
    # Evidence: NORMAL_VOL outperforms trên 9/10 mã (avg delta +0.95%)
    # ══════════════════════════════════════════════════════════════════════════
    c2_pts = 0
    if vol_regime == 'NORMAL_VOL':
        c2_pts = 2
        c2_emoji = '✅'
        c2_detail = f'VNI ATR/price = {vni_atr_ratio:.2f}% (NORMAL_VOL) — tốt nhất cho 9/10 mã'
    elif vol_regime == 'LOW_VOL':
        c2_pts = 1
        c2_emoji = '🟡'
        c2_detail = f'VNI ATR = {vni_atr_ratio:.2f}% (LOW_VOL) — ổn, nhưng NORMAL tốt hơn'
    else:  # HIGH_VOL
        # CTS là ngoại lệ — WR 58% kể cả HIGH_VOL
        if sym in ('CTS', 'NKG'):
            c2_pts = 1
            c2_emoji = '🟡'
            c2_detail = f'VNI HIGH_VOL ({vni_atr_ratio:.2f}%) nhưng {sym} chịu được'
            override_notes.append(f'{sym}: chịu HIGH_VOL tốt hơn trung bình')
        else:
            c2_pts = -1
            c2_emoji = '❌'
            c2_detail = f'VNI ATR = {vni_atr_ratio:.2f}% (HIGH_VOL) — SL tăng, 8/10 mã Exp giảm'

    checks.append({
        'id': 'CF2_VOL_REGIME',
        'name': 'VNI Volatility Regime',
        'label': f'VNI ATR Regime: {vol_regime}',
        'result': vol_regime in ('NORMAL_VOL', 'LOW_VOL'),
        'points': c2_pts,
        'max_points': 2,
        'emoji': c2_emoji,
        'detail': c2_detail,
        'n_evidence': '9/10 mã',
        'weight': 'HIGH',
    })

    # ══════════════════════════════════════════════════════════════════════════
    # CHECK 3: MA20 ZONE — FIX: EXTENDED cần tách trend continuation vs exhaustion
    # FIX (reviewer điểm 2): EXTENDED >5% có 2 trạng thái hoàn toàn khác nhau:
    #   - EXTENDED + slope RISING  → trend continuation → tốt (evidence 8/10 mã)
    #   - EXTENDED + slope FLAT/FALLING → late-stage extension → rủi ro cao
    # Không thể kết luận "EXTENDED = tốt" mà không xét slope.
    # ══════════════════════════════════════════════════════════════════════════
    c3_pts = 0
    if ma20_zone == 'EXTENDED':
        if slope_zone == 'RISING':
            c3_pts = 2
            c3_emoji = '✅'
            c3_detail = f'MA20 dist={ma20_dist:+.1f}% (EXTENDED) + slope RISING — trend continuation, edge tốt'
        elif slope_zone == 'FLAT':
            c3_pts = 1
            c3_emoji = '🟡'
            c3_detail = f'MA20 dist={ma20_dist:+.1f}% (EXTENDED) + slope FLAT — momentum đang chậm lại'
        else:  # FALLING
            c3_pts = -1
            c3_emoji = '⚠'
            c3_detail = f'MA20 dist={ma20_dist:+.1f}% (EXTENDED) + slope FALLING — late-stage extension, rủi ro cao'
    elif ma20_zone == 'BELOW':
        below_good = sym in ('DGC', 'NKG', 'STB', 'CTS', 'HAH', 'SSI')
        if below_good:
            c3_pts = 2
            c3_emoji = '✅'
            c3_detail = f'MA20 dist={ma20_dist:+.1f}% (BELOW MA20) — Exp tốt cho {sym}'
        else:
            c3_pts = 0
            c3_emoji = '🟡'
            c3_detail = f'MA20 dist={ma20_dist:+.1f}% (BELOW MA20) — mixed per-symbol'
    elif ma20_zone == 'NEAR':
        c3_pts = 0
        c3_emoji = '🟡'
        c3_detail = f'MA20 dist={ma20_dist:+.1f}% (NEAR 0-2%) — trung bình'
    else:  # OK (2-5%) — danger zone
        ok_exceptions = ('HAH', 'FRT', 'HCM', 'STB', 'PC1')
        if sym in ok_exceptions:
            c3_pts = 1
            c3_emoji = '🟡'
            c3_detail = f'MA20 dist={ma20_dist:+.1f}% (OK 2-5%) — chấp nhận cho {sym}'
        else:
            c3_pts = -1
            c3_emoji = '❌'
            c3_detail = f'MA20 dist={ma20_dist:+.1f}% (OK 2-5%) — Exp thấp/âm trên 6/10 mã, variance cao'

    checks.append({
        'id': 'CF3_MA20_ZONE',
        'name': 'MA20 Distance Zone',
        'label': f'MA20 Zone: {ma20_zone} ({ma20_dist:+.1f}%)',
        'result': ma20_zone not in ('OK',) or sym in ('HAH','FRT','HCM','STB','PC1'),
        'points': c3_pts,
        'max_points': 2,
        'emoji': c3_emoji,
        'detail': c3_detail,
        'n_evidence': '8/10 mã',
        'weight': 'HIGH',
    })

    # ══════════════════════════════════════════════════════════════════════════
    # CHECK 4: VOLUME CONTEXT
    # FIX (reviewer điểm 1): Tách CLIMAX_ACCUM vs CLIMAX_BLOWOFF.
    # "Exp dương 10/10 mã" không đủ — DGC +0.65%, STB +1.44% là rất yếu.
    # CLIMAX cần thêm slope context mới có edge thật.
    # Label NORMAL_VOL (VNI regime) vs LOW_VOL (stock volume) là 2 dimension khác nhau
    # → Không mâu thuẫn, nhưng cần label rõ ràng để tránh nhầm.
    # ══════════════════════════════════════════════════════════════════════════
    c4_pts = 0

    if vol_ctx == 'CLIMAX_ACCUM':
        # Volume spike extended + slope rising = accumulation thật
        # Evidence mạnh: MCH +5.35, CTS +6.09 (nhưng DGC/STB thấp)
        c4_pts = 3 + (1 if overrides.get('climax_boost') else 0)
        c4_emoji = '🚀'
        c4_detail = (f'CLIMAX_ACCUM (vol={vol_ratio:.1f}x, slope={slope_zone}) — '
                     f'volume spike + trend vẫn tăng → accumulation, không phải blow-off')

    elif vol_ctx == 'CLIMAX_BLOWOFF':
        # Volume spike extended + slope flat/falling = exhaustion
        c4_pts = -2
        c4_emoji = '❌'
        c4_detail = (f'CLIMAX_BLOWOFF (vol={vol_ratio:.1f}x, slope={slope_zone}) — '
                     f'volume spike + trend mất đà → blow-off/distribution, rủi ro vào đỉnh cao')

    elif vol_ctx == 'NORMAL':
        c4_pts = 1 + (1 if overrides.get('normal_vol_boost') else 0)
        c4_emoji = '✅'
        c4_detail = f'NORMAL volume (ratio={vol_ratio:.1f}x stock) — môi trường ổn định'

    elif vol_ctx == 'BREAKOUT_VOL':
        if sym in ('DGC', 'HAH', 'SSI'):
            c4_pts = 2
            c4_emoji = '✅'
            c4_detail = f'BREAKOUT_VOL (ratio={vol_ratio:.1f}x) — spike gần base, tốt cho {sym}'
        elif overrides.get('need_volume_confirm'):
            c4_pts = 0
            c4_emoji = '🟡'
            c4_detail = f'BREAKOUT_VOL (ratio={vol_ratio:.1f}x) — {sym} cần thêm confirm'
        else:
            c4_pts = 0
            c4_emoji = '🟡'
            c4_detail = f'BREAKOUT_VOL (ratio={vol_ratio:.1f}x) — mixed, xem context khác'

    elif vol_ctx == 'LOW_VOL':
        # LOW_VOL = khối lượng stock thấp (khác NORMAL_VOL = VNI ATR thấp)
        if sym in ('STB', 'DGC', 'NKG'):
            c4_pts = 2
            c4_emoji = '✅'
            c4_detail = f'LOW_VOL stock (ratio={vol_ratio:.1f}x) — tốt cho {sym} (Exp +2.0-2.5%)'
        elif overrides.get('need_volume_confirm'):
            c4_pts = -1
            c4_emoji = '❌'
            c4_detail = f'LOW_VOL stock + {sym} cần volume confirm — tránh'
        else:
            c4_pts = 0
            c4_emoji = '🟡'
            c4_detail = f'LOW_VOL stock (ratio={vol_ratio:.1f}x) — chấp nhận, không có edge rõ'

    else:  # HIGH_VOL stock
        c4_pts = -1
        c4_emoji = '❌'
        c4_detail = f'HIGH_VOL stock (ratio={vol_ratio:.1f}x) — noise cao, SL khó kiểm soát'

    checks.append({
        'id': 'CF4_VOLUME',
        'name': 'Volume Context',
        'label': f'Vol: {vol_ctx} ({vol_ratio:.1f}x stock)',
        'result': vol_ctx in ('CLIMAX_ACCUM', 'NORMAL', 'BREAKOUT_VOL'),
        'points': c4_pts,
        'max_points': 4,
        'emoji': c4_emoji,
        'detail': c4_detail,
        'n_evidence': 'CLIMAX_ACCUM: evidence cần thêm live | NORMAL: 9/10 mã',
        'weight': 'HIGH',
    })

    # ══════════════════════════════════════════════════════════════════════════
    # CHECK 5: SCORE BUCKET
    # FIX (reviewer điểm 4): 75-84 không phải "danger zone" tuyệt đối.
    # NKG +2.94%, DGC +3.86%, PC1 +1.72% cho thấy bucket này không nhất quán.
    # Kết luận đúng hơn: variance cao, cần context filter mạnh.
    # Avg weighted Exp vẫn thấp nhất (~+0.85%) nhưng không đều.
    # ══════════════════════════════════════════════════════════════════════════
    c5_pts = 0
    if score >= SCORE_STRONG:
        c5_pts = 3
        c5_emoji = '✅'
        c5_detail = f'Score {score} ≥ 95 — nhất quán nhất, Exp dương 10/10 mã, avg +2.06%'
    elif score >= SCORE_GOOD:
        c5_pts = 2
        c5_emoji = '✅'
        c5_detail = f'Score {score} (85-94) — reliable, Exp dương phần lớn mã'
    elif score >= SCORE_MID:
        # FIX: không gọi là "danger zone" — variance cao, không nhất quán
        c5_pts = 0
        c5_emoji = '🟡'
        c5_detail = (f'Score {score} (75-84) — VARIANCE CAO: avg Exp +0.85% nhưng '
                     f'range từ -3.2% (CTS) đến +3.86% (DGC). '
                     f'Kết quả phụ thuộc nhiều vào context — cần CF pass mạnh')
    else:
        c5_pts = -1
        c5_emoji = '⚠'
        c5_detail = f'Score {score} (65-74) — thấp, chỉ trade khi context rất tốt'

    checks.append({
        'id': 'CF5_SCORE_BUCKET',
        'name': 'Score Bucket',
        'label': f'Score {score} → {"95+" if score>=95 else "85-94" if score>=85 else "75-84" if score>=75 else "65-74"}',
        'result': score >= SCORE_GOOD,
        'points': c5_pts,
        'max_points': 3,
        'emoji': c5_emoji,
        'detail': c5_detail,
        'n_evidence': '10/10 mã',
        'weight': 'HIGH',
    })

    # ══════════════════════════════════════════════════════════════════════════
    # CHECK 6: VNI TREND (bonus/penalty nhẹ)
    # Evidence: VNI FLAT/DOWN tốt hơn UP cho 7/10 mã
    # ══════════════════════════════════════════════════════════════════════════
    c6_pts = 0
    if vni_trend == 'FLAT':
        c6_pts = 1
        c6_emoji = '✅'
        c6_detail = f'VNI Slope = {vni_slope:+.1f}% (FLAT) — môi trường lý tưởng nhất'
    elif vni_trend == 'DOWN':
        c6_pts = 1
        c6_emoji = '✅'
        c6_detail = f'VNI Slope = {vni_slope:+.1f}% (DOWN) — counter-cyclical edge tốt'
    else:  # UP
        if overrides.get('vni_up_bonus'):
            c6_pts = 2
            c6_emoji = '🚀'
            c6_detail = f'VNI UP ({vni_slope:+.1f}%) — {sym} outperforms khi VNI tăng mạnh (Exp +3.94)'
            override_notes.append(f'{sym}: VNI UP là best regime (Exp +3.94%)')
        else:
            c6_pts = 0
            c6_emoji = '🟡'
            c6_detail = f'VNI Slope = {vni_slope:+.1f}% (UP) — không lý tưởng, 7/10 mã prefer FLAT/DOWN'

    checks.append({
        'id': 'CF6_VNI_TREND',
        'name': 'VNI Trend',
        'label': f'VNI Trend: {vni_trend} ({vni_slope:+.1f}%)',
        'result': vni_trend in ('FLAT', 'DOWN') or overrides.get('vni_up_bonus'),
        'points': c6_pts,
        'max_points': 2,
        'emoji': c6_emoji,
        'detail': c6_detail,
        'n_evidence': '7/10 mã',
        'weight': 'MEDIUM',
    })

    # ══════════════════════════════════════════════════════════════════════════
    # CHECK 7: STRUCTURE (HH20 + AT_BREAKOUT)
    # Evidence: AT_BREAKOUT tốt trên 7/10 mã; NEAR kém hơn trên 6/10 mã
    # ══════════════════════════════════════════════════════════════════════════
    c7_pts = 0
    if hh20_zone == 'AT_BREAKOUT':
        c7_pts = 2
        c7_emoji = '✅'
        c7_detail = f'AT_BREAKOUT (HH20 dist={hh20_dist:.1f}%) — vào ngay breakout, 7/10 mã Exp tốt'
    elif hh20_zone == 'EXTENDED':
        if sym in ('DGC', 'NKG', 'STB', 'CTS'):
            c7_pts = 1
            c7_emoji = '🟡'
            c7_detail = f'EXTENDED từ HH20 ({hh20_dist:.1f}%) — {sym} có Exp tốt ở zone này'
        else:
            c7_pts = -1
            c7_emoji = '⚠'
            c7_detail = f'EXTENDED từ HH20 ({hh20_dist:.1f}%) — đã vào muộn, Exp mixed'
    else:  # NEAR
        c7_pts = 0
        c7_emoji = '🟡'
        c7_detail = f'NEAR HH20 ({hh20_dist:.1f}%) — đợi thêm hay đã muộn?'

    checks.append({
        'id': 'CF7_STRUCTURE',
        'name': 'Price Structure (HH20)',
        'label': f'HH20: {hh20_zone} ({hh20_dist:.1f}%)',
        'result': hh20_zone == 'AT_BREAKOUT',
        'points': c7_pts,
        'max_points': 2,
        'emoji': c7_emoji,
        'detail': c7_detail,
        'n_evidence': '7/10 mã',
        'weight': 'MEDIUM',
    })

    # ══════════════════════════════════════════════════════════════════════════
    # CHECK 8: ROC5 + MA20 SLOPE (combo momentum)
    # Evidence: ROC5 STRONG > 3% tốt cho 8/10 mã; WEAK < 0% kém cho 7/10 mã
    # ══════════════════════════════════════════════════════════════════════════
    c8_pts = 0
    slope_ok = slope_zone == 'RISING' or (slope_zone == 'FALLING' and overrides.get('falling_slope_ok'))

    if roc5_zone == 'STRONG' and slope_ok:
        c8_pts = 2
        c8_emoji = '✅'
        c8_detail = f'ROC5={roc5:+.1f}% (STRONG) + MA20 slope {slope_zone} — momentum tốt'
    elif roc5_zone == 'STRONG':
        c8_pts = 1
        c8_emoji = '🟡'
        c8_detail = f'ROC5={roc5:+.1f}% (STRONG) nhưng MA20 slope {slope_zone}'
    elif roc5_zone == 'OK' and slope_ok:
        c8_pts = 1
        c8_emoji = '🟡'
        c8_detail = f'ROC5={roc5:+.1f}% (OK) + MA20 RISING — ổn'
    elif roc5_zone == 'WEAK':
        if sym in ('DGC',) and slope_zone == 'FALLING':
            c8_pts = 0
            c8_emoji = '🟡'
            c8_detail = f'ROC5={roc5:+.1f}% (WEAK) — DGC có Exp dương với WEAK ROC khi slope giảm'
        else:
            c8_pts = -1
            c8_emoji = '⚠'
            c8_detail = f'ROC5={roc5:+.1f}% (WEAK <0%) + slope {slope_zone} — momentum yếu, 7/10 mã kém'
    else:
        c8_pts = 0
        c8_emoji = '🟡'
        c8_detail = f'ROC5={roc5:+.1f}% ({roc5_zone}) + slope {slope_zone}'

    checks.append({
        'id': 'CF8_MOMENTUM',
        'name': 'Momentum (ROC5 + Slope)',
        'label': f'ROC5: {roc5_zone} ({roc5:+.1f}%) | Slope: {slope_zone}',
        'result': roc5_zone != 'WEAK',
        'points': c8_pts,
        'max_points': 2,
        'emoji': c8_emoji,
        'detail': c8_detail,
        'n_evidence': '7/10 mã',
        'weight': 'MEDIUM',
    })

    # ── Tổng điểm ─────────────────────────────────────────────────────────
    total_points = sum(c['points'] for c in checks)
    max_points   = sum(c['max_points'] for c in checks)
    total_points_display = max(0, total_points)

    # ── Grading ────────────────────────────────────────────────────────────
    high_checks  = [c for c in checks if c['weight'] == 'HIGH']
    high_pass    = sum(1 for c in high_checks if c['points'] > 0)
    has_critical_fail = any(c['points'] < 0 for c in high_checks)

    # FIX: CLIMAX_BLOWOFF là hard veto — không trade dù score cao
    has_blowoff  = vol_ctx == 'CLIMAX_BLOWOFF'

    pct = total_points / max_points if max_points > 0 else 0

    # FIX (S16): min_checks_req thực sự được áp dụng vào grading.
    # Trước: biến được set nhưng không dùng → SSI override 'min_checks_for_buy': 6 vô hiệu.
    # Sau: nếu high_pass < min_checks_req → không đạt STRONG/MODERATE.
    min_checks_req = overrides.get('min_checks_for_buy', 4)

    if has_blowoff:
        grade = 'SKIP'
        override_notes.append('CLIMAX_BLOWOFF: volume spike + slope mất đà → veto cứng')
    elif has_critical_fail and sum(c['points'] < 0 for c in high_checks) >= 2:
        grade = 'SKIP'
    elif pct >= 0.70 and high_pass >= min_checks_req and not has_critical_fail:
        grade = 'STRONG'
    elif pct >= 0.50 and high_pass >= max(3, min_checks_req - 1):
        grade = 'MODERATE'
    elif pct >= 0.35 and high_pass >= 2:
        grade = 'WEAK'
    else:
        grade = 'SKIP'

    # Override: MaxDD caution symbols
    if overrides.get('max_dd_caution') and grade == 'STRONG':
        grade = 'MODERATE'
        override_notes.append(f'{sym}: MaxDD -67%, downgrade STRONG → MODERATE')

    # ── Sizing ────────────────────────────────────────────────────────────
    base_size = _get_base_sizing(score)

    if grade == 'STRONG':
        # FIX (S16): CLIMAX_VOL là tên cũ không tồn tại — đổi thành CLIMAX_ACCUM
        if vol_ctx == 'CLIMAX_ACCUM' and score >= SCORE_GOOD:
            sizing = 'FULL SIZE 💪'
        elif score >= SCORE_STRONG:
            sizing = 'FULL SIZE 💪'
        else:
            sizing = '70-80%'
    elif grade == 'MODERATE':
        if overrides.get('size_cap'):
            sizing = overrides['size_cap']
        elif score >= SCORE_STRONG:
            sizing = '70-80%'
        else:
            sizing = '50%'
    elif grade == 'WEAK':
        sizing = '25%'
    else:
        sizing = 'SKIP ❌'

    # ── Summary line ──────────────────────────────────────────────────────
    grade_emoji = {
        'STRONG':   '🟢',
        'MODERATE': '🟡',
        'WEAK':     '🟠',
        'SKIP':     '🔴',
    }.get(grade, '⚪')

    summary_line = (
        f'{grade_emoji} {grade} ({total_points_display}/{max_points}pt) '
        f'— Sizing: {sizing}'
    )

    return {
        'symbol': sym,
        'score': score,
        'checks': checks,
        'total_points': total_points_display,
        'raw_points': total_points,
        'max_points': max_points,
        'pct': round(pct * 100, 1),
        'grade': grade,
        'sizing': sizing,
        'summary_line': summary_line,
        'classifications': {
            'vni_trend': vni_trend,
            'vni_ob': vni_ob,
            'vol_regime': vol_regime,          # VNI ATR regime (NORMAL_VOL/HIGH_VOL/LOW_VOL)
            'ma20_zone': ma20_zone,
            'vol_ctx': vol_ctx,                # Stock volume context (CLIMAX_ACCUM/BLOWOFF/NORMAL/...)
            'hh20_zone': hh20_zone,
            'roc5_zone': roc5_zone,
            'slope_zone': slope_zone,
            # FIX label clarity: 2 dimensions khác nhau, không mâu thuẫn
            '_note': 'vol_regime=VNI ATR | vol_ctx=stock volume — 2 dimensions độc lập',
        },
        'override_notes': override_notes,
    }


# ─── TELEGRAM FORMATTER ──────────────────────────────────────────────────────

def format_scorecard_msg(result: dict, compact: bool = False) -> str:
    """
    Format scorecard thành Telegram message (plain text).

    compact=True: chỉ 1 block ngắn cho /signals
    compact=False: full detail cho /analyze
    """
    sym     = result['symbol']
    grade   = result['grade']
    sizing  = result['sizing']
    pts     = result['total_points']
    maxpts  = result['max_points']
    pct     = result['pct']
    checks  = result['checks']
    cls     = result['classifications']
    notes   = result['override_notes']

    grade_emoji = {
        'STRONG':   '🟢',
        'MODERATE': '🟡',
        'WEAK':     '🟠',
        'SKIP':     '🔴',
    }.get(grade, '⚪')

    grade_vi = {
        'STRONG':   'MUA MANH',
        'MODERATE': 'CAN NHAC',
        'WEAK':     'CANH TRANH',
        'SKIP':     'BO QUA',
    }.get(grade, grade)

    lines = []

    if compact:
        lines.append(f'Context Scorecard: {grade_emoji} {grade_vi} ({pts}/{maxpts}pt = {pct:.0f}%)')
        lines.append(f'Sizing: {sizing}')

        fails  = [c for c in checks if c['points'] < 0]
        stars  = [c for c in checks if c['points'] >= 3]

        if stars:
            lines.append('Edge: ' + ' | '.join(f"{c['emoji']} {c['label']}" for c in stars[:2]))
        if fails:
            lines.append('Risk: ' + ' | '.join(f"{c['emoji']} {c['label']}" for c in fails[:2]))

        # FIX: cảnh báo CLIMAX_BLOWOFF rõ ràng
        if cls.get('vol_ctx') == 'CLIMAX_BLOWOFF':
            lines.append('⚠ BLOWOFF DETECTED: slope mất đà tại vùng extended — tránh')

        if notes:
            lines.append('Note: ' + '; '.join(notes[:1]))

    else:
        # ── Full mode cho /analyze ────────────────────────────────────────
        lines.append('=== CONTEXT SCORECARD (Layer 2) ===')
        lines.append(f'{grade_emoji} {grade_vi} — {pts}/{maxpts} diem ({pct:.0f}%)')
        lines.append(f'Sizing: {sizing}')
        lines.append('')

        # Checks
        for c in checks:
            pass_fail = 'PASS' if c['points'] > 0 else ('FAIL' if c['points'] < 0 else 'NEUTRAL')
            pt_str    = f'+{c["points"]}' if c['points'] > 0 else str(c['points'])
            lines.append(
                f'{c["emoji"]} [{pass_fail}] {c["name"]} ({pt_str}/{c["max_points"]}pt)'
            )
            lines.append(f'   {c["label"]}')
            lines.append(f'   {c["detail"]}')
            lines.append(f'   Evidence: {c["n_evidence"]}')

        lines.append('')
        lines.append(f'Dimensions (2 loại khác nhau):')
        lines.append(f'  vol_regime = VNI ATR/price: {cls["vol_regime"]} (market volatility)')
        lines.append(f'  vol_ctx    = Stock vol ratio: {cls["vol_ctx"]} (stock-level volume)')
        lines.append(f'  VNI: {cls["vni_trend"]} | {cls["vni_ob"]}')
        lines.append(f'  Stock: MA20={cls["ma20_zone"]} | HH20={cls["hh20_zone"]}')
        lines.append(f'  Momentum: ROC5={cls["roc5_zone"]} | Slope={cls["slope_zone"]}')

        if notes:
            lines.append('')
            lines.append('Per-symbol notes:')
            for n in notes:
                lines.append(f'  * {n}')

        lines.append('')
        lines.append('--- Cross-val: 10 ma x 7 nam x ~860 lenh ---')
        lines.append('--- Time stability: pending CF Walk-Forward ---')

    return '\n'.join(lines)


# ─── INTEGRATION HELPER ──────────────────────────────────────────────────────

def compute_scorecard_from_trade_fields(symbol: str, score: int,
                                         trade_fields: dict,
                                         vni_atr_median: float = 0.80) -> dict:
    """
    Shortcut: nhận dict trade_fields từ backtest (các key rich fields)
    và trả về scorecard result.

    Dùng khi đã có computed context từ backtest.py (ví dụ trong /analyze handler).

    trade_fields expected keys:
        ma20_dist, hh20_dist, vol_ratio, vol_structure,
        roc5, ma20_slope, vni_slope, vni_ma20_dist, vni_atr_ratio
    """
    return evaluate_signal(
        symbol      = symbol,
        score       = score,
        ma20_dist   = trade_fields.get('ma20_dist',   0.0),
        hh20_dist   = trade_fields.get('hh20_dist',   0.0),
        vol_ratio   = trade_fields.get('vol_ratio',   1.0),
        vol_structure=trade_fields.get('vol_structure',0.0),
        roc5        = trade_fields.get('roc5',         0.0),
        ma20_slope  = trade_fields.get('ma20_slope',   0.0),
        vni_slope   = trade_fields.get('vni_slope',    0.0),
        vni_ma20_dist=trade_fields.get('vni_ma20_dist',0.0),
        vni_atr_ratio=trade_fields.get('vni_atr_ratio',0.0),
        vni_atr_median=vni_atr_median,
    )


def compute_realtime_context(symbol: str, score: int) -> dict:
    """
    Tính realtime context từ vnstock và trả về scorecard.
    Dùng trực tiếp trong telegram_bot.py khi nhận signal.

    Returns: (scorecard_result, error_msg)
    """
    try:
        import numpy as np
        import sys, os
        # Thêm bot dir vào path
        bot_dir = os.path.dirname(os.path.abspath(__file__))
        if bot_dir not in sys.path:
            sys.path.insert(0, bot_dir)

        import backtest as bt

        # Load stock data (90 ngày đủ cho MA50, HH20, ROC5)
        df, _ = bt.load_data(symbol, days=120)
        if df is None:
            return None, f'Không load được data {symbol}'

        cc = bt.find_col(df, ['close', 'closeprice', 'close_price'])
        hc = bt.find_col(df, ['high',  'highprice',  'high_price'])
        lc = bt.find_col(df, ['low',   'lowprice',   'low_price'])
        vc = next((c for c in df.columns if c.lower() in
                   ('volume','volume_match','klgd','vol','trading_volume',
                    'match_volume','total_volume')), None)
        if cc is None:
            return None, 'Không tìm thấy cột close'

        closes  = bt.to_arr(df[cc])
        highs   = bt.to_arr(df[hc]) if hc else closes.copy()
        lows    = bt.to_arr(df[lc]) if lc else closes.copy()
        volumes = bt.to_arr(df[vc]) if vc else np.ones(len(closes))

        for arr in [closes, highs, lows]:
            if arr.max() < 1000:
                arr *= 1000

        i = len(closes) - 1  # candle cuối cùng (hôm nay)

        # Tính các fields (đồng nhất với backtest.py rich fields)
        _ep   = closes[i]
        _ma20 = float(np.mean(closes[max(0,i-20):i])) if i >= 20 else _ep
        _ma20_dist = (_ep / _ma20 - 1) * 100 if _ma20 > 0 else 0.0

        _ma20_prev = float(np.mean(closes[max(0,i-25):max(1,i-5)])) if i >= 25 else _ma20
        _ma20_slope = (_ma20 / _ma20_prev - 1) * 100 if _ma20_prev > 0 else 0.0

        _roc5 = (closes[i] / closes[max(0,i-5)] - 1) * 100 if i >= 5 else 0.0

        _hh20 = float(np.max(closes[max(0,i-20):i])) if i >= 5 else _ep
        _hh20_dist = (_ep / _hh20 - 1) * 100 if _hh20 > 0 else 0.0

        _vol_entry = float(volumes[i])
        _vol_avg20 = float(np.mean(volumes[max(0,i-20):i])) if i >= 5 else 1
        _vol_ratio = _vol_entry / _vol_avg20 if _vol_avg20 > 0 else 1.0

        _vol_structure = 0.0
        if _vol_ratio >= 1.5:
            _vol_structure = _vol_ratio * (1 if _hh20_dist >= -2 and _ma20_dist < 5 else -1)

        # Load VNI
        _vni_slope = 0.0
        _vni_ma20_dist = 0.0
        _vni_atr_ratio = 0.0
        _vni_atr_median = 0.80

        try:
            df_vni, _ = bt.load_data('VNINDEX', days=120)
            if df_vni is not None:
                cc_vni = bt.find_col(df_vni, ['close', 'closeprice', 'close_price'])
                if cc_vni:
                    _vni_raw = bt.to_arr(df_vni[cc_vni])
                    _vni = np.where(_vni_raw < 1000, _vni_raw * 1000, _vni_raw)
                    _vi  = len(_vni) - 1

                    if _vi >= 10:
                        _vni_slope = float((_vni[_vi] / _vni[max(0,_vi-10)] - 1) * 100)
                    if _vi >= 20:
                        _vni_ma20v = float(np.mean(_vni[max(0,_vi-20):_vi]))
                        _vni_ma20_dist = float((_vni[_vi] / _vni_ma20v - 1) * 100) if _vni_ma20v > 0 else 0.0
                    if _vi >= 14:
                        _atr_v = float(np.mean(np.abs(np.diff(_vni[max(0,_vi-15):_vi+1]))))
                        _vni_atr_ratio = _atr_v / _vni[_vi] * 100 if _vni[_vi] > 0 else 0.0
                        # Tính median từ 60 ngày lịch sử
                        if _vi >= 60:
                            _atrs = []
                            for _k in range(_vi-45, _vi-10):
                                _a = float(np.mean(np.abs(np.diff(_vni[max(0,_k-15):_k+1]))))
                                _atrs.append(_a / _vni[_k] * 100 if _vni[_k] > 0 else 0)
                            if _atrs:
                                _vni_atr_median = float(np.median(_atrs))
        except Exception:
            pass

        result = evaluate_signal(
            symbol        = symbol,
            score         = score,
            ma20_dist     = round(_ma20_dist, 2),
            hh20_dist     = round(_hh20_dist, 2),
            vol_ratio     = round(_vol_ratio, 2),
            vol_structure = round(_vol_structure, 2),
            roc5          = round(_roc5, 2),
            ma20_slope    = round(_ma20_slope, 2),
            vni_slope     = round(_vni_slope, 2),
            vni_ma20_dist = round(_vni_ma20_dist, 2),
            vni_atr_ratio = round(_vni_atr_ratio, 2),
            vni_atr_median= _vni_atr_median,
        )
        return result, None

    except Exception as e:
        return None, str(e)[:120]
