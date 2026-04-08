"""
config.py — Single Source of Truth cho VN Trader Bot
=====================================================
Tất cả config quan trọng được định nghĩa MỘT LẦN ở đây.
app.py, backtest.py, telegram_bot.py đều import từ file này.

Cập nhật: 20/03/2026 — sync từ watchlist_setup.docx (lần 2)
  SL/TP điều chỉnh 7 mã theo Avg Win thực tế từ backtest:
    NKG: TP 14%→10% (Avg Win thực tế +7.1%)
    MBB: TP 9%→7%  (Avg Win thực tế +5.1%)
    HCM: SL 5%→6%, TP 9%→10% (giảm SL rate 38.9%)
    HSG: TP 14%→10%
    FRT: TP 14%→10%
    BID: TP 9%→7%
    NT2: SL 5%→3%, TP 9%→6% (utility stock beta thấp 0.6x)
  min_score per-symbol: 65 (Tier 1) / 70-75 (Tier 2) / 75-80 (Manual)
  b_filter per-symbol thêm vào SYMBOL_CONFIG (DCM=True, FRT=True, còn lại=False)
  get_min_score() cập nhật đọc per-symbol từ SYMBOL_CONFIG
  SIGNALS_WATCHLIST: 9 mã AUTO (Tier1+Tier2), VND/PDR/NVL/NT2 → SIGNALS_MANUAL

Cách thêm mã mới:
  1. Thêm vào SYMBOL_CONFIG với sl, tp, min_score
  2. Chạy /bt <MA> full trên Telegram → xem WR/PF/OOS
  3. Nếu PF>1.2 và OOS>50%: thêm vào SIGNALS_WATCHLIST
  4. Cập nhật wf_verdict
"""

# ═══════════════════════════════════════════════════════════════════════════════
# TRADING RULES — Quy tắc giao dịch cốt lõi
# ═══════════════════════════════════════════════════════════════════════════════

# T+2: Kể từ 8/11/2021 HoSE chuyển sang T+2 settlement.
# Mua ngày T → CP về tài khoản lưu ký ngày T+2 → bán được từ T+2 ngày giao dịch.
# Ví dụ: Mua thứ Hai → bán được từ thứ Tư (không phải thứ Năm T+3).
SETTLEMENT_DAYS  = 2        # T+2 chuẩn TTCK VN hiện hành (sửa từ T+3 sai)

HOLD_DAYS        = 10       # Giữ tối đa 10 phiên sau settlement
STOP_LOSS        = -0.07    # SL default toàn hệ thống (-7%)
TAKE_PROFIT      =  0.14    # TP default toàn hệ thống (+14%)
MIN_SCORE_BUY    = 65       # Ngưỡng MUA TOÀN HỆ THỐNG — áp dụng đồng nhất cho TẤT CẢ mã
MAX_SCORE_SELL   = 35       # Ngưỡng BAN TOÀN HỆ THỐNG — áp dụng đồng nhất cho TẤT CẢ mã
LOOKBACK_DAYS    = 2555     # 7 năm dữ liệu cho backtest
COMMISSION       = 0.005    # 0.5% khứ hồi (mua + bán + thuế TNCN 0.1%)

# ═══════════════════════════════════════════════════════════════════════════════
# SYMBOL_CONFIG — Cấu hình SL/TP/score theo từng mã
# ═══════════════════════════════════════════════════════════════════════════════
# sl/tp: số thập phân (0.05 = 5%, 0.07 = 7%)
# min_score: ngưỡng MUA tối ưu từ backtest per-symbol
# wf_verdict: kết quả walk-forward ('TOT'/'CHAP'/'YEU'/'' nếu chưa chạy)
# group: nhóm ngành (dùng cho Telegram hiển thị)
#
# Format thống nhất — KHÔNG dùng số nguyên (7) hay số % (7%) ở nơi khác
SYMBOL_CONFIG = {
    # ═══════════════════════════════════════════════════════════════════════════
    # sl/tp: số thập phân theo Avg Win thực tế từ backtest 7 năm
    # min_score: ngưỡng MUA per-symbol (get_min_score() ưu tiên giá trị này)
    # wf_verdict: TOT/CHAP/YEU/THEO_DOI/'' (tham khảo, không ảnh hưởng logic)
    # Cập nhật: 20/03/2026 — sync từ watchlist_setup.docx
    # ═══════════════════════════════════════════════════════════════════════════

    # ── TIER 1 — AUTO TRADE (6 mã, min_score=65) ─────────────────────────────
    'DGC': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'b_filter': False, 'group': 'Hoa chat',     'wf_verdict': 'TOT'  , 'shark_mode': 'none', 'use_regime': False, 'trigger_mode': 'score_primary'},
    # WR=55.6% PF=2.45 OOS=56.4% | B-filter TẮT (breakout bị lọc nhầm) | Entry: T bắt buộc
    'DCM': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'b_filter': True,  'group': 'Hoa chat',     'wf_verdict': 'TOT'  , 'shark_mode': 'S', 'shark_min': 65, 'use_regime': False, 'trigger_mode': 'score_primary', 'min_conviction': 3.0},
    # WR=55.4% PF=1.99 OOS=59.5% decay=-12% | B-filter BẬT (+2.3% WR) | Entry: T khuyến nghị
    'SSI': {'sl': 0.05, 'tp': 0.09, 'min_score': 65, 'b_filter': False, 'group': 'Chung khoan',  'wf_verdict': 'TOT'  , 'shark_mode': 'none', 'use_regime': True, 'trigger_mode': 'score_primary'},
    # WR=58.3% PF=1.82 OOS=61.1% | B-filter N/A (0 lệnh) | reproducibility cao nhất
    'NKG': {'sl': 0.07, 'tp': 0.10, 'min_score': 65, 'b_filter': False, 'group': 'Thep',         'wf_verdict': 'TOT',  'entry': 'T',   'entry_note': 'T bat buoc (-4.6% WR neu T+1)', 'shark_mode': 'none', 'use_regime': False, 'trigger_mode': 'score_primary', 'min_conviction': 3.0},
    # WR=58.1% PF=1.66 OOS=65.0% | TP 14%→10% (Avg Win +7.1%) | Entry: T BẮT BUỘC (-4.6% WR nếu T+1)
    'MBB': {'sl': 0.05, 'tp': 0.07, 'min_score': 55, 'b_filter': False, 'group': 'Ngan hang',    'wf_verdict': 'CHAP_NHAN',  'entry': 'T',   'entry_note': 'T bat buoc (-10.9% WR neu T+1)', 'shark_mode': 'S', 'shark_min': 55, 'use_regime': False, 'trigger_mode': 'filter_led'},
    # WR=56.4% PF=1.50 OOS=58.9% | TP 9%→7% (Avg Win +5.1%) | Entry: T BẮT BUỘC (-10.9% WR nếu T+1)
    'HCM': {'sl': 0.06, 'tp': 0.10, 'min_score': 65, 'b_filter': False, 'group': 'Chung khoan',  'wf_verdict': 'TOT' , 'shark_mode': 'S', 'shark_min': 55, 'use_regime': True, 'entry': 'T+1', 'entry_note': 'T+1 tot hon T (+0.43% PnL khi co regime)', 'use_vwap': False, 'trigger_mode': 'score_primary'},
    # WR=51.9% PF=1.45 OOS=50.2% | SL 5%→6% (giảm SL rate 38.9%) | TP 9%→10%

    # ── TIER 2 — AUTO TRADE CẨN THẬN (3 mã) ─────────────────────────────────
    'HSG': {'sl': 0.07, 'tp': 0.10, 'min_score': 65, 'b_filter': False, 'group': 'Thep',         'wf_verdict': 'TOT'        , 'shark_mode': 'none', 'use_regime': False, 'use_vwap': False, 'trigger_mode': 'filter_led'},
    # ML Sprint 6: WR=51.7% PF=1.85 WF Robust (đã update từ CHAP_NHAN)
    # WR=57.1% PF=1.91 OOS=52.8% | TP 14%→10% | Decay adj=-0.2% (bỏ outlier 2021) | Score<75 rủi ro
    'FRT': {'sl': 0.07, 'tp': 0.10, 'min_score': 65, 'b_filter': True,  'group': 'Ban le',       'wf_verdict': 'TOT'  , 'shark_mode': 'AS', 'shark_min': 50, 'shark_warn': 60, 'use_regime': False, 'use_vwap': False, 'trigger_mode': 'score_primary', 'min_conviction': 3.0},
    # WR=51.9% PF=1.35 OOS=55.7% | TP 14%→10% | B-filter BẬT (+1.9% WR) | Score 65-69 WR=45.5%
    'BID': {'sl': 0.05, 'tp': 0.07, 'min_score': 55, 'b_filter': False, 'group': 'Ngan hang',    'wf_verdict': 'YEU' , 'shark_mode': 'AS', 'shark_min': 50, 'use_regime': False, 'trigger_mode': 'filter_confirm', 'min_conviction': 2.5},
    # WR=50.0% PF=1.29 OOS=50.0% | TP 9%→7% | 4/5 cửa sổ WF có OOS≥IS

    # ── TIER 3 — MANUAL ONLY (4 mã, không vào SIGNALS_WATCHLIST) ─────────────
    'VND': {'sl': 0.05, 'tp': 0.09, 'min_score': 65, 'b_filter': False, 'group': 'Chung khoan',  'wf_verdict': 'CHAP_NHAN'  , 'shark_mode': 'none', 'use_regime': False, 'use_vwap': False, 'trigger_mode': 'filter_led'},
    # WR=52.2% PF=1.28 OOS=61.3% | Score 65-74: WR=47.1% (noise) → chỉ trade >=75
    'VCI': {'sl': 0.05, 'tp': 0.09, 'min_score': 65, 'b_filter': False, 'group': 'Chung khoan',  'wf_verdict': 'CHUA_BT'    , 'shark_mode': 'none', 'use_regime': False, 'use_vwap': False, 'trigger_mode': 'score_primary'},
    # Chua backtest — them vao de lam peer Sector RS cho HCM/SSI/VND | /mlscan extended
    'NT2': {'sl': 0.03, 'tp': 0.06, 'min_score': 80, 'b_filter': False, 'group': 'Dien',         'wf_verdict': 'CHAP' , 'shark_mode': 'AS', 'shark_min': 55, 'shark_warn': 60},
    # WR=50.9% PF=1.15 OOS=50.8% | SL 5%→3% TP 9%→6% (beta 0.6x) | Score 65-79 WR=39.1%
    'PDR': {'sl': 0.07, 'tp': 0.14, 'min_score': 55, 'b_filter': False, 'group': 'Bat dong san', 'wf_verdict': 'CHAP_NHAN', 'entry': 'T',   'entry_note': 'T bat buoc (-14.2% WR neu T+1)', 'shark_mode': 'dangerous', 'use_regime': False, 'trigger_mode': 'filter_led', 'min_conviction': 2.5},
    # WR=46.9% PF=1.43 OOS=51.3% | ⚠ Entry T BẮT BUỘC (T+1 kém -14.2%)
    'NVL': {'sl': 0.07, 'tp': 0.14, 'min_score': 55, 'b_filter': False, 'group': 'Bat dong san', 'wf_verdict': 'CHAP_NHAN', 'entry': 'T+1', 'entry_note': 'T+1 tot hon T (+0.53% PnL)', 'shark_mode': 'qualitative', 'use_regime': False, 'use_vwap': False, 'trigger_mode': 'filter_led'},
    # WR=44.1% PF=1.27 OOS=50.7% | Entry T+1 tốt hơn T (+0.53% PnL)

    # ── THEO DÕI — WF tốt nhưng PF<1, chưa đủ điều kiện SIGNALS ─────────────
    'VIC': {'sl': 0.07, 'tp': 0.14, 'min_score': 45, 'group': 'Bat dong san', 'wf_verdict': 'YEU' , 'shark_mode': 'none', 'use_regime': True, 'trigger_mode': 'filter_led'},
    # PF=2.24 PnL=+3.19% nhưng WF YEU (OOS=36%) — 3 TP liên tiếp 2025, theo dõi thêm
    'KBC': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'group': 'KCN',          'wf_verdict': 'TOT' , 'shark_mode': 'S', 'shark_min': 55, 'shark_warn': 65, 'shark_min_hard': 50, 'use_regime': False, 'use_vwap': False, 'trigger_mode': 'score_primary', 'min_conviction': 3.0},
    # WF TOT OOS=58.3% decay=-17.4% nhưng PF=0.84<1 — KCN China+1, theo dõi thêm

    # ── ĐÃ XÓA KHỎI SIGNALS — Giữ lại để backtest nghiên cứu ────────────────
    'FPT': {'sl': 0.05, 'tp': 0.09, 'min_score': 65, 'group': 'Cong nghe',    'wf_verdict': 'CHAP_NHAN'      , 'shark_mode': 'weak', 'shark_min': 55, 'use_regime': True, 'use_vwap': False, 'trigger_mode': 'score_primary', 'min_conviction': 2.5},
    'SZC': {'sl': 0.07, 'tp': 0.14, 'min_score': 45, 'group': 'KCN',          'wf_verdict': 'YEU'      , 'shark_mode': 'S', 'shark_min': 60, 'use_regime': False, 'use_vwap': False, 'trigger_mode': 'filter_led', 'min_conviction': 3.0},
    'PC1': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'group': 'Dien',         'wf_verdict': 'YEU'      , 'shark_mode': 'S', 'shark_min': 50},
    'KDH': {'sl': 0.07, 'tp': 0.14, 'min_score': 45, 'group': 'Bat dong san', 'wf_verdict': 'YEU'      , 'shark_mode': 'S', 'shark_min': 60, 'shark_min_hard': 50, 'use_regime': False, 'entry': 'T+1', 'entry_note': 'T+1 tot hon T (+6.9% WR)', 'trigger_mode': 'filter_led'},
    'GAS': {'sl': 0.07, 'tp': 0.14, 'min_score': 45, 'group': 'Dau khi',      'wf_verdict': 'YEU'      , 'shark_mode': 'S', 'shark_min': 55, 'shark_warn': 65, 'shark_min_hard': 50, 'use_regime': True, 'trigger_mode': 'filter_led', 'min_conviction': 3.0},
    'PVS': {'sl': 0.07, 'tp': 0.14, 'min_score': 45, 'group': 'Dau khi',      'wf_verdict': 'YEU'      , 'shark_mode': 'S', 'shark_min': 55, 'use_regime': True, 'entry': 'T+1', 'entry_note': 'T+1 tot hon T (+6.5% WR, +1.43% PnL)', 'trigger_mode': 'filter_led', 'min_conviction': 3.0},
    'POW': {'sl': 0.07, 'tp': 0.14, 'min_score': 45, 'group': 'Dien',         'wf_verdict': 'TOT'      , 'shark_mode': 'S', 'shark_min': 60, 'use_regime': True, 'trigger_mode': 'filter_led', 'min_conviction': 2.5},
    # ML Sprint 6: WR=52.4% PF=2.10 WF Robust (đã update từ YEU)

    # ── CHƯA BACKTEST — Giữ để nghiên cứu sau ────────────────────────────────
    'HPG': {'sl': 0.07, 'tp': 0.14, 'min_score': 55, 'group': 'Thep',         'wf_verdict': 'YEU'         , 'shark_mode': 'none', 'use_regime': False, 'use_vwap': False, 'trigger_mode': 'filter_confirm'},
    'TCB': {'sl': 0.05, 'tp': 0.09, 'min_score': 55, 'group': 'Ngan hang',    'wf_verdict': 'YEU'         , 'shark_mode': 'S', 'shark_min': 60, 'use_regime': False, 'entry': 'T+1', 'entry_note': 'T+1 tot hon T (+12.5% WR)', 'trigger_mode': 'filter_led'},
    'VPB': {'sl': 0.05, 'tp': 0.09, 'min_score': 55, 'group': 'Ngan hang',    'wf_verdict': 'CHAP_NHAN'   , 'shark_mode': 'S', 'shark_min': 65, 'use_regime': False, 'trigger_mode': 'filter_confirm'},
    # ML Sprint 6: WR=52.9% PF=2.03 WF decay +0.3% — Trung bình (đã update từ YEU)
    'VCB': {'sl': 0.05, 'tp': 0.09, 'min_score': 55, 'group': 'Ngan hang',    'wf_verdict': 'YEU'      , 'shark_mode': 'AS', 'shark_min': 50, 'shark_warn': 65, 'use_regime': False, 'trigger_mode': 'filter_confirm', 'min_conviction': 3.0},
    'MWG': {'sl': 0.05, 'tp': 0.09, 'min_score': 65, 'group': 'Ban le',       'wf_verdict': 'CHAP_NHAN'         , 'shark_mode': 'weak', 'shark_min': 60, 'use_regime': True, 'trigger_mode': 'score_primary'},
    'CMG': {'sl': 0.05, 'tp': 0.09, 'min_score': 55, 'group': 'Cong nghe',    'wf_verdict': 'YEU'         , 'shark_mode': 'AS', 'shark_min': 60, 'use_regime': True, 'entry': 'T+1', 'entry_note': 'T+1 tot hon T (+3.8% WR)', 'trigger_mode': 'filter_led'},
    'PVD': {'sl': 0.07, 'tp': 0.14, 'min_score': 45, 'group': 'Dau khi',      'wf_verdict': 'YEU'         , 'shark_mode': 'S', 'shark_min': 55, 'use_regime': True, 'trigger_mode': 'filter_led', 'min_conviction': 2.5},
    'REE': {'sl': 0.07, 'tp': 0.14, 'min_score': 45, 'group': 'Dien',         'wf_verdict': 'YEU'         , 'shark_mode': 'S', 'shark_min': 65, 'shark_min_hard': 50, 'use_regime': False, 'entry': 'T+1', 'entry_note': 'T+1 dao nguoc WR+8.5% PnL+1.35%', 'trigger_mode': 'filter_led'},
}

# Default khi mã chưa có trong SYMBOL_CONFIG
DEFAULT_SL = 0.07
DEFAULT_TP = 0.14

# ═══════════════════════════════════════════════════════════════════════════════
# WATCHLISTS — Danh sách mã theo từng mục đích
# ═══════════════════════════════════════════════════════════════════════════════

# SIGNALS_WATCHLIST: mã AUTO — xuất hiện trong /signals và background cache alert
# Cập nhật: 06/04/2026 — sau ML Backtest Sprint 6 (47 mã, combo sl=5 tp=15 s=90)
#
# TIER 1 — TIN CẬY CAO (7 mã, WR>=53% PF>=2.3, WF Robust)
#   Dùng ML signal làm trigger chính, không cần điều kiện lọc thêm
#   DGC  WR=66.7% PF=3.72 WF=-1.8%  — Hóa chất
#   SSI  WR=57.1% PF=3.29 WF=-19.5% — Chứng khoán
#   GEX  WR=56.8% PF=3.02 WF=-13.0% — Điện cơ (mới)
#   HCM  WR=58.8% PF=2.54 WF=+2.5%  — Chứng khoán
#   BSR  WR=55.6% PF=2.52 WF=-13.7% — Lọc hóa dầu (mới)
#   NKG  WR=53.6% PF=2.65 WF=+18.2% — Thép
#   FRT  WR=55.9% PF=2.34 WF=-27.0% — Bán lẻ
#
# TIER 2 — CHẤP NHẬN (6 mã, PF 1.7–2.6, kết hợp regime filter hoặc Score A)
#   VPB  WR=52.9% PF=2.03 WF=+0.3%  — Ngân hàng tư
#   POW  WR=52.4% PF=2.10 WF=-10.0% — Điện
#   HSG  WR=51.7% PF=1.85 WF=-10.4% — Thép
#   HAH  WR=51.6% PF=1.88 WF=+4.5%  — Vận tải biển
#   BID  WR=52.0% PF=1.71 WF=-28.4% — Ngân hàng quốc doanh
#   NVL  WR=60.9% PF=2.60 WF=+12.9% — BĐS (mới, WF decay chú ý)
#
SIGNALS_WATCHLIST = [
    # ── Tier 1 — Tin cậy cao (7 mã) ──────────────────────────────────────────
    'DGC',   # Hóa chất      | PF=3.72 | WR=66.7% | WF Robust
    'SSI',   # Chứng khoán   | PF=3.29 | WR=57.1% | WF Robust
    'GEX',   # Điện cơ       | PF=3.02 | WR=56.8% | WF Robust
    'HCM',   # Chứng khoán   | PF=2.54 | WR=58.8% | WF Robust
    'BSR',   # Lọc hóa dầu   | PF=2.52 | WR=55.6% | WF Robust
    'NKG',   # Thép          | PF=2.65 | WR=53.6% | WF decay +18.2% (chú ý)
    'FRT',   # Bán lẻ        | PF=2.34 | WR=55.9% | WF Robust
    # ── Tier 2 — Chấp nhận (6 mã) ────────────────────────────────────────────
    'VPB',   # Ngân hàng tư  | PF=2.03 | WR=52.9% | WF decay +0.3%
    'POW',   # Điện          | PF=2.10 | WR=52.4% | WF Robust
    'HSG',   # Thép          | PF=1.85 | WR=51.7% | WF Robust
    'HAH',   # Vận tải biển  | PF=1.88 | WR=51.6% | WF decay +4.5%
    'BID',   # NH quốc doanh | PF=1.71 | WR=52.0% | WF decay -28.4% (chú ý)
    'NVL',   # BĐS           | PF=2.60 | WR=60.9% | WF decay +12.9% (chú ý)
]

# SIGNALS_MANUAL: mã theo dõi thủ công — KHÔNG auto-signal
# Chỉ trade khi có điều kiện đặc biệt hoặc regime rõ ràng
# Cập nhật: 06/04/2026
#   KBC  WR=46.7% PF=1.73 — KCN, PF ok nhưng WR thấp
#   DCM  WR=46.7% PF=1.76 — Hóa chất, WR thấp nhưng PF chấp nhận
#   VND  WR=43.9% PF=1.59 — Chứng khoán, WF Robust nhưng WR biên
#   VIC  WR=40.0% PF=1.73 — BĐS, payoff asymmetric tốt (+12.8% vs -4.9%)
#   NVL  WR=60.9% PF=2.60 — BĐS, WR cao nhưng WF decay +12.9%
#   PDR  WR=54.8% PF=1.93 — BĐS, WF decay +21.4% (thận trọng)
#   DGW  WR=43.9% PF=1.70 — Phân phối, WF OOS tốt hơn IS
SIGNALS_MANUAL = [
    'MBB',   # Ngân hàng tư | PF=1.49 | WR=47.0% | min_score=85 | OOS=50%
    'KBC',   # KCN          | PF=1.73 | WR=46.7% | min_score=80
    'DCM',   # Hóa chất     | PF=1.76 | WR=46.7% | min_score=80
    'VND',   # Chứng khoán  | PF=1.59 | WR=43.9% | min_score=80
    'VIC',   # BĐS          | PF=1.73 | WR=40.0% | min_score=85 | payoff asymmetric
    'PDR',   # BĐS          | PF=1.93 | WR=54.8% | min_score=85 | WF decay cao
    'DGW',   # Phân phối    | PF=1.70 | WR=43.9% | min_score=80
]

# BACKTEST_WATCHLIST: ~30 mã ML-approved dùng cho /mlbt all và /mlscan
# Chỉ giữ mã có PF>1.5 và đã backtest đầy đủ — loại bỏ noise
# Cập nhật: Sprint 7 — 08/04/2026
BACKTEST_WATCHLIST = [
    # ── ML Confirmed Tier A (full size) ────────────────────────────────────────
    'HCM', 'FRT', 'VCI', 'LPB',
    # ── ML Confirmed Tier B (size 70-80%) ──────────────────────────────────────
    'DGC', 'NKG', 'SSI', 'MWG', 'VIX', 'BSI', 'ORS', 'HDB',
    # ── ML Confirmed Tier B* (size 30%, STRONG-only) ───────────────────────────
    'POM',
    # ── Theo dõi (chưa confirm, chạy bt riêng) ────────────────────────────────
    'OCB', 'MBS',
    # ── Manual / SIGNALS_WATCHLIST (chạy bt riêng, không dùng /mlbt all) ──────
    'MBB', 'KBC', 'DCM', 'VND', 'VIC', 'PDR', 'DGW',
]

# ML_CONFIRMED_WATCHLIST: 13 mã đã xác nhận edge — dùng cho auto alert ML
# Cập nhật: Sprint 7 — sau backtest 4 đợt mở rộng (42 mã, hit rate 31%)
# LƯU Ý GRADE FILTER:
#   HDB: chỉ vào lệnh khi grade == 'STRONG' (score >= 90)
#   POM: chỉ vào lệnh khi grade == 'STRONG'. HNX → SL=8%
ML_CONFIRMED_WATCHLIST = {
    # sym   : (tier, sl_pct, tp_pct, hold_days, note)
    # ── Tier A — full size ────────────────────────────────────────────────────
    'HCM': ('A', 5, 15, 18, 'WR=68% PF=2.96 OOS=53%'),
    'FRT': ('A', 5, 15, 18, 'WR=62% PF=2.06 OOS=53%'),
    'VCI': ('A', 5, 15, 18, 'WR=60% PF=3.33 OOS=80% — n_OOS nho'),
    'LPB': ('A', 6, 17, 18, 'WR=64% PF=2.62 OOS=67% — dot 3, WF Robust xuat sac'),
    # ── Tier B — size 70-80% ─────────────────────────────────────────────────
    'DGC': ('B', 5, 15, 18, 'WR=52% PF=2.07 OOS=72%'),
    'NKG': ('B', 5, 15, 18, 'WR=52% PF=2.30 OOS=60%'),
    'SSI': ('B', 5, 15, 18, 'WR=52% PF=2.33 OOS=58%'),
    'MWG': ('B', 5, 15, 18, 'WR=58% PF=1.85 OOS=42% — than trong'),
    'VIX': ('B', 6, 17, 18, 'WR=51% PF=2.10 OOS=52% — dot 1'),
    'BSI': ('B', 6, 17, 18, 'WR=50% PF=2.09 OOS=52% — dot 1'),
    'ORS': ('B', 6, 17, 18, 'WR=52% PF=1.53 OOS=61% — dot 2, WF Robust'),
    'HDB': ('B', 6, 17, 18, 'WR=65%* PF=1.73* OOS=66% — dot 4, CHI STRONG grade'),
    # ── Tier B* — size 30%, strict condition ─────────────────────────────────
    'POM': ('B*', 8, 17, 18, 'WR=57% PF=2.63 OOS=42% — HNX, CHI STRONG, SL=8%'),
}

def get_sl_tp(symbol):
    """
    Lấy (sl_float, tp_float) cho symbol.
    Trả về số thập phân: 0.05 = 5%, 0.07 = 7%.
    Ưu tiên SYMBOL_CONFIG → DEFAULT.
    """
    cfg = SYMBOL_CONFIG.get(symbol.upper(), {})
    return cfg.get('sl', DEFAULT_SL), cfg.get('tp', DEFAULT_TP)


def get_sl_tp_pct(symbol):
    """
    Lấy (sl_pct_int, tp_pct_int) cho symbol.
    Trả về số nguyên %: 5 = 5%, 7 = 7%.
    Dùng cho Telegram hiển thị và paper trading.
    """
    sl, tp = get_sl_tp(symbol)
    return int(sl * 100), int(tp * 100)


def get_min_score(symbol=None):
    """
    Ngưỡng MUA per-symbol từ SYMBOL_CONFIG, fallback về MIN_SCORE_BUY=65.
    Sync từ watchlist_setup.docx:
      Tier 1 (DGC/DCM/SSI/NKG/MBB/HCM) = 65
      Tier 2 (HSG/BID) = 75 | FRT = 70
      Manual (VND/PDR) = 75 | NVL = 78 | NT2 = 80
    """
    if symbol:
        cfg = SYMBOL_CONFIG.get(str(symbol).upper(), {})
        return cfg.get('min_score', MIN_SCORE_BUY)
    return MIN_SCORE_BUY


def get_b_filter(symbol):
    """
    B-filter per-symbol từ SYMBOL_CONFIG.
    True = bật breakout filter | False = tắt (default an toàn)
    Sync từ watchlist_setup.docx: DCM=True, FRT=True, còn lại=False
    """
    cfg = SYMBOL_CONFIG.get(symbol.upper(), {})
    return cfg.get('b_filter', False)


def get_symbol_group(symbol):
    """Lấy nhóm ngành của symbol."""
    return SYMBOL_CONFIG.get(symbol.upper(), {}).get('group', 'Khac')


def get_wf_verdict(symbol):
    """Lấy kết quả walk-forward của symbol ('TOT'/'CHAP'/'YEU'/'')."""
    return SYMBOL_CONFIG.get(symbol.upper(), {}).get('wf_verdict', '')


# ═══════════════════════════════════════════════════════════════════════════
# SHARK CONFIG HELPER — Dựa trên backtest 10 mã (2017-2024)
# ═══════════════════════════════════════════════════════════════════════════

# P&L backtest thực tế và verdict theo từng mã — module-level constant (không tạo lại mỗi call)
# Nguồn: backtest 7 năm (2017-2024), ngưỡng: P&L >= 1.2% mới đủ tiêu chuẩn
_SHARK_PNL_DATA = {
    # sym : (pnl,  verdict_line,                              note)
    'VCB': (3.96, 'Shark có ích khi KẾT HỢP Score A',       'Không dùng khi Shark >= 65'),
    'PC1': (2.18, 'Shark có ích ĐỘC LẬP',                   ''),
    'PVD': (2.02, 'Shark có ích ĐỘC LẬP',                   ''),
    'FRT': (1.97, 'Shark có ích khi KẾT HỢP Score A',       'Không dùng khi Shark >= 60'),
    'CMG': (1.69, 'Shark có ích khi KẾT HỢP Score A',       ''),
    'POW': (1.59, 'Shark có ích ĐỘC LẬP',                   ''),
    'DCM': (1.59, 'Shark có ích ĐỘC LẬP',                   ''),
    'PVS': (1.54, 'Shark có ích ĐỘC LẬP',                   ''),
    'KDH': (1.52, 'Shark có ích ĐỘC LẬP',                   'Không vào lệnh nếu Shark < 50'),
    'TCB': (1.51, 'Shark có ích ĐỘC LẬP',                   ''),
    'BID': (1.46, 'Shark có ích khi KẾT HỢP Score A',       ''),
    # Vùng xám — giữ nhưng kèm note
    'MBB': (1.09, 'Shark có ích ĐỘC LẬP',                   'P&L backtest thấp (+1.09%) — tín hiệu vừa'),
    'VPB': (1.04, 'Shark có ích ĐỘC LẬP',                   'P&L backtest thấp (+1.04%) — tín hiệu vừa'),
    # Loại — P&L < 1.2%
    'SZC': (0.88, 'Shark không đủ tin cậy (P&L thấp)',      'P&L +0.88% dưới ngưỡng 1.2%'),
    'HCM': (0.50, 'Shark không đủ tin cậy (P&L thấp)',      'Chỉ dùng Shark 55-59, tránh >= 60 (P&L âm)'),
    'KBC': (0.69, 'Shark không đủ tin cậy (P&L thấp)',      'P&L +0.69% dưới ngưỡng 1.2%'),
    'GAS': (0.44, 'Shark không đủ tin cậy (P&L thấp)',      'P&L +0.44% dưới ngưỡng | Tránh >= 65 (cliff)'),
    'REE': (0.49, 'Shark không đủ tin cậy (P&L thấp)',      'P&L +0.49% dưới ngưỡng 1.2%'),
    # Weak group
    'FPT': (0.70, 'Shark yếu — bonus nhẹ không bắt buộc',  ''),
    'NT2': (0.34, 'Shark yếu — bonus nhẹ không bắt buộc',  ''),
    'MWG': (0.61, 'Shark yếu — bonus nhẹ không bắt buộc',  ''),
}


def get_shark_config(symbol: str) -> dict:
    """
    Trả về cấu hình Shark Detector cho từng mã dựa trên backtest thực nghiệm.

    Returns dict:
      mode         : 'S' | 'AS' | 'none' | 'qualitative' | 'weak' | 'dangerous'
      min_score    : ngưỡng Shark tối thiểu (0 = không dùng)
      warn_score   : ngưỡng Shark bắt đầu nguy hiểm (0 = không có)
      shark_min_hard: không vào lệnh nếu Shark < ngưỡng này (0 = không có)
      pnl_ok       : True nếu P&L backtest >= 1.2% (đủ tiêu chuẩn)
      pnl_val      : P&L backtest thực tế (%)
      verdict      : kết luận ngắn gọn để hiển thị trong bot
      note         : ghi chú thêm (ngưỡng tránh, điều kiện đặc biệt)
    """
    cfg   = SYMBOL_CONFIG.get(symbol.upper(), {})
    mode  = cfg.get('shark_mode')
    mn    = cfg.get('shark_min',  0)
    warn  = cfg.get('shark_warn', 0)
    hard  = cfg.get('shark_min_hard', 0)

    # Mã không có ích / nguy hiểm / định tính
    if mode == 'dangerous':
        return {
            'mode': 'dangerous', 'min_score': 0, 'warn_score': 0, 'shark_min_hard': 0,
            'pnl_ok': False, 'pnl_val': None,
            'verdict': '⛔ Shark PHẢN TÁC DỤNG — điểm càng cao càng nguy hiểm, KHÔNG mua',
            'note': '',
        }
    if mode == 'none':
        return {
            'mode': 'none', 'min_score': 0, 'warn_score': 0, 'shark_min_hard': 0,
            'pnl_ok': False, 'pnl_val': None,
            'verdict': '❌ Shark không có ích — dùng Score A đơn thuần',
            'note': '',
        }
    if mode == 'qualitative':
        return {
            'mode': 'qualitative', 'min_score': 0, 'warn_score': 0, 'shark_min_hard': 0,
            'pnl_ok': False, 'pnl_val': None,
            'verdict': '⚠ Không dùng Shark — theo dõi tin pháp lý',
            'note': '',
        }

    # Mã có mode — tra P&L data từ module-level constant _SHARK_PNL_DATA
    sym_up  = symbol.upper()
    pdata   = _SHARK_PNL_DATA.get(sym_up)
    pnl_val = pdata[0] if pdata else None
    pnl_ok  = (pnl_val is not None and pnl_val >= 1.2)

    if pdata:
        verdict_line = pdata[1]
        extra_note   = pdata[2]
    else:
        verdict_line = 'Shark có ích' if mode in ('S','AS') else 'Shark yếu'
        extra_note   = 'Chưa có dữ liệu P&L backtest'

    # Build verdict string đầy đủ
    _emoji = '✅' if pnl_ok else ('⚠' if pnl_val is not None else '✅')
    if mode == 'weak':
        verdict = f'〰 {verdict_line}'
        if mn: verdict += f' (Shark >= {mn})'
    elif mode == 'AS':
        verdict = f'{_emoji} {verdict_line}: Score A >= 65 VÀ Shark >= {mn}'
        if warn: verdict += f' | Không dùng >= {warn}'
    elif mode == 'S':
        verdict = f'{_emoji} {verdict_line}: Shark >= {mn}'
        if hard: verdict += f' | Không vào nếu Shark < {hard}'
        if warn: verdict += f' | Không dùng >= {warn}'
    else:
        verdict = verdict_line

    # Thêm cảnh báo P&L nếu không đủ tiêu chuẩn
    if pnl_val is not None and not pnl_ok:
        verdict += f' ⚠ P&L backtest thấp ({pnl_val:+.2f}%)'

    return {
        'mode':          mode,
        'min_score':     mn,
        'warn_score':    warn,
        'shark_min_hard':hard,
        'pnl_ok':        pnl_ok,
        'pnl_val':       pnl_val,
        'verdict':       verdict,
        'note':          extra_note,
    }

