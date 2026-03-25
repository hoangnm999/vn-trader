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
    'DGC': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'b_filter': False, 'group': 'Hoa chat',     'wf_verdict': 'TOT'  , 'shark_mode': 'none', 'use_regime': False},
    # WR=55.6% PF=2.45 OOS=56.4% | B-filter TẮT (breakout bị lọc nhầm) | Entry: T bắt buộc
    'DCM': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'b_filter': True,  'group': 'Hoa chat',     'wf_verdict': 'TOT'  , 'shark_mode': 'S', 'shark_min': 65, 'use_regime': False},
    # WR=55.4% PF=1.99 OOS=59.5% decay=-12% | B-filter BẬT (+2.3% WR) | Entry: T khuyến nghị
    'SSI': {'sl': 0.05, 'tp': 0.09, 'min_score': 65, 'b_filter': False, 'group': 'Chung khoan',  'wf_verdict': 'TOT'  , 'shark_mode': 'none', 'use_regime': True},
    # WR=58.3% PF=1.82 OOS=61.1% | B-filter N/A (0 lệnh) | reproducibility cao nhất
    'NKG': {'sl': 0.07, 'tp': 0.10, 'min_score': 65, 'b_filter': False, 'group': 'Thep',         'wf_verdict': 'TOT',  'entry': 'T',   'entry_note': 'T bat buoc (-4.6% WR neu T+1)', 'shark_mode': 'none', 'use_regime': False},
    # WR=58.1% PF=1.66 OOS=65.0% | TP 14%→10% (Avg Win +7.1%) | Entry: T BẮT BUỘC (-4.6% WR nếu T+1)
    'MBB': {'sl': 0.05, 'tp': 0.07, 'min_score': 65, 'b_filter': False, 'group': 'Ngan hang',    'wf_verdict': 'CHAP_NHAN',  'entry': 'T',   'entry_note': 'T bat buoc (-10.9% WR neu T+1)', 'shark_mode': 'S', 'shark_min': 55, 'use_regime': True},
    # WR=56.4% PF=1.50 OOS=58.9% | TP 9%→7% (Avg Win +5.1%) | Entry: T BẮT BUỘC (-10.9% WR nếu T+1)
    'HCM': {'sl': 0.06, 'tp': 0.10, 'min_score': 65, 'b_filter': False, 'group': 'Chung khoan',  'wf_verdict': 'TOT' , 'shark_mode': 'S', 'shark_min': 55, 'use_regime': True},
    # WR=51.9% PF=1.45 OOS=50.2% | SL 5%→6% (giảm SL rate 38.9%) | TP 9%→10%

    # ── TIER 2 — AUTO TRADE CẨN THẬN (3 mã) ─────────────────────────────────
    'HSG': {'sl': 0.07, 'tp': 0.10, 'min_score': 75, 'b_filter': False, 'group': 'Thep',         'wf_verdict': 'CHAP_NHAN'  , 'shark_mode': 'none', 'use_regime': True},
    # WR=57.1% PF=1.91 OOS=52.8% | TP 14%→10% | Decay adj=-0.2% (bỏ outlier 2021) | Score<75 rủi ro
    'FRT': {'sl': 0.07, 'tp': 0.10, 'min_score': 70, 'b_filter': True,  'group': 'Ban le',       'wf_verdict': 'TOT'  , 'shark_mode': 'AS', 'shark_min': 50, 'shark_warn': 60, 'use_regime': True},
    # WR=51.9% PF=1.35 OOS=55.7% | TP 14%→10% | B-filter BẬT (+1.9% WR) | Score 65-69 WR=45.5%
    'BID': {'sl': 0.05, 'tp': 0.07, 'min_score': 75, 'b_filter': False, 'group': 'Ngan hang',    'wf_verdict': 'YEU' , 'shark_mode': 'AS', 'shark_min': 50, 'use_regime': True},
    # WR=50.0% PF=1.29 OOS=50.0% | TP 9%→7% | 4/5 cửa sổ WF có OOS≥IS

    # ── TIER 3 — MANUAL ONLY (4 mã, không vào SIGNALS_WATCHLIST) ─────────────
    'VND': {'sl': 0.05, 'tp': 0.09, 'min_score': 75, 'b_filter': False, 'group': 'Chung khoan',  'wf_verdict': 'CHAP_NHAN'  , 'shark_mode': 'none', 'use_regime': True},
    # WR=52.2% PF=1.28 OOS=61.3% | Score 65-74: WR=47.1% (noise) → chỉ trade >=75
    'NT2': {'sl': 0.03, 'tp': 0.06, 'min_score': 80, 'b_filter': False, 'group': 'Dien',         'wf_verdict': 'CHAP' , 'shark_mode': 'AS', 'shark_min': 55, 'shark_warn': 60},
    # WR=50.9% PF=1.15 OOS=50.8% | SL 5%→3% TP 9%→6% (beta 0.6x) | Score 65-79 WR=39.1%
    'PDR': {'sl': 0.07, 'tp': 0.14, 'min_score': 75, 'b_filter': False, 'group': 'Bat dong san', 'wf_verdict': 'CHAP_NHAN', 'entry': 'T',   'entry_note': 'T bat buoc (-14.2% WR neu T+1)', 'shark_mode': 'dangerous', 'use_regime': True},
    # WR=46.9% PF=1.43 OOS=51.3% | ⚠ Entry T BẮT BUỘC (T+1 kém -14.2%)
    'NVL': {'sl': 0.07, 'tp': 0.14, 'min_score': 78, 'b_filter': False, 'group': 'Bat dong san', 'wf_verdict': 'CHAP_NHAN', 'entry': 'T+1', 'entry_note': 'T+1 tot hon T (+0.53% PnL)', 'shark_mode': 'qualitative', 'use_regime': True},
    # WR=44.1% PF=1.27 OOS=50.7% | Entry T+1 tốt hơn T (+0.53% PnL)

    # ── THEO DÕI — WF tốt nhưng PF<1, chưa đủ điều kiện SIGNALS ─────────────
    'VIC': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'group': 'Bat dong san', 'wf_verdict': 'YEU' , 'shark_mode': 'none', 'use_regime': True},
    # PF=2.24 PnL=+3.19% nhưng WF YEU (OOS=36%) — 3 TP liên tiếp 2025, theo dõi thêm
    'KBC': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'group': 'KCN',          'wf_verdict': 'TOT' , 'shark_mode': 'S', 'shark_min': 55, 'shark_warn': 65, 'shark_min_hard': 50, 'use_regime': True},
    # WF TOT OOS=58.3% decay=-17.4% nhưng PF=0.84<1 — KCN China+1, theo dõi thêm

    # ── ĐÃ XÓA KHỎI SIGNALS — Giữ lại để backtest nghiên cứu ────────────────
    'FPT': {'sl': 0.05, 'tp': 0.09, 'min_score': 65, 'group': 'Cong nghe',    'wf_verdict': 'CHAP_NHAN'      , 'shark_mode': 'weak', 'shark_min': 55, 'use_regime': True},
    'SZC': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'group': 'KCN',          'wf_verdict': 'YEU'      , 'shark_mode': 'S', 'shark_min': 60, 'use_regime': True},
    'PC1': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'group': 'Dien',         'wf_verdict': 'YEU'      , 'shark_mode': 'S', 'shark_min': 50},
    'KDH': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'group': 'Bat dong san', 'wf_verdict': 'YEU'      , 'shark_mode': 'S', 'shark_min': 60, 'shark_min_hard': 50, 'use_regime': True},
    'GAS': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'group': 'Dau khi',      'wf_verdict': 'YEU'      , 'shark_mode': 'S', 'shark_min': 55, 'shark_warn': 65, 'shark_min_hard': 50, 'use_regime': True},
    'PVS': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'group': 'Dau khi',      'wf_verdict': 'YEU'      , 'shark_mode': 'S', 'shark_min': 55, 'use_regime': True},
    'POW': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'group': 'Dien',         'wf_verdict': 'YEU'      , 'shark_mode': 'S', 'shark_min': 60, 'use_regime': True},

    # ── CHƯA BACKTEST — Giữ để nghiên cứu sau ────────────────────────────────
    'HPG': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'group': 'Thep',         'wf_verdict': 'YEU'         , 'shark_mode': 'none', 'use_regime': True},
    'TCB': {'sl': 0.05, 'tp': 0.09, 'min_score': 65, 'group': 'Ngan hang',    'wf_verdict': 'YEU'         , 'shark_mode': 'S', 'shark_min': 60, 'use_regime': True},
    'VPB': {'sl': 0.05, 'tp': 0.09, 'min_score': 65, 'group': 'Ngan hang',    'wf_verdict': 'YEU'         , 'shark_mode': 'S', 'shark_min': 65, 'use_regime': True},
    'VCB': {'sl': 0.05, 'tp': 0.09, 'min_score': 65, 'group': 'Ngan hang',    'wf_verdict': 'YEU'      , 'shark_mode': 'AS', 'shark_min': 50, 'shark_warn': 65, 'use_regime': True},
    'MWG': {'sl': 0.05, 'tp': 0.09, 'min_score': 65, 'group': 'Ban le',       'wf_verdict': 'CHAP_NHAN'         , 'shark_mode': 'weak', 'shark_min': 60, 'use_regime': True},
    'CMG': {'sl': 0.05, 'tp': 0.09, 'min_score': 65, 'group': 'Cong nghe',    'wf_verdict': 'YEU'         , 'shark_mode': 'AS', 'shark_min': 60, 'use_regime': True},
    'PVD': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'group': 'Dau khi',      'wf_verdict': 'YEU'         , 'shark_mode': 'S', 'shark_min': 55, 'use_regime': True},
    'REE': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'group': 'Dien',         'wf_verdict': 'YEU'         , 'shark_mode': 'S', 'shark_min': 65, 'shark_min_hard': 50, 'use_regime': True},
}

# Default khi mã chưa có trong SYMBOL_CONFIG
DEFAULT_SL = 0.07
DEFAULT_TP = 0.14

# ═══════════════════════════════════════════════════════════════════════════════
# WATCHLISTS — Danh sách mã theo từng mục đích
# ═══════════════════════════════════════════════════════════════════════════════

# SIGNALS_WATCHLIST: 9 mã AUTO — xuất hiện trong /signals và background cache alert
# Sync từ watchlist_setup.docx — WATCHLIST = Tier1 + Tier2 (có điều kiện bổ sung)
# Cập nhật: 20/03/2026
#
# TIER 1 — AUTO TRADE (6 mã, min_score=65):
#   DGC  WR=55.6% PF=2.45 OOS=56.4% — Hóa chất | B-filter: TẮT | Entry: T
#   DCM  WR=55.4% PF=1.99 OOS=59.5% — Hóa chất | B-filter: BẬT | Entry: T
#   SSI  WR=58.3% PF=1.82 OOS=61.1% — CK       | B-filter: N/A | Entry: T
#   NKG  WR=58.1% PF=1.66 OOS=65.0% — Thép     | B-filter: TẮT | Entry: T bắt buộc
#   MBB  WR=56.4% PF=1.50 OOS=58.9% — NH tư    | B-filter: N/A | Entry: T bắt buộc
#   HCM  WR=51.9% PF=1.45 OOS=50.2% — CK       | B-filter: N/A | Entry: T/T+1
#
# TIER 2 — AUTO TRADE CẨN THẬN (3 mã, min_score=70-75):
#   HSG  WR=57.1% PF=1.91 OOS=52.8% — Thép     | B-filter: TẮT | min_score=75
#   FRT  WR=51.9% PF=1.35 OOS=55.7% — Bán lẻ   | B-filter: BẬT | min_score=70
#   BID  WR=50.0% PF=1.29 OOS=50.0% — NH quốc doanh | B-filter: N/A | min_score=75
SIGNALS_WATCHLIST = [
    # ── Tier 1 — AUTO TRADE (6 mã) ───────────────────────────────────────────
    'DGC',   # Hóa chất  | PF=2.45 | OOS=56.4% | min_score=65 | b_filter=False
    'DCM',   # Hóa chất  | PF=1.99 | OOS=59.5% | min_score=65 | b_filter=True
    'SSI',   # CK        | PF=1.82 | OOS=61.1% | min_score=65 | b_filter=False
    'NKG',   # Thép      | PF=1.66 | OOS=65.0% | min_score=65 | b_filter=False | Entry T bắt buộc
    'MBB',   # NH tư     | PF=1.50 | OOS=58.9% | min_score=65 | b_filter=False | Entry T bắt buộc
    'HCM',   # CK        | PF=1.45 | OOS=50.2% | min_score=65 | b_filter=False
    # ── Tier 2 — AUTO TRADE CẨN THẬN (3 mã) ─────────────────────────────────
    'HSG',   # Thép      | PF=1.91 | OOS=52.8% | min_score=75 | b_filter=False
    'FRT',   # Bán lẻ    | PF=1.35 | OOS=55.7% | min_score=70 | b_filter=True
    'BID',   # NH quốc doanh | PF=1.29 | OOS=50.0% | min_score=75 | b_filter=False
]

# SIGNALS_MANUAL: 4 mã theo dõi thủ công — KHÔNG auto-signal
# Sync từ watchlist_setup.docx — WATCHLIST_MANUAL
# Chỉ trade khi có điều kiện đặc biệt, min_score cao hơn (75-80)
SIGNALS_MANUAL = [
    'VND',   # CK   | PF=1.28 | OOS=61.3% | min_score=75 | Score<75 WR=47.1% (noise)
    'NT2',   # Điện | PF=1.15 | OOS=50.8% | min_score=80 | Score<80 WR=39.1% | SL=3% TP=6%
    'PDR',   # BĐS  | PF=1.43 | OOS=51.3% | min_score=75 | ⚠ Entry T BẮT BUỘC
    'NVL',   # BĐS  | PF=1.27 | OOS=50.7% | min_score=78 | Entry T+1 tốt hơn
]

# BACKTEST_WATCHLIST: mã dùng cho /bt và nghiên cứu — không nhất thiết trong SIGNALS
# Bao gồm cả mã đã xóa khỏi SIGNALS để theo dõi xu hướng thay đổi
BACKTEST_WATCHLIST = [
    # ── Tier 1 + Tier 2 (trong SIGNALS) ──────────────────────────────────────
    'DGC', 'DCM', 'NKG', 'MBB', 'SSI', 'FRT', 'VND',
    'HSG', 'PDR', 'HCM', 'BID', 'NVL', 'NT2',
    # ── Theo dõi (PF<1 nhưng WF tốt — tiềm năng) ────────────────────────────
    'VIC', 'KBC',
    # ── Ngân hàng chưa backtest đầy đủ ───────────────────────────────────────
    'TCB', 'VPB', 'VCB',
    # ── Công nghệ / Bán lẻ ───────────────────────────────────────────────────
    'FPT', 'MWG', 'CMG',
    # ── Thép ─────────────────────────────────────────────────────────────────
    'HPG',
    # ── Dầu khí / Điện ───────────────────────────────────────────────────────
    'GAS', 'PVS', 'PVD', 'POW', 'PC1', 'REE',
    # ── KCN / BĐS ────────────────────────────────────────────────────────────
    'SZC', 'KDH',
]

# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS — Dùng chung ở mọi file
# ═══════════════════════════════════════════════════════════════════════════════

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

    # ── P&L backtest thực tế và verdict theo từng mã ─────────────────────────
    # Nguồn: backtest 7 năm (2017-2024), ngưỡng: P&L >= 1.2% mới đủ tiêu chuẩn
    _PNL_DATA = {
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
        'VPB': (1.04, 'Shark có ích ĐỘC LẬP',                   'P&L backtest thấp (+1.04%) — tín hiệu vừa'),
        'HCM': (0.50, 'Shark không đủ tin cậy (P&L thấp)',      'Chỉ dùng Shark 55-59, tránh >= 60 (P&L âm)'),
        'KBC': (0.69, 'Shark không đủ tin cậy (P&L thấp)',      'P&L +0.69% dưới ngưỡng 1.2%'),
        'GAS': (0.44, 'Shark không đủ tin cậy (P&L thấp)',      'P&L +0.44% dưới ngưỡng | Tránh >= 65 (cliff)'),
        'REE': (0.49, 'Shark không đủ tin cậy (P&L thấp)',      'P&L +0.49% dưới ngưỡng 1.2%'),
        # Weak group
        'FPT': (0.70, 'Shark yếu — bonus nhẹ không bắt buộc',  ''),
        'NT2': (0.34, 'Shark yếu — bonus nhẹ không bắt buộc',  ''),
        'MWG': (0.61, 'Shark yếu — bonus nhẹ không bắt buộc',  ''),
    }

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

    # Mã có mode — tra P&L data
    sym_up  = symbol.upper()
    pdata   = _PNL_DATA.get(sym_up)
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

