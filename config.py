"""
config.py — Single Source of Truth cho VN Trader Bot
=====================================================
Tất cả config quan trọng được định nghĩa MỘT LẦN ở đây.
app.py, backtest.py, telegram_bot.py đều import từ file này.

Cách thêm mã mới:
  1. Thêm vào SYMBOL_CONFIG với sl, tp, min_score
  2. Thêm vào SIGNALS_WATCHLIST nếu muốn xuất hiện trong /signals
  3. Chạy /backtest <MA> → /wf <MA> để xác nhận → cập nhật wf_verdict
  4. Không cần sửa file nào khác
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
    # Chỉ lưu SL/TP và group ngành.
    # min_score KHÔNG còn per-symbol — tất cả dùng MIN_SCORE_BUY=65 / MAX_SCORE_SELL=35
    # wf_verdict giữ lại để tham khảo (không ảnh hưởng logic giao dịch)
    # ═══════════════════════════════════════════════════════════════════════════

    # ── Bộ A: Bluechip / Tăng trưởng — SL=5% TP=9% ───────────────────────────
    'VCB': {'sl': 0.05, 'tp': 0.09, 'group': 'Ngan hang',   'wf_verdict': 'YEU'      },
    'BID': {'sl': 0.05, 'tp': 0.09, 'group': 'Ngan hang',   'wf_verdict': 'CHAP'     },
    'TCB': {'sl': 0.05, 'tp': 0.09, 'group': 'Ngan hang',   'wf_verdict': ''         },
    'VPB': {'sl': 0.05, 'tp': 0.09, 'group': 'Ngan hang',   'wf_verdict': ''         },
    'MBB': {'sl': 0.05, 'tp': 0.09, 'group': 'Ngan hang',   'wf_verdict': 'CHAP'     },
    'SSI': {'sl': 0.05, 'tp': 0.09, 'group': 'Chung khoan', 'wf_verdict': 'TOT'      },
    'HCM': {'sl': 0.05, 'tp': 0.09, 'group': 'Chung khoan', 'wf_verdict': 'TOT'      },
    'VND': {'sl': 0.05, 'tp': 0.09, 'group': 'Chung khoan', 'wf_verdict': 'YEU'      },
    'FPT': {'sl': 0.05, 'tp': 0.09, 'group': 'Cong nghe',   'wf_verdict': 'YEU'      },
    'MWG': {'sl': 0.05, 'tp': 0.09, 'group': 'Ban le',      'wf_verdict': ''         },
    'CMG': {'sl': 0.05, 'tp': 0.09, 'group': 'Cong nghe',   'wf_verdict': ''         },
    'NT2': {'sl': 0.05, 'tp': 0.09, 'group': 'Dien',        'wf_verdict': 'YEU'      },

    # ── Bộ B: Cyclical / Mid-cap — SL=7% TP=14% ──────────────────────────────
    'DGC': {'sl': 0.07, 'tp': 0.14, 'group': 'Hoa chat',    'wf_verdict': 'TOT'      },
    'DCM': {'sl': 0.07, 'tp': 0.14, 'group': 'Hoa chat',    'wf_verdict': 'TOT'      },
    'NKG': {'sl': 0.07, 'tp': 0.14, 'group': 'Thep',        'wf_verdict': 'TOT'      },
    'HSG': {'sl': 0.07, 'tp': 0.14, 'group': 'Thep',        'wf_verdict': 'CANH_BAO' },
    'HPG': {'sl': 0.07, 'tp': 0.14, 'group': 'Thep',        'wf_verdict': ''         },
    'FRT': {'sl': 0.07, 'tp': 0.14, 'group': 'Ban le',      'wf_verdict': 'CHAP'     },
    'PC1': {'sl': 0.07, 'tp': 0.14, 'group': 'Dien',        'wf_verdict': 'YEU'      },
    'POW': {'sl': 0.07, 'tp': 0.14, 'group': 'Dien',        'wf_verdict': 'YEU'      },
    'REE': {'sl': 0.07, 'tp': 0.14, 'group': 'Dien',        'wf_verdict': ''         },
    'GAS': {'sl': 0.07, 'tp': 0.14, 'group': 'Dau khi',     'wf_verdict': 'YEU'      },
    'PVS': {'sl': 0.07, 'tp': 0.14, 'group': 'Dau khi',     'wf_verdict': 'YEU'      },
    'PVD': {'sl': 0.07, 'tp': 0.14, 'group': 'Dau khi',     'wf_verdict': ''         },
    'KBC': {'sl': 0.07, 'tp': 0.14, 'group': 'KCN',         'wf_verdict': 'TOT'      },
    'SZC': {'sl': 0.07, 'tp': 0.14, 'group': 'KCN',         'wf_verdict': 'YEU'      },
    'NVL': {'sl': 0.07, 'tp': 0.14, 'group': 'Bat dong san','wf_verdict': 'YEU'      },
    'PDR': {'sl': 0.07, 'tp': 0.14, 'group': 'Bat dong san','wf_verdict': 'YEU'      },
    'KDH': {'sl': 0.07, 'tp': 0.14, 'group': 'Bat dong san','wf_verdict': 'YEU'      },
    'VIC': {'sl': 0.07, 'tp': 0.14, 'group': 'Bat dong san','wf_verdict': 'YEU'      },
}

# Default khi mã chưa có trong SYMBOL_CONFIG
DEFAULT_SL = 0.07
DEFAULT_TP = 0.14

# ═══════════════════════════════════════════════════════════════════════════════
# WATCHLISTS — Danh sách mã theo từng mục đích
# ═══════════════════════════════════════════════════════════════════════════════

# SIGNALS_WATCHLIST: mã xuất hiện trong /signals và background cache
# Chỉ giữ mã PF>1.0 và PnL>0 — đã xác nhận bởi backtest 7 năm
# Cập nhật: 19/03/2026 sau phân tích backtest đầy đủ
# XÓA: FPT (PF=0.78), PC1 (OOS=38% thấp) khỏi signals tự động
# THÊM: SSI (WF-OOS=80% — xuất sắc nhất), NKG (WF=TOT)
SIGNALS_WATCHLIST = [
    # ── Tier 1: WF xác nhận, trade tự động ──────────────────────
    'DGC',   # WF-OOS=69%, PF=2.45 — tốt nhất
    'DCM',   # WF-OOS=65%, decay=-0.1% — cực ổn định
    'HCM',   # WF-OOS=61%, WF=TOT
    'NKG',   # WF-OOS=61%, WF=TOT. Ngưỡng MUA giảm 70→60
    'SSI',   # WF-OOS=80% — CAO NHẤT, decay=-2.4%
    # ── Tier 2: Theo dõi, trade thận trọng ───────────────────────
    'MBB',   # WF=CHAP, PF=1.26. Chú ý: T+1 kém T đáng kể
    'FRT',   # WF=CHAP, PF=1.55. Ngưỡng nâng 68→80
    'VND',   # B-filter có giá trị. Ngưỡng 75→78
]

# BACKTEST_WATCHLIST: 28 mã cho backtest toàn diện và nghiên cứu
BACKTEST_WATCHLIST = [
    # ── Ngân hàng ──
    'VCB', 'BID', 'TCB', 'MBB', 'VPB',
    # ── Bất động sản ──
    'VHM', 'VIC', 'NVL', 'PDR',
    # ── Công nghệ / Bán lẻ ──
    'FPT', 'CMG', 'MWG', 'FRT',
    # ── Thép / Hóa chất ──
    'HPG', 'HSG', 'NKG', 'DCM', 'DGC',
    # ── Chứng khoán ──
    'SSI', 'VND', 'HCM',
    # ── Dầu khí ──
    'GAS', 'PVD', 'PVS',
    # ── Tiêu dùng ──
    'VNM', 'MSN',
    # ── Điện / Hạ tầng ──
    'POW', 'REE', 'PC1',
    # ── KCN ──
    'KBC', 'SZC',
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
    Ngưỡng MUA toàn hệ thống — 65 cho TẤT CẢ mã.
    Tham số symbol giữ lại để tương thích API nhưng không dùng nữa.
    """
    return MIN_SCORE_BUY


def get_symbol_group(symbol):
    """Lấy nhóm ngành của symbol."""
    return SYMBOL_CONFIG.get(symbol.upper(), {}).get('group', 'Khac')


def get_wf_verdict(symbol):
    """Lấy kết quả walk-forward của symbol ('TOT'/'CHAP'/'YEU'/'')."""
    return SYMBOL_CONFIG.get(symbol.upper(), {}).get('wf_verdict', '')
