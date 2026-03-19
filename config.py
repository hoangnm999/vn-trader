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
MIN_SCORE_BUY    = 65       # Ngưỡng MUA tối thiểu (global — mỗi mã có thể cao hơn)
MAX_SCORE_SELL   = 35       # Ngưỡng BAN
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
    # ── Bộ A: Bluechip / Tăng trưởng (SL=5% TP=9%) ───────────────────────
    'VCB': {'sl': 0.05, 'tp': 0.09, 'min_score': 80, 'wf_verdict': '',    'group': 'Ngan hang'  },
    'BID': {'sl': 0.05, 'tp': 0.09, 'min_score': 80, 'wf_verdict': '',    'group': 'Ngan hang'  },
    'TCB': {'sl': 0.05, 'tp': 0.09, 'min_score': 75, 'wf_verdict': '',    'group': 'Ngan hang'  },
    'VPB': {'sl': 0.05, 'tp': 0.09, 'min_score': 75, 'wf_verdict': '',    'group': 'Ngan hang'  },
    'FPT': {'sl': 0.05, 'tp': 0.09, 'min_score': 70, 'wf_verdict': '',    'group': 'Cong nghe'  },
    'SSI': {'sl': 0.05, 'tp': 0.09, 'min_score': 70, 'wf_verdict': '',    'group': 'Chung khoan'},
    'MBB': {'sl': 0.05, 'tp': 0.09, 'min_score': 70, 'wf_verdict': 'TOT', 'group': 'Ngan hang'  },
    'HCM': {'sl': 0.05, 'tp': 0.09, 'min_score': 75, 'wf_verdict': 'TOT', 'group': 'Chung khoan'},
    'VND': {'sl': 0.05, 'tp': 0.09, 'min_score': 75, 'wf_verdict': '',    'group': 'Chung khoan'},
    'MWG': {'sl': 0.05, 'tp': 0.09, 'min_score': 70, 'wf_verdict': '',    'group': 'Ban le'     },
    'CMG': {'sl': 0.05, 'tp': 0.09, 'min_score': 70, 'wf_verdict': '',    'group': 'Cong nghe'  },
    'NT2': {'sl': 0.05, 'tp': 0.09, 'min_score': 80, 'wf_verdict': '',    'group': 'Dien'       },

    # ── Bộ B: Cyclical / Mid-cap (SL=7% TP=14%) ──────────────────────────
    'DGC': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'wf_verdict': 'TOT', 'group': 'Hoa chat'   },
    'DCM': {'sl': 0.07, 'tp': 0.14, 'min_score': 70, 'wf_verdict': 'CHAP','group': 'Hoa chat'   },
    'PC1': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'wf_verdict': 'TOT', 'group': 'Dien'       },
    'FRT': {'sl': 0.07, 'tp': 0.14, 'min_score': 68, 'wf_verdict': '',    'group': 'Ban le'     },
    'NKG': {'sl': 0.07, 'tp': 0.14, 'min_score': 70, 'wf_verdict': '',    'group': 'Thep'       },
    'HPG': {'sl': 0.07, 'tp': 0.14, 'min_score': 70, 'wf_verdict': '',    'group': 'Thep'       },
    'HSG': {'sl': 0.07, 'tp': 0.14, 'min_score': 70, 'wf_verdict': '',    'group': 'Thep'       },
    'GAS': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'wf_verdict': 'YEU', 'group': 'Dau khi'    },
    'SZC': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'wf_verdict': 'YEU', 'group': 'KCN'        },
    'KDH': {'sl': 0.07, 'tp': 0.14, 'min_score': 80, 'wf_verdict': '',    'group': 'Bat dong san'},
    'PDR': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'wf_verdict': '',    'group': 'Bat dong san'},
    'NVL': {'sl': 0.07, 'tp': 0.14, 'min_score': 70, 'wf_verdict': '',    'group': 'Bat dong san'},
    'PVS': {'sl': 0.07, 'tp': 0.14, 'min_score': 70, 'wf_verdict': '',    'group': 'Dau khi'    },
    'PVD': {'sl': 0.07, 'tp': 0.14, 'min_score': 70, 'wf_verdict': '',    'group': 'Dau khi'    },
    'POW': {'sl': 0.07, 'tp': 0.14, 'min_score': 80, 'wf_verdict': '',    'group': 'Dien'       },
    'REE': {'sl': 0.07, 'tp': 0.14, 'min_score': 70, 'wf_verdict': '',    'group': 'Dien'       },
    'KBC': {'sl': 0.07, 'tp': 0.14, 'min_score': 70, 'wf_verdict': '',    'group': 'KCN'        },
}

# Default khi mã chưa có trong SYMBOL_CONFIG
DEFAULT_SL = 0.07
DEFAULT_TP = 0.14

# ═══════════════════════════════════════════════════════════════════════════════
# WATCHLISTS — Danh sách mã theo từng mục đích
# ═══════════════════════════════════════════════════════════════════════════════

# SIGNALS_WATCHLIST: 9 mã xuất hiện trong /signals và background cache
# Đây là mã đã qua backtest + walk-forward, đủ tin cậy để alert tự động
# Đọc từ env var WATCHLIST_SYMBOLS nếu muốn cập nhật không cần deploy
SIGNALS_WATCHLIST = [
    'DGC', 'DCM', 'MBB', 'HCM', 'PC1',   # 5 Tier 1 — WF xác nhận
    'FRT', 'VND', 'FPT', 'NKG',            # 4 mới — cần thêm WF
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


def get_min_score(symbol):
    """Lấy ngưỡng MUA tối ưu từ backtest cho symbol."""
    return SYMBOL_CONFIG.get(symbol.upper(), {}).get('min_score', MIN_SCORE_BUY)


def get_symbol_group(symbol):
    """Lấy nhóm ngành của symbol."""
    return SYMBOL_CONFIG.get(symbol.upper(), {}).get('group', 'Khac')


def get_wf_verdict(symbol):
    """Lấy kết quả walk-forward của symbol ('TOT'/'CHAP'/'YEU'/'')."""
    return SYMBOL_CONFIG.get(symbol.upper(), {}).get('wf_verdict', '')
