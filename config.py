"""
config.py — Single Source of Truth cho VN Trader Bot
=====================================================
Tất cả config quan trọng được định nghĩa MỘT LẦN ở đây.
app.py, backtest.py, telegram_bot.py đều import từ file này.

Cập nhật: 20/03/2026 — sau backtest đầy đủ 22 mã, ngưỡng đồng nhất 65/35
  SIGNALS_WATCHLIST: 13 mã (7 Tier1-TOT + 6 Tier2-CHAP)
  Xóa: FPT, SZC, PC1, KDH, GAS, PVS, POW (PF<1 hoặc WF YEU)
  Theo dõi: VIC, KBC (WF tốt nhưng PF<1 — chưa đủ điều kiện)

Cách thêm mã mới:
  1. Thêm vào SYMBOL_CONFIG với sl, tp
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
    # Ngưỡng MUA/BAN: MIN_SCORE_BUY=65 / MAX_SCORE_SELL=35 — ĐỒNG NHẤT tất cả mã
    # wf_verdict: TOT/CHAP/YEU/THEO_DOI/'' (chỉ tham khảo, không ảnh hưởng logic)
    # Cập nhật: 20/03/2026 từ backtest 7 năm + walk-forward 22 mã
    # ═══════════════════════════════════════════════════════════════════════════

    # ── TIER 1 — WF=TOT, trade tự động (SL/TP theo nhóm) ─────────────────────
    # Hoá chất — SL=7% TP=14%
    'DGC': {'sl': 0.07, 'tp': 0.14, 'group': 'Hoa chat',     'wf_verdict': 'TOT'      },
    # WR=55.6% PF=2.45 OOS=56.4% decay=-1.6% | B-filter: TẮT | Entry: T
    'DCM': {'sl': 0.07, 'tp': 0.14, 'group': 'Hoa chat',     'wf_verdict': 'TOT'      },
    # WR=55.4% PF=1.99 OOS=59.5% decay=-12% | B-filter: BẬT | Entry: T+1

    # Thép — SL=7% TP=14%
    'NKG': {'sl': 0.07, 'tp': 0.14, 'group': 'Thep',         'wf_verdict': 'TOT'      },
    # WR=58.1% PF=1.66 OOS=65.0% decay=-1.9% | B-filter: TẮT | Entry: T

    # Ngân hàng tư nhân — SL=5% TP=9%
    'MBB': {'sl': 0.05, 'tp': 0.09, 'group': 'Ngan hang',    'wf_verdict': 'TOT'      },
    # WR=56.4% PF=1.50 OOS=58.9% decay=-1.7% | Entry: T BẮT BUỘC (T+1 kém -10.9%)

    # Chứng khoán — SL=5% TP=9%
    'SSI': {'sl': 0.05, 'tp': 0.09, 'group': 'Chung khoan',  'wf_verdict': 'TOT'      },
    # WR=58.3% PF=1.82 OOS=61.1% decay=+1.8% | OOS PnL≈IS PnL — reproducibility cao nhất

    # Bán lẻ — SL=7% TP=14%
    'FRT': {'sl': 0.07, 'tp': 0.14, 'group': 'Ban le',       'wf_verdict': 'TOT'      },
    # WR=51.9% PF=1.35 OOS=55.7% decay=-5.9% | B-filter: bật nhẹ

    # Chứng khoán — SL=5% TP=9%
    'VND': {'sl': 0.05, 'tp': 0.09, 'group': 'Chung khoan',  'wf_verdict': 'TOT'      },
    # WR=52.2% PF=1.28 OOS=61.3% decay=-1.3% | OOS WR cao nhì watchlist

    # ── TIER 2 — WF=CHAP, trade thận trọng ───────────────────────────────────
    # Thép — SL=7% TP=14%
    'HSG': {'sl': 0.07, 'tp': 0.14, 'group': 'Thep',         'wf_verdict': 'CHAP'     },
    # WR=57.1% PF=1.91 OOS=52.8% decay=+14.4% | ⚠ Decay cao, giám sát chặt

    # Bất động sản — SL=7% TP=14%
    'PDR': {'sl': 0.07, 'tp': 0.14, 'group': 'Bat dong san', 'wf_verdict': 'CHAP'     },
    # WR=46.9% PF=1.43 OOS=51.3% | ⚠ Entry T BẮT BUỘC (T+1 kém -14.2%)
    'NVL': {'sl': 0.07, 'tp': 0.14, 'group': 'Bat dong san', 'wf_verdict': 'CHAP'     },
    # WR=44.1% PF=1.27 OOS=50.7% | Entry T+1 tốt hơn T về PnL

    # Chứng khoán — SL=5% TP=9%
    'HCM': {'sl': 0.05, 'tp': 0.09, 'group': 'Chung khoan',  'wf_verdict': 'CHAP'     },
    # WR=51.9% PF=1.45 OOS=50.2% decay=-0.8% | PF cao hơn VND

    # Ngân hàng quốc doanh — SL=5% TP=9%
    'BID': {'sl': 0.05, 'tp': 0.09, 'group': 'Ngan hang',    'wf_verdict': 'CHAP'     },
    # WR=50.0% PF=1.29 OOS=50.0% decay=-1.8% | @65 biến PF từ <1 thành >1

    # Điện — SL=5% TP=9%
    'NT2': {'sl': 0.05, 'tp': 0.09, 'group': 'Dien',         'wf_verdict': 'CHAP'     },
    # WR=50.9% PF=1.15 OOS=50.8% | ⚠ B-filter THIẾT YẾU (+0.63% PnL) | HK=57%

    # ── THEO DÕI — WF tốt nhưng PF<1, chưa đủ điều kiện SIGNALS ─────────────
    'VIC': {'sl': 0.07, 'tp': 0.14, 'group': 'Bat dong san', 'wf_verdict': 'THEO_DOI' },
    # PF=2.24 PnL=+3.19% nhưng WF YEU (OOS=36%) — 3 TP liên tiếp 2025, theo dõi thêm
    'KBC': {'sl': 0.07, 'tp': 0.14, 'group': 'KCN',          'wf_verdict': 'THEO_DOI' },
    # WF TOT OOS=58.3% decay=-17.4% nhưng PF=0.84<1 — KCN China+1, theo dõi thêm

    # ── ĐÃ XÓA KHỎI SIGNALS — Giữ lại để backtest nghiên cứu ────────────────
    'FPT': {'sl': 0.05, 'tp': 0.09, 'group': 'Cong nghe',    'wf_verdict': 'YEU'      },
    # PF=0.78<1 — GROWTH stock, TP=9% quá nhỏ, OOS=46.7%
    'SZC': {'sl': 0.07, 'tp': 0.14, 'group': 'KCN',          'wf_verdict': 'YEU'      },
    # PF=0.94<1 — Thanh khoản thấp
    'PC1': {'sl': 0.07, 'tp': 0.14, 'group': 'Dien',         'wf_verdict': 'YEU'      },
    # PF=1.13, OOS=45.5% — Điện/hạ tầng đặc thù lãi suất
    'KDH': {'sl': 0.07, 'tp': 0.14, 'group': 'Bat dong san', 'wf_verdict': 'YEU'      },
    # PF=0.79<1 WR=36.7% — Tệ nhất watchlist
    'GAS': {'sl': 0.07, 'tp': 0.14, 'group': 'Dau khi',      'wf_verdict': 'YEU'      },
    # PF=0.72<1 OOS=38.9% — Quốc doanh độc quyền, FA là chính
    'PVS': {'sl': 0.07, 'tp': 0.14, 'group': 'Dau khi',      'wf_verdict': 'YEU'      },
    # PF=0.68<1 — Thấp nhất toàn watchlist
    'POW': {'sl': 0.07, 'tp': 0.14, 'group': 'Dien',         'wf_verdict': 'YEU'      },
    # PF=0.77<1 HK=69% — SL/TP không phù hợp biến động chậm của điện gió

    # ── CHƯA BACKTEST — Giữ để nghiên cứu sau ────────────────────────────────
    'HPG': {'sl': 0.07, 'tp': 0.14, 'group': 'Thep',         'wf_verdict': ''         },
    'TCB': {'sl': 0.05, 'tp': 0.09, 'group': 'Ngan hang',    'wf_verdict': ''         },
    'VPB': {'sl': 0.05, 'tp': 0.09, 'group': 'Ngan hang',    'wf_verdict': ''         },
    'VCB': {'sl': 0.05, 'tp': 0.09, 'group': 'Ngan hang',    'wf_verdict': 'YEU'      },
    'MWG': {'sl': 0.05, 'tp': 0.09, 'group': 'Ban le',       'wf_verdict': ''         },
    'CMG': {'sl': 0.05, 'tp': 0.09, 'group': 'Cong nghe',    'wf_verdict': ''         },
    'PVD': {'sl': 0.07, 'tp': 0.14, 'group': 'Dau khi',      'wf_verdict': ''         },
    'REE': {'sl': 0.07, 'tp': 0.14, 'group': 'Dien',         'wf_verdict': ''         },
}

# Default khi mã chưa có trong SYMBOL_CONFIG
DEFAULT_SL = 0.07
DEFAULT_TP = 0.14

# ═══════════════════════════════════════════════════════════════════════════════
# WATCHLISTS — Danh sách mã theo từng mục đích
# ═══════════════════════════════════════════════════════════════════════════════

# SIGNALS_WATCHLIST: 13 mã xuất hiện trong /signals và background cache alert
# Tiêu chí: PF>1.1 và OOS WR>50% — đã xác nhận bởi backtest 7 năm + walk-forward
# Cập nhật: 20/03/2026
#
# TIER 1 — WF=TOT (7 mã): trade tự động, độ tin cậy cao
#   DGC  WR=55.6% PF=2.45 OOS=56.4% — Hóa chất, mạnh nhất
#   DCM  WR=55.4% PF=1.99 OOS=59.5% — Hóa chất, ổn định nhất (decay -12%)
#   NKG  WR=58.1% PF=1.66 OOS=65.0% — Thép, OOS WR cao nhất
#   MBB  WR=56.4% PF=1.50 OOS=58.9% — Ngân hàng, vào T bắt buộc
#   SSI  WR=58.3% PF=1.82 OOS=61.1% — CK, reproducibility cao nhất
#   FRT  WR=51.9% PF=1.35 OOS=55.7% — Bán lẻ
#   VND  WR=52.2% PF=1.28 OOS=61.3% — CK, OOS WR cao nhì
#
# TIER 2 — WF=CHAP (6 mã): trade thận trọng, vị thế nhỏ hơn
#   HSG  WR=57.1% PF=1.91 OOS=52.8% — Thép, ⚠ decay +14.4%
#   PDR  WR=46.9% PF=1.43 OOS=51.3% — BĐS, ⚠ vào T bắt buộc
#   HCM  WR=51.9% PF=1.45 OOS=50.2% — CK
#   BID  WR=50.0% PF=1.29 OOS=50.0% — NH quốc doanh
#   NVL  WR=44.1% PF=1.27 OOS=50.7% — BĐS, ⚠ vào T+1 tốt hơn
#   NT2  WR=50.9% PF=1.15 OOS=50.8% — Điện, ⚠ cần B-filter bật
SIGNALS_WATCHLIST = [
    # ── Tier 1: WF=TOT — trade tự động ──────────────────────────────────────
    'DGC',   # Hóa chất | PF=2.45 | OOS=56.4% | B-filter: TẮT | Entry: T
    'DCM',   # Hóa chất | PF=1.99 | OOS=59.5% | B-filter: BẬT | Entry: T+1
    'NKG',   # Thép     | PF=1.66 | OOS=65.0% | B-filter: TẮT | Entry: T
    'MBB',   # NH tư    | PF=1.50 | OOS=58.9% | Entry: T BẮT BUỘC
    'SSI',   # CK       | PF=1.82 | OOS=61.1% | Entry: T
    'FRT',   # Bán lẻ   | PF=1.35 | OOS=55.7% | B-filter: bật nhẹ
    'VND',   # CK       | PF=1.28 | OOS=61.3% | Entry: T/T+1 đều ok
    # ── Tier 2: WF=CHAP — trade thận trọng, vị thế nhỏ ─────────────────────
    'HSG',   # Thép     | PF=1.91 | OOS=52.8% | ⚠ Decay +14.4%, giám sát
    'PDR',   # BĐS      | PF=1.43 | OOS=51.3% | ⚠ Entry T BẮT BUỘC (-14.2% nếu T+1)
    'HCM',   # CK       | PF=1.45 | OOS=50.2% | Entry: T
    'BID',   # NH QD    | PF=1.29 | OOS=50.0% | Entry: T
    'NVL',   # BĐS      | PF=1.27 | OOS=50.7% | ⚠ Entry T+1 tốt hơn về PnL
    'NT2',   # Điện     | PF=1.15 | OOS=50.8% | ⚠ B-filter PHẢI BẬT
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
