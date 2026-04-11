"""
config.py — Single Source of Truth cho VN Trader Bot
=====================================================
Tất cả config quan trọng được định nghĩa MỘT LẦN ở đây.
app.py, backtest.py, telegram_bot.py đều import từ file này.

Cập nhật: 11/04/2026 — Session 4: Score A /bt validation 20 mã
  SIGNALS_WATCHLIST thay đổi:
    Thêm (sascreen WF=V + /bt Robust): HAH, PC1, STB, MCH, CTS
    Loại (WF yếu hoặc PF<1 qua /bt): GEX, BSR, BID, VPB, POW
  SIGNALS_MANUAL thay đổi:
    Loại toàn bộ (WF yếu qua /bt): VND, VIC, PDR → chuyển sang THEO DÕI
  SYMBOL_CONFIG:
    PC1: wf_verdict TOT (update từ YEU)
    STB, MCH, CTS: thêm mới với params chuẩn Score A
    GEX, BSR, VPB, POW: wf_verdict YEU (update)
    BID: wf_verdict YEU, PF<1 qua /bt
    VND, VIC, PDR: wf_verdict YEU (update từ /bt)
"""

# ═══════════════════════════════════════════════════════════════════════════════
# TRADING RULES — Quy tắc giao dịch cốt lõi
# ═══════════════════════════════════════════════════════════════════════════════

SETTLEMENT_DAYS  = 2
HOLD_DAYS        = 10
STOP_LOSS        = -0.07
TAKE_PROFIT      =  0.14
MIN_SCORE_BUY    = 65
MAX_SCORE_SELL   = 35
LOOKBACK_DAYS    = 2555
COMMISSION       = 0.005

# ═══════════════════════════════════════════════════════════════════════════════
# SYMBOL_CONFIG — Cấu hình SL/TP/score theo từng mã
# ═══════════════════════════════════════════════════════════════════════════════
SYMBOL_CONFIG = {
    # ── TIER 1 — Score A AUTO TRADE (6 mã) ───────────────────────────────────
    'DGC': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'b_filter': False, 'group': 'Hoa chat',     'wf_verdict': 'TOT'  , 'shark_mode': 'none', 'use_regime': False, 'trigger_mode': 'score_primary'},
    'DCM': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'b_filter': True,  'group': 'Hoa chat',     'wf_verdict': 'TOT'  , 'shark_mode': 'S', 'shark_min': 65, 'use_regime': False, 'trigger_mode': 'score_primary', 'min_conviction': 3.0},
    'SSI': {'sl': 0.05, 'tp': 0.09, 'min_score': 65, 'b_filter': False, 'group': 'Chung khoan',  'wf_verdict': 'TOT'  , 'shark_mode': 'none', 'use_regime': True, 'trigger_mode': 'score_primary'},
    'NKG': {'sl': 0.07, 'tp': 0.10, 'min_score': 65, 'b_filter': False, 'group': 'Thep',         'wf_verdict': 'TOT',  'entry': 'T', 'entry_note': 'T bat buoc (-4.6% WR neu T+1)', 'shark_mode': 'none', 'use_regime': False, 'trigger_mode': 'score_primary', 'min_conviction': 3.0},
    'MBB': {'sl': 0.05, 'tp': 0.07, 'min_score': 55, 'b_filter': False, 'group': 'Ngan hang',    'wf_verdict': 'CHAP_NHAN',  'entry': 'T', 'entry_note': 'T bat buoc (-10.9% WR neu T+1)', 'shark_mode': 'S', 'shark_min': 55, 'use_regime': False, 'trigger_mode': 'filter_led'},
    'HCM': {'sl': 0.06, 'tp': 0.10, 'min_score': 65, 'b_filter': False, 'group': 'Chung khoan',  'wf_verdict': 'TOT' , 'shark_mode': 'S', 'shark_min': 55, 'use_regime': True, 'entry': 'T+1', 'entry_note': 'T+1 tot hon T (+0.43% PnL khi co regime)', 'use_vwap': False, 'trigger_mode': 'score_primary'},

    # ── TIER 2 — Score A AUTO TRADE CẨN THẬN ─────────────────────────────────
    'HSG': {'sl': 0.07, 'tp': 0.10, 'min_score': 65, 'b_filter': False, 'group': 'Thep',         'wf_verdict': 'TOT'        , 'shark_mode': 'none', 'use_regime': False, 'use_vwap': False, 'trigger_mode': 'filter_led'},
    'FRT': {'sl': 0.07, 'tp': 0.10, 'min_score': 65, 'b_filter': True,  'group': 'Ban le',       'wf_verdict': 'TOT'  , 'shark_mode': 'AS', 'shark_min': 50, 'shark_warn': 60, 'use_regime': False, 'use_vwap': False, 'trigger_mode': 'score_primary', 'min_conviction': 3.0},

    # ── MÃ MỚI — Score A, thêm từ sascreen session 4 ─────────────────────────
    'HAH': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'b_filter': False, 'group': 'Logistics',    'wf_verdict': 'TOT'  , 'shark_mode': 'none', 'use_regime': False, 'trigger_mode': 'score_primary'},
    # WR=54.3% PF=1.50 OOS WR=75% decay=-20.7% | /bt Robust | sascreen WF=V PF=2.09
    'PC1': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'b_filter': False, 'group': 'Dien',         'wf_verdict': 'TOT'  , 'shark_mode': 'none', 'use_regime': False, 'trigger_mode': 'score_primary'},
    # WR=53.5% PF=1.48 OOS WR=83% decay=-34.5% | /bt Robust | sascreen WF=V PF=1.51 (update từ YEU)
    'STB': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'b_filter': False, 'group': 'Ngan hang',    'wf_verdict': 'TOT'  , 'shark_mode': 'none', 'use_regime': False, 'trigger_mode': 'score_primary'},
    # WR=57.8% PF=1.49 OOS WR=83% decay=-34.6% | /bt Robust | sascreen WF=V PF=1.56
    'MCH': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'b_filter': False, 'group': 'FMCG',         'wf_verdict': 'TOT'  , 'shark_mode': 'none', 'use_regime': False, 'trigger_mode': 'score_primary'},
    # WR=47.1% PF=1.51 OOS WR=75% decay=-25.0% | /bt Robust | sascreen WF=V PF=1.61
    'CTS': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'b_filter': False, 'group': 'Chung khoan',  'wf_verdict': 'TOT'  , 'shark_mode': 'none', 'use_regime': False, 'trigger_mode': 'score_primary'},
    # WR=54.3% PF=1.42 OOS WR=75% decay=-24.8% | /bt Robust | sascreen WF=V PF=1.77

    # ── LOẠI KHỎI SIGNALS — WF yếu qua /bt, giữ để nghiên cứu ───────────────
    'GEX': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'b_filter': False, 'group': 'Dien co',      'wf_verdict': 'YEU'  , 'shark_mode': 'none', 'use_regime': False, 'trigger_mode': 'score_primary'},
    # /bt: OOS WR=25% decay=+22.4% — loại khỏi SIGNALS_WATCHLIST session 4
    'BSR': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'b_filter': False, 'group': 'Loc hoa dau',  'wf_verdict': 'YEU'  , 'shark_mode': 'none', 'use_regime': False, 'trigger_mode': 'score_primary'},
    # /bt: OOS WR=0% decay=+50% — loại khỏi SIGNALS_WATCHLIST session 4
    'BID': {'sl': 0.05, 'tp': 0.07, 'min_score': 55, 'b_filter': False, 'group': 'Ngan hang',    'wf_verdict': 'YEU'  , 'shark_mode': 'AS', 'shark_min': 50, 'use_regime': False, 'trigger_mode': 'filter_confirm', 'min_conviction': 2.5},
    # /bt: PF=0.98 OOS WR=44% — loại khỏi SIGNALS_WATCHLIST session 4
    'VPB': {'sl': 0.05, 'tp': 0.09, 'min_score': 55, 'b_filter': False, 'group': 'Ngan hang',    'wf_verdict': 'YEU'  , 'shark_mode': 'S', 'shark_min': 65, 'use_regime': False, 'trigger_mode': 'filter_confirm'},
    # /bt: OOS WR=13% decay=+33.7% — loại khỏi SIGNALS_WATCHLIST session 4
    'POW': {'sl': 0.07, 'tp': 0.14, 'min_score': 45, 'group': 'Dien',   'wf_verdict': 'YEU'      , 'shark_mode': 'S', 'shark_min': 60, 'use_regime': True, 'trigger_mode': 'filter_led', 'min_conviction': 2.5},
    # /bt: PF=1.09 WF yếu — loại khỏi SIGNALS_WATCHLIST session 4

    # ── SIGNALS_MANUAL cũ — WF yếu qua /bt, chuyển sang THEO DÕI ─────────────
    'VND': {'sl': 0.05, 'tp': 0.09, 'min_score': 65, 'b_filter': False, 'group': 'Chung khoan',  'wf_verdict': 'YEU'  , 'shark_mode': 'none', 'use_regime': False, 'use_vwap': False, 'trigger_mode': 'filter_led'},
    # /bt: OOS WR=33% decay=+16% — loại khỏi SIGNALS_MANUAL session 4
    'VCI': {'sl': 0.05, 'tp': 0.09, 'min_score': 65, 'b_filter': False, 'group': 'Chung khoan',  'wf_verdict': 'CHUA_BT'    , 'shark_mode': 'none', 'use_regime': False, 'use_vwap': False, 'trigger_mode': 'score_primary'},
    'NT2': {'sl': 0.03, 'tp': 0.06, 'min_score': 80, 'b_filter': False, 'group': 'Dien',         'wf_verdict': 'CHAP' , 'shark_mode': 'AS', 'shark_min': 55, 'shark_warn': 60},
    'PDR': {'sl': 0.07, 'tp': 0.14, 'min_score': 55, 'b_filter': False, 'group': 'Bat dong san', 'wf_verdict': 'YEU'  , 'entry': 'T', 'entry_note': 'T bat buoc (-14.2% WR neu T+1)', 'shark_mode': 'dangerous', 'use_regime': False, 'trigger_mode': 'filter_led', 'min_conviction': 2.5},
    # /bt: OOS WR=33% decay=+16.7% — loại khỏi SIGNALS_MANUAL session 4
    'NVL': {'sl': 0.07, 'tp': 0.14, 'min_score': 55, 'b_filter': False, 'group': 'Bat dong san', 'wf_verdict': 'YEU'  , 'entry': 'T+1', 'entry_note': 'T+1 tot hon T (+0.53% PnL)', 'shark_mode': 'qualitative', 'use_regime': False, 'use_vwap': False, 'trigger_mode': 'filter_led'},
    # /bt: OOS WR=39% decay=+13.9% — loại khỏi SIGNALS_MANUAL session 4
    'VIC': {'sl': 0.07, 'tp': 0.14, 'min_score': 45, 'group': 'Bat dong san', 'wf_verdict': 'YEU', 'shark_mode': 'none', 'use_regime': True, 'trigger_mode': 'filter_led'},
    # /bt: OOS WR=19% decay=+17% — loại khỏi SIGNALS_MANUAL session 4

    # ── THEO DÕI — WF tốt nhưng chưa đủ điều kiện SIGNALS ───────────────────
    'KBC': {'sl': 0.07, 'tp': 0.14, 'min_score': 65, 'group': 'KCN',          'wf_verdict': 'TOT' , 'shark_mode': 'S', 'shark_min': 55, 'shark_warn': 65, 'shark_min_hard': 50, 'use_regime': False, 'use_vwap': False, 'trigger_mode': 'score_primary', 'min_conviction': 3.0},

    # ── ĐÃ XÓA KHỎI SIGNALS — Giữ lại để backtest nghiên cứu ────────────────
    'FPT': {'sl': 0.05, 'tp': 0.09, 'min_score': 65, 'group': 'Cong nghe',    'wf_verdict': 'CHAP_NHAN', 'shark_mode': 'weak', 'shark_min': 55, 'use_regime': True, 'use_vwap': False, 'trigger_mode': 'score_primary', 'min_conviction': 2.5},
    'SZC': {'sl': 0.07, 'tp': 0.14, 'min_score': 45, 'group': 'KCN',          'wf_verdict': 'YEU'      , 'shark_mode': 'S', 'shark_min': 60, 'use_regime': False, 'use_vwap': False, 'trigger_mode': 'filter_led', 'min_conviction': 3.0},
    'KDH': {'sl': 0.07, 'tp': 0.14, 'min_score': 45, 'group': 'Bat dong san', 'wf_verdict': 'YEU'      , 'shark_mode': 'S', 'shark_min': 60, 'shark_min_hard': 50, 'use_regime': False, 'entry': 'T+1', 'entry_note': 'T+1 tot hon T (+6.9% WR)', 'trigger_mode': 'filter_led'},
    'GAS': {'sl': 0.07, 'tp': 0.14, 'min_score': 45, 'group': 'Dau khi',      'wf_verdict': 'YEU'      , 'shark_mode': 'S', 'shark_min': 55, 'shark_warn': 65, 'shark_min_hard': 50, 'use_regime': True, 'trigger_mode': 'filter_led', 'min_conviction': 3.0},
    'PVS': {'sl': 0.07, 'tp': 0.14, 'min_score': 45, 'group': 'Dau khi',      'wf_verdict': 'YEU'      , 'shark_mode': 'S', 'shark_min': 55, 'use_regime': True, 'entry': 'T+1', 'entry_note': 'T+1 tot hon T (+6.5% WR, +1.43% PnL)', 'trigger_mode': 'filter_led', 'min_conviction': 3.0},

    # ── CHƯA BACKTEST — Giữ để nghiên cứu sau ────────────────────────────────
    'HPG': {'sl': 0.07, 'tp': 0.14, 'min_score': 55, 'group': 'Thep',         'wf_verdict': 'YEU'         , 'shark_mode': 'none', 'use_regime': False, 'use_vwap': False, 'trigger_mode': 'filter_confirm'},
    'TCB': {'sl': 0.05, 'tp': 0.09, 'min_score': 55, 'group': 'Ngan hang',    'wf_verdict': 'YEU'         , 'shark_mode': 'S', 'shark_min': 60, 'use_regime': False, 'entry': 'T+1', 'entry_note': 'T+1 tot hon T (+12.5% WR)', 'trigger_mode': 'filter_led'},
    'VCB': {'sl': 0.05, 'tp': 0.09, 'min_score': 55, 'group': 'Ngan hang',    'wf_verdict': 'YEU'      , 'shark_mode': 'AS', 'shark_min': 50, 'shark_warn': 65, 'use_regime': False, 'trigger_mode': 'filter_confirm', 'min_conviction': 3.0},
    'MWG': {'sl': 0.05, 'tp': 0.09, 'min_score': 65, 'group': 'Ban le',       'wf_verdict': 'CHAP_NHAN'         , 'shark_mode': 'weak', 'shark_min': 60, 'use_regime': True, 'trigger_mode': 'score_primary'},
    'CMG': {'sl': 0.05, 'tp': 0.09, 'min_score': 55, 'group': 'Cong nghe',    'wf_verdict': 'YEU'         , 'shark_mode': 'AS', 'shark_min': 60, 'use_regime': True, 'entry': 'T+1', 'entry_note': 'T+1 tot hon T (+3.8% WR)', 'trigger_mode': 'filter_led'},
    'PVD': {'sl': 0.07, 'tp': 0.14, 'min_score': 45, 'group': 'Dau khi',      'wf_verdict': 'YEU'         , 'shark_mode': 'S', 'shark_min': 55, 'use_regime': True, 'trigger_mode': 'filter_led', 'min_conviction': 2.5},
    'REE': {'sl': 0.07, 'tp': 0.14, 'min_score': 45, 'group': 'Dien',         'wf_verdict': 'YEU'         , 'shark_mode': 'S', 'shark_min': 65, 'shark_min_hard': 50, 'use_regime': False, 'entry': 'T+1', 'entry_note': 'T+1 dao nguoc WR+8.5% PnL+1.35%', 'trigger_mode': 'filter_led'},
    # DGC đã có trong score A watchlist nhưng WF yếu qua /bt (OOS=33%) — giữ theo dõi
    'DGC_NOTE': {},  # placeholder — DGC vẫn trong SIGNALS_WATCHLIST nhưng cần monitor WF
}

# Xóa placeholder không cần thiết
del SYMBOL_CONFIG['DGC_NOTE']

DEFAULT_SL = 0.07
DEFAULT_TP = 0.14

# ═══════════════════════════════════════════════════════════════════════════════
# WATCHLISTS
# ═══════════════════════════════════════════════════════════════════════════════

# ── SIGNALS_WATCHLIST — Score A only ─────────────────────────────────────────
# Dùng cho /signals và /sascreen. KHÔNG liên quan ML.
# Cập nhật session 4 (11/04/2026):
#   Thêm: HAH, PC1, STB, MCH, CTS (sascreen WF=V + /bt Robust)
#   Loại: GEX (OOS=25%), BSR (OOS=0%), BID (PF<1), VPB (OOS=13%), POW (WF yếu)
#
# Tier 1 — /bt Robust, PF>=1.4, OOS WR>=50%
#   DGC  WR=53.6% PF=1.52 OOS=33% — giữ, theo dõi WF (IS PF tốt)
#   SSI  WR=46.2% PF=1.37 OOS=50%
#   HCM  WR=47.7% PF=1.25 OOS=50% — giữ, theo dõi
#   NKG  WR=59.5% PF=1.83 OOS=50%
#   FRT  WR=52.3% PF=1.30 OOS=54%
# Tier 2 — mới từ sascreen session 4, /bt Robust
#   HAH  WR=54.3% PF=1.50 OOS=75% decay=-20.7%
#   PC1  WR=53.5% PF=1.48 OOS=83% decay=-34.5%
#   STB  WR=57.8% PF=1.49 OOS=83% decay=-34.6%
#   MCH  WR=47.1% PF=1.51 OOS=75% decay=-25.0%
#   CTS  WR=54.3% PF=1.42 OOS=75% decay=-24.8%
SIGNALS_WATCHLIST = [
    # ── Tier 1 (5 mã, watchlist cũ còn lại) ──────────────────────────────────
    'DGC',   # Hóa chất      | PF=1.52 | WR=53.6% | theo dõi WF OOS
    'SSI',   # Chứng khoán   | PF=1.37 | WR=46.2% | OOS=50%
    'HCM',   # Chứng khoán   | PF=1.25 | WR=47.7% | OOS=50% theo dõi
    'NKG',   # Thép          | PF=1.83 | WR=59.5% | OOS=50%
    'FRT',   # Bán lẻ        | PF=1.30 | WR=52.3% | OOS=54%
    # ── Tier 2 (5 mã mới từ sascreen session 4) ───────────────────────────────
    'HAH',   # Logistics     | PF=1.50 | WR=54.3% | OOS=75% decay=-20.7%
    'PC1',   # Điện/XD       | PF=1.48 | WR=53.5% | OOS=83% decay=-34.5%
    'STB',   # Ngân hàng     | PF=1.49 | WR=57.8% | OOS=83% decay=-34.6%
    'MCH',   # FMCG          | PF=1.51 | WR=47.1% | OOS=75% decay=-25.0%
    'CTS',   # Chứng khoán   | PF=1.42 | WR=54.3% | OOS=75% decay=-24.8%
]

# ── SIGNALS_MANUAL — đã xóa toàn bộ (VND/VIC/PDR WF yếu qua /bt) ────────────
# Giữ list rỗng để không break code cũ import SIGNALS_MANUAL
SIGNALS_MANUAL = []

# ── BACKTEST_WATCHLIST — ML watchlist, dùng cho /mlscan và /mlbt all ─────────
# KHÔNG liên quan Score A. Chỉ chứa mã đã pass ML walk-forward validation.
# Cập nhật: đợt 10-11 (08/04/2026) — không thay đổi trong session 4
BACKTEST_WATCHLIST = [
    # ── ML Confirmed Tier A (full size) ────────────────────────────────────────
    'HCM', 'FRT', 'VCI', 'LPB',
    # ── ML Confirmed Tier B (size 70-80%) ──────────────────────────────────────
    'DGC', 'NKG', 'SSI', 'MWG', 'VIX', 'BSI', 'ORS', 'HDB',
    # ── Theo dõi live ──────────────────────────────────────────────────────────
    'POW', 'OCB', 'MBS', 'VDS', 'SHS',
    # ── Manual theo dõi ────────────────────────────────────────────────────────
    'VND', 'VIC', 'PDR',
]

# ── ML_CONFIRMED_WATCHLIST — auto alert ML, KHÔNG thay đổi session 4 ─────────
ML_CONFIRMED_WATCHLIST = {
    # sym   : (tier, sl_pct, tp_pct, hold_days, note)
    'HCM': ('A', 5, 15, 18, 'WR=68% PF=2.96 OOS=53%'),
    'FRT': ('A', 5, 15, 18, 'WR=62% PF=2.06 OOS=53%'),
    'VCI': ('A', 5, 15, 18, 'WR=60% PF=3.33 OOS=80% — n_OOS nho'),
    'LPB': ('A', 6, 17, 18, 'WR=64% PF=2.62 OOS=67% — dot 3, WF Robust xuat sac'),
    'DGC': ('B', 5, 15, 18, 'WR=52% PF=2.07 OOS=72%'),
    'NKG': ('B', 5, 15, 18, 'WR=52% PF=2.30 OOS=60%'),
    'SSI': ('B', 5, 15, 18, 'WR=52% PF=2.33 OOS=58%'),
    'MWG': ('B', 5, 15, 18, 'WR=55% PF=1.42 WF Robust decay=0.6% — theo doi live 2025-2026'),
    'VIX': ('B', 6, 17, 18, 'WR=51% PF=2.10 OOS=52% — dot 1'),
    'BSI': ('B', 6, 17, 18, 'WR=50% PF=2.09 OOS=52% — dot 1'),
    'ORS': ('B', 6, 17, 18, 'WR=52% PF=1.53 OOS=61% — dot 2, WF Robust'),
    'HDB': ('B', 6, 17, 18, 'WR=65%* PF=1.73* OOS=66% — dot 4, CHI STRONG grade'),
    'SZC': ('B', 7, 14, 18, 'OOS=64% PF=1.49 WF=V n=15 — PAPER TRADE, CHI STRONG grade'),
}


def get_sl_tp(symbol):
    cfg = SYMBOL_CONFIG.get(symbol.upper(), {})
    return cfg.get('sl', DEFAULT_SL), cfg.get('tp', DEFAULT_TP)


def get_sl_tp_pct(symbol):
    sl, tp = get_sl_tp(symbol)
    return int(sl * 100), int(tp * 100)


def get_min_score(symbol=None):
    if symbol:
        cfg = SYMBOL_CONFIG.get(str(symbol).upper(), {})
        return cfg.get('min_score', MIN_SCORE_BUY)
    return MIN_SCORE_BUY


def get_b_filter(symbol):
    cfg = SYMBOL_CONFIG.get(symbol.upper(), {})
    return cfg.get('b_filter', False)


def get_symbol_group(symbol):
    return SYMBOL_CONFIG.get(symbol.upper(), {}).get('group', 'Khac')


def get_wf_verdict(symbol):
    return SYMBOL_CONFIG.get(symbol.upper(), {}).get('wf_verdict', '')


# ═══════════════════════════════════════════════════════════════════════════
# SHARK CONFIG HELPER
# ═══════════════════════════════════════════════════════════════════════════
_SHARK_PNL_DATA = {
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
    'MBB': (1.09, 'Shark có ích ĐỘC LẬP',                   'P&L backtest thấp (+1.09%) — tín hiệu vừa'),
    'VPB': (1.04, 'Shark có ích ĐỘC LẬP',                   'P&L backtest thấp (+1.04%) — tín hiệu vừa'),
    'SZC': (0.88, 'Shark không đủ tin cậy (P&L thấp)',      'P&L +0.88% dưới ngưỡng 1.2%'),
    'HCM': (0.50, 'Shark không đủ tin cậy (P&L thấp)',      'Chỉ dùng Shark 55-59, tránh >= 60 (P&L âm)'),
    'KBC': (0.69, 'Shark không đủ tin cậy (P&L thấp)',      'P&L +0.69% dưới ngưỡng 1.2%'),
    'GAS': (0.44, 'Shark không đủ tin cậy (P&L thấp)',      'P&L +0.44% dưới ngưỡng | Tránh >= 65 (cliff)'),
    'REE': (0.49, 'Shark không đủ tin cậy (P&L thấp)',      'P&L +0.49% dưới ngưỡng 1.2%'),
    'FPT': (0.70, 'Shark yếu — bonus nhẹ không bắt buộc',  ''),
    'NT2': (0.34, 'Shark yếu — bonus nhẹ không bắt buộc',  ''),
    'MWG': (0.61, 'Shark yếu — bonus nhẹ không bắt buộc',  ''),
}


def get_shark_config(symbol: str) -> dict:
    cfg   = SYMBOL_CONFIG.get(symbol.upper(), {})
    mode  = cfg.get('shark_mode')
    mn    = cfg.get('shark_min',  0)
    warn  = cfg.get('shark_warn', 0)
    hard  = cfg.get('shark_min_hard', 0)

    if mode == 'dangerous':
        return {'mode': 'dangerous', 'min_score': 0, 'warn_score': 0, 'shark_min_hard': 0,
                'pnl_ok': False, 'pnl_val': None,
                'verdict': '⛔ Shark PHẢN TÁC DỤNG — điểm càng cao càng nguy hiểm, KHÔNG mua', 'note': ''}
    if mode == 'none':
        return {'mode': 'none', 'min_score': 0, 'warn_score': 0, 'shark_min_hard': 0,
                'pnl_ok': False, 'pnl_val': None,
                'verdict': '❌ Shark không có ích — dùng Score A đơn thuần', 'note': ''}
    if mode == 'qualitative':
        return {'mode': 'qualitative', 'min_score': 0, 'warn_score': 0, 'shark_min_hard': 0,
                'pnl_ok': False, 'pnl_val': None,
                'verdict': '⚠ Không dùng Shark — theo dõi tin pháp lý', 'note': ''}

    sym_up  = symbol.upper()
    pdata   = _SHARK_PNL_DATA.get(sym_up)
    pnl_val = pdata[0] if pdata else None
    pnl_ok  = (pnl_val is not None and pnl_val >= 1.2)

    if pdata:
        verdict_line = pdata[1]
        extra_note   = pdata[2]
    else:
        verdict_line = 'Shark có ích' if mode in ('S', 'AS') else 'Shark yếu'
        extra_note   = 'Chưa có dữ liệu P&L backtest'

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

    if pnl_val is not None and not pnl_ok:
        verdict += f' ⚠ P&L backtest thấp ({pnl_val:+.2f}%)'

    return {'mode': mode, 'min_score': mn, 'warn_score': warn, 'shark_min_hard': hard,
            'pnl_ok': pnl_ok, 'pnl_val': pnl_val, 'verdict': verdict, 'note': extra_note}
