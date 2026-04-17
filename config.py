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

Cập nhật: 14/04/2026 — Session 9:
  SIGNALS_WATCHLIST: loại HCM (WR=42.2%, PF=1.15, CLIMAX_VOL âm, không bucket Exp>1%)
  HOLD_DAYS_OVERRIDE: STB/HAH/PC1 → 20d
  POSITION_SIZE_CAPS: NKG=50%, DGC=50%, MCH=70%
  SCORE_THRESHOLDS_PER_SYMBOL: SSI≥85, HAH≥75, DGC≥75

Cập nhật: 16/04/2026 — Session 16:
  SCB_WATCHLIST_TIER_B: 6 → 11 mã (thêm FRT, VTP, DCM, PHP, DXS)
  SCB_SCORE_A_MIN: thêm 5 mã mới (PHP=95, FRT/DXS=75, VTP/DCM=65)
  SCB_HARD_SKIP: thêm rules cho 5 mã mới; DCM bổ sung skip NEAR (S16 fix)
  SCB_WF_STATS: thêm 5 mã mới; PHP upgrade Prom→Robust (3/3 windows pass)
  SCB_BT_STATS: thêm 5 mã mới
  Duplicate SCB block đã xóa (bug S15)
  SCB_WATCHLIST tổng: 19 → 24 mã

Cập nhật: 16/04/2026 — Session 17:
  SCB_WATCHLIST_TIER_A: thêm HMC (Robust 3/3, ΔExp=+2.21%, PF=1.64)
                         demote CSV Robust→Prom (BT mới: WF xuống Prom)
                         demote CTG Robust→Prom (BT mới: WF xuống Prom)
  SCB_WATCHLIST_TIER_B: loại HAX (BT mới: WEAK verdict, ΔExp=+0.78%)
                         thêm CSV, CTG (demote từ Tier A)
  SCB_SCORE_A_MIN: thêm HMC=65 | loại HAX
  SCB_HARD_SKIP: thêm HMC rules; loại HAX
  SCB_WF_STATS: thêm HMC; update CSV/CTG wf→Prom; loại HAX
  SCB_BT_STATS: thêm HMC; loại HAX
  SCB_WATCHLIST tổng: 24 mã (không đổi — swap HAX ra, HMC vào)
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
    'VND': {'sl': 0.05, 'tp': 0.09, 'min_score': 65, 'b_filter': False, 'group': 'Chung khoan',  'wf_verdict': 'TOT'  , 'shark_mode': 'none', 'use_regime': False, 'use_vwap': False, 'trigger_mode': 'score_primary'},
    # Session 10: WR=53.2% PF=1.74 OOS=66.7% decay=-16.5% WF=Robust | SL=5% TP=9%
    # Bucket anomaly: 65-74 BEST(+3.03) | 75-84 WORST(-2.21) → skip 75-84 via SCORE_SKIP_BUCKETS
    # VNI DOWN Exp=+0.16 (9L) → skip | HIGH_VOL Exp=+2.14 → tích cực
    # Sector: Chứng khoán — cùng nhóm SSI, CTS → monitor correlation khi trade đồng thời
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
# Session 9 (14/04/2026): loại HCM, update số liệu v2
# Session 10 (14/04/2026): thêm VND thay slot HCM
#
# Tier 1 — validated sessions cũ, /bt Robust
#   DGC  WR=50.0% PF=1.36 OOS=42%  decay=+9%  | Cap 50% | skip score 85-94
#   SSI  WR=47.6% PF=1.39 OOS=50%  decay=-3%  | Score≥85
#   NKG  WR=53.7% PF=1.51 OOS=25%  decay=+28% | Cap 50% | IS overfit, monitor
#   FRT  WR=51.1% PF=1.38 OOS=62%  decay=-5%  | skip VNI DOWN + HIGH_VOL
# Tier 2 — sascreen session 4 + validated session 9
#   HAH  WR=52.2% PF=1.39 OOS=65%  decay=-16% | Hold 20d | skip score <75
#   PC1  WR=56.8% PF=1.71 OOS=78%  decay=-23% | Hold 20d | skip score 95+
#   STB  WR=56.5% PF=1.75 OOS=83%  decay=-28% | Hold 20d | skip VNI DOWN
#   MCH  WR=48.8% PF=1.61 OOS=72%  decay=-27% | Cap 70%  | req EXTENDED>5%
#   CTS  WR=51.9% PF=1.63 OOS=50%  decay=+7%  | WF OOS 50% caution
# Session 10 — thay thế HCM
#   VND  WR=53.2% PF=1.74 OOS=66.7% decay=-16.5% | Cap 70% | skip score 75-84
SIGNALS_WATCHLIST = [
    # ── Tier 1 (4 mã — HCM đã loại session 9) ────────────────────────────────
    'DGC',   # Hóa chất    | PF=1.36 | WR=50.0% | OOS=42%   | Cap 50%
    'SSI',   # CK          | PF=1.39 | WR=47.6% | OOS=50%   | Score≥85
    'NKG',   # Thép        | PF=1.51 | WR=53.7% | OOS=25%   | Cap 50% ⚠ overfit
    'FRT',   # Bán lẻ      | PF=1.38 | WR=51.1% | OOS=62%   | skip DOWN/HIGH_VOL
    # ── Tier 2 (5 mã session 4+9) ─────────────────────────────────────────────
    'HAH',   # Logistics   | PF=1.39 | WR=52.2% | OOS=65%   | Hold 20d
    'PC1',   # Điện/XD     | PF=1.71 | WR=56.8% | OOS=78%   | Hold 20d
    'STB',   # Ngân hàng   | PF=1.75 | WR=56.5% | OOS=83%   | Hold 20d
    'MCH',   # FMCG        | PF=1.61 | WR=48.8% | OOS=72%   | Cap 70%
    'CTS',   # CK          | PF=1.63 | WR=51.9% | OOS=50%   | caution OOS
    # ── Session 10 — thay thế HCM ─────────────────────────────────────────────
    'VND',   # CK          | PF=1.74 | WR=53.2% | OOS=66.7% | Cap 70% ⚠ CK thứ 3
]
# HCM loại session 9. VND thêm session 10 (sascreen WF=V + /bt Robust).
# ⚠ Sector concentration: VND+SSI+CTS = 3/10 mã chứng khoán — tránh trade đồng thời 3 mã.

# ── SIGNALS_MANUAL — đã xóa toàn bộ (VND/VIC/PDR WF yếu qua /bt) ────────────
# Giữ list rỗng để không break code cũ import SIGNALS_MANUAL
SIGNALS_MANUAL = []

# ═══════════════════════════════════════════════════════════════════════════════
# SCB_WATCHLIST — Score B Signal System (Session 14)
# Độc lập với SIGNALS_WATCHLIST. Dùng cho /scbscan và auto ScB scanner.
# Tier A: full size | Tier B: max 50% size
# ═══════════════════════════════════════════════════════════════════════════════

SCB_WATCHLIST_TIER_A = [
    'BSR', 'BSI', 'ORS', 'PDR', 'CTS', 'PC1',
    'CNG', 'GAS', 'DPM', 'DVP', 'FPT',
    'HMC',   # S17: thêm mới — ΔExp=+2.21% PF=1.64 WR=50% WF=Robust | skip VNI DOWN + Vol MED + Score 75-84 + MA20 OPT/EXT
    # CSV: demote → Tier B (S17: BT mới WF Robust→Prom)
    # CTG: demote → Tier B (S17: BT mới WF Robust→Prom)
]  # 12 mã — deploy ngay, đủ Exp>=1% + PF>=1.3 + WR>=50% + WF Robust

SCB_WATCHLIST_TIER_B = [
    'MCH', 'HBC', 'VIC', 'AGG', 'VCB',
    # S17: demote từ Tier A (BT mới WF Robust→Prom)
    'CSV',   # ΔExp=+3.46% PF=1.57 WR=52% WF=Prom | demote S17
    'CTG',   # ΔExp=+1.58% PF=1.38 WR=55% WF=Prom | demote S17
    # HAX: loại S17 (BT mới: WEAK verdict, ΔExp=+0.78%, không đủ gate)
    # S16 — 5 mã mới từ backtest report (BT confirm + WF Promising/Robust)
    'FRT',   # ΔExp=+2.57% PF=2.36 WR=59% WF=Prom | skip score 65-74 & 95+; sweet spot 75-94
    'VTP',   # ΔExp=+3.34% PF=3.14 WR=60% WF=Prom | skip VNI DOWN
    'DCM',   # ΔExp=+2.03% PF=1.90 WR=55% WF=Prom | skip Vol LOW + MA20 NEAR + MA20 EXT
    'PHP',   # ΔExp=+1.85% PF=1.81 WR=53% WF=Robust| Score A min=95; EXT là PREMIUM (+7.55%)
    'DXS',   # ΔExp=+2.98% PF=2.05 WR=53% WF=Prom | borderline n=30L; skip Vol HIGH + EXT
]  # 12 mã — có điều kiện, max 50% size

SCB_WATCHLIST = SCB_WATCHLIST_TIER_A + SCB_WATCHLIST_TIER_B  # 24 mã tổng (S17: 12+12)

# Score A minimum per mã (từ backtest params)
SCB_SCORE_A_MIN = {
    'BSR': 65, 'BSI': 65, 'ORS': 65, 'PDR': 55, 'CTS': 65,
    'PC1': 65, 'CNG': 65, 'CSV': 65, 'GAS': 45, 'DPM': 65,
    'DVP': 65, 'FPT': 65, 'CTG': 65,
    'HMC': 65,   # S17: mới thêm Tier A
    'MCH': 65, 'HBC': 65, 'VIC': 45, 'AGG': 65, 'VCB': 55,
    # HAX: loại S17
    # S16 — 5 mã mới
    'FRT': 75,   # sweet spot 75-94; skip 65-74 & 95+
    'VTP': 65,   # skip VNI DOWN, score không restrict
    'DCM': 65,   # skip Vol LOW + MA20 NEAR/EXT
    'PHP': 95,   # chỉ bucket 95+ có edge rõ (C3: Score 95+ WR=52% Exp=+2.25%)
    'DXS': 75,   # skip score 65-74 (C3: Exp=-0.40% n=10L)
}

SCB_SCORE_B_MIN = 60  # ngưỡng ScB cứng — áp dụng tất cả mã

# Per-symbol hard skip rules — chỉ giữ rule có n>=20L và |Exp|>=1.5%
# Format: list of (condition_key, condition_value, reason)
SCB_HARD_SKIP = {
    # ── Tier A (không đổi) ────────────────────────────────────────────────────
    'BSR': [('vni_down', True,        'BSR VNI DOWN Exp -5.17% (12L)')],
    'BSI': [('vni_down', True,        'BSI VNI DOWN Exp -4.11% (7L)')],
    'GAS': [('vni_down', True,        'GAS VNI DOWN Exp -2.90% (12L)'),
            ('score_bucket', '75-84', 'GAS score 75-84 Exp -1.80% (14L)')],
    'CTG': [('vni_down', True,        'CTG VNI DOWN Exp -3.28% (10L)')],
    'FPT': [('ma20_zone', 'EXT',      'FPT EXT Exp -2.69% — overbought')],
    'CNG': [('vni_down', True,        'CNG VNI DOWN Exp -4.15% (7L)')],
    'DPM': [('ma20_zone', 'EXT',      'DPM EXT Exp -1.10% (10L)')],
    # S18: HMC — xóa MA20 OPT (Exp=-0.016% n=35L = FLAT, không justify skip)
    # Giữ: VNI DOWN (Exp=-4.33%), Vol MED (WR=33%), Score 75-84 (WR=23%), MA20 EXT (n nhỏ nhưng giữ phòng thủ)
    'HMC': [('vni_down', True,        'HMC VNI DOWN WR=23% Exp=-4.33% (13L)'),
            ('vol_med',  True,        'HMC Vol MED WR=33% Exp=-0.17% (21L)'),
            ('score_bucket', '75-84', 'HMC Score 75-84 WR=23% Exp=-0.535% (13L)'),
            ('ma20_zone', 'EXT',      'HMC MA20 EXT Exp=-0.025% (8L) — flat, giữ phòng thủ')],
    # ── Tier B ────────────────────────────────────────────────────────────────
    'HBC': [('vni_down', True,        'HBC VNI DOWN Exp -5.05% (12L)'),
            ('ma20_zone', 'FAR',      'HBC FAR Exp -7.50% (4L)')],
    'VIC': [('score_bucket', '65-74', 'VIC score 65-74 Exp -3.40% (23L)')],
    # HAX: loại S17 — WEAK verdict
    'VCB': [('vni_down', True,        'VCB VNI DOWN Exp -2.16% (7L)')],
    # ── Tier B — S16: 5 mã mới ───────────────────────────────────────────────
    # S18: FRT — xóa 95+ skip (Exp=+0.922% WR=45% n=29L = dương, đang bỏ lỡ edge)
    # Thêm NEAR skip (Exp=-1.845% WR=28% n=18L — âm rõ và WR rất thấp)
    # score 65-74 vẫn block via SCB_SCORE_A_MIN=75
    'FRT': [('ma20_zone', 'NEAR',     'FRT MA20 NEAR WR=28% Exp=-1.845% (18L) — S18')],
    # VTP: VNI DOWN rất xấu — C1: WR=17% Exp=-3.80% (12L)
    'VTP': [('vni_down', True,        'VTP VNI DOWN Exp -3.80% (12L)')],
    # S18: DCM — thêm VNI FLAT skip (Exp=-0.488% WR=30% n=23L — âm + WR thấp nhất trong C1)
    # Giữ: Vol LOW (Exp=-2.05%), NEAR (borderline WR=35%), EXT (Exp=-1.50% n nhỏ)
    'DCM': [('vni_flat', True,        'DCM VNI FLAT WR=30% Exp=-0.488% (23L) — S18'),
            ('vol_low', True,         'DCM Vol LOW Exp -2.05% (19L)'),
            ('ma20_zone', 'NEAR',     'DCM MA20 NEAR Exp -0.40% (20L) — S16'),
            ('ma20_zone', 'EXT',      'DCM MA20 EXT Exp -1.50% (14L)')],
    # S18: PHP — xóa NEAR skip (Exp=-0.085% n=22L = FLAT, không justify)
    # Giữ OPT skip (Exp=-0.325% WR=43% n=37L — borderline nhưng n lớn đủ tin)
    # EXT là PREMIUM (WR=85% Exp=+7.55%) — KHÔNG skip
    'PHP': [('ma20_zone', 'OPT',      'PHP MA20 OPTIMAL Exp -0.33% WR=43% (37L) — borderline'),
            ('vni_down', True,        'PHP VNI DOWN Exp -2.03% (14L)')],
    # DXS: Vol HIGH và MA20 EXT xấu; score 65-74 block via SCB_SCORE_A_MIN=75
    'DXS': [('vol_high', True,        'DXS Vol HIGH Exp -0.50% (6L)'),
            ('ma20_zone', 'EXT',      'DXS MA20 EXT Exp -1.31% (9L)')],
}

# WF stats — median OOS Exp và worst window (thay binary "3/3 pass")
SCB_WF_STATS = {
    'BSR': {'wf': 'Robust', 'median': 2.19, 'worst':  0.13},
    'BSI': {'wf': 'Robust', 'median': 3.58, 'worst':  0.49},
    'ORS': {'wf': 'Prom',   'median': 3.68, 'worst':  1.15},
    'PDR': {'wf': 'Robust', 'median': 2.32, 'worst':  1.82},
    'CTS': {'wf': 'Robust', 'median': 3.35, 'worst':  2.71},
    'PC1': {'wf': 'Robust', 'median': 1.09, 'worst':  0.38},
    'CNG': {'wf': 'Robust', 'median': 2.82, 'worst': -2.96},
    # CSV: entry cũ (Robust) đã xóa S18 — duplicate với entry S17 bên dưới (Prom)
    'GAS': {'wf': 'Robust', 'median': 1.06, 'worst': -0.08},
    'DPM': {'wf': 'Robust', 'median': 2.21, 'worst':  0.94},
    'DVP': {'wf': 'Robust', 'median': 1.23, 'worst': -0.25},
    'FPT': {'wf': 'Robust', 'median': 1.17, 'worst':  0.74},
    'CTG': {'wf': 'Prom',   'median': 0.87, 'worst':  0.09},  # S17: demote Robust→Prom (BT mới)
    'MCH': {'wf': 'Robust', 'median': 3.92, 'worst':  2.81},
    'HBC': {'wf': 'Robust', 'median': 2.71, 'worst': -1.66},
    'VIC': {'wf': 'Robust', 'median': 0.49, 'worst': -2.21},
    'AGG': {'wf': 'Robust', 'median': 2.29, 'worst': -0.91},
    # HAX: loại S17
    'HMC': {'wf': 'Robust', 'median': 1.82, 'worst': -0.56},  # S17: mới thêm Tier A (W4 OOS=-0.555%)
    'CSV': {'wf': 'Prom',   'median': 1.06, 'worst': -0.65},  # S17: demote Robust→Prom (BT mới)
    'VCB': {'wf': 'Robust', 'median': 0.61, 'worst':  0.55},
    # S16 — 5 mã mới Tier B
    'FRT': {'wf': 'Prom',   'median': 1.16, 'worst':  0.95},  # 2/3 pass; W3 IS❌ OOS=+2.36% (paradox)
    'VTP': {'wf': 'Prom',   'median': 2.78, 'worst':  1.38},  # 2/3 pass; W3 IS❌ OOS=+3.73% (paradox)
    'DCM': {'wf': 'Prom',   'median': 1.67, 'worst':  1.16},  # 2/3 pass; W3 IS❌ OOS=+1.16%
    'PHP': {'wf': 'Robust', 'median': 1.30, 'worst': -0.31},  # S16: 3/3 pass — upgrade Prom→Robust
    'DXS': {'wf': 'Prom',   'median': 4.77, 'worst': -2.60},  # 2/3 pass; W3 OOS=-2.60% fail nặng
}

# Backtest stats — Exp hi / PF / WR (reference cho notification)
SCB_BT_STATS = {
    'BSR': {'exp': 3.08, 'pf': 2.43, 'wr': 65},
    'BSI': {'exp': 2.63, 'pf': 2.02, 'wr': 61},
    'ORS': {'exp': 3.24, 'pf': 2.52, 'wr': 64},
    'PDR': {'exp': 2.58, 'pf': 2.17, 'wr': 61},
    'CTS': {'exp': 2.87, 'pf': 2.17, 'wr': 58},
    'PC1': {'exp': 2.58, 'pf': 2.36, 'wr': 66},
    'CNG': {'exp': 1.88, 'pf': 1.88, 'wr': 54},
    # CSV: entry cũ (exp=1.55) đã xóa S18 — duplicate với entry S17 bên dưới (exp=1.38)
    'GAS': {'exp': 1.42, 'pf': 1.86, 'wr': 58},
    'DPM': {'exp': 1.33, 'pf': 1.58, 'wr': 50},
    'DVP': {'exp': 1.63, 'pf': 2.27, 'wr': 58},
    'FPT': {'exp': 1.35, 'pf': 1.79, 'wr': 60},
    'CTG': {'exp': 1.58, 'pf': 1.38, 'wr': 55},   # S17: exp update từ BT mới (1.10→1.58)
    'MCH': {'exp': 3.93, 'pf': 4.31, 'wr': 59},
    'HBC': {'exp': 1.43, 'pf': 1.46, 'wr': 51},
    'VIC': {'exp': 1.25, 'pf': 1.46, 'wr': 47},
    'AGG': {'exp': 0.93, 'pf': 1.39, 'wr': 54},
    # HAX: loại S17
    'HMC': {'exp': 1.26, 'pf': 1.64, 'wr': 50},   # S17: mới thêm Tier A
    'CSV': {'exp': 1.38, 'pf': 1.57, 'wr': 52},   # S17: exp update từ BT mới (1.55→1.38)
    'VCB': {'exp': 0.66, 'pf': 1.41, 'wr': 54},
    # S16 — 5 mã mới Tier B (Exp = ΔExp ScB≥60 vs baseline)
    'FRT': {'exp': 2.57, 'pf': 2.36, 'wr': 59},
    'VTP': {'exp': 3.34, 'pf': 3.14, 'wr': 60},
    'DCM': {'exp': 2.03, 'pf': 1.90, 'wr': 55},
    'PHP': {'exp': 1.85, 'pf': 1.81, 'wr': 53},
    'DXS': {'exp': 2.98, 'pf': 2.05, 'wr': 53},
}


# ── PORTFOLIO RISK — Manual rule (không automate trong bot) ──────────────────
# Sau 4-5 SL liên tiếp TOÀN WATCHLIST (không phải per-symbol):
#   → Giảm 50% size TẤT CẢ mã cho đến khi có lệnh thắng đầu tiên
# Nguồn: CrossValidation Report v2 Section 8.3
# Trader tự theo dõi và áp dụng thủ công.
# Per-symbol SL streak đã có trong /bt Deep Analytics Block 3E.
PORTFOLIO_SL_STREAK_THRESHOLD = 4  # Ngưỡng SL liên tiếp toàn watchlist → giảm 50% size
HOLD_DAYS_OVERRIDE = {
    'STB': 20,   # HK rate 59%, MFE +6.9% tại d20
    'HAH': 20,   # HK rate 63%, MFE +7.2% tại d20
    'PC1': 20,   # HK rate 70%, MFE +7.6% tại d21
}

# ── SESSION 9+10 PATCH: Position size caps ────────────────────────────────────
POSITION_SIZE_CAPS = {
    'NKG': 0.50,   # WF OOS=25%, IS overfit
    'DGC': 0.50,   # MaxDD -74.8%, suy yếu
    'MCH': 0.70,   # False BK 80%, req EXTENDED>5%
    'VND': 0.70,   # Session 10: new entry, MaxDD -37.2% năm 2024 — size nhỏ khi mới thêm
}

# ── SESSION 9 PATCH: Per-symbol score thresholds ──────────────────────────────
SCORE_THRESHOLDS_PER_SYMBOL = {
    'SSI': 85,   # <85 Exp âm: 65-74: -1.32%, 75-84: -0.05%
    'HAH': 75,   # <75 Exp -1.74% (n=12L)
    'DGC': 75,   # skip bucket 85-94 (Exp -0.233%, n=27L)
}

# ── SESSION 10 PATCH: Score skip buckets (range, không chỉ threshold tối thiểu) ─
# Dict format: symbol → list of (lo, hi) buckets cần SKIP
# Khác với SCORE_THRESHOLDS_PER_SYMBOL (threshold tối thiểu đơn giản):
#   SCORE_SKIP_BUCKETS dùng cho mã có "bucket giữa" âm bất thường
# VND: 65-74 BEST (+3.03) | 75-84 WORST (-2.21) | 85+ tốt → skip bucket 75-84
SCORE_SKIP_BUCKETS = {
    'VND': [(75, 84)],   # Exp=-2.21 (17L) — bucket WORST, ngược intuition
    'DGC': [(85, 94)],   # Exp=-0.233% (27L) — bucket âm dù score cao
    # DGC: SCORE_THRESHOLDS=75 chỉ block <75, không block 85-94.
    # Cần SCORE_SKIP_BUCKETS riêng để skip đúng bucket 85-94.
    # Trade DGC khi score 75-84 hoặc 95+.
}

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
    """Session 9+10: trả về score threshold — ưu tiên SCORE_THRESHOLDS_PER_SYMBOL."""
    if symbol:
        sym_up = str(symbol).upper()
        if sym_up in SCORE_THRESHOLDS_PER_SYMBOL:
            return SCORE_THRESHOLDS_PER_SYMBOL[sym_up]
        cfg = SYMBOL_CONFIG.get(sym_up, {})
        return cfg.get('min_score', MIN_SCORE_BUY)
    return MIN_SCORE_BUY


def is_score_in_skip_bucket(symbol, score):
    """
    Session 10: kiểm tra score có rơi vào bucket cần skip không.
    Dùng cho mã có bucket anomaly (VND: 75-84 worst).
    Trả về True nếu nên skip lệnh này.
    """
    sym_up = str(symbol).upper()
    buckets = SCORE_SKIP_BUCKETS.get(sym_up, [])
    for lo, hi in buckets:
        if lo <= score <= hi:
            return True
    return False


def get_hold_days(symbol=None):
    """Session 9: trả về hold_days theo per-symbol override, fallback HOLD_DAYS."""
    if symbol:
        sym_up = str(symbol).upper()
        if sym_up in HOLD_DAYS_OVERRIDE:
            return HOLD_DAYS_OVERRIDE[sym_up]
        cfg = SYMBOL_CONFIG.get(sym_up, {})
        return cfg.get('hold_days', HOLD_DAYS)
    return HOLD_DAYS


def get_position_size_cap(symbol):
    """Session 9+10: trả về position size cap (0.0–1.0). Mặc định 1.0 (full)."""
    return POSITION_SIZE_CAPS.get(str(symbol).upper(), 1.0)


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
