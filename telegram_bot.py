import os
import json
import logging
import time
import threading
import requests
from datetime import datetime, timedelta
import pytz

VN_TZ = pytz.timezone('Asia/Ho_Chi_Minh')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
# Strip khoảng trắng và ký tự = thừa (lỗi copy-paste khi set Railway env)
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '').strip().lstrip('=').strip()

# ── Subscriber system — PostgreSQL + fallback file ───────────────────────────
SUBSCRIBERS_FILE = '/tmp/subscribers.txt'
DATABASE_URL = os.environ.get('DATABASE_URL', '')

_PSYCOPG2_AVAILABLE = None  # Cache check một lần

def _get_db_conn():
    global _PSYCOPG2_AVAILABLE
    if not DATABASE_URL:
        return None
    # Kiểm tra psycopg2 có sẵn không (chỉ log 1 lần)
    if _PSYCOPG2_AVAILABLE is None:
        try:
            import psycopg2
            _PSYCOPG2_AVAILABLE = True
        except ImportError:
            _PSYCOPG2_AVAILABLE = False
            logger.warning('psycopg2 chua cai — dung file fallback. '
                           'Them psycopg2-binary vao requirements.txt')
    if not _PSYCOPG2_AVAILABLE:
        return None
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
        return conn
    except Exception as e:
        logger.warning('DB connect failed: ' + str(e))
        return None

def _init_db():
    conn = _get_db_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            'CREATE TABLE IF NOT EXISTS subscribers '
            '(chat_id TEXT PRIMARY KEY, name TEXT, joined TIMESTAMP DEFAULT NOW())'
        )
        conn.commit()
        cur.close()
        conn.close()
        logger.info('DB: subscribers table ready')
    except Exception as e:
        logger.warning('DB init failed: ' + str(e))

def load_subscribers():
    subs = set()
    if CHAT_ID:
        subs.add(str(CHAT_ID))
    # Thử PostgreSQL — một connection duy nhất, auto-retry sau init
    conn = _get_db_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute('SELECT chat_id FROM subscribers')
            for r in cur.fetchall():
                subs.add(str(r[0]))
            cur.close()
            conn.close()
            return subs
        except Exception as e:
            err = str(e)
            logger.warning('DB load failed: ' + err)
            try:
                conn.close()
            except Exception:
                pass
            # Auto-init table nếu chưa tồn tại rồi thử lại 1 lần
            if 'does not exist' in err or 'relation' in err:
                logger.info('DB: table missing, running init...')
                _init_db()
                conn2 = _get_db_conn()
                if conn2:
                    try:
                        cur2 = conn2.cursor()
                        cur2.execute('SELECT chat_id FROM subscribers')
                        for r in cur2.fetchall():
                            subs.add(str(r[0]))
                        cur2.close()
                        conn2.close()
                        return subs
                    except Exception as e2:
                        logger.warning('DB load retry failed: ' + str(e2))
                        try:
                            conn2.close()
                        except Exception:
                            pass
    # Fallback: đọc file
    try:
        with open(SUBSCRIBERS_FILE) as f:
            for line in f:
                cid = line.strip()
                if cid:
                    subs.add(cid)
    except FileNotFoundError:
        pass
    return subs

def save_subscriber(cid, name=''):
    # Thử PostgreSQL trước
    conn = _get_db_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(
                'INSERT INTO subscribers (chat_id, name) VALUES (%s, %s) '
                'ON CONFLICT (chat_id) DO UPDATE SET name=%s',
                (str(cid), name, name)
            )
            conn.commit()
            cur.close()
            conn.close()
            return
        except Exception as e:
            logger.warning('DB save failed: ' + str(e))
    # Fallback: ghi file
    subs = load_subscribers()
    subs.add(str(cid))
    with open(SUBSCRIBERS_FILE, 'w') as f:
        f.write(chr(10).join(sorted(subs)))

def remove_subscriber(cid):
    # Thử PostgreSQL trước
    conn = _get_db_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute('DELETE FROM subscribers WHERE chat_id=%s', (str(cid),))
            conn.commit()
            cur.close()
            conn.close()
            return
        except Exception as e:
            logger.warning('DB remove failed: ' + str(e))
    # Fallback: sửa file
    subs = load_subscribers()
    subs.discard(str(cid))
    if CHAT_ID:
        subs.add(str(CHAT_ID))
    with open(SUBSCRIBERS_FILE, 'w') as f:
        f.write(chr(10).join(sorted(subs)))

def broadcast(text):
    for cid in load_subscribers():
        send(text, cid)

_last_broadcast_time = 0

def broadcast_signals():
    global _last_broadcast_time
    import time as _t
    # Chống duplicate: không broadcast_signals trong vòng 3 phút
    if _t.time() - _last_broadcast_time < 180:
        logger.info('broadcast_signals skipped (cooldown 3min)')
        return
    _last_broadcast_time = _t.time()
    for cid in load_subscribers():
        handle_signals(cid)
from config import (
    SETTLEMENT_DAYS, SYMBOL_CONFIG, SIGNALS_WATCHLIST as _SIGNALS_WATCHLIST_CFG, SIGNALS_MANUAL,
    get_sl_tp, get_sl_tp_pct, get_min_score, get_wf_verdict,
    MIN_SCORE_BUY,
)

# ── SIGNALS_WATCHLIST — Score A — Nguồn: config.py (Sprint 6, 08/04/2026) ───
# KHÔNG override local — dùng trực tiếp từ config.py để đảm bảo đồng bộ.
# 13 mã: DGC, SSI, GEX, HCM, BSR, NKG, FRT (Tier1) + VPB, POW, HSG, HAH, BID, NVL (Tier2)
# Tiêu chí: OOS WR >= 37% VÀ Decay <= 22% VÀ PF IS >= 1.0
SIGNALS_WATCHLIST = list(_SIGNALS_WATCHLIST_CFG)

# ── DUP-01: import market_context một lần duy nhất ──────────────────────────
try:
    import market_context as _mc
except ImportError:
    _mc = None
    logger.warning('market_context module not found — B-filter disabled')

API_URL = os.environ.get('API_BASE_URL', 'http://localhost:8080')

# ── PERF-01: WF summary cache 24h — tránh gọi backtest nặng lặp lại ────────
_WF_CACHE    = {}   # symbol -> wf_summary dict
_WF_CACHE_TS = {}   # symbol -> timestamp (float)

# ── ML_CONFIRMED_WATCHLIST — Mã đã xác nhận edge qua backtest (11 đợt, ~98 mã) ─
# Chỉ các mã này mới nhận auto alert ML signal (broadcast cho subscribers).
# Các mã khác vẫn hiển thị trong /mlscan nhưng không gửi alert tự động.
#
# Tier A (full size):    HCM, FRT, VCI, LPB
# Tier B (size 70-80%):  DGC, NKG, SSI, MWG, VIX, BSI, ORS, HDB
#
# LƯU Ý GRADE FILTER:
#   HDB: chỉ vào lệnh khi grade == 'STRONG' (score >= 90). PASS grade PF=0.48.
#
# Đã loại (suspend): POM — WF sụp cả đợt 5 lẫn đợt 6 với SL=8% đúng
#
# Cập nhật: Sprint 7 + đợt 5–9 (08/04/2026) — 12 mã confirmed, hit rate 28%
# Nguồn: backtest.py run_backtest_momentum() SL=5-6% TP=15-17% Hold=18d Score≥90
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
    'MWG': ('B', 5, 15, 18, 'WR=55% PF=1.42 WF Robust decay=0.6% — theo doi live 2025-2026'),
    'VIX': ('B', 6, 17, 18, 'WR=51% PF=2.10 OOS=52% — dot 1'),
    'BSI': ('B', 6, 17, 18, 'WR=50% PF=2.09 OOS=52% — dot 1'),
    'ORS': ('B', 6, 17, 18, 'WR=52% PF=1.53 OOS=61% — dot 2, WF Robust'),
    'HDB': ('B', 6, 17, 18, 'WR=65%* PF=1.73* OOS=66% — dot 4, CHI STRONG grade'),
}

# ═══════════════════════════════════════════════════════════════════════════════
# WATCHLIST RIÊNG CHO TỪNG VERSION ML BACKTEST
# ───────────────────────────────────────────────────────────────────────────────
# mlbt v1 all  → dùng config.BACKTEST_WATCHLIST (20 mã active)
# mlbtv3 all   → MLBT_V3_WATCHLIST bên dưới (2 pattern types: momentum/counter)
# ═══════════════════════════════════════════════════════════════════════════════
MLBT_V3_WATCHLIST = {
    # fmt: (source, pattern_type, note)
    # pattern_type: 'momentum_v3' hoặc 'counter_v3' — tự detect qua _get_v3_mode()
    # ── event-study confirmed (7 mã, pooled pattern) ─────────────────────────
    'NKG': ('confirmed', 'momentum_v3', 'trend_10d 6.31x — pool NKG+POW'),
    'POW': ('confirmed', 'momentum_v3', 'trend_10d 6.4x — pool NKG+POW'),
    'FRT': ('confirmed', 'momentum_v3', 'vol_trend_10d + vol_spike_3d — pool FRT+DGW'),
    'DGW': ('confirmed', 'momentum_v3', 'vol_trend_10d + vol_spike_3d — pool FRT+DGW'),
    'MBS': ('confirmed', 'momentum_v3', 'rs_vni_5d neg p=0.0045 — pool DGW+MBS'),
    'MWG': ('confirmed', 'counter_v3',  'near_52w_low 4.3x + rsi_oversold — pool MWG+PNJ'),
    'PNJ': ('confirmed', 'counter_v3',  'near_52w_low 4.4x + rsi_oversold 3.2x — pool MWG+PNJ'),
    # ── v1-eliminated: chạy v3 để xem có cải thiện không ────────────────────
    'SMC': ('v1-elim', 'momentum_v3', ''), 'LAS': ('v1-elim', 'momentum_v3', ''),
    'IDC': ('v1-elim', 'momentum_v3', ''), 'VSC': ('v1-elim', 'momentum_v3', ''),
    'QNS': ('v1-elim', 'momentum_v3', ''), 'TLH': ('v1-elim', 'momentum_v3', ''),
    'AGR': ('v1-elim', 'momentum_v3', ''), 'TVN': ('v1-elim', 'momentum_v3', ''),
    'BCG': ('v1-elim', 'momentum_v3', ''), 'SIP': ('v1-elim', 'momentum_v3', ''),
    'GVR': ('v1-elim', 'momentum_v3', ''), 'PXS': ('v1-elim', 'momentum_v3', ''),
    'CNG': ('v1-elim', 'momentum_v3', ''), 'PHP': ('v1-elim', 'momentum_v3', ''),
    'REE': ('v1-elim', 'momentum_v3', ''), 'CMX': ('v1-elim', 'momentum_v3', ''),
    'ANV': ('v1-elim', 'counter_v3',  ''), 'VHC': ('v1-elim', 'counter_v3',  ''),
    'EIB': ('v1-elim', 'momentum_v3', ''), 'NAB': ('v1-elim', 'momentum_v3', ''),
    'STB': ('v1-elim', 'momentum_v3', ''), 'TCM': ('v1-elim', 'momentum_v3', ''),
    'VCS': ('v1-elim', 'momentum_v3', ''), 'FTS': ('v1-elim', 'momentum_v3', ''),
    'PLX': ('v1-elim', 'momentum_v3', ''), 'BVH': ('v1-elim', 'momentum_v3', ''),
    'PVI': ('v1-elim', 'momentum_v3', ''), 'PGB': ('v1-elim', 'momentum_v3', ''),
    'BVB': ('v1-elim', 'momentum_v3', ''), 'TPB': ('v1-elim', 'momentum_v3', ''),
    'KDH': ('v1-elim', 'momentum_v3', ''), 'VHM': ('v1-elim', 'momentum_v3', ''),
    'VNM': ('v1-elim', 'counter_v3',  'n=5 events qua it — theo doi'),
    'ACB': ('v1-elim', 'momentum_v3', ''), 'TCB': ('v1-elim', 'momentum_v3', ''),
    'GAS': ('v1-elim', 'momentum_v3', 'v3 OOS=68% Robust — theo doi'),
    'DCM': ('v1-elim', 'momentum_v3', 'event study 0 pattern'),
    'DPM': ('v1-elim', 'momentum_v3', ''), 'VIB': ('v1-elim', 'momentum_v3', ''),
    'PVS': ('v1-elim', 'momentum_v3', ''), 'MSB': ('v1-elim', 'momentum_v3', ''),
    'VPB': ('v1-elim', 'momentum_v3', ''), 'GMD': ('v1-elim', 'momentum_v3', ''),
    'PVT': ('v1-elim', 'momentum_v3', ''), 'BCM': ('v1-elim', 'momentum_v3', ''),
    'PVD': ('v1-elim', 'momentum_v3', ''), 'CTS': ('v1-elim', 'momentum_v3', ''),
    'TVS': ('v1-elim', 'momentum_v3', ''), 'ART': ('v1-elim', 'momentum_v3', ''),
    'PTI': ('v1-elim', 'momentum_v3', ''), 'MIG': ('v1-elim', 'momentum_v3', ''),
    'VGC': ('v1-elim', 'momentum_v3', ''), 'BMP': ('v1-elim', 'momentum_v3', ''),
    'EVF': ('v1-elim', 'momentum_v3', ''), 'PC1': ('v1-elim', 'momentum_v3', ''),
    'FMC': ('v1-elim', 'counter_v3',  ''), 'MSN': ('v1-elim', 'counter_v3',  ''),
    # ── new: chưa test v1 ─────────────────────────────────────────────────────
    'DHC': ('new', 'momentum_v3', 'n_OOS nho — can confirm'),
    'KDC': ('new', 'counter_v3',  'n_OOS nho — can confirm'),
    'HAH': ('new', 'momentum_v3', 'cang/logistics, signals watchlist'),
    'HSG': ('new', 'momentum_v3', 'thep, signals watchlist'),
    'BSR': ('new', 'momentum_v3', 'loc dau Binh Son, signals watchlist'),
    'OCB': ('new', 'momentum_v3', 'NH monitoring live'),
    'DGC': ('new', 'momentum_v3', 'event study 0 pattern — fallback v1'),
    'HPG': ('new', 'momentum_v3', ''), 'PHR': ('new', 'momentum_v3', ''),
    'DPR': ('new', 'momentum_v3', ''),
}

def _get_sl_tp(symbol):
    """Wrapper tương thích — dùng get_sl_tp_pct() từ config.py."""
    return get_sl_tp_pct(symbol)

# ── Watchlist 2 tầng ─────────────────────────────────────────────────────────
# bt=True  : đã backtest 7 năm, ngưỡng score đã tối ưu
# ── Watchlist 21 mã — đã backtest 7 năm, score_min tối ưu từ kết quả thực tế ─
# Luồng quyết định:
#   [A] Score kỹ thuật (RSI/MA/MACD/Vol...) → tín hiệu MUA/BAN/THEO DÕI
#   [B] B-adjustment (Wyckoff/Liquidity/Wick) → cộng/trừ score A
#       B không tạo signal độc lập — chỉ điều chỉnh score của A
#   Kết quả: score_adj = max(0, min(100, score_A + b_delta))
#
# WATCHLIST_META: build từ SYMBOL_CONFIG (config.py) — không hardcode nữa
# Chỉ gồm mã có wf_verdict != '' (đã qua walk-forward) + mã trong SIGNALS_WATCHLIST
WATCHLIST_META = {}
for _sym in SIGNALS_WATCHLIST:
    _cfg = SYMBOL_CONFIG.get(_sym, {})
    _sl_pct, _tp_pct = get_sl_tp_pct(_sym)
    WATCHLIST_META[_sym] = {
        'score_min': get_min_score(_sym),
        'sl':        _sl_pct,
        'tp':        _tp_pct,
        'sl_pct':    _sl_pct,
        'tp_pct':    _tp_pct,
        'group':     _cfg.get('group', 'Khac'),
        'wf_verdict':get_wf_verdict(_sym),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PAPER TRADING — Lưu trữ lệnh giả lập
# ═══════════════════════════════════════════════════════════════════════════════
PAPER_FILE   = '/tmp/paper_trades.json'
PAPER_MONTHS = 2   # Thời gian theo dõi (tháng)

def _load_paper():
    """Đọc file JSON lưu paper trades."""
    try:
        if os.path.exists(PAPER_FILE):
            with open(PAPER_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {'trades': [], 'created': datetime.now(VN_TZ).isoformat()}

def _save_paper(data):
    """Ghi file JSON paper trades."""
    try:
        with open(PAPER_FILE, 'w') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f'paper save error: {e}')

def _add_paper_trade(symbol, price, score, sl_pct, tp_pct):
    """Thêm lệnh MUA paper trade mới."""
    data = _load_paper()
    # Tránh trùng lệnh trong cùng 1 ngày
    today = datetime.now(VN_TZ).strftime('%Y-%m-%d')
    existing = [t for t in data['trades']
                if t['symbol'] == symbol and t['entry_date'] == today
                and t['status'] == 'OPEN']
    if existing:
        return False, 'Da co lệnh OPEN cho ' + symbol + ' hom nay'

    sl_price = round(price * (1 - sl_pct / 100))
    tp_price = round(price * (1 + tp_pct / 100))

    # Settlement: T+2 ngày giao dịch — mới bán được từ ngày này (HOSE từ 11/2021)
    settlement_date = _trading_days_after(today, SETTLEMENT_DAYS)

    # Expire: tính từ settlement_date (ngày có thể bán), không phải entry_date
    # Lý do: lệnh chỉ "có hiệu lực" từ T+2 — PAPER_MONTHS tính từ lúc có thể giao dịch
    trading_days_expire = PAPER_MONTHS * 22
    expire = _trading_days_after(settlement_date, trading_days_expire)

    trade = {
        'id':              len(data['trades']) + 1,
        'symbol':          symbol,
        'entry_date':      today,
        'entry_price':     price,
        'score':           score,
        'sl_price':        sl_price,
        'tp_price':        tp_price,
        'sl_pct':          sl_pct,
        'tp_pct':          tp_pct,
        'settlement_date': settlement_date,   # T+2 ngày GD — mới bán được
        'expire_date':     expire,
        'status':          'OPEN',
        'exit_date':       None,
        'exit_price':      None,
        'pnl_pct':         None,
        'exit_reason':     None,
    }
    data['trades'].append(trade)
    _save_paper(data)
    return True, trade

def _trading_days_after(date_str, n):
    """
    Tính ngày giao dịch thứ N sau date_str.
    Bỏ qua: thứ 7, CN, và ngày lễ chính thức TTCK Việt Nam.

    FIX: Phiên bản cũ chỉ bỏ T7/CN → T+2 trước Tết / 30-4 / 2-9 bị tính sai.
    Ngày lễ được hardcode đến 2027 và cập nhật hàng năm.
    Nguồn: Thông báo nghỉ lễ của HOSE/HNX hàng năm.
    """
    # ── Ngày lễ TTCK Việt Nam (ngày GD nghỉ) ────────────────────────────────
    # Format: 'YYYY-MM-DD'
    # Ghi chú: nếu lễ trùng T7/CN, ngày nghỉ bù thường là T2 tuần sau — đã bao gồm.
    VN_HOLIDAYS = {
        # 2024
        '2024-01-01',                                               # Tết Dương lịch
        '2024-02-08', '2024-02-09', '2024-02-12', '2024-02-13',   # Tết Nguyên Đán + nghỉ bù
        '2024-02-14', '2024-02-15', '2024-02-16',
        '2024-04-18',                                               # Giỗ Tổ Hùng Vương (29/3 âm)
        '2024-04-29', '2024-04-30',                                 # 30/4 + nghỉ bù
        '2024-05-01',                                               # 1/5 Quốc tế Lao động
        '2024-09-02', '2024-09-03',                                 # 2/9 + nghỉ bù
        # 2025
        '2025-01-01',                                               # Tết Dương lịch
        '2025-01-27', '2025-01-28', '2025-01-29',                  # Tết Nguyên Đán
        '2025-01-30', '2025-01-31', '2025-02-03',
        '2025-04-07',                                               # Giỗ Tổ Hùng Vương
        '2025-04-30', '2025-05-01', '2025-05-02',                  # 30/4 + 1/5 + nghỉ bù
        '2025-09-01', '2025-09-02',                                 # 2/9 + nghỉ bù (T2)
        # 2026
        '2026-01-01', '2026-01-02',                                 # Tết Dương lịch + nghỉ bù
        '2026-02-16', '2026-02-17', '2026-02-18',                  # Tết Nguyên Đán
        '2026-02-19', '2026-02-20', '2026-02-23',
        '2026-03-27',                                               # Giỗ Tổ Hùng Vương (ước tính)
        '2026-04-30', '2026-05-01',                                 # 30/4 + 1/5
        '2026-09-02',                                               # 2/9
        # 2027
        '2027-01-01',                                               # Tết Dương lịch
        '2027-02-05', '2027-02-06', '2027-02-07',                  # Tết Nguyên Đán (ước tính)
        '2027-02-08', '2027-02-09', '2027-02-10',
        '2027-04-16',                                               # Giỗ Tổ Hùng Vương (ước tính)
        '2027-04-30', '2027-05-01', '2027-05-03',                  # 30/4 + 1/5 + nghỉ bù
        '2027-09-02',                                               # 2/9
    }

    dt = datetime.strptime(date_str, '%Y-%m-%d').date()
    count = 0
    while count < n:
        dt += timedelta(days=1)
        dt_str = dt.strftime('%Y-%m-%d')
        if dt.weekday() < 5 and dt_str not in VN_HOLIDAYS:   # T2-T6 và không phải lễ
            count += 1
    return dt.strftime('%Y-%m-%d')


def _update_paper_prices():
    """
    Cập nhật giá hiện tại cho tất cả lệnh OPEN, kiểm tra TP/SL/EXPIRED.
    Gọi từ background scanner.

    Quy tắc T+2 TTCK Việt Nam (HOSE từ 11/2021):
      - Mua ngày T: CP về TK sau 2 ngày GIAO DỊCH (T+2).
      - Chỉ kiểm tra SL/TP từ ngày T+2 trở đi.
      - Lệnh mua thứ 5 → T+2 là thứ 4 tuần sau (bỏ qua T7, CN).
    """
    data = _load_paper()
    changed = False
    today   = datetime.now(VN_TZ).strftime('%Y-%m-%d')

    for t in data['trades']:
        if t['status'] != 'OPEN':
            continue

        # ── Quy tắc T+2: chưa đến ngày settlement thì chỉ cập nhật giá, không SL/TP
        settlement_date = t.get('settlement_date')
        if not settlement_date:
            # Tính và lưu lại settlement_date cho lệnh cũ chưa có field này
            settlement_date = _trading_days_after(t['entry_date'], SETTLEMENT_DAYS)
            t['settlement_date'] = settlement_date
            changed = True

        can_sell = today >= settlement_date

        # Kiểm tra hết hạn
        if today >= t['expire_date']:
            t['status']     = 'EXPIRED'
            t['exit_date']  = today
            t['exit_price'] = t['entry_price']
            t['pnl_pct']    = 0.0
            t['exit_reason']= 'EXPIRED'
            changed = True
            continue

        # Lấy giá hiện tại qua API
        try:
            d = call_api('/api/price/' + t['symbol'])
            if not d:
                continue
            cur_price = d.get('price', 0)
            if cur_price <= 0:
                continue
            t['current_price'] = cur_price
            pnl = (cur_price - t['entry_price']) / t['entry_price'] * 100

            if not can_sell:
                # T+1 / T+2: chỉ theo dõi giá, chưa thể bán
                t['pnl_pct'] = round(pnl, 2)
                t['note'] = 'Cho T+2 (' + settlement_date + ') moi ban duoc'
                changed = True
                continue

            if cur_price <= t['sl_price']:
                t['status']     = 'SL'
                t['exit_date']  = today
                t['exit_price'] = cur_price
                t['pnl_pct']    = round(pnl, 2)
                t['exit_reason']= 'SL'
                changed = True
            elif cur_price >= t['tp_price']:
                t['status']     = 'TP'
                t['exit_date']  = today
                t['exit_price'] = cur_price
                t['pnl_pct']    = round(pnl, 2)
                t['exit_reason']= 'TP'
                changed = True
            else:
                t['pnl_pct'] = round(pnl, 2)
                changed = True
        except Exception as e:
            logger.error(f'paper update {t["symbol"]}: {e}')

    if changed:
        _save_paper(data)
    return data


def send(text, chat_id=None, plain=False):
    """
    Gửi message Telegram.
    plain=True: không dùng parse_mode HTML (an toàn cho text có ký tự đặc biệt).
    plain=False (default): parse_mode=HTML — cần escape < > & trong text thường.
    FIX: Nếu HTML gửi lỗi 400 (parse error), tự động retry bằng plain text.
    """
    cid = chat_id or CHAT_ID
    if not TOKEN or not cid:
        return False

    MAX = 3800
    chunks = []
    if len(text) <= MAX:
        chunks = [text]
    else:
        lines = text.split('\n')
        current = ''
        for line in lines:
            if len(current) + len(line) + 1 > MAX:
                chunks.append(current)
                current = line
            else:
                current = current + '\n' + line if current else line
        if current:
            chunks.append(current)

    ok = True
    for chunk in chunks:
        payload = {'chat_id': cid, 'text': chunk}
        if not plain:
            payload['parse_mode'] = 'HTML'
        try:
            r = requests.post(
                'https://api.telegram.org/bot' + TOKEN + '/sendMessage',
                json=payload,
                timeout=10
            )
            if r.status_code == 200:
                continue
            # FIX: Nếu lỗi 400 parse entities → retry bằng plain text
            if r.status_code == 400 and 'parse entities' in r.text:
                logger.warning('send HTML parse error, retrying as plain: ' + r.text[:80])
                r2 = requests.post(
                    'https://api.telegram.org/bot' + TOKEN + '/sendMessage',
                    json={'chat_id': cid, 'text': chunk},
                    timeout=10
                )
                if r2.status_code != 200:
                    logger.error('send plain retry error: ' + r2.text[:80])
                    ok = False
            else:
                logger.error('send error: ' + r.text)
                ok = False
        except Exception as e:
            logger.error('send: ' + str(e))
            ok = False
    return ok


def call_api(endpoint):
    # Bot và Flask chạy cùng process trên Railway
    # → localhost LUÔN nhanh hơn, không qua internet, không timeout
    # → External URL chỉ là fallback khi chạy local dev
    #
    # Timeout hợp lý:
    #   /analyze/*  : 55s — FA compute mất 20-30s, cộng thêm buffer khi rate limit
    #   /fairvalue/*: 55s — tương tự FA compute
    #   /signals    : 90s — cold start cần 73s warmup, tăng buffer
    #   khác        : 15s — price, market, warmup...
    bases = ['http://localhost:8080', 'http://127.0.0.1:8080', API_URL]
    seen = set()
    ordered = []
    for b in bases:
        if b not in seen:
            seen.add(b)
            ordered.append(b)

    for base in ordered:
        try:
            if 'localhost' in base or '127.0.0.1' in base:
                if '/analyze/' in endpoint or '/fairvalue/' in endpoint:
                    t = 55
                elif '/signals' in endpoint:
                    t = 180
                elif '/foreign/' in endpoint:
                    t = 40   # shark_detector 5 sources × ~8s
                else:
                    t = 15
            else:
                if '/analyze/' in endpoint or '/fairvalue/' in endpoint:
                    t = 90
                elif '/signals' in endpoint:
                    t = 45
                elif '/foreign/' in endpoint:
                    t = 50
                else:
                    t = 30
            r = requests.get(base + endpoint, timeout=t)
            if r.status_code == 200:
                return r.json()
        except requests.exceptions.Timeout:
            logger.warning(f'api {base}{endpoint}: timeout ({t}s)')
        except Exception as e:
            logger.warning('api ' + base + endpoint + ': ' + str(e))
    return {}


def fmt_vol(v):
    if v >= 1000000:
        return f'{v / 1000000:.1f}M'
    if v >= 1000:
        return f'{v / 1000:.0f}K'
    return str(int(v))


def action_emoji(action):
    if 'MUA' in action:
        return '🟢'
    if 'BAN' in action:
        return '🔴'
    return '🟡'


def sig_emoji(typ):
    if typ == 'bull':
        return '📈'
    if typ == 'bear':
        return '📉'
    return '➡'


def escape_html(txt):
    return (str(txt)
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;')
            .replace('&amp;lt;', '&lt;')
            .replace('&amp;gt;', '&gt;')
            .replace('&amp;amp;', '&amp;')
            )


def get_group(signals, key):
    lines = []
    for item in signals:
        if isinstance(item, (list, tuple)) and len(item) == 3:
            g, t, txt = item
            if g == key:
                lines.append(' ' + sig_emoji(t) + ' ' + escape_html(txt))
    return '\n'.join(lines) if lines else ''


def build_action_lines(data):
    action  = data.get('action', '')
    price   = data.get('price', 0)
    sl      = data.get('stop_loss', 0)
    tp      = data.get('take_profit', 0)
    sl_lbl  = data.get('sl_label', '')
    tp_lbl  = data.get('tp_label', '')
    NL      = chr(10)

    if action == 'MUA':
        entry_opt = data.get('entry_opt', price)
        entry_max = data.get('entry_max', price)
        entry_lbl = data.get('entry_label', '')

        if entry_opt < price * 0.999:
            entry_line = (
                ' 🎯 <b>Vào lệnh tối ưu:</b>' + NL
                + '   Dat Limit: <b>' + f'{entry_opt:,.0f}' + 'd</b>'
                + ' ~ ' + f'{entry_max:,.0f}' + 'd' + NL
                + '   (' + entry_lbl + ')' + NL
                + '   Hoac mua ngay: ' + f'{price:,.0f}' + 'd (chac khop, kem dep hon)' + NL
            )
        else:
            entry_line = (
                ' 🎯 <b>Vào lệnh: Mua ngay ' + f'{price:,.0f}' + 'd</b>' + NL
                + '   (HT rat gan, không can cho pullback)' + NL
            )

        return (
            entry_line
            + ' 🛑 Stop Loss : <b>' + f'{sl:,.0f}' + 'd</b> (' + sl_lbl + ')' + NL
            + ' 💰 Chốt lời  : <b>' + f'{tp:,.0f}' + 'd</b> (' + tp_lbl + ')' + NL
            + NL
        )
    elif action == 'BAN':
        NL = chr(10)
        return (
            ' Nen ban o : ' + f'{price:,.0f}' + 'd (gia hiện tại)' + NL
            + ' Vùng mua lại: ' + f'{tp:,.0f}' + 'd (vung ho tro gan nhat)' + NL
            + ' Neu đã mua : Cắt lỗ neu gia tiep tuc giam them -7%' + NL + NL
        )
    else:
        NL = chr(10)
        return (
            ' Theo doi vung: ' + f'{sl:,.0f}' + 'd - ' + f'{tp:,.0f}' + 'd' + NL
            + ' Chưa đủ tín hiệu để vào lệnh' + NL + NL
        )


def _build_conclusion(score_a, score_ab, b_delta, b_details,
                      b_overall, action, ae, data):
    """KẾT LUẬN A+B — b_details là list of dicts từ calc_b_adjustment."""
    lines = ''
    sym = data.get('symbol', '')

    # Fix #4: Hiển thị ngưỡng MUA thực tế của mã (per-symbol, không phải 65 chung)
    meta      = WATCHLIST_META.get(sym, {})
    score_min = meta.get('score_min', 65)
    is_watchlist = sym in WATCHLIST_META

    if b_delta == 0 or not b_details:
        lines += ' ' + ae + ' <b>' + action + '</b> (' + str(score_a) + '/100)\n'
    else:
        lines += ' Score A   (ky thuat): ' + str(score_a) + '/100\n'
        for d in b_details:
            if isinstance(d, dict):
                ds = ('+' if d['delta'] > 0 else '') + str(d['delta']) + 'd'
                lines += ' ' + d['icon'] + ' ' + d['label'] + ': ' + ds + '\n'
            else:
                lines += ' ' + str(d) + '\n'
        sign = '+' if b_delta > 0 else ''
        lines += (' Score A+B (tong hop): <b>' + str(score_ab) + '/100</b>'
                  + ' (' + sign + str(b_delta) + 'd)\n')
        if b_overall:
            fmap = {'NGUY HIEM':'🔴', 'CAN THAN':'⚠',
                    'CHAP NHAN':'🟡', 'THUAN LOI':'✅'}
            lines += ' ' + fmap.get(b_overall, '❓') + ' TT VN: <b>' + b_overall + '</b>\n'
        lines += ' ' + ae + ' <b>' + action + '</b>\n'

    # Hiển thị ngưỡng thực tế — tránh nhầm lẫn score 67 = MUA với VCB cần 80
    if is_watchlist and score_min != 65:
        effective_action = 'MUA' if score_ab >= score_min else 'THEO DOI'
        if effective_action != action:
            lines += ('\n&#x26A0; <i>Luu y: ' + sym + ' can score &gt;= ' + str(score_min)
                      + ' (backtest). Voi score ' + str(score_ab) + '/100'
                      + ' &#x2192; thuc te: </i><b>' + effective_action + '</b>\n')
        else:
            lines += '\n<i>Ngưỡng MUA cua ' + sym + ': &gt;= ' + str(score_min) + ' (tu backtest)</i>\n'
    elif not is_watchlist:
        lines += '\n<i>Ngưỡng MUA mặc định: &gt;= 65 (chua backtest per-symbol)</i>\n'

    lines += build_action_lines(data)
    lines += '<i>Score A: ky thuat | Score A+B: tong hop voi dieu kien TT VN</i>\n'
    lines += '<i>Chi mang tinh tham khao, không phai tu van dau tu</i>'
    return lines


def _format_1h_warnings(warnings_1h):
    """
    Format cảnh báo 1H — chỉ còn Volume Spike.
    Trả về chuỗi rỗng nếu không có cảnh báo (không hiển thị mục thừa).
    """
    if not warnings_1h:
        return ''

    w = warnings_1h[0]   # Chỉ có 1 loại cảnh báo: VOL_SPIKE_1H
    emoji = '🔴' if w.get('level') == 'HIGH' else '⚠'
    msg   = w.get('message', '')
    return (
        '<b>⏰ Dòng tiền 1H:</b>\n'
        ' ' + emoji + ' ' + escape_html(msg) + '\n'
        '<i>(Chỉ tham khảo — không anh huong score 1D)</i>\n\n'
    )


def _format_fair_value(fv, ta_action='', ta_score=50):
    """
    Format Fair Value section cho tin nhắn phân tích.
    Hiển thị vùng giá hợp lý, upside/downside%, và cảnh báo khi TA+FA mâu thuẫn.
    FIX: discount field nay = upside% từ giá lên FV (dương=còn tiềm năng, âm=đã đắt).
    """
    if not fv or not fv.get('ok'):
        err = fv.get('error', '') if fv else ''
        if err:
            return '<b>📊 Dinh gia co ban:</b>\n <i>Không tinh duoc: ' + escape_html(err[:60]) + '</i>\n\n'
        return ''   # Không hiển thị nếu không có dữ liệu

    valuation  = fv.get('valuation', '')
    fair_low   = fv.get('fair_low', 0)
    fair_val   = fv.get('fair_value', 0)
    fair_high  = fv.get('fair_high', 0)
    upside     = fv.get('discount', 0)   # Đây là upside% từ giá lên FV
    method     = fv.get('method', '')
    details    = fv.get('details', {})
    note       = fv.get('note', '')

    val_emoji = {
        'UNDERVALUED': '🟢',  # Xanh
        'FAIR':        '🟡',  # Vàng
        'OVERVALUED':  '🔴',  # Đỏ
    }.get(valuation, '❓')

    val_vn = {
        'UNDERVALUED': 'DANG RE (duoi vung hợp lý)',
        'FAIR':        'GIA HOP LY',
        'OVERVALUED':  'DANG DAT (tren vung hợp lý)',
    }.get(valuation, valuation)

    # Upside/downside từ giá hiện tại lên FV
    if upside >= 0:
        upside_s = '+' + f'{upside:.1f}%' + ' upside'
    else:
        upside_s = f'{upside:.1f}%' + ' (dang premium)'

    # Các chỉ số cơ bản
    detail_lines = ''
    for k, v in details.items():
        if v and v != 0:
            if isinstance(v, float) and v > 1000:
                detail_lines += f' {k}: {v:,.0f}d\n'
            else:
                detail_lines += f' {k}: {v}\n'

    # Cảnh báo kết hợp TA + FA
    ta_fa_note = ''
    if ta_action == 'MUA' and valuation == 'UNDERVALUED':
        ta_fa_note = ' ✅ KT + CB dong thuan: MUA co co so co ban\n'
    elif ta_action == 'MUA' and valuation == 'OVERVALUED':
        ta_fa_note = ' ⚠ CẢNH BÁO: KT=MUA nhung gia dang dat hon fair value ' + upside_s + '\n'
    elif ta_action == 'BAN' and valuation == 'UNDERVALUED':
        ta_fa_note = ' ⚠ CẢNH BÁO: KT=BAN nhung gia dang re hon fair value ' + upside_s + '\n'

    lines = (
        '<b>📊 Dinh gia co ban (' + escape_html(method) + '):</b>\n'
        ' Vùng giá hợp lý: <b>' + f'{fair_low:,.0f}' + 'd — ' + f'{fair_high:,.0f}' + 'd</b>\n'
        ' Fair value TT  : <b>' + f'{fair_val:,.0f}' + 'd</b>\n'
        ' Tiem nang      : <b>' + upside_s + '</b>\n'
        ' Dinh gia       : ' + val_emoji + ' <b>' + val_vn + '</b>\n'
    )
    if detail_lines:
        lines += detail_lines
    if note:
        lines += ' <i>Luu y: ' + escape_html(note) + '</i>\n'
    if ta_fa_note:
        lines += ta_fa_note
    lines += '<i>Cap nhat 1 lan/ngay luc 8:30 | Không phải tư vấn đầu tư</i>\n\n'
    return lines


def _fmt_rs_inline(data):
    """Dòng RS ngắn cho header analyze."""
    rs20 = data.get('rs_20d')
    b52  = data.get('breakout_52w', False)
    if rs20 is None:
        return ''
    if b52:
        return chr(10) + '&#x1F3AF; <b>PHÁ ĐỈNH 52 TUẦN! RS20=' + f'{rs20:+.1f}%</b>'
    if rs20 > 5:
        return chr(10) + '&#x1F680; RS20=' + f'{rs20:+.1f}% (Dan dau thi truong)'
    if rs20 < -5:
        return chr(10) + '&#x2B07; RS20=' + f'{rs20:+.1f}% (Lag thi truong)'
    return ''


def _fmt_regime_compare(result_no, result_yes):
    """So sánh backtest co/không co regime filter cho /bt output."""
    NL = chr(10)
    if not result_no or not result_yes:
        return ''
    buy_no  = result_no.get('buy',  {})
    buy_yes = result_yes.get('buy', {})
    if not buy_no or not buy_yes:
        return ''

    wr_no   = buy_no.get('win_rate', 0)
    wr_yes  = buy_yes.get('win_rate', 0)
    pnl_no  = buy_no.get('avg_pnl',  0)
    pnl_yes = buy_yes.get('avg_pnl', 0)
    pf_no   = buy_no.get('profit_factor', 0)
    pf_yes  = buy_yes.get('profit_factor', 0)
    n_no    = buy_no.get('total', 0)
    n_yes   = buy_yes.get('total', 0)

    dwr   = round(wr_yes  - wr_no,  1)
    dpnl  = round(pnl_yes - pnl_no, 2)
    dpf   = round(pf_yes  - pf_no,  2)
    block = n_no - n_yes  # lenh bi block (duong = da block)

    if dwr >= 1.5 and dpnl >= 0:
        emoji, verdict = '&#x1F7E2;', 'Regime filter CO ICH — WR va PnL cai thien'
    elif dwr >= 0 and dpnl >= 0:
        emoji, verdict = '&#x1F7E1;', 'Regime filter TRUNG TINH — cai thien nhe'
    elif block > 0 and dwr < 0:
        emoji, verdict = '&#x26A0;', 'Regime filter CO HAI — block lệnh tot (ma counter-cyclical?)'
    else:
        emoji, verdict = '&#x1F7E1;', 'Regime filter it tac dong'

    pf_no_s  = f'{pf_no:.2f}'  if pf_no  != float('inf') else 'inf'
    pf_yes_s = f'{pf_yes:.2f}' if pf_yes != float('inf') else 'inf'

    out  = '&#x1F4CA; <b>Market Regime Filter:</b>' + NL
    out += chr(9472) * 24 + NL
    out += f'  Không regime: {n_no}L | WR={wr_no}% | PnL={pnl_no:+.2f}% | PF={pf_no_s}' + NL
    out += f'  Co regime:    {n_yes}L | WR={wr_yes}% | PnL={pnl_yes:+.2f}% | PF={pf_yes_s}' + NL
    sign_wr = '+' if dwr >= 0 else ''
    sign_pnl= '+' if dpnl>= 0 else ''
    sign_pf = '+' if dpf >= 0 else ''
    out += f'  Delta: WR={sign_wr}{dwr}% | PnL={sign_pnl}{dpnl}% | PF={sign_pf}{dpf} | Block={block}L' + NL
    out += f'{emoji} {verdict}' + NL
    return out





def _fmt_score_breakdown_inline(data):
    """Breakdown Score A — nhom theo section, hien thi chinh xac."""
    log = data.get('score_log', [])
    if not log or len(log) < 2:
        return ''
    NL = chr(10)

    # Gom delta theo label (cong don)
    section_totals = {}
    section_order  = []
    for e in log:
        if e[0] == 'Baseline':
            continue
        label = e[0]
        delta = e[1] if len(e) > 1 else 0
        if delta == 0:
            continue
        if label not in section_totals:
            section_totals[label] = 0
            section_order.append(label)
        section_totals[label] += delta

    if not section_totals:
        return ''

    # Chia pos / neg
    pos_parts = []
    neg_parts = []
    for lbl in section_order:
        d = section_totals[lbl]
        if d > 0:
            pos_parts.append((d, lbl))
        elif d < 0:
            neg_parts.append((d, lbl))

    # Sort theo abs delta
    pos_parts.sort(reverse=True)
    neg_parts.sort(key=lambda x: x[0])

    lines = [NL + '<b>&#x1F4CA; Score Breakdown:</b>']
    lines.append('  Base=50')

    if pos_parts:
        parts = [f'+{d} {l}' for d, l in pos_parts[:6]]
        lines.append('  &#x2795; ' + ' | '.join(parts))

    if neg_parts:
        parts = [f'{d} {l}' for d, l in neg_parts[:4]]
        lines.append('  &#x2796; ' + ' | '.join(parts))

    # Verify: tong cong = score
    total = 50 + sum(section_totals.values())
    final = data.get('score', 50)
    match = '✓' if abs(total - final) <= 1 else f'⚠ tính={total}'
    lines.append(f'  <b>= Score A: {final}/100</b> {match}')
    return NL.join(lines)


def _fmt_sprint4(data):
    """Hien thi Sprint 4 indicators: ADX, BB Squeeze, OBV, ROC."""
    NL = chr(10)
    out = ''

    adx    = data.get('adx')
    atr    = data.get('atr')
    sq     = data.get('squeeze', False)
    vc     = data.get('vol_compress', False)
    obv_d  = data.get('obv_div', False)
    roc    = data.get('roc')
    ms     = data.get('ma50_slope')

    lines = []

    # ADX
    if adx is not None:
        if adx >= 35:
            lines.append(f'ADX={adx:.0f} — Xu hướng MẠNH')
        elif adx >= 25:
            lines.append(f'ADX={adx:.0f} — Có xu hướng')
        else:
            lines.append(f'ADX={adx:.0f} — Thị trường sideway')

    # BB Squeeze + Vol Compression combo
    if sq and vc:
        lines.append('&#x26A1; BB Squeeze + Vol tích lũy — Breakout sắp xảy ra')
    elif sq:
        lines.append('&#x1F4A1; BB Squeeze — Biến động sắp nổ')
    elif vc:
        lines.append('&#x1F4C9; Vol đang tích lũy (gom hàng âm thầm)')

    # OBV Divergence
    if obv_d:
        lines.append('&#x1F4C8; OBV tăng trong khi giá đi ngang — Smart money tích lũy')

    # ROC
    if roc is not None:
        if roc > 8:
            lines.append(f'ROC10={roc:+.1f}% — Momentum bung no')
        elif roc > 3:
            lines.append(f'ROC10={roc:+.1f}% — Momentum tang')
        elif roc < -8:
            lines.append(f'ROC10={roc:+.1f}% — Momentum giam manh')

    # MA50 Slope
    if ms is not None and abs(ms) > 0.5:
        arrow = '&#x2197;' if ms > 0 else '&#x2198;'
        lines.append(f'MA50 slope={ms:+.1f}% {arrow} {"(tang toc)" if ms > 0 else "(cham lai)"}')

    # ATR hint (for SL)
    if atr is not None:
        entry = data.get('entry', 0)
        sl_atr = round(atr * 2)
        sl_pct = round(sl_atr / entry * 100, 1) if entry > 0 else 0
        lines.append(f'ATR(14)={atr:,.0f}d | SL dong={sl_atr:,.0f}d ({sl_pct:.1f}%)')

    if not lines:
        return ''
    return NL + '&#x1F4CA; <b>Sprint4:</b> ' + (' | '.join(lines[:3]))


def _fmt_sector_rs(data):
    """Hien thi Intra-Sector RS trong /score va /analyze."""
    sr = data.get('sector_rs', {})
    if not sr or not sr.get('available'):
        return ''
    NL = chr(10)
    grp        = sr.get('group', '')
    rank       = sr.get('rank', 0)
    total      = sr.get('total', 0)
    pct        = sr.get('percentile', 0)
    sym_ret    = sr.get('symbol_ret', 0)      # raw return của mã
    sec_avg    = sr.get('sector_avg_ret', None)  # trung bình peers
    diff       = sr.get('diff_vs_avg', None)     # mã vs ngành ← số quan trọng nhất
    label      = sr.get('label', '')
    peer_rets  = sr.get('peer_rets', {})

    # Nếu file cũ chưa có diff_vs_avg → tính tại chỗ
    if diff is None and peer_rets:
        sec_avg = round(sum(peer_rets.values()) / len(peer_rets), 1)
        diff    = round(sym_ret - sec_avg, 1) if sym_ret is not None else 0

    if pct >= 80:   icon = '&#x1F525;'
    elif pct >= 60: icon = '&#x1F4C8;'
    elif pct >= 40: icon = '&#x27A1;'
    elif pct >= 20: icon = '&#x1F4C9;'
    else:           icon = '&#x2B07;'

    diff_str = f'{diff:+.1f}%' if diff is not None else 'N/A'
    sym_str  = f'{sym_ret:+.1f}%' if sym_ret is not None else 'N/A'
    avg_str  = f'{sec_avg:+.1f}%' if sec_avg is not None else 'N/A'

    # Dòng 1: kết quả chính
    line = (
        NL + icon + f' <b>Sector RS</b> [{grp}] Hang {rank}/{total} ({pct:.0f}%ile)'
        + NL
        + f'   Mã: <b>{sym_str}</b> | TB ngành: <b>{avg_str}</b> | Vs ngành: <b>{diff_str}</b>'
    )

    # Dòng 2: tất cả peers (không cắt top 3 để VCI luôn hiển thị)
    if peer_rets:
        peers_sorted = sorted(peer_rets.items(), key=lambda x: -x[1])
        peer_str = '  '.join(f'{p}:{r:+.1f}%' for p, r in peers_sorted)
        line += NL + '   ' + peer_str

    return line


def _fmt_wf_summary(data):
    """Hien thi WF summary 1 dong trong /score."""
    wf = data.get('wf_summary', {})
    if not wf:
        return ''
    return chr(10) + wf.get('label', '')


def _fmt_vwap(data):
    """Hien thi VWAP tuan + thang trong /score va /analyze."""
    vi = data.get('vwap_info', {})
    if not vi or not vi.get('vwap_w'):
        return ''
    NL = chr(10)
    vw  = vi.get('vwap_w', 0)
    vm  = vi.get('vwap_m', 0)
    pw  = vi.get('pct_w', 0.0)
    pm  = vi.get('pct_m', 0.0)
    bon = vi.get('bonus', 0)
    arrow_w = '&#x25B2;' if pw >= 0 else '&#x25BC;'
    arrow_m = '&#x25B2;' if pm >= 0 else '&#x25BC;'
    bon_str = ('+' if bon >= 0 else '') + str(bon)
    return (NL + '&#x1F4CA; VWAP  W:' + f'{vw:,.0f}' + 'd'
            + ' (' + arrow_w + f'{pw:+.1f}%)'
            + '  M:' + f'{vm:,.0f}' + 'd'
            + ' (' + arrow_m + f'{pm:+.1f}%)'
            + '  Bonus:' + bon_str)


def _fmt_regime_inline(data):
    """Dòng cảnh báo Market Regime cho header /analyze."""
    regime  = data.get('market_regime', 'UNKNOWN')
    note    = data.get('regime_note', '')
    vni     = data.get('market_regime_vni', 0)
    ma200   = data.get('market_regime_ma200', 0)
    exempt  = data.get('regime_exempt', False)
    NL = chr(10)
    if exempt:
        # Mã counter-cyclical — không áp dụng regime
        if regime == 'BEAR':
            return NL + '&#x26AA; Regime BEAR nhung mã này MIEN TRU (counter-cyclical)'
        return ''
    if regime == 'BEAR':
        return (NL + '&#x1F534; <b>BEAR MARKET</b> — VNI(' + f'{vni:,.0f}'
                + ') duoi MA200(' + f'{ma200:,.0f}' + ') — Score da duoc cap, giam size')
    elif regime == 'BULL':
        line = NL + '&#x1F7E2; <b>BULL MARKET</b> — VNI>MA50>MA200'
        if note:
            line += ' (' + note + ')'
        return line
    return ''  # NEUTRAL: khong hien thi de bot gon


def _fmt_shark_inline(shark_score):
    """Dòng shark score ngắn cho header analyze."""
    if shark_score >= 80:
        return chr(10) + '&#x1F988;&#x1F988; <b>Shark Score: ' + str(shark_score) + '/100 — GOM MANH!</b>'
    elif shark_score >= 60:
        return chr(10) + '&#x1F988; Shark Score: ' + str(shark_score) + '/100 — Co dau hieu gom hang'
    elif shark_score >= 40:
        return chr(10) + '&#x1F440; Shark Score: ' + str(shark_score) + '/100 — Theo doi them'
    return ''


def _fmt_foreign(data):
    """
    Hiển thị foreign flow inline trong /analyze và /score.
    Hỗ trợ cả mode='historical' (series) và mode='snapshot' (VCI GraphQL).
    """
    fi = data.get('foreign_info', {})
    if not fi or not fi.get('available'):
        return ''
    NL    = chr(10)
    mode  = fi.get('mode', 'historical')
    bonus = fi.get('bonus', 0)
    bonus_str = (f' <b>({bonus:+d}đ Score A)</b>' if bonus != 0 else '')

    if mode == 'snapshot':
        c_ratio   = fi.get('current_ratio', 0)
        m_ratio   = fi.get('max_ratio', 0)
        room_used = fi.get('room_used_pct', 0)  # = c_ratio/m_ratio*100
        room_left = fi.get('room_left_pct', round(m_ratio - c_ratio, 1))
        if room_used >= 95:
            icon = '&#x1F534;&#x1F30F;'
        elif room_used >= 80:
            icon = '&#x1F7E0;&#x1F30F;'
        elif room_used >= 50:
            icon = '&#x1F7E1;&#x1F30F;'
        else:
            icon = '&#x1F7E2;&#x1F30F;'
        return (
            NL + icon
            + f' <b>Khối ngoại:</b> Room {room_used:.1f}% đã dùng'
            + f' | Hold <b>{c_ratio:.1f}%</b>/{m_ratio:.1f}%'
            + bonus_str
        )

    # historical mode
    cbuy  = fi.get('consecutive_buy', 0)
    csell = fi.get('consecutive_sell', 0)
    net10 = fi.get('net_10d', 0)
    nratio = fi.get('net_ratio', 0)

    if cbuy >= 5:
        icon  = '&#x1F30F;&#x1F4C8;'
        trend = f'Mua ròng <b>{cbuy} phiên</b> liên tiếp'
    elif cbuy >= 3:
        icon  = '&#x1F30F;'
        trend = f'Mua ròng {cbuy} phiên liên tiếp'
    elif cbuy >= 1:
        icon  = '&#x1F30F;'
        trend = f'Mua ròng {cbuy} phiên'
    elif csell >= 3:
        icon  = '&#x26A0;&#x1F30F;'
        trend = f'Bán ròng <b>{csell} phiên</b> liên tiếp'
    elif csell >= 1:
        icon  = '&#x1F30F;'
        trend = f'Bán ròng {csell} phiên'
    else:
        return ''  # trung tính, không hiển thị

    net_str = f' | Net10d: {net10:+.2f}M cp ({nratio:+.1f}% ADTV)'
    return (NL + icon + ' <b>Khối ngoại:</b> ' + trend + net_str + bonus_str)


def handle_ml(symbol, chat_id):
    """
    /ml SYM — Momentum Leader: hiển thị 2-tier scoring chi tiết.
    Độc lập với Score A. Tier1: Price>MA50+Vol>1.2x. Tier2: 0-120đ.
    """
    NL = chr(10)
    send('&#x1F4CA; Đang tính <b>Momentum Leader ' + symbol + '</b>...', chat_id)
    try:
        data = call_api('/api/analyze/' + symbol.upper())
        if not data or 'error' in data:
            send('&#x274C; Không lấy được dữ liệu ' + symbol, chat_id)
            return

        ms    = data.get('momentum_signal', {})
        price = data.get('price', 0)
        score_a = data.get('score', 50)
        action  = data.get('action', 'THEO_DOI')

        # Tier 1 fail
        if not ms.get('tier1_pass'):
            ma50     = data.get('ma50', 0)
            vol_rat  = data.get('vol_ratio', 1.0)
            reasons  = []
            if price <= ma50:
                reasons.append(f'&#x1F4CC; Giá <b>{price:,.0f}đ</b> ≤ MA50 <b>{ma50:,.0f}đ</b> (cần Price &gt; MA50)')
            if vol_rat < 1.2:
                reasons.append(f'&#x1F4CC; Vol <b>{vol_rat:.1f}x</b> &lt; 1.2x MA20 (thanh khoản yếu)')
            send(
                '&#x26AA; <b>Momentum Leader — ' + symbol + '</b>' + NL
                + '━' * 26 + NL
                + '&#x274C; <b>Không qua Tier 1 (Core Filter)</b>' + NL + NL
                + NL.join(reasons) + NL + NL
                + '━' * 26 + NL
                + f'&#x1F4CA; Score A: <b>{score_a}/100</b> ({action})' + NL
                + '<i>&#x2139; Tier 1 yêu cầu: Price &gt; MA50 VÀ Vol &gt; MA20 × 1.2</i>',
                chat_id
            )
            return

        grade = ms.get('grade', '')
        score = ms.get('score', 0)
        comps = ms.get('components', {})
        pens  = ms.get('penalties', [])
        pct52 = ms.get('pct52w', 0)
        rsi4  = ms.get('rsi4', 0)
        h52w  = ms.get('high52w', 0)

        # Grade header
        if grade == 'STRONG':
            header = '&#x1F525;&#x1F4CA; <b>STRONG MOMENTUM LEADER — ' + symbol + '</b>'
        elif grade == 'PASS':
            header = '&#x1F4C8; <b>MOMENTUM SIGNAL — ' + symbol + '</b>'
        else:
            header = '&#x26AA; <b>Momentum Scan — ' + symbol + '</b> (chưa đủ điều kiện)'

        # Score bar
        filled = round(score / 12)
        bar = '&#x2588;' * filled + '&#x2591;' * (10 - filled)

        # Component table
        comp_order = [
            ('rs_vni',       'RS vs VNI',       20),
            ('rs_sector',    'RS vs Ngành',      20),
            ('rsi_level',    'RSI Level',        10),
            ('rsi_4d',       'RSI Speed 4d',     10),
            ('rsi_5d',       'RSI Speed 5d',     10),
            ('price_range',  'Price in Range',   10),
            ('breakout_5d',  'Breakout 5d',      10),
            ('vol_expansion','Vol Expansion',    10),
            ('w52',          '52W Proximity',    20),
        ]
        comp_lines = ''
        total_possible = 0
        for key, name, max_pt in comp_order:
            c = comps.get(key, (0, ''))
            pts, desc = c[0], c[1]
            total_possible += max_pt
            icon = '&#x2705;' if pts > 0 else '&#x25AB;'
            comp_lines += f'  {icon} {name:<18} {pts:>2}/{max_pt}d  {str(desc).replace("<", "&lt;").replace(">", "&gt;")}' + NL

        pen_lines = ''
        for p in pens:
            pen_lines += '  &#x26A0; ' + p + NL

        msg = (
            header + NL
            + '=' * 30 + NL
            + f'{bar} <b>{score}/120</b>' + NL
            + f'Giá: <b>{price:,.0f}d</b>  |  52W: <b>{pct52:.1f}%</b> ({h52w:,.0f}d đỉnh)' + NL
            + f'Score A (hệ thống cũ): {score_a}/100 ({action})' + NL + NL
            + '<b>Tier 2 — Chi tiết:</b>' + NL
            + comp_lines
            + (NL + '<b>Penalty:</b>' + NL + pen_lines if pen_lines else '')
            + NL + '─' * 28 + NL
        )

        if grade == 'STRONG':
            msg += '&#x1F525; <b>STRONG MOMENTUM LEADER</b> — RS + RSI + Structure đều mạnh'
        elif grade == 'PASS':
            msg += '&#x1F4C8; <b>PASS</b> — Đủ điều kiện, theo dõi breakout tiếp theo'
        else:
            msg += f'&#x26AA; Chưa đủ (cần ≥75, hiện {score}đ) — theo dõi thêm'

        msg += NL + NL + '<i>Độc lập với Score A | Cooldown 24h/mã</i>'
        send(msg, chat_id)

    except Exception as e:
        logger.error('handle_ml ' + symbol + ': ' + str(e))
        send('&#x274C; Lỗi Momentum Leader: ' + str(e)[:100], chat_id)



def handle_mlscan(mode, chat_id):
    """
    /mlscan          — Scan mã trong BACKTEST_WATCHLIST (ML confirmed + theo dõi).
    /mlscan extended — Giống base, toàn bộ BACKTEST_WATCHLIST.
    Kết quả: Strong 🔥 / Pass 📈 / Near Miss 🟡

    FIX: Chỉ quét mã trong BACKTEST_WATCHLIST — không mở rộng thêm mã ngoài.
    Mã đã loại khỏi watchlist sẽ không được quét nữa.
    """
    NL = chr(10)

    try:
        from config import BACKTEST_WATCHLIST
        sym_list = list(BACKTEST_WATCHLIST)
    except Exception:
        sym_list = list(ML_CONFIRMED_WATCHLIST.keys())

    # extended = giống base (BACKTEST_WATCHLIST đã là danh sách đầy đủ đã tuyển chọn)
    mode_label = (f'Full scan ({len(sym_list)} mã)'
                  if mode == 'extended'
                  else f'Watchlist ({len(sym_list)} mã)')

    send(
        '&#x1F4CA; <b>ML Scan — ' + mode_label + '</b>' + NL
        + 'Đang quét Momentum Leader 2-tier...' + NL
        + '<i>Tier 1: Price&gt;MA50 + Vol&gt;1.2x | Tier 2: 0-120đ</i>',
        chat_id
    )

    import concurrent.futures, time as _time
    t0      = _time.time()
    results = {'strong': [], 'pass_': [], 'near': [], 'fail_t1': [], 'error': []}
    total   = len(sym_list)

    def _scan_one(sym):
        try:
            data = call_api('/api/analyze/' + sym)
            if not data or 'error' in data:
                return sym, None
            ms = data.get('momentum_signal', {})
            return sym, {
                'sym':     sym,
                'price':   data.get('price', 0),
                'score_a': data.get('score', 50),
                'action':  data.get('action', 'THEO_DOI'),
                'ms':      ms,
            }
        except Exception as e:
            logger.debug(f'mlscan {sym}: {e}')
            return sym, None

    # max_workers=10: /api/analyze/ mất ~15-20s/mã do FA compute
    # Song song 10 mã → tổng thời gian ~30-40s thay vì 4-6 phút tuần tự
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(_scan_one, s): s for s in sym_list}
        for fut in concurrent.futures.as_completed(futures):
            sym, r = fut.result()
            if r is None:
                results['error'].append(sym)
                continue
            ms = r['ms']
            if not ms or not ms.get('tier1_pass'):
                results['fail_t1'].append(r)
            else:
                grade = ms.get('grade', '')
                score = ms.get('score', 0)
                if   grade == 'STRONG':  results['strong'].append(r)
                elif grade == 'PASS':    results['pass_'].append(r)
                elif score >= 60:        results['near'].append(r)
                else:                    results['fail_t1'].append(r)

    elapsed = _time.time() - t0

    def _s(r): return r['ms'].get('score', 0) if r.get('ms') else 0
    for k in ('strong', 'pass_', 'near'):
        results[k].sort(key=_s, reverse=True)

    def _row(r, detail=True):
        sym   = r['sym']
        ms    = r['ms'] or {}
        score = ms.get('score', 0)
        p52   = ms.get('pct52w', 0)
        pens  = ms.get('penalties', [])
        pen   = ' &#x26A0;' if pens else ''
        # Badge xác nhận backtest
        ml_cfg = ML_CONFIRMED_WATCHLIST.get(sym)
        if ml_cfg:
            badge = ' &#x2705;' if ml_cfg[0] == 'A' else ' &#x1F7E1;'
        else:
            badge = ''
        if detail:
            return (f'  <b>{sym}</b>{badge}: {score}/120'
                    f' | 52W:{p52:.0f}%'
                    f' | A:{r["score_a"]}'
                    f' | {r["price"]:,.0f}đ{pen}')
        return f'  <b>{sym}</b>{badge}: {score}/120 | 52W:{p52:.0f}%{pen}'

    parts = []
    if results['strong']:
        parts.append(
            '&#x1F525; <b>STRONG LEADER</b> ≥90đ — '
            + str(len(results['strong'])) + ' mã' + NL
            + NL.join(_row(r) for r in results['strong'])
        )
    if results['pass_']:
        rows   = results['pass_'][:8]
        suffix = (NL + f'  <i>+ {len(results["pass_"])-8} mã nữa...</i>'
                  if len(results['pass_']) > 8 else '')
        parts.append(
            '&#x1F4C8; <b>PASS</b> 75-89đ — '
            + str(len(results['pass_'])) + ' mã' + NL
            + NL.join(_row(r) for r in rows) + suffix
        )
    if results['near']:
        parts.append(
            '&#x1F7E1; <b>NEAR MISS</b> 60-74đ — '
            + str(len(results['near'])) + ' mã' + NL
            + NL.join(_row(r, detail=False) for r in results['near'][:5])
        )
    if not parts:
        parts.append(
            '&#x26AA; Không có mã nào qua Tier 1' + NL
            + f'({len(results["fail_t1"])} mã bị lọc — Price≤MA50 hoặc Vol&lt;1.2x)' + NL
            + '<i>Thị trường yếu — chờ breakout.</i>'
        )

    n = lambda k: len(results[k])
    footer = (
        NL + '━' * 26 + NL
        + f'&#x1F4CA; <b>Tổng kết</b>: {total} mã | {elapsed:.0f}s' + NL
        + f'  🔥 Strong:{n("strong")} | 📈 Pass:{n("pass_")} | 🟡 Near:{n("near")} | ❌ Fail:{n("fail_t1")}' + NL
        + '<i>✅ = Tier A backtest | 🟡 = Tier B backtest | /ml SYM để xem chi tiết</i>'
    )

    send(
        '&#x1F4CA; <b>ML Scan — ' + mode_label + '</b>' + NL
        + '━' * 26 + NL
        + (NL + '━' * 22 + NL).join(parts)
        + footer,
        chat_id
    )


def handle_foreign(symbol, chat_id):
    """
    /foreign SYM — Hiển thị chi tiết foreign trading 10 phiên gần nhất.
    """
    NL = chr(10)
    send('&#x1F30F; Đang tải dữ liệu <b>Khối ngoại ' + symbol + '</b>...', chat_id)
    try:
        data = call_api('/api/foreign/' + symbol.upper())
        if not data or not data.get('available'):
            send(
                '&#x1F30F; <b>Khối ngoại — ' + symbol + '</b>' + NL
                + '━' * 26 + NL
                + '&#x26A0; Chưa lấy được data tự động.' + NL
                + '<i>(vnstock/TCBS/SSI/CafeF đều chưa có dữ liệu)</i>' + NL + NL
                + '&#x1F4F1; <b>Xem thủ công:</b>' + NL
                + '  • <a href="https://iboard.ssi.com.vn">SSI iBoard</a> → Tìm <b>' + symbol + '</b> → Khối ngoại' + NL
                + '  • <a href="https://dstock.vndirect.com.vn/tim-kiem-co-phieu/' + symbol + '">VN Direct — ' + symbol + '</a>' + NL
                + '  • <a href="https://fireant.vn/ma-chung-khoan/' + symbol + '">FireAnt — ' + symbol + '</a>',
                chat_id
            )
            return

        mode  = data.get('mode', 'historical')
        label = data.get('label', '')

        if mode == 'snapshot':
            # ── Snapshot mode: VCI GraphQL room/holding data ─────────────────
            c_ratio    = data.get('current_ratio', 0)
            m_ratio    = data.get('max_ratio', 0)
            room_used  = data.get('room_used_pct', 0)
            room_left  = data.get('room_left_pct', round(m_ratio - c_ratio, 1))

            # Room bar visual (room_used = c_ratio/m_ratio*100 — đúng)
            filled    = round(room_used / 10)
            room_bar  = '&#x2588;' * filled + '&#x2591;' * (10 - filled)
            if room_used >= 95:
                room_emoji = '&#x1F534;'
            elif room_used >= 80:
                room_emoji = '&#x1F7E0;'
            elif room_used >= 50:
                room_emoji = '&#x1F7E1;'
            else:
                room_emoji = '&#x1F7E2;'

            msg = (
                '&#x1F30F; <b>Khối ngoại — ' + symbol + '</b>' + NL
                + '=' * 28 + NL + NL
                + room_emoji + ' <b>' + label + '</b>' + NL + NL
                + f'Room: <b>{room_bar} {room_used:.1f}%</b> đã dùng' + NL
                + f'Đang nắm: <b>{c_ratio:.1f}%</b> / tối đa <b>{m_ratio:.1f}%</b>' + NL
                + f'Còn lại:  <b>{room_left:.1f}%</b> room' + NL
                + NL + '<i>Nguồn: VCI GraphQL (snapshot hôm nay)</i>'
            )
        else:
            # ── Historical mode: flow series 30 ngày ────────────────────────
            cbuy   = data.get('consecutive_buy', 0)
            csell  = data.get('consecutive_sell', 0)
            net5   = data.get('net_5d', 0)
            net10  = data.get('net_10d', 0)
            nratio = data.get('net_ratio', 0)
            raw    = data.get('raw_days', [])
            dates  = data.get('dates', [])

            if cbuy >= 5:    trend_icon = '&#x1F7E2;&#x1F7E2;'
            elif cbuy >= 3:  trend_icon = '&#x1F7E2;'
            elif cbuy >= 1:  trend_icon = '&#x1F7E1;'
            elif csell >= 3: trend_icon = '&#x1F534;&#x1F534;'
            elif csell >= 1: trend_icon = '&#x1F534;'
            else:             trend_icon = '&#x26AA;'

            bar_lines = ''
            for i, val in enumerate(raw[-10:]):
                d = dates[i] if i < len(dates) else f'T-{10-i}'
                arrow = '&#x25B2;' if val > 0 else ('&#x25BC;' if val < 0 else '&#x25A0;')
                bar_lines += f'  {d}: {arrow} {val:+.0f}k cp' + NL

            msg = (
                '&#x1F30F; <b>Khối ngoại — ' + symbol + '</b>' + NL
                + '=' * 28 + NL + NL
                + trend_icon + ' <b>' + label + '</b>' + NL + NL
                + f'Net 5 phiên:  <b>{net5:+.2f}M cp</b>' + NL
                + f'Net 10 phiên: <b>{net10:+.2f}M cp</b> ({nratio:+.1f}% ADTV)' + NL + NL
                + '<b>Chi tiết 10 phiên gần nhất:</b>' + NL
                + bar_lines
                + NL + '<i>Nguồn: TCBS/VCI | Đơn vị: nghìn CP (k cp)</i>'
            )
        send(msg, chat_id)
    except Exception as e:
        logger.error('handle_foreign ' + symbol + ': ' + str(e))
        send('&#x274C; Lỗi tải foreign data: ' + str(e)[:100], chat_id)


def format_momentum_signal(sym, ms, price, ml_tier=None, ml_note=None):
    """
    Format alert cho Momentum Leader Signal (2-tier).
    ms = momentum_signal dict từ /api/analyze
    ml_tier: 'A' / 'B' / None (nếu None = không qua backtest confirm)
    ml_note: ghi chú từ ML_CONFIRMED_WATCHLIST
    """
    NL    = chr(10)
    grade = ms.get('grade', '')
    score = ms.get('score', 0)
    label = ms.get('label', '')
    comps = ms.get('components', {})
    pens  = ms.get('penalties', [])
    pct52 = ms.get('pct52w', 0)

    if grade == 'STRONG':
        emoji = '&#x1F525;&#x1F4CA;'
        title = 'STRONG MOMENTUM LEADER'
    else:
        emoji = '&#x1F4C8;'
        title = 'MOMENTUM SIGNAL'

    # Tier badge từ backtest
    if ml_tier == 'A':
        tier_line = '&#x2705; <b>Tier A</b> — Backtest xac nhan (WR≥60%, PF≥2.0)' + NL
    elif ml_tier == 'B':
        tier_line = '&#x1F7E1; <b>Tier B</b> — Backtest chap nhan (WR≥52%, PF≥1.8)' + NL
    else:
        tier_line = ''

    # Component summary (chỉ hiện những gì có điểm)
    comp_lines = ''
    for key, name in [
        ('rs_vni',       'RS vs VNI'),
        ('rs_sector',    'RS vs Nganh'),
        ('rsi_level',    'RSI Level'),
        ('rsi_4d',       'RSI Speed 4d'),
        ('rsi_5d',       'RSI Speed 5d'),
        ('price_range',  'Price Range'),
        ('breakout_5d',  'Breakout 5d'),
        ('vol_expansion','Vol Expansion'),
        ('w52',          '52W Proximity'),
    ]:
        c = comps.get(key)
        if c and c[0] > 0:
            comp_lines += f'  &#x2705; {name}: {c[1]} (+{c[0]}d)' + NL

    pen_lines = ''
    for p in pens:
        pen_lines += f'  &#x26A0; {p}' + NL

    note_line = (f'<i>Note: {ml_note}</i>' + NL) if ml_note else ''

    msg = (
        emoji + ' <b>' + title + ' — ' + sym + '</b>' + NL
        + '=' * 30 + NL
        + tier_line
        + f'Gia: <b>{price:,.0f}d</b> | Score: <b>{score}/120</b>' + NL
        + f'Vi tri 52W: <b>{pct52:.1f}%</b>' + NL + NL
        + '<b>Chi tiet:</b>' + NL
        + comp_lines
        + (NL + '<b>Canh bao:</b>' + NL + pen_lines if pen_lines else '')
        + NL + f'<i>{label}</i>' + NL
        + note_line
        + 'Dung /analyze ' + sym + ' de xem phan tich day du'
    )
    return msg


def _fmt_momentum_signal(data):
    """Inline display của Momentum Signal trong /analyze và /score."""
    ms = data.get('momentum_signal', {})
    if not ms:
        return ''
    NL    = chr(10)
    grade = ms.get('grade', '')
    score = ms.get('score', 0)
    pct52 = ms.get('pct52w', 0)
    pens  = ms.get('penalties', [])

    # Tier 1 không pass — show brief reason
    if not ms.get('tier1_pass'):
        return NL + '&#x26AA; <b>ML:</b> Chưa qua Tier 1 (Price&lt;MA50 hoặc Vol&lt;1.2x) | /ml để xem chi tiết'

    # Tier 2 pass/strong
    if grade == 'STRONG':
        icon  = '&#x1F525;&#x1F4CA;'
        label = f'<b>Strong Momentum Leader</b> {score}/120'
    elif grade == 'PASS':
        icon  = '&#x1F4C8;'
        label = f'Momentum Signal {score}/120'
    else:
        # Tier 1 pass nhưng Tier 2 chưa đủ
        return NL + f'&#x26AA; <b>ML:</b> Tier 1 ✓ | Score {score}/120 (cần &gt;=75) | /ml để xem chi tiết'

    pen_str = (' | &#x26A0; ' + ' | '.join([p.split('(')[0].strip() for p in pens])) if pens else ''
    return NL + icon + ' ' + label + f' | 52W: {pct52:.0f}%' + pen_str + ' | /ml để xem chi tiết'


def handle_market_scan(chat_id):
    """Chạy market scan toàn sàn và gửi kết quả."""
    try:
        n_syms = len(__import__('market_scanner').HOSE_LIQUID)
    except Exception:
        n_syms = 250
    send('&#x1F4E1; <b>Market Scanner v2</b> đang quét ~' + str(n_syms) + ' ma...' + chr(10)
         + 'Loc 3 tang: ADTV&gt;5ty | Giá&gt;MA50&gt;MA200 | RSI&lt;70 | ScoreA&gt;=60' + chr(10)
         + 'Vui long cho 3-5 phut.', chat_id)

    def run():
        import time as _time
        t0 = _time.time()
        try:
            import sys, os, importlib
            bot_dir = os.path.dirname(os.path.abspath(__file__))
            if bot_dir not in sys.path:
                sys.path.insert(0, bot_dir)
            import market_scanner as ms
            importlib.reload(ms)

            # Dùng call_api để tận dụng cache Flask, tránh đốt rate limit
            # scan_symbol_via_api: lấy data từ /api/analyze thay vì gọi Vnstock trực tiếp
            def scan_via_api(sym):
                """Dùng /api/analyze để lấy data đã cache sẵn."""
                data = call_api('/api/analyze/' + sym)
                if not data or 'score' not in data:
                    return None
                price   = data.get('price', 0)
                score_a = data.get('score', 0)
                adtv_b  = data.get('vol_today', 0) * price / 1e9 if price else 0
                ma50    = data.get('ma50', 0)
                ma200   = data.get('ma200', 0)
                rs20    = data.get('rs_20d') or 0
                rsi_val = data.get('rsi', 50)
                action  = data.get('action', 'THEO_DOI')
                b52     = data.get('breakout_52w', False)
                rs_bon  = data.get('rs_bonus', 0)
                # Áp lọc 3 tầng
                if price < 10000: return None
                if adtv_b < 5.0:  return None
                if ma50 <= 0 or price < ma50 * 0.98: return None
                if ma200 <= 0 or ma50 < ma200 * 0.98: return None
                if ma50 > 0 and price > ma50 * 1.15: return None
                if rsi_val > 70: return None
                if rs20 < -2.0:  return None
                if score_a < 60: return None
                pct_ma50 = round((price/ma50-1)*100, 1) if ma50 else 0
                return {
                    'symbol': sym, 'price': price, 'score_a': score_a,
                    'action': action, 'rs_5d': data.get('rs_5d', 0),
                    'rs_20d': rs20, 'rs_bonus': rs_bon,
                    'score_total': score_a + rs_bon,
                    'adtv_b': round(adtv_b, 1), 'ma50': ma50, 'ma200': ma200,
                    'rsi': rsi_val, 'pct_above_ma50': pct_ma50,
                    'breakout_52w': b52, 'breakout_60d': data.get('breakout_60d', False),
                    'rs_emoji': data.get('rs_emoji', ''),
                    'rs_label': data.get('rs_label', ''),
                    'ready_to_buy': (score_a >= 65 and action == 'MUA'),
                }

            # Quét tuần tự với delay nhỏ để không đốt rate limit
            from config import SIGNALS_WATCHLIST
            watchlist_syms = list(SIGNALS_WATCHLIST)
            # extra_syms: mã bổ sung ngoài SIGNALS_WATCHLIST cho market scan
            # Cập nhật đợt 10-11: xóa VPB/MBB/DCM/ACB/TCB/GAS/VHM/REE/STB/PNJ (đã loại ML)
            # extra_syms: mã bổ sung ngoài SIGNALS_WATCHLIST cho market scan
            # TPB đã loại vĩnh viễn (Sprint cũ)
            extra_syms = ['VCB','BID','CTG','FPT',
                          'SSI','VND','HCM','DGC','HSG','NKG',
                          'HDB','MWG','FRT','POW']
            all_syms = list(dict.fromkeys(watchlist_syms + extra_syms))

            results = []
            total   = len(all_syms)
            import concurrent.futures

            def _scan_one(sym):
                try:
                    return scan_via_api(sym)
                except Exception:
                    return None

            # Parallel scan - 6 threads, tránh quá tải Flask (max ~10 concurrent)
            done = [0]
            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
                futures = {executor.submit(_scan_one, sym): sym for sym in all_syms}
                for fut in concurrent.futures.as_completed(futures):
                    done[0] += 1
                    r = fut.result()
                    if r:
                        results.append(r)
                    if done[0] % 10 == 0:
                        pct = int(done[0] / total * 100)
                        send('&#x23F3; ' + str(done[0]) + '/' + str(total) + ' ma (' + str(pct) + '%)...', chat_id)

            results.sort(key=lambda x: (
                x.get('breakout_52w', False),
                x.get('score_total', 0),
                x.get('rs_20d', 0),
            ), reverse=True)
            results = results[:10]

            elapsed = _time.time() - t0
            msg = ms.format_scan_msg(results, scan_time_sec=elapsed)
            broadcast(msg)
        except Exception as e:
            logger.error('handle_market_scan: ' + str(e))
            import traceback
            logger.error(traceback.format_exc())
            send('&#x274C; Loi Market Scanner: ' + str(e)[:120], chat_id)

    threading.Thread(target=run, daemon=True).start()


def build_analysis_msg(data, prefix='Phân tích', b_ctx=None):
    sym = data.get('symbol', '')
    price = data.get('price', 0)
    score = data.get('score', 50)
    action = data.get('action', 'THEO DOI')

    # ── Tính B-filter penalty nếu có ─────────────────────────────────────
    b_delta    = 0
    b_details  = []
    b_overall  = ''
    if b_ctx:
        b_delta, _b_flags, b_details = _mc.calc_b_adjustment(b_ctx)
        b_overall = b_ctx.get('overall', '')

    score_adj = max(0, min(100, score + b_delta))
    if b_delta != 0:
        if   score_adj >= 65: action = 'MUA'
        elif score_adj <= 35: action = 'BAN'
        else:                 action = 'THEO DOI'
    ae = action_emoji(action)
    sigs = data.get('signals', [])
    ichi = data.get('ichimoku', {})
    sups = data.get('supports', [])
    ress = data.get('resistances', [])
    div = data.get('rsi_divergence', {})
    vr = data.get('vol_ratio', 1.0)
    tio = data.get('three_in_one', False)
    gc = data.get('golden_cross', False)
    dc = data.get('death_cross', False)

    vol_bar = '&#x1F525;' if vr >= 1.5 else ('⬆' if vr >= 1.0 else ('➡' if vr >= 0.7 else '⬇'))
    ct = ichi.get('cloud_top', 0)
    cb = ichi.get('cloud_bottom', 0)
    if price > ct:
        ichi_s = 'Tren may (tang)'
    elif price < cb:
        ichi_s = 'Duoi may (giam)'
    else:
        ichi_s = 'Trong may'

    sup_txt = ', '.join(f'{s["price"]:,.0f}({s["count"]}x)' for s in sups[:2]) if sups else ''
    res_txt = ', '.join(f'{r["price"]:,.0f}({r["count"]}x)' for r in ress[:2]) if ress else ''

    div_line = ''
    if div.get('type') != 'none' and div.get('message'):
        div_line = '\n\n<b>PHAN KY RSI:</b>\n ' + escape_html(div['message'])

    tio_line = ''
    if tio:
        tio_line = '\n\nHOI TU 3-TRONG-1: Giá trên MA20 + Vol đột biến + RSI hop le -&gt; Du dieu kien'

    rsi_lines  = get_group(sigs, 'RSI')
    div_lines  = get_group(sigs, 'DIV')
    macd_lines = get_group(sigs, 'MACD')
    ma_lines   = get_group(sigs, 'MA')
    bb_lines   = get_group(sigs, 'BB')
    vol_lines  = get_group(sigs, 'VOL')
    ichi_lines = get_group(sigs, 'ICHI')
    sr_lines   = get_group(sigs, 'SR')
    weekly_lines = get_group(sigs, '1W')
    ma10_lines = get_group(sigs, 'MA10')
    ma50_lines = get_group(sigs, 'MA50')

    # Weekly trend summary line cho header
    wt       = data.get('weekly_trend', '')
    wt_vn    = escape_html(data.get('weekly_trend_vn', ''))
    ma100    = data.get('ma100', 0)
    ma200    = data.get('ma200', 0)
    wt_emoji = {'STRONG_UP': '🟢', 'UP': '🟢',
                'WEAK_UP': '🟡', 'PULLBACK': '🟡',
                'DOWN': '🔴'}.get(wt, '❓')

    # MA10/MA50 horizon summary
    ma10_val      = data.get('ma10', 0)
    ma10_cross_up = data.get('ma10_cross_up', False)
    above_ma10    = data.get('above_ma10', False)
    above_ma50    = data.get('above_ma50', False)
    ma50_slope_up = data.get('ma50_slope_up', False)

    # Icon MA10 cross — nổi bật khi vừa cắt lên (giống vol spike)
    ma10_cross_line = ''
    if ma10_cross_up:
        ma10_cross_line = '\n⚡ <b>MA10 CROSS UP</b> — Giá vua cat len MA10 hom nay!'

    msg = (
            '<b>' + prefix + ' ' + sym + '</b>\n'
            + '=' * 30 + '\n'
            + 'Giá: <b>' + f'{price:,.0f}' + 'd</b>'
            + ' Diem A: <b>' + str(score) + '/100</b>'
            + (' → A+B: <b>' + str(score_adj) + '/100</b>'
               + (' (+' if b_delta > 0 else ' (') + str(b_delta) + 'd)'
               if b_delta != 0 else '')
            + ' ' + ae + tio_line + div_line + ma10_cross_line
            + (_fmt_shark_inline(data.get('shark_score', 0)) if data.get('shark_score', 0) >= 40 else '')
            + (_fmt_rs_inline(data) if data.get('rs_20d') is not None else '')
            + _fmt_regime_inline(data) + _fmt_vwap(data) + _fmt_foreign(data) + _fmt_momentum_signal(data) + _fmt_sector_rs(data) + _fmt_sprint4(data)
            + '\n\n'
            + '<b>1. RSI(14)</b>\n' + (rsi_lines or ' -&gt; Trung tính') + '\n\n'
            + '<b>2. RSI Phân kỳ</b>\n' + (div_lines or ' -&gt; Không phat hien phan ky') + '\n\n'
            + '<b>3. MACD</b>\n'
            + ' Line:' + f'{data.get("macd", 0):+.0f}' + ' Sig:' + f'{data.get("macd_signal", 0):+.0f}\n' + (macd_lines or '') + '\n\n'
            + '<b>4. MA10 / MA20 / MA50</b>\n'
            + ' MA10:' + f'{ma10_val:,.0f}' + ' MA20:' + f'{data.get("ma20", 0):,.0f}' + ' MA50:' + f'{data.get("ma50", 0):,.0f}\n'
            + (ma10_lines or '') + '\n'
            + (ma50_lines or '') + '\n'
            + (ma_lines or '') + '\n\n'
            + '<b>5. Volume (Dòng tiền)</b>\n'
            + ' Hom nay:' + fmt_vol(data.get('vol_today', 0)) + ' TB20:' + fmt_vol(data.get('vol_tb20', 0)) + '\n'
            + (vol_lines or '') + '\n'
            + _vol_time_note(vr) + '\n\n'
            + '<b>6. Hỗ trợ / Kháng cự</b>\n'
            + ' HT: ' + (sup_txt or '(chua xac dinh)') + '\n'
            + ' KC: ' + (res_txt or '(chua xac dinh)') + '\n'
            + (sr_lines or '') + '\n'
            + ' <i>BB: ' + f'{data.get("bb_lower", 0):,.0f}' + '–' + f'{data.get("bb_upper", 0):,.0f}'
            + (' | ' + bb_lines.strip() if bb_lines and bb_lines.strip() else '') + '</i>\n'
            + ' <i>Ichimoku: May ' + f'{cb:,.0f}' + '–' + f'{ct:,.0f}' + ' | ' + ichi_s
            + (' | TK:' + f'{ichi.get("tenkan",0):,.0f}' + ' KJ:' + f'{ichi.get("kijun",0):,.0f}' if ichi.get('tenkan') else '') + '</i>\n'
            + ((' <i>' + ichi_lines.strip() + '</i>\n') if ichi_lines and ichi_lines.strip() else '')
            + '\n'
            + '<b>7. Xu hướng Tuan (1W)</b>\n'
            + ' MA10W~MA100D:' + f'{ma100:,.0f}' + ' MA20W~MA200D:' + f'{ma200:,.0f}' + '\n'
            + ' ' + wt_emoji + ' ' + wt_vn + '\n'
            + (weekly_lines or '') + '\n\n'
            + _format_1h_warnings(data.get('warnings_1h', []))
            + _format_fair_value(data.get('fair_value', {}), data.get('action', ''), data.get('score', 50))
            + _fmt_wf_summary(data) + '\n'
            + _fmt_foreign(data) + '\n'
            + '<b>KẾT LUẬN</b>\n'
            + _build_conclusion(score, score_adj, b_delta, b_details,
                                b_overall, action, ae, data)
    )
    # Thêm conviction block vào cuối
    _conv_block, _, _ = _build_conviction_block(data, score_adj=score_adj)
    msg += _conv_block

    # Thêm score breakdown inline
    _breakdown = _fmt_score_breakdown_inline(data)
    if _breakdown:
        msg += _breakdown

    return msg


def handle_bt(args, chat_id):
    """
    /bt <MA>                     — Backtest compact
    /bt <MA> full                — Full + B-filter
    /bt <MA> s=60 sl=5 tp=20     — Custom score/SL/TP
    /bt <MA> s=55 sl=7 tp=20 hold=7 — Full custom
    /bt all                      — Toàn watchlist
    """
    import sys, os, re
    bot_dir = os.path.dirname(os.path.abspath(__file__))
    if bot_dir not in sys.path:
        sys.path.insert(0, bot_dir)

    raw_parts = args.split()
    # Tách keyword params (s=, sl=, tp=, hold=) ra khỏi positional parts
    kw, pos = {}, []
    for p in raw_parts:
        m = re.match(r'^(s|sl|tp|hold)=([\d.]+)$', p.lower())
        if m:
            kw[m.group(1)] = float(m.group(2))
        else:
            pos.append(p.upper())

    symbol  = pos[0] if pos else ''
    is_full = 'FULL' in pos
    is_all  = symbol == 'ALL'

    # Custom params
    custom_score = int(kw['s'])    if 's'    in kw else None
    custom_sl    = kw['sl'] / 100  if 'sl'   in kw else None
    custom_tp    = kw['tp'] / 100  if 'tp'   in kw else None
    custom_hold  = int(kw['hold']) if 'hold' in kw else None
    has_custom   = bool(kw)

    if not symbol:
        send(
            '&#x1F4CA; <b>Lenh /bt — Backtest</b>\n\n'
            '<b>Cu phap chuan:</b>\n'
            ' /bt MBB        — Backtest compact (~3 phut)\n'
            ' /bt MBB full   — Full + B-filter (~7 phut)\n'
            ' /bt all        — Toàn watchlist (~15 phut)\n\n'
            '<b>Cu phap tuy chinh:</b>\n'
            ' /bt DGC s=60           — Score threshold = 60\n'
            ' /bt DGC sl=5 tp=20     — SL=5% TP=20%\n'
            ' /bt DGC s=55 sl=7 tp=20 hold=7 — Full custom\n\n'
            '<b>Giái thich:</b>\n'
            ' s=   Score threshold (mặc định 65)\n'
            ' sl=  Stop Loss % (mặc định 7)\n'
            ' tp=  Take Profit % (mặc định 14)\n'
            ' hold= So phien giu lệnh (mặc định 10)',
            chat_id
        )
        return

    if is_all and not has_custom:
        # All watchlist — chạy trong thread
        import threading
        threading.Thread(
            target=lambda c=chat_id, f=is_full: _handle_bt_all(c, f),
            daemon=True
        ).start()
        return

    # Single symbol
    import threading
    threading.Thread(
        target=lambda s=symbol, c=chat_id, f=is_full,
                       cs=custom_score, csl=custom_sl,
                       ctp=custom_tp, ch=custom_hold:
            _handle_bt_symbol(s, c, f, cs, csl, ctp, ch),
        daemon=True
    ).start()


def _fmt_verdict(wr, pnl, pf, n, ci_lo=0):
    """Trả về (icon, text) đánh giá tổng thể."""
    if n < 20:
        return '&#x26A0;', 'IT LENH — Chua du de ket luan tin cay'
    if wr >= 60 and pnl >= 2 and pf >= 1.8:
        return '&#x2705;', 'TIN CAY CAO'
    elif wr >= 55 and pnl >= 1 and pf >= 1.3:
        return '&#x1F7E1;', 'CHAP NHAN — On nhung chua xuat sac'
    elif wr >= 50 and pnl >= 0:
        return '&#x1F7E1;', 'TRUNG BINH — Chỉ tham khảo'
    else:
        return '&#x274C;', 'KEM HIEU QUA — Nen xem lai'


def _fmt_decay(decay):
    if decay <= 5:   return '&#x2705; Rat on dinh'
    if decay <= 10:  return '&#x1F7E2; On dinh'
    if decay <= 20:  return '&#x1F7E1; Chap nhan'
    if decay <= 30:  return '&#x26A0; Cảnh báo overfit'
    return '&#x274C; Co the overfit nghiem trong'


def _handle_bt_symbol(symbol, chat_id, full_mode=False, custom_score=None, custom_sl=None, custom_tp=None, custom_hold=None):
    """Chạy BT+WF cho 1 mã, gửi output compact."""
    custom_parts = []
    if custom_score is not None: custom_parts.append(f's={custom_score}')
    if custom_sl    is not None: custom_parts.append(f'sl={int(custom_sl*100)}%')
    if custom_tp    is not None: custom_parts.append(f'tp={int(custom_tp*100)}%')
    if custom_hold  is not None: custom_parts.append(f'hold={custom_hold}p')
    custom_label = ' [' + ' '.join(custom_parts) + ']' if custom_parts else ''
    mode_txt = ' (full mode)' if full_mode else ''
    eta = '~7 phut' if full_mode else '~3 phut'
    send(
        '&#x1F504; Đang chạy <b>Backtest + Walk-Forward ' + symbol + '</b>'
        + custom_label + mode_txt + '\n'
        + '<i>' + eta + ', vui lòng chờ...</i>',
        chat_id
    )

    def run():
        try:
            import sys, os, importlib   # FIX: import trong thread, không dùng scope ngoài
            bot_dir = os.path.dirname(os.path.abspath(__file__))
            if bot_dir not in sys.path:
                sys.path.insert(0, bot_dir)
            import backtest as bt

            # ── BACKTEST ─────────────────────────────────────────────────────
            # Load data 1 lần, tái dùng cho BT + WF + Optimize
            _df_shared, _ = bt.load_data(symbol)

            res       = bt.run_backtest_symbol(symbol, verbose=False, use_regime=False,
                            sl=custom_sl, tp=custom_tp,
                            hold_days=custom_hold, min_score=custom_score,
                            _df_cache=_df_shared)  # baseline
            res_regime= bt.run_backtest_symbol(symbol, verbose=False, use_regime=None,
                            sl=custom_sl, tp=custom_tp,
                            hold_days=custom_hold, min_score=custom_score,
                            _df_cache=_df_shared)   # per-symbol config
            if not res or not res.get('buy'):
                send('&#x274C; ' + symbol + ': Không đủ dữ liệu hoặc không co lệnh MUA.', chat_id)
                return

            buy   = res['buy']
            cfg_sl    = custom_sl    if custom_sl    is not None else res.get('sl', 0.07)
            cfg_tp    = custom_tp    if custom_tp    is not None else res.get('tp', 0.14)
            cfg_score = custom_score if custom_score is not None else res.get('min_score', 65)
            cfg_hold  = custom_hold  if custom_hold  is not None else res.get('days', 10)
            wr    = buy.get('win_rate', 0)
            pnl   = buy.get('avg_pnl', 0)
            pf    = buy.get('profit_factor', 0)
            n     = buy.get('total', 0)
            tp_   = buy.get('tp', 0)
            sl_   = buy.get('sl', 0)
            hk_   = buy.get('expired', 0)
            aw    = buy.get('avg_win', 0)
            al    = buy.get('avg_loss', 0)
            pf_s  = f'{pf:.2f}' if pf != float('inf') else '&#x221E;'

            # CI 95%
            conf  = res.get('conf', {})
            ci_lo = conf.get('ci_low', 0)
            ci_hi = conf.get('ci_high', 100)

            v_icon, v_txt = _fmt_verdict(wr, pnl, pf, n, ci_lo)

            # Bảng năm — compact 1 dòng/năm
            yr_data = res.get('yearly', {}).get('yearly', {})
            yr_lines = ''
            PHASE_SHORT = {
                2020:'Covid', 2021:'Bull+130%', 2022:'Bear-50%',
                2023:'Phuc hoi', 2024:'On dinh', 2025:'Bien dong', 2026:'2026'
            }
            for yr in sorted(yr_data.keys()):
                if yr == 0: continue
                d = yr_data[yr]
                yr_wr  = d.get('win_rate', 0)
                yr_pnl = d.get('avg_pnl', 0)
                yr_n   = d.get('total', 0)
                if yr_n < 5: continue  # Bỏ qua năm có < 5 lệnh (không đủ ý nghĩa thống kê)
                icon = '&#x2705;' if yr_wr >= 60 else ('&#x1F7E1;' if yr_wr >= 50 else '&#x274C;')
                ph   = PHASE_SHORT.get(yr, str(yr))
                yr_lines += (
                    f' {icon} <b>{yr}</b> ({ph}): '
                    f'WR={yr_wr:.0f}% PnL={yr_pnl:+.1f}% '
                    f'({yr_n}L){" ⚠" if yr_n < 10 else ""}\n'
                )

            bull_bias   = res.get('yearly', {}).get('bull_bias', 'N/A')
            consistency = res.get('yearly', {}).get('consistency', '')

            # Ngưỡng tối ưu
            best_thr   = res.get('thresh', {}).get('best_threshold', cfg_score)
            thr_note   = (f'Ngưỡng hiện tại ({cfg_score}) la tối ưu &#x2713;'
                         if best_thr == cfg_score
                         else f'Ngưỡng tối ưu la <b>{best_thr}</b> (hiện tại {cfg_score})')

            # 3 lệnh gần nhất
            recent = ''
            trades_df = res.get('trades')
            if trades_df is not None and len(trades_df) > 0:
                buy_df = trades_df[trades_df['action'] == 'MUA'].tail(3)
                for _, row in buy_df.iterrows():
                    icon = '&#x2705;' if row['pnl'] > 0 else '&#x274C;'
                    recent += (
                        f' {icon} {row["date"]} @{row["price"]:,.0f}d '
                        f'S={row["score"]} → {row["pnl"]:+.1f}% ({row["reason"]})\n'
                    )

            msg_bt = (
                '&#x1F4CA; <b>BACKTEST ' + symbol + ' (7 NAM)</b>\n'
                + '&#x3D;' * 28 + '\n\n'

                + '<b>Tổng quan:</b>\n'
                + f' Lenh: {n} | TP: {tp_} | SL: {sl_} | HK: {hk_}\n'
                + f' WR: <b>{wr}%</b> | PnL TB: <b>{pnl:+.2f}%</b>\n'
                + f' PF: <b>{pf_s}</b> | Thang TB: {aw:+.1f}% | Thua TB: {al:+.1f}%\n'
                + f' CI 95%: [{ci_lo}% – {ci_hi}%]\n\n'

                + '<b>Kết quả theo năm:</b>\n'
                + (yr_lines or ' (không co du lieu)') + '\n'

                + (f'&#x26A0; Bull Bias: <b>{bull_bias}</b>\n' if bull_bias not in ('KHÔNG', 'N/A', '') else '')
                + (f'&#x1F4CC; {consistency[:80]}\n' if consistency else '')
                + '\n'

                + '<b>Ngưỡng score:</b> ' + thr_note + '\n'
                + f' SL=-{cfg_sl*100:.0f}% TP=+{cfg_tp*100:.0f}% | Score&gt;={cfg_score}\n\n'

                + ('<b>3 lệnh MUA gan nhat:</b>\n' + recent + '\n' if recent else '')

                + v_icon + ' <b>' + v_txt + '</b>\n'
                + '<i>Chua tinh phi GD ~0.3%. QK không dam bao TL.</i>'
            )
            # Thêm regime compare block vào cuối msg_bt
            msg_bt += chr(10) + _fmt_regime_compare(res, res_regime)
            send(msg_bt, chat_id)

            # ── WALK-FORWARD ─────────────────────────────────────────────────
            send('&#x1F504; Đang chạy <b>Walk-Forward</b> ' + symbol + '...', chat_id)
            wf = bt.run_walk_forward(symbol, verbose=False, _df_cache=_df_shared)

            # FIX: Nếu WF fail do ít rows (<400), thử reload với days lớn hơn
            if not wf and _df_shared is not None and len(_df_shared) < 400:
                send('&#x23F3; Dữ liệu ít (' + str(len(_df_shared)) + ' rows), đang tải thêm...', chat_id)
                _df_extra, _ = bt.load_data(symbol, days=3650)  # thử 10 năm
                if _df_extra is not None and len(_df_extra) > len(_df_shared):
                    wf = bt.run_walk_forward(symbol, verbose=False, _df_cache=_df_extra)

            if not wf:
                rows_info = str(len(_df_shared)) + ' rows' if _df_shared is not None else 'N/A'
                send('&#x26A0; ' + symbol + ': Không đủ dữ liệu Walk-Forward (' + rows_info + ').\n'
                     '<i>WF cần tối thiểu 400 nến (~1.5 năm). vnstock Community giới hạn 60 req/phút.\n'
                     'Thử lại sau 1-2 phút hoặc dùng /bt ' + symbol + ' để xem BT đơn.</i>', chat_id)
            else:
                avg_is  = wf['avg_is_wr']
                avg_oos = wf['avg_oos_wr']
                is_pnl  = wf['avg_is_pnl']
                oos_pnl = wf['avg_oos_pnl']
                decay   = wf['decay_wr']
                verdict = wf['verdict']
                vtxt    = wf['verdict_txt']
                thrs    = wf['thresholds']
                stable  = wf['thr_stable']

                wf_icon = {'V':'&#x2705;','~':'&#x1F7E1;','!':'&#x26A0;'}.get(verdict, '&#x274C;')

                # Bảng cửa sổ compact
                win_lines = ''
                for w in wf['windows']:
                    if w['oos_wr'] is None: continue
                    dw = w['is_wr'] - w['oos_wr']
                    fi = '&#x2705;' if dw<=5 else ('&#x1F7E1;' if dw<=15 else ('&#x26A0;' if dw<=25 else '&#x274C;'))
                    win_lines += (
                        f' {fi} OOS <b>{w["oos_label"]}</b> (thr&gt;={w["best_thr"]}): '
                        f'IS={w["is_wr"]:.0f}% → OOS=<b>{w["oos_wr"]:.0f}%</b> '
                        f'PnL={w["oos_pnl"]:+.1f}% decay={dw:+.0f}%\n'
                    )

                thr_s = ('&#x2705; Ngưỡng ổn định: ' if stable else '&#x26A0; Ngưỡng biến động: ') + str(thrs)
                decay_txt = _fmt_decay(decay)

                msg_wf = (
                    '&#x1F504; <b>WALK-FORWARD ' + symbol + '</b>\n'
                    + f'IS=2nam → OOS=1nam | SL={wf["sl"]*100:.0f}% TP={wf["tp"]*100:.0f}%\n'
                    + '&#x3D;' * 28 + '\n\n'
                    + win_lines + '\n'
                    + f'<b>Tong ket:</b>\n'
                    + f' IS  TB: WR={avg_is:.1f}% PnL={is_pnl:+.2f}%\n'
                    + f' OOS TB: WR=<b>{avg_oos:.1f}%</b> PnL=<b>{oos_pnl:+.2f}%</b>\n'
                    + f' Decay: <b>{decay:+.1f}%</b> — {decay_txt}\n'
                    + f' {thr_s}\n\n'
                    + wf_icon + ' <b>' + vtxt + '</b>\n'
                    + '<i>Decay thap = he thong robust, it overfit.</i>'
                )
                send(msg_wf, chat_id)

            # ── FULL MODE: Entry bias + B-filter ─────────────────────────────
            if full_mode:
                # Entry bias T vs T+1
                send('&#x1F504; Dang kiem tra <b>Entry Bias</b> ' + symbol + '...', chat_id)
                dual = bt.run_backtest_dual(symbol, verbose=False)
                if dual:
                    mt   = dual['mode_T']
                    mt1  = dual['mode_T1']
                    wd   = dual['wr_diff']
                    pd_  = dual['pnl_diff']
                    flag = dual['bias_flag']
                    rec  = dual['recommend']
                    fi   = {'V':'&#x2705;','~':'&#x1F7E1;','!':'&#x26A0;'}.get(flag, '&#x274C;')
                    msg_dual = (
                        '&#x1F522; <b>ENTRY BIAS ' + symbol + '</b>\n'
                        + f' Entry T  : WR={mt["wr"]:.1f}% PnL={mt["pnl"]:+.2f}% ({mt["n"]}L)\n'
                        + f' Entry T+1: WR=<b>{mt1["wr"]:.1f}%</b> PnL=<b>{mt1["pnl"]:+.2f}%</b>\n'
                        + f' Chenh lech: WR={wd:+.1f}% PnL={pd_:+.2f}%\n'
                        + f' {fi} {rec}'
                    )
                    send(msg_dual, chat_id)

                # B-filter check
                send('&#x1F504; Đang chạy <b>B-Filter Check</b> ' + symbol + '...', chat_id)
                bf = bt.run_b_filter_comparison(symbol, verbose=False)
                if bf:
                    ma  = bf['mode_A']
                    mab = bf['mode_AB']
                    wd  = bf['wr_diff']
                    pd_ = bf['pnl_diff']
                    nf  = bf['n_filtered']
                    fi  = {'V':'&#x2705;','~':'&#x1F7E1;','-':'&#x27A1;','!':'&#x26A0;'}.get(bf['flag'], '&#x274C;')
                    msg_bf = (
                        '&#x1F6E1; <b>B-FILTER CHECK ' + symbol + '</b>\n'
                        + f' Mode A   (KT thuan): WR={ma["wr"]:.1f}% PnL={ma["pnl"]:+.2f}% ({ma["n"]}L)\n'
                        + f' Mode A+B (+ BFilter): WR=<b>{mab["wr"]:.1f}%</b> PnL=<b>{mab["pnl"]:+.2f}%</b>\n'
                        + f' Chenh lech: WR={wd:+.1f}% PnL={pd_:+.2f}% | Loc: {nf}L\n'
                        + f' {fi} {bf["verdict"]}'
                    )
                    send(msg_bf, chat_id)

        except Exception as e:
            logger.error(f'_handle_bt_symbol {symbol}: {e}')
            import traceback
            logger.error(traceback.format_exc())
            send('&#x274C; Lỗi khi chạy BT ' + symbol + ': ' + str(e)[:120], chat_id)

    threading.Thread(target=run, daemon=True).start()


def _handle_bt_all(full_mode, chat_id):
    """Chạy BT cho toàn bộ watchlist, tóm tắt 1 dòng/mã."""
    syms = list(WATCHLIST_META.keys())
    mode_txt = ' (full)' if full_mode else ''
    eta = '~30 phut' if full_mode else '~15 phut'

    send(
        '&#x1F4CA; <b>Backtest ' + str(len(syms)) + ' ma watchlist' + mode_txt + '</b>\n'
        '<i>' + eta + ' — gui ket qua tung ma khi xong...</i>',
        chat_id
    )

    def run():
        try:
            import sys, os, importlib   # FIX: import trong thread, không dùng scope ngoài
            bot_dir = os.path.dirname(os.path.abspath(__file__))
            if bot_dir not in sys.path:
                sys.path.insert(0, bot_dir)
            import backtest as bt

            summary_rows = []
            for sym in syms:
                try:
                    res = bt.run_backtest_symbol(sym, verbose=False)
                    wf  = bt.run_walk_forward(sym, verbose=False)
                    if not res or not res.get('buy'):
                        send(f'&#x26A0; {sym}: khong du du lieu', chat_id)
                        continue

                    buy       = res['buy']
                    wr        = buy.get('win_rate', 0)
                    pnl       = buy.get('avg_pnl', 0)
                    n         = buy.get('total', 0)
                    oos_wr    = wf['avg_oos_wr'] if wf else None
                    decay     = wf['decay_wr']   if wf else None
                    best_thr  = res.get('thresh', {}).get('best_threshold', 65)
                    cfg_score = res.get('min_score', 65)
                    v_icon, v_txt = _fmt_verdict(wr, pnl, buy.get('profit_factor',0), n)

                    # OOS icon
                    if oos_wr is None:
                        oos_s = 'N/A'
                        oos_icon = '&#x2753;'
                    else:
                        oos_s = f'{oos_wr:.0f}%'
                        oos_icon = '&#x2705;' if oos_wr >= 55 else ('&#x1F7E1;' if oos_wr >= 50 else '&#x274C;')

                    decay_s = f'{decay:+.0f}%' if decay is not None else 'N/A'
                    thr_flag = f' &#x2B50;{best_thr}' if best_thr != cfg_score else ''

                    line = (
                        f'{v_icon} <b>{sym}</b>: '
                        f'WR={wr:.0f}% OOS={oos_icon}{oos_s} '
                        f'PnL={pnl:+.1f}% decay={decay_s} ({n}L){thr_flag}'
                    )
                    send(line, chat_id)
                    summary_rows.append({
                        'sym': sym, 'wr': wr, 'oos_wr': oos_wr,
                        'pnl': pnl, 'decay': decay, 'n': n,
                        'v_icon': v_icon
                    })

                except Exception as e:
                    send(f'&#x274C; {sym}: {str(e)[:60]}', chat_id)

            # ── Tổng kết ─────────────────────────────────────────────────────
            if summary_rows:
                good = [r for r in summary_rows if r['wr'] >= 55 and r['pnl'] > 0]
                weak = [r for r in summary_rows if r['wr'] < 50 or r['pnl'] < 0]
                stable = [r for r in summary_rows
                          if r['decay'] is not None and r['decay'] <= 10]
                avg_wr  = sum(r['wr']  for r in summary_rows) / len(summary_rows)
                avg_pnl = sum(r['pnl'] for r in summary_rows) / len(summary_rows)

                summary = (
                    '\n&#x1F4CB; <b>TONG KET WATCHLIST</b>\n'
                    + '&#x3D;' * 28 + '\n'
                    + f' WR TB  : {avg_wr:.1f}%  |  PnL TB: {avg_pnl:+.1f}%\n'
                    + f' Tot ({len(good)}): ' + ' '.join(r["sym"] for r in good) + '\n'
                    + f' Yeu ({len(weak)}): ' + ' '.join(r["sym"] for r in weak) + '\n'
                    + f' Robust ({len(stable)}): ' + ' '.join(r["sym"] for r in stable) + '\n'
                    + '<i>Tot = WR&gt;=55% va PnL&gt;0 | Yeu = WR&lt;50% hoặc PnL&lt;0\n'
                    + 'Robust = decay WF &lt;= 10%</i>'
                )
                send(summary, chat_id)

        except Exception as e:
            logger.error(f'_handle_bt_all: {e}')
            import traceback
            logger.error(traceback.format_exc())
            send('&#x274C; Lỗi khi chạy BT all: ' + str(e)[:120], chat_id)

    threading.Thread(target=run, daemon=True).start()


# ─── MOMENTUM LEADER BACKTEST ─────────────────────────────────────────────────

def handle_mlbt(args, chat_id):
    """
    /mlbt <SYM>                  — Backtest Momentum Leader (SL=6% TP=17% Hold=18d)
    /mlbt <SYM> sl=5 tp=18       — Custom SL/TP
    /mlbt <SYM> hold=15 s=90     — Custom hold + chỉ lấy STRONG signal
    /mlbt all                    — Toàn watchlist BACKTEST_WATCHLIST
    """
    NL   = chr(10)
    args = [a.strip() for a in args if a.strip()]

    if not args:
        send(
            '&#x1F4CA; <b>Lệnh /mlbt — Backtest Momentum Leader</b>' + NL + NL
            + ' /mlbt HCM        — BT + WF cơ bản (~3 phút)' + NL
            + ' /mlbt SSI sl=5 tp=18  — Custom SL/TP' + NL
            + ' /mlbt MBB hold=15 s=90 — Chỉ STRONG signal' + NL
            + ' /mlbt all        — Toàn watchlist (~15 phút)' + NL + NL
            + '<i>Default: SL=6% TP=17% Hold=18d Score>=75</i>',
            chat_id
        )
        return

    # Parse args
    sym  = None
    c_sl = None; c_tp = None; c_hold = None; c_score = None

    for a in args:
        al = a.lower()
        if al == 'all':
            sym = 'all'
        elif al.startswith('sl='):
            try: c_sl = float(al[3:]) / 100
            except: pass
        elif al.startswith('tp='):
            try: c_tp = float(al[3:]) / 100
            except: pass
        elif al.startswith('hold='):
            try: c_hold = int(al[5:])
            except: pass
        elif al.startswith('s='):
            try: c_score = int(al[2:])
            except: pass
        elif sym is None and al not in ('full',):
            sym = a.upper()

    if not sym:
        send('Cú pháp: <b>/mlbt HCM</b> hoặc <b>/mlbt all</b>', chat_id)
        return

    if sym == 'all':
        threading.Thread(
            target=lambda c=chat_id, sl=c_sl, tp=c_tp, hold=c_hold, sc=c_score:
                _handle_mlbt_all(c, sl, tp, hold, sc),
            daemon=True
        ).start()
    elif sym == 'EXTENDED':
        threading.Thread(
            target=lambda c=chat_id, sl=c_sl, tp=c_tp, hold=c_hold, sc=c_score:
                _handle_mlbt_extended(c, sl, tp, hold, sc),
            daemon=True
        ).start()
    else:
        threading.Thread(
            target=lambda s=sym, c=chat_id, sl=c_sl, tp=c_tp, hold=c_hold, sc=c_score:
                _handle_mlbt_symbol(s, c, sl, tp, hold, sc),
            daemon=True
        ).start()


def _handle_mlbt_symbol(symbol, chat_id, custom_sl=None, custom_tp=None,
                         custom_hold=None, custom_score=None):
    """Chạy ML BT + WF cho 1 mã, gửi kết quả compact."""
    NL = chr(10)
    sl    = custom_sl    if custom_sl    is not None else 0.06
    tp    = custom_tp    if custom_tp    is not None else 0.17
    hold  = custom_hold  if custom_hold  is not None else 18
    score = custom_score if custom_score is not None else 75

    custom_parts = []
    if custom_sl    is not None: custom_parts.append(f'sl={int(sl*100)}%')
    if custom_tp    is not None: custom_parts.append(f'tp={int(tp*100)}%')
    if custom_hold  is not None: custom_parts.append(f'hold={hold}d')
    if custom_score is not None: custom_parts.append(f's={score}')
    label = ' [' + ' '.join(custom_parts) + ']' if custom_parts else ''

    send(
        '&#x1F504; Đang chạy <b>ML Backtest + Walk-Forward ' + symbol + '</b>'
        + label + NL + '<i>~3-5 phút, vui lòng chờ...</i>',
        chat_id
    )

    try:
        import sys, os
        bot_dir = os.path.dirname(os.path.abspath(__file__))
        if bot_dir not in sys.path:
            sys.path.insert(0, bot_dir)
        import backtest as bt

        # Load data 1 lần, tái dùng cho BT + WF
        _df_shared, _ = bt.load_data(symbol)

        # ── Backtest ──────────────────────────────────────────────────────────
        res = bt.run_backtest_momentum(
            symbol, sl=sl, tp=tp, hold_days=hold,
            min_ml_score=score, verbose=False, _df_cache=_df_shared
        )
        if not res or not res.get('buy'):
            send('&#x274C; ' + symbol + ': Không có tín hiệu ML nào. '
                 'Thử giảm score (vd: /mlbt ' + symbol + ' s=65)', chat_id)
            return

        buy   = res['buy']
        n     = buy['total']
        wr    = buy['win_rate']
        pnl   = buy['avg_pnl']
        pf    = buy['profit_factor']
        aw    = buy['avg_win']
        al    = buy['avg_loss']
        tp_   = buy['tp']
        sl_   = buy['sl']
        hk_   = buy['expired']
        ci_lo = buy['ci_low']
        ci_hi = buy['ci_high']
        avg_d = buy['avg_days']
        pf_s  = f'{pf:.2f}' if pf != float('inf') else '∞'

        v_icon, v_txt = _fmt_verdict(wr, pnl, pf, n, ci_lo)

        # Yearly table
        PHASE_SHORT = {
            2020:'Covid', 2021:'Bull+130%', 2022:'Bear-50%',
            2023:'Phuc hoi', 2024:'On dinh', 2025:'Bien dong', 2026:'2026'
        }
        yr_data  = res.get('yearly', {}).get('yearly', {})
        yr_lines = ''
        for yr in sorted(yr_data.keys()):
            if yr == 0: continue
            d = yr_data[yr]
            if d['total'] < 3: continue
            icon = '&#x2705;' if d['win_rate'] >= 60 else ('&#x1F7E1;' if d['win_rate'] >= 50 else '&#x274C;')
            ph   = PHASE_SHORT.get(yr, str(yr))
            warn = ' ⚠' if d['total'] < 8 else ''
            yr_lines += (f' {icon} <b>{yr}</b> ({ph}): '
                         f'WR={d["win_rate"]:.0f}% PnL={d["avg_pnl"]:+.1f}% ({d["total"]}L){warn}' + NL)

        # Grade breakdown: STRONG vs PASS
        grade_lines = ''
        grade_stats = res.get('grade_stats', {})
        for g in ['STRONG', 'PASS']:
            gs = grade_stats.get(g)
            if gs and gs['total'] >= 3:
                gp = f'{gs["profit_factor"]:.2f}' if gs["profit_factor"] != float('inf') else '∞'
                grade_lines += (f' [{g}] {gs["total"]}L WR={gs["win_rate"]:.0f}% '
                                f'PnL={gs["avg_pnl"]:+.2f}% PF={gp}' + NL)

        # 3 lệnh gần nhất
        recent = ''
        df_t = res.get('trades')
        if df_t is not None and len(df_t) > 0:
            for _, row in df_t.tail(3).iterrows():
                icon = '&#x2705;' if row['pnl'] > 0 else '&#x274C;'
                recent += (f' {icon} {row["date"]} @{row["price"]:,.0f}đ '
                           f'ML={row["ml_score"]} [{row["grade"]}] '
                           f'→ {row["pnl"]:+.1f}% ({row["reason"]}, {row["days"]}d)' + NL)

        msg_bt = (
            '&#x1F4CA; <b>ML BACKTEST ' + symbol + '</b>' + NL
            + '&#x3D;' * 28 + NL + NL
            + f'&#x2699; SL=-{sl*100:.0f}% TP=+{tp*100:.0f}% Hold={hold}d Score>={score}' + NL + NL

            + '<b>Tổng quan:</b>' + NL
            + f' Lệnh: {n} | TP: {tp_} | SL: {sl_} | Hết: {hk_}' + NL
            + f' WR: <b>{wr}%</b> [{ci_lo}–{ci_hi}%] | PnL TB: <b>{pnl:+.2f}%</b>' + NL
            + f' PF: <b>{pf_s}</b> | Thắng TB: {aw:+.1f}% | Thua TB: {al:+.1f}%' + NL
            + f' Hold TB: {avg_d:.0f} ngày' + NL + NL

            + ('<b>Theo Grade:</b>' + NL + grade_lines + NL if grade_lines else '')

            + '<b>Kết quả theo năm:</b>' + NL
            + (yr_lines or ' (không có dữ liệu)') + NL

            + ('<b>3 tín hiệu gần nhất:</b>' + NL + recent + NL if recent else '')

            + v_icon + ' <b>' + v_txt + '</b>' + NL
            + '<i>Chua tinh phi GD ~0.5%. QK không đảm bảo TL.</i>'
        )
        send(msg_bt, chat_id)

        # ── Walk-Forward ──────────────────────────────────────────────────────
        send('&#x1F504; Đang chạy <b>ML Walk-Forward</b> ' + symbol + '...', chat_id)
        wf = bt.run_walk_forward_momentum(
            symbol, sl=sl, tp=tp, hold_days=hold,
            min_ml_score=score, verbose=False
        )
        if not wf:
            send('&#x26A0; ' + symbol + ': Không đủ dữ liệu Walk-Forward ML.', chat_id)
            return

        # WF output
        v_wf  = wf['verdict']
        vt_wf = wf['verdict_txt']
        wf_icon = '&#x2705;' if v_wf == 'V' else ('&#x1F7E1;' if v_wf == '~' else '&#x274C;')
        decay_s = f'{wf["decay_wr"]:+.1f}%'

        win_lines = ''
        for w in wf['windows']:
            o_icon = '&#x2705;' if w['oos_wr'] >= 55 else ('&#x1F7E1;' if w['oos_wr'] >= 48 else '&#x274C;')
            win_lines += (
                f' {o_icon} OOS {w["oos_label"]}: {w["oos_n"]}L '
                f'IS {w["is_wr"]:.0f}% → OOS {w["oos_wr"]:.0f}% PnL={w["oos_pnl"]:+.2f}%' + NL
            )

        msg_wf = (
            '&#x1F4CA; <b>ML Walk-Forward ' + symbol + f'</b> ({len(wf["windows"])} windows)' + NL
            + '&#x3D;' * 28 + NL + NL
            + f' Avg IS:  WR={wf["avg_is_wr"]:.1f}% PnL={wf["avg_is_pnl"]:+.2f}%' + NL
            + f' Avg OOS: WR={wf["avg_oos_wr"]:.1f}% PnL={wf["avg_oos_pnl"]:+.2f}% (decay={decay_s})' + NL + NL
            + win_lines + NL
            + wf_icon + ' <b>' + vt_wf + '</b>' + NL
            + '<i>SL=' + f'{sl*100:.0f}% TP={tp*100:.0f}% Hold={hold}d Score>={score}</i>'
        )
        send(msg_wf, chat_id)

    except Exception as e:
        logger.error('handle_mlbt ' + symbol + ': ' + str(e))
        import traceback
        logger.error(traceback.format_exc())
        send('&#x274C; Lỗi ML Backtest ' + symbol + ': ' + str(e)[:120], chat_id)


def _handle_mlbt_all(chat_id, custom_sl=None, custom_tp=None,
                      custom_hold=None, custom_score=None):
    """Chạy ML BT cho toàn watchlist, tóm tắt 1 dòng/mã."""
    NL    = chr(10)
    sl    = custom_sl    if custom_sl    is not None else 0.06
    tp    = custom_tp    if custom_tp    is not None else 0.17
    hold  = custom_hold  if custom_hold  is not None else 18
    score = custom_score if custom_score is not None else 75

    from config import BACKTEST_WATCHLIST
    syms = list(BACKTEST_WATCHLIST)

    send(
        f'&#x1F4CA; <b>ML Backtest {len(syms)} mã watchlist</b>' + NL
        + f'<i>SL={sl*100:.0f}% TP={tp*100:.0f}% Hold={hold}d Score>={score}</i>' + NL
        + '<i>~15 phút — gửi kết quả khi xong...</i>',
        chat_id
    )

    try:
        import sys, os
        bot_dir = os.path.dirname(os.path.abspath(__file__))
        if bot_dir not in sys.path:
            sys.path.insert(0, bot_dir)
        import backtest as bt

        # FIX: Parallel 4 workers thay vì tuần tự → ~3-5 phút thay vì ~15 phút
        def _bt_one_all(sym):
            try:
                # RATE LIMIT FIX: sleep nhỏ trước mỗi mã để stagger vnstock calls
                # 2 workers × sleep 3s = tối đa ~20 req/phút thay vì burst
                time.sleep(3)
                res = bt.run_backtest_momentum(
                    sym, sl=sl, tp=tp, hold_days=hold,
                    min_ml_score=score, verbose=False
                )
                wf = bt.run_walk_forward_momentum(
                    sym, sl=sl, tp=tp, hold_days=hold,
                    min_ml_score=score, verbose=False
                )
                if not res or not res.get('buy') or res['buy']['total'] < 3:
                    return (sym, 3, f'&#x26AA; <b>{sym}</b>: ít tín hiệu (<3L)')
                buy     = res['buy']
                wr      = buy['win_rate']; pnl = buy['avg_pnl']; pf = buy['profit_factor']
                n       = buy['total']
                oos_wr  = wf['avg_oos_wr'] if wf else None
                pf_s    = f'{pf:.2f}' if pf != float('inf') else '∞'
                v_icon, _ = _fmt_verdict(wr, pnl, pf, n)
                oos_s   = f' OOS={oos_wr:.0f}%' if oos_wr else ''
                ml_cfg  = ML_CONFIRMED_WATCHLIST.get(sym)
                badge   = (' &#x2705;' if ml_cfg and ml_cfg[0] == 'A'
                           else ' &#x1F7E1;' if ml_cfg else '')
                sort_k  = 0 if (ml_cfg and ml_cfg[0]=='A') else 1 if ml_cfg else 2
                return (sym, sort_k,
                        f'{v_icon} <b>{sym}</b>{badge}: {n}L WR={wr:.0f}% '
                        f'PnL={pnl:+.2f}% PF={pf_s}{oos_s}')
            except Exception as ex:
                return (sym, 4, f'&#x274C; <b>{sym}</b>: {str(ex)[:60]}')

        import concurrent.futures as _cft
        raw = []
        # RATE LIMIT FIX: giảm workers 4→2 để tránh burst đồng thời với background scanner
        with _cft.ThreadPoolExecutor(max_workers=2) as ex:
            futs = {ex.submit(_bt_one_all, s): s for s in syms}
            for fut in _cft.as_completed(futs):
                raw.append(fut.result())

        raw.sort(key=lambda x: (x[1], x[0]))
        summary = [line for _, _, line in raw]

        # Gửi tóm tắt
        batch = ''
        for line in summary:
            batch += line + NL
            if len(batch) > 3000:
                send(batch, chat_id)
                batch = ''
        if batch:
            send(
                '&#x1F4CA; <b>ML BT Summary</b> (SL='
                + f'{sl*100:.0f}% TP={tp*100:.0f}% Hold={hold}d)' + NL + batch,
                chat_id
            )
    except Exception as e:
        logger.error('_handle_mlbt_all: ' + str(e))
        send('&#x274C; Lỗi ML BT all: ' + str(e)[:120], chat_id)



def _handle_mlbt_extended(chat_id, custom_sl=None, custom_tp=None,
                           custom_hold=None, custom_score=None):
    """
    Chạy ML BT cho toàn BACKTEST_WATCHLIST, tóm tắt 1 dòng/mã.
    FIX: Dùng BACKTEST_WATCHLIST thay vì WATCHLIST_EXTENDED.
         Mã đã loại không được quét nữa.
    FIX: Parallel 4 workers → giảm từ ~20-30 phút xuống ~5-8 phút.
    """
    NL    = chr(10)
    sl    = custom_sl    if custom_sl    is not None else 0.06
    tp    = custom_tp    if custom_tp    is not None else 0.17
    hold  = custom_hold  if custom_hold  is not None else 18
    score = custom_score if custom_score is not None else 75

    from config import BACKTEST_WATCHLIST
    syms = list(BACKTEST_WATCHLIST)

    send(
        f'&#x1F4CA; <b>ML Backtest EXTENDED — {len(syms)} mã</b>' + NL
        + f'<i>SL={sl*100:.0f}% TP={tp*100:.0f}% Hold={hold}d Score>={score}</i>' + NL
        + '<i>~5-8 phút (parallel) — gửi kết quả khi xong...</i>',
        chat_id
    )

    try:
        import sys, os, concurrent.futures
        bot_dir = os.path.dirname(os.path.abspath(__file__))
        if bot_dir not in sys.path:
            sys.path.insert(0, bot_dir)
        import backtest as bt

        def _bt_one(sym):
            try:
                res = bt.run_backtest_momentum(
                    sym, sl=sl, tp=tp, hold_days=hold,
                    min_ml_score=score, verbose=False
                )
                wf = bt.run_walk_forward_momentum(
                    sym, sl=sl, tp=tp, hold_days=hold,
                    min_ml_score=score, verbose=False
                )
                if not res or not res.get('buy') or res['buy']['total'] < 3:
                    return (sym, 3, f'&#x26AA; <b>{sym}</b>: ít tín hiệu (<3L)')
                buy     = res['buy']
                wr      = buy['win_rate']; pnl = buy['avg_pnl']; pf = buy['profit_factor']
                n       = buy['total']
                oos_wr  = wf['avg_oos_wr'] if wf else None
                pf_s    = f'{pf:.2f}' if pf != float('inf') else '∞'
                v_icon, _ = _fmt_verdict(wr, pnl, pf, n)
                oos_s   = f' OOS={oos_wr:.0f}%' if oos_wr else ''
                ml_cfg  = ML_CONFIRMED_WATCHLIST.get(sym)
                badge   = (' &#x2705;' if ml_cfg and ml_cfg[0] == 'A'
                           else ' &#x1F7E1;' if ml_cfg else '')
                # Sort key: Tier A=0, Tier B=1, theo doi=2, ít signal=3
                sort_k  = 0 if (ml_cfg and ml_cfg[0]=='A') else 1 if ml_cfg else 2
                return (sym, sort_k,
                        f'{v_icon} <b>{sym}</b>{badge}: {n}L WR={wr:.0f}% '
                        f'PnL={pnl:+.2f}% PF={pf_s}{oos_s}')
            except Exception as ex:
                return (sym, 4, f'&#x274C; <b>{sym}</b>: {str(ex)[:60]}')

        # Parallel backtest — 4 workers (CPU-bound, không cần nhiều hơn)
        raw = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(_bt_one, s): s for s in syms}
            for fut in concurrent.futures.as_completed(futs):
                raw.append(fut.result())

        # Sắp xếp: Tier A → Tier B → theo dõi → ít signal → lỗi
        raw.sort(key=lambda x: (x[1], x[0]))
        summary = [line for _, _, line in raw]

        # Gửi tóm tắt theo batch
        batch = ''
        for line in summary:
            batch += line + NL
            if len(batch) > 3000:
                send(batch, chat_id)
                batch = ''
        if batch:
            send(
                '&#x1F4CA; <b>ML BT Extended Summary</b> (SL='
                + f'{sl*100:.0f}% TP={tp*100:.0f}% Hold={hold}d)' + NL + batch,
                chat_id
            )
    except Exception as e:
        logger.error('_handle_mlbt_extended: ' + str(e))
        send('&#x274C; Lỗi ML BT extended: ' + str(e)[:120], chat_id)


# ═══════════════════════════════════════════════════════════════════════════════
# /scanstatus + /signal_scan + /vol_scan + /ma_scan + /ml_scan + /ext_scan + /shark_scan
# Scanner ON/OFF control
# ═══════════════════════════════════════════════════════════════════════════════

def handle_scanstatus(chat_id):
    """
    /scanstatus — Hiển thị trạng thái ON/OFF của từng scanner,
    kèm lệnh toggle theo đúng format cũ.
    """
    NL = chr(10)
    now_str = datetime.now(VN_TZ).strftime('%d/%m %H:%M')

    _cfg = [
        ('signal', '📡', 'Signal MUA/BAN',    f'{SCAN_INTERVAL_MIN} phút/lần',     '/signal_scan'),
        ('ma',     '📈', 'MA10/MA50 Cross',    f'{MA_SCAN_INTERVAL_MIN//60} tiếng/lần', '/ma_scan'),
        ('ml',     '🤖', 'ML Momentum',        f'{ML_SCAN_INTERVAL_MIN} phút/lần',  '/ml_scan'),
        ('ext',    '🔹', 'Extended MA tier2',  f'{EXT_SCAN_INTERVAL_MIN//60} tiếng/lần', '/ext_scan'),
    ]

    lines = ''
    for key, icon, label, freq, cmd_prefix in _cfg:
        enabled = SCANNER_ENABLED.get(key, True)
        if enabled:
            lines += (
                f'🟢 ON — {label} — {freq}' + NL
                + f'   ↳ Bấm để đổi: {cmd_prefix} off' + NL
            )
        else:
            lines += (
                f'🔴 OFF — {label} — {freq}' + NL
                + f'   ↳ Bấm để đổi: {cmd_prefix} on' + NL
            )

    msg = (
        f'📡 Trạng thái Auto Scan — {now_str}' + NL + NL
        + lines + NL
        + '<i>Dung /scanner off all de tat het truoc khi chay /mlscreen</i>'
    )
    send(msg, chat_id)


def _toggle_scanner(key, action, chat_id):
    """
    Hàm chung toggle 1 scanner: action = 'on' | 'off' | 'toggle'.
    Thay đổi SCANNER_ENABLED ngay lập tức, persist qua biến global.
    """
    NL = chr(10)
    labels = {
        'signal': 'Signal MUA/BAN',
        'vol':    'Vol Spike 1H',
        'ma':     'MA10/MA50 Cross',
        'ml':     'ML Momentum',
        'ext':    'Extended MA tier2',
        'shark':  'Shark Detector',
    }
    if key == 'all':
        new_val = (action == 'on')
        for k in SCANNER_ENABLED:
            SCANNER_ENABLED[k] = new_val
        state = '🟢 ON' if new_val else '🔴 OFF'
        send(f'{state} — Tất cả scanner đã được {"bật" if new_val else "tắt"}.' + NL
             + 'Dung /scanstatus de xem chi tiet.', chat_id)
        logger.info(f'Scanner ALL → {action.upper()} by chat_id={chat_id}')
        return

    if key not in SCANNER_ENABLED:
        send(f'&#x274C; Scanner không tồn tại: <b>{key}</b>' + NL
             + 'Hợp lệ: signal, vol, ma, ml, ext, shark, all', chat_id)
        return

    if action == 'toggle':
        SCANNER_ENABLED[key] = not SCANNER_ENABLED[key]
    else:
        SCANNER_ENABLED[key] = (action == 'on')

    enabled  = SCANNER_ENABLED[key]
    state    = '🟢 ON' if enabled else '🔴 OFF'
    label    = labels.get(key, key)
    cmd_back = f'/{key}_scan {"off" if enabled else "on"}'
    send(
        f'{state} — <b>{label}</b> đã được {"bật" if enabled else "tắt"}.' + NL
        + f'Để đổi lại: <b>{cmd_back}</b>' + NL
        + 'Dung /scanstatus de xem toan bo.',
        chat_id
    )
    logger.info(f'Scanner {key} → {"ON" if enabled else "OFF"} by chat_id={chat_id}')


def handle_signal_scan(args, chat_id):
    action = (args[0].lower().strip() if args else 'toggle')
    _toggle_scanner('signal', action, chat_id)


def handle_ma_scan(args, chat_id):
    action = (args[0].lower().strip() if args else 'toggle')
    _toggle_scanner('ma', action, chat_id)

def handle_ml_scan(args, chat_id):
    action = (args[0].lower().strip() if args else 'toggle')
    _toggle_scanner('ml', action, chat_id)

def handle_ext_scan(args, chat_id):
    action = (args[0].lower().strip() if args else 'toggle')
    _toggle_scanner('ext', action, chat_id)


def handle_scanner(args, chat_id):
    """
    /scanner on/off all|signal|vol|ma|ml|ext|shark
    Wrapper cho lệnh /scanner (cú pháp cũ).
    """
    if len(args) < 1:
        handle_scanstatus(chat_id)
        return
    action = args[0].lower().strip()   # on / off
    key    = args[1].lower().strip() if len(args) > 1 else 'all'
    if action not in ('on', 'off'):
        handle_scanstatus(chat_id)
        return
    _toggle_scanner(key, action, chat_id)


# ═══════════════════════════════════════════════════════════════════════════════
# /mlscreen — Systematic screening toàn HOSE bằng ML V1
# Chia thành batch ~70 mã, chạy BT + WF, filter promising
# ═══════════════════════════════════════════════════════════════════════════════

# Danh sách HOSE — fetch dynamic khi bot start, fallback hardcode
# Chỉ lấy mã có đủ thanh khoản (min_liquidity_bil >= 3 tỷ/ngày trong BT)
# Sắp xếp alphabet → chia batch nhất quán

def _get_hose_symbols():
    """Fetch danh sách mã HOSE từ vnstock. Fallback về hardcode nếu lỗi."""
    try:
        from vnstock import Vnstock
        df = Vnstock().stock(symbol='ACB', source='KBS').listing.symbols_by_exchange()
        if df is not None and len(df) > 0:
            # Filter HOSE, lấy cột ticker
            col_ex  = next((c for c in df.columns if 'exchange' in c.lower()), None)
            col_sym = next((c for c in df.columns
                           if c.lower() in ('ticker','symbol','code','stock_code')), None)
            if col_ex and col_sym:
                hose = df[df[col_ex].str.upper() == 'HOSE'][col_sym].str.upper().tolist()
                hose = sorted(set(hose))
                if len(hose) > 100:
                    return hose
    except Exception:
        pass
    # Fallback: hardcode ~280 mã HOSE có thanh khoản tốt (update định kỳ)
    return sorted([
        'ACB','AGG','AGR','ANV','ASM','BCG','BCM','BFC','BID','BMI',
        'BMP','BRC','BSR','BVH','CII','CMG','CMX','CNG','CRE','CTC',
        'CTD','CTG','CTR','CTS','DBC','DCM','DGC','DGW','DHC','DIG',
        'DPM','DPR','DRC','DRH','DSN','DXG','DXS','EIB','EVF','FMC',
        'FPT','FRT','FTS','GAS','GEX','GMD','GVR','HAH','HAX','HBC',
        'HCM','HDC','HDB','HDG','HHV','HPG','HSG','HTN','HUT','ICT',
        'IDC','IJC','IMP','ITA','KBC','KDC','KDH','KHG','KOS','KSB',
        'LAS','LCG','LDG','LGC','LHG','LPB','MBB','MBS','MCH','MCP',
        'MIG','MSB','MSN','MWG','NAB','NAF','NAV','NCT','NKG','NLG',
        'NNT','NRC','NTL','NVL','NVT','OCB','ORS','PAC','PAN','PC1',
        'PDR','PGC','PGD','PGS','PHP','PIT','PLX','PNJ','POW','PPC',
        'PTB','PTI','PVD','PVS','PVT','QCG','QNS','RAL','REE','SAB',
        'SAF','SAM','SBT','SCR','SCS','SGN','SHB','SHI','SHP','SHS',
        'SII','SIP','SKG','SJS','SMC','SPM','SRC','SRF','SSB','SSI',
        'STB','STK','SVC','SZC','TAC','TCB','TCH','TCL','TCM','TDC',
        'TDH','TDM','TGG','TIG','TIP','TIS','TLH','TNH','TNT','TPB',
        'TPC','TRA','TRC','TSC','TTB','TTF','TV2','TVB','TVN','TVS',
        'TYA','VCB','VCI','VCS','VDS','VGC','VGI','VHC','VHM','VIB',
        'VIC','VID','VIX','VJC','VND','VNM','VOS','VPB','VPI','VRC',
        'VRE','VSC','VSH','VSI','VTP','XMD','YEG',
    ])


def _build_screen_batches(batch_size=70):
    """Chia HOSE symbols thành các batch ~70 mã."""
    import re as _re
    syms = _get_hose_symbols()
    # Loại trừ mã đã có trong ML_CONFIRMED (đã biết kết quả)
    from config import BACKTEST_WATCHLIST
    known = set(BACKTEST_WATCHLIST)
    syms  = [s for s in syms if s not in known]
    # FIX: Loại mã phái sinh / chứng quyền (chứa chữ số trong tên, ví dụ BW2526, CFPT2526)
    # Mã cổ phiếu HOSE thông thường chỉ có 3 chữ cái, không chứa số
    syms  = [s for s in syms if not _re.search(r'\d', s)]
    batches = []
    for i in range(0, len(syms), batch_size):
        batches.append(syms[i:i+batch_size])
    return batches, syms


def handle_mlscreen(args, chat_id):
    """
    /mlscreen batch1     — Chạy batch 1 (~70 mã A-D)
    /mlscreen batch2     — Chạy batch 2 (~70 mã D-H)
    /mlscreen batchN     — Chạy batch N
    /mlscreen status     — Xem danh sách batch + mã promising đã tìm được
    /mlscreen all        — Chạy toàn bộ (warning: ~5-6 tiếng)
    """
    NL = chr(10)
    batches, all_syms = _build_screen_batches(batch_size=70)
    n_batches = len(batches)

    if not args or args[0].strip() == '':
        batch_preview = ''
        for i, b in enumerate(batches, 1):
            batch_preview += f' batch{i}: {b[0]}–{b[-1]} ({len(b)} mã)' + NL
        send(
            '&#x1F50D; <b>ML Screen — Systematic V1 Screening</b>' + NL + NL
            + f'Tong: <b>{len(all_syms)} mã HOSE</b> (sau khi loai confirmed) '
            + f'chia thanh <b>{n_batches} batch</b>' + NL + NL
            + batch_preview + NL
            + ' /mlscreen batch1   — Chay batch 1 (~70 phut)' + NL
            + ' /mlscreen status   — Xem ket qua da chay' + NL
            + ' /mlscreen all      — Toan bo (~' + str(n_batches) + ' tieng)',
            chat_id
        )
        return

    cmd = args[0].lower().strip()

    if cmd == 'status':
        _handle_mlscreen_status(chat_id, batches)
        return

    if cmd == 'all':
        import threading
        threading.Thread(
            target=_handle_mlscreen_run,
            args=(chat_id, batches, list(range(len(batches)))),
            daemon=True
        ).start()
        send(
            f'&#x23F3; Bat dau chay toan bo {n_batches} batch '
            f'({len(all_syms)} ma).' + NL
            + f'Uoc tinh ~{n_batches} tieng. Ket qua gui theo tung batch.',
            chat_id
        )
        return

    # batch1, batch2, ...
    import re
    m = re.match(r'^batch(\d+)$', cmd)
    if m:
        idx = int(m.group(1)) - 1
        if idx < 0 or idx >= n_batches:
            send(f'&#x274C; Chi co batch1 den batch{n_batches}.', chat_id)
            return
        import threading
        threading.Thread(
            target=_handle_mlscreen_run,
            args=(chat_id, batches, [idx]),
            daemon=True
        ).start()
        b = batches[idx]
        send(
            f'&#x23F3; Bat dau chay <b>batch{idx+1}</b>: '
            f'{b[0]}–{b[-1]} ({len(b)} ma)' + NL
            + f'Uoc tinh ~{len(b)} phut. Gui ket qua khi xong.',
            chat_id
        )
        return

    send(f'Cu phap: /mlscreen batch1 hoac /mlscreen status', chat_id)


def _handle_mlscreen_status(chat_id, batches):
    """Hiển thị batch nào đã chạy và promising list tổng hợp."""
    NL  = chr(10)
    try:
        import json, os
        cache_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  'mlscreen_results.json')
        if not os.path.exists(cache_file):
            send('&#x26AA; Chua co ket qua nao. Chay /mlscreen batch1 truoc.', chat_id)
            return

        with open(cache_file, 'r') as f:
            cache = json.load(f)

        completed = cache.get('completed_batches', [])
        promising = cache.get('promising', [])

        status_lines = ''
        for i, b in enumerate(batches, 1):
            icon = '&#x2705;' if i in completed else '&#x26AA;'
            status_lines += f' {icon} batch{i}: {b[0]}–{b[-1]} ({len(b)} ma)' + NL

        prom_lines = ''
        if promising:
            for p in sorted(promising, key=lambda x: -x.get('oos_wr', 0)):
                prom_lines += (
                    f' &#x1F7E2; <b>{p["sym"]}</b>  '
                    f'PF={p["pf"]} WR={p["wr"]}% OOS={p["oos"]}% '
                    f'WF={p["wf"]} n={p["n_lenhOOS"]}L' + NL
                )
        else:
            prom_lines = ' (chua co)' + NL

        send(
            '&#x1F4CA; <b>ML Screen Status</b>' + NL + NL
            + f'Batch da chay: {len(completed)}/{len(batches)}' + NL
            + status_lines + NL
            + f'<b>Promising ({len(promising)} ma — OOS>=50%, PF>=1.2, n>=15):</b>' + NL
            + prom_lines,
            chat_id
        )
    except Exception as e:
        send(f'&#x274C; Loi doc status: {str(e)[:100]}', chat_id)


def _handle_mlscreen_run(chat_id, batches, batch_indices):
    """Worker: chạy BT + WF V1 cho các batch được chỉ định."""
    import json, os, time as _time
    NL = chr(10)

    cache_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              'mlscreen_results.json')

    # Load cache hiện tại
    try:
        with open(cache_file, 'r') as f:
            cache = json.load(f)
    except Exception:
        cache = {'completed_batches': [], 'promising': [], 'all_results': {}}

    try:
        import sys
        bot_dir = os.path.dirname(os.path.abspath(__file__))
        if bot_dir not in sys.path:
            sys.path.insert(0, bot_dir)
        import backtest as bt

        # Load VNI 1 lần dùng chung
        try:
            df_vni, _ = bt.load_data('VNINDEX', days=bt.LOOKBACK_DAYS + 60)
        except Exception:
            df_vni = None

        for batch_idx in batch_indices:
            batch_syms = batches[batch_idx]
            batch_num  = batch_idx + 1

            send(
                f'&#x23F3; <b>Batch {batch_num}/{len(batches)}</b>: '
                f'{batch_syms[0]}–{batch_syms[-1]} ({len(batch_syms)} ma)' + NL
                + f'Uoc tinh ~{len(batch_syms)} phut...',
                chat_id
            )

            batch_results   = []
            batch_promising = []
            skipped         = []
            _done_count     = 0   # mã đã xử lý (OK + skip)

            for sym in batch_syms:
                try:
                    df_sym, src = bt.load_data(sym, days=bt.LOOKBACK_DAYS)

                    # Skip nếu không có data
                    if df_sym is None or len(df_sym) < 200:
                        skipped.append(f'{sym}(no data)')
                        _time.sleep(1)
                        continue

                    # Quick liquidity check trước khi chạy full BT
                    try:
                        import numpy as np
                        cc = bt.find_col(df_sym, ['close','closeprice','close_price'])
                        vc = next((c for c in df_sym.columns
                                  if c.lower() in ('volume','volume_match','klgd','vol',
                                                   'trading_volume','match_volume')), None)
                        if cc and vc:
                            closes_q = bt.to_arr(df_sym[cc])
                            vols_q   = bt.to_arr(df_sym[vc])
                            if closes_q.max() < 1000:
                                closes_q *= 1000
                            liq = float(np.mean(vols_q[-20:])) * float(closes_q[-1]) / 1e9
                            if liq < 3.0:
                                skipped.append(f'{sym}(liq={liq:.1f}ty)')
                                _time.sleep(1)
                                continue
                    except Exception:
                        pass

                    # Chạy BT
                    r1 = bt.run_backtest_momentum(
                        sym, sl=0.06, tp=0.17, hold_days=18, min_ml_score=75,
                        verbose=False, _df_cache=df_sym, _vni_cache=df_vni)

                    # Skip WF nếu BT quá ít lệnh — tiết kiệm ~45s/mã
                    n_lenh = r1['buy']['total'] if r1 and r1.get('buy') else 0
                    if n_lenh < 10:
                        skipped.append(f'{sym}(n={n_lenh})')
                        _time.sleep(2)
                        continue

                    # Chạy WF
                    w1 = bt.run_walk_forward_momentum(
                        sym, sl=0.06, tp=0.17, hold_days=18, min_ml_score=75,
                        verbose=False, _df_cache=df_sym, _vni_cache=df_vni)

                    # Parse kết quả
                    buy  = r1['buy'] if r1 else {}
                    wr1  = buy.get('win_rate', 0)
                    pf1  = buy.get('profit_factor', 0)
                    pnl1 = buy.get('avg_pnl', 0)
                    pf1s = f'{pf1:.2f}' if pf1 and pf1 != float('inf') else '∞'

                    oos_wr = w1['avg_oos_wr'] if w1 else 0
                    decay  = w1['decay_wr']   if w1 else 0
                    vrd    = w1['verdict']     if w1 else '?'
                    n_oos  = sum(w['oos_n'] for w in w1['windows']) if w1 else 0
                    oos_s  = f'{oos_wr:.0f}%' if w1 else '-'
                    vrd_s  = vrd[:5] if vrd else '?'

                    # Icon verdict
                    if oos_wr >= 55 and pf1 >= 1.3 and n_oos >= 15 and decay <= 15:
                        icon = '&#x1F7E2;'
                    elif oos_wr >= 50 and pf1 >= 1.2 and n_oos >= 15:
                        icon = '&#x1F7E1;'
                    elif n_oos < 15 and oos_wr >= 50:
                        icon = '&#x26A0;'
                    else:
                        icon = '&#x274C;'

                    n_warn = ' &#x26A0;n&lt;15' if n_oos < 15 else ''  # FIX: escape < cho HTML mode
                    row = (
                        f'{icon} <b>{sym}</b>  '
                        f'n={n_lenh}L WR={wr1}% PF={pf1s} PnL={pnl1:+.1f}% | '
                        f'OOS={oos_s} decay={decay:+.0f}% WF={vrd_s}{n_warn}'
                    )
                    batch_results.append(row)

                    # Lưu vào cache
                    cache['all_results'][sym] = {
                        'wr': wr1, 'pf': round(pf1, 2), 'n': n_lenh,
                        'oos_wr': round(oos_wr, 1), 'decay': round(decay, 1),
                        'wf': vrd_s, 'n_lenhOOS': n_oos,
                        'pnl': round(pnl1, 2),
                    }

                    # Promising criteria: OOS>=50% + PF>=1.2 + n_OOS>=15
                    is_promising = (oos_wr >= 50 and pf1 >= 1.2
                                    and n_oos >= 15 and n_lenh >= 15)
                    if is_promising:
                        entry = {
                            'sym': sym, 'wr': wr1, 'pf': pf1s,
                            'oos': round(oos_wr, 0), 'wf': vrd_s,
                            'n_lenhOOS': n_oos, 'decay': round(decay, 1),
                            'pnl': round(pnl1, 2),
                        }
                        batch_promising.append(entry)
                        # Merge vào global promising (tránh duplicate)
                        existing_syms = {p['sym'] for p in cache['promising']}
                        if sym not in existing_syms:
                            cache['promising'].append(entry)

                    logger.info(f'mlscreen batch{batch_num}: {sym} OK '
                                f'OOS={oos_wr:.0f}% PF={pf1:.2f}')

                except Exception as e_sym:
                    logger.error(f'mlscreen {sym}: {e_sym}')
                    skipped.append(f'{sym}(err)')

                # ── Progress ping mỗi 10 mã ──────────────────────────────────
                _done_count += 1
                if _done_count % 10 == 0 or _done_count == len(batch_syms):
                    _pct = int(_done_count / len(batch_syms) * 100)
                    _ok  = len(batch_results)
                    _sk  = len(skipped)
                    _pr  = len(batch_promising)
                    try:
                        send(
                            f'&#x23F3; Batch {batch_num} — {_done_count}/{len(batch_syms)} ma ({_pct}%)' + NL
                            + f'OK: {_ok} | Skip: {_sk} | Promising: {_pr}' + NL
                            + f'Vua xu ly: {sym}',
                            chat_id
                        )
                    except Exception:
                        pass  # progress ping loi cung khong sao

                _time.sleep(3)

            # ── Gửi kết quả batch ────────────────────────────────────────────
            # Summary table
            header = (
                f'&#x1F4CA; <b>Batch {batch_num} xong</b>: '
                f'{batch_syms[0]}–{batch_syms[-1]}' + NL
                + f'Chay: {len(batch_results)}ma | '
                + f'Skip: {len(skipped)}ma | '
                + f'Promising: {len(batch_promising)}ma' + NL
                + '─' * 30 + NL
            )

            # Gửi từng chunk ~3800 ký tự
            chunk = header
            for row in batch_results:
                if len(chunk) + len(row) > 3600:
                    send(chunk, chat_id)
                    chunk = row + NL
                else:
                    chunk += row + NL
            if chunk.strip():
                send(chunk, chat_id)

            # Promising riêng
            if batch_promising:
                prom_msg = (
                    f'&#x1F50D; <b>Promising — Batch {batch_num} '
                    f'({len(batch_promising)} ma)</b>' + NL
                    + '─' * 28 + NL + NL
                )
                for p in sorted(batch_promising, key=lambda x: -x.get('oos', 0)):
                    prom_msg += (
                        f'&#x1F7E2; <b>{p["sym"]}</b>  '
                        f'PF={p["pf"]} WR={p["wr"]}% OOS={p["oos"]}% '
                        f'decay={p["decay"]:+.0f}% WF={p["wf"]} '
                        f'n_OOS={p["n_lenhOOS"]}L' + NL
                    )
                prom_msg += NL + '<i>Tieu chi: OOS>=50% + PF>=1.2 + n_OOS>=15</i>'
                send(prom_msg, chat_id)

            if skipped:
                skip_str = ', '.join(skipped[:20])
                if len(skipped) > 20:
                    skip_str += f' ... (+{len(skipped)-20})'
                send(f'&#x26AA; Skip ({len(skipped)}ma): {skip_str}', chat_id)

            # Lưu cache
            if batch_num not in cache['completed_batches']:
                cache['completed_batches'].append(batch_num)
            try:
                with open(cache_file, 'w') as f:
                    json.dump(cache, f, ensure_ascii=False, indent=2)
            except Exception as e_cache:
                logger.error(f'mlscreen cache write: {e_cache}')

        # ── Tổng kết nếu chạy all ────────────────────────────────────────────
        if len(batch_indices) > 1:
            all_prom = cache.get('promising', [])
            final_msg = (
                f'&#x2705; <b>ML Screen hoan thanh!</b>' + NL
                + f'Da chay: {len(batch_indices)} batch | '
                + f'Tong promising: {len(all_prom)} ma' + NL + NL
            )
            if all_prom:
                final_msg += '<b>Toan bo promising (OOS>=50%, PF>=1.2, n>=15):</b>' + NL
                for p in sorted(all_prom, key=lambda x: -x.get('oos', 0)):
                    final_msg += (
                        f' &#x1F7E2; <b>{p["sym"]}</b>  '
                        f'PF={p["pf"]} OOS={p["oos"]}% WF={p["wf"]}' + NL
                    )
                final_msg += NL + '<i>Chay /mlbt <SYM> de backtest chi tiet.</i>'
            send(final_msg, chat_id)

    except Exception as e:
        logger.error(f'_handle_mlscreen_run: {e}')
        import traceback
        logger.error(traceback.format_exc())
        send(f'&#x274C; Loi mlscreen: {str(e)[:150]}', chat_id)


# ═══════════════════════════════════════════════════════════════════════════════
# /sascreen — Score A Screening toàn HOSE
# Pattern giống mlscreen, dùng run_backtest_symbol + run_walk_forward
# ═══════════════════════════════════════════════════════════════════════════════

def handle_sascreen(args, chat_id):
    """
    /sascreen [batch_num|all|status] — Backtest Score A toàn HOSE theo batch.
    Dùng run_backtest_symbol() + run_walk_forward() thay vì ML v1.
    Params: SL/TP từ SYMBOL_CONFIG nếu có, fallback sl=7% tp=14% hold=10d score>=65
    """
    import threading
    NL = chr(10)

    syms = _get_hose_symbols()
    BATCH_SIZE = 30
    batches = [syms[i:i+BATCH_SIZE] for i in range(0, len(syms), BATCH_SIZE)]

    arg = args[0].strip().lower() if args else ''

    if arg == 'status':
        _handle_sascreen_status(chat_id, batches)
        return

    if arg == 'all':
        batch_indices = list(range(len(batches)))
        send(
            f'&#x1F7E0; <b>SA Screen — ALL {len(batches)} batch</b>' + NL
            + f'Tong {len(syms)} ma | Liquid >= 3ty | Score A logic' + NL
            + f'Uoc tinh ~{len(syms)//2} phut (Score A nhanh hon ML)',
            chat_id
        )
    elif arg.isdigit():
        b = int(arg) - 1
        if b < 0 or b >= len(batches):
            send(f'&#x274C; Batch {arg} khong ton tai. Co {len(batches)} batch.', chat_id)
            return
        batch_indices = [b]
        send(
            f'&#x1F7E0; <b>SA Screen Batch {arg}</b>: '
            f'{batches[b][0]}–{batches[b][-1]} ({len(batches[b])} ma)' + NL
            + 'Dang chay...',
            chat_id
        )
    else:
        n_batches = len(batches)
        send(
            '&#x1F4CA; <b>SA Screen — Score A Screening HOSE</b>' + NL + NL
            + 'Lenh:' + NL
            + '  /sascreen 1        — Batch 1' + NL
            + '  /sascreen all      — Toan bo HOSE' + NL
            + '  /sascreen status   — Xem tien do' + NL + NL
            + f'Tong {n_batches} batch (~30 ma/batch)' + NL
            + 'Tieu chi pass: OOS>=50% + PF>=1.2 + n_OOS>=15' + NL
            + '<i>Score A: RSI/MA/Vol/Breakout/52W/RS | sl=7% tp=14% hold=10d score>=75 (STRONG only)</i>',
            chat_id
        )
        return

    threading.Thread(
        target=_handle_sascreen_run,
        args=(chat_id, batches, batch_indices),
        daemon=True
    ).start()


def _handle_sascreen_status(chat_id, batches):
    """Hiển thị tiến độ sascreen từ cache."""
    import json, os
    NL = chr(10)
    cache_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'sascreen_results.json'
    )
    try:
        with open(cache_file, 'r') as f:
            cache = json.load(f)
    except Exception:
        cache = {'completed_batches': [], 'promising': []}

    done   = cache.get('completed_batches', [])
    prom   = cache.get('promising', [])
    remain = [i+1 for i in range(len(batches)) if i+1 not in done]

    msg = (
        f'&#x1F4CA; <b>SA Screen Status</b>' + NL
        + f'Hoan thanh: {len(done)}/{len(batches)} batch' + NL
        + f'Promising: {len(prom)} ma' + NL
    )
    if done:
        msg += f'Da xong: batch {", ".join(map(str, sorted(done)))}' + NL
    if remain:
        msg += f'Con lai: batch {", ".join(map(str, remain[:5]))}...' + NL
    if prom:
        msg += NL + '<b>Promising hien tai:</b>' + NL
        for p in sorted(prom, key=lambda x: -x.get('oos', 0)):
            msg += (
                f' &#x1F7E2; <b>{p["sym"]}</b> '
                f'PF={p["pf"]} OOS={p["oos"]}% WF={p["wf"]}' + NL
            )
    send(msg, chat_id)


def _handle_sascreen_run(chat_id, batches, batch_indices):
    """Worker: chạy Score A BT + WF cho các batch được chỉ định."""
    import json, os, time as _time
    NL = chr(10)

    cache_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'sascreen_results.json'
    )
    try:
        with open(cache_file, 'r') as f:
            cache = json.load(f)
    except Exception:
        cache = {'completed_batches': [], 'promising': [], 'all_results': {}}

    try:
        import sys
        bot_dir = os.path.dirname(os.path.abspath(__file__))
        if bot_dir not in sys.path:
            sys.path.insert(0, bot_dir)
        import backtest as bt
        from config import SYMBOL_CONFIG

        for batch_idx in batch_indices:
            batch_syms = batches[batch_idx]
            batch_num  = batch_idx + 1

            send(
                f'&#x23F3; <b>SA Screen Batch {batch_num}/{len(batches)}</b>: '
                f'{batch_syms[0]}–{batch_syms[-1]} ({len(batch_syms)} ma)' + NL
                + f'Uoc tinh ~{len(batch_syms)//2} phut...',
                chat_id
            )

            batch_results   = []
            batch_promising = []
            skipped         = []
            _done_count     = 0

            for sym in batch_syms:
                try:
                    df_sym, _ = bt.load_data(sym, days=bt.LOOKBACK_DAYS)

                    if df_sym is None or len(df_sym) < 200:
                        skipped.append(f'{sym}(no data)')
                        _time.sleep(1)
                        continue

                    # Liquidity check — giống mlscreen
                    try:
                        import numpy as np
                        cc = bt.find_col(df_sym, ['close', 'closeprice', 'close_price'])
                        vc = next((c for c in df_sym.columns
                                   if c.lower() in ('volume', 'volume_match', 'klgd', 'vol',
                                                    'trading_volume', 'match_volume')), None)
                        if cc and vc:
                            closes_q = bt.to_arr(df_sym[cc])
                            vols_q   = bt.to_arr(df_sym[vc])
                            if closes_q.max() < 1000:
                                closes_q *= 1000
                            liq = float(np.mean(vols_q[-20:])) * float(closes_q[-1]) / 1e9
                            if liq < 3.0:
                                skipped.append(f'{sym}(liq={liq:.1f}ty)')
                                _time.sleep(1)
                                continue
                    except Exception:
                        pass

                    # ── Score A Backtest ──────────────────────────────────────
                    # Lấy params từ SYMBOL_CONFIG nếu có, fallback default
                    # SA screening dùng score>=75 (STRONG only) để WF ổn định hơn
                    _cfg  = SYMBOL_CONFIG.get(sym.upper(), {})
                    _sl   = _cfg.get('sl',   0.07)
                    _tp   = _cfg.get('tp',   0.14)
                    _hold = _cfg.get('hold_days', 10)
                    _msc  = max(_cfg.get('min_score', 65), 75)

                    r1 = bt.run_backtest_symbol(
                        sym,
                        sl=_sl, tp=_tp, hold_days=_hold, min_score=_msc,
                        verbose=False,
                        use_regime=False,    # Tắt regime để screening nhất quán
                        use_vwap=False,      # Tắt VWAP để không bias
                        use_b_filter=False,
                        _df_cache=df_sym,
                        trigger_mode='score_primary',
                    )

                    n_lenh = r1['buy']['total'] if r1 and r1.get('buy') else 0
                    if n_lenh < 10:
                        skipped.append(f'{sym}(n={n_lenh})')
                        _time.sleep(1)
                        continue

                    # ── Score A Walk-Forward ──────────────────────────────────
                    w1 = bt.run_walk_forward(
                        sym,
                        verbose=False,
                        _df_cache=df_sym,
                    )

                    # Parse kết quả
                    buy  = r1['buy']
                    wr1  = buy.get('win_rate', 0)
                    pf1  = buy.get('profit_factor', 0)
                    pnl1 = buy.get('avg_pnl', 0)
                    pf1s = f'{pf1:.2f}' if pf1 and pf1 != float('inf') else '∞'

                    oos_wr = w1['avg_oos_wr'] if w1 else 0
                    decay  = w1['decay_wr']   if w1 else 0
                    vrd    = w1['verdict']     if w1 else '?'
                    n_oos  = sum(w['oos_n'] for w in w1['windows']) if w1 else 0
                    oos_s  = f'{oos_wr:.0f}%' if w1 else '-'
                    vrd_s  = vrd[:5] if vrd else '?'

                    # Icon — cùng tiêu chí mlscreen để so sánh được
                    if oos_wr >= 55 and pf1 >= 1.3 and n_oos >= 15 and decay <= 15:
                        icon = '&#x1F7E2;'   # Xanh — strong
                    elif oos_wr >= 50 and pf1 >= 1.2 and n_oos >= 15:
                        icon = '&#x1F7E1;'   # Vàng — promising
                    elif n_oos < 15 and oos_wr >= 50:
                        icon = '&#x26A0;'    # Cảnh báo — ít lệnh
                    else:
                        icon = '&#x274C;'    # Đỏ — fail

                    n_warn = ' &#x26A0;n&lt;15' if n_oos < 15 else ''
                    row = (
                        f'{icon} <b>{sym}</b>  '
                        f'n={n_lenh}L WR={wr1}% PF={pf1s} PnL={pnl1:+.1f}% | '
                        f'OOS={oos_s} decay={decay:+.0f}% WF={vrd_s}{n_warn}'
                    )
                    batch_results.append(row)

                    # Cache kết quả
                    cache['all_results'][sym] = {
                        'wr': wr1, 'pf': round(pf1, 2), 'n': n_lenh,
                        'oos_wr': round(oos_wr, 1), 'decay': round(decay, 1),
                        'wf': vrd_s, 'n_lenhOOS': n_oos, 'pnl': round(pnl1, 2),
                    }

                    # Promising — cùng ngưỡng mlscreen để so sánh trực tiếp
                    is_promising = (oos_wr >= 50 and pf1 >= 1.2
                                    and n_oos >= 15 and n_lenh >= 15)
                    if is_promising:
                        entry = {
                            'sym': sym, 'wr': wr1, 'pf': pf1s,
                            'oos': round(oos_wr, 0), 'wf': vrd_s,
                            'n_lenhOOS': n_oos, 'decay': round(decay, 1),
                        }
                        batch_promising.append(entry)
                        existing_syms = {p['sym'] for p in cache['promising']}
                        if sym not in existing_syms:
                            cache['promising'].append(entry)

                    logger.info(
                        f'sascreen batch{batch_num}: {sym} OK '
                        f'OOS={oos_wr:.0f}% PF={pf1:.2f}'
                    )

                except Exception as e_sym:
                    logger.error(f'sascreen {sym}: {e_sym}')
                    skipped.append(f'{sym}(err)')

                # Progress ping mỗi 10 mã
                _done_count += 1
                if _done_count % 10 == 0 or _done_count == len(batch_syms):
                    _pct = int(_done_count / len(batch_syms) * 100)
                    try:
                        send(
                            f'&#x23F3; SA Batch {batch_num} — '
                            f'{_done_count}/{len(batch_syms)} ma ({_pct}%)' + NL
                            + f'OK: {len(batch_results)} | Skip: {len(skipped)} '
                            + f'| Promising: {len(batch_promising)}' + NL
                            + f'Vua xu ly: {sym}',
                            chat_id
                        )
                    except Exception:
                        pass

                _time.sleep(2)  # Score A nhanh hơn ML, sleep ít hơn

            # ── Gửi kết quả batch ────────────────────────────────────────────
            header = (
                f'&#x1F4CA; <b>SA Batch {batch_num} xong</b>: '
                f'{batch_syms[0]}–{batch_syms[-1]}' + NL
                + f'Chay: {len(batch_results)}ma | '
                + f'Skip: {len(skipped)}ma | '
                + f'Promising: {len(batch_promising)}ma' + NL
                + '─' * 30 + NL
            )

            chunk = header
            for row in batch_results:
                if len(chunk) + len(row) > 3600:
                    send(chunk, chat_id)
                    chunk = row + NL
                else:
                    chunk += row + NL
            if chunk.strip():
                send(chunk, chat_id)

            if batch_promising:
                prom_msg = (
                    f'&#x1F50D; <b>Promising SA — Batch {batch_num} '
                    f'({len(batch_promising)} ma)</b>' + NL
                    + '─' * 28 + NL + NL
                )
                for p in sorted(batch_promising, key=lambda x: -x.get('oos', 0)):
                    prom_msg += (
                        f'&#x1F7E2; <b>{p["sym"]}</b>  '
                        f'PF={p["pf"]} WR={p["wr"]}% OOS={p["oos"]}% '
                        f'decay={p["decay"]:+.0f}% WF={p["wf"]} '
                        f'n_OOS={p["n_lenhOOS"]}L' + NL
                    )
                prom_msg += NL + '<i>Tieu chi: OOS>=50% + PF>=1.2 + n_OOS>=15</i>'
                send(prom_msg, chat_id)

            if skipped:
                skip_str = ', '.join(skipped[:20])
                if len(skipped) > 20:
                    skip_str += f' ... (+{len(skipped)-20})'
                send(f'&#x26AA; Skip ({len(skipped)}ma): {skip_str}', chat_id)

            if batch_num not in cache['completed_batches']:
                cache['completed_batches'].append(batch_num)
            try:
                with open(cache_file, 'w') as f:
                    json.dump(cache, f, ensure_ascii=False, indent=2)
            except Exception as e_cache:
                logger.error(f'sascreen cache write: {e_cache}')

        # Tổng kết nếu all
        if len(batch_indices) > 1:
            all_prom = cache.get('promising', [])
            final_msg = (
                f'&#x2705; <b>SA Screen hoan thanh!</b>' + NL
                + f'Da chay: {len(batch_indices)} batch | '
                + f'Tong promising: {len(all_prom)} ma' + NL + NL
            )
            if all_prom:
                final_msg += '<b>Toan bo promising Score A:</b>' + NL
                for p in sorted(all_prom, key=lambda x: -x.get('oos', 0)):
                    final_msg += (
                        f' &#x1F7E2; <b>{p["sym"]}</b>  '
                        f'PF={p["pf"]} OOS={p["oos"]}% WF={p["wf"]}' + NL
                    )
                final_msg += NL + '<i>Chay /bt <SYM> de backtest chi tiet.</i>'
            send(final_msg, chat_id)

    except Exception as e:
        logger.error(f'_handle_sascreen_run: {e}')
        import traceback
        logger.error(traceback.format_exc())
        send(f'&#x274C; Loi sascreen: {str(e)[:150]}', chat_id)


def handle_mlbtv3(args, chat_id):
    """
    /mlbtv3 <SYM>              — BT + WF ML v3 cho bất kỳ mã nào
    /mlbtv3 <SYM> sl=5 tp=15   — Custom SL/TP
    /mlbtv3 <SYM> s=90         — Chi STRONG signal
    /mlbtv3 all                — Toan bo MLBT_V3_WATCHLIST
    """
    NL = chr(10)

    # Parse args
    sl, tp, hold, sc = None, None, None, None
    syms = []
    for a in args:
        al = a.lower()
        if al.startswith('sl='):
            try: sl = float(al[3:]) / 100
            except ValueError: pass
        elif al.startswith('tp='):
            try: tp = float(al[3:]) / 100
            except ValueError: pass
        elif al.startswith('hold='):
            try: hold = int(al[5:])
            except ValueError: pass
        elif al.startswith('s='):
            try: sc = int(al[2:])
            except ValueError: pass
        elif al not in ('', 'all'):
            syms.append(a.upper())

    if not args or (len(args) == 1 and args[0].strip() == ''):
        send(
            '&#x1F4CA; <b>Lệnh /mlbtv3 — ML Backtest v3 (Pooled Event Study)</b>' + NL + NL
            + ' /mlbtv3 NKG       — BT + WF momentum_v3 (~5 phút)' + NL
            + ' /mlbtv3 MWG       — BT + WF counter_v3 (~5 phút)' + NL
            + ' /mlbtv3 <SYM>     — Bất kỳ mã nào (~5 phút)' + NL
            + ' /mlbtv3 all       — Toan bo watchlist (~N*5 phút)' + NL
            + ' /mlbtv3 NKG sl=5 tp=15 — Custom SL/TP' + NL + NL
            + '<i>2 pattern types:</i>' + NL
            + ' momentum_v3: trend_10d + vol_trend + vol_spike + rs_vni_5d_neg' + NL
            + ' counter_v3 (MWG/PNJ): near_52w_low + rsi_oversold + rs_vni_neg' + NL + NL
            + '<i>Event study confirmed: NKG POW FRT DGW MBS (momentum) | MWG PNJ (counter)</i>',
            chat_id
        )
        return

    if 'all' in [a.lower() for a in args]:
        import threading
        t = threading.Thread(
            target=_handle_mlbtv3_all,
            args=(chat_id, sl, tp, hold, sc),
            daemon=True
        )
        t.start()
        return

    sym_raw = syms[0] if syms else (args[0].upper() if args else '')
    if not sym_raw:
        send('Cu phap: <b>/mlbtv3 NKG</b> hoac <b>/mlbtv3 all</b>', chat_id)
        return

    import threading
    t = threading.Thread(
        target=_handle_mlbtv3_symbol,
        args=(sym_raw, chat_id, sl, tp, hold, sc),
        daemon=True
    )
    t.start()



def handle_mrabt(args, chat_id):
    """
    /mrabt <SYM>          — Backtest MRA cho 1 mã (Trigger A: break high 5d)
    /mrabt <SYM> b        — Trigger B (BB upper breakout)
    /mrabt <SYM> c        — Trigger C (micro-breakout MA20+1.5%)
    /mrabt <SYM> sl=6     — Custom SL%
    /mrabt all            — Toàn BACKTEST_WATCHLIST (test nhanh)
    """
    NL   = chr(10)
    args = [a.strip() for a in args if a.strip()]

    if not args:
        send(
            '&#x1F4CA; <b>Lệnh /mrabt — MRA Backtest</b>' + NL + NL
            + ' /mrabt STB         — BT + WF Trigger A (break high 5d)' + NL
            + ' /mrabt VCB b       — Trigger B (BB upper)' + NL
            + ' /mrabt VNM c       — Trigger C (MA20+1.5% micro-breakout)' + NL
            + ' /mrabt MCH sl=6    — Custom SL' + NL
            + ' /mrabt all         — Toàn BACKTEST_WATCHLIST' + NL + NL
            + '<i>Default: Trigger A | SL=7% | Không TP cứng</i>' + NL
            + '<i>Exit: Close&lt;MA20×0.98 | Fail-fast 7d | Stop -7%</i>',
            chat_id
        )
        return

    sym     = None
    trigger = 'A'
    c_sl    = None

    for a in args:
        al = a.lower()
        if   al == 'all':      sym = 'all'
        elif al == 'b':        trigger = 'B'
        elif al == 'c':        trigger = 'C'
        elif al == 'a':        trigger = 'A'
        elif al.startswith('sl='):
            try: c_sl = float(al[3:]) / 100
            except: pass
        elif sym is None:
            sym = a.upper()

    if not sym:
        send('Cú pháp: <b>/mrabt STB</b> hoặc <b>/mrabt all</b>', chat_id)
        return

    if sym == 'all':
        threading.Thread(
            target=_handle_mrabt_all,
            args=(chat_id, trigger, c_sl),
            daemon=True
        ).start()
    else:
        threading.Thread(
            target=_handle_mrabt_symbol,
            args=(sym, chat_id, trigger, c_sl),
            daemon=True
        ).start()


def _handle_mrabt_symbol(symbol, chat_id, trigger='A', custom_sl=None):
    """Chạy MRA BT + WF cho 1 mã, gửi kết quả."""
    NL = chr(10)
    sl = custom_sl if custom_sl is not None else 0.07

    send(
        '&#x1F504; Đang chạy <b>MRA Backtest ' + symbol + '</b>'
        + f' [Trigger={trigger} SL={int(sl*100)}%]' + NL
        + '<i>~2-3 phút...</i>',
        chat_id
    )

    try:
        import sys, os
        bot_dir = os.path.dirname(os.path.abspath(__file__))
        if bot_dir not in sys.path:
            sys.path.insert(0, bot_dir)
        import backtest as bt

        df_shared, _ = bt.load_data(symbol, days=max(bt.LOOKBACK_DAYS, 1826))
        if df_shared is None:
            send('&#x274C; ' + symbol + ': Không load được data.', chat_id)
            return

        # ── IS Backtest ───────────────────────────────────────────────────────
        res = bt.run_backtest_mra(symbol, trigger=trigger, sl=sl,
                                  verbose=False, _df_cache=df_shared)
        if not res or not res.get('buy') or res['buy']['total'] == 0:
            send('&#x274C; ' + symbol + ': Không có lệnh MRA nào. '
                 'Thử trigger khác (vd: /mrabt ' + symbol + ' b)', chat_id)
            return

        buy  = res['buy']
        n    = buy['total']
        wr   = buy.get('win_rate', 0)
        pf   = buy.get('profit_factor', 0)
        apnl = buy.get('avg_pnl', 0)
        aw   = buy.get('avg_win', 0)
        al_  = buy.get('avg_loss', 0)
        pfs  = f'{pf:.2f}' if pf and pf != float('inf') else '∞'

        # Exit reason breakdown
        df_t     = res.get('trades')
        reasons  = ''
        if df_t is not None and len(df_t):
            rc = df_t['reason'].value_counts().to_dict()
            reasons = '  '.join(f'{k}:{v}' for k, v in rc.items())

        # ── WF ────────────────────────────────────────────────────────────────
        wf = bt.run_walk_forward_mra(symbol, trigger=trigger, sl=sl,
                                     verbose=False, _df_cache=df_shared)
        if wf:
            oos_wr  = wf['avg_oos_wr']
            decay   = wf['decay_wr']
            verdict = wf['verdict']
            n_oos   = wf['n_oos']
            vrd_s   = verdict
        else:
            oos_wr = decay = 0; vrd_s = '?'; n_oos = 0

        # ── Format output ─────────────────────────────────────────────────────
        wf_icon = ('&#x2705;' if vrd_s == 'V' else
                   '&#x1F7E1;' if vrd_s == '~' else '&#x274C;')

        msg = (
            f'&#x1F4CA; <b>MRA Backtest — {symbol}</b>'
            + f' [Trigger {trigger} | SL={int(sl*100)}%]' + NL
            + '─' * 28 + NL + NL
            + f'&#x1F4B0; <b>IS (7 năm)</b>' + NL
            + f'  Lệnh : <b>{n}</b>  WR: <b>{wr:.1f}%</b>'
            + f'  PF: <b>{pfs}</b>  AvgPnL: <b>{apnl:+.2f}%</b>' + NL
            + f'  AvgWin: {aw:+.2f}%  AvgLoss: {al_:+.2f}%' + NL
            + (f'  Exit  : {reasons}' + NL if reasons else '')
            + NL
            + f'{wf_icon} <b>Walk-Forward</b>' + NL
            + f'  OOS WR: <b>{oos_wr:.1f}%</b>'
            + f'  Decay: <b>{decay:+.1f}%</b>'
            + f'  n_OOS: {n_oos}  [{vrd_s}]' + NL
            + NL
            + '<i>MRA: Mean Reversion + Accumulation'
            + ' | Exit: MA20×0.98 / fail-fast / stop</i>'
        )
        send(msg, chat_id)

        # WF window detail nếu có
        if wf and wf.get('windows'):
            det = '&#x1F50D; <b>WF Windows:</b>' + NL
            for idx, w in enumerate(wf['windows'], 1):
                det += (f'  W{idx}: IS {w["is_wr"]:.0f}%({w["is_n"]}L)'
                        f' → OOS {w["oos_wr"]:.0f}%({w["oos_n"]}L)'
                        f' PnL:{w["oos_pnl"]:+.1f}%' + NL)
            send(det, chat_id)

    except Exception as e:
        import traceback
        send('&#x274C; Lỗi mrabt ' + symbol + ': ' + str(e)[:200], chat_id)


def _handle_mrabt_all(chat_id, trigger='A', custom_sl=None):
    """Chạy MRA BT trên toàn BACKTEST_WATCHLIST — test nhanh."""
    NL  = chr(10)
    sl  = custom_sl if custom_sl is not None else 0.07

    send(
        '&#x1F504; MRA Batch test — toàn BACKTEST_WATCHLIST'
        + f' [Trigger {trigger} SL={int(sl*100)}%]' + NL
        + '<i>~15-20 phút...</i>',
        chat_id
    )

    try:
        import sys, os, time as _time
        bot_dir = os.path.dirname(os.path.abspath(__file__))
        if bot_dir not in sys.path:
            sys.path.insert(0, bot_dir)
        import backtest as bt
        from config import BACKTEST_WATCHLIST

        results = []
        for sym in BACKTEST_WATCHLIST:
            try:
                df_, _ = bt.load_data(sym, days=max(bt.LOOKBACK_DAYS, 1826))
                if df_ is None: continue
                r = bt.run_backtest_mra(sym, trigger=trigger, sl=sl,
                                        verbose=False, _df_cache=df_)
                w = bt.run_walk_forward_mra(sym, trigger=trigger, sl=sl,
                                            verbose=False, _df_cache=df_)
                if not r or not r.get('buy'): continue
                buy = r['buy']
                results.append({
                    'sym':    sym,
                    'n':      buy['total'],
                    'wr':     buy.get('win_rate', 0),
                    'pf':     buy.get('profit_factor', 0),
                    'apnl':   buy.get('avg_pnl', 0),
                    'oos_wr': w['avg_oos_wr'] if w else 0,
                    'decay':  w['decay_wr']   if w else 0,
                    'vrd':    w['verdict']     if w else '?',
                    'n_oos':  w['n_oos']       if w else 0,
                })
            except Exception as e_s:
                pass
            _time.sleep(2)

        if not results:
            send('&#x274C; Không có kết quả MRA nào.', chat_id)
            return

        # Sort by OOS WR desc
        results.sort(key=lambda x: -x['oos_wr'])

        msg = (f'&#x1F4CA; <b>MRA Batch — Trigger {trigger}'
               f' ({len(results)} mã)</b>' + NL + '─' * 30 + NL + NL)

        for r in results:
            pfs = f'{r["pf"]:.2f}' if r["pf"] != float('inf') else '∞'
            vrd = r['vrd']
            icon = ('&#x2705;' if vrd == 'V' else
                    '&#x1F7E1;' if vrd == '~' else '&#x274C;')
            msg += (f'{icon} <b>{r["sym"]:5s}</b>'
                    + f' n={r["n"]:3d}L WR={r["wr"]:.1f}%'
                    + f' PF={pfs}'
                    + f' | OOS={r["oos_wr"]:.0f}%'
                    + f' decay={r["decay"]:+.0f}%'
                    + f' [{vrd}]' + NL)
            if len(msg) > 3400:
                send(msg, chat_id)
                msg = ''

        if msg.strip():
            send(msg, chat_id)

    except Exception as e:
        send('&#x274C; Lỗi mrabt all: ' + str(e)[:200], chat_id)


def handle_mradebug(args, chat_id):
    """/mradebug <SYM> — Debug từng filter Stage 1 của MRA để chẩn đoán n=0."""
    NL = chr(10)
    symbol = args[0].upper() if args else 'STB'

    import backtest as bt
    import numpy as np

    send(f'🔬 Đang debug MRA filters cho <b>{symbol}</b>...', chat_id)
    try:
        df, _ = bt.load_data(symbol)
        if df is None or len(df) < 220:
            send(f'❌ Không lấy được data {symbol}', chat_id)
            return

        cc = bt.find_col(df, ['close', 'closeprice', 'close_price'])
        hc = bt.find_col(df, ['high',  'highprice',  'high_price'])
        lc = bt.find_col(df, ['low',   'lowprice',   'low_price'])
        vc = bt.find_col(df, ['volume','matchvolume'])

        closes  = df[cc].values.astype(float)
        highs   = df[hc].values.astype(float)
        lows    = df[lc].values.astype(float)
        volumes = df[vc].values.astype(float)

        total = 0
        f1_bb = 0     # BB width < P25
        f1_atr = 0    # ATR/Price > 1%
        f1_both = 0   # F1 pass (cả hai)
        f2_pass = 0   # price vs MA200 [-10%,+20%] (v2: không slope)
        f3_pass = 0   # HH20-LL20 < 15% (v2)
        f4_pass = 0   # |Close-MA20| < 5%
        all_pass = 0  # pass cả 4 filter

        bb_widths = []

        for i in range(252, len(closes)):   # v2: loop start=252 (BB percentile)
            total += 1

            # F2 v2: price vs MA200 [-10%, +20%], KHÔNG slope
            if i >= 200:
                ma200 = np.mean(closes[i-200:i+1])
                pct_vs_ma200 = (closes[i] - ma200) / ma200
                f2 = (-0.10 <= pct_vs_ma200 <= 0.20)
            else:
                f2 = False
            if f2: f2_pass += 1

            # F3 v2: HH20-LL20 < 15%
            hh20 = max(highs[i-20:i+1])
            ll20 = min(lows[i-20:i+1])
            range_pct = (hh20 - ll20) / ll20 if ll20 > 0 else 999
            f3 = range_pct < 0.15
            if f3: f3_pass += 1

            # F4: |Close - MA20| < 5%
            ma20 = np.mean(closes[i-20:i+1])
            f4 = abs(closes[i] - ma20) / ma20 < 0.05 if ma20 > 0 else False
            if f4: f4_pass += 1

            # F1: BB width < P25 (rolling, cần 252 ngày lịch sử)
            bb_window = closes[i-20:i+1]
            bb_std = np.std(bb_window, ddof=1)
            bb_mid = np.mean(bb_window)
            bb_w = (4 * bb_std) / bb_mid if bb_mid > 0 else 0
            bb_widths.append(bb_w)

            # ATR(14)
            tr_list = []
            for j in range(max(1, i-13), i+1):
                tr = max(highs[j] - lows[j],
                         abs(highs[j] - closes[j-1]),
                         abs(lows[j]  - closes[j-1]))
                tr_list.append(tr)
            atr = np.mean(tr_list) if tr_list else closes[i] * 0.02
            atr_pct = atr / closes[i] if closes[i] > 0 else 0

            p25_bb = np.percentile(bb_widths, 25) if len(bb_widths) >= 20 else 999

            _f1_bb  = bb_w < p25_bb
            _f1_atr = atr_pct > 0.01
            if _f1_bb:  f1_bb  += 1
            if _f1_atr: f1_atr += 1
            if _f1_bb and _f1_atr: f1_both += 1

            if f2 and f3 and f4 and _f1_bb and _f1_atr:
                all_pass += 1

        def pct(n): return f'{n/total*100:.0f}%' if total > 0 else '0%'

        # Tính range_pct trung bình (100 candles gần nhất)
        range_samples = []
        start_r = max(252, len(closes) - 100)
        for i in range(start_r, len(closes)):
            hh = max(highs[i-20:i+1])
            ll = min(lows[i-20:i+1])
            if ll > 0: range_samples.append((hh-ll)/ll*100)
        avg_range = np.mean(range_samples) if range_samples else 0

        msg = (
            f'🔬 <b>MRA Filter Debug — {symbol}</b>{NL}'
            f'Candles kiểm tra: {total}{NL}{NL}'
            f'<b>Stage 1 Filters (độc lập, setting v2):</b>{NL}'
            f'F1a BB width &lt; P25:           {f1_bb:4d} ({pct(f1_bb)}){NL}'
            f'F1b ATR/Price &gt; 1%:           {f1_atr:4d} ({pct(f1_atr)}){NL}'
            f'F1  cả hai pass:                {f1_both:4d} ({pct(f1_both)}){NL}'
            f'F2  Price vs MA200 [-10%,+20%]: {f2_pass:4d} ({pct(f2_pass)}){NL}'
            f'F3  HH20-LL20 &lt; 15%:          {f3_pass:4d} ({pct(f3_pass)}){NL}'
            f'F4  |Close-MA20| &lt; 5%:        {f4_pass:4d} ({pct(f4_pass)}){NL}{NL}'
            f'✅ <b>Pass cả 4 filters:</b>        {all_pass:4d} ({pct(all_pass)}){NL}{NL}'
            f'📊 HH20-LL20 avg (100 candles gần): <b>{avg_range:.1f}%</b> (ngưỡng: 15%){NL}'
            f'→ Filter nào gần 0% = bottleneck chính'
        )
        send(msg, chat_id)

    except Exception as e:
        send(f'❌ Lỗi mradebug {symbol}: {str(e)[:300]}', chat_id)


def handle_mradebug2(args, chat_id):
    """/mradebug2 <SYM> — Debug Stage 2 score distribution cho candles đã pass Stage 1."""
    NL = chr(10)
    symbol = args[0].upper() if args else 'VCB'

    import backtest as bt
    import numpy as np

    send(f'🔬 Đang debug MRA Stage 2 cho <b>{symbol}</b>...', chat_id)
    try:
        df, _ = bt.load_data(symbol, days=max(bt.LOOKBACK_DAYS, 1826))
        if df is None or len(df) < 252:
            send(f'❌ Không lấy được data {symbol}', chat_id)
            return

        cc = bt.find_col(df, ['close', 'closeprice', 'close_price'])
        hc = bt.find_col(df, ['high',  'highprice',  'high_price'])
        lc = bt.find_col(df, ['low',   'lowprice',   'low_price'])
        vc = next((c for c in df.columns if c.lower() in
                   ('volume','volume_match','klgd','vol',
                    'trading_volume','match_volume','total_volume')), None)

        closes  = bt.to_arr(df[cc])
        highs   = bt.to_arr(df[hc]) if hc else closes.copy()
        lows    = bt.to_arr(df[lc]) if lc else closes.copy()
        volumes = bt.to_arr(df[vc]) if vc else np.ones(len(closes))
        if closes.max() < 1000:
            closes *= 1000; highs *= 1000; lows *= 1000

        ENTRY_THRESH = 40   # khớp run_backtest_mra

        scores       = []
        triggered_a  = 0
        triggered_b  = 0
        triggered_c  = 0
        score_buckets = {
            '0-29':  0,
            '30-39': 0,
            '40-54': 0,
            '55-69': 0,
            '70+':   0,
        }

        for i in range(252, len(closes)):
            res = bt.compute_mra_score(closes, highs, lows, volumes, i, trigger='A')
            if not res['filter_pass']:
                continue
            s = res['score']
            scores.append(s)
            if s >= ENTRY_THRESH and res['triggered']:
                triggered_a += 1

            res_b = bt.compute_mra_score(closes, highs, lows, volumes, i, trigger='B')
            if res_b['filter_pass'] and res_b['score'] >= ENTRY_THRESH and res_b['triggered']:
                triggered_b += 1

            res_c = bt.compute_mra_score(closes, highs, lows, volumes, i, trigger='C')
            if res_c['filter_pass'] and res_c['score'] >= ENTRY_THRESH and res_c['triggered']:
                triggered_c += 1

            if s < 30:   score_buckets['0-29']  += 1
            elif s < 40: score_buckets['30-39'] += 1
            elif s < 55: score_buckets['40-54'] += 1
            elif s < 70: score_buckets['55-69'] += 1
            else:        score_buckets['70+']   += 1

        if not scores:
            send(f'⚠️ {symbol}: 0 candles pass Stage 1 — filter quá chặt', chat_id)
            return

        avg_s    = np.mean(scores)
        med_s    = np.median(scores)
        max_s    = max(scores)
        n_pass   = len(scores)
        n_entry  = score_buckets['40-54'] + score_buckets['55-69'] + score_buckets['70+']

        msg = (
            f'🔬 <b>MRA Stage 2 Debug — {symbol}</b>{NL}'
            f'Candles pass Stage 1: <b>{n_pass}</b>{NL}{NL}'
            f'<b>Score distribution (ngưỡng entry = {ENTRY_THRESH}):</b>{NL}'
            f'  0–29  (dưới ngưỡng xa):  {score_buckets["0-29"]:4d}{NL}'
            f'  30–39 (dưới ngưỡng gần): {score_buckets["30-39"]:4d}{NL}'
            f'  40–54 ✅ entry zone:      {score_buckets["40-54"]:4d}{NL}'
            f'  55–69 ✅ entry zone:      {score_buckets["55-69"]:4d}{NL}'
            f'  70+   ✅ entry zone:      {score_buckets["70+"]:4d}{NL}'
            f'  → Tổng score≥{ENTRY_THRESH}: <b>{n_entry}</b> ← entry candidates{NL}{NL}'
            f'avg={avg_s:.1f} | median={med_s:.1f} | max={max_s}{NL}{NL}'
            f'<b>Trigger fire (score≥{ENTRY_THRESH}):</b>{NL}'
            f'  Trigger A (high 5d):    {triggered_a}{NL}'
            f'  Trigger B (BB upper):   {triggered_b}{NL}'
            f'  Trigger C (MA20+1.5%): {triggered_c}{NL}{NL}'
            f'→ Nếu n_entry=0: score threshold vẫn cao, thử hạ xuống 30{NL}'
            f'→ Nếu n_entry>0 nhưng trigger=0: trigger quá chặt'
        )
        send(msg, chat_id)

    except Exception as e:
        send(f'❌ Lỗi mradebug2 {symbol}: {str(e)[:300]}', chat_id)


def handle_mradebug3(args, chat_id):
    """/mradebug3 <SYM> — Phân tích incremental: bỏ từng filter thì ra bao nhiêu lệnh.
    Mục đích: xác định filter nào đang block signal hợp lý vs filter nào cần giữ.
    """
    NL = chr(10)
    symbol = args[0].upper() if args else 'VCB'

    import backtest as bt
    import numpy as np

    send(f'🔬 Đang phân tích filter contribution cho <b>{symbol}</b>...', chat_id)
    try:
        df, _ = bt.load_data(symbol)
        if df is None or len(df) < 252:
            send(f'❌ Không lấy được data {symbol}', chat_id)
            return

        cc = bt.find_col(df, ['close', 'closeprice', 'close_price'])
        hc = bt.find_col(df, ['high',  'highprice',  'high_price'])
        lc = bt.find_col(df, ['low',   'lowprice',   'low_price'])
        vc = bt.find_col(df, ['volume','matchvolume'])

        closes  = df[cc].values.astype(float)
        highs   = df[hc].values.astype(float)
        lows    = df[lc].values.astype(float)
        volumes = df[vc].values.astype(float)

        MIN_SCORE = 50  # khớp run_backtest_mra

        def _count(skip_f1=False, skip_f2=False, skip_f3=False, skip_f4=False,
                   score_thresh=MIN_SCORE):
            """Đếm entry candidates khi bỏ một hoặc nhiều filter."""
            cand_a = cand_c = 0
            for i in range(252, len(closes)):
                res = bt.compute_mra_score(closes, highs, lows, volumes, i, trigger='A')
                d   = res['detail']

                # Rebuild filter_pass với skip flags
                f1 = skip_f1 or res['detail'].get('f1_bb_compress', False)
                # F2: recalc từ detail
                dist = d.get('dist_ma200_pct')
                f2 = skip_f2 or (dist is not None and -10 <= dist <= 20)
                # F3
                rng = d.get('range20_pct')
                f3 = skip_f3 or (rng is not None and rng < 15)
                # F4
                dist20 = d.get('dist_ma20_pct')
                f4 = skip_f4 or (dist20 is not None and dist20 < 5)

                if not (f1 and f2 and f3 and f4):
                    continue
                if res['score'] < score_thresh:
                    continue

                # Trigger A (high 5d)
                if res['triggered']:
                    cand_a += 1
                # Trigger C (MA20+1.5%) — recalc
                ma20 = np.mean(closes[i-20:i+1])
                if closes[i] > ma20 * 1.015:
                    cand_c += 1

            return cand_a, cand_c

        # Baseline — tất cả filter giữ nguyên
        base_a, base_c = _count()

        # Bỏ từng filter một
        no_f1_a, no_f1_c = _count(skip_f1=True)
        no_f2_a, no_f2_c = _count(skip_f2=True)
        no_f3_a, no_f3_c = _count(skip_f3=True)
        no_f4_a, no_f4_c = _count(skip_f4=True)

        # Hạ score threshold
        s60_a, s60_c = _count(score_thresh=60)
        s40_a, s40_c = _count(score_thresh=40)

        def row(label, a, c):
            return f'  {label:<22} A={a:2d}  C={c:2d}{NL}'

        msg = (
            f'🔬 <b>MRA Filter Contribution — {symbol}</b>{NL}'
            f'(score≥{MIN_SCORE}, Trigger A=high5d / C=MA20+1.5%){NL}{NL}'
            f'<b>Baseline (tất cả filter ON):</b>{NL}'
            + row('All filters:', base_a, base_c)
            + NL
            + f'<b>Bỏ từng filter (giữ các filter còn lại):</b>{NL}'
            + row('Bỏ F1 (BB compress):', no_f1_a, no_f1_c)
            + row('Bỏ F2 (MA200 range):', no_f2_a, no_f2_c)
            + row('Bỏ F3 (HH20-LL20<15%):', no_f3_a, no_f3_c)
            + row('Bỏ F4 (|C-MA20|<5%):', no_f4_a, no_f4_c)
            + NL
            + f'<b>Hạ score threshold:</b>{NL}'
            + row('score≥60:', s60_a, s60_c)
            + row('score≥40:', s40_a, s40_c)
            + NL
            + f'→ Filter nào tăng nhiều nhất = đang block signal hợp lý{NL}'
            + f'→ Filter nào tăng ít = đang làm đúng nhiệm vụ (giữ nguyên)'
        )
        send(msg, chat_id)

    except Exception as e:
        send(f'❌ Lỗi mradebug3 {symbol}: {str(e)[:300]}', chat_id)


def handle_mradebug4(args, chat_id):
    """/mradebug4 <SYM> [a|c] — Trace trực tiếp run_backtest_mra, in lý do n=0."""
    NL = chr(10)
    symbol  = 'VCB'
    trigger = 'C'
    for a in (args or []):
        al = a.lower()
        if al in ('a', 'b', 'c'): trigger = al.upper()
        else: symbol = a.upper()

    import backtest as bt
    import numpy as np

    send(f'🔬 Trace MRA <b>{symbol}</b> Trigger={trigger}...', chat_id)
    try:
        df, src = bt.load_data(symbol)
        if df is None:
            send('❌ Không load được data', chat_id); return

        cc = bt.find_col(df, ['close','closeprice','close_price'])
        hc = bt.find_col(df, ['high','highprice','high_price'])
        lc = bt.find_col(df, ['low','lowprice','low_price'])
        vc = next((c for c in df.columns if c.lower() in
                   ('volume','volume_match','klgd','vol',
                    'trading_volume','match_volume','total_volume')), None)

        closes  = bt.to_arr(df[cc])
        highs   = bt.to_arr(df[hc]) if hc else closes.copy()
        lows    = bt.to_arr(df[lc]) if lc else closes.copy()
        volumes = bt.to_arr(df[vc]) if vc else np.ones(len(closes))

        # Normalize như run_backtest_mra
        if closes.max() < 1000:
            closes *= 1000; highs *= 1000; lows *= 1000

        n = len(closes)

        # Counters
        n_fp    = 0   # filter_pass=True
        n_score = 0   # score >= 40
        n_trig  = 0   # triggered=True
        n_entry = 0   # entry candidates (fp + score + trigger)

        # Sample 3 candles pass filter để xem score/trigger detail
        samples = []

        for i in range(252, n):
            res = bt.compute_mra_score(closes, highs, lows, volumes, i,
                                       trigger=trigger)
            if not res['filter_pass']:
                continue
            n_fp += 1
            s = res['score']
            if s >= 40:
                n_score += 1
            if res['triggered']:
                n_trig += 1
            if s >= 40 and res['triggered']:
                n_entry += 1
                if len(samples) < 3:
                    d = res['detail']
                    samples.append(
                        f"  i={i} score={s} "
                        f"bb={d.get('bb_width_pct','?')} "
                        f"dist200={d.get('dist_ma200_pct','?')}% "
                        f"range={d.get('range20_pct','?')}% "
                        f"dist20={d.get('dist_ma20_pct','?')}%"
                    )

        sample_txt = (NL.join(samples)) if samples else '  (không có)'

        msg = (
            f'🔬 <b>MRA Trace — {symbol} Trigger={trigger}</b>{NL}'
            f'Data: {n} candles | scan từ i=252{NL}'
            f'Close range: {closes[252]:.0f}–{closes[-1]:.0f}{NL}{NL}'
            f'filter_pass=True:      {n_fp}{NL}'
            f'score≥40:              {n_score}{NL}'
            f'triggered=True:        {n_trig}{NL}'
            f'<b>Entry candidates:      {n_entry}</b>{NL}{NL}'
            f'Bottleneck:{NL}'
            f'  {"→ filter_pass=0: Stage 1 vẫn chặn hết" if n_fp==0 else ""}'
            f'  {"→ score thấp: Stage 2 scoring sai" if n_fp>0 and n_score==0 else ""}'
            f'  {"→ trigger không fire" if n_score>0 and n_trig==0 else ""}'
            f'  {"→ score≥40 và trigger không overlap" if n_score>0 and n_trig>0 and n_entry==0 else ""}{NL}{NL}'
            f'Sample entry candles:{NL}{sample_txt}'
        )
        send(msg, chat_id)

    except Exception as e:
        import traceback
        send(f'❌ {str(e)[:200]}{NL}{traceback.format_exc()[-300:]}', chat_id)


def _handle_mlbtv3_symbol(symbol, chat_id, custom_sl=None, custom_tp=None,
                            custom_hold=None, custom_score=None):
    """Chạy ML v3 BT + WF cho 1 mã, so sánh v1 vs v3."""
    NL    = chr(10)
    sl    = custom_sl    if custom_sl    is not None else 0.06
    tp    = custom_tp    if custom_tp    is not None else 0.17
    hold  = custom_hold  if custom_hold  is not None else 18
    score = custom_score if custom_score is not None else 75

    import backtest as bt
    _mode = bt._get_v3_mode(symbol)
    _mode_label = 'COUNTER_V3' if _mode == 'counter_v3' else 'MOMENTUM_V3'

    _mode_note = {
        'MOMENTUM_V3': (
            'Pool: NKG+POW+FRT+DGW+MBS | '
            'trend_10d (+15d) + vol_trend_10d (+10d) + vol_spike_3d (+10d) + rs_vni_5d_neg (+10d)'
        ),
        'COUNTER_V3': (
            'Pool: MWG+PNJ | '
            'near_52w_low (+10/20d) + rsi_oversold (+10/20d) + rs_vni_5d_neg (+15d) | '
            'Gate: near_52w_low OR oversold'
        ),
    }.get(_mode_label, '')

    send(
        '&#x1F504; Đang chạy <b>ML v3 Backtest + Walk-Forward '
        + symbol + '</b> [' + _mode_label + ']' + NL
        + '<i>So sanh v1 vs v3 — ~5-8 phut...</i>',
        chat_id
    )

    try:
        import sys, os
        bot_dir = os.path.dirname(os.path.abspath(__file__))
        if bot_dir not in sys.path:
            sys.path.insert(0, bot_dir)

        df_sym, _ = bt.load_data(symbol)
        try:
            df_vni, _ = bt.load_data('VNINDEX', days=bt.LOOKBACK_DAYS + 60)
        except Exception:
            df_vni = None

        PHASE_SHORT = {
            2020:'Covid', 2021:'Bull+130%', 2022:'Bear-50%',
            2023:'Phuc hoi', 2024:'On dinh', 2025:'Bien dong', 2026:'2026'
        }

        def _fmt_bt(res, vlabel):
            if not res or not res.get('buy'):
                return None
            buy  = res['buy']
            n_   = buy['total']; wr_ = buy['win_rate']
            pnl_ = buy['avg_pnl']; pf_ = buy['profit_factor']
            pf_s = f'{pf_:.2f}' if pf_ != float('inf') else '∞'
            ci_lo = buy['ci_low']; ci_hi = buy['ci_high']
            v_icon, v_txt = _fmt_verdict(wr_, pnl_, pf_, n_, ci_lo)

            yr_data  = res.get('yearly', {}).get('yearly', {})
            yr_lines = ''
            for yr in sorted(yr_data.keys()):
                if yr == 0: continue
                d = yr_data[yr]
                if d['total'] < 2: continue
                icon = ('&#x2705;' if d['win_rate'] >= 60
                        else '&#x1F7E1;' if d['win_rate'] >= 50
                        else '&#x274C;')
                ph   = PHASE_SHORT.get(yr, str(yr))
                yr_lines += (f' {icon} <b>{yr}</b> ({ph}): '
                             f'WR={d["win_rate"]:.0f}% PnL={d["avg_pnl"]:+.1f}% '
                             f'({d["total"]}L)' + NL)

            grade_lines = ''
            for g in ['STRONG', 'PASS']:
                gs = res.get('grade_stats', {}).get(g)
                if gs and gs['total'] >= 3:
                    gp = f'{gs["profit_factor"]:.2f}' if gs["profit_factor"] != float('inf') else '∞'
                    grade_lines += (f' [{g}] {gs["total"]}L WR={gs["win_rate"]:.0f}% '
                                    f'PnL={gs["avg_pnl"]:+.2f}% PF={gp}' + NL)

            recent = ''
            df_t = res.get('trades')
            if df_t is not None and len(df_t) > 0:
                for _, row in df_t.tail(3).iterrows():
                    ri = '&#x2705;' if row['pnl'] > 0 else '&#x274C;'
                    recent += (f' {ri} {row["date"]} @{row["price"]:,.0f}d '
                               f'ML={row["ml_score"]} [{row["grade"]}] '
                               f'→ {row["pnl"]:+.1f}% ({row["reason"]}, {row["days"]}d)' + NL)

            return (
                '&#x1F4CA; <b>ML ' + vlabel + ' BACKTEST ' + symbol + '</b>'
                + ' [' + _mode_label + ']' + NL
                + '&#x3D;' * 28 + NL + NL
                + (f'&#x2139; <i>{_mode_note}</i>' + NL + NL if vlabel == 'v3' else '')
                + f'&#x2699; SL=-{sl*100:.0f}% TP=+{tp*100:.0f}% Hold={hold}d Score>={score}' + NL + NL
                + f' Lenh: {n_} | TP: {buy["tp"]} | SL: {buy["sl"]} | Het: {buy["expired"]}' + NL
                + f' WR: <b>{wr_}%</b> [{ci_lo}–{ci_hi}%] | PnL TB: <b>{pnl_:+.2f}%</b>' + NL
                + f' PF: <b>{pf_s}</b> | Thang TB: {buy["avg_win"]:+.1f}% | Thua TB: {buy["avg_loss"]:+.1f}%' + NL + NL
                + ('<b>Theo Grade:</b>' + NL + grade_lines + NL if grade_lines else '')
                + '<b>Ket qua theo nam:</b>' + NL + (yr_lines or ' (khong co)') + NL
                + ('<b>3 tin hieu gan nhat:</b>' + NL + recent + NL if recent else '')
                + v_icon + ' <b>' + v_txt + '</b>' + NL
                + '<i>Chua tinh phi GD ~0.5%. Qua khu khong dam bao tuong lai.</i>'
            )

        def _fmt_wf(wf, vlabel):
            if not wf:
                return None
            win_lines = ''
            for w in wf['windows']:
                o_icon = ('&#x2705;' if w['oos_wr'] >= 55
                          else '&#x1F7E1;' if w['oos_wr'] >= 48
                          else '&#x274C;')
                win_lines += (f' {o_icon} OOS {w["oos_label"]}: {w["oos_n"]}L '
                              f'IS {w["is_wr"]:.0f}% → OOS {w["oos_wr"]:.0f}% '
                              f'PnL={w["oos_pnl"]:+.2f}%' + NL)
            return (
                '&#x1F4CA; <b>ML ' + vlabel + ' Walk-Forward ' + symbol
                + f'</b> ({len(wf["windows"])} windows)' + NL
                + '&#x3D;' * 28 + NL + NL
                + f' Avg IS:  WR={wf["avg_is_wr"]:.1f}% PnL={wf["avg_is_pnl"]:+.2f}%' + NL
                + f' Avg OOS: WR={wf["avg_oos_wr"]:.1f}% PnL={wf["avg_oos_pnl"]:+.2f}%'
                + f' (decay={wf["decay_wr"]:+.1f}%)' + NL + NL
                + win_lines + NL
                + '<b>' + wf['verdict_txt'] + '</b>' + NL
                + f'<i>SL={sl*100:.0f}% TP={tp*100:.0f}% Hold={hold}d Score>={score}</i>'
            )

        # Chạy v1 và v3 — dùng cache tránh rate limit
        res_bt1 = bt.run_backtest_momentum(
            symbol, sl=sl, tp=tp, hold_days=hold, min_ml_score=score,
            verbose=False, _df_cache=df_sym, _vni_cache=df_vni)
        res_wf1 = bt.run_walk_forward_momentum(
            symbol, sl=sl, tp=tp, hold_days=hold, min_ml_score=score,
            verbose=False, _df_cache=df_sym, _vni_cache=df_vni)

        res_bt3 = bt.run_backtest_momentum_v3(
            symbol, sl=sl, tp=tp, hold_days=hold, min_ml_score=score,
            verbose=False, _df_cache=df_sym, _vni_cache=df_vni)
        res_wf3 = bt.run_walk_forward_momentum_v3(
            symbol, sl=sl, tp=tp, hold_days=hold, min_ml_score=score,
            verbose=False, _df_cache=df_sym, _vni_cache=df_vni)

        # Gửi kết quả v1 và v3
        for res_bt, res_wf, vlabel in [
            (res_bt1, res_wf1, 'v1'),
            (res_bt3, res_wf3, 'v3'),
        ]:
            msg_bt = _fmt_bt(res_bt, vlabel)
            if msg_bt: send(msg_bt, chat_id)
            msg_wf = _fmt_wf(res_wf, vlabel)
            if msg_wf: send(msg_wf, chat_id)

        # So sánh tổng hợp v1 vs v3
        def _safe_wr(res):
            return res['buy']['win_rate'] if res and res.get('buy') else 0
        def _safe_pf(res):
            if not res or not res.get('buy'): return '-'
            pf = res['buy']['profit_factor']
            return f'{pf:.2f}' if pf != float('inf') else '∞'
        def _safe_n(res):
            return res['buy']['total'] if res and res.get('buy') else 0
        def _safe_oos(wf):
            return f'{wf["avg_oos_wr"]:.0f}%' if wf else '-'
        def _safe_verdict(wf):
            if not wf: return '?'
            v = wf['verdict']
            return '✅' if v == 'Robust' else ('🟡' if 'Chap' in v else '❌')

        compare = (
            '&#x1F50D; <b>SO SANH v1 vs v3 — ' + symbol
            + ' [' + _mode_label + ']</b>' + NL
            + '&#x3D;' * 32 + NL + NL
            + f'{"":12} {"v1":>8}  {"v3":>8}' + NL
            + f'{"IS WR":12} {_safe_wr(res_bt1):>7}%  {_safe_wr(res_bt3):>7}%' + NL
            + f'{"IS PF":12} {_safe_pf(res_bt1):>8}  {_safe_pf(res_bt3):>8}' + NL
            + f'{"So lenh":12} {_safe_n(res_bt1):>8}  {_safe_n(res_bt3):>8}' + NL
            + f'{"OOS WR":12} {_safe_oos(res_wf1):>8}  {_safe_oos(res_wf3):>8}' + NL
            + f'{"WF":12} {_safe_verdict(res_wf1):>8}  {_safe_verdict(res_wf3):>8}' + NL + NL
        )

        oos3 = res_wf3['avg_oos_wr'] if res_wf3 else 0
        oos1 = res_wf1['avg_oos_wr'] if res_wf1 else 0
        pf3  = res_bt3['buy']['profit_factor'] if res_bt3 and res_bt3.get('buy') else 0

        if oos3 >= 55 and oos3 > oos1 + 2 and pf3 >= 1.3:
            concl = '&#x1F7E2; v3 CAI THIEN RO — OOS WR tot hon v1, PF>=1.3'
        elif oos3 >= 55 and oos3 > oos1:
            concl = '&#x1F7E1; v3 CAI THIEN NHE — OOS WR nhinh hon v1'
        elif oos3 >= 50:
            concl = '&#x1F7E1; v3 OOS chap nhan duoc — can theo doi live them'
        else:
            concl = '&#x274C; v3 chua dat target — pooled pattern chua hop voi ma nay'

        compare += concl
        send(compare, chat_id)

    except Exception as e:
        logger.error('mlbtv3 ' + symbol + ': ' + str(e))
        import traceback
        logger.error(traceback.format_exc())
        send('&#x274C; Loi ML v3 Backtest ' + symbol + ': ' + str(e)[:150], chat_id)



    NL = chr(10)

    # Parse args
    sl, tp, hold, sc = None, None, None, None
    syms = []
    for a in args:
        al = a.lower()
        if al.startswith('sl='):
            try: sl = float(al[3:]) / 100
            except ValueError: pass
        elif al.startswith('tp='):
            try: tp = float(al[3:]) / 100
            except ValueError: pass
        elif al.startswith('hold='):
            try: hold = int(al[5:])
            except ValueError: pass
        elif al.startswith('s='):
            try: sc = int(al[2:])
            except ValueError: pass
        elif al not in ('', 'all'):
            syms.append(a.upper())

    if not args or (len(args) == 1 and args[0].strip() == ''):
        send(
            '&#x1F4CA; <b>Lệnh /mlbtv3 — ML Backtest v3 (Event Study Driven)</b>' + NL + NL
            + ' /mlbtv3 NKG       — BT + WF commodity_v3 (~5 phút)' + NL
            + ' /mlbtv3 FRT       — BT + WF retail_v3 (~5 phút)' + NL
            + ' /mlbtv3 MWG       — BT + WF retail_v3_mwg counter-trend (~5 phút)' + NL
            + ' /mlbtv3 POW       — BT + WF commodity_v3_pow (~5 phút)' + NL
            + ' /mlbtv3 MBS       — BT + WF broker_v3_mbs (~5 phút)' + NL
            + ' /mlbtv3 PNJ       — BT + WF retail_v3_pnj counter-trend (~5 phút)' + NL
            + ' /mlbtv3 DGW       — BT + WF retail_v3_dgw vol momentum (~5 phút)' + NL
            + ' /mlbtv3 all       — Tất cả 7 mã có event study (~35 phút)' + NL
            + ' /mlbtv3 NKG sl=5 tp=15 — Custom SL/TP' + NL + NL
            + '<i>v3 event study confirmed (7 mã):</i>' + NL
            + ' NKG/POW: trend_10d momentum | FRT/DGW: vol momentum' + NL
            + ' MWG/PNJ: counter-trend (near_52w_low + rsi_oversold)' + NL
            + ' MBS: rs_vni_5d am (broker counter-momentum)',
            chat_id
        )
        return

    # all → chạy NKG + FRT tuần tự
    if 'all' in [a.lower() for a in args]:
        import threading
        t = threading.Thread(
            target=_handle_mlbtv3_all,
            args=(chat_id, sl, tp, hold, sc),
            daemon=True
        )
        t.start()
        return

    # 1 mã cụ thể
    sym_raw = syms[0] if syms else (args[0].upper() if args else '')
    if not sym_raw:
        send('Cú pháp: <b>/mlbtv3 NKG</b> hoặc <b>/mlbtv3 all</b>', chat_id)
        return

    import threading
    t = threading.Thread(
        target=_handle_mlbtv3_symbol,
        args=(sym_raw, chat_id, sl, tp, hold, sc),
        daemon=True
    )
    t.start()




def _handle_mlbtv3_all(chat_id, custom_sl=None, custom_tp=None,
                        custom_hold=None, custom_score=None):
    """Chạy ML v3 BT + WF cho toàn MLBT_V3_WATCHLIST.

    Watchlist gồm 3 nhóm:
      confirmed   : 7 mã có event study (momentum_v3 + counter_v3)
      v1-elim     : mã bị loại qua 11 đợt backtest v1
      new         : gợi ý mới chưa test

    Output: block đầy đủ mỗi mã + bảng tổng kết + danh sách promising.
    Rate limit: tuần tự + sleep(3s)/mã, fetch data 1 lần/mã.
    """
    NL    = chr(10)
    sl    = custom_sl    if custom_sl    is not None else 0.06
    tp    = custom_tp    if custom_tp    is not None else 0.17
    hold  = custom_hold  if custom_hold  is not None else 18
    score = custom_score if custom_score is not None else 75

    SOURCE_ORDER = ['confirmed', 'v1-elim', 'new']
    V3_SYMS = sorted(
        MLBT_V3_WATCHLIST.keys(),
        key=lambda s: (SOURCE_ORDER.index(MLBT_V3_WATCHLIST[s][0])
                       if MLBT_V3_WATCHLIST[s][0] in SOURCE_ORDER else 99,
                       s)
    )

    counts = {}
    for s in V3_SYMS:
        src = MLBT_V3_WATCHLIST[s][0]
        counts[src] = counts.get(src, 0) + 1

    count_str = ' | '.join(f'{src}: {n}m' for src, n in
                           sorted(counts.items(), key=lambda x: SOURCE_ORDER.index(x[0])
                                  if x[0] in SOURCE_ORDER else 99))

    send(
        f'&#x1F4CA; <b>ML v3 — {len(V3_SYMS)} mã (MLBT_V3_WATCHLIST)</b>' + NL
        + f'<i>SL={sl*100:.0f}% TP={tp*100:.0f}% Hold={hold}d Score>={score}</i>' + NL
        + f'<i>{count_str}</i>' + NL
        + '<i>~2-3h (tuan tu) — gui ket qua theo nhom khi xong...</i>',
        chat_id
    )

    try:
        import sys, os
        bot_dir = os.path.dirname(os.path.abspath(__file__))
        if bot_dir not in sys.path:
            sys.path.insert(0, bot_dir)
        import backtest as bt

        try:
            df_vni, _ = bt.load_data('VNINDEX', days=bt.LOOKBACK_DAYS + 60)
        except Exception:
            df_vni = None

        PHASE_SHORT = {
            2020:'Covid', 2021:'Bull+130%', 2022:'Bear-50%',
            2023:'Phuc hoi', 2024:'On dinh', 2025:'Bien dong', 2026:'2026'
        }

        def _fmt_v3_row(sym, r1, w1, r3, w3, source, pattern_mode):
            """Format block đầy đủ cho 1 mã — đủ thông tin phân tích v3."""
            note = MLBT_V3_WATCHLIST.get(sym, ('?', '?', ''))[2]

            # v1 stats
            if r1:
                b1 = r1['buy']
                n1,wr1,pf1,pnl1 = b1['total'],b1['win_rate'],b1['profit_factor'],b1['avg_pnl']
                pf1s = f'{pf1:.2f}' if pf1 != float('inf') else '∞'
            else:
                n1=0; wr1=0; pf1=0; pnl1=0; pf1s='-'

            # v3 stats
            if r3:
                b3 = r3['buy']
                n3,wr3,pf3,pnl3 = b3['total'],b3['win_rate'],b3['profit_factor'],b3['avg_pnl']
                aw3,al3 = b3['avg_win'],b3['avg_loss']
                ci_lo,ci_hi = b3['ci_low'],b3['ci_high']
                pf3s = f'{pf3:.2f}' if pf3 != float('inf') else '∞'
            else:
                n3=0; wr3=0; pf3=0; pnl3=0; aw3=0; al3=0; ci_lo=0; ci_hi=100; pf3s='-'

            # WF stats
            oos1s = f'{w1["avg_oos_wr"]:.0f}%' if w1 else '-'
            oos3s = f'{w3["avg_oos_wr"]:.0f}%' if w3 else '-'
            dec1s = f'{w1["decay_wr"]:+.0f}%' if w1 else '-'
            dec3s = f'{w3["decay_wr"]:+.0f}%' if w3 else '-'
            vrd3  = w3['verdict'] if w3 else '?'

            # n_OOS tổng — cảnh báo nếu quá nhỏ
            n_oos3 = sum(w['oos_n'] for w in w3['windows']) if w3 else 0
            n_warn = ' ⚠n_OOS nhỏ' if n_oos3 < 15 else f' (n_OOS={n_oos3}L)'

            # Yearly v3 — 4 năm gần nhất
            yr_str = ''
            if r3:
                yr_data = r3.get('yearly', {}).get('yearly', {})
                yr_parts = []
                for yr in sorted(yr_data.keys(), reverse=True)[:4]:
                    if yr == 0: continue
                    d = yr_data[yr]
                    if d['total'] < 2: continue
                    ico = ('✅' if d['win_rate'] >= 60
                           else '🟡' if d['win_rate'] >= 50 else '❌')
                    ph = PHASE_SHORT.get(yr, str(yr))
                    yr_parts.append(
                        f'{ico}{yr}({ph}):WR={d["win_rate"]:.0f}%'
                        f'/PnL={d["avg_pnl"]:+.1f}%({d["total"]}L)'
                    )
                yr_str = ' | '.join(yr_parts)

            # Grade v3
            grd_str = ''
            if r3:
                for g in ['STRONG', 'PASS']:
                    gs = r3.get('grade_stats', {}).get(g)
                    if gs and gs['total'] >= 3:
                        gp = f'{gs["profit_factor"]:.2f}' if gs['profit_factor'] != float('inf') else '∞'
                        grd_str += f'[{g}]{gs["total"]}L WR={gs["win_rate"]:.0f}% PF={gp}  '

            # Verdict
            pf3_ok  = pf3 > pf1 * 1.05 if pf1 and pf3 else (pf3 > 1.3 if pf3 else False)
            oos3_ok = (w3 and w1 and w3['avg_oos_wr'] > w1['avg_oos_wr'] + 2) if w1 else (w3 and w3['avg_oos_wr'] >= 55 if w3 else False)
            oos3_abs = w3 and w3['avg_oos_wr'] >= 50 and n_oos3 >= 15
            if pf3_ok and oos3_ok and oos3_abs:
                v_icon = '&#x1F7E2;'; v_txt = 'V3 CẢI THIỆN RÕ'
            elif (pf3_ok or oos3_ok) and oos3_abs:
                v_icon = '&#x1F7E1;'; v_txt = 'V3 CẢI THIỆN NHẸ'
            elif oos3_abs:
                v_icon = '&#x1F7E1;'; v_txt = 'V3 OOS ổn'
            elif n_oos3 < 15 and w3 and w3['avg_oos_wr'] >= 50:
                v_icon = '&#x26A0;'; v_txt = 'OOS cao nhưng n nhỏ'
            else:
                v_icon = '&#x274C;'; v_txt = 'V3 không cải thiện'

            return (
                f'{v_icon} <b>{sym}</b> [{source}/{"CTR" if pattern_mode=="counter_v3" else "MOM"}]  {v_txt}{n_warn}' + NL
                + (f'  <i>{note}</i>' + NL if note else '')
                + f'  v1: {n1}L WR={wr1}% PF={pf1s} PnL={pnl1:+.2f}% | OOS={oos1s} decay={dec1s}' + NL
                + f'  v3: {n3}L WR={wr3}% <b>PF={pf3s}</b> PnL={pnl3:+.2f}%'
                + f' [{ci_lo}-{ci_hi}%] | <b>OOS={oos3s}</b> decay={dec3s} [{vrd3}]' + NL
                + (f'  Win={aw3:+.1f}% Loss={al3:+.1f}%' + NL if r3 else '')
                + (f'  Grade: {grd_str}' + NL if grd_str else '')
                + (f'  Năm: {yr_str}' + NL if yr_str else '')
            ), oos3_abs

        # ── Chạy tuần tự, group theo source, gửi kết quả sau mỗi nhóm ────────
        all_results = {}
        promising   = []  # (sym, source, pattern, pf3s, wr3, oos3s, vrd3)

        for source_key in SOURCE_ORDER:
            batch_syms = [s for s in V3_SYMS
                          if MLBT_V3_WATCHLIST[s][0] == source_key]
            if not batch_syms:
                continue

            send(f'&#x23F3; Đang chạy <b>{source_key}</b> ({len(batch_syms)} mã)...', chat_id)

            batch_msgs = []
            for sym in batch_syms:
                source, pattern, _ = MLBT_V3_WATCHLIST[sym]
                _mode = bt._get_v3_mode(sym)   # auto-detect: momentum_v3 / counter_v3
                try:
                    df_sym, _ = bt.load_data(sym, days=bt.LOOKBACK_DAYS)
                    if df_sym is None:
                        all_results[sym] = (None, None, None, None)
                        batch_msgs.append(f'&#x26AA; <b>{sym}</b>: khong co du lieu' + NL)
                        time.sleep(3)
                        continue

                    r1 = bt.run_backtest_momentum(
                        sym, sl=sl, tp=tp, hold_days=hold, min_ml_score=score,
                        verbose=False, _df_cache=df_sym, _vni_cache=df_vni)
                    w1 = bt.run_walk_forward_momentum(
                        sym, sl=sl, tp=tp, hold_days=hold, min_ml_score=score,
                        verbose=False, _df_cache=df_sym, _vni_cache=df_vni)
                    r3 = bt.run_backtest_momentum_v3(
                        sym, sl=sl, tp=tp, hold_days=hold, min_ml_score=score,
                        verbose=False, _df_cache=df_sym, _vni_cache=df_vni,
                        sector_mode=_mode)
                    w3 = bt.run_walk_forward_momentum_v3(
                        sym, sl=sl, tp=tp, hold_days=hold, min_ml_score=score,
                        verbose=False, _df_cache=df_sym, _vni_cache=df_vni,
                        sector_mode=_mode)

                    all_results[sym] = (r1, w1, r3, w3)
                    msg, is_promising = _fmt_v3_row(sym, r1, w1, r3, w3, source, _mode)
                    batch_msgs.append(msg)

                    if is_promising:
                        pf3 = r3['buy']['profit_factor'] if r3 else 0
                        wr3 = r3['buy']['win_rate'] if r3 else 0
                        oos3s = f'{w3["avg_oos_wr"]:.0f}%' if w3 else '-'
                        vrd3  = w3['verdict'] if w3 else '?'
                        pf3s  = f'{pf3:.2f}' if pf3 and pf3 != float('inf') else '∞'
                        promising.append((sym, source, _mode, pf3s, wr3, oos3s, vrd3))

                    logger.info(f'mlbtv3_all: {sym} xong')
                except Exception as e_sym:
                    logger.error(f'mlbtv3_all {sym}: {e_sym}')
                    all_results[sym] = (None, None, None, None)
                    batch_msgs.append(f'&#x274C; <b>{sym}</b>: loi — {str(e_sym)[:60]}' + NL)

                time.sleep(3)

            # Gửi kết quả batch
            chunk = f'<b>── {source_key} ({len(batch_syms)} mã) ──</b>' + NL + NL
            for msg in batch_msgs:
                if len(chunk) + len(msg) > 3800:
                    send(chunk, chat_id)
                    chunk = msg
                else:
                    chunk += msg + NL
            if chunk.strip():
                send(chunk, chat_id)

        # ── Bang tong ket ────────────────────────────────────
        summary_header = (
            '&#x1F4CA; <b>ML v3 ALL — Tong ket ' + str(len(V3_SYMS)) + ' ma</b>' + NL
            + '&#x3D;' * 32 + NL + NL
            + f'{"Ma":<5} {"Src":<10} {"Type":<5} {"v3 PF":>6} '
              f'{"v3 WR":>6} {"v1 OOS":>7} {"v3 OOS":>7} {"WF":>8}  Verdict' + NL
            + '-' * 64 + NL
        )
        summary_rows = ''

        for sym in V3_SYMS:
            r1, w1, r3, w3 = all_results.get(sym, (None,None,None,None))
            source, pattern, _ = MLBT_V3_WATCHLIST[sym]
            _ptype = 'CTR' if pattern == 'counter_v3' else 'MOM'
            if not r3:
                summary_rows += f'{sym:<5} {source[:10]:<10} — khong co du lieu' + NL
                continue

            b3  = r3['buy']
            pf3 = b3['profit_factor']; wr3 = b3['win_rate']
            pf3s= f'{pf3:.2f}' if pf3 != float('inf') else '∞'
            oos1= f'{w1["avg_oos_wr"]:.0f}%' if w1 else '-'
            oos3= f'{w3["avg_oos_wr"]:.0f}%' if w3 else '-'
            vrd3= w3['verdict'][:5] if w3 else '?'
            n_oos3 = sum(w['oos_n'] for w in w3['windows']) if w3 else 0

            pf1 = r1['buy']['profit_factor'] if r1 else 0
            pf3_ok  = pf3 > pf1 * 1.05 if pf1 and pf3 else (pf3 > 1.3 if pf3 else False)
            oos3_ok = w3 and w1 and w3['avg_oos_wr'] > (w1['avg_oos_wr'] if w1 else 0) + 2
            oos3_abs= w3 and w3['avg_oos_wr'] >= 50 and n_oos3 >= 15

            if pf3_ok and oos3_ok and oos3_abs:    icon = '&#x1F7E2;'
            elif (pf3_ok or oos3_ok) and oos3_abs: icon = '&#x1F7E1;'
            elif oos3_abs:                          icon = '&#x1F7E1;'
            elif n_oos3 < 15 and w3 and w3['avg_oos_wr'] >= 50:
                                                    icon = '&#x26A0;'
            else:                                   icon = '&#x274C;'

            n_tag = ' ⚠' if n_oos3 < 15 else ''
            summary_rows += (
                f'{sym:<5} {source[:10]:<10} {_ptype:<5} {pf3s:>6} '
                f'{wr3:>5}% {oos1:>7} {oos3:>7} {vrd3:>8}  {icon}{n_tag}' + NL
            )

        send(summary_header + summary_rows, chat_id)

        # ── Danh sach promising (OOS≥50%, n≥15) ────────────────────────────
        if promising:
            prom_msg = (
                '&#x1F50D; <b>Ma dang chu y — OOS v3 >= 50% (n>=15L)</b>' + NL
                + '&#x3D;' * 28 + NL + NL
            )
            for sym, source, _mode, pf3s, wr3, oos3s, vrd3 in promising:
                r1, w1, r3, w3 = all_results.get(sym, (None,None,None,None))
                decay = f'{w3["decay_wr"]:+.0f}%' if w3 else '-'
                pnl3  = f'{r3["buy"]["avg_pnl"]:+.2f}%' if r3 else '-'
                n_oos = sum(w['oos_n'] for w in w3['windows']) if w3 else 0
                _pt   = 'CTR' if _mode == 'counter_v3' else 'MOM'
                prom_msg += (
                    f'&#x1F7E2; <b>{sym}</b> [{source}/{_pt}]' + NL
                    + f'  PF={pf3s} WR={wr3}% OOS={oos3s} decay={decay} PnL={pnl3}' + NL
                    + f'  WF: {vrd3} | n_OOS={n_oos}L' + NL + NL
                )
            prom_msg += (
                '<i>Tieu chi Tier B v3: OOS>=55% + PF>=1.3 + decay<15% + n_OOS>=15</i>'
            )
            send(prom_msg, chat_id)
        else:
            send('&#x26AA; Không có mã nào đạt OOS≥50% với n_OOS≥15L.', chat_id)

    except Exception as e:
        logger.error('_handle_mlbtv3_all: ' + str(e))
        import traceback
        logger.error(traceback.format_exc())
        send('&#x274C; Lỗi ML v3 all: ' + str(e)[:120], chat_id)




def _handle_optimize(symbol, chat_id):
    """Tìm SL/TP/Hold/Score tối ưu voi Walk-Forward validation."""
    NL = chr(10)
    send('&#x1F50D; Đang chạy <b>Optimize ' + symbol + '</b>...'
         + NL + '<i>Grid search 144 combos + WF validation, ~3-5 phut</i>',
         chat_id)
    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from backtest import run_optimize_symbol

        results = run_optimize_symbol(symbol, verbose=False)
        if not results:
            send('&#x274C; ' + symbol + ': Không đủ dữ liệu hoặc không co combo hop le', chat_id)
            return

        per_score = results.get('per_score', [])
        ob        = results.get('overall', {})

        st_icon = {
            'ROBUST':  '&#x2705;',
            'THIN':    '&#x26A0; it lệnh',
            'NO_WF':   '&#x2014;',
            'OVERFIT': '&#x274C;',
        }

        lines = [
            f'&#x1F50D; <b>Optimize {symbol} — Best per Score</b>',
            f'<i>IS=in-sample | OOS=out-of-sample (WF)</i>',
            '',
            f'<b>S&gt;=  SL   TP  Hold | IS              | OOS</b>',
            '&#x2500;' * 22,
        ]

        # Kiểm tra xem có rows nào bị trùng IS result không
        _is_signatures = set()
        _has_duplicates = False
        for r in per_score:
            _sig = (r['n_is'], r['wr_is'], r['pnl_is'])
            if _sig in _is_signatures:
                _has_duplicates = True
            _is_signatures.add(_sig)

        for r in per_score:
            is_s  = f"{r['n_is']}L {r['wr_is']:.0f}% {r['pnl_is']:+.1f}%"
            oos_s = (f"{r['n_oos']}L {r['wr_oos']:.0f}% {r['pnl_oos']:+.1f}%"
                     if r['n_oos'] > 0 else 'N/A')
            st    = st_icon.get(r['oos_status'], '?')
            win   = ' &#x1F3AF;' if (ob and r['score'] == ob['score']
                                     and r['sl'] == ob['sl']) else ''
            # Thêm warning nếu OOS quá ít
            oos_warn = ' &#x26A0;ít' if 0 < r['n_oos'] < 12 else ''
            lines.append(
                f"{r['score']:2d}  {r['sl']:2d}%  {r['tp']:2d}%  {r['hold']:2d}p"
                f" | {is_s:15} | {oos_s} {st}{oos_warn}{win}"
            )

        if _has_duplicates:
            lines.append('<i>⚠ Một số ngưỡng score cho cùng tập lệnh (Score A</i>')
            lines.append('<i>  không có lệnh trong vùng 55-64 — score bị phân cực)</i>')

        lines.append('')

        if ob:
            if ob['oos_status'] == 'ROBUST':
                lines.append(
                    f'&#x1F4A1; <b>Khuyến nghị: S&gt;={ob["score"]}'
                    f' SL={ob["sl"]}% TP={ob["tp"]}% Hold={ob["hold"]}p</b>'
                )
                lines.append(f'   OOS: {ob["n_oos"]}L WR={ob["wr_oos"]}% PnL={ob["pnl_oos"]:+.2f}%')
                lines.append(
                    f'   Test: /bt {symbol}'
                    f' s={ob["score"]} sl={ob["sl"]} tp={ob["tp"]} hold={ob["hold"]}'
                )
            elif ob['oos_status'] in ('THIN', 'NO_WF'):
                lines.append(f'&#x26A0; OOS qua it lệnh ({ob["n_oos"]}L) — dung IS lam tham khao')
                lines.append(f'   IS tot nhat: S&gt;={ob["score"]} SL={ob["sl"]}% TP={ob["tp"]}% Hold={ob["hold"]}p')
            else:
                lines.append('&#x274C; Tất cả combos overfit — giu config mac dinh')

        lines.append('')
        lines.append('<i>/bt SYM s=X sl=X tp=X hold=X de test thu</i>')
        send(NL.join(lines), chat_id)

    except Exception as e:
        import traceback
        send('&#x274C; Lỗi Optimize: ' + str(e)[:200], chat_id)


def handle_start(chat_id):
    # Kick Flask warmup ngay khi user bắt đầu dùng bot
    try:
        call_api('/api/warmup')
    except Exception:
        pass
    msg = (
        '<b>VN Trader Bot v4.4</b> — Chao mung!\n\n'
        '<b>&#x1F4CB; QUY TRINH (doc truoc khi dung):</b>\n'



        'T4 Auto alerts (không can lam gi)\n'
        'T5 Cuoi tuan: /bt /mlbt (research)\n\n'
                '<b>Lenh thuong dung:</b>\n'
                '/price VCB - Giá hiện tại\n'
        '/analyze FPT - Phân tích day du 8 lop\n'
        '/score FPT    - Tuong tu /analyze (alias)\n'


        
        '/bt MBB              — Backtest compact (~3 phut)\n'
        '/bt MBB s=60 sl=5 tp=20 — Custom score/SL/TP\n'
        '/bt all              — Cả watchlist (~15 phut)\n'
        '/optimize MBB        — Tim SL/TP/Hold tối ưu (~5 phut)\n'




        '/macro        — Systemic Risk Score (VN market)\n'


        '/signals      — Top tín hiệu hôm nay\n'

        '<b>Smart Money (Shark Detector v4):</b>\n'

        '/foreign DGC  — Chi tiết Khối ngoại 10 phiên (mua/bán ròng theo ngày)\n'
        '/ml DGC       — Momentum Leader: hệ thống chấm điểm 2-tier (0-120đ, độc lập Score A)\n'
        '/mlscan       — Quét ML toàn watchlist 30 mã\n'
        '/mlscan extended — Quét mở rộng ~49 mã\n'
        '/mlbt DGC    — Backtest Momentum Leader (SL/TP/Hold tối ưu riêng)\n'
        '/mlbtv3 NKG  — ML v3 Pooled Event Study (momentum_v3/counter_v3, moi ma)\n'
        '/mlscreen batch1 — Systematic V1 screening toan HOSE (batch ~70 ma)\n'
        '/mlbt all    — ML backtest toan watchlist\n'
            + '/mrabt <SYM>    — MRA backtest (Trigger A: high 5d)\n'
            + '/mrabt <SYM> c  — Trigger C (MA20+1.5% micro-breakout)\n'
            + '/mrabt all      — MRA batch test toan watchlist\n'
        '/eventstudy NKG — Event Study: pattern trước big moves (P3)\n'
        '/eventstudy all — Event Study toàn bộ 10 mã Sector-Map\n'
        '/sharkbt DGC  — Backtest Shark Score vs Score A (tim ngưỡng tối ưu)\n'
        '/scan         — Market Scanner ~250 ma HOSE/HNX (top 10 tiem nang)\n\n'
        '<b>Quan ly theo doi:</b>\n'
        '/subscribe    — Dang ky nhan alert tu dong\n'
        '/unsubscribe  — Huy dang ky alert\n'
        '/subscribers  — Danh sach nguoi theo doi (admin)\n\n'

        'Không phải tư vấn đầu tư</i>'
    )
    send(msg, chat_id)


def handle_price(symbol, chat_id):
    send('Dang lay gia ' + symbol + '...', chat_id)
    d = call_api('/api/price/' + symbol)
    if d.get('price', 0) > 0:
        chg = d.get('change_pct', 0)
        arr = '+' if chg >= 0 else ''
        send('<b>' + symbol + '</b>\nGiá: <b>' + f'{d["price"]:,.0f}' + 'd</b>\nThay doi: ' + arr + f'{chg:.2f}%', chat_id)
    else:
        send(symbol + ': ' + d.get('error', 'Không lay duoc gia'), chat_id)


def _send_market_context(symbol, api_data, chat_id):
    """
    Gửi phân tích (B) — Đặc tính thị trường VN.
    Chạy trong thread riêng để không block.
    """
    try:
        import sys, os
        bot_dir = os.path.dirname(os.path.abspath(__file__))
        if bot_dir not in sys.path:
            sys.path.insert(0, bot_dir)
        mc = _mc

        # Lấy data từ vnstock để tính Wyckoff + Wick
        from backtest import load_data
        import importlib, backtest as bt
        df, _ = bt.load_data(symbol, days=200)  # 200 ngày calendar = ~140 phiên

        if df is None:
            return

        price     = api_data.get('price', 0)
        vol_ratio = api_data.get('vol_ratio', 1.0)
        score     = api_data.get('score', 50)

        ctx = mc.build_market_context(df, symbol, price, vol_ratio, score)
        ctx_txt = mc.format_market_context_msg(ctx, symbol)

        msg = (
            '&#x1F1FB;&#x1F1F3; <b>(B) DAC TINH THI TRUONG VN: ' + symbol + '</b>\n'
            + '=' * 30 + '\n\n'
            + ctx_txt + '\n\n'
            + '<i>Phân tích theo Blueprint VN Trading Signal\n'
            + 'Liquidity Tier | Wick Filter | Weekend Rule | Wyckoff Phase</i>'
        )
        send(msg, chat_id)

    except Exception as e:
        logger.error(f'market_context {symbol}: {e}')
        import traceback
        logger.error(traceback.format_exc())


def handle_analyze(symbol, chat_id):
    send('Đang phân tích <b>' + symbol + '</b>...', chat_id)
    d = call_api('/api/analyze/' + symbol)
    if 'error' in d:
        send(symbol + ': ' + d['error'], chat_id)
        return

    def run():
        try:
            import sys, os, importlib, traceback
            bot_dir = os.path.dirname(os.path.abspath(__file__))
            if bot_dir not in sys.path: sys.path.insert(0, bot_dir)
            mc = _mc
            import backtest as bt

            # Lấy B-filter context
            b_ctx  = None
            b_err  = ''
            try:
                df, src_name = bt.load_data(symbol, days=300)  # 300 ngày = ~120 phiên GD, đủ cho Wyckoff 90 nến
                if df is not None:
                    b_ctx = mc.build_market_context(
                        df, symbol,
                        d.get('price', 0),
                        d.get('vol_ratio', 1.0),
                        d.get('score', 50)
                    )
                else:
                    b_err = 'load_data tra ve None'
            except Exception as ex:
                b_err = str(ex)[:120]
                logger.warning('b_ctx ' + symbol + ': ' + b_err)

            # Tính b_delta/b_details trong scope run() để dùng cho tin 2
            score      = d.get('score', 50)
            b_delta    = 0
            b_details  = []
            score_adj  = score
            if b_ctx:
                try:
                    b_delta, _bf, b_details = _mc.calc_b_adjustment(b_ctx)
                    score_adj = max(0, min(100, score + b_delta))
                except Exception:
                    pass

            # Fetch WF summary (cached 24h, non-blocking nếu đã cache)
            try:
                from backtest import get_wf_summary
                d['wf_summary'] = get_wf_summary(symbol) or {}
            except Exception:
                d['wf_summary'] = {}

            # Gửi tin 1: A+B tổng hợp
            send(build_analysis_msg(d, b_ctx=b_ctx), chat_id)

            # Gửi tin 2: chi tiết TT VN + tóm tắt B-adjustment
            if b_ctx:
                ctx_txt       = mc.format_market_context_msg(b_ctx, symbol)
                overall       = b_ctx.get('overall', '')
                overall_emoji = b_ctx.get('overall_emoji', '&#x1F1FB;&#x1F1F3;')

                # Thêm dòng tóm tắt B-adjustment nếu có điều chỉnh
                b_summary = ''
                if b_delta != 0 and b_details:
                    b_lines = []
                    for bd in b_details:
                        sign = '+' if bd['delta'] > 0 else ''
                        b_lines.append(bd['icon'] + ' ' + bd['label']
                                       + ': <b>' + sign + str(bd['delta']) + 'd</b>')
                    sign_total = '+' if b_delta > 0 else ''
                    b_summary = (
                        '\n\n📊 <b>Dieu chinh B-filter:</b>\n'
                        + '\n'.join(b_lines)
                        + '\n→ Tong: <b>' + sign_total + str(b_delta) + 'd</b>'
                        + ' (' + str(score) + ' → <b>' + str(score_adj) + '</b>)'
                    )

                send(
                    overall_emoji + ' <b>Dieu kien TT VN: ' + symbol + '</b>\n'
                    + '=' * 28 + '\n\n'
                    + ctx_txt
                    + b_summary + '\n\n'
                    + '<i>Liquidity | Wick | Weekend | Wyckoff</i>',
                    chat_id
                )
            else:
                # Thông báo lý do không có tin 2
                send(
                    '&#x2139; <b>TT VN ' + symbol + ':</b> Không tải được dữ liệu B-filter\n'
                    + ('<i>' + b_err + '</i>' if b_err else '<i>load_data that bai</i>'),
                    chat_id
                )
        except Exception as e:
            logger.error('handle_analyze ' + symbol + ': ' + str(e))
            logger.error(traceback.format_exc())
            # Fallback: gửi chỉ A
            send(build_analysis_msg(d), chat_id)
            send('⚠ Loi B-filter: ' + str(e)[:100], chat_id)

    threading.Thread(target=run, daemon=True).start()


def handle_signals(chat_id):
    send('Đang quét tín hiệu thị trường...', chat_id)
    # Regime warning ở đầu signals
    try:
        from backtest import get_market_regime
        _reg = get_market_regime()
        if _reg.get('regime') == 'BEAR':
            send(
                '&#x1F534; <b>CANH BAO: BEAR MARKET</b>' + chr(10)
                + 'VNI=' + f'{_reg["vni"]:,.0f}' + ' duoi MA200=' + f'{_reg["ma200"]:,.0f}' + chr(10)
                + '<i>Hệ thống da cap score xuong 58 — uu tien THEO DOI, giam size.</i>',
                chat_id
            )
        elif _reg.get('regime') == 'BULL':
            send(
                '&#x1F7E2; <b>BULL MARKET</b> — VNI>MA50>MA200' + chr(10)
                + '<i>+3 bonus cho ma score 62-64 sat ngưỡng.</i>',
                chat_id
            )
    except Exception:
        pass
    data = call_api('/api/signals')
    if not data:
        send('&#x274C; Flask chưa khởi động xong. Thử lại sau 30 giây.\n'
             'Hoặc dùng: /analyze VCB', chat_id)
        return

    # FIX: Xử lý cache_warming — app.py giờ trả ngay, không block
    # Nếu cache chưa đủ → thông báo và retry 1 lần sau 35s
    _cache_warming = False
    if isinstance(data, dict):
        _cache_warming = data.get('cache_warming', False)
        _cache_ready   = data.get('cache_ready', 0)
        _cache_total   = data.get('cache_total', len(WATCHLIST_META))

    if _cache_warming:
        import time as _t
        send(
            f'&#x23F3; Cache đang khởi động ({_cache_ready}/{_cache_total} mã sẵn sàng).\n'
            'Đang chờ 35 giây rồi thử lại tự động...', chat_id
        )
        _t.sleep(35)
        data = call_api('/api/signals')
        if not data:
            send('&#x274C; Không lấy được tín hiệu. Thử lại sau 1-2 phút.', chat_id)
            return

    # Tính Macro Risk Score nhanh để gắn vào đầu signals
    _macro_prefix = ''
    try:
        import sys, os
        bot_dir = os.path.dirname(os.path.abspath(__file__))
        if bot_dir not in sys.path: sys.path.insert(0, bot_dir)
        mc = _mc
        import backtest as bt, importlib, numpy as np, pandas as pd
        vn_df, _ = bt.load_data('VNINDEX', days=60)
        vn_ma20 = 0
        if vn_df is not None:
            cc = bt.find_col(vn_df, ['close','closeprice','close_price'])
            if cc:
                closes = pd.to_numeric(vn_df[cc], errors='coerce').fillna(0).values
                if closes.max() < 100: closes = closes * 1000
                vn_ma20 = float(np.mean(closes[-20:])) if len(closes) >= 20 else 0
        market_raw = call_api('/api/market') or {}
        vn_raw = market_raw.get('VNINDEX', {})
        vnindex_data = {'price': vn_raw.get('price',0), 'change_pct': vn_raw.get('change_pct',0), 'ma20': vn_ma20}
        _sig_list = data.get('signals', data) if isinstance(data, dict) else data
        watchlist_scores = [{'symbol': x.get('symbol',''), 'score': x.get('score',50), 'action': x.get('action','')} for x in (_sig_list or [])]
        macro = mc.analyze_macro_risk(vnindex_data, watchlist_scores)
        _macro_prefix = (
            macro['emoji'] + ' <b>Macro Risk: ' + macro['status']
            + ' (' + str(macro['score']) + '/100)</b> — ' + macro['action'] + '\n'
        )
    except Exception:
        pass

    # Lọc chỉ mã trong WATCHLIST_META + kiểm tra score >= score_min
    # Fallback: mã Tier 1 không có trong cache → gọi /api/analyze riêng
    wl_signals  = []  # Tín hiệu hợp lệ trong watchlist
    skipped     = []  # Mã watchlist có tín hiệu nhưng score chưa đủ

    # Index data từ /api/signals theo symbol
    # api_signals giờ trả về {signals:[], breadth:{}} hoặc list cũ
    _breadth_data = {}
    if isinstance(data, dict) and 'signals' in data:
        _breadth_data = data.get('breadth', {})
        data = data.get('signals', [])
    data_by_sym = {d.get('symbol', ''): d for d in (data or [])}

    # Breadth sẽ được tính SAU khi có wl_signals đầy đủ — xem cuối loop

    # Đảm bảo tất cả mã Tier 1 đều được xét dù cache có hay không
    for sym, meta in WATCHLIST_META.items():
        item = data_by_sym.get(sym)
        if item is None:
            # Không có trong cache — gọi trực tiếp
            fallback = call_api('/api/analyze/' + sym)
            if fallback and 'error' not in fallback:
                item = fallback
            else:
                continue
        score  = item.get('score', 0)
        action = item.get('action', '')
        item['symbol'] = sym  # đảm bảo có key symbol

        # ── Soft filter (B): điều chỉnh score theo đặc tính thị trường VN ──
        b_penalty  = 0
        b_warnings = []
        try:
            import sys, os
            bot_dir = os.path.dirname(os.path.abspath(__file__))
            if bot_dir not in sys.path: sys.path.insert(0, bot_dir)
            mc = _mc
            import backtest as bt_mod, importlib
            df_b, _ = bt_mod.load_data(sym, days=200)
            if df_b is not None:
                ctx_b = mc.build_market_context(df_b, sym,
                            item.get('price', 0),
                            item.get('vol_ratio', 1.0), score)
                # Dùng hàm chung calc_b_adjustment (cộng/trừ nhất quán)
                _b_delta, _b_flags, _b_dets = _mc.calc_b_adjustment(ctx_b)
                b_penalty = -_b_delta  # âm = cộng điểm, dương = trừ điểm
                b_warnings = [
                    ('+' if d['delta'] > 0 else '') + str(d['delta'])
                    + 'd ' + d['label']
                    for d in _b_dets
                ]
        except Exception:
            pass

        score_adj = max(0, min(100, score - b_penalty))  # b_penalty âm = cộng
        item['score_adj']   = score_adj
        item['b_penalty']   = b_penalty
        item['b_warnings']  = b_warnings

        if action == 'MUA' and score_adj < meta['score_min']:
            skipped.append((sym, score, meta['score_min'], meta,
                           score_adj, b_warnings))
            continue
        wl_signals.append((item, meta))

    now_str = datetime.now(VN_TZ).strftime('%d/%m %H:%M')
    msg = f'<b>📋 Tin Hieu Watchlist — {now_str}</b>\n'
    msg += '(' + str(len(WATCHLIST_META)) + ' ma | Score &gt;= ngưỡng BT | B-filter ON)\n'
    if _macro_prefix:
        msg += _macro_prefix
    msg += '\n'
    buy_symbols = []

    if not wl_signals:
        msg += '🟡 Hom nay chưa có tín hiệu hop le trong watchlist.\n'
        msg += '(Cac ma có thể dang o THEO DOI hoặc score chưa đủ ngưỡng)\n'
    else:
        for item, meta in wl_signals:
            sym    = item.get('symbol', '')
            action = item.get('action', '')
            score  = item.get('score', 0)
            ae     = action_emoji(action)
            vr     = item.get('vol_ratio', 1.0)
            vb     = '&#x1F525;' if vr >= 1.5 else ('⬆' if vr >= 1.0 else ('➡' if vr >= 0.7 else '⬇'))
            div    = item.get('rsi_divergence', {})
            tio    = item.get('three_in_one', False)
            ichi   = item.get('ichimoku', {})
            p      = item.get('price', 0)
            ct     = ichi.get('cloud_top', 0)
            cb     = ichi.get('cloud_bottom', 0)
            sups   = item.get('supports', [])
            ress   = item.get('resistances', [])

            is_    = 'Tren may' if p > ct else ('Duoi may' if p < cb else 'Trong may')
            div_txt = '\n PHAN KY: ' + escape_html(div['message']) if div.get('type') != 'none' else ''
            tio_txt = '\n &#x1F525; HOI TU 3-TRONG-1!' if tio else ''

            # Nhóm + ngưỡng score + SL/TP
            meta_line = (f' &#x1F4CC; {meta["group"]} | '
                         f'Score&gt;={meta["score_min"]} | '
                         f'SL={meta["sl"]}% TP={meta["tp"]}%\n')

            # Score vs ngưỡng (hiển thị score gốc + điều chỉnh nếu có penalty)
            score_adj  = item.get('score_adj', score)
            b_penalty  = item.get('b_penalty', 0)
            b_warn     = item.get('b_warnings', [])
            if b_penalty != 0 and b_warn:
                # Có điều chỉnh B (cộng hoặc trừ)
                sign      = '-' if b_penalty > 0 else '+'
                abs_pen   = abs(b_penalty)
                adj_txt   = ' (' + str(score) + sign + str(abs_pen) + '=' + str(score_adj) + 'd)'
                b_icon    = '⚠' if b_penalty > 0 else '📈'
                score_note = (
                    f' ✅ Score {score_adj}{adj_txt} &gt;= {meta["score_min"]} (đạt ngưỡng)\n'
                    if score_adj >= meta['score_min'] else
                    f' ⚠ Score {score_adj}{adj_txt} (ngưỡng: {meta["score_min"]})\n'
                )
                score_note += f' {b_icon} B-filter: {", ".join(b_warn)}\n'
            else:
                # Không có điều chỉnh B (neutral)
                score_note = (
                    f' ✅ Score {score} &gt;= {meta["score_min"]} (đạt ngưỡng)\n'
                    if score >= meta['score_min'] else
                    f' ⚠ Score {score} (ngưỡng: {meta["score_min"]})\n'
                )

            # Entry timing warning cho mã có entry bias mạnh
            entry_warn = ''
            cfg_sym = SYMBOL_CONFIG.get(sym, {})
            if action == 'MUA' and cfg_sym.get('entry'):
                entry_t    = cfg_sym['entry']
                entry_note = cfg_sym.get('entry_note', '')
                entry_warn = '\n &#x23F0; <b>Vào lệnh: ' + entry_t + '</b>'
                if entry_note:
                    entry_warn += ' — ' + entry_note

            msg += (
                ae + ' <b>' + sym + '</b> — <b>' + action + '</b> (' + str(score) + '/100)\n'
                + meta_line
                + score_note
                + ' Giá: ' + f'{p:,.0f}' + 'd  RSI: ' + str(item.get('rsi', 0)) + '\n'
                + ' ' + vb + ' Vol: ' + f'{vr:.1f}' + 'x  ' + is_ + '\n'
                + (' HT: ' + f'{sups[0]["price"]:,.0f}' if sups else '')
                + (' KC: ' + f'{ress[0]["price"]:,.0f}' if ress else '') + '\n'
                + div_txt + tio_txt + entry_warn + '\n\n'
            )
            if action == 'MUA':
                buy_symbols.append({'symbol': sym, 'score': score})
                # Gắn thêm market context vào item để gửi cùng
                item['_meta'] = meta
                # Tự động ghi paper trade
                sl_pct = meta.get('sl', 7)
                tp_pct = meta.get('tp', 14)
                ok, result = _add_paper_trade(sym, p, score, sl_pct, tp_pct)
                if ok:
                    logger.info(f'Paper trade added: {sym} @{p} score={score}')

    # Mã bị lọc vì score chưa đủ
    if skipped:
        msg += '&#x23F3; <b>Cho ngưỡng score:</b>\n'
        for row in skipped:
            sym, sc, min_sc, meta = row[0], row[1], row[2], row[3]
            sc_adj  = row[4] if len(row) > 4 else sc
            b_warns = row[5] if len(row) > 5 else []
            if sc_adj < sc:
                msg += (f' ⚠ {sym} ({meta["group"]}): '
                        f'Score={sc}-{sc-sc_adj}={sc_adj} (can &gt;={min_sc}) '
                        f'[{", ".join(b_warns)}]\n')
            else:
                msg += f' &#x1F4CC; {sym} ({meta["group"]}): Score={sc} (can &gt;={min_sc})\n'
        msg += '\n'

    # ── BUG-08 FIX: Tính breadth TRƯỚC khi send(msg) ─────────────────────────
    if wl_signals:
        _all_items = [item for item, _ in wl_signals]
        _n_total   = len(_all_items)
        _n_buy     = sum(1 for x in _all_items if x.get('action') == 'MUA')
        _n_sell    = sum(1 for x in _all_items if x.get('action') == 'BAN')
        _n_watch   = _n_total - _n_buy - _n_sell
        _buy_pct   = round(_n_buy / _n_total * 100) if _n_total > 0 else 0
        _bl        = ('BULLISH' if _buy_pct >= 60 else
                      'BEARISH' if _buy_pct <= 25 else 'NEUTRAL')
        _bl_icon   = ('&#x1F7E2;' if _bl == 'BULLISH' else
                      '&#x1F534;' if _bl == 'BEARISH' else '&#x1F7E1;')
        if _n_total >= 5:
            msg += (
                f'{_bl_icon} <b>Breadth {_buy_pct:.0f}%</b> '
                f'({_n_buy}MUA/{_n_sell}BAN/{_n_watch}THEO) — {_bl}\n'
            )
        else:
            msg += f'&#x2139; Breadth: {_n_total} ma (chua du de danh gia)\n'

    msg += '<i>Không phải tư vấn đầu tư</i>'
    send(msg, chat_id)

    if wl_signals:
        mua_items = [(item, meta) for item, meta in wl_signals if item.get('action') == 'MUA']
        if mua_items:
            def send_wl_context():
                try:
                    import sys, os
                    bot_dir = os.path.dirname(os.path.abspath(__file__))
                    if bot_dir not in sys.path: sys.path.insert(0, bot_dir)
                    mc = _mc
                    import backtest as bt, importlib

                    ctx_lines = ''
                    for item, meta in mua_items:
                        sym2 = item.get('symbol', '')
                        try:
                            df2, _ = bt.load_data(sym2, days=60)
                            if df2 is None: continue
                            ctx  = mc.build_market_context(df2, sym2,
                                       item.get('price',0),
                                       item.get('vol_ratio',1.0),
                                       item.get('score',50))
                            flag = ctx['overall_emoji']
                            ovr  = ctx['overall']
                            wyck = ctx['wyckoff']
                            liq  = ctx['liquidity']
                            wknd = ctx['weekend']
                            adtv = liq['adtv'] / 1_000_000_000

                            ctx_lines += (
                                f'{flag} <b>{sym2}</b> ({meta["group"]}) — {ovr}\n'
                                f'  💰 TK: {liq["emoji"]} {adtv:.1f}ty | '
                                f'Wyckoff: {wyck["emoji"]} {wyck["phase"]} | '
                                f'Weekend: {wknd["emoji"]}\n'
                            )
                            if ctx['red_flags']:
                                for rf in ctx['red_flags']:
                                    ctx_lines += f'  ⚠ {rf}\n'
                            ctx_lines += '\n'
                        except Exception as ex:
                            logger.error(f'wl ctx {sym2}: {ex}')

                    if ctx_lines:
                        send(
                            '&#x1F1FB;&#x1F1F3; <b>(B) DAC TINH THI TRUONG VN</b>\n'
                            + '(Danh cho cac ma MUA hom nay)\n'
                            + '=' * 28 + '\n\n'
                            + ctx_lines
                            + '<i>Liquidity | Wyckoff | Weekend Rule</i>',
                            chat_id
                        )
                except Exception as e:
                    logger.error(f'signals wl_ctx: {e}')
            threading.Thread(target=send_wl_context, daemon=True).start()

    # run_bt_context đã xóa — bỏ backtest context sau /signals để giảm tải API.
    # Dùng /mlbt <SYM> hoặc /bt <SYM> để xem backtest chi tiết từng mã.


def handle_macro(chat_id):
    """
    /macro — Systemic Risk Score: đánh giá rủi ro vĩ mô thị trường VN.
    Kết hợp VNINDEX trend + market breadth + volatility + weekend risk.
    """
    send('📊 Đang tính <b>Macro Risk Score</b>...', chat_id)
    try:
        import sys, os
        bot_dir = os.path.dirname(os.path.abspath(__file__))
        if bot_dir not in sys.path: sys.path.insert(0, bot_dir)
        mc = _mc

        # Lấy VNINDEX data
        market_data = call_api('/api/market')
        vn_raw = market_data.get('VNINDEX', {}) if market_data else {}

        # Lấy VNINDEX MA20 từ backtest module
        import backtest as bt, importlib
        vn_df, _ = bt.load_data('VNINDEX', days=60)
        vn_ma20  = 0
        if vn_df is not None:
            import pandas as pd, numpy as np
            cc = bt.find_col(vn_df, ['close','closeprice','close_price'])
            if cc:
                closes = pd.to_numeric(vn_df[cc], errors='coerce').fillna(0).values
                if closes.max() < 100: closes = closes * 1000
                vn_ma20 = float(np.mean(closes[-20:])) if len(closes) >= 20 else 0

        vnindex_data = {
            'price':      vn_raw.get('price', 0),
            'change_pct': vn_raw.get('change_pct', 0),
            'ma20':       vn_ma20,
        }

        # Lấy watchlist scores từ /api/signals
        signals_data = call_api('/api/signals') or []
        watchlist_scores = []
        for x in signals_data:
            if isinstance(x, dict):
                watchlist_scores.append({
                    'symbol': x.get('symbol', ''),
                    'score':  x.get('score', 50),
                    'action': x.get('action', ''),
                })

        macro = mc.analyze_macro_risk(vnindex_data, watchlist_scores)
        macro_txt = mc.format_macro_risk_msg(macro)

        score = macro['score']
        comp  = macro['components']
        vn_c  = comp.get('vnindex', {})
        br_c  = comp.get('breadth', {})
        vol_c = comp.get('volatility', {})
        wknd_c= comp.get('weekend', {})

        detail = (
            f'\n<b>Chi tiet diem so:</b>\n'
            f' VN-INDEX vs MA20 : {vn_c.get("score", 0):>3}/35\n'
            f' Market Breadth   : {br_c.get("score", 0):>3}/30\n'
            f' Volatility       : {vol_c.get("score", 0):>3}/20\n'
            f' Weekend Risk     : {wknd_c.get("score", 0):>3}/15\n'
            f' Tong cong        : {score:>3}/100'
        )

        msg = (
            '📊 <b>MACRO RISK FILTER</b>\n'
            + '=' * 28 + '\n\n'
            + macro_txt
            + detail + '\n\n'
            + '<i>Score 0-30: XANH | 30-50: VANG | 50-70: CAM | 70-85: DO | 85+: DO THAM\n'
            + 'Nguon: VNINDEX MA20 + Market Breadth + Volatility + Weekend Rule\n'
            + 'Không phải tư vấn đầu tư</i>'
        )
        send(msg, chat_id)

    except Exception as e:
        logger.error(f'handle_macro: {e}')
        import traceback
        logger.error(traceback.format_exc())
        send('❌ Loi macro risk: ' + str(e)[:120], chat_id)


def poll_updates():
    if not TOKEN:
        logger.error('Không co TOKEN')
        return

    # Khởi tạo DB table khi bot start
    _init_db()
    logger.info('Bot v4.4 polling... (RS+Scanner+SharkV4+AutoShark ready)')
    # Kick warmup ngay khi khởi động — giảm cold start latency
    def _kick_warmup():
        import time as _t
        _t.sleep(5)  # Chờ Flask listen xong
        try:
            call_api('/api/warmup')
            logger.info('Warmup kicked at startup')
        except Exception as e:
            logger.warning('Warmup kick failed: ' + str(e))
    threading.Thread(target=_kick_warmup, daemon=True).start()
    offset = 0
    retry_delay = 1  # exponential backoff

    while True:
        try:
            resp = requests.get(
                'https://api.telegram.org/bot' + TOKEN + '/getUpdates',
                params={'offset': offset, 'timeout': 30},
                timeout=35
            )
            retry_delay = 1  # reset khi thành công
            for upd in resp.json().get('result', []):
                offset = upd['update_id'] + 1
                msg = upd.get('message', {})
                if not msg:
                    continue

                cid = str(msg.get('chat', {}).get('id', ''))
                text = msg.get('text', '').strip()
                if not text:
                    continue

                logger.info('CMD: ' + text + ' | chat_id=' + cid)
                # Cảnh báo nếu CHAT_ID env chưa set hoặc sai
                if not CHAT_ID:
                    logger.warning('*** TELEGRAM_CHAT_ID chua set tren Railway! ***')
                    logger.warning('*** Set bien moi truong: TELEGRAM_CHAT_ID=' + cid + ' ***')
                elif cid != str(CHAT_ID):
                    logger.warning('*** CHAT_ID MISMATCH: env=' + str(CHAT_ID)
                                   + ' user=' + cid + ' ***')
                    logger.warning('*** Sua TELEGRAM_CHAT_ID=' + cid + ' tren Railway ***')
                parts = text.split()
                cmd = parts[0].lower().split('@')[0]

                if cmd in ('/start', '/help'):
                    handle_start(cid)
                elif cmd == '/subscribe':
                    user = msg.get('from', {})
                    uname = user.get('username') or user.get('first_name') or str(cid)
                    save_subscriber(cid, uname)
                    send('✅ <b>Dang ky thanh cong!</b>' + chr(10)
                         + 'Ban se nhan bao cao 1 tieng/lan va alert MUA/BAN.' + chr(10)
                         + 'De huy: /unsubscribe', cid)
                    logger.info('Subscribe: ' + str(cid) + ' (' + uname + ')')
                elif cmd == '/unsubscribe':
                    if str(cid) == str(CHAT_ID):
                        send('⚠ Ban la owner, không the huy.', cid)
                    else:
                        remove_subscriber(cid)
                        send('❌ <b>Da huy dang ky.</b>' + chr(10)
                             + 'De dang ky lai: /subscribe', cid)
                        logger.info('Unsubscribe: ' + str(cid))
                elif cmd == '/subscribers':
                    if str(cid) == str(CHAT_ID):
                        subs = load_subscribers()
                        send('&#x1F465; <b>Subscribers (' + str(len(subs)) + '):</b>' + chr(10)
                             + chr(10).join(sorted(subs)), cid)
                    else:
                        send('⚠ Chi admin moi xem duoc.', cid)

                elif cmd == '/price':
                    handle_price(parts[1].upper() if len(parts) > 1 else 'VCB', cid)

                elif cmd in ('/analyze', '/score'):
                    _sym = parts[1].upper() if len(parts) > 1 else 'VCB'
                    threading.Thread(
                        target=handle_analyze, args=(_sym, cid),
                        daemon=True).start()

                elif cmd == '/ml':
                    _s = parts[1].upper() if len(parts) > 1 else ''
                    if not _s:
                        send('Cú pháp: <b>/ml SYM</b>\nVí dụ: <b>/ml MBB</b>\n\n'
                             '<i>Momentum Leader: hệ thống chấm điểm 2-tier độc lập Score A\n'
                             'Tier 1: Price&gt;MA50 + Vol&gt;1.2x\n'
                             'Tier 2: RS/RSI/Price Structure/52W (0-120đ)\n'
                             '≥90 = Strong Leader | ≥75 = Pass</i>', cid)
                    else:
                        threading.Thread(target=handle_ml, args=(_s, cid), daemon=True).start()
                elif cmd in ('/mlscan', '/mlscanextended'):
                    # Support both "/mlscan extended" and "/mlscanextended"
                    if cmd == '/mlscanextended':
                        _mode = 'extended'
                    else:
                        _mode = (parts[1].lower() if len(parts) > 1 else 'watchlist')
                        if _mode not in ('extended',):
                            _mode = 'watchlist'
                    threading.Thread(target=handle_mlscan, args=(_mode, cid), daemon=True).start()
                elif cmd == '/foreign':
                    _s = parts[1].upper() if len(parts) > 1 else ''
                    if not _s:
                        send('Cú pháp: <b>/foreign SYM</b>\nVí dụ: <b>/foreign MBB</b>', cid)
                    else:
                        threading.Thread(target=handle_foreign, args=(_s, cid), daemon=True).start()




                elif cmd == '/optimize':
                    _sym = parts[1].upper() if len(parts) > 1 else ''
                    if not _sym:
                        send('Cu phap: <b>/optimize SYM</b>\nVi du: <b>/optimize DGC</b>', cid)
                    else:
                        threading.Thread(target=lambda s=_sym, c=cid: _handle_optimize(s, c),
                                         daemon=True).start()

                elif cmd == '/scan':
                    threading.Thread(
                        target=handle_market_scan, args=(cid,),
                        daemon=True).start()


                elif cmd == '/signals':
                    threading.Thread(
                        target=handle_signals, args=(cid,),
                        daemon=True).start()

                elif cmd == '/macro':
                    threading.Thread(
                        target=handle_macro, args=(cid,),
                        daemon=True).start()



                elif cmd == '/mlbt':
                    handle_mlbt(parts[1:], cid)
                elif cmd == '/mlbtv2':
                    send('&#x26A0; /mlbtv2 da bi xoa. Dung <b>/mlbtv3</b> thay the (chay duoc moi ma).', cid)
                elif cmd == '/mlbtv3':
                    handle_mlbtv3(parts[1:], cid)
                elif cmd == '/mrabt':
                    handle_mrabt(parts[1:], cid)
                elif cmd == '/mradebug':
                    handle_mradebug(parts[1:], cid)
                elif cmd == '/mradebug2':
                    handle_mradebug2(parts[1:], cid)
                elif cmd == '/mradebug3':
                    handle_mradebug3(parts[1:], cid)
                elif cmd == '/mradebug4':
                    handle_mradebug4(parts[1:], cid)
                elif cmd == '/mlscreen':
                    # FIX: threading — batch chạy ~70 phút, không được block polling loop
                    threading.Thread(
                        target=handle_mlscreen, args=(list(parts[1:]), cid),
                        daemon=True).start()
                elif cmd == '/sascreen':
                    threading.Thread(
                        target=handle_sascreen, args=(list(parts[1:]), cid),
                        daemon=True).start()
                elif cmd == '/scanstatus':
                    threading.Thread(
                        target=handle_scanstatus, args=(cid,),
                        daemon=True).start()
                elif cmd == '/scanner':
                    # /scanner on/off all|signal|vol|ma|ml|ext|shark
                    threading.Thread(
                        target=handle_scanner, args=(list(parts[1:]), cid),
                        daemon=True).start()
                elif cmd == '/signal_scan':
                    threading.Thread(
                        target=handle_signal_scan, args=(list(parts[1:]), cid),
                        daemon=True).start()

                elif cmd == '/ma_scan':
                    threading.Thread(
                        target=handle_ma_scan, args=(list(parts[1:]), cid),
                        daemon=True).start()
                elif cmd == '/ml_scan':
                    threading.Thread(
                        target=handle_ml_scan, args=(list(parts[1:]), cid),
                        daemon=True).start()
                elif cmd == '/ext_scan':
                    threading.Thread(
                        target=handle_ext_scan, args=(list(parts[1:]), cid),
                        daemon=True).start()

                elif cmd == '/eventstudy':
                    handle_event_study(parts[1:], cid)
                elif cmd == '/bt':
                    arg = ' '.join(parts[1:]) if len(parts) > 1 else ''
                    handle_bt(arg, cid)
                elif cmd in ('/wf', '/backtest'):
                    # FIX: /wf alias → /bt full mode
                    _sym = parts[1].upper() if len(parts) > 1 else ''
                    if not _sym:
                        send('Cú pháp: <b>/wf SYM</b>\nVí dụ: <b>/wf DGC</b>', cid)
                    else:
                        handle_bt(_sym + ' full', cid)
                elif cmd == '/btest_b':
                    # FIX: B-filter comparison backtest
                    _sym = parts[1].upper() if len(parts) > 1 else ''
                    if not _sym:
                        send('Cú pháp: <b>/btest_b SYM</b>\nVí dụ: <b>/btest_b DCM</b>', cid)
                    else:
                        def _run_btest_b(sym=_sym, chat=cid):
                            try:
                                import backtest as _bt
                                send('&#x1F9EA; Đang chạy B-filter comparison <b>' + sym + '</b>...', chat)
                                df, _ = _bt.load_data(sym)
                                bf = _bt.run_b_filter_comparison(sym, verbose=False)
                                if not bf:
                                    send('&#x274C; Không đủ dữ liệu cho ' + sym, chat)
                                    return
                                NL = chr(10)
                                # Dùng keys đã chuẩn hóa
                                ma  = bf.get('mode_A',  {})
                                mab = bf.get('mode_AB', {})
                                flag    = bf.get('flag', '-')
                                verdict = bf.get('verdict', '')
                                nf      = bf.get('n_filtered', 0)
                                fi = {'V': '&#x2705;', '~': '&#x1F7E1;', '-': '&#x27A1;', '!': '&#x274C;'}.get(flag, '&#x1F7E1;')
                                msg = (
                                    '&#x1F4CA; <b>B-Filter Comparison — ' + sym + '</b>' + NL
                                    + '─' * 30 + NL
                                    + f'Không B-filter: {ma.get("n",0)}L WR={ma.get("wr",0):.1f}% PnL={ma.get("pnl",0):+.2f}%' + NL
                                    + f'Có B-filter   : {mab.get("n",0)}L WR={mab.get("wr",0):.1f}% PnL={mab.get("pnl",0):+.2f}%' + NL
                                    + f'Lọc được: {nf} lệnh nhiễu' + NL
                                    + f'{fi} {verdict}'
                                )
                                send(msg, chat)
                            except Exception as e:
                                send('&#x274C; Lỗi btest_b: ' + str(e)[:100], chat)
                        threading.Thread(target=_run_btest_b, daemon=True).start()
                
        except requests.exceptions.Timeout:
            pass
        except Exception as e:
            logger.error('Polling: ' + str(e))
            # FIX: Exponential backoff thay vì sleep cố định 5s
            time.sleep(min(retry_delay, 60))
            retry_delay = min(retry_delay * 2, 60)


# ── Cấu hình alert ──────────────────────────────────────────────────────────
SCORE_STRONG_BUY = 72
SCORE_STRONG_SELL = 28
ALERT_INTERVAL = 30
TRADING_HOURS = ((9, 0), (15, 0))
_last_alerts       = {}


def is_trading_hours():
    now = datetime.now(VN_TZ)
    if now.weekday() >= 5:
        return False
    h, m = now.hour, now.minute
    start_h, start_m = TRADING_HOURS[0]
    end_h, end_m = TRADING_HOURS[1]
    after_open = (h > start_h) or (h == start_h and m >= start_m)
    before_close = (h < end_h) or (h == end_h and m <= end_m)
    return after_open and before_close


def _vol_time_note(vol_ratio):
    """
    Trả về ghi chú độ tin cậy volume theo khung giờ VN.

    Lý do cần thiết: hệ thống dùng EOD data từ vnstock.
    Volume nến ngày tích lũy dần trong phiên — chưa đóng cửa
    thì con số chưa đại diện cho cả ngày.

    Ngưỡng vol_ratio tối thiểu được nâng cao dần trong ngày:
      < 11:00  : cần vol >= 2.0x (ATO hay bị làm giá)
      11:00-14:00: cần vol >= 1.8x (giữa phiên, tích lũy dần)
      >= 14:00 : cần vol >= 1.5x  (cuối phiên, đáng tin nhất)
    """
    now  = datetime.now(VN_TZ)
    h, m = now.hour, now.minute

    if h < 11:
        threshold = 2.0
        time_label = '⏰ Vol ATO (truoc 11:00)'
        if vol_ratio >= threshold:
            return (
                '\n⚠ <i>Vol ' + f'{vol_ratio:.1f}' + 'x — manh nhung do ATO, '
                'nen cho xac nhan sau 11:00 truoc khi vao lệnh</i>'
            )
        else:
            return (
                '\n⚠ <i>' + time_label + ': Vol chua du tin cay '
                '(' + f'{vol_ratio:.1f}' + 'x &lt; ' + f'{threshold:.1f}' + 'x cần thiết). '
                'Chờ đến sau 11:00 de xac nhan</i>'
            )
    elif h < 14:
        threshold = 1.8
        time_label = '&#x23F3; Vol giua phien (' + f'{h:02d}:{m:02d}' + ')'
        if vol_ratio >= threshold:
            return (
                '\n🟡 <i>' + time_label + ': Vol ' + f'{vol_ratio:.1f}' + 'x — '
                'kha manh, có thể vao lệnh nhung chua toan dien. '
                'Vol cuoi phien se chac chan hon</i>'
            )
        else:
            return (
                '\n⚠ <i>' + time_label + ': Vol chưa đầy đủ '
                '(' + f'{vol_ratio:.1f}' + 'x &lt; ' + f'{threshold:.1f}' + 'x cần thiết). '
                'Tín hiệu volume chưa đáng tin — chờ thêm</i>'
            )
    else:
        # Sau 14:00 — nến gần đóng, ngưỡng bình thường
        if vol_ratio >= 2.5:
            return '\n✅ <i>Vol đột biến ' + f'{vol_ratio:.1f}' + 'x — tín hiệu mạnh nhat trong ngay</i>'
        elif vol_ratio >= 1.5:
            return '\n&#x2139; <i>Vol ' + f'{vol_ratio:.1f}' + 'x — xac nhan tot</i>'
        else:
            return ''   # Vol thấp không cần ghi chú thêm


def _build_conviction_block(data, score_adj=None):
    """
    Block xac nhan da chieu 4 tang: Score A | Regime | VWAP | Shark
    Returns: (block_text, passed_count, level_label)
    """
    from config import SYMBOL_CONFIG
    NL    = chr(10)
    sym   = data.get('symbol', '').upper()
    cfg        = SYMBOL_CONFIG.get(sym, {})
    score      = score_adj if score_adj is not None else data.get('score', 50)
    _sym_trigger = cfg.get('trigger_mode', data.get('trigger_mode', 'score_primary'))
    min_score    = cfg.get('min_score', data.get('min_score', 65))
    checks = []

    # [1] Score A — min_score đã được set từ trigger_mode
    if score >= min_score:
        checks.append((True,  f'Score A: <b>{score}/100</b> (&gt;={min_score}) — Kỹ thuật xác nhận'))
    elif score >= 60:
        checks.append((None,  f'Score A: <b>{score}/100</b> — Gần ngưỡng, chua du {min_score}'))
    else:
        checks.append((False, f'Score A: <b>{score}/100</b> — Chua du ngưỡng {min_score}'))

    # [2] Regime Filter
    use_regime = cfg.get('use_regime', True)
    regime     = data.get('market_regime', 'UNKNOWN')
    exempt     = data.get('regime_exempt', False)
    if not use_regime or exempt:
        checks.append((None, f'Regime: MIEN TRU ({regime}) — Ma dac thu'))
    elif regime == 'BULL':
        checks.append((True,  'Regime: BULL — VNI&gt;MA50&gt;MA200 &#x2197;'))
    elif regime == 'NEUTRAL':
        checks.append((None,  'Regime: NEUTRAL — VNI tren MA200'))
    elif regime == 'BEAR':
        checks.append((False, 'Regime: BEAR — VNI duoi MA200 &#x26A0;'))
    else:
        checks.append((None,  f'Regime: {regime}'))

    # [3] VWAP
    use_vwap = cfg.get('use_vwap', True)
    vi       = data.get('vwap_info', {})
    pct_w    = vi.get('pct_w', 0.0)
    pct_m    = vi.get('pct_m', 0.0)
    vwap_w   = vi.get('vwap_w', 0)
    if not use_vwap or not vwap_w:
        checks.append((None, 'VWAP: Không áp dụng cho mã này'))
    elif pct_w >= 0 and pct_m >= 0:
        checks.append((True,  f'VWAP: P&gt;W({pct_w:+.1f}%) P&gt;M({pct_m:+.1f}%) — Dòng tiền ủng hộ &#x1F4C8;'))
    elif pct_w >= 0 or pct_m >= 0:
        pct_w_s = f'P&gt;W({pct_w:+.1f}%)' if pct_w >= 0 else f'P&lt;W({pct_w:+.1f}%)'
        pct_m_s = f'P&gt;M({pct_m:+.1f}%)' if pct_m >= 0 else f'P&lt;M({pct_m:+.1f}%)'
        checks.append((None,  f'VWAP: {pct_w_s} {pct_m_s} — Tín hiệu pha tron'))
    else:
        checks.append((False, f'VWAP: P&lt;W({pct_w:+.1f}%) P&lt;M({pct_m:+.1f}%) — Dòng tiền yeu &#x1F4C9;'))

    # [4] Shark Score
    shark      = data.get('shark_score', 0)
    shark_mode = cfg.get('shark_mode', 'none')
    shark_min  = cfg.get('shark_min', 55)
    if shark_mode == 'none':
        checks.append((None, 'Shark: Không áp dụng cho mã này'))
    elif shark >= shark_min:
        checks.append((True,  f'Shark: <b>{shark}/100</b> (&gt;={shark_min}) — Tich luy ro &#x1F988;'))
    elif shark >= 50:
        checks.append((None,  f'Shark: <b>{shark}/100</b> — Đang tích lũy, chua du {shark_min}'))
    else:
        checks.append((False, f'Shark: <b>{shark}/100</b> — Chưa có tín hiệu tich luy'))

    # Conviction score
    passed = sum(1 for p, _ in checks if p is True)
    half   = sum(1 for p, _ in checks if p is None)

    if passed >= 4:   level = 'STRONG BUY &#x1F525;'
    elif passed >= 3: level = 'MUA &#x2705;'
    elif passed == 2: level = 'THEO DOI &#x1F7E1;'
    else:             level = 'YEU &#x26A0;'

    icon_map = {True: '&#x2705;', None: '&#x26A0; ', False: '&#x274C;'}
    rows = [f'{icon_map[p]} [{i}] {label}' for i, (p, label) in enumerate(checks, 1)]

    # Auto remark
    miss = [f'[{i}]' for i, (p,_) in enumerate(checks,1) if p is False]
    if passed == 4:  remark = 'Tất cả 4 tang xac nhan — do tin cay cao nhat.'
    elif passed == 3:remark = f'3/4 xac nhan. {" ".join(miss)} chua ho tro — có thể vao lệnh.'
    elif passed == 2:remark = 'Tín hiệu pha tron. Nen doi them xac nhan.'
    else:            remark = 'Tín hiệu yếu. Nên chờ cơ hội tốt hơn.'

    wf = cfg.get('wf_verdict', '')
    if wf == 'TOT':  remark += ' WF=TỐT: Hệ thống robust.'
    elif wf == 'YEU':remark += ' WF=YẾU: Giám size de phong ngua.'

    tier_label = {'score_primary': 'Tier1-KT', 'filter_confirm': 'Tier2-CF', 'filter_led': 'Tier3-FL'}
    tier_str   = tier_label.get(_sym_trigger, _sym_trigger)
    block = (
        NL + '&#x2501;' * 20 + NL
        + f'&#x1F4CA; <b>Xac nhan: {passed}/4</b> — {level}  <i>[{tier_str} &gt;={min_score}]</i>' + NL
        + NL.join(rows) + NL
        + NL + '&#x1F4A1; ' + remark
    )
    return block, passed, level


def format_alert(item):
    """Alert realtime MUA/BAN — gọn, đủ thông tin để ra quyết định."""
    NL     = chr(10)
    action = item.get('action', '')
    sym    = item.get('symbol', '')
    score  = item.get('score', 50)
    price  = item.get('price', 0)
    rsi    = item.get('rsi', 0)
    vr     = item.get('vol_ratio', 1.0)
    rs20   = item.get('rs_20d') or 0
    shark  = item.get('shark_score', 0)
    sups   = item.get('supports', [])
    ress   = item.get('resistances', [])
    sl     = item.get('stop_loss', 0)
    tp     = item.get('take_profit', 0)
    entry  = item.get('entry_opt', price)

    is_buy = (action == 'MUA')
    icon   = '&#x1F7E2;&#x1F6A8;' if is_buy else '&#x1F534;&#x1F6A8;'
    lbl    = 'MUA' if is_buy else 'BAN'

    # Xây dựng từng dòng — chỉ hiện khi có giá trị
    lines = [
        icon + ' <b>' + sym + ' — ' + lbl + '</b>  ' + str(score) + '/100',
        '&#x2500;' * 20,
        '&#x1F4B0; Gia: <b>' + f'{price:,.0f}' + 'd</b>'
        + ('  Vao: ' + f'{entry:,.0f}' + 'd' if is_buy and entry != price else ''),
    ]

    # SL/TP chỉ khi MUA
    if is_buy and sl and tp:
        sl_pct = round((sl - price) / price * 100, 1) if price else -7
        tp_pct = round((tp - price) / price * 100, 1) if price else 14
        lines.append(f'&#x26D4; SL: {sl:,.0f}d ({sl_pct:.1f}%)  '
                     f'&#x1F3AF; TP: {tp:,.0f}d (+{tp_pct:.1f}%)')

    # Các tín hiệu xác nhận
    confirms = []
    if vr >= 1.5:  confirms.append(f'Vol {vr:.1f}x &#x1F525;')
    if rs20 > 3:   confirms.append(f'RS {rs20:+.1f}% &#x1F680;')
    if shark >= 60: confirms.append(f'Shark {shark} &#x1F988;')
    if item.get('ma10_cross_up'): confirms.append('MA10 cross &#x26A1;')
    if item.get('three_in_one'): confirms.append('3-trong-1 &#x2705;')
    if item.get('rsi_divergence', {}).get('type') != 'none':
        confirms.append('Phân kỳ RSI &#x1F514;')
    if confirms:
        lines.append('&#x1F4A1; ' + ' | '.join(confirms))

    # RSI ngắn gọn
    lines.append(f'RSI: {rsi:.0f}')

    # Hỗ trợ/kháng cự
    if is_buy and sups:
        lines.append('&#x2795; HT: ' + f'{sups[0]["price"]:,.0f}' + 'd')
    if not is_buy and ress:
        lines.append('&#x2796; KC: ' + f'{ress[0]["price"]:,.0f}' + 'd')

    # Conviction block
    conv_block, conv_passed, conv_level = _build_conviction_block(item)
    lines.append(conv_block)

    lines += ['', '&#x1F50E; /analyze ' + sym + ' de xem chi tiet',
              '<i>Chi mang tinh tham khao, không phai tu van</i>']

    return NL.join(lines)


# ── Cấu hình Scanner ────────────────────────────────────────────────────────
SCAN_INTERVAL_MIN      = 10    # Quét signal MUA/BAN mỗi 10 phút trong giờ giao dịch
ALERT_COOLDOWN_SEC     = 5400  # Không alert lại cùng mã
# ── Background scan intervals (API quota vnstock Community = 60 req/phút) ────
# Tần suất được tính để tổng background < 5 calls/phút, còn ~55/phút cho user
# ML scan có sleep(3s)/mã để tránh burst khi /mlbtv3 all chạy cùng lúc

MA_SCAN_INTERVAL_MIN   = 240   # MA10/MA50 scan: 4 tiếng/lần → 0.05 calls/phút
MA_CROSS_COOLDOWN_SEC  = 14400

ML_SCAN_INTERVAL_MIN   = 10    # ML Momentum: 10 phút/lần + sleep(3s)/mã → 20 calls/phút peak
EXT_SCAN_INTERVAL_MIN  = 60    # Extended MA: 1 tiếng/lần → 0.12 calls/phút
MA_EXT_COOLDOWN_SEC    = 28800 # Cooldown 8h cho mã tier 2
_MA_ALERTS_FILE       = '/tmp/ma_alerts_state.json'

# ── Scanner ON/OFF control — dùng lệnh /scanner on/off <tên> ────────────────
# Tắt scanner để giải phóng quota khi cần chạy /mlbtv3 all hoặc backtest nặng
# Dùng: /scanner off all   → tắt hết trước khi chạy /mlbtv3 all
#        /scanner on all    → bật lại sau khi chạy xong
SCANNER_ENABLED = {
    'signal':   True,   # Signal MUA/BAN (10 phút/lần) — ~6 calls/lần
    'ma':       True,   # MA10/MA50 cross (4 tiếng/lần) — ~13 calls/lần
    'ml':       True,   # ML Momentum (10 phút/lần) — ~12 calls/lần
    'ext':      True,   # Extended MA tier2 (1 tiếng/lần) — ~7 calls/lần
}

def _load_ma_alerts():
    """Load MA alert state từ file để persist qua restart."""
    try:
        if os.path.exists(_MA_ALERTS_FILE):
            with open(_MA_ALERTS_FILE) as f:
                raw = json.load(f)
            # Convert list → tuple
            out = {}
            for sym, events in raw.items():
                out[sym] = {}
                for k, v in events.items():
                    if isinstance(v, list) and len(v) == 2:
                        out[sym][k] = (v[0], v[1])
                    else:
                        out[sym][k] = v
            return out
    except Exception:
        pass
    return {}

def _save_ma_alerts(d):
    try:
        # Convert tuple → list để JSON serialize
        raw = {}
        for sym, events in d.items():
            raw[sym] = {}
            for k, v in events.items():
                raw[sym][k] = list(v) if isinstance(v, tuple) else v
        with open(_MA_ALERTS_FILE, 'w') as f:
            json.dump(raw, f)
    except Exception as e:
        logger.warning(f'_save_ma_alerts error: {e}')

_last_ma_alerts = _load_ma_alerts()

# ── Watchlist mở rộng — 23 mã ngoài watchlist chính ─────────────────────────
# Nguồn: danh sách 28 mã backtest, bỏ 5 mã đã có trong WATCHLIST_META
# Chỉ alert MA10 cross UP/DOWN và Vol spike — không alert Signal 1D và MA50
# (chưa backtest → chưa có min_score calibrate, MA50 nhiều noise hơn)
# WATCHLIST_EXTENDED: Tự động build từ SYMBOL_CONFIG
# Loại mã đã có trong SIGNALS_WATCHLIST hoặc SIGNALS_MANUAL (tránh alert trùng)
# Chỉ lấy mã có wf_verdict tốt (không phải rỗng hoặc YEU)
# _EXTENDED_BASE: dùng cho /volscan và MA auto alert tier 2
# Cập nhật đợt 10-11: xóa TCB/VPB/VHM/GAS/PVS/REE/VNM/HPG/KBC (đã loại vĩnh viễn)
_EXTENDED_BASE = [
    'VCB', 'VIC',                              # Ngân hàng / BĐS còn lại
    'FPT', 'CMG',                              # Công nghệ
    'POW', 'PVD',                              # Điện / Dầu khí (theo dõi live)
    'MWG', 'MSN',                              # Tiêu dùng
    'SZC',                                     # KCN
]
# Loại bỏ mã đã có trong watchlist chính để tránh alert trùng
WATCHLIST_EXTENDED = [
    s for s in _EXTENDED_BASE
    if s not in set(SIGNALS_WATCHLIST) | set(SIGNALS_MANUAL)
]
# Cooldown dài hơn tầng 1 để giảm noise từ mã chưa được calibrate
MA_EXT_COOLDOWN_SEC = 28800  # 8 tiếng cho mã tầng 2

def format_ma_alert(sym, event, price, score, ma10, ma50, ma50_slope_up, tier=1):
    """
    Format alert MA10 cross hoặc MA50 uptrend.
    event: 'MA10_CROSS_UP' | 'MA10_CROSS_DOWN' | 'MA50_UPTREND' | 'MA50_LOST'
    tier:  1 = watchlist chính, 2 = watchlist mở rộng (chưa backtest)
    """
    tier_note = '' if tier == 1 else '\n<i>⚠ Chưa backtest — chi tham khao them</i>'

    if event == 'MA10_CROSS_UP':
        emoji  = '⚡🟢'
        title  = 'MA10 CROSS UP — NGAN HAN'
        detail = (f'Giá ({price:,.0f}d) vua cat LEN MA10 ({ma10:,.0f}d) hom nay\n'
                  f' -> Momentum ngắn hạn phục hồi\n'
                  f' -> TP tham khao: +7% den +10%')
    elif event == 'MA10_CROSS_DOWN':
        emoji  = '⚡🔴'
        title  = 'MA10 CROSS DOWN — MAT DONG LUC'
        detail = (f'Giá ({price:,.0f}d) vua cat XUONG MA10 ({ma10:,.0f}d) hom nay\n'
                  f' -> Động lực ngắn hạn giảm\n'
                  f' -> Neu dang so huu: can than, xem xet chot loi')
    elif event == 'MA50_UPTREND':
        dist = (price - ma50) / ma50 * 100 if ma50 > 0 else 0
        emoji  = '📈🟢'
        title  = 'MA50 UPTREND — TRUNG HAN'
        detail = (f'Giá ({price:,.0f}d) tren MA50 ({ma50:,.0f}d) +{dist:.1f}% + MA50 doc len\n'
                  f' -> Uptrend trung han xac nhan\n'
                  f' -> TP tham khao: +25% den +30%')
    elif event == 'MA50_LOST':
        emoji  = '📉🔴'
        title  = 'MA50 LOST — TRUNG HAN SUY YEU'
        detail = (f'Giá ({price:,.0f}d) vua ro XUONG MA50 ({ma50:,.0f}d)\n'
                  f' -> Uptrend trung han có thể ket thuc\n'
                  f' -> Nen xem xet giam vi the hoặc dat SL chat hon')
    else:
        return None

    score_s = str(score) + '/100' if score else '—'
    return (
        emoji + ' <b>' + title + ': ' + sym + '</b>\n'
        + '=' * 28 + '\n'
        + ' Score 1D: <b>' + score_s + '</b>\n\n'
        + ' ' + escape_html(detail)
        + tier_note + '\n\n'
        + 'Dùng /analyze ' + sym + ' để xem phân tích đầy đủ\n'
        + '<i>Chỉ tham khảo — không phai tu van dau tu</i>'
    )


def _build_morning_report(data, market_data=None):
    """Báo cáo sáng 08:30 — gọn, actionable."""
    NL  = chr(10)
    now = datetime.now(VN_TZ).strftime('%d/%m')
    
    # Header VNINDEX
    vni = (market_data or {}).get('VNINDEX', {})
    vni_price  = vni.get('price', 0)
    vni_change = vni.get('change_pct', 0)
    vni_icon   = '&#x1F7E2;' if vni_change >= 0 else '&#x1F534;'
    vni_line   = (f'{vni_icon} VNINDEX: {vni_price:,.0f} ({vni_change:+.1f}%)' 
                  if vni_price else '')

    msg  = f'&#x1F4CB; <b>CHUAN BI PHIEN — {now}</b>' + NL
    if vni_line: msg += vni_line + NL
    msg += '&#x2500;' * 22 + NL

    # Tín hiệu watchlist — chỉ hiện mã có tín hiệu rõ
    mua_list = []
    ban_list = []
    theo_doi = []
    
    for item in (data or []):
        sym    = item.get('symbol', '')
        action = item.get('action', '')
        score  = item.get('score', 0)
        price  = item.get('price', 0)
        rsi    = item.get('rsi', 0)
        rs20   = item.get('rs_20d') or 0
        shark  = item.get('shark_score', 0)
        hf     = item.get('hard_filter', '')
        
        meta   = WATCHLIST_META.get(sym, {})
        min_sc = meta.get('score_min', 65)
        
        # Bỏ qua mã có hard filter bear
        if hf and 'down' in hf.lower():
            continue
            
        rs_txt   = f' RS{rs20:+.1f}%' if rs20 != 0 else ''
        shark_txt = f' &#x1F988;{shark}' if shark >= 60 else ''
        line     = f'<b>{sym}</b> {score}d | {price:,.0f}d | RSI:{rsi:.0f}{rs_txt}{shark_txt}'
        
        if action == 'MUA' and score >= min_sc:
            mua_list.append('&#x1F7E2; ' + line)
        elif action == 'BAN':
            ban_list.append('&#x1F534; ' + line)
        elif score >= 55:
            theo_doi.append('&#x1F7E1; ' + line + ' (gan ngưỡng)')

    if mua_list:
        msg += NL.join(mua_list) + NL
    if ban_list:
        msg += NL.join(ban_list) + NL
    if theo_doi:
        msg += '&#x23F3; <i>Theo doi: ' + ', '.join(
            t.split('<b>')[1].split('</b>')[0] for t in theo_doi
        ) + '</i>' + NL
    if not mua_list and not ban_list:
        msg += '&#x1F7E1; Chua co tin hieu ro — thi truong dang sideway' + NL

    msg += '&#x2500;' * 22 + NL
    msg += '<i>Dung /signals hoặc /analyze MA de xem chi tiet</i>'
    return msg


def _build_closing_report(data, market_data=None):
    """Báo cáo tổng kết phiên 15:10 — kết quả + tín hiệu nổi bật."""
    NL  = chr(10)
    now = datetime.now(VN_TZ).strftime('%d/%m')

    vni = (market_data or {}).get('VNINDEX', {})
    vni_price  = vni.get('price', 0)
    vni_change = vni.get('change_pct', 0)
    vni_icon   = '&#x1F7E2;' if vni_change >= 0 else '&#x1F534;'
    vni_line   = f'{vni_icon} VNINDEX: {vni_price:,.0f} ({vni_change:+.1f}%)' if vni_price else ''

    msg  = f'&#x1F4CA; <b>TONG KET PHIEN — {now}</b>' + NL
    if vni_line: msg += vni_line + NL
    msg += '&#x2500;' * 22 + NL

    # Tín hiệu MUA có giá trị nhất hôm nay
    signals = []
    for item in (data or []):
        sym    = item.get('symbol', '')
        action = item.get('action', '')
        score  = item.get('score', 0)
        price  = item.get('price', 0)
        shark  = item.get('shark_score', 0)
        rs20   = item.get('rs_20d') or 0
        meta   = WATCHLIST_META.get(sym, {})
        min_sc = meta.get('score_min', 65)
        if action == 'MUA' and score >= min_sc:
            shark_txt = f' &#x1F988;Shark:{shark}' if shark >= 60 else ''
            rs_txt    = f' RS{rs20:+.1f}%' if rs20 != 0 else ''
            signals.append((score, f'&#x1F7E2; <b>{sym}</b> MUA {score}d | {price:,.0f}d{rs_txt}{shark_txt}'))
        elif action == 'BAN':
            signals.append((score, f'&#x1F534; <b>{sym}</b> BAN {score}d | {price:,.0f}d'))

    if signals:
        signals.sort(reverse=True)
        msg += NL.join(s[1] for s in signals[:5]) + NL
    else:
        msg += '&#x1F7E1; Khong co tin hieu MUA/BAN dat ngưỡng hom nay' + NL

    # Top RS (mã đang dẫn dắt thị trường)
    rs_list = [(item.get('rs_20d') or 0, item.get('symbol','')) 
               for item in (data or []) if item.get('rs_20d')]
    rs_list = sorted(rs_list, reverse=True)[:3]
    if rs_list:
        rs_str = ', '.join(f'{sym} {rs:+.1f}%' for rs, sym in rs_list if rs > 0)
        if rs_str:
            msg += '&#x1F680; <i>Dan dau TT: ' + rs_str + '</i>' + NL

    msg += '&#x2500;' * 22 + NL
    msg += '<i>Dung /signals de xem day du hoặc /scan de tim ma moi</i>'
    return msg



def handle_event_study(args, chat_id):
    """
    /eventstudy <symbol>   — Event study 1 mã
    /eventstudy all        — Toàn bộ EVENT_STUDY_SYMBOLS (confirmed pool)
    /eventstudy fail       — Placeholder: filter mã v3 fail (update sau /mlbtv3 all)

    Tìm pattern trước big moves (+15%/18d) trong data 2018-2025.
    Anti-hindsight: features chỉ dùng data T-1 trở về trước.
    """
    def _run():
        try:
            import backtest as _bt
            from vnstock import Vnstock

            target = args[0].upper() if args else 'ALL'

            if target == 'ALL':
                syms = _bt.EVENT_STUDY_SYMBOLS
            elif target == 'FAIL':
                # TODO: điền mã v3 fail sau khi chạy /mlbtv3 all
                # Hiện tại: chạy all tạm thời
                syms = _bt.EVENT_STUDY_SYMBOLS
                send(
                    '&#x26A0; Chua co ket qua /mlbtv3 — dang chay all '
                    + str(len(syms)) + ' ma thay the.',
                    chat_id
                )
            else:
                syms = [target]

            est = len(syms) * 7
            send(
                '&#x1F50D; Event study <b>' + str(len(syms)) + ' ma</b>...'
                + ' (~' + str(est) + 's)' + chr(10)
                + 'Big move: +' + f'{_bt.EVENT_BIG_MOVE_PCT:.0%}'
                + ' / ' + str(_bt.EVENT_WINDOW_DAYS) + 'd | '
                + 'Data: ' + _bt.EVENT_STUDY_START[:4]
                + '\u2013' + _bt.EVENT_STUDY_END[:4],
                chat_id
            )

            def fetcher(symbol, start, end):
                for src in ['VCI', 'KBS']:
                    try:
                        df = Vnstock().stock(symbol=symbol, source=src).quote.history(
                            start=start, end=end, interval='1D'
                        )
                        if df is not None and len(df) >= 100:
                            return df
                    except Exception:
                        pass
                return None

            results = _bt.run_event_study_all(
                symbols=syms,
                data_fetcher=fetcher,
                max_workers=3,   # thấp để tránh rate limit vnstock 60 req/phút
            )
            report = _bt.format_event_study_report(results)

            # Split nếu > 3800 ký tự (Telegram limit ~4096)
            MAX_TG = 3800
            if len(report) <= MAX_TG:
                send(report, chat_id)
            else:
                lines  = report.split(chr(10))
                chunk  = ''
                chunks = []
                for line in lines:
                    if len(chunk) + len(line) + 1 > MAX_TG:
                        chunks.append(chunk)
                        chunk = line
                    else:
                        chunk = (chunk + chr(10) + line) if chunk else line
                if chunk:
                    chunks.append(chunk)
                for c in chunks:
                    send(c, chat_id)
                    time.sleep(1)

        except Exception as e:
            logger.error('handle_event_study: ' + str(e))
            send('&#x274C; Loi event study: ' + str(e)[:200], chat_id)

    threading.Thread(target=_run, daemon=True).start()


def auto_alert_scanner():
    """
    Scanner tự động — quét mỗi 10 phút trong giờ giao dịch.
    Chỉ gửi alert khi tín hiệu đạt ngưỡng score_min của từng mã (từ WATCHLIST_META).
    Cooldown 90 phút để tránh spam cùng mã liên tục.
    """
    if not CHAT_ID:
        return
    logger.info('Auto alert scanner started (interval=%dmin)' % SCAN_INTERVAL_MIN)

    _last_scan_slot   = -1   # Slot 10 phút cuối đã quét signal
    _last_report_slot = -998  # Slot báo cáo sáng/chiều

    while True:
        try:
            now      = datetime.now(VN_TZ)
            weekday  = now.weekday()
            h, m     = now.hour, now.minute
            in_session = is_trading_hours()
            total_min  = h * 60 + m

            # ── Báo cáo 08:30 — chuẩn bị phiên (GỌN) ──────────────────────
            if weekday < 5 and h == 8 and m == 30 and _last_report_slot != -998:
                _last_report_slot = -998
                logger.info('Morning report 08:30')
                try:
                    data_s = call_api('/api/signals') or []
                    mkt    = call_api('/api/market')  or {}
                    msg    = _build_morning_report(data_s, mkt)
                    broadcast(msg)
                except Exception as e:
                    logger.warning('Morning report error: ' + str(e))
                time.sleep(60)
                continue

            # ── Báo cáo 15:10 — tổng kết phiên (GỌN) ───────────────────────
            if weekday < 5 and h == 15 and m == 10 and _last_scan_slot != total_min:
                _last_scan_slot = total_min
                logger.info('Closing report 15:10')
                try:
                    data_s = call_api('/api/signals') or []
                    mkt    = call_api('/api/market')  or {}
                    msg    = _build_closing_report(data_s, mkt)
                    broadcast(msg)
                    # Market scan sau phiên — chạy trong thread riêng
                    threading.Thread(
                        target=lambda: handle_market_scan(CHAT_ID), daemon=True
                    ).start()
                except Exception as e:
                    logger.warning('Closing report error: ' + str(e))
                time.sleep(60)
                continue

            # ── Quét tín hiệu MUA/BAN mỗi 10 phút ───────────────────────────
            # Bỏ qua ATO (9:00-9:15) và ATC (14:30-15:00) — giá không ổn định
            in_ato = (h == 9 and m < 15)
            in_atc = (h == 14 and m >= 30) or (h == 15 and m == 0)

            scan_slot = total_min // SCAN_INTERVAL_MIN

            if weekday < 5 and in_session and scan_slot != _last_scan_slot and not in_ato and not in_atc and SCANNER_ENABLED.get('signal', True):
                _last_scan_slot = scan_slot
                logger.info('Scanner tick: ' + now.strftime('%H:%M %a'))

                data = call_api('/api/signals')
                # BUG-FIX: call_api trả {} khi lỗi → iterate dict keys → item là string → crash
                # Đảm bảo data luôn là list of dicts trước khi iterate
                if not data or not isinstance(data, list):
                    time.sleep(30)
                    continue

                for item in data:
                    if not isinstance(item, dict):
                        continue
                    sym    = item.get('symbol', '')
                    score  = item.get('score', 50)
                    action = item.get('action', '')
                    meta   = WATCHLIST_META.get(sym, {})
                    min_sc = meta.get('score_min', 65)

                    # Chỉ alert khi đạt ngưỡng score_min của mã đó
                    if action == 'MUA' and score < min_sc:
                        continue
                    if action not in ('MUA', 'BAN'):
                        continue

                    # Cooldown — không spam cùng mã cùng chiều
                    last = _last_alerts.get(sym)
                    if last:
                        last_score, last_time = last
                        same_dir = (last_score >= min_sc) == (score >= min_sc)
                        if same_dir and (time.time() - last_time) < ALERT_COOLDOWN_SEC:
                            continue

                    _last_alerts[sym] = (score, time.time())
                    send(format_alert(item), CHAT_ID)
                    time.sleep(2)


            # ── Quét MA10 / MA50 cross — 4 tiếng/lần ────────────────────────
            ma_scan_slot = total_min // MA_SCAN_INTERVAL_MIN
            if not hasattr(auto_alert_scanner, '_last_ma_slot'):
                auto_alert_scanner._last_ma_slot = -1

            if (weekday < 5 and in_session
                    and ma_scan_slot != auto_alert_scanner._last_ma_slot
                    and not in_ato and not in_atc
                    and SCANNER_ENABLED.get('ma', True)):
                auto_alert_scanner._last_ma_slot = ma_scan_slot
                logger.info('MA10/MA50 scan (4h): ' + now.strftime('%H:%M'))

                for sym in list(WATCHLIST_META.keys()):
                    try:
                        data = call_api('/api/analyze/' + sym)
                        if not data or not isinstance(data, dict) or data.get('error'):
                            continue

                        price      = data.get('price', 0)
                        score      = data.get('score', 50)
                        ma10_val   = data.get('ma10', 0)
                        ma50_val   = data.get('ma50', 0)
                        cross_up   = data.get('ma10_cross_up', False)
                        cross_down = data.get('ma10_cross_down', False)
                        ma50_slope = data.get('ma50_slope_up', False)

                        sym_last = _last_ma_alerts.setdefault(sym, {})
                        now_ts   = time.time()

                        if cross_up:
                            last = sym_last.get('ma10', ('', 0))
                            if last[0] != 'MA10_CROSS_UP' or now_ts - last[1] > MA_CROSS_COOLDOWN_SEC:
                                sym_last['ma10'] = ('MA10_CROSS_UP', now_ts)
                                msg = format_ma_alert(sym, 'MA10_CROSS_UP',
                                                      price, score, ma10_val, ma50_val, ma50_slope)
                                if msg:
                                    broadcast(msg)
                                    time.sleep(2)
                        elif cross_down:
                            last = sym_last.get('ma10', ('', 0))
                            if last[0] != 'MA10_CROSS_DOWN' or now_ts - last[1] > MA_CROSS_COOLDOWN_SEC:
                                sym_last['ma10'] = ('MA10_CROSS_DOWN', now_ts)
                                msg = format_ma_alert(sym, 'MA10_CROSS_DOWN',
                                                      price, score, ma10_val, ma50_val, ma50_slope)
                                if msg:
                                    broadcast(msg)
                                    time.sleep(2)

                    except Exception as e:
                        logger.warning(f'MA scan {sym}: {e}')
                _save_ma_alerts(_last_ma_alerts)

            # ── Extended MA scan (tier 2) — 1 tiếng/lần ─────────────────────
            ext_scan_slot = total_min // EXT_SCAN_INTERVAL_MIN
            if not hasattr(auto_alert_scanner, '_last_ext_slot'):
                auto_alert_scanner._last_ext_slot = -1

            if (weekday < 5 and in_session
                    and ext_scan_slot != auto_alert_scanner._last_ext_slot
                    and not in_ato and not in_atc
                    and SCANNER_ENABLED.get('ext', True)):
                auto_alert_scanner._last_ext_slot = ext_scan_slot
                logger.info('Extended MA scan (1h, tier2): ' + now.strftime('%H:%M'))

                for sym in WATCHLIST_EXTENDED:
                    if sym in WATCHLIST_META:
                        continue
                    try:
                        data = call_api('/api/analyze/' + sym)
                        if not data or not isinstance(data, dict) or data.get('error'):
                            continue

                        price      = data.get('price', 0)
                        score      = data.get('score', 50)
                        ma10_val   = data.get('ma10', 0)
                        ma50_val   = data.get('ma50', 0)
                        cross_up   = data.get('ma10_cross_up', False)
                        ma50_slope = data.get('ma50_slope_up', False)

                        sym_last = _last_ma_alerts.setdefault(sym, {})
                        now_ts   = time.time()

                        if cross_up and score >= 55:
                            last = sym_last.get('ma10_ext', ('', 0))
                            if last[0] != 'MA10_CROSS_UP' or now_ts - last[1] > MA_EXT_COOLDOWN_SEC:
                                sym_last['ma10_ext'] = ('MA10_CROSS_UP', now_ts)
                                msg = format_ma_alert(sym, 'MA10_CROSS_UP',
                                                      price, score, ma10_val, ma50_val,
                                                      ma50_slope, tier=2)
                                if msg:
                                    msg += chr(10) + '<i>⚠ Tier 2 — chua backtest, chi tham khao</i>'
                                    broadcast(msg)
                                    logger.info(f'[Tier2] MA10 cross up: {sym}')
                                    time.sleep(2)

                    except Exception as e:
                        logger.warning(f'Extended scan {sym}: {e}')
                    time.sleep(1)

            # ── ML MOMENTUM SCAN — 10 phút/lần, sleep(3s)/mã để tránh burst ──
            # 12 mã × sleep 3s = 36s hoàn thành → ~20 calls/phút peak (an toàn)
            ml_scan_slot = total_min // ML_SCAN_INTERVAL_MIN
            if (weekday < 5 and in_session
                    and ml_scan_slot != getattr(auto_alert_scanner, '_last_ml_slot', -1)
                    and not in_ato and not in_atc
                    and SCANNER_ENABLED.get('ml', True)):
                auto_alert_scanner._last_ml_slot = ml_scan_slot
                logger.info('ML Momentum scan (10min): ' + now.strftime('%H:%M')
                            + f' ({len(ML_CONFIRMED_WATCHLIST)} mã)')

                for sym in list(ML_CONFIRMED_WATCHLIST.keys()):
                    try:
                        time.sleep(3)  # Stagger calls — tránh burst 12 mã cùng lúc
                        data = call_api('/api/analyze/' + sym)
                        if not data or data.get('error'):
                            continue
                        price = data.get('price', 0)
                        _ms   = data.get('momentum_signal', {})
                        if not (_ms.get('tier1_pass')
                                and _ms.get('grade') in ('PASS', 'STRONG')):
                            continue
                        _ml_cfg      = ML_CONFIRMED_WATCHLIST[sym]
                        _ml_tier     = _ml_cfg[0]
                        _ml_note     = _ml_cfg[4]
                        # HDB: chỉ alert khi grade == STRONG (score >= 90)
                        if sym == 'HDB' and _ms.get('grade') != 'STRONG':
                            continue
                        # Tier A: cooldown 24h | Tier B: cooldown 36h
                        _ms_cooldown = 86400 if _ml_tier == 'A' else 129600
                        sym_last     = _last_ma_alerts.setdefault(sym, {})
                        now_ts       = time.time()
                        # Dùng key riêng per-symbol tránh conflict với MA state
                        last_ms = sym_last.get(f'momentum_{sym}', ('', 0))
                        if (last_ms[0] != 'MOMENTUM'
                                or now_ts - last_ms[1] > _ms_cooldown):
                            sym_last[f'momentum_{sym}'] = ('MOMENTUM', now_ts)
                            msg = format_momentum_signal(sym, _ms, price,
                                                         ml_tier=_ml_tier,
                                                         ml_note=_ml_note)
                            if msg:
                                broadcast(msg)
                                logger.info(
                                    f'ML alert [{_ml_tier}] [{_ms.get("grade")}]:'
                                    f' {sym} score={_ms.get("score")}'
                                )
                                time.sleep(2)
                    except Exception as e:
                        logger.warning(f'ML scan {sym}: {e}')
                _save_ma_alerts(_last_ma_alerts)

            # Sleep ngắn để vòng lặp nhẹ, kiểm tra mỗi 30 giây
            time.sleep(30)

        except Exception as e:
            logger.error('Scanner error: ' + str(e))
            time.sleep(60)


def main():
    import os
    import fcntl

    lock_fd = None
    try:
        # FIX: Xóa lock file cũ trước khi tạo mới để tránh bị kẹt sau Railway restart
        lock_path = '/tmp/scanner.lock'
        lock_fd = open(lock_path, 'w')
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fd.write(str(os.getpid()))
        lock_fd.flush()
        logger.info('Scanner lock acquired (PID=%d)' % os.getpid())
        t = threading.Thread(target=auto_alert_scanner, daemon=True)
        t.start()

    except (IOError, OSError):
        logger.info('Scanner already running in another process, skipping')
        if lock_fd:
            try:
                lock_fd.close()
            except Exception:
                pass

    poll_updates()


if __name__ == '__main__':
    main()
