import os
import json
import logging
import time
import threading
import requests
from datetime import datetime, timedelta
import pytz

VN_TZ = pytz.timezone('Asia/Ho_Chi_Minh')

# ── Semaphore giới hạn concurrent /api/analyze calls ─────────────────────────
# Flask dev server xử lý tuần tự → nhiều call đồng thời sẽ queue → timeout
# Giới hạn tối đa 2 call /analyze cùng lúc từ bot để Flask không bị overwhelm
_API_ANALYZE_SEM = threading.Semaphore(2)

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

from config import (
    SETTLEMENT_DAYS, SYMBOL_CONFIG, SIGNALS_WATCHLIST as _SIGNALS_WATCHLIST_CFG, SIGNALS_MANUAL,
    get_sl_tp, get_sl_tp_pct, get_min_score, get_wf_verdict,
    MIN_SCORE_BUY,
    HOLD_DAYS_OVERRIDE, POSITION_SIZE_CAPS, SCORE_THRESHOLDS_PER_SYMBOL,
    get_hold_days, get_position_size_cap,
    SCORE_SKIP_BUCKETS, is_score_in_skip_bucket,
    # ── SCB Signal System (S14) ────────────────────────────────────────────────
    SCB_WATCHLIST, SCB_WATCHLIST_TIER_A, SCB_WATCHLIST_TIER_B,
    SCB_SCORE_A_MIN, SCB_SCORE_B_MIN,
    SCB_HARD_SKIP, SCB_WF_STATS, SCB_BT_STATS,
    # ── ML Confirmed Watchlist ─────────────────────────────────────────────────
    ML_CONFIRMED_WATCHLIST,
)

# ── SIGNALS_WATCHLIST — Score A ────────────────────────────────────────────────
SIGNALS_WATCHLIST = list(_SIGNALS_WATCHLIST_CFG)

# ── market_context: import một lần duy nhất ────────────────────────────────────
try:
    import market_context as _mc
except ImportError:
    _mc = None
    logger.warning('market_context module not found — B-filter disabled')

# ── SCORECARD: Context Scorecard v3 — Advisory Layer ──────────────────────────
try:
    import context_scorecard_v3 as _sc
    logger.info('context_scorecard_v3 loaded OK')
except ImportError:
    try:
        import context_scorecard as _sc
        logger.warning('context_scorecard_v3 not found — fallback to v1')
    except ImportError:
        _sc = None
        logger.warning('context_scorecard module not found — scorecard disabled')

API_URL = os.environ.get('API_BASE_URL', 'http://localhost:8080')

TRADE_PERSONALITY = {
    # sym: (pattern, hold_note, emoji)
    'STB': (
        'slow',
        'Slow wins (WR slow=63% vs fast=29%) — giữ đủ hold, đừng exit sớm. '
        '86% HK đã lên MFE+7.6% rồi rơi → cân nhắc trailing khi +5%.',
        '🐢'
    ),
    'PC1': (
        'slow',
        'Slow wins (WR slow=62% vs fast=35%) — kiên nhẫn là edge. '
        '77% HK đã lên MFE+7.9% → trailing khi +5% có thể cứu nhiều lệnh.',
        '🐢'
    ),
    'MCH': (
        'slow',
        'Slow wins (WR slow=52% vs fast=38%) — lệnh cần thời gian. '
        '56% HK từng lên MFE+7.9% → trailing khi +5%.',
        '🐢'
    ),
    'HAH': (
        'slow',
        'Slow wins rõ (WR slow=61% vs fast=32%, Exp gap=+2.69%) — '
        'KHÔNG exit sớm. 90% HK đã lên MFE+7.7% → trailing ưu tiên.',
        '🐢'
    ),
    'DGC': (
        'slow',
        'Slow wins mạnh (Exp slow=+1.44% vs fast=-1.20%) — '
        'lệnh resolve chậm mới có edge. Đừng bị nhiễu bởi lình xình đầu.',
        '🐢'
    ),
    'NKG': (
        'slow',
        'Slow wins (WR slow=56% vs fast=48%, Exp gap=+1.07%) — '
        'giữ đủ hold. 85% HK đã lên MFE+6.1% → trailing khi +4%.',
        '🐢'
    ),
    'CTS': (
        'slow',
        'Slow wins (WR slow=59% vs fast=41%, Exp gap=+1.31%) — '
        'kiên nhẫn với mã này. Hold đủ ngưỡng, không cut sớm.',
        '🐢'
    ),
    'VND': (
        'slow',
        'Slow wins cực rõ (Exp slow=+2.08% vs fast=-1.85%) — '
        'KHÔNG exit sớm dù lình xình. Slow pattern mạnh nhất watchlist.',
        '🐢'
    ),
    'FRT': (
        'fast',
        'Fast wins (WR fast=58% vs slow=48%, Exp gap=+1.89%) — '
        'breakout mã: nếu sau 4 phiên MFE<+3% thì xem xét exit. '
        'Lệnh tốt resolve nhanh.',
        '🚀'
    ),
    'SSI': (
        'neutral',
        'Không có pattern rõ fast/slow (Exp gap chỉ 0.19%) — '
        'hold theo kế hoạch, không cần điều chỉnh timing.',
        '➡'
    ),
}


def _fmt_trade_personality(sym, hold_days):
    """
    Format trade personality note ngắn gọn cho /signals output.
    Trả về string 1-2 dòng.
    """
    p = TRADE_PERSONALITY.get(sym)
    if not p:
        return ''
    pattern, note, emoji = p
    NL = chr(10)

    if pattern == 'slow':
        header = f'{emoji} <b>Hold style: PATIENT</b> — giữ đủ {hold_days}d'
    elif pattern == 'fast':
        header = f'{emoji} <b>Hold style: BREAKOUT</b> — resolve trong 4p hoặc exit'
    else:
        header = f'{emoji} <b>Hold style: NEUTRAL</b> — hold theo kế hoạch {hold_days}d'

    return NL + ' ' + header + NL + ' <i>' + note + '</i>'


WATCHLIST_META = {}
for _sym in SIGNALS_WATCHLIST:
    _cfg = SYMBOL_CONFIG.get(_sym, {})
    _sl_pct, _tp_pct = get_sl_tp_pct(_sym)
    WATCHLIST_META[_sym] = {
        'score_min':    get_min_score(_sym),
        'sl':           _sl_pct,
        'tp':           _tp_pct,
        'sl_pct':       _sl_pct,
        'tp_pct':       _tp_pct,
        'group':        _cfg.get('group', 'Khac'),
        'wf_verdict':   get_wf_verdict(_sym),
        'hold_days':    get_hold_days(_sym),           # Session 9
        'size_cap':     get_position_size_cap(_sym),   # Session 9+10
        'skip_buckets': SCORE_SKIP_BUCKETS.get(_sym, []),  # Session 10: VND
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PAPER TRADING — Lưu trữ lệnh giả lập
# ═══════════════════════════════════════════════════════════════════════════════
PAPER_FILE   = '/tmp/paper_trades.json'
PAPER_MONTHS = 2   # Thời gian theo dõi (tháng)

# FIX (S16): Lock tránh race condition khi ML alert + ScB alert cùng lúc gọi
# _add_paper_trade() và đọc/ghi file đồng thời → duplicate ID hoặc mất trade.
_paper_lock = threading.Lock()

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

def _add_paper_trade(symbol, price, score, sl_pct, tp_pct,
                     source='ScoreA', extra=None):
    """
    Thêm lệnh MUA paper trade mới.
    source: 'ScoreA' | 'ScB' | 'ML'
    extra : dict metadata tuỳ source (score_b, ml_grade, vni_zone, ma20_zone, vol_zone...)

    FIX (S16): Wrap với _paper_lock để tránh race condition khi 2 thread
    (ML alert + ScB alert) gọi đồng thời → duplicate ID hoặc ghi đè nhau.
    ID dùng timestamp-microsecond thay vì len() để đảm bảo unique kể cả
    khi 2 lệnh vào cùng giây.
    """
    import time as _time
    with _paper_lock:
        data = _load_paper()
        # Tránh trùng lệnh cùng source trong cùng 1 ngày
        today = datetime.now(VN_TZ).strftime('%Y-%m-%d')
        existing = [t for t in data['trades']
                    if t['symbol'] == symbol and t['entry_date'] == today
                    and t['status'] == 'OPEN' and t.get('source', 'ScoreA') == source]
        if existing:
            return False, 'Da co lệnh OPEN [' + source + '] cho ' + symbol + ' hom nay'

        sl_price = round(price * (1 - sl_pct / 100))
        tp_price = round(price * (1 + tp_pct / 100))

        # Settlement: T+2 ngày giao dịch — mới bán được từ ngày này (HOSE từ 11/2021)
        settlement_date = _trading_days_after(today, SETTLEMENT_DAYS)

        # Expire: tính từ settlement_date (ngày có thể bán), không phải entry_date
        # Lý do: lệnh chỉ "có hiệu lực" từ T+2 — PAPER_MONTHS tính từ lúc có thể giao dịch
        trading_days_expire = PAPER_MONTHS * 22
        expire = _trading_days_after(settlement_date, trading_days_expire)

        # FIX: ID dùng timestamp microsecond thay vì len() — unique dù 2 thread cùng lúc
        trade_id = f'{today}-{symbol}-{source}-{int(_time.time()*1000) % 100000}'

        trade = {
            'id':              trade_id,
            'symbol':          symbol,
            'source':          source,
            'entry_date':      today,
            'entry_price':     price,
            'score':           score,
            'sl_price':        sl_price,
            'tp_price':        tp_price,
            'sl_pct':          sl_pct,
            'tp_pct':          tp_pct,
            'settlement_date': settlement_date,
            'expire_date':     expire,
            'status':          'OPEN',
            'exit_date':       None,
            'exit_price':      None,
            'pnl_pct':         None,
            'exit_reason':     None,
            'meta':            extra or {},
        }
        data['trades'].append(trade)
        _save_paper(data)
    return True, trade

def _update_paper_trades():
    """
    Cập nhật trạng thái các lệnh OPEN bằng cách lấy giá hiện tại từ API.
    Gọi định kỳ từ auto_alert_scanner (cuối phiên 15:00) hoặc khi /ptreport.

    Logic close:
      1. SL hit   : giá hiện tại ≤ sl_price → close với pnl = -sl_pct
      2. TP hit   : giá hiện tại ≥ tp_price → close với pnl = +tp_pct
      3. Expire   : hôm nay ≥ expire_date   → close với pnl = (current/entry - 1)%
      4. Settlement chưa qua: bỏ qua (chưa được phép bán)

    FIX (S16): Đây là hàm thiếu khiến /ptreport luôn báo closed:0.
    Paper trades không bao giờ close nếu không có hàm này.
    """
    with _paper_lock:
        data = _load_paper()
        open_trades = [t for t in data['trades'] if t.get('status') == 'OPEN']
        if not open_trades:
            return 0

        today = datetime.now(VN_TZ).strftime('%Y-%m-%d')
        closed_count = 0

        # Nhóm theo symbol để gọi API một lần/symbol
        sym_set = {t['symbol'] for t in open_trades}
        price_map = {}
        for sym in sym_set:
            try:
                _d = call_api('/api/analyze/' + sym)
                if _d and isinstance(_d, dict):
                    p = float(_d.get('price', 0) or 0)
                    if p > 0:
                        price_map[sym] = p
            except Exception as e:
                logger.warning(f'_update_paper price fetch {sym}: {e}')

        for trade in data['trades']:
            if trade.get('status') != 'OPEN':
                continue

            sym         = trade['symbol']
            entry_price = float(trade['entry_price'])
            sl_price    = float(trade['sl_price'])
            tp_price    = float(trade['tp_price'])
            sl_pct      = float(trade.get('sl_pct', 7.0))
            tp_pct      = float(trade.get('tp_pct', 14.0))
            settlement  = trade.get('settlement_date', today)
            expire      = trade.get('expire_date', today)

            # Chưa qua settlement → không thể bán, bỏ qua
            if today < settlement:
                continue

            current_price = price_map.get(sym)

            # ── Kiểm tra SL ──────────────────────────────────────────────
            if current_price and current_price <= sl_price:
                trade['status']      = 'CLOSED'
                trade['exit_date']   = today
                trade['exit_price']  = sl_price          # worst-case: thoát đúng SL
                trade['pnl_pct']     = round(-sl_pct, 2)
                trade['exit_reason'] = 'SL'
                closed_count += 1
                logger.info(f'Paper SL closed: {sym} [{trade.get("source")}]'
                            f' entry={entry_price} sl={sl_price} pnl={trade["pnl_pct"]}%')
                continue

            # ── Kiểm tra TP ──────────────────────────────────────────────
            if current_price and current_price >= tp_price:
                trade['status']      = 'CLOSED'
                trade['exit_date']   = today
                trade['exit_price']  = tp_price
                trade['pnl_pct']     = round(tp_pct, 2)
                trade['exit_reason'] = 'TP'
                closed_count += 1
                logger.info(f'Paper TP closed: {sym} [{trade.get("source")}]'
                            f' entry={entry_price} tp={tp_price} pnl={trade["pnl_pct"]}%')
                continue

            # ── Kiểm tra Expire ──────────────────────────────────────────
            if today >= expire:
                if current_price and entry_price > 0:
                    pnl = round((current_price / entry_price - 1) * 100, 2)
                    exit_p = current_price
                else:
                    # Không lấy được giá → dùng entry (pnl = 0, phản ánh unknown)
                    pnl    = 0.0
                    exit_p = entry_price
                trade['status']      = 'CLOSED'
                trade['exit_date']   = today
                trade['exit_price']  = exit_p
                trade['pnl_pct']     = pnl
                trade['exit_reason'] = 'EXPIRE'
                closed_count += 1
                logger.info(f'Paper EXPIRE closed: {sym} [{trade.get("source")}]'
                            f' entry={entry_price} exit={exit_p} pnl={pnl}%')

        if closed_count > 0:
            _save_paper(data)
            logger.info(f'_update_paper_trades: {closed_count} lệnh đã close')

    return closed_count


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
                    t = 90
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
            if '/analyze/' in endpoint or '/fairvalue/' in endpoint:
                with _API_ANALYZE_SEM:
                    r = requests.get(base + endpoint, timeout=t)
            else:
                r = requests.get(base + endpoint, timeout=t)
            if r.status_code == 200:
                return r.json()
        except requests.exceptions.Timeout:
            logger.warning(f'api {base}{endpoint}: timeout ({t}s)')
        except Exception as e:
            logger.warning('api ' + base + endpoint + ': ' + str(e))
    return {}


def call_api_fast(endpoint, timeout=20):
    """
    Gọi API với timeout ngắn — chỉ dùng cho sectorscan và các batch
    không cần FA compute. Không retry sang external URL để tránh tốn thêm
    thời gian khi Flask đang bận.
    Trả về {} nếu timeout/lỗi.
    """
    for base in ('http://localhost:8080', 'http://127.0.0.1:8080'):
        try:
            with _API_ANALYZE_SEM:
                r = requests.get(base + endpoint, timeout=timeout)
            if r.status_code == 200:
                return r.json()
        except requests.exceptions.Timeout:
            pass
        except Exception:
            pass
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

    # max_workers=2: match _API_ANALYZE_SEM = Semaphore(2)
    # Tránh deadlock khi 10 thread cùng chờ semaphore → timeout chồng timeout
    # Progress ping mỗi 5 mã để user biết bot đang chạy
    _done_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        futures = {ex.submit(_scan_one, s): s for s in sym_list}
        for fut in concurrent.futures.as_completed(futures):
            sym, r = fut.result()
            _done_count += 1
            # Progress ping mỗi 5 mã
            if _done_count % 5 == 0 or _done_count == total:
                send(f'⏳ Đang quét... {_done_count}/{total} mã', chat_id)
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

    # Split message để tránh Telegram reject khi > 4096 ký tự
    _header = '&#x1F4CA; <b>ML Scan — ' + mode_label + '</b>' + NL + '━' * 26
    send(_header, chat_id)
    for _part in parts:
        # Mỗi section (STRONG / PASS / NEAR) gửi riêng
        if len(_part) > 3800:
            # Cắt nếu quá dài (hiếm khi xảy ra)
            _part = _part[:3800] + NL + '<i>... (còn nữa, dùng /ml SYM để xem chi tiết)</i>'
        send(_part, chat_id)
    send(footer, chat_id)


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



# ═══════════════════════════════════════════════════════════════════════════════
# SCB SCAN — /scbscan handler (Session 14)
# ═══════════════════════════════════════════════════════════════════════════════

def _scb_get_ma20_zone(dist):
    if dist < 0:      return 'BELOW'
    elif dist < 2:    return 'NEAR'
    elif dist < 10:   return 'OPT'
    elif dist < 20:   return 'EXT'
    else:             return 'FAR'

def _scb_get_score_bucket(score):
    if score < 75:    return '65-74'
    elif score < 85:  return '75-84'
    elif score < 95:  return '85-94'
    else:             return '95+'

def _scb_get_zones(vni_chg, ma20_dist, vol_ratio, score_a):
    """Helper: classify zones cho ScB filter."""
    # VNI regime
    if vni_chg >= 1.0:   vni_regime = 'UP'
    elif vni_chg <= -2.0: vni_regime = 'DOWN'
    else:                 vni_regime = 'FLAT'

    # MA20 zone
    if ma20_dist < 0:     ma20_zone = 'BELOW'
    elif ma20_dist < 2:   ma20_zone = 'NEAR'
    elif ma20_dist < 10:  ma20_zone = 'OPT'
    elif ma20_dist < 20:  ma20_zone = 'EXT'
    else:                 ma20_zone = 'FAR'

    # Score bucket
    if score_a < 75:      score_bucket = '65-74'
    elif score_a < 85:    score_bucket = '75-84'
    elif score_a < 95:    score_bucket = '85-94'
    else:                 score_bucket = '95+'

    # Vol pattern
    if vol_ratio >= 2.0:  vol_pat = 'HIGH'
    elif vol_ratio >= 1.2: vol_pat = 'MED'
    elif vol_ratio >= 0.8: vol_pat = 'NORMAL'
    else:                 vol_pat = 'LOW'

    return vni_regime, ma20_zone, score_bucket, vol_pat


def _scb_check_hard_skip(sym, vni_regime, ma20_zone, score_bucket, vol_pat):
    """
    Kiểm tra per-symbol hard skip rules từ SCB_HARD_SKIP.
    Return: (skip: bool, reason: str)
    """
    rules = SCB_HARD_SKIP.get(sym, [])
    for cond_key, cond_val, reason in rules:
        if cond_key == 'vni_down'    and vni_regime == 'DOWN' and cond_val: return True, reason
        if cond_key == 'ma20_zone'   and ma20_zone  == cond_val:            return True, reason
        if cond_key == 'score_bucket' and score_bucket == cond_val:         return True, reason
        if cond_key == 'vol_high'    and vol_pat     == 'HIGH' and cond_val: return True, reason
    return False, ''


def _scb_format_signal(sym, score_a, score_b, vni_chg, ma20_dist, vol_ratio):
    """
    Format notification cho 1 mã ScB.
    3 layer: Universal gate → Per-symbol hard skip → Context notes + Premium flags.
    Return: (verdict, msg) với verdict in ('GO', 'CAUTION', 'SKIP')
    """
    NL = chr(10)
    tier = 'A' if sym in SCB_WATCHLIST_TIER_A else 'B'
    bt   = SCB_BT_STATS.get(sym, {})
    wf   = SCB_WF_STATS.get(sym, {})

    vni_regime, ma20_zone, score_bucket, vol_pat = _scb_get_zones(
        vni_chg, ma20_dist, vol_ratio, score_a
    )

    # ── Layer 1: Universal gate (3 điều kiện cứng) ───────────────────────────
    if vni_regime == 'DOWN':
        return 'SKIP', 'VNI↓'
    if score_a < SCB_SCORE_A_MIN.get(sym, 65):
        return 'SKIP', f'ScA={score_a}<{SCB_SCORE_A_MIN.get(sym, 65)}'
    if ma20_zone == 'BELOW':
        return 'SKIP', 'MA20↓'

    # ── Layer 2: Per-symbol hard skip ────────────────────────────────────────
    skip, skip_reason = _scb_check_hard_skip(sym, vni_regime, ma20_zone, score_bucket, vol_pat)
    if skip:
        # Rút gọn reason để fit vào skip list
        short = (skip_reason.split('Exp')[0].strip(' —').split('(')[0].strip()
                 if skip_reason else 'rule')
        return 'SKIP', short

    # ── Layer 3: Context notes + verdict ─────────────────────────────────────
    notes    = []
    premiums = []
    verdict  = 'GO'

    # VNI flat
    if vni_regime == 'FLAT':
        notes.append('⚠ VNI sideways — giảm size 30%')
        verdict = 'CAUTION'

    # Volume
    if vol_pat == 'LOW':
        notes.append('⚠ Vol thấp — thanh khoản yếu')
        verdict = 'CAUTION'

    # MA20 NEAR với mã nhạy cảm
    if ma20_zone == 'NEAR' and sym in ('CTG', 'VCB', 'GAS', 'DPM'):
        notes.append('⚠ Sát MA20 — cân nhắc chờ confirm')

    # ORS WF Prom
    if sym == 'ORS':
        notes.append('⚠ WF Prom — size 70%')

    # Tier B
    if tier == 'B':
        notes.append('⚠ Tier B — max 50% size')
        verdict = 'CAUTION'

    # MCH n_hi nhỏ
    if sym == 'MCH':
        notes.append('ℹ MCH n_hi=22L — sample nhỏ, 50% size')

    # Premium flags
    if vni_regime == 'UP' and score_a >= 85 and ma20_zone in ('OPT', 'EXT'):
        # EXT exceptions
        if not (sym in ('FPT', 'HAX', 'DPM') and ma20_zone == 'EXT'):
            premiums.append('🌟 GOLDEN SETUP — VNI UP + Score≥85 + MA20 OPT/EXT')
    if sym in ('BSI', 'CTG') and vol_pat == 'HIGH':
        premiums.append('🌟 Vol Breakout — BSI/CTG vol HIGH edge >6%')
    if sym in ('PC1', 'CTS', 'FPT') and vni_regime == 'FLAT':
        premiums.append('🛡 Defensive — mã này OK khi VNI sideways')
    if sym == 'PDR' and vni_regime == 'UP':
        premiums.append('🌟 PDR Momentum — WR 61% khi VNI UP')
    if sym == 'CSV' and vni_regime in ('FLAT', 'DOWN'):
        premiums.append('🛡 CSV Defensive — tốt khi thị trường yếu')

    # ── Build message ─────────────────────────────────────────────────────────
    vni_icon  = '✅' if vni_regime == 'UP' else '⚠'
    vol_icon  = '✅' if vol_pat == 'MED' else ('⚠' if vol_pat in ('LOW', 'HIGH') else '·')
    ma20_icon = '✅' if ma20_zone == 'OPT' else ('🌟' if ma20_zone == 'EXT' else '·')

    size_note = ('| Full size'   if tier == 'A' and sym != 'ORS'
                 else '| 70% size' if sym == 'ORS'
                 else '| ⚠ Half size')

    wf_line = ''
    if wf:
        worst_str = (f'+{wf["worst"]:.2f}%' if wf['worst'] >= 0 else f'{wf["worst"]:.2f}%')
        wf_line = (NL + f'WF: {wf["wf"]} | Median OOS: +{wf["median"]:.2f}% | Worst: {worst_str}')

    note_str    = (NL + NL.join(notes))    if notes    else ''
    premium_str = (NL + NL.join(premiums)) if premiums else ''
    verdict_str = '→ ✅ ĐỦ ĐIỀU KIỆN' if verdict == 'GO' else '→ ⚠ VÀO NHỎ HOẶC CHỜ'

    msg = (
        f'📊 <b>ScB SIGNAL — {sym}</b> | Tier {tier} {size_note}' + NL
        + f'ScA: <b>{score_a}</b> | ScB: <b>{score_b}</b>'
        + f' | WR: {bt.get("wr","?")}% | Exp: +{bt.get("exp","?")}% | PF: {bt.get("pf","?")}'
        + wf_line + NL
        + f'VNI: {vni_icon} {vni_regime} ({vni_chg:+.1f}%)'
        + f' | Vol: {vol_icon} {vol_pat} ({vol_ratio:.1f}x)'
        + f' | MA20: {ma20_icon} {ma20_zone} {ma20_dist:+.1f}%'
        + note_str
        + premium_str + NL
        + f'<b>{verdict_str}</b>'
    )
    return verdict, msg


def handle_scbscan(chat_id):
    """
    /scbscan — Quét 19 mã ScB watchlist.
    Gửi summary rồi từng signal GO/CAUTION.
    Chạy trong thread riêng.
    """
    NL = chr(10)

    def run():
        try:
            send('⏳ Đang quét ScB watchlist (19 mã)...', chat_id)

            # Lấy VNI change
            try:
                vni_data = call_api('/api/analyze/VNINDEX') or {}
                vni_chg  = float(vni_data.get('change_pct') or
                                  vni_data.get('vni_change_pct') or 0.0)
            except Exception:
                vni_chg = 0.0

            vni_icon = '▲' if vni_chg >= 1.0 else ('▼' if vni_chg <= -2.0 else '─')

            signals_go      = []
            signals_caution = []
            skip_list       = []

            for sym in SCB_WATCHLIST:
                try:
                    time.sleep(1)   # stagger — tránh rate limit
                    data = call_api('/api/analyze/' + sym)
                    if not data or data.get('error'):
                        skip_list.append(sym + '(err)')
                        continue

                    score_a   = int(data.get('score', 0) or 0)
                    score_b, _, _ = calc_score_b(data)
                    ma20_dist = float(data.get('dist_ma20_pct') or data.get('ma20_dist', 0) or 0)
                    vol_ratio = float(data.get('vol_ratio', 1.0) or 1.0)
                    sym_vni   = float(data.get('vni_change_pct') or vni_chg)

                    # ScB threshold check
                    if score_b < SCB_SCORE_B_MIN:
                        skip_list.append(f'{sym}(ScB={score_b})')
                        continue

                    verdict, msg = _scb_format_signal(
                        sym, score_a, score_b, sym_vni, ma20_dist, vol_ratio
                    )
                    if verdict == 'GO':
                        signals_go.append((sym, msg))
                        # Auto paper trade log
                        _price = float(data.get('price', 0) or 0)
                        if _price > 0:
                            _bt = SCB_BT_STATS.get(sym, {})
                            _ok, _ = _add_paper_trade(
                                sym, _price, score_a,
                                sl_pct=7.0, tp_pct=14.0,
                                source='ScB',
                                extra={
                                    'score_b':   score_b,
                                    'vni_chg':   round(sym_vni, 2),
                                    'ma20_dist': round(ma20_dist, 2),
                                    'vol_ratio': round(vol_ratio, 2),
                                    'bt_exp':    _bt.get('exp', 0),
                                    'bt_pf':     _bt.get('pf', 0),
                                    'bt_wr':     _bt.get('wr', 0),
                                    'tier':      'A' if sym in SCB_WATCHLIST_TIER_A else 'B',
                                }
                            )
                            if _ok:
                                logger.info(f'ScB paper trade logged: {sym} @{_price} ScB={score_b}')
                    elif verdict == 'CAUTION':
                        signals_caution.append((sym, msg))
                        # FIX: Log CAUTION vào paper trade với source='ScB_C'
                        # Để track performance của CAUTION signals (Tier B + VNI FLAT + Vol LOW)
                        # Phân biệt với GO ('ScB') trong /ptreport
                        _price_c = float(data.get('price', 0) or 0)
                        if _price_c > 0:
                            _bt_c = SCB_BT_STATS.get(sym, {})
                            _ok_c, _ = _add_paper_trade(
                                sym, _price_c, score_a,
                                sl_pct=7.0, tp_pct=14.0,
                                source='ScB_C',
                                extra={
                                    'score_b':   score_b,
                                    'verdict':   'CAUTION',
                                    'vni_chg':   round(sym_vni, 2),
                                    'ma20_dist': round(ma20_dist, 2),
                                    'vol_ratio': round(vol_ratio, 2),
                                    'bt_exp':    _bt_c.get('exp', 0),
                                    'bt_pf':     _bt_c.get('pf', 0),
                                    'bt_wr':     _bt_c.get('wr', 0),
                                    'tier':      'A' if sym in SCB_WATCHLIST_TIER_A else 'B',
                                }
                            )
                            if _ok_c:
                                logger.info(f'ScB_C paper trade logged: {sym} @{_price_c} ScB={score_b}')
                    else:
                        # msg chứa skip reason khi verdict==SKIP
                        reason = msg if msg else 'skip'
                        skip_list.append(f'{sym}({reason})')

                except Exception as e:
                    skip_list.append(sym + '(err)')
                    logger.debug(f'scbscan {sym}: {e}')

            # Summary header
            now_str = datetime.now(VN_TZ).strftime('%H:%M')
            header = (
                f'📊 <b>ScB SCAN</b> — {now_str}'
                + f' | VNI: {vni_icon} ({vni_chg:+.1f}%)' + NL
                + '─' * 28 + NL
                + f'✅ GO: {len(signals_go)}'
                + f' | ⚠ CAUTION: {len(signals_caution)}'
                + f' | Skip: {len(skip_list)}' + NL
                + '─' * 28
            )
            send(header, chat_id)

            # GO signals
            for _, msg in signals_go:
                send(msg, chat_id)
                time.sleep(0.5)

            # CAUTION signals
            for _, msg in signals_caution:
                send(msg, chat_id)
                time.sleep(0.5)

            # Skip summary
            if skip_list:
                send('⏭ Skip: ' + ' '.join(skip_list), chat_id)

            if not signals_go and not signals_caution:
                send('Không có ScB signal đủ điều kiện lúc này.', chat_id)

        except Exception as e:
            logger.error('handle_scbscan: ' + str(e))
            send('❌ Lỗi ScB scan: ' + str(e)[:120], chat_id)

    threading.Thread(target=run, daemon=True).start()



# ═══════════════════════════════════════════════════════════════════════════════
# PAPER TRADE REPORT — ScB & ML tracking
# ═══════════════════════════════════════════════════════════════════════════════

def _pt_summary_by_source(trades, source):
    """Tính WR/Exp/PF cho một nhóm lệnh đã closed theo source."""
    closed = [t for t in trades
              if t.get('source') == source and t.get('status') != 'OPEN'
              and t.get('pnl_pct') is not None]
    if not closed:
        return None
    n      = len(closed)
    wins   = [t for t in closed if t['pnl_pct'] > 0]
    losses = [t for t in closed if t['pnl_pct'] <= 0]
    wr     = len(wins) / n * 100
    avg    = sum(t['pnl_pct'] for t in closed) / n
    gross_win  = sum(t['pnl_pct'] for t in wins)  or 0
    gross_loss = abs(sum(t['pnl_pct'] for t in losses)) or 0.001
    pf     = round(gross_win / gross_loss, 2)
    return {'n': n, 'wr': round(wr, 1), 'avg': round(avg, 2), 'pf': pf,
            'wins': len(wins), 'losses': len(losses)}


def handle_ptreport(chat_id, args=None):
    """
    /ptreport        — Báo cáo paper trade tổng hợp (ScB + ML)
    /ptreport scb    — Chỉ ScB
    /ptreport ml     — Chỉ ML
    /ptreport open   — Lệnh đang mở

    FIX (S16): Nhận args để sub-command hoạt động.
    FIX (S16): Gọi _update_paper_trades() trước khi render để close SL/TP/expire.
    """
    NL = chr(10)

    # FIX: Parse filter trước khi load data
    _filter = (args[0].lower() if args else '').strip()

    # FIX: Update trước — close các lệnh SL/TP/expire rồi mới đọc stats
    _n_closed = _update_paper_trades()
    if _n_closed > 0:
        send(f'🔄 Đã tự động close <b>{_n_closed}</b> lệnh (SL/TP/Expire).', chat_id)

    data   = _load_paper()
    trades = data.get('trades', [])
    if not trades:
        send('📋 Chưa có paper trade nào được ghi nhận.', chat_id)
        return

    # FIX: Sub-command filter — /ptreport open, scb, ml
    if _filter == 'open':
        open_trades = [t for t in trades if t.get('status') == 'OPEN']
        if not open_trades:
            send('📂 Không có lệnh đang mở.', chat_id)
            return
        msg = f'📂 <b>Lệnh đang mở ({len(open_trades)}):</b>' + NL
        for t in sorted(open_trades, key=lambda x: x.get('entry_date',''), reverse=True):
            src = t.get('source', 'ScoreA')
            src_tag = {'ScB': '🔵', 'ScB_C': '🔵⚠', 'ML': '🟣', 'ScoreA': '🟢'}.get(src, '⚪')
            msg += (f'  {src_tag} <b>{t["symbol"]}</b> [{src}]'
                    f' | {t["entry_date"]} @{t["entry_price"]:,.0f}'
                    f' | SL: {t["sl_price"]:,.0f} TP: {t["tp_price"]:,.0f}' + NL)
        send(msg, chat_id)
        return

    # Filter theo source nếu có
    _src_filter = {'scb': 'ScB', 'ml': 'ML', 'sca': 'ScoreA', 'scbc': 'ScB_C'}.get(_filter)

    now_str = datetime.now(VN_TZ).strftime('%d/%m/%Y %H:%M')

    # ── Header ────────────────────────────────────────────────────────────────
    open_trades  = [t for t in trades if t.get('status') == 'OPEN']
    closed_trades = [t for t in trades if t.get('status') != 'OPEN']
    scb_open  = [t for t in open_trades if t.get('source') == 'ScB']
    scbc_open = [t for t in open_trades if t.get('source') == 'ScB_C']
    ml_open   = [t for t in open_trades if t.get('source') == 'ML']
    sca_open  = [t for t in open_trades if t.get('source') == 'ScoreA']

    msg = (
        f'📊 <b>PAPER TRADE REPORT</b> — {now_str}' + NL
        + '─' * 30 + NL
        + f'Tổng: {len(trades)}L | Đang mở: {len(open_trades)}L | Đã đóng: {len(closed_trades)}L' + NL
        + f'  ScB GO: {len(scb_open)} | ScB CAUTION: {len(scbc_open)} | ML: {len(ml_open)} | ScA: {len(sca_open)}' + NL
        + '─' * 30
    )
    send(msg, chat_id)

    # FIX: Nếu có filter source → chỉ hiển thị source đó
    sources_to_show = [
        ('ScB',    '📊 Score B (GO)',      'backtest avg ~2%+'),
        ('ScB_C',  '📊 Score B (CAUTION)', 'Tier B + VNI FLAT — track only'),
        ('ML',     '🤖 ML Momentum',       'backtest WR 55-67%'),
        ('ScoreA', '🎯 Score A',           'backtest PF avg 1.52'),
    ]
    if _src_filter:
        sources_to_show = [(s, l, r) for s, l, r in sources_to_show if s == _src_filter]

    for src, label, bt_exp_ref in sources_to_show:
        stats = _pt_summary_by_source(trades, src)
        open_n = len([t for t in open_trades if t.get('source') == src])
        if not stats and open_n == 0:
            continue

        if not stats:
            send(f'{label}: {open_n} lệnh đang mở, chưa có lệnh đóng.', chat_id)
            continue

        # So sánh WR với backtest expectation
        if src == 'ScB':
            bt_wr_ref = 54  # avg Tier A
            wr_diff = stats['wr'] - bt_wr_ref
            wr_icon = '✅' if wr_diff >= -5 else ('⚠️' if wr_diff >= -10 else '❌')
        elif src == 'ML':
            bt_wr_ref = 60
            wr_diff = stats['wr'] - bt_wr_ref
            wr_icon = '✅' if wr_diff >= -5 else ('⚠️' if wr_diff >= -10 else '❌')
        else:
            wr_icon = ''
            wr_diff = 0

        pf_icon = '✅' if stats['pf'] >= 1.3 else ('⚠️' if stats['pf'] >= 1.0 else '❌')

        block = (
            NL + f'<b>{label}</b>' + NL
            + f'  Closed: {stats["n"]}L | Open: {open_n}L' + NL
            + f'  WR: {stats["wr"]}% {wr_icon}'
            + (f' (vs BT ~{bt_wr_ref}%: {wr_diff:+.0f}%)' if wr_icon else '') + NL
            + f'  Avg PnL: {stats["avg"]:+.2f}% | PF: {stats["pf"]} {pf_icon}' + NL
            + f'  W:{stats["wins"]} / L:{stats["losses"]}' + NL
            + f'  Ref: {bt_exp_ref}'
        )
        send(block, chat_id)

    # ── Lệnh đang mở ─────────────────────────────────────────────────────────
    if open_trades:
        open_msg = NL + '📂 <b>Lệnh đang mở:</b>' + NL
        for t in sorted(open_trades, key=lambda x: x.get('entry_date',''), reverse=True)[:15]:
            src   = t.get('source', 'ScoreA')
            src_tag = {'ScB': '🔵', 'ML': '🟣', 'ScoreA': '🟢'}.get(src, '⚪')
            meta  = t.get('meta', {})
            extra = ''
            if src == 'ScB' and meta.get('score_b'):
                extra = f' ScB={meta["score_b"]}'
            elif src == 'ML' and meta.get('ml_grade'):
                extra = f' {meta["ml_grade"]}'
            open_msg += (
                f'  {src_tag} <b>{t["symbol"]}</b> [{src}]{extra}'
                f' | Entry: {t["entry_date"]} @{t["entry_price"]:,.0f}'
                f' | SL: {t["sl_price"]:,.0f} TP: {t["tp_price"]:,.0f}' + NL
            )
        if len(open_trades) > 15:
            open_msg += f'  ... và {len(open_trades)-15} lệnh nữa' + NL
        send(open_msg, chat_id)

    # ── Per-symbol breakdown cho ScB ─────────────────────────────────────────
    scb_closed = [t for t in closed_trades if t.get('source') == 'ScB']
    if scb_closed:
        sym_stats = {}
        for t in scb_closed:
            s = t['symbol']
            if s not in sym_stats:
                sym_stats[s] = {'n': 0, 'wins': 0, 'pnl': []}
            sym_stats[s]['n'] += 1
            sym_stats[s]['pnl'].append(t['pnl_pct'])
            if t['pnl_pct'] > 0:
                sym_stats[s]['wins'] += 1

        sym_lines = NL + '🔵 <b>ScB per-symbol (closed):</b>' + NL
        for sym, ss in sorted(sym_stats.items()):
            avg_pnl = sum(ss['pnl']) / ss['n']
            wr_s    = ss['wins'] / ss['n'] * 100
            bt      = SCB_BT_STATS.get(sym, {})
            diff    = avg_pnl - bt.get('exp', 0)
            diff_icon = '✅' if diff >= -0.5 else ('⚠️' if diff >= -1.5 else '❌')
            sym_lines += (
                f'  <b>{sym}</b>: {ss["n"]}L WR={wr_s:.0f}%'
                f' Avg={avg_pnl:+.2f}% {diff_icon}'
                f' (BT={bt.get("exp",0):+.2f}%)' + NL
            )
        send(sym_lines, chat_id)

    # ── Footer: methodology note ──────────────────────────────────────────────
    send(
        NL + '<i>⚠️ ≥20L closed/source mới đủ tin cậy thống kê.'
        + NL + 'Nếu live Avg thấp hơn BT Exp quá 1.5% trong 3 tháng → review methodology.</i>',
        chat_id
    )



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

    # FIX: Score display với lý do trừ điểm inline + hard block warning
    def _fmt_score_header(score, score_adj, b_delta, b_details):
        if b_delta == 0 or not b_details:
            return 'Diem A: <b>' + str(score) + '/100</b>'
        # Lấy tối đa 2 reasons, ưu tiên critical trước
        critical = [d for d in b_details if d['delta'] <= -15]
        others   = [d for d in b_details if -15 < d['delta'] != 0]
        reasons  = (critical + others)[:2]
        reason_str = ' | '.join(d['icon'] + ' ' + d['label'] for d in reasons)
        sign = '+' if b_delta > 0 else ''
        return (
            'Diem A: <b>' + str(score) + '/100</b>'
            + ' → <b>' + str(score_adj) + '/100</b>'
            + ' (' + sign + str(b_delta) + 'd: ' + reason_str + ')'
        )

    hard_block_line = ''
    if b_details:
        _hkeys = {d['key'] for d in b_details}
        if 'TIER0' in _hkeys:
            hard_block_line = chr(10) + '🔴 <b>HARD BLOCK: Thanh khoản TRAI — KHÔNG TRADE dù score cao</b>'
        elif 'UTAD' in _hkeys:
            hard_block_line = chr(10) + '🔴 <b>HARD BLOCK: UTAD — Bẫy tăng nguy hiểm, tránh vào lệnh</b>'

    msg = (
            '<b>' + prefix + ' ' + sym + '</b>\n'
            + '=' * 30 + '\n'
            + 'Giá: <b>' + f'{price:,.0f}' + 'd</b>'
            + '  ' + _fmt_score_header(score, score_adj, b_delta, b_details)
            + ' ' + ae
            + hard_block_line
            + tio_line + div_line + ma10_cross_line
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
        m = re.match(r'^(s|sl|tp|hold|trail)=([\d.]+)$', p.lower())
        if m:
            kw[m.group(1)] = float(m.group(2))
        else:
            pos.append(p.upper())

    symbol     = pos[0] if pos else ''
    is_full    = 'FULL'    in pos
    is_all     = symbol == 'ALL'
    # Custom params
    custom_score = int(kw['s'])    if 's'    in kw else None
    custom_sl    = kw['sl'] / 100  if 'sl'   in kw else None
    custom_tp    = kw['tp'] / 100  if 'tp'   in kw else None
    custom_hold  = int(kw['hold']) if 'hold' in kw else None
    # Trailing stop: bật bằng /bt <MA> trail=1  hoặc /bt <MA> ts
    use_ts       = 'TS' in pos or (bool(int(kw['trail'])) if 'trail' in kw else False)
    has_custom   = bool(kw)

    if not symbol:
        send(
            '&#x1F4CA; <b>Lenh /bt — Backtest</b>\n\n'
            '<b>Cu phap chuan:</b>\n'
            ' /bt MBB         — Backtest compact (~3 phut)\n'
            ' /bt MBB full    — Full + B-filter (~7 phut)\n'
            ' /bt all         — Toàn watchlist (~15 phut)\n\n'
            '<b>Cu phap tuy chinh:</b>\n'
            ' /bt DGC s=60           — Score threshold = 60\n'
            ' /bt DGC sl=5 tp=20     — SL=5% TP=20%\n'
            ' /bt DGC s=55 sl=7 tp=20 hold=7 — Full custom\n\n'
            '<b>Giái thich:</b>\n'
            ' s=     Score threshold (mặc định 65)\n'
            ' sl=    Stop Loss % (mặc định 7)\n'
            ' tp=    Take Profit % (mặc định 14)\n'
            ' hold=  So phien giu lệnh (mặc định 10)\n'
            ' trail=1  Trailing Stop 1R/3R (TP mo rong)',
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
                       ctp=custom_tp, ch=custom_hold, ts=use_ts:
            _handle_bt_symbol(s, c, f, cs, csl, ctp, ch, trailing_stop=ts),
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


def _fmt_trade_analytics(trades_df, cfg_sl, cfg_score):
    """
    Deep trade analytics — 4 blocks:
      1. Market Regime
      2. Entry Quality + Combo Discovery (2F)
      3. MFE/MAE + Post-Entry Path (with fixes)
      4. Expectancy + Drawdown
    Returns list of HTML strings.
    """
    import numpy as np
    NL = chr(10)

    # ── helpers ───────────────────────────────────────────────────────────────
    def _wr(df):
        return len(df[df['pnl'] > 0]) / len(df) * 100 if len(df) > 0 else 0.0

    def _exp(df):
        if len(df) == 0: return 0.0
        wins   = df[df['pnl'] > 0]['pnl']
        losses = df[df['pnl'] <= 0]['pnl']
        wr = len(wins) / len(df)
        aw = wins.mean()        if len(wins)   > 0 else 0.0
        al = abs(losses.mean()) if len(losses) > 0 else 0.0
        return round(wr * aw - (1 - wr) * al, 3)

    def _wi(wr, exp=None):
        if exp is not None:
            if exp > 0.5:  return '&#x2705;'
            if exp > 0:    return '&#x1F7E1;'
            return '&#x274C;'
        if wr >= 60: return '&#x2705;'
        if wr >= 50: return '&#x1F7E1;'
        return '&#x274C;'

    def _sizing(exp, wr):
        if exp >= 0.8 and wr >= 60: return 'FULL SIZE &#x1F4AA;'
        if exp >= 0.4 and wr >= 55: return '70-80% size'
        if exp >= 0.1 and wr >= 50: return '50% size'
        return 'SKIP / 25% size &#x26A0;'

    def _n_label(n):
        if n >= 15: return ''
        if n >= 8:  return ' &#x26A0;explr'
        return None   # bỏ qua

    def _pct(a, b):
        return f'{a/b*100:.0f}%' if b > 0 else 'N/A'

    try:
        buy_df = trades_df[trades_df['action'] == 'MUA'].copy()
        if len(buy_df) < 10:
            return []

        sl_df  = buy_df[buy_df['reason'] == 'sl']
        tp_df  = buy_df[buy_df['reason'].isin(['tp', 'trail'])]
        hk_df  = buy_df[buy_df['reason'] == 'expired']
        n      = len(buy_df)
        overall_exp = _exp(buy_df)

        has = lambda c: c in buy_df.columns and buy_df[c].notna().sum() > 5
        messages = []

        # ══════════════════════════════════════════════════════════════════
        # BLOCK 1 — MARKET REGIME
        # ══════════════════════════════════════════════════════════════════
        b1 = ['&#x1F30F; <b>BLOCK 1 — MARKET REGIME</b>', '─'*26]

        if has('vni_slope'):
            buy_df['_vtrend'] = buy_df['vni_slope'].apply(
                lambda s: 'UP(>2%)' if s >= 2 else ('DOWN(<-2%)' if s <= -2 else 'FLAT'))
            b1.append('<b>1A. VNI Trend luc entry:</b>')
            for ctx, icon in [('UP(>2%)', '&#x1F4C8;'), ('FLAT', '&#x27A1;'), ('DOWN(<-2%)', '&#x1F4C9;')]:
                d = buy_df[buy_df['_vtrend'] == ctx]
                if len(d) < 3: continue
                wr_c = _wr(d); ex_c = _exp(d)
                b1.append(f'  {icon} {ctx}: {_wi(wr_c, ex_c)} WR={wr_c:.0f}%'
                          f' Exp={ex_c:+.2f} ({len(d)}L)')

        if has('vni_atr_ratio'):
            med_atr = buy_df['vni_atr_ratio'].median()
            buy_df['_vatr'] = buy_df['vni_atr_ratio'].apply(
                lambda x: 'HIGH_VOL' if x > med_atr * 1.3
                          else ('LOW_VOL' if x < med_atr * 0.7 else 'NORMAL_VOL'))
            b1.append(f'{NL}<b>1B. Market Volatility (VNI ATR proxy, median={med_atr:.2f}%):</b>')
            for ctx, icon in [('LOW_VOL', '&#x1F7E2;'), ('NORMAL_VOL', '&#x1F7E1;'), ('HIGH_VOL', '&#x1F534;')]:
                d = buy_df[buy_df['_vatr'] == ctx]
                if len(d) < 3: continue
                wr_c = _wr(d); ex_c = _exp(d)
                b1.append(f'  {icon} {ctx}: {_wi(wr_c, ex_c)} WR={wr_c:.0f}%'
                          f' Exp={ex_c:+.2f} ({len(d)}L)')

        if has('vni_ma20_dist'):
            buy_df['_vma'] = buy_df['vni_ma20_dist'].apply(
                lambda d: 'OVERBOUGHT(>3%)' if d >= 3
                          else ('OVERSOLD(<-3%)' if d <= -3 else 'NEUTRAL'))
            b1.append(f'{NL}<b>1C. VNI vs MA20 (macro OB/OS):</b>')
            for ctx in ['OVERSOLD(<-3%)', 'NEUTRAL', 'OVERBOUGHT(>3%)']:
                d = buy_df[buy_df['_vma'] == ctx]
                if len(d) < 3: continue
                wr_c = _wr(d); ex_c = _exp(d)
                b1.append(f'  {_wi(wr_c, ex_c)} {ctx}: WR={wr_c:.0f}%'
                          f' Exp={ex_c:+.2f} ({len(d)}L)')

        if has('vni_slope') and has('vni_atr_ratio'):
            buy_df['_combo1'] = buy_df['_vtrend'] + '|' + buy_df['_vatr']
            combos = [(k, g) for k, g in buy_df.groupby('_combo1') if len(g) >= 3]
            combos.sort(key=lambda x: _exp(x[1]), reverse=True)
            b1.append(f'{NL}<b>1D. Best/Worst regime combos (Trend|Vol):</b>')
            for combo, gdf in combos[:3]:
                wr_c = _wr(gdf); ex_c = _exp(gdf)
                b1.append(f'  {_wi(wr_c, ex_c)} {combo}: WR={wr_c:.0f}%'
                          f' Exp={ex_c:+.2f} ({len(gdf)}L)')
            if len(combos) > 3:
                combo, gdf = combos[-1]
                wr_c = _wr(gdf); ex_c = _exp(gdf)
                b1.append(f'  &#x274C; WORST: {combo}: WR={wr_c:.0f}%'
                          f' Exp={ex_c:+.2f} ({len(gdf)}L)')

        # Insight
        if '_vtrend' in buy_df.columns and '_vatr' in buy_df.columns:
            vatr_exp = {ctx: _exp(buy_df[buy_df['_vatr'] == ctx])
                        for ctx in ['LOW_VOL', 'NORMAL_VOL', 'HIGH_VOL']
                        if len(buy_df[buy_df['_vatr'] == ctx]) >= 3}
            vtrend_exp = {ctx: _exp(buy_df[buy_df['_vtrend'] == ctx])
                          for ctx in ['UP(>2%)', 'FLAT', 'DOWN(<-2%)']
                          if len(buy_df[buy_df['_vtrend'] == ctx]) >= 3}
            insights = []
            if vatr_exp:
                best_v = max(vatr_exp, key=vatr_exp.get)
                worst_v = min(vatr_exp, key=vatr_exp.get)
                if vatr_exp[best_v] - vatr_exp[worst_v] >= 0.5:
                    insights.append(
                        f'{best_v} Exp cao nhat — '
                        + ('tranh trade khi bien dong manh'
                           if best_v == 'LOW_VOL'
                           else 'ma nay THICH high vol'))
            if vtrend_exp:
                best_t = max(vtrend_exp, key=vtrend_exp.get)
                worst_t = min(vtrend_exp, key=vtrend_exp.get)
                if vtrend_exp[best_t] - vtrend_exp[worst_t] >= 0.3:
                    insights.append(
                        f'VNI {best_t} la tot nhat '
                        f'(Exp gap={vtrend_exp[best_t]-vtrend_exp[worst_t]:+.2f})')
            if insights:
                b1.append(f'{NL}&#x1F4A1; ' + f'{NL}&#x1F4A1; '.join(insights))

        if len(b1) > 2:
            messages.append(NL.join(b1))

        # ══════════════════════════════════════════════════════════════════
        # BLOCK 2 — ENTRY QUALITY + COMBO DISCOVERY
        # ══════════════════════════════════════════════════════════════════
        b2 = ['&#x1F3AF; <b>BLOCK 2 — ENTRY QUALITY</b>', '─'*26]

        if has('hh20_dist'):
            buy_df['_hh20_ctx'] = buy_df['hh20_dist'].apply(
                lambda d: 'AT_BREAKOUT(0%)' if d >= -1
                          else ('NEAR(-1to-5%)' if d >= -5 else 'EXTENDED(<-5%)'))
            b2.append('<b>2A. Structure — khoang cach tu HH20:</b>')
            for ctx in ['AT_BREAKOUT(0%)', 'NEAR(-1to-5%)', 'EXTENDED(<-5%)']:
                d = buy_df[buy_df['_hh20_ctx'] == ctx]
                if len(d) < 3: continue
                wr_c = _wr(d); ex_c = _exp(d)
                b2.append(f'  {_wi(wr_c, ex_c)} {ctx}: WR={wr_c:.0f}%'
                          f' Exp={ex_c:+.2f} ({len(d)}L)')

        if has('ma20_dist'):
            buy_df['_ma20_ctx'] = buy_df['ma20_dist'].apply(
                lambda d: 'BELOW_MA20' if d < 0
                          else ('NEAR(0-2%)' if d < 2
                                else ('OK(2-5%)' if d < 5 else 'EXTENDED(>5%)')))
            b2.append(f'{NL}<b>2B. Gia entry vs MA20:</b>')
            for ctx in ['BELOW_MA20', 'NEAR(0-2%)', 'OK(2-5%)', 'EXTENDED(>5%)']:
                d = buy_df[buy_df['_ma20_ctx'] == ctx]
                if len(d) < 3: continue
                wr_c = _wr(d); ex_c = _exp(d)
                b2.append(f'  {_wi(wr_c, ex_c)} {ctx}: WR={wr_c:.0f}%'
                          f' Exp={ex_c:+.2f} ({len(d)}L)')

        if has('ma20_slope'):
            buy_df['_slope_ctx'] = buy_df['ma20_slope'].apply(
                lambda s: 'RISING(>0.5%)' if s >= 0.5
                          else ('FLAT' if s >= -0.5 else 'FALLING(<-0.5%)'))
            b2.append(f'{NL}<b>2C. MA20 Slope (momentum):</b>')
            for ctx in ['RISING(>0.5%)', 'FLAT', 'FALLING(<-0.5%)']:
                d = buy_df[buy_df['_slope_ctx'] == ctx]
                if len(d) < 3: continue
                wr_c = _wr(d); ex_c = _exp(d)
                b2.append(f'  {_wi(wr_c, ex_c)} {ctx}: WR={wr_c:.0f}%'
                          f' Exp={ex_c:+.2f} ({len(d)}L)')

        if has('vol_structure') and has('vol_ratio'):
            buy_df['_vol_ctx'] = buy_df.apply(lambda r: (
                'CLIMAX_VOL(spike+ext)'  if r['vol_structure'] < 0 else (
                'BREAKOUT_VOL(spike+base)' if r['vol_structure'] > 0 else (
                'HIGH_VOL(>1.5x)'        if r['vol_ratio'] >= 1.5 else (
                'LOW_VOL(<0.8x)'         if r['vol_ratio'] < 0.8 else 'NORMAL')))), axis=1)
            b2.append(f'{NL}<b>2D. Volume context:</b>')
            for ctx in ['BREAKOUT_VOL(spike+base)', 'NORMAL', 'HIGH_VOL(>1.5x)',
                        'LOW_VOL(<0.8x)', 'CLIMAX_VOL(spike+ext)']:
                d = buy_df[buy_df['_vol_ctx'] == ctx]
                if len(d) < 3: continue
                wr_c = _wr(d); ex_c = _exp(d)
                b2.append(f'  {_wi(wr_c, ex_c)} {ctx}: WR={wr_c:.0f}%'
                          f' Exp={ex_c:+.2f} ({len(d)}L)')

        if has('roc5'):
            buy_df['_roc_ctx'] = buy_df['roc5'].apply(
                lambda r: 'STRONG(>3%)' if r >= 3
                          else ('OK(0-3%)' if r >= 0 else 'WEAK(<0%)'))
            b2.append(f'{NL}<b>2E. ROC-5 (toc do tang 5 phien truoc entry):</b>')
            for ctx in ['STRONG(>3%)', 'OK(0-3%)', 'WEAK(<0%)']:
                d = buy_df[buy_df['_roc_ctx'] == ctx]
                if len(d) < 3: continue
                wr_c = _wr(d); ex_c = _exp(d)
                b2.append(f'  {_wi(wr_c, ex_c)} {ctx}: WR={wr_c:.0f}%'
                          f' Exp={ex_c:+.2f} ({len(d)}L)')

        # ── 2F. Combo Discovery ───────────────────────────────────────────
        # 6 cặp có ý nghĩa kinh tế — không brute force
        COMBO_PAIRS = [
            ('_vtrend',   '_ma20_ctx',  'VNI_trend × MA20_dist'),
            ('_vtrend',   '_vol_ctx',   'VNI_trend × Vol_context'),
            ('_vatr',     '_ma20_ctx',  'ATR_vol × MA20_dist'),
            ('_vatr',     '_vol_ctx',   'ATR_vol × Vol_context'),
            ('_ma20_ctx', '_slope_ctx', 'MA20_dist × Slope'),
            ('_vol_ctx',  '_slope_ctx', 'Vol_context × Slope'),
        ]
        valid_pairs = [(f1, f2, label) for f1, f2, label in COMBO_PAIRS
                       if f1 in buy_df.columns and f2 in buy_df.columns]

        if valid_pairs:
            b2.append(f'{NL}<b>2F. Combo Discovery (2-way, n>=8):</b>')

            # Collect all valid combos across pairs
            all_combos = []
            for f1, f2, pair_label in valid_pairs:
                for (v1, v2), gdf in buy_df.groupby([f1, f2]):
                    if len(gdf) < 8: continue
                    ex_c = _exp(gdf)
                    wr_c = _wr(gdf)
                    # Time stability: split first vs second half
                    mid = len(buy_df) // 2
                    first_half  = gdf[gdf.index < buy_df.index[mid]]
                    second_half = gdf[gdf.index >= buy_df.index[mid]]
                    exp_h1 = _exp(first_half)  if len(first_half)  >= 3 else None
                    exp_h2 = _exp(second_half) if len(second_half) >= 3 else None
                    unstable = (exp_h1 is not None and exp_h2 is not None
                                and exp_h1 * exp_h2 < 0)  # đổi dấu
                    # Effect size vs overall
                    strong = ex_c > overall_exp + 1.0
                    combo_label = f'{v1}+{v2}'
                    all_combos.append({
                        'label': combo_label, 'pair': pair_label,
                        'n': len(gdf), 'wr': wr_c, 'exp': ex_c,
                        'unstable': unstable, 'strong': strong,
                    })

            if all_combos:
                all_combos.sort(key=lambda x: x['exp'], reverse=True)
                top3   = all_combos[:3]
                worst1 = all_combos[-1] if len(all_combos) > 3 else None

                b2.append(f'  (Tong {len(all_combos)} combos du n>=8 | '
                          f'overall Exp={overall_exp:+.2f})')

                for c in top3:
                    nl = _n_label(c['n'])
                    if nl is None: continue
                    edge = 'STRONG EDGE' if c['strong'] else 'marginal'
                    ustag = ' &#x26A0;unstable' if c['unstable'] else ''
                    b2.append(
                        f'  {_wi(c["wr"], c["exp"])} {c["label"]}{nl}{ustag}: '
                        f'WR={c["wr"]:.0f}% Exp={c["exp"]:+.2f} ({c["n"]}L)'
                        f' [{edge}]'
                    )

                if worst1:
                    nl = _n_label(worst1['n'])
                    if nl is not None:
                        b2.append(
                            f'  &#x274C; AVOID: {worst1["label"]}: '
                            f'Exp={worst1["exp"]:+.2f} ({worst1["n"]}L)'
                        )

                # Insight từ combo
                if top3 and top3[0]['strong']:
                    b2.append(
                        f'{NL}&#x1F4A1; Best combo "{top3[0]["label"]}" '
                        f'Exp={top3[0]["exp"]:+.2f} vs overall {overall_exp:+.2f}'
                        f' (+{top3[0]["exp"]-overall_exp:.2f}) — dang xet cho Context Filter'
                    )
            else:
                b2.append('  (Khong du n>=8 cho bat ky combo nao — can them data)')

        if len(b2) > 2:
            messages.append(NL.join(b2))

        # ══════════════════════════════════════════════════════════════════
        # BLOCK 3 — MFE/MAE + POST-ENTRY PATH
        # ══════════════════════════════════════════════════════════════════
        if has('mfe') and has('mae'):
            b3 = ['&#x1F6E4; <b>BLOCK 3 — MFE/MAE + POST-ENTRY PATH</b>', '─'*26]

            # 3A. MFE distribution by outcome
            b3.append('<b>3A. MFE distribution theo outcome:</b>')
            for reason, label in [('tp', 'TP/Trail'), ('sl', 'SL'), ('expired', 'HK')]:
                d = (buy_df[buy_df['reason'].isin(['tp', 'trail'])]
                     if reason == 'tp' else buy_df[buy_df['reason'] == reason])
                if len(d) < 3: continue
                mfe_avg = d['mfe'].mean()
                mfe_p25 = d['mfe'].quantile(0.25)
                mfe_p75 = d['mfe'].quantile(0.75)
                mfe_p90 = d['mfe'].quantile(0.90)
                mae_avg = d['mae'].mean()
                b3.append(
                    f'  <b>{label}</b> ({len(d)}L): '
                    f'avg=+{mfe_avg:.1f}%{NL}'
                    f'    P25=+{mfe_p25:.1f}% | P75=+{mfe_p75:.1f}% | P90=+{mfe_p90:.1f}%{NL}'
                    f'    MAE avg={mae_avg:.1f}%'
                )

            # 3B. MAE lenh thang — SL co qua chat?
            win_d = buy_df[buy_df['pnl'] > 0]
            if len(win_d) >= 5:
                mae_avg = win_d['mae'].mean()
                b3.append(f'{NL}<b>3B. MAE tren lenh THANG ({len(win_d)}L):</b>')
                b3.append(f'  Avg MAE = {mae_avg:.1f}% truoc khi phuc hoi')
                if mae_avg > -2:
                    b3.append('  &#x2705; It bi keo xuong — SL hien tai hop ly')
                elif mae_avg > -4:
                    b3.append(f'  &#x1F7E1; Bi keo {abs(mae_avg):.1f}%'
                              f' — SL co the hoi chat, thu noi rong 0.5-1%')
                else:
                    b3.append(f'  &#x26A0; Bi keo sau ({abs(mae_avg):.1f}%)'
                              f' — SL nen noi rong de lenh "tho" hon')

            # 3C. False breakout — dynamic threshold = SL * 0.3
            if len(sl_df) >= 5 and has('mfe'):
                fb_thresh = cfg_sl * 0.3 * 100   # e.g. 7% * 0.3 = 2.1%
                fb        = sl_df[sl_df['mfe'] < fb_thresh]
                reversal  = sl_df[sl_df['mfe'] >= cfg_sl * 1.0 * 100]
                mid_sl    = sl_df[(sl_df['mfe'] >= fb_thresh) &
                                   (sl_df['mfe'] < cfg_sl * 1.0 * 100)]
                b3.append(f'{NL}<b>3C. Phan loai lenh SL ({len(sl_df)}L)'
                          f' | false_breakout thresh={fb_thresh:.1f}%:</b>')
                b3.append(
                    f'  &#x274C; False breakout (MFE<{fb_thresh:.1f}%): '
                    f'{len(fb)}L = {_pct(len(fb),len(sl_df))}')
                b3.append(
                    f'  &#x1F7E1; Len it roi SL: '
                    f'{len(mid_sl)}L = {_pct(len(mid_sl),len(sl_df))}')
                b3.append(
                    f'  &#x26A0; Dao chieu sau khi len (MFE>={cfg_sl*100:.0f}%): '
                    f'{len(reversal)}L = {_pct(len(reversal),len(sl_df))}')

                # SL speed
                fast_sl = sl_df[sl_df['days'] <= 3]
                slow_sl = sl_df[sl_df['days'] > 3]
                b3.append(f'{NL}  SL nhanh (<=3p): {len(fast_sl)}L'
                          f' = {_pct(len(fast_sl),len(sl_df))}'
                          f' MFE avg=+{fast_sl["mfe"].mean():.1f}%'
                          if len(fast_sl) > 0 else '')
                b3.append(f'  SL cham (>3p): {len(slow_sl)}L'
                          f' = {_pct(len(slow_sl),len(sl_df))}'
                          f' MFE avg=+{slow_sl["mfe"].mean():.1f}%'
                          if len(slow_sl) > 0 else '')

            # 3D-bis. VNI during hold — normalize by hold_days
            if 'vni_during_hold' in buy_df.columns and 'days' in buy_df.columns \
                    and len(sl_df) >= 5:
                sl_df2 = sl_df.copy()
                sl_df2['_vni_daily'] = sl_df2.apply(
                    lambda r: r['vni_during_hold'] / r['days']
                              if r['days'] > 0 else 0, axis=1)
                threshold_daily = -0.15   # -0.15%/ngay
                sl_market = sl_df2[sl_df2['_vni_daily'] <= threshold_daily]
                sl_signal = sl_df2[sl_df2['_vni_daily'] > threshold_daily]
                b3.append(f'{NL}<b>3D. SL do MARKET hay SIGNAL? ({len(sl_df)}L)</b>')
                b3.append(f'  (Nguong: VNI daily rate <= {threshold_daily}%/phien)')
                if len(sl_market) > 0:
                    b3.append(
                        f'  &#x1F4C9; Market risk: {len(sl_market)}L'
                        f' = {_pct(len(sl_market),len(sl_df))}'
                        f' (VNI rate avg={sl_market["_vni_daily"].mean():.2f}%/p)')
                if len(sl_signal) > 0:
                    b3.append(
                        f'  &#x26A0; Signal/stock risk: {len(sl_signal)}L'
                        f' = {_pct(len(sl_signal),len(sl_df))}'
                        f' (VNI rate avg={sl_signal["_vni_daily"].mean():.2f}%/p)')
                # Verdict
                pct_market = len(sl_market) / len(sl_df)
                if pct_market >= 0.5:
                    b3.append('  &#x1F4A1; Phan lon market-driven'
                              ' → regime filter se giam SL ro rang')
                else:
                    b3.append('  &#x1F4A1; Phan lon signal risk'
                              ' → cai thien entry filter hieu qua hon trailing')

            # 3E. Consecutive loss analysis
            buy_trades = buy_df.sort_values('date').reset_index(drop=True)
            if len(buy_trades) >= 10:
                b3.append(f'{NL}<b>3E. Consecutive Loss Pattern:</b>')
                # Build streaks
                after_streak = {1: [], 2: [], 3: []}
                streak = 0
                for idx, row in buy_trades.iterrows():
                    if row['pnl'] <= 0:
                        streak += 1
                    else:
                        if streak >= 1 and idx + 1 < len(buy_trades):
                            for s in [1, 2, 3]:
                                if streak >= s:
                                    after_streak[s].append(
                                        buy_trades.iloc[idx + 1]['pnl']
                                        if idx + 1 < len(buy_trades) else None)
                        streak = 0

                for s in [1, 2, 3]:
                    nexts = [x for x in after_streak[s] if x is not None]
                    if len(nexts) < 2: continue
                    wr_after = sum(1 for x in nexts if x > 0) / len(nexts) * 100
                    wi = _wi(wr_after)
                    rule = ''
                    if s >= 2 and wr_after < 50:
                        rule = ' → xem xet giam size'
                    b3.append(f'  {wi} Sau {s} SL lien tiep:'
                              f' WR={wr_after:.0f}% ({len(nexts)} truong hop){rule}')

                # Max streak
                max_streak = 0
                cur = 0
                for _, row in buy_trades.iterrows():
                    cur = cur + 1 if row['pnl'] <= 0 else 0
                    max_streak = max(max_streak, cur)
                b3.append(f'  Max consecutive SL: {max_streak}L')

            # 3F. HK expired
            if len(hk_df) >= 5:
                hk_pos = hk_df[hk_df['pnl'] > 0]
                hk_neg = hk_df[hk_df['pnl'] <= 0]
                b3.append(f'{NL}<b>3F. Lenh het hold HK ({len(hk_df)}L):</b>')
                b3.append(f'  MFE avg=+{hk_df["mfe"].mean():.1f}%'
                          f' — dinh cao nhat dat duoc trong hold')
                if len(hk_pos) > 0:
                    b3.append(
                        f'  &#x1F7E2; Duong: {len(hk_pos)}L'
                        f' avg=+{hk_pos["pnl"].mean():.1f}%'
                        + (' → hold lau hon co the giup'
                           if hk_pos['pnl'].mean() > 1.5 else ''))
                if len(hk_neg) > 0:
                    b3.append(
                        f'  &#x274C; Am: {len(hk_neg)}L'
                        f' avg={hk_neg["pnl"].mean():.1f}%'
                        + (' → lenh yeu tu dau'
                           if abs(hk_neg['pnl'].mean()) > 1.5 else ''))

            # 3G. Conclusion
            b3.append(f'{NL}<b>3G. Ket luan post-entry:</b>')
            if len(sl_df) >= 5 and has('mfe'):
                fb_thresh = cfg_sl * 0.3 * 100
                pct_fb  = len(sl_df[sl_df['mfe'] < fb_thresh]) / len(sl_df)
                pct_rev = len(sl_df[sl_df['mfe'] >= cfg_sl * 100]) / len(sl_df)
                mae_w   = win_d['mae'].mean() if len(win_d) >= 3 else 0
                pct_mkt = (len(sl_df[sl_df['vni_during_hold'] /
                               sl_df['days'].clip(lower=1) <= -0.15])
                           / len(sl_df)
                           if 'vni_during_hold' in sl_df.columns else 0)
                if pct_fb >= 0.55:
                    b3.append('  &#x274C; Chu yeu false breakout'
                              ' → fix o entry (score/filter), trailing it giup')
                elif pct_mkt >= 0.5:
                    b3.append('  &#x1F4C9; Chu yeu market-driven'
                              ' → regime filter / giam size khi VNI yeu')
                elif pct_rev >= 0.3 and abs(mae_w) < 3:
                    b3.append('  &#x26A0; Nhieu lenh len cao roi dao chieu'
                              ' → trailing/BE stop co the cap duoc')
                elif abs(mae_w) > 4:
                    b3.append('  &#x26A0; Lenh thang bi keo sau'
                              ' → noi rong SL de "tho" hon')
                else:
                    b3.append('  &#x1F7E1; Mix — xem ky Block 2 entry quality')


            # ── 3H. TIME-TO-TP / FAST vs SLOW pattern ────────────────────
            # Câu hỏi: lệnh TP trong bao nhiêu ngày? Lệnh lình xình bao lâu?
            if has('mfe') and 'days' in buy_df.columns and buy_df['days'].notna().sum() >= 10:
                b3.append(f'{NL}<b>3H. Time-to-Exit — Fast TP vs Slow Drift:</b>')

                tp_df_t  = buy_df[buy_df['reason'].isin(['tp', 'trail'])]
                sl_df_t  = buy_df[buy_df['reason'] == 'sl']
                hk_df_t  = buy_df[buy_df['reason'] == 'expired']

                # Days distribution per outcome
                for grp_df, lbl in [(tp_df_t,'TP/Trail'), (sl_df_t,'SL'), (hk_df_t,'HK expired')]:
                    if len(grp_df) < 3: continue
                    d_med = grp_df['days'].median()
                    d_avg = grp_df['days'].mean()
                    d_p25 = grp_df['days'].quantile(0.25)
                    d_p75 = grp_df['days'].quantile(0.75)
                    b3.append(
                        f'  <b>{lbl}</b> ({len(grp_df)}L): '
                        f'avg={d_avg:.1f}p | median={d_med:.0f}p '
                        f'| P25={d_p25:.0f}p–P75={d_p75:.0f}p'
                    )

                # Split TP theo fast / slow — ngưỡng = 40% hold_days
                if len(tp_df_t) >= 6:
                    fast_thresh = max(3, round(cfg_sl * 0 + buy_df['days'].median() * 0.4))
                    # Dùng median hold của toàn bộ làm ngưỡng
                    median_hold = buy_df['days'].median()
                    fast_thresh = max(3, round(median_hold * 0.5))

                    tp_fast = tp_df_t[tp_df_t['days'] <= fast_thresh]
                    tp_slow = tp_df_t[tp_df_t['days'] > fast_thresh]

                    b3.append(f'{NL}  <b>Split TP theo tốc độ (ngưỡng={fast_thresh}p):</b>')

                    if len(tp_fast) >= 3:
                        mfe_f = tp_fast['mfe'].mean()
                        pnl_f = tp_fast['pnl'].mean()
                        b3.append(
                            f'  🚀 Fast TP (≤{fast_thresh}p): {len(tp_fast)}L = '
                            f'{len(tp_fast)/len(tp_df_t)*100:.0f}% của TP' + NL
                            + f'     Avg exit={pnl_f:+.1f}% | MFE avg=+{mfe_f:.1f}%'
                        )

                    if len(tp_slow) >= 3:
                        mfe_s = tp_slow['mfe'].mean()
                        pnl_s = tp_slow['pnl'].mean()
                        b3.append(
                            f'  🐢 Slow TP (>{fast_thresh}p): {len(tp_slow)}L = '
                            f'{len(tp_slow)/len(tp_df_t)*100:.0f}% của TP' + NL
                            + f'     Avg exit={pnl_s:+.1f}% | MFE avg=+{mfe_s:.1f}%'
                        )

                # Drift detection: lệnh HK có MFE thấp = lình xình thật
                if len(hk_df_t) >= 5 and 'mfe' in hk_df_t.columns:
                    hk_low_mfe  = hk_df_t[hk_df_t['mfe'] < 3.0]   # ≤3% peak = lình xình
                    hk_high_mfe = hk_df_t[hk_df_t['mfe'] >= 3.0]  # từng lên nhưng rơi
                    b3.append(f'{NL}  <b>Phân loại lệnh HK expired ({len(hk_df_t)}L):</b>')
                    if len(hk_low_mfe) > 0:
                        b3.append(
                            f'  ↔ Lình xình (MFE<3%): {len(hk_low_mfe)}L = '
                            f'{len(hk_low_mfe)/len(hk_df_t)*100:.0f}%'
                            + f' | Avg PnL={hk_low_mfe["pnl"].mean():+.1f}%'
                        )
                    if len(hk_high_mfe) > 0:
                        b3.append(
                            f'  📈→📉 Từng lên rồi rơi (MFE≥3%): {len(hk_high_mfe)}L = '
                            f'{len(hk_high_mfe)/len(hk_df_t)*100:.0f}%'
                            + f' | MFE avg=+{hk_high_mfe["mfe"].mean():.1f}%'
                        )

                # Key insight: so sánh WR fast vs WR slow entry (dùng days để detect)
                if len(tp_df_t) >= 6 and len(sl_df_t) >= 5:
                    # Fast resolution = TP hoặc SL trong ≤ fast_thresh ngày
                    fast_res = buy_df[buy_df['days'] <= fast_thresh]
                    slow_res = buy_df[buy_df['days'] > fast_thresh]
                    if len(fast_res) >= 5 and len(slow_res) >= 5:
                        wr_fast = _wr(fast_res); exp_fast = _exp(fast_res)
                        wr_slow = _wr(slow_res); exp_slow = _exp(slow_res)
                        b3.append(f'{NL}  <b>Edge: Fast resolution vs Slow:</b>')
                        b3.append(
                            f'  {_wi(wr_fast, exp_fast)} Fast (≤{fast_thresh}p): '
                            f'WR={wr_fast:.0f}% Exp={exp_fast:+.3f}% ({len(fast_res)}L)'
                        )
                        b3.append(
                            f'  {_wi(wr_slow, exp_slow)} Slow (>{fast_thresh}p): '
                            f'WR={wr_slow:.0f}% Exp={exp_slow:+.3f}% ({len(slow_res)}L)'
                        )
                        d_exp_ts = exp_fast - exp_slow
                        if d_exp_ts >= 0.3:
                            b3.append(
                                f'  💡 Fast resolution Exp cao hơn {d_exp_ts:+.3f}% '
                                f'→ Lệnh resolve nhanh = tốt hơn'
                            )
                            b3.append(
                                f'  → Xét thêm rule: nếu sau {fast_thresh}p chưa '
                                f'+{(len(tp_fast) > 0 and tp_fast["pnl"].mean()*0.3 or 2):.0f}% '
                                f'→ exit sớm'
                            )
                        elif d_exp_ts < -0.2:
                            b3.append(
                                f'  💡 Slow resolution lại tốt hơn '
                                f'→ Mã này cần kiên nhẫn, không nên exit sớm'
                            )
                        else:
                            b3.append(
                                f'  → Không có pattern rõ giữa fast/slow — hold theo kế hoạch'
                            )

            if len(b3) > 2:
                messages.append(NL.join(b3))

        # ══════════════════════════════════════════════════════════════════
        # BLOCK 4 — EXPECTANCY + SIZING + DRAWDOWN
        # ══════════════════════════════════════════════════════════════════
        b4 = ['&#x1F4B0; <b>BLOCK 4 — EXPECTANCY + SIZING + DRAWDOWN</b>', '─'*26]

        # 4A. Expectancy by score bucket
        b4.append('<b>4A. Expectancy theo score bucket:</b>')
        for lo, hi, label in [(65,74,'65-74'),(75,84,'75-84'),(85,94,'85-94'),(95,100,'95+')]:
            d = buy_df[(buy_df['score'] >= lo) & (buy_df['score'] <= hi)]
            if len(d) < 5: continue
            wr_c = _wr(d); ex_c = _exp(d)
            b4.append(f'  {_wi(wr_c, ex_c)} Score {label}:'
                      f' WR={wr_c:.0f}% Exp={ex_c:+.3f}'
                      f' → {_sizing(ex_c, wr_c)} ({len(d)}L)')

        # 4B. Expectancy by VNI regime
        if '_vtrend' in buy_df.columns:
            b4.append(f'{NL}<b>4B. Expectancy theo VNI regime:</b>')
            for ctx in ['UP(>2%)', 'FLAT', 'DOWN(<-2%)']:
                d = buy_df[buy_df['_vtrend'] == ctx]
                if len(d) < 3: continue
                ex_c = _exp(d); wr_c = _wr(d)
                b4.append(f'  {_wi(wr_c, ex_c)} {ctx}:'
                          f' Exp={ex_c:+.3f} → {_sizing(ex_c, wr_c)}')

        # 4C. Expectancy by MA20 dist
        if '_ma20_ctx' in buy_df.columns:
            b4.append(f'{NL}<b>4C. Expectancy theo MA20 dist:</b>')
            for ctx in ['BELOW_MA20', 'NEAR(0-2%)', 'OK(2-5%)', 'EXTENDED(>5%)']:
                d = buy_df[buy_df['_ma20_ctx'] == ctx]
                if len(d) < 3: continue
                ex_c = _exp(d); wr_c = _wr(d)
                b4.append(f'  {_wi(wr_c, ex_c)} {ctx}:'
                          f' Exp={ex_c:+.3f} → {_sizing(ex_c, wr_c)}')

        # 4D. Overall — realized vs planned RR
        overall_wr  = _wr(buy_df)
        wins_df     = buy_df[buy_df['pnl'] > 0]
        losses_df   = buy_df[buy_df['pnl'] <= 0]
        avg_win     = wins_df['pnl'].mean()   if len(wins_df)   > 0 else 0
        avg_loss    = abs(losses_df['pnl'].mean()) if len(losses_df) > 0 else 0
        realized_rr = avg_win / avg_loss if avg_loss > 0 else 0
        planned_rr  = (buy_df.get('tp') or 0)   # fallback
        # use cfg values
        _planned_rr_val = 0
        try:
            from config import SYMBOL_CONFIG
        except Exception:
            SYMBOL_CONFIG = {}

        b4.append(f'{NL}<b>4D. Tong ket system:</b>')
        b4.append(f'  Expectancy: <b>{overall_exp:+.3f}%/lenh</b>')
        b4.append(f'  WR={overall_wr:.1f}%'
                  f' | AvgWin=+{avg_win:.2f}% | AvgLoss=-{avg_loss:.2f}%')
        b4.append(f'  Realized R:R = {realized_rr:.2f}:1'
                  f' | Planned R:R = TP/SL = {cfg_sl*100:.0f}%/{cfg_sl*100:.0f}%')
        # Explain gap
        n_hk = len(hk_df)
        if n_hk > 0 and realized_rr < 1.8:
            b4.append(f'  &#x26A0; Realized R:R thap — {n_hk}L expired exit'
                      f' truoc TP ({_pct(n_hk, n)})')
            b4.append(f'  → Goi y: tang hold_days co the cai thien R:R')

        if overall_exp > 0.5:
            b4.append('  &#x2705; He thong co EDGE RO RANG — trade theo plan')
        elif overall_exp > 0.1:
            b4.append('  &#x1F7E1; Edge tich cuc nho — can strict filter')
        elif overall_exp > 0:
            b4.append('  &#x26A0; Edge rat mong — review system')
        else:
            b4.append('  &#x274C; NEGATIVE expectancy — review toan bo')

        # 4E. Top setups — from combo discovery
        if 'all_combos' in dir() and all_combos:
            b4.append(f'{NL}<b>4E. Top setups (tu combo discovery):</b>')
            shown = 0
            for c in all_combos:
                if c['n'] < 8: continue
                nl = _n_label(c['n'])
                if nl is None: continue
                b4.append(
                    f'  {_wi(c["wr"], c["exp"])} {c["label"]}{nl}:'
                    f' Exp={c["exp"]:+.3f} → {_sizing(c["exp"], c["wr"])}')
                shown += 1
                if shown >= 3: break
            if all_combos:
                worst = all_combos[-1]
                if worst['n'] >= 8:
                    b4.append(
                        f'  &#x274C; AVOID: {worst["label"]}:'
                        f' Exp={worst["exp"]:+.3f}')

        # 4F. Drawdown analysis
        buy_sorted = buy_df.sort_values('date').reset_index(drop=True)
        if len(buy_sorted) >= 10:
            b4.append(f'{NL}<b>4F. Drawdown Analysis:</b>')

            # Max consecutive SL
            max_sl_streak = 0
            cur_sl = 0
            max_sl_start = ''
            cur_start = ''
            for _, row in buy_sorted.iterrows():
                if row['pnl'] <= 0:
                    if cur_sl == 0: cur_start = row.get('date', '')
                    cur_sl += 1
                    if cur_sl > max_sl_streak:
                        max_sl_streak = cur_sl
                        max_sl_start  = cur_start
                else:
                    cur_sl = 0

            # Cumulative PnL drawdown
            cum_pnl = buy_sorted['pnl'].cumsum()
            roll_max = cum_pnl.cummax()
            drawdown = cum_pnl - roll_max
            max_dd   = drawdown.min()
            max_dd_idx = drawdown.idxmin()
            peak_idx   = roll_max[:max_dd_idx].idxmax() if max_dd_idx > 0 else 0

            # Recovery
            recovery_date = ''
            for idx in range(max_dd_idx, len(cum_pnl)):
                if cum_pnl.iloc[idx] >= roll_max.iloc[max_dd_idx]:
                    recovery_date = buy_sorted.iloc[idx].get('date', '')
                    break

            b4.append(f'  Max SL streak: {max_sl_streak}L'
                      + (f' (bat dau ~{max_sl_start[:7]})' if max_sl_start else ''))
            b4.append(f'  Max cumulative drawdown: {max_dd:.1f}%')
            if recovery_date:
                b4.append(f'  Recovery: ~{recovery_date[:7]}')

            # Yearly worst/best
            if 'date' in buy_sorted.columns:
                try:
                    import pandas as pd
                    buy_sorted['_yr'] = pd.to_datetime(
                        buy_sorted['date'], errors='coerce').dt.year
                    yr_exp = buy_sorted.groupby('_yr').apply(_exp)
                    if len(yr_exp) > 0:
                        worst_yr = yr_exp.idxmin()
                        best_yr  = yr_exp.idxmax()
                        b4.append(
                            f'  Nam tot nhat: {best_yr}'
                            f' Exp={yr_exp[best_yr]:+.2f}')
                        b4.append(
                            f'  Nam te nhat: {worst_yr}'
                            f' Exp={yr_exp[worst_yr]:+.2f}')
                except Exception:
                    pass

            # Sizing rule from drawdown
            if max_sl_streak >= 2:
                b4.append(
                    f'{NL}  &#x1F4A1; Sizing rule:'
                    f' Sau {max_sl_streak}L SL lien tiep'
                    f' → giam 50% size cho den khi co lenh thang')

        if len(b4) > 2:
            messages.append(NL.join(b4))

        return messages

    except Exception as e:
        import traceback
        return [f'&#x26A0; Analytics error: {str(e)[:150]}']


def _handle_bt_symbol(symbol, chat_id, full_mode=False, custom_score=None, custom_sl=None, custom_tp=None, custom_hold=None, trailing_stop=False):
    """Chạy BT+WF cho 1 mã, gửi output compact."""
    custom_parts = []
    if custom_score is not None: custom_parts.append(f's={custom_score}')
    if custom_sl    is not None: custom_parts.append(f'sl={int(custom_sl*100)}%')
    if custom_tp    is not None: custom_parts.append(f'tp={int(custom_tp*100)}%')
    if custom_hold  is not None: custom_parts.append(f'hold={custom_hold}p')
    if trailing_stop:            custom_parts.append('TS=1R/3R')
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
            send('⏳ Đang tải data <b>' + symbol + '</b> từ VCI...', chat_id)
            _df_shared, _ = _load_data_retry(symbol, chat_id)

            res       = bt.run_backtest_symbol(symbol, verbose=False, use_regime=False,
                            sl=custom_sl, tp=custom_tp,
                            hold_days=custom_hold, min_score=custom_score,
                            trailing_stop=trailing_stop,
                            _df_cache=_df_shared)  # baseline
            res_regime= bt.run_backtest_symbol(symbol, verbose=False, use_regime=None,
                            sl=custom_sl, tp=custom_tp,
                            hold_days=custom_hold, min_score=custom_score,
                            trailing_stop=trailing_stop,
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
            tr_   = buy.get('trail', 0)   # trailing stop exits
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
                '&#x1F4CA; <b>BACKTEST ' + symbol + ' (7 NAM)'
                + (' &#x1F503; TRAIL 1R/3R' if trailing_stop else '') + '</b>\n'
                + '&#x3D;' * 28 + '\n\n'

                + '<b>Tổng quan:</b>\n'
                + (f' Lenh: {n} | TP: {tp_} | Trail: {tr_} | SL: {sl_} | HK: {hk_}\n'
                   if trailing_stop else
                   f' Lenh: {n} | TP: {tp_} | SL: {sl_} | HK: {hk_}\n')
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
            send(msg_bt, chat_id)

            # ── TRADE ANALYTICS ──────────────────────────────────────────
            trades_df2 = res.get('trades')
            if trades_df2 is not None and len(trades_df2) >= 10:
                analytics_blocks = _fmt_trade_analytics(trades_df2, cfg_sl, cfg_score)
                for _blk in analytics_blocks:
                    if _blk:
                        send(_blk, chat_id)

            # ── WALK-FORWARD ─────────────────────────────────────────────────
            send('&#x1F504; Đang chạy <b>Walk-Forward</b> ' + symbol + '...', chat_id)
            wf = bt.run_walk_forward(symbol, verbose=False, _df_cache=_df_shared,
                                     sl=custom_sl, tp=custom_tp,
                                     min_score=custom_score, hold_days=custom_hold,
                                     trailing_stop=trailing_stop)

            # FIX: Nếu WF fail do ít rows (<400), thử reload với days lớn hơn
            if not wf and _df_shared is not None and len(_df_shared) < 400:
                send('&#x23F3; Dữ liệu ít (' + str(len(_df_shared)) + ' rows), đang tải thêm...', chat_id)
                _df_extra, _ = bt.load_data(symbol, days=3650)  # thử 10 năm
                if _df_extra is not None and len(_df_extra) > len(_df_shared):
                    wf = bt.run_walk_forward(symbol, verbose=False, _df_cache=_df_extra,
                                             sl=custom_sl, tp=custom_tp,
                                             min_score=custom_score, hold_days=custom_hold,
                                             trailing_stop=trailing_stop)

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
            for i_sym, sym in enumerate(syms, 1):
                send(f'⏳ [{i_sym}/{len(syms)}] Đang chạy BT <b>{sym}</b>...', chat_id)
                try:
                    # Tải data trước với retry, truyền cache vào BT
                    _df_cache_all, _ = _load_data_retry(sym, chat_id, label=sym)
                    res = bt.run_backtest_symbol(sym, verbose=False, _df_cache=_df_cache_all)
                    wf  = bt.run_walk_forward(sym, verbose=False, _df_cache=_df_cache_all)
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


# ─── SCORE B BACKTEST ─────────────────────────────────────────────────────────

def _calc_score_b_from_row(row, sym):
    """
    Reconstruct Score B từ 1 dòng trong trades_df.
    Dùng các columns đã có: vni_slope, vol_ratio, score, ma20_dist.
    Giống calc_score_b() nhưng input từ historical trade row.
    """
    data = {
        'symbol':         sym,
        'vni_change_pct': float(row.get('vni_slope', 0) or 0),
        'vol_ratio':      float(row.get('vol_ratio', 1.0) or 1.0),
        'score':          float(row.get('score', 0) or 0),
        'score_a':        float(row.get('score', 0) or 0),
        'ma20':           0,   # không có trực tiếp — dùng ma20_dist
        'price':          0,
        # Truyền ma20_dist trực tiếp để calc_score_b dùng
        'dist_ma20_pct':  float(row.get('ma20_dist', 0) or 0),
    }
    scb, bd, badge = calc_score_b(data)
    return scb, bd


def _run_sbtbt(symbol, chat_id, custom_sl=None, custom_tp=None,
               custom_hold=None, custom_score=None):
    """
    Score B Backtest: chạy Score A BT → split trades theo ScB >= 60 vs < 60
    → so sánh WR/Exp/PF → Walk-Forward IS/OOS.
    """
    NL  = chr(10)
    _sl  = custom_sl    or 0.07
    _tp  = custom_tp    or 0.14
    _hold= custom_hold  or 10
    _msc = custom_score or get_min_score(symbol)

    send(f'⏳ Score B BT <b>{symbol}</b> — đang load data...', chat_id)

    try:
        import sys, os
        bot_dir = os.path.dirname(os.path.abspath(__file__))
        if bot_dir not in sys.path:
            sys.path.insert(0, bot_dir)
        import backtest as bt
        import numpy as np

        # ── Load data + chạy Score A BT để lấy trades_df ────────────────
        df, _ = _load_data_retry(symbol, chat_id)
        if df is None or len(df) < 400:
            send(f'❌ {symbol}: không đủ data (cần ≥400 nến)', chat_id)
            return

        send(f'⏳ Đang chạy Score A backtest để lấy trade log...', chat_id)
        res = bt.run_backtest_symbol(
            symbol, verbose=False, use_regime=False,
            sl=_sl, tp=_tp, hold_days=_hold, min_score=_msc,
            _df_cache=df
        )
        if not res or not res.get('buy'):
            send(f'❌ {symbol}: Score A không có lệnh MUA đủ ngưỡng', chat_id)
            return

        trades_df = res.get('trades')
        if trades_df is None or len(trades_df) < 20:
            send(f'❌ {symbol}: không đủ trade log (n={len(trades_df) if trades_df is not None else 0})', chat_id)
            return

        buy_df = trades_df[trades_df['action'] == 'MUA'].copy()
        if len(buy_df) < 15:
            send(f'❌ {symbol}: ít lệnh quá (n={len(buy_df)} < 15)', chat_id)
            return

        # ── Tính Score B cho mỗi lệnh ────────────────────────────────────
        send(f'⏳ Đang tính Score B cho {len(buy_df)} lệnh...', chat_id)

        has_vni  = 'vni_slope'  in buy_df.columns and buy_df['vni_slope'].notna().sum() > 5
        has_vol  = 'vol_ratio'  in buy_df.columns and buy_df['vol_ratio'].notna().sum() > 5
        has_ma20 = 'ma20_dist'  in buy_df.columns and buy_df['ma20_dist'].notna().sum() > 5

        missing = []
        if not has_vni:  missing.append('vni_slope')
        if not has_vol:  missing.append('vol_ratio')
        if not has_ma20: missing.append('ma20_dist')

        if missing:
            send(f'⚠ {symbol}: thiếu columns {missing} — Score B sẽ dùng proxy', chat_id)

        scb_vals = []
        for _, row in buy_df.iterrows():
            scb_val, _ = _calc_score_b_from_row(row, symbol)
            scb_vals.append(scb_val)

        buy_df = buy_df.copy()
        buy_df['score_b'] = scb_vals

        # ── Split: ScB >= 60 vs < 60 ─────────────────────────────────────
        hi_df = buy_df[buy_df['score_b'] >= 60]
        lo_df = buy_df[buy_df['score_b'] < 60]

        def _wr(df):
            if len(df) == 0: return 0.0
            return len(df[df['pnl'] > 0]) / len(df) * 100

        def _exp(df):
            if len(df) == 0: return 0.0
            wins   = df[df['pnl'] > 0]['pnl']
            losses = df[df['pnl'] <= 0]['pnl']
            wr_ = len(wins) / len(df)
            aw  = wins.mean()        if len(wins)   > 0 else 0.0
            al  = abs(losses.mean()) if len(losses) > 0 else 0.0
            return round(wr_ * aw - (1 - wr_) * al, 3)

        def _pf(df):
            wins   = df[df['pnl'] > 0]['pnl'].sum()
            losses = abs(df[df['pnl'] < 0]['pnl'].sum())
            return round(wins / losses, 2) if losses > 0 else float('inf')

        def _fmt_group(df, label):
            if len(df) == 0:
                return f'  {label}: 0 lệnh — không đủ data'
            wr_  = _wr(df)
            exp_ = _exp(df)
            pf_  = _pf(df)
            avg_ = df['pnl'].mean()
            wi   = '✅' if exp_ > 0.3 and wr_ >= 52 else ('🟡' if exp_ > 0 else '❌')
            pf_s = f'{pf_:.2f}' if pf_ != float('inf') else '∞'
            return (f'  {wi} {label} (n={len(df)}L)' + NL
                    + f'     WR={wr_:.1f}% | Exp={exp_:+.3f}% | PF={pf_s} | Avg={avg_:+.2f}%')

        # ── Overall comparison ────────────────────────────────────────────
        wr_all  = _wr(buy_df);  exp_all  = _exp(buy_df)
        wr_hi   = _wr(hi_df);   exp_hi   = _exp(hi_df)
        wr_lo   = _wr(lo_df);   exp_lo   = _exp(lo_df)
        d_wr    = wr_hi - wr_lo
        d_exp   = exp_hi - exp_lo

        # Edge verdict
        if d_exp >= 0.3 and d_wr >= 3.0 and len(hi_df) >= 15:
            edge_icon = '✅'
            edge_txt  = f'ScB có edge rõ ràng — Exp gap={d_exp:+.3f}% WR gap={d_wr:+.1f}%'
        elif d_exp >= 0.1 and len(hi_df) >= 10:
            edge_icon = '🟡'
            edge_txt  = f'ScB có edge nhẹ — cần thêm data (n_hi={len(hi_df)})'
        elif d_exp < 0:
            edge_icon = '❌'
            edge_txt  = f'ScB KHÔNG có edge (ScB>=60 tệ hơn!) — xem lại công thức'
        else:
            edge_icon = '⚠'
            edge_txt  = f'ScB edge không rõ — Exp gap={d_exp:+.3f}% quá nhỏ'

        msg_main = (
            f'📊 <b>Score B Backtest — {symbol}</b>' + NL
            + '=' * 28 + NL
            + f'Score A params: SL={_sl*100:.0f}% TP={_tp*100:.0f}% Hold={_hold}d Score≥{_msc}' + NL
            + f'Tổng lệnh MUA: <b>{len(buy_df)}L</b>' + NL + NL

            + '<b>Split theo Score B:</b>' + NL
            + _fmt_group(hi_df, 'ScB ≥ 60 (PASS)') + NL
            + _fmt_group(lo_df, 'ScB < 60 (FAIL)') + NL + NL

            + '<b>Baseline (tất cả lệnh Score A):</b>' + NL
            + f'  WR={wr_all:.1f}% | Exp={exp_all:+.3f}%' + NL + NL

            + f'<b>Edge gap (PASS vs FAIL):</b>' + NL
            + f'  ΔWR={d_wr:+.1f}% | ΔExp={d_exp:+.3f}%' + NL + NL

            + f'{edge_icon} <b>{edge_txt}</b>'
        )
        send(msg_main, chat_id)

        # ── Breakdown per ScB component ───────────────────────────────────
        send('⏳ Đang phân tích breakdown per component...', chat_id)

        bd_lines = [f'🔍 <b>Score B Component Breakdown — {symbol}</b>', '─'*26]

        # C1: VNI Slope
        if has_vni:
            buy_df['_vni_cat'] = buy_df['vni_slope'].apply(
                lambda v: 'UP(≥1%)' if v >= 1 else ('DOWN(<-2%)' if v < -2 else 'FLAT'))
            bd_lines.append(NL + '<b>C1 — VNI Regime:</b>')
            for cat in ['UP(≥1%)', 'FLAT', 'DOWN(<-2%)']:
                g = buy_df[buy_df['_vni_cat'] == cat]
                if len(g) < 3: continue
                bd_lines.append(
                    f'  {"✅" if _exp(g)>0.2 else ("🟡" if _exp(g)>0 else "❌")} '
                    f'{cat}: WR={_wr(g):.0f}% Exp={_exp(g):+.3f}% (n={len(g)}L)')

        # C2: Vol pattern
        if has_vol:
            buy_df['_vol_cat'] = buy_df['vol_ratio'].apply(
                lambda v: 'HIGH(≥2x)' if v >= 2 else ('MED(1.2-2x)' if v >= 1.2
                          else ('NORMAL(0.8-1.2x)' if v >= 0.8 else 'LOW(<0.8x)')))
            bd_lines.append(NL + '<b>C2 — Volume Pattern:</b>')
            for cat in ['HIGH(≥2x)', 'MED(1.2-2x)', 'NORMAL(0.8-1.2x)', 'LOW(<0.8x)']:
                g = buy_df[buy_df['_vol_cat'] == cat]
                if len(g) < 3: continue
                bd_lines.append(
                    f'  {"✅" if _exp(g)>0.2 else ("🟡" if _exp(g)>0 else "❌")} '
                    f'{cat}: WR={_wr(g):.0f}% Exp={_exp(g):+.3f}% (n={len(g)}L)')

        # C3: Score bucket
        buy_df['_sc_cat'] = buy_df['score'].apply(
            lambda s: '95+' if s >= 95 else ('85-94' if s >= 85
                      else ('75-84' if s >= 75 else ('65-74' if s >= 65 else '<65'))))
        bd_lines.append(NL + '<b>C3 — Score A Bucket:</b>')
        for cat in ['<65', '65-74', '75-84', '85-94', '95+']:
            g = buy_df[buy_df['_sc_cat'] == cat]
            if len(g) < 3: continue
            bd_lines.append(
                f'  {"✅" if _exp(g)>0.2 else ("🟡" if _exp(g)>0 else "❌")} '
                f'Score {cat}: WR={_wr(g):.0f}% Exp={_exp(g):+.3f}% (n={len(g)}L)')

        # C4: MA20 dist
        if has_ma20:
            buy_df['_ma20_cat'] = buy_df['ma20_dist'].apply(
                lambda d: 'BELOW(<0)' if d < 0 else ('NEAR(0-2%)' if d < 2
                          else ('OPTIMAL(2-10%)' if d < 10
                                else ('EXT(10-20%)' if d < 20 else 'FAR(>20%)'))))
            bd_lines.append(NL + '<b>C4 — MA20 Distance:</b>')
            for cat in ['BELOW(<0)', 'NEAR(0-2%)', 'OPTIMAL(2-10%)', 'EXT(10-20%)', 'FAR(>20%)']:
                g = buy_df[buy_df['_ma20_cat'] == cat]
                if len(g) < 3: continue
                bd_lines.append(
                    f'  {"✅" if _exp(g)>0.2 else ("🟡" if _exp(g)>0 else "❌")} '
                    f'MA20 {cat}: WR={_wr(g):.0f}% Exp={_exp(g):+.3f}% (n={len(g)}L)')

        send(NL.join(bd_lines), chat_id)

        # ── Walk-Forward: IS=756 / OOS=252 / 4 windows ───────────────────
        send('⏳ Đang chạy Walk-Forward (IS=756 / OOS=252 / 4 windows)...', chat_id)

        # Reset index để dùng integer indexing
        buy_sorted = buy_df.sort_values('date').reset_index(drop=True)
        n_total    = len(buy_sorted)

        IS_SIZE  = 756   # rows price data — nhưng trades ít hơn nhiều
        OOS_SIZE = 252   # tương tự

        # Adaptive: nếu ít lệnh thì chia theo % thay vì rows cứng
        # Dùng trade-based split: IS=60% OOS=40% với 4 windows
        wf_results = []

        # Tính số trades per window
        n_windows = 4
        window_size = n_total // n_windows

        if window_size < 5:
            send(f'⚠ {symbol}: quá ít lệnh cho WF ({n_total}L / {n_windows} windows = {window_size}L/window)', chat_id)
        else:
            for w in range(n_windows):
                # IS: tất cả lệnh trước window này
                is_end   = w * window_size
                oos_start= w * window_size
                oos_end  = min((w + 1) * window_size, n_total)

                if is_end < 5:
                    continue  # IS quá nhỏ

                is_df  = buy_sorted.iloc[:is_end]
                oos_df = buy_sorted.iloc[oos_start:oos_end]

                # IS: tìm ngưỡng ScB tối ưu (60 fixed theo methodology)
                # OOS: áp ngưỡng 60 lên OOS data
                oos_hi = oos_df[oos_df['score_b'] >= 60]
                oos_lo = oos_df[oos_df['score_b'] < 60]
                oos_all= oos_df

                is_exp_hi = _exp(is_df[is_df['score_b'] >= 60])
                is_exp_lo = _exp(is_df[is_df['score_b'] < 60])

                oos_exp_hi = _exp(oos_hi)
                oos_exp_lo = _exp(oos_lo)
                oos_wr_hi  = _wr(oos_hi)

                # Lấy date range của window
                try:
                    w_start = oos_df['date'].iloc[0]
                    w_end   = oos_df['date'].iloc[-1]
                    w_label = f'{w_start[:7]}–{w_end[:7]}'
                except Exception:
                    w_label = f'Window {w+1}'

                wf_results.append({
                    'w':          w + 1,
                    'label':      w_label,
                    'is_n':       len(is_df),
                    'is_exp_hi':  is_exp_hi,
                    'is_exp_lo':  is_exp_lo,
                    'oos_n':      len(oos_df),
                    'oos_n_hi':   len(oos_hi),
                    'oos_exp_hi': oos_exp_hi,
                    'oos_wr_hi':  oos_wr_hi,
                    'oos_exp_lo': oos_exp_lo,
                    'pass':       oos_exp_hi > oos_exp_lo and len(oos_hi) >= 3,
                })

            # Format WF output
            wf_lines = [f'📈 <b>Walk-Forward ScB≥60 — {symbol}</b>', '─'*26,
                        f'IS: {window_size}L each | OOS: {window_size}L each | {n_windows} windows']
            n_pass = 0
            for r in wf_results:
                ok   = '✅' if r['pass'] else '❌'
                if r['pass']: n_pass += 1
                wf_lines.append(
                    f'{ok} W{r["w"]} [{r["label"]}] IS={r["is_n"]}L OOS={r["oos_n"]}L' + NL
                    + f'   IS Exp(hi/lo)={r["is_exp_hi"]:+.3f}/{r["is_exp_lo"]:+.3f} | '
                    + f'OOS hi: WR={r["oos_wr_hi"]:.0f}% Exp={r["oos_exp_hi"]:+.3f}% (n={r["oos_n_hi"]}L)')

            # WF verdict
            pct_pass = n_pass / len(wf_results) * 100 if wf_results else 0
            if pct_pass >= 75:
                wf_v = f'✅ Robust ({n_pass}/{len(wf_results)} windows pass)'
            elif pct_pass >= 50:
                wf_v = f'🟡 Promising ({n_pass}/{len(wf_results)} windows pass)'
            else:
                wf_v = f'❌ Không ổn định ({n_pass}/{len(wf_results)} windows pass)'

            wf_lines += ['', f'<b>WF Verdict: {wf_v}</b>',
                         f'<i>Ngưỡng deploy: ≥3/4 windows pass + Exp(hi)>Exp(lo)</i>']
            send(NL.join(wf_lines), chat_id)

        # ── Final recommendation ──────────────────────────────────────────
        rec_lines = [f'💡 <b>Khuyến nghị — {symbol}</b>', '─'*26]

        if edge_icon == '✅' and (not wf_results or n_pass >= 3):
            rec_lines += [
                '✅ Score B có edge VÀ ổn định qua WF',
                f'→ Có thể promote ScB thành gate cứng trong /signals',
                f'→ Điều kiện: Score A ≥ {_msc} VÀ ScB ≥ 60',
                '',
                '⚠ Lưu ý: validate trên thêm mã trước khi deploy toàn bộ',
            ]
        elif edge_icon == '🟡':
            rec_lines += [
                '🟡 Score B có edge nhẹ — chưa đủ để gate cứng',
                '→ Tiếp tục dùng ở mức advisory (ScB hiển thị trong output)',
                f'→ Thu thập thêm live data, target n≥30L nhóm ScB≥60',
            ]
        elif edge_icon == '❌':
            rec_lines += [
                '❌ Score B KHÔNG có edge trên mã này',
                '→ Xem breakdown để identify component nào gây ra',
                '→ Có thể cần điều chỉnh weights hoặc ngưỡng cho mã này',
            ]
        else:
            rec_lines += [
                '⚠ Kết quả không kết luận được — cần thêm data',
                f'→ n_hi={len(hi_df)}L (cần ≥15L để tin cậy)',
            ]

        rec_lines += ['', f'<i>Methodology: Entry T+1 | Phí 0.15%/chiều | WF {n_windows} windows</i>']
        send(NL.join(rec_lines), chat_id)

    except Exception as e:
        import traceback
        logger.error(f'_run_sbtbt {symbol}: {traceback.format_exc()}')
        send(f'❌ Lỗi /sbtbt {symbol}: {str(e)[:150]}', chat_id)


def _run_sbtbt_batch(syms, chat_id, custom_sl=None, custom_tp=None,
                     custom_hold=None, custom_score=None):
    """
    Batch mode: chạy Score B BT cho nhiều mã, gom kết quả thành 1 bảng tổng kết.
    Không spam từng mã — chỉ gửi progress ping + 1 summary cuối.
    """
    NL = chr(10)
    total = len(syms)
    send(
        f'📊 <b>Score B BT Batch — {total} mã</b>' + NL
        + f'Đang chạy tuần tự, ~{total*2}-{total*3} phút...' + NL
        + '<i>Sẽ gửi bảng tổng kết khi xong tất cả.</i>',
        chat_id
    )

    rows = []   # list of result dicts per symbol

    for idx, symbol in enumerate(syms, 1):
        send(f'⏳ [{idx}/{total}] Đang chạy <b>{symbol}</b>...', chat_id)
        try:
            import backtest as bt
            import numpy as np

            _sl  = custom_sl    or 0.07
            _tp  = custom_tp    or 0.14
            _hold= custom_hold  or 10
            _msc = custom_score or get_min_score(symbol)

            df, _ = _load_data_retry(symbol, chat_id, label=symbol)
            if df is None or len(df) < 400:
                rows.append({'sym': symbol, 'status': 'no_data', 'err': 'data<400'})
                continue

            res = bt.run_backtest_symbol(
                symbol, verbose=False, use_regime=False,
                sl=_sl, tp=_tp, hold_days=_hold, min_score=_msc,
                _df_cache=df
            )
            if not res or not res.get('buy'):
                rows.append({'sym': symbol, 'status': 'no_signal', 'err': 'no MUA'})
                continue

            trades_df = res.get('trades')
            if trades_df is None or len(trades_df) < 10:
                rows.append({'sym': symbol, 'status': 'few_trades',
                             'err': f'n={len(trades_df) if trades_df is not None else 0}'})
                continue

            buy_df = trades_df[trades_df['action'] == 'MUA'].copy()
            if len(buy_df) < 10:
                rows.append({'sym': symbol, 'status': 'few_trades',
                             'err': f'n_buy={len(buy_df)}'})
                continue

            # Tính Score B cho mỗi lệnh
            scb_vals = []
            for _, row in buy_df.iterrows():
                sv, _ = _calc_score_b_from_row(row, symbol)
                scb_vals.append(sv)
            buy_df = buy_df.copy()
            buy_df['score_b'] = scb_vals

            hi_df = buy_df[buy_df['score_b'] >= 60]
            lo_df = buy_df[buy_df['score_b'] < 60]

            def _wr(df):
                return len(df[df['pnl'] > 0]) / len(df) * 100 if len(df) > 0 else 0.0
            def _exp(df):
                if len(df) == 0: return 0.0
                wins = df[df['pnl'] > 0]['pnl']
                loss = df[df['pnl'] <= 0]['pnl']
                wr_  = len(wins) / len(df)
                aw   = wins.mean() if len(wins) > 0 else 0.0
                al   = abs(loss.mean()) if len(loss) > 0 else 0.0
                return round(wr_ * aw - (1 - wr_) * al, 3)
            def _pf(df):
                wins = df[df['pnl'] > 0]['pnl'].sum()
                loss = abs(df[df['pnl'] < 0]['pnl'].sum())
                return round(wins / loss, 2) if loss > 0 else float('inf')

            wr_hi  = _wr(hi_df);   exp_hi = _exp(hi_df)
            wr_lo  = _wr(lo_df);   exp_lo = _exp(lo_df)
            wr_all = _wr(buy_df);  exp_all= _exp(buy_df)
            pf_hi  = _pf(hi_df)
            d_exp  = exp_hi - exp_lo
            d_wr   = wr_hi  - wr_lo

            # WF — trade-based split 4 windows
            buy_sorted = buy_df.sort_values('date').reset_index(drop=True)
            n_total_t  = len(buy_sorted)
            n_windows  = 4
            win_sz     = n_total_t // n_windows
            n_pass_wf  = 0
            n_wf_done  = 0

            if win_sz >= 4:
                for w in range(n_windows):
                    is_df  = buy_sorted.iloc[:w * win_sz]
                    oos_df = buy_sorted.iloc[w * win_sz: (w+1) * win_sz]
                    if len(is_df) < 4 or len(oos_df) < 4:
                        continue
                    oos_hi = oos_df[oos_df['score_b'] >= 60]
                    oos_lo = oos_df[oos_df['score_b'] < 60]
                    if _exp(oos_hi) > _exp(oos_lo) and len(oos_hi) >= 3:
                        n_pass_wf += 1
                    n_wf_done += 1

            pct_pass = n_pass_wf / n_wf_done * 100 if n_wf_done > 0 else 0

            # Edge verdict
            if d_exp >= 0.3 and d_wr >= 3.0 and len(hi_df) >= 15:
                edge = 'STRONG'
            elif d_exp >= 0.1 and len(hi_df) >= 8:
                edge = 'WEAK'
            elif d_exp < 0:
                edge = 'NEGATIVE'
            else:
                edge = 'UNCLEAR'

            # WF verdict
            if n_wf_done == 0:
                wf_vrd = 'n/a'
            elif pct_pass >= 75:
                wf_vrd = f'Robust({n_pass_wf}/{n_wf_done})'
            elif pct_pass >= 50:
                wf_vrd = f'Prom({n_pass_wf}/{n_wf_done})'
            else:
                wf_vrd = f'Fail({n_pass_wf}/{n_wf_done})'

            rows.append({
                'sym':      symbol,
                'status':   'ok',
                'n_all':    len(buy_df),
                'n_hi':     len(hi_df),
                'n_lo':     len(lo_df),
                'wr_hi':    round(wr_hi, 1),
                'wr_lo':    round(wr_lo, 1),
                'wr_all':   round(wr_all, 1),
                'exp_hi':   exp_hi,
                'exp_lo':   exp_lo,
                'exp_all':  exp_all,
                'pf_hi':    pf_hi,
                'd_exp':    d_exp,
                'd_wr':     d_wr,
                'edge':     edge,
                'wf_vrd':   wf_vrd,
                'n_pass_wf':n_pass_wf,
                'n_wf':     n_wf_done,
                'sl':       _sl, 'tp': _tp, 'hold': _hold, 'msc': _msc,
            })

        except Exception as e:
            import traceback
            logger.error(f'sbtbt_batch {symbol}: {traceback.format_exc()}')
            rows.append({'sym': symbol, 'status': 'error', 'err': str(e)[:60]})

    # ── Bảng tổng kết ────────────────────────────────────────────────────
    ok_rows  = [r for r in rows if r['status'] == 'ok']
    err_rows = [r for r in rows if r['status'] != 'ok']

    # Sort: STRONG Robust trên cùng
    def _sort_key(r):
        e_rank = {'STRONG': 0, 'WEAK': 1, 'UNCLEAR': 2, 'NEGATIVE': 3}.get(r['edge'], 4)
        w_rank = 0 if 'Robust' in r['wf_vrd'] else (1 if 'Prom' in r['wf_vrd'] else 2)
        return (e_rank, w_rank, -r.get('d_exp', 0))

    ok_rows.sort(key=_sort_key)

    # Header
    summary = [
        f'📊 <b>Score B BT Batch — {total} mã</b>',
        '─' * 32,
        f'{"Mã":<5} {"n":>4} {"ScB≥60":>6} | {"WR_hi":>6} {"WR_lo":>6} | {"Exp_hi":>7} {"Exp_lo":>7} | {"ΔExp":>6} | {"WF":>12} | Edge',
        '─' * 32,
    ]

    edge_icons = {'STRONG': '✅', 'WEAK': '🟡', 'UNCLEAR': '⚠', 'NEGATIVE': '❌'}

    for r in ok_rows:
        pf_s  = f'{r["pf_hi"]:.2f}' if r['pf_hi'] != float('inf') else '∞'
        ei    = edge_icons.get(r['edge'], '❓')
        d_exp_s = f'{r["d_exp"]:+.3f}'
        line = (
            f'{ei} <b>{r["sym"]:<5}</b>'
            f' {r["n_all"]:>4}L  hi={r["n_hi"]:>3}L'
            + NL
            + f'   WR: {r["wr_hi"]:.0f}%↑ vs {r["wr_lo"]:.0f}%↓ (all={r["wr_all"]:.0f}%)'
            + NL
            + f'   Exp: {r["exp_hi"]:+.3f}↑ vs {r["exp_lo"]:+.3f}↓  Δ={d_exp_s}  PF_hi={pf_s}'
            + NL
            + f'   WF: {r["wf_vrd"]}  Edge: {r["edge"]}'
        )
        summary.append(line)

    # Aggregate stats
    if ok_rows:
        n_strong  = sum(1 for r in ok_rows if r['edge'] == 'STRONG')
        n_weak    = sum(1 for r in ok_rows if r['edge'] == 'WEAK')
        n_neg     = sum(1 for r in ok_rows if r['edge'] == 'NEGATIVE')
        n_robust  = sum(1 for r in ok_rows if 'Robust' in r['wf_vrd'])
        avg_d_exp = sum(r['d_exp'] for r in ok_rows) / len(ok_rows)

        summary += [
            '',
            '─' * 32,
            f'<b>Tổng kết ({len(ok_rows)}/{total} mã OK):</b>',
            f'  ✅ STRONG edge: {n_strong} mã  |  🟡 WEAK: {n_weak} mã  |  ❌ NEG: {n_neg} mã',
            f'  WF Robust: {n_robust} mã  |  ΔExp TB: {avg_d_exp:+.3f}%',
            '',
        ]

        # Deploy recommendation
        if n_strong >= 7 and n_robust >= 5:
            summary += [
                '✅ <b>RECOMMEND PROMOTE:</b> Score B có edge nhất quán ≥7/10 mã',
                '→ Thêm ScB≥60 làm gate cứng trong /signals',
            ]
        elif n_strong + n_weak >= 6:
            summary += [
                '🟡 <b>ADVISORY ONLY:</b> Edge có nhưng chưa đủ nhất quán',
                '→ Giữ ScB là advisory, collect thêm live data',
            ]
        else:
            summary += [
                '❌ <b>KHÔNG DEPLOY:</b> Score B không có edge đủ rộng',
                '→ Review công thức — xem breakdown từng mã bằng /sbtbt SYM',
            ]

        summary.append(NL + '<i>Dùng /sbtbt SYM để xem breakdown chi tiết + component analysis.</i>')

    if err_rows:
        err_str = ', '.join(f'{r["sym"]}({r["err"]})' for r in err_rows[:10])
        summary.append(NL + f'⚠ Skip ({len(err_rows)} mã): {err_str}')

    # Gửi theo chunk nếu dài
    full_msg = NL.join(summary)
    if len(full_msg) <= 4000:
        send(full_msg, chat_id)
    else:
        # Gửi header + rows riêng
        header_msg = NL.join(summary[:4])
        send(header_msg, chat_id)
        chunk = ''
        for line in summary[4:]:
            if len(chunk) + len(line) + 1 > 3800:
                send(chunk, chat_id)
                chunk = line + NL
            else:
                chunk += line + NL
        if chunk.strip():
            send(chunk, chat_id)


def _run_sbtbt_batch_hose(chat_id, custom_sl=None, custom_tp=None,
                          custom_hold=None, custom_score=None):
    """
    Batch Score B BT cho toàn bộ mã HOSE (~144 mã từ SECTOR_MAP, dedup).
    Chạy flat (không grouping theo ngành) — progress ping [i/total],
    1 bảng tổng kết phẳng sort theo edge/ΔExp.
    """
    NL = chr(10)

    # Build flat list từ SECTOR_MAP, dedup
    seen, all_syms = set(), []
    for syms in SECTOR_MAP.values():
        for s in syms:
            if s not in seen:
                seen.add(s)
                all_syms.append(s)
    total = len(all_syms)

    send(
        f'🏭 <b>Score B BT — Toàn HOSE ({total} mã)</b>' + NL
        + f'Chạy tuần tự, ước tính ~{total*2}-{total*3} phút.' + NL
        + '<i>Dùng /sbtbt hose sl=7 tp=14 hold=10 s=65 để custom params.</i>',
        chat_id
    )

    def _wr(df):
        return len(df[df['pnl'] > 0]) / len(df) * 100 if len(df) > 0 else 0.0

    def _exp(df):
        if len(df) == 0: return 0.0
        wins = df[df['pnl'] > 0]['pnl']
        loss = df[df['pnl'] <= 0]['pnl']
        wr_  = len(wins) / len(df)
        aw   = wins.mean() if len(wins) > 0 else 0.0
        al   = abs(loss.mean()) if len(loss) > 0 else 0.0
        return round(wr_ * aw - (1 - wr_) * al, 3)

    def _pf(df):
        w = df[df['pnl'] > 0]['pnl'].sum()
        l = abs(df[df['pnl'] < 0]['pnl'].sum())
        return round(w / l, 2) if l > 0 else float('inf')

    rows = []
    for idx, symbol in enumerate(all_syms, 1):
        send(f'⏳ [{idx}/{total}] <b>{symbol}</b>...', chat_id)
        try:
            import backtest as bt

            _sl   = custom_sl    or 0.07
            _tp   = custom_tp    or 0.14
            _hold = custom_hold  or 10
            _msc  = custom_score or get_min_score(symbol)

            df, _ = _load_data_retry(symbol, chat_id, label=symbol)
            if df is None or len(df) < 400:
                rows.append({'sym': symbol, 'status': 'no_data', 'err': 'data<400'})
                continue

            res = bt.run_backtest_symbol(
                symbol, verbose=False, use_regime=False,
                sl=_sl, tp=_tp, hold_days=_hold, min_score=_msc,
                _df_cache=df
            )
            if not res or not res.get('buy'):
                rows.append({'sym': symbol, 'status': 'no_signal', 'err': 'no MUA'})
                continue

            trades_df = res.get('trades')
            if trades_df is None or len(trades_df) < 10:
                rows.append({'sym': symbol, 'status': 'few_trades',
                             'err': f'n={len(trades_df) if trades_df is not None else 0}'})
                continue

            buy_df = trades_df[trades_df['action'] == 'MUA'].copy()
            if len(buy_df) < 10:
                rows.append({'sym': symbol, 'status': 'few_trades',
                             'err': f'n_buy={len(buy_df)}'})
                continue

            # Tính Score B per lệnh
            scb_vals = [_calc_score_b_from_row(row, symbol)[0]
                        for _, row in buy_df.iterrows()]
            buy_df = buy_df.copy()
            buy_df['score_b'] = scb_vals

            hi_df = buy_df[buy_df['score_b'] >= 60]
            lo_df = buy_df[buy_df['score_b'] < 60]

            wr_hi  = _wr(hi_df);   exp_hi  = _exp(hi_df)
            wr_lo  = _wr(lo_df);   exp_lo  = _exp(lo_df)
            wr_all = _wr(buy_df);  exp_all = _exp(buy_df)
            pf_hi  = _pf(hi_df)
            d_exp  = round(exp_hi - exp_lo, 3)
            d_wr   = round(wr_hi  - wr_lo,  1)

            # Walk-Forward — trade-based 4 windows
            buy_sorted = buy_df.sort_values('date').reset_index(drop=True)
            n_total_t  = len(buy_sorted)
            n_windows  = 4
            win_sz     = n_total_t // n_windows
            n_pass_wf  = 0
            n_wf_done  = 0
            if win_sz >= 4:
                for w in range(n_windows):
                    is_df  = buy_sorted.iloc[:w * win_sz]
                    oos_df = buy_sorted.iloc[w * win_sz:(w+1) * win_sz]
                    if len(is_df) < 4 or len(oos_df) < 4:
                        continue
                    oos_hi = oos_df[oos_df['score_b'] >= 60]
                    oos_lo = oos_df[oos_df['score_b'] < 60]
                    if _exp(oos_hi) > _exp(oos_lo) and len(oos_hi) >= 3:
                        n_pass_wf += 1
                    n_wf_done += 1

            pct_pass = n_pass_wf / n_wf_done * 100 if n_wf_done > 0 else 0

            # Edge verdict
            if d_exp >= 0.3 and d_wr >= 3.0 and len(hi_df) >= 15:
                edge = 'STRONG'
            elif d_exp >= 0.1 and len(hi_df) >= 8:
                edge = 'WEAK'
            elif d_exp < 0:
                edge = 'NEGATIVE'
            else:
                edge = 'UNCLEAR'

            # WF verdict
            if n_wf_done == 0:       wf_vrd = 'n/a'
            elif pct_pass >= 75:     wf_vrd = f'Robust({n_pass_wf}/{n_wf_done})'
            elif pct_pass >= 50:     wf_vrd = f'Prom({n_pass_wf}/{n_wf_done})'
            else:                    wf_vrd = f'Fail({n_pass_wf}/{n_wf_done})'

            rows.append({
                'sym':       symbol,
                'status':    'ok',
                'n_all':     len(buy_df),
                'n_hi':      len(hi_df),
                'n_lo':      len(lo_df),
                'wr_hi':     round(wr_hi, 1),
                'wr_lo':     round(wr_lo, 1),
                'wr_all':    round(wr_all, 1),
                'exp_hi':    exp_hi,
                'exp_lo':    exp_lo,
                'exp_all':   exp_all,
                'pf_hi':     pf_hi,
                'd_exp':     d_exp,
                'd_wr':      d_wr,
                'edge':      edge,
                'wf_vrd':    wf_vrd,
                'n_pass_wf': n_pass_wf,
                'n_wf':      n_wf_done,
            })

        except Exception as e:
            import traceback
            logger.error(f'sbtbt_hose {symbol}: {traceback.format_exc()}')
            rows.append({'sym': symbol, 'status': 'error', 'err': str(e)[:60]})

    # ── Bảng tổng kết phẳng ──────────────────────────────────────────────
    ok_rows  = [r for r in rows if r['status'] == 'ok']
    err_rows = [r for r in rows if r['status'] != 'ok']

    edge_icons = {'STRONG': '✅', 'WEAK': '🟡', 'UNCLEAR': '⚠', 'NEGATIVE': '❌'}

    def _sort_key(r):
        e_rank = {'STRONG': 0, 'WEAK': 1, 'UNCLEAR': 2, 'NEGATIVE': 3}.get(r['edge'], 4)
        w_rank = 0 if 'Robust' in r['wf_vrd'] else (1 if 'Prom' in r['wf_vrd'] else 2)
        return (e_rank, w_rank, -r.get('d_exp', 0))

    ok_rows.sort(key=_sort_key)

    n_strong  = sum(1 for r in ok_rows if r['edge'] == 'STRONG')
    n_weak    = sum(1 for r in ok_rows if r['edge'] == 'WEAK')
    n_neg     = sum(1 for r in ok_rows if r['edge'] == 'NEGATIVE')
    n_unclear = sum(1 for r in ok_rows if r['edge'] == 'UNCLEAR')
    n_robust  = sum(1 for r in ok_rows if 'Robust' in r['wf_vrd'])
    avg_dexp  = sum(r['d_exp'] for r in ok_rows) / len(ok_rows) if ok_rows else 0

    # Header tổng kết
    hdr = (
        f'📊 <b>Score B BT — Toàn HOSE ({total} mã)</b>' + NL
        + '─' * 30 + NL
        + f'OK: {len(ok_rows)} | Skip/Lỗi: {len(err_rows)}' + NL
        + f'✅ STRONG: {n_strong} | 🟡 WEAK: {n_weak} | ⚠ UNCLEAR: {n_unclear} | ❌ NEG: {n_neg}' + NL
        + f'WF Robust: {n_robust} | ΔExp TB: {avg_dexp:+.3f}%' + NL
    )

    if n_strong >= round(total * 0.15) and n_robust >= round(total * 0.10):
        hdr += NL + '✅ <b>Score B có edge nhất quán trên HOSE</b>' + NL + '→ Cân nhắc promote ScB thành gate cứng'
    elif n_strong + n_weak >= round(total * 0.25):
        hdr += NL + '🟡 <b>Edge có nhưng chưa đủ rộng</b> — giữ advisory'
    else:
        hdr += NL + '❌ <b>Chưa đủ edge để deploy rộng</b> — review công thức'
    send(hdr, chat_id)

    # Bảng chi tiết — gửi theo chunk
    detail_lines = [f'📋 <b>Chi tiết {len(ok_rows)} mã (sort: STRONG→WEAK→edge):</b>']
    for r in ok_rows:
        ei   = edge_icons.get(r['edge'], '❓')
        pf_s = f'{r["pf_hi"]:.2f}' if r['pf_hi'] != float('inf') else '∞'
        detail_lines.append(
            f'{ei} <b>{r["sym"]}</b> {r["n_all"]}L(hi={r["n_hi"]}L)' + NL
            + f'   WR {r["wr_hi"]:.0f}%↑/{r["wr_lo"]:.0f}%↓ '
            + f'Exp {r["exp_hi"]:+.3f}↑/{r["exp_lo"]:+.3f}↓ '
            + f'Δ={r["d_exp"]:+.3f} PF={pf_s}' + NL
            + f'   WF:{r["wf_vrd"]} | {r["edge"]}'
        )

    chunk = ''
    for line in detail_lines:
        if len(chunk) + len(line) + 1 > 3800:
            send(chunk, chat_id)
            chunk = line + NL
        else:
            chunk += line + NL
    if chunk.strip():
        send(chunk, chat_id)

    # Skip summary
    if err_rows:
        by_type = {}
        for r in err_rows:
            by_type.setdefault(r['status'], []).append(r['sym'])
        err_lines = [f'⚠ Skip/Lỗi ({len(err_rows)} mã):']
        for etype, esyms in by_type.items():
            err_lines.append(f'  {etype}: {", ".join(esyms[:15])}'
                             + (f' +{len(esyms)-15}' if len(esyms) > 15 else ''))
        send(NL.join(err_lines), chat_id)

    send('✅ Score B BT HOSE xong. Dùng /sbtbt SYM để xem breakdown chi tiết.', chat_id)

def _send_chunked(text, chat_id, chunk_size=3800):
    """Gửi text dài theo chunk, tách theo dòng."""
    NL = chr(10)
    lines = text.split(NL)
    chunk = ''
    for line in lines:
        if len(chunk) + len(line) + 1 > chunk_size:
            if chunk.strip():
                send(chunk, chat_id)
            chunk = line + NL
        else:
            chunk += line + NL
    if chunk.strip():
        send(chunk, chat_id)


def handle_sbtbt(args, chat_id):
    """
    /sbtbt SYM               — Score B BT 1 ma, full breakdown
    /sbtbt batch N           — batch N ma dau trong SIGNALS_WATCHLIST (N=1..10)
    /sbtbt hose              — batch toan HOSE (~144 ma / 12 nganh)
    /sbtbt SYM sl=7 tp=14 hold=10 s=65 — custom params
    """
    NL   = chr(10)
    args = [a.strip() for a in args if a.strip()]

    if not args:
        n_wl = len(WATCHLIST_META)
        send(
            '\U0001f4ca <b>/sbtbt — Score B Backtest</b>' + NL + NL
            + 'Validate Score B co edge khong bang cach split trade log' + NL
            + 'Score A theo ScB>=60 vs ScB<60 -> so sanh WR/Exp/PF + WF.' + NL + NL
            + ' /sbtbt STB              — detail 1 ma (full breakdown)' + NL
            + f' /sbtbt batch N          — batch N ma trong watchlist (N=1..{n_wl})' + NL
            + ' /sbtbt batch 5          — vi du: chay 5 ma dau' + NL
            + ' /sbtbt hose             — batch toan HOSE (~144 ma / 12 nganh)' + NL
            + ' /sbtbt STB sl=7 tp=14 hold=10 — custom params' + NL + NL
            + '<i>batch N = chay N ma trong SIGNALS_WATCHLIST, 1 bang tong ket</i>',
            chat_id
        )
        return

    sym = args[0].upper()
    c_sl = c_tp = c_hold = c_score = None
    for a in args[1:]:
        al = a.lower()
        try:
            if al.startswith('sl='):     c_sl    = float(al[3:]) / 100
            elif al.startswith('tp='):   c_tp    = float(al[3:]) / 100
            elif al.startswith('hold='): c_hold  = int(al[5:])
            elif al.startswith('s='):    c_score = int(al[2:])
        except Exception:
            pass

    if sym == 'HOSE':
        # Batch toan HOSE (~144 ma / 12 nganh)
        threading.Thread(
            target=_run_sbtbt_batch_hose,
            args=(chat_id, c_sl, c_tp, c_hold, c_score),
            daemon=True
        ).start()

    elif sym == 'BATCH':
        # /sbtbt batch N — chay N ma dau trong SIGNALS_WATCHLIST
        n = None
        if args[1:]:
            try:
                n = int(args[1])
            except Exception:
                pass
        all_wl = list(WATCHLIST_META.keys())
        if n is None or n < 1:
            send(
                '\u274c Dung: /sbtbt batch N (N = so ma, vi du /sbtbt batch 5)' + NL
                + f'Watchlist hien tai: {len(all_wl)} ma: {", ".join(all_wl)}',
                chat_id
            )
            return
        n = min(n, len(all_wl))
        syms = all_wl[:n]
        send(
            f'\U0001f4ca Score B BT batch {n} ma: {", ".join(syms)}' + NL
            + f'Uoc tinh ~{n * 2}-{n * 3} phut...',
            chat_id
        )
        threading.Thread(
            target=_run_sbtbt_batch,
            args=(syms, chat_id, c_sl, c_tp, c_hold, c_score),
            daemon=True
        ).start()

    else:
        # Single symbol — full detail
        threading.Thread(
            target=_run_sbtbt,
            args=(sym, chat_id, c_sl, c_tp, c_hold, c_score),
            daemon=True
        ).start()


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
        send('⏳ Đang tải data <b>' + symbol + '</b> từ VCI...', chat_id)
        _df_shared, _ = _load_data_retry(symbol, chat_id)

        # ── Backtest ──────────────────────────────────────────────────────────
        send('⏳ Đang chạy ML Backtest <b>' + symbol + '</b>...', chat_id)
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

        # FIX: Parallel 2 workers, mỗi mã có retry + progress ping
        def _bt_one_all(sym):
            try:
                # RATE LIMIT FIX: sleep nhỏ trước mỗi mã để stagger vnstock calls
                time.sleep(3)
                send(f'⏳ ML BT đang chạy <b>{sym}</b>...', chat_id)
                _df_one, _ = _load_data_retry(sym, chat_id, label=sym)
                res = bt.run_backtest_momentum(
                    sym, sl=sl, tp=tp, hold_days=hold,
                    min_ml_score=score, verbose=False, _df_cache=_df_one
                )
                wf = bt.run_walk_forward_momentum(
                    sym, sl=sl, tp=tp, hold_days=hold,
                    min_ml_score=score, verbose=False, _df_cache=_df_one
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
# /scanstatus + /signal_scan + /vol_scan + /ml_scan + /shark_scan
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
        ('ml',     '🤖', 'ML Momentum',        f'{ML_SCAN_INTERVAL_MIN} phút/lần',  '/ml_scan'),
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
        'ml':     'ML Momentum',
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
             + 'Hợp lệ: signal, vol, ml, shark, all', chat_id)
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


def handle_ml_scan(args, chat_id):
    action = (args[0].lower().strip() if args else 'toggle')
    _toggle_scanner('ml', action, chat_id)

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
                for p in sorted(batch_promising, key=lambda x: (-x.get('score_b_pass', False), -x.get('oos', 0))):
                    _pb   = p.get('score_b', 0)
                    _pbdg = p.get('score_b_badge', '➡')
                    _pbp  = '✅' if p.get('score_b_pass') else '⚠'
                    prom_msg += (
                        f'&#x1F7E2; <b>{p["sym"]}</b>  '
                        f'PF={p["pf"]} WR={p["wr"]}% OOS={p["oos"]}% '
                        f'decay={p["decay"]:+.0f}% WF={p["wf"]} '
                        f'n_OOS={p["n_lenhOOS"]}L | ScB={_pb}{_pbdg}{_pbp}' + NL
                    )
                prom_msg += NL + '<i>Tieu chi: OOS>=50% + PF>=1.2 + n_OOS>=15 | ScB>=60✅=timing tot</i>'
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
    /sascreen [batch_num|all|status] [ts] — Backtest Score A toàn HOSE theo batch.
    Dùng run_backtest_symbol() + run_walk_forward() thay vì ML v1.
    Params: SL/TP từ SYMBOL_CONFIG nếu có, fallback sl=7% tp=14% hold=10d score>=65
    ts flag: dùng Trailing Stop 1R/3R mode thay vì TP cứng
    """
    import threading
    NL = chr(10)

    syms = _get_hose_symbols()
    BATCH_SIZE = 30
    batches = [syms[i:i+BATCH_SIZE] for i in range(0, len(syms), BATCH_SIZE)]

    # Tách ts flag ra khỏi args
    args_clean = [a for a in args if a.lower() != 'ts']
    use_ts = len(args_clean) < len(args)   # True nếu có 'ts' trong args
    arg = args_clean[0].strip().lower() if args_clean else ''

    ts_label = ' [TS=1R/3R]' if use_ts else ''

    if arg == 'status':
        _handle_sascreen_status(chat_id, batches)
        return

    if arg == 'all':
        batch_indices = list(range(len(batches)))
        send(
            f'&#x1F7E0; <b>SA Screen — ALL {len(batches)} batch{ts_label}</b>' + NL
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
            f'&#x1F7E0; <b>SA Screen Batch {arg}{ts_label}</b>: '
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
            + '  /sascreen status   — Xem tien do' + NL
            + '  /sascreen 1 ts     — Batch 1 voi Trailing Stop' + NL
            + '  /sascreen all ts   — Toan HOSE voi Trailing Stop' + NL + NL
            + f'Tong {n_batches} batch (~30 ma/batch)' + NL
            + 'Tieu chi pass: OOS>=50% + PF>=1.2 + n_OOS>=15' + NL
            + '<i>Score A: RSI/MA/Vol/Breakout/52W/RS | sl=7% tp=14% hold=10d score>=75 (STRONG only)</i>',
            chat_id
        )
        return

    threading.Thread(
        target=_handle_sascreen_run,
        args=(chat_id, batches, batch_indices),
        kwargs={'trailing_stop': use_ts},
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


def _handle_sascreen_run(chat_id, batches, batch_indices, trailing_stop=False):
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
                        trailing_stop=trailing_stop,   # TRAIL patch
                    )

                    n_lenh = r1['buy']['total'] if r1 and r1.get('buy') else 0
                    if n_lenh < 10:
                        skipped.append(f'{sym}(n={n_lenh})')
                        _time.sleep(1)
                        continue

                    # ── Score A Walk-Forward ──────────────────────────────────
                    # SA-5 FIX2: pass cùng params với run_backtest_symbol để IS/OOS nhất quán
                    w1 = bt.run_walk_forward(
                        sym,
                        verbose=False,
                        _df_cache=df_sym,
                        sl=_sl, tp=_tp,
                        min_score=_msc, hold_days=_hold,
                        trailing_stop=trailing_stop,   # TRAIL patch
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

                    # ── Overfit detector (Session 10 — từ CrossValidation Report v2) ──
                    # Pattern: IS cao + OOS sụp + decay dương mạnh = IS overfit
                    # Ngưỡng: decay > +20% VÀ OOS_WR < 40% (từ NKG/DGC empirical data)
                    # Hiển thị warning ⛔ trong output để trader biết khi scan
                    _is_overfit = (w1 is not None and decay > 20 and oos_wr < 40)
                    _overfit_flag = ' &#x26D4;OVERFIT' if _is_overfit else ''

                    # Icon — cùng tiêu chí mlscreen để so sánh được
                    if oos_wr >= 55 and pf1 >= 1.3 and n_oos >= 15 and decay <= 15:
                        icon = '&#x1F7E2;'   # Xanh — strong
                    elif oos_wr >= 50 and pf1 >= 1.2 and n_oos >= 15:
                        icon = '&#x1F7E1;'   # Vàng — promising
                    elif n_oos < 15 and oos_wr >= 50:
                        icon = '&#x26A0;'    # Cảnh báo — ít lệnh
                    else:
                        icon = '&#x274C;'    # Đỏ — fail

                    # ── Score B: build proxy data từ df_sym + r1 ──────
                    try:
                        import numpy as np
                        cc_ = bt.find_col(df_sym, ['close','closeprice','close_price'])
                        vc_ = next((c for c in df_sym.columns if c.lower() in
                                   ('volume','volume_match','klgd','vol','trading_volume','match_volume')), None)
                        closes_  = bt.to_arr(df_sym[cc_]) if cc_ else np.array([])
                        if len(closes_) > 0 and closes_.max() < 1000: closes_ *= 1000
                        vols_    = bt.to_arr(df_sym[vc_]) if vc_ else np.array([])
                        # RS proxy: % change vs 20d ago
                        rs20_proxy = float((closes_[-1]/closes_[-21]-1)*100) if len(closes_) >= 21 else 0.0
                        vol_rat_   = float(vols_[-1]/np.mean(vols_[-20:])) if len(vols_) >= 20 and np.mean(vols_[-20:]) > 0 else 1.0
                        _scb_data  = {
                            'rs_20d': rs20_proxy,
                            'rs_bonus': max(0, rs20_proxy * 1.5),  # proxy: rs_bonus tỉ lệ rs20
                            'rs_5d': float((closes_[-1]/closes_[-6]-1)*100) if len(closes_) >= 6 else 0.0,
                            'sector_rs': {},  # không có sector data trong sascreen
                            'vol_ratio': round(vol_rat_, 2),
                        }
                        _scb_val, _scb_bd, _scb_badge = calc_score_b(_scb_data)
                    except Exception:
                        _scb_val, _scb_badge = 0, '➡'
                    _scb_pass = _scb_val >= 60
                    _scb_str  = f' | ScB={_scb_val}{_scb_badge}' + ('✅' if _scb_pass else '')

                    n_warn = ' &#x26A0;n&lt;15' if n_oos < 15 else ''
                    row = (
                        f'{icon} <b>{sym}</b>  '
                        f'n={n_lenh}L WR={wr1}% PF={pf1s} PnL={pnl1:+.1f}% | '
                        f'OOS={oos_s} decay={decay:+.0f}% WF={vrd_s}{n_warn}{_overfit_flag}{_scb_str}'
                    )
                    batch_results.append(row)

                    # Cache kết quả
                    _cache_key = f'{sym}_ts' if trailing_stop else sym
                    cache['all_results'][_cache_key] = {
                        'wr': wr1, 'pf': round(pf1, 2), 'n': n_lenh,
                        'oos_wr': round(oos_wr, 1), 'decay': round(decay, 1),
                        'wf': vrd_s, 'n_lenhOOS': n_oos, 'pnl': round(pnl1, 2),
                        'trailing_stop': trailing_stop,
                        'overfit': bool(_is_overfit),   # cast tránh numpy.bool_
                        'score_b': _scb_val, 'score_b_pass': bool(_scb_pass),
                    }

                    # Promising — cùng ngưỡng mlscreen để so sánh trực tiếp
                    # Loại trừ mã bị flag overfit khỏi promising list
                    is_promising = (oos_wr >= 50 and pf1 >= 1.2
                                    and n_oos >= 15 and n_lenh >= 15
                                    and not _is_overfit)   # Session 10: overfit guard
                    if is_promising:
                        entry = {
                            'sym': sym, 'wr': wr1, 'pf': pf1s,
                            'oos': round(oos_wr, 0), 'wf': vrd_s,
                            'n_lenhOOS': n_oos, 'decay': round(decay, 1),
                            'score_b': _scb_val, 'score_b_pass': bool(_scb_pass), 'score_b_badge': _scb_badge,
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
                # Thêm legend nếu batch có mã bị flag overfit
                _has_overfit_in_batch = any('OVERFIT' in r for r in batch_results)
                if _has_overfit_in_batch:
                    chunk += NL + '<i>&#x26D4;OVERFIT = decay&gt;+20% + OOS&lt;40% — IS overfit, promising list đã loại</i>'
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
                for p in sorted(all_prom, key=lambda x: (-x.get('score_b_pass', False), -x.get('oos', 0))):
                    _pb2  = p.get('score_b', 0)
                    _pbdg2 = p.get('score_b_badge', '➡')
                    _pbp2  = '✅' if p.get('score_b_pass') else '⚠'
                    final_msg += (
                        f' &#x1F7E2; <b>{p["sym"]}</b>  '
                        f'PF={p["pf"]} OOS={p["oos"]}% WF={p["wf"]} | ScB={_pb2}{_pbdg2}{_pbp2}' + NL
                    )
                final_msg += NL + '<i>✅ScB>=60=timing tot, ⚠=chờ RS | Chay /bt <SYM> de backtest chi tiet.</i>'
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
        '<b>VN Trader Bot V6 S17</b> — Chao mung!\n\n'

        '<b>&#x1F4E1; TIN HIEU CHINH (hang ngay):</b>\n'
        '/signals — Score A (10 ma) + B-filter + scorecard v3\n'
        '/scbscan — Score B scan 24 ma (Tier A full / Tier B 50%)\n'
        '/scb SYM — Score B realtime 1 ma: breakdown C1/C2/C3/C4\n'
        '/mlscan — ML Momentum Leader scan watchlist\n\n'

        '<b>&#x1F50D; PHAN TICH MA:</b>\n'
        '/analyze SYM — Phan tich day du + scorecard v3\n'
        '/bt SYM — Backtest + Deep Analytics\n'
        '/bt SYM full — Full + B-filter + Entry bias + Block 3H\n'
        '/bt all — Toan watchlist Score A (10 ma)\n'
        '/optimize SYM — Tim SL/TP/Hold/Score toi uu\n\n'

        '<b>&#x1F4CA; SCORE B BACKTEST:</b>\n'
        '/sbtbt STB — Score B BT + WF: validate ScB edge\n'
        '/sbtbt batch N — Batch N ma dau watchlist\n'
        '/sbtbt hose — Batch toan HOSE (~143 ma)\n\n'

        '<b>&#x1F916; ML BACKTEST:</b>\n'
        '/mlbt SYM — Backtest ML v1\n'
        '/mlbt all — ML backtest toan watchlist\n'
        '/mlbtv3 SYM — ML v3 Pooled Event Study\n\n'

        '<b>&#x1F4C8; SECTOR & SCREENING:</b>\n'
        '/sascreen — Score A screening HOSE + overfit flag ⛔ + ScB\n'
        '/scan — Market Scanner 68 ma (T1/T2/T3) + Gate 1/2/3\n'
        '/sectorscan — Top3 moi nganh + RS vs VNI\n'
        '/sectorscan history — Lich su top3 5 ngay\n'
        '/sectorbt — Backtest Sector Rotation 7 nam\n'
        '/sectorbt fast — Backtest nhanh 3 nam\n\n'

        '<b>&#x1F4DD; PAPER TRADE & TRANG THAI:</b>\n'
        '/ptreport — Paper trade report (tat ca)\n'
        '/ptreport open — Lenh dang mo\n'
        '/ptreport scb — Chi Score B\n'
        '/ptreport ml — Chi ML\n'
        '/scanstatus — Trang thai scanner (signal/ml/scb)\n'
        '/scanner on/off all — Bat/tat scanner\n\n'

        '<b>&#x1F465; QUAN LY:</b>\n'
        '/subscribe — Dang ky nhan alert tu dong\n'
        '/unsubscribe — Huy dang ky\n'
        '/subscribers — Danh sach nguoi theo doi (admin)\n\n'

        '<i>Khong phai tu van dau tu. VN Trader Bot V6 S17 — 16/04/2026</i>'
    )
    send(msg, chat_id)


def _build_scorecard_v3_inputs(sym, score, d):
    """Map API response → inputs cho context_scorecard_v3.evaluate_signal()."""
    try:
        import numpy as np
        price   = d.get('price', 0)
        ma20    = d.get('ma20', 0) or d.get('sma20', 0)
        ma20_distance_pct = ((price - ma20) / ma20 * 100) if ma20 else 0.0
        vni_chg = d.get('vni_change_pct', d.get('vni_ret', 0)) or 0
        vni_trend = 'UP' if vni_chg > 2 else ('DOWN' if vni_chg < -2 else 'FLAT')
        vni_vs_ma20_pct  = d.get('vni_vs_ma20_pct', 0) or 0
        vni_vol_regime   = d.get('vni_vol_regime', 'NORMAL') or 'NORMAL'
        vol_ratio = d.get('vol_ratio', 1.0) or 1.0
        raw_vc    = d.get('volume_context', d.get('vol_context', d.get('volume_regime', '')))
        if raw_vc and raw_vc.upper() in ('CLIMAX', 'BREAKOUT', 'NORMAL', 'LOW'):
            volume_context = raw_vc.upper()
        else:
            volume_context = ('CLIMAX' if vol_ratio >= 2.5 else
                              'BREAKOUT' if vol_ratio >= 1.5 else
                              'LOW' if vol_ratio < 0.7 else 'NORMAL')
        hh20 = d.get('hh20', d.get('high_20', 0)) or 0
        at_breakout = (abs((price - hh20) / hh20 * 100) < 2.0) if hh20 else False
        return dict(symbol=sym, score=score,
                    ma20_distance_pct=round(ma20_distance_pct, 2),
                    vni_trend=vni_trend,
                    vni_vs_ma20_pct=round(vni_vs_ma20_pct, 2),
                    vni_vol_regime=vni_vol_regime,
                    volume_context=volume_context,
                    at_breakout=at_breakout)
    except Exception as _ex:
        logger.warning(f'_build_scorecard_v3_inputs {sym}: {_ex}')
        return None


def _run_scorecard_v3(sym, score, d, compact=True):
    """Chạy scorecard v3 (hoặc v1 fallback). Trả về (text, err_str)."""
    if _sc is None:
        return None, 'scorecard module not loaded'
    try:
        if hasattr(_sc, 'evaluate_signal') and hasattr(_sc, 'format_for_telegram'):
            inputs = _build_scorecard_v3_inputs(sym, score, d)
            if inputs is None:
                return None, 'inputs build failed'
            result = _sc.evaluate_signal(**inputs)
            return _sc.format_for_telegram(result, compact=compact), None
        if hasattr(_sc, 'compute_realtime_context') and hasattr(_sc, 'format_scorecard_msg'):
            sc_result, sc_err = _sc.compute_realtime_context(sym, score)
            if sc_result:
                return _sc.format_scorecard_msg(sc_result, compact=compact), None
            return None, str(sc_err or 'N/A')
        return None, 'scorecard API không tương thích'
    except Exception as _ex:
        return None, str(_ex)


def _run_scorecard_v3_with_grade(sym, score, d, compact=True):
    """
    Giống _run_scorecard_v3 nhưng trả về (text, grade, err_str).
    grade: 'STRONG' | 'MODERATE' | 'WEAK' | 'SKIP' | None (nếu lỗi/không load)

    FIX (S16): Dùng thay _run_scorecard_v3 trong handle_signals để
    biết grade trước khi quyết định log paper trade.
    Scorecard là advisory layer — nếu grade=SKIP thì KHÔNG log paper trade.
    """
    if _sc is None:
        return None, None, 'scorecard module not loaded'
    try:
        # Path 1: evaluate_signal trả về dict có 'grade'
        if hasattr(_sc, 'evaluate_signal') and hasattr(_sc, 'format_for_telegram'):
            inputs = _build_scorecard_v3_inputs(sym, score, d)
            if inputs is None:
                return None, None, 'inputs build failed'
            result = _sc.evaluate_signal(**inputs)
            grade  = result.get('grade')  # 'STRONG'/'MODERATE'/'WEAK'/'SKIP'
            text   = _sc.format_for_telegram(result, compact=compact)
            return text, grade, None
        # Path 2: compute_realtime_context (fallback v1) — trả về dict có 'grade'
        if hasattr(_sc, 'compute_realtime_context') and hasattr(_sc, 'format_scorecard_msg'):
            sc_result, sc_err = _sc.compute_realtime_context(sym, score)
            if sc_result:
                grade = sc_result.get('grade')
                text  = _sc.format_scorecard_msg(sc_result, compact=compact)
                return text, grade, None
            return None, None, str(sc_err or 'N/A')
        return None, None, 'scorecard API không tương thích'
    except Exception as _ex:
        return None, None, str(_ex)


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

            # ── Tin 3: Full Context Scorecard v3 (Advisory) ──────────────────
            if _sc is not None:
                try:
                    _sc_text, _sc_err = _run_scorecard_v3(symbol, score_adj, d, compact=False)
                    if _sc_text:
                        send(
                            '&#x1F4CA; <b>Context Scorecard v3 (Advisory): ' + symbol + '</b>\n'
                            + '=' * 28 + '\n'
                            + _sc_text,
                            chat_id
                        )
                    else:
                        send('&#x2139; Scorecard ' + symbol + ': ' + str(_sc_err or 'N/A'), chat_id)
                except Exception as _sc_ex:
                    logger.warning('scorecard analyze ' + symbol + ': ' + str(_sc_ex))

        except Exception as e:
            logger.error('handle_analyze ' + symbol + ': ' + str(e))
            logger.error(traceback.format_exc())
            # Fallback: gửi chỉ A
            send(build_analysis_msg(d), chat_id)
            send('⚠ Loi B-filter: ' + str(e)[:100], chat_id)

    threading.Thread(target=run, daemon=True).start()


# ── PER-SYMBOL RULE ENGINE (Session 10) ─────────────────────────────────────
# Tập trung toàn bộ per-symbol exceptions từ backtest data vào 1 hàm.
# Gọi trong gate /signals SAU khi đã qua score_min + skip_bucket checks.
# Trả về (should_skip: bool, reason: str | None)
#
# Rules được implement (data-driven từ /bt Deep Analytics):
#   SSI : score ≥ 85 (score <85: 65-74 Exp=-1.32, 75-84 Exp=-0.05) — đã qua get_min_score()
#   MCH : yêu cầu ma20_distance > 5% (EXTENDED). False BK 80%, OK zone nguy hiểm
#   CTS : score 75-84 = FULL SIZE (EXPLORE +5.68%, n=13) — không skip, ghi chú size
#   FRT : SKIP khi VNI DOWN (<-2%). Exp=-1.47% (17L)
#   STB : SKIP khi VNI DOWN (<-2%). Exp=-0.22% (16L)
#   VND : đã xử lý qua SCORE_SKIP_BUCKETS (75-84 skip)
#         VNI DOWN Exp=+0.16 (9L) — gần flat, ghi chú caution (không skip cứng)
#   NKG : hard cap 50% — xử lý qua POSITION_SIZE_CAPS
#   DGC : score 85-94 skip — đã qua get_min_score()/SCORE_THRESHOLDS
#   HAH : score <75 skip — đã qua get_min_score()
#   PC1 : score 95+ skip — xử lý riêng bên dưới (score threshold)

# Config per-symbol rules — data-driven, không hardcode trong logic
_PER_SYMBOL_RULES = {
    # sym: dict of rules
    'MCH': {
        'require_extended_ma20': True,   # ma20_distance_pct > 5%: Exp tốt. ≤5% False BK 80%
        'extended_threshold': 5.0,
        'skip_vni_down': True,           # VNI change < -2%: Exp=-0.61% (10L) — cùng pattern FRT/STB
        'skip_reason': 'MCH: cần EXTENDED>5% (False BK 80% khi OK zone)',
        'skip_reason_vni': 'MCH: SKIP VNI DOWN (Exp=-0.61%, 10L)',
    },
    'FRT': {
        'skip_vni_down': True,           # VNI change < -2%: Exp=-1.47% (17L)
        'skip_high_vol': True,           # vol_ratio >= 2.0: Exp=-0.17% (21L) — ngược PC1/NKG
        'skip_score_above': 95,          # Score 95+: Exp=+0.113% (28L) — flat, không đủ edge
        'skip_reason': 'FRT: SKIP VNI DOWN (Exp=-1.47%, 17L)',
        'skip_reason_hvol': 'FRT: SKIP HIGH_VOL (Exp=-0.17%, 21L)',
        'skip_reason_score': 'FRT: SKIP score 95+ (Exp flat +0.113%, 28L)',
    },
    'STB': {
        'skip_vni_down': True,           # VNI change < -2%: Exp=-0.22% (16L)
        'skip_reason': 'STB: SKIP VNI DOWN (Exp=-0.22%, 16L)',
    },
    'PC1': {
        'skip_score_above': 95,          # Score 95+: Exp=+0.024% (32L) — quá flat
        'skip_reason': 'PC1: SKIP score 95+ (Exp flat +0.024%, 32L)',
    },
}

# Symbols có CTS explore note (không skip, chỉ ghi chú)
_EXPLORE_NOTES = {
    'CTS': {
        'score_range': (75, 84),
        'note': 'CTS 75-84: EXPLORE FULL SIZE (Exp=+5.68%, n=13L — chưa confirm)',
    },
    'PC1': {
        'score_range': (None, None),  # any score with HIGH_VOL
        'high_vol_note': 'PC1 HIGH_VOL: EXPLORE boost (Exp=+6.44%, n=20L)',
    },
    'FRT': {
        'score_range': (75, 84),
        'note': 'FRT 75-84: BEST BUCKET — FULL SIZE (Exp=+2.27%, n=16L)',
    },
    'NKG': {
        'high_vol_note': 'NKG HIGH_VOL: tích cực (Exp=+3.10%, n=27L) — vẫn áp cap 50%',
    },
    'VND': {
        'vni_down_note': 'VND VNI DOWN: Exp=+0.16 (9L) — gần flat, size nhỏ',
    },
}


def _check_per_symbol_rules(sym, score_adj, item, vni_change_pct):
    """
    Kiểm tra per-symbol rules. Trả về (skip: bool, reason: str, notes: list[str]).
    - skip=True: gate lệnh → vào skipped list
    - notes: advisory messages hiển thị trong signal (không block)
    vni_change_pct: float, thay đổi % VNI trong ngày (từ /api/market hoặc vnindex_data)
    """
    rules = _PER_SYMBOL_RULES.get(sym, {})
    notes = []

    # ── HARD SKIP rules ────────────────────────────────────────────────────────
    # 1. MCH: yêu cầu EXTENDED > 5% MA20
    if rules.get('require_extended_ma20'):
        ma20_dist = item.get('dist_ma20_pct', None)
        if ma20_dist is None:
            # Fallback: tính từ price + ma20 nếu API có
            price = item.get('price', 0)
            ma20  = item.get('ma20', 0) or item.get('sma20', 0)
            if price and ma20:
                ma20_dist = (price - ma20) / ma20 * 100
        threshold = rules.get('extended_threshold', 5.0)
        if ma20_dist is not None and ma20_dist <= threshold:
            return True, rules['skip_reason'] + f' (MA20 dist={ma20_dist:.1f}%)', notes

    # 2. FRT/STB/MCH: SKIP VNI DOWN
    if rules.get('skip_vni_down') and vni_change_pct is not None:
        if vni_change_pct < -2.0:
            reason = rules.get('skip_reason_vni', rules.get('skip_reason', ''))
            return True, reason + f' (VNI={vni_change_pct:+.1f}%)', notes

    # 3. FRT: SKIP HIGH_VOL (vol_ratio >= 2.0, ngược PC1/NKG)
    if rules.get('skip_high_vol'):
        vol_ratio = item.get('vol_ratio', 1.0) or 1.0
        if vol_ratio >= 2.0:
            reason = rules.get('skip_reason_hvol', rules.get('skip_reason', ''))
            return True, reason + f' (vol={vol_ratio:.1f}x)', notes

    # 4. FRT/PC1: SKIP score 95+ (dùng skip_reason_score nếu có)
    if rules.get('skip_score_above') and score_adj >= rules['skip_score_above']:
        reason = rules.get('skip_reason_score', rules.get('skip_reason', ''))
        return True, reason + f' (score={score_adj})', notes

    # ── SOFT NOTES (advisory, không block) ────────────────────────────────────
    explore = _EXPLORE_NOTES.get(sym, {})

    # CTS 75-84: EXPLORE FULL SIZE note
    if sym == 'CTS' and explore:
        lo, hi = explore['score_range']
        if lo is not None and lo <= score_adj <= hi:
            notes.append(explore['note'])

    # FRT 75-84: BEST BUCKET note
    if sym == 'FRT' and explore:
        lo, hi = explore.get('score_range', (None, None))
        if lo is not None and lo <= score_adj <= hi:
            notes.append(explore['note'])

    # NKG HIGH_VOL: tích cực note
    if sym == 'NKG' and explore:
        vol_ratio = item.get('vol_ratio', 1.0)
        if vol_ratio and vol_ratio >= 2.0 and 'high_vol_note' in explore:
            notes.append(explore['high_vol_note'] + f' (vol={vol_ratio:.1f}x)')

    # VND VNI DOWN caution (Exp flat, không skip)
    if sym == 'VND' and vni_change_pct is not None and vni_change_pct < -2.0:
        if 'vni_down_note' in explore:
            notes.append(explore['vni_down_note'] + f' (VNI={vni_change_pct:+.1f}%)')

    # PC1 HIGH_VOL explore note
    if sym == 'PC1' and explore:
        vol_ratio = item.get('vol_ratio', 1.0)
        if vol_ratio and vol_ratio >= 2.0 and 'high_vol_note' in explore:
            notes.append(explore['high_vol_note'] + f' (vol={vol_ratio:.1f}x)')

    return False, None, notes

# ══════════════════════════════════════════════════════════════════
# SCORE B — RS Momentum Score (0–100), độc lập với Score A
# Đo sức mạnh tương đối của mã vs thị trường + volume trend
# Dùng data đã có từ API response — không cần gọi VCI thêm
#
# Công thức:
#   RS_VNI_20d  (40đ): momentum ngắn hạn vs index
#   RS_VNI_60d  (30đ): momentum trung hạn vs index  ← dùng rs_bonus proxy
#   Sector RS   (20đ): rank trong ngành
#   Vol Trend   (10đ): ADTV tăng = tiền đang vào
#
# Ngưỡng deploy: >= 60 = mã đang outperform, timing tốt
# ══════════════════════════════════════════════════════════════════
def calc_score_b(data):
    """
    Score B — Context Timing Score (0–100)
    Đo CONTEXT thị trường + per-symbol pattern, ORTHOGONAL với Score A và ML.

    Khác ML v1: ML đo kỹ thuật của mã (RS/RSI/Structure/52W).
                Score B đo TIMING: thị trường đang ở trạng thái nào cho mã này?

    4 Components — tất cả đều có Exp$ từ backtest thực tế:
      C1 (30đ): VNI Regime — FRT/STB/MCH Exp xấu khi VNI DOWN
      C2 (25đ): Volume Pattern per-symbol — PC1/NKG HIGH_VOL tốt, FRT HIGH_VOL xấu
      C3 (25đ): Score Bucket Quality — bucket nào có Exp dương/âm
      C4 (20đ): MA20 Distance — MCH False BK 80% khi ≤5%, extended quá xa cũng rủi ro

    Ngưỡng: >= 60 = context tốt, timing hợp lệ
    Input : data dict từ /api/analyze, scan_via_api, hoặc sascreen proxy
    Output: (score_b: int, breakdown: dict, badge: str)
    """
    score_b = 0
    bd = {}

    sym       = str(data.get('symbol', '') or '').upper()
    vol_ratio = float(data.get('vol_ratio') or 1.0)
    score_a   = float(data.get('score', 0) or data.get('score_a', 0) or 0)
    price     = float(data.get('price', 0) or 0)
    ma20      = float(data.get('ma20', 0) or data.get('sma20', 0) or 0)
    vni_chg   = float(data.get('vni_change_pct', 0) or data.get('vni_chg', 0) or 0)

    # MA20 distance
    if ma20 > 0 and price > 0:
        ma20_dist = (price - ma20) / ma20 * 100
    else:
        ma20_dist = float(data.get('dist_ma20_pct', 0) or 0)

    # ── C1: VNI Regime (30đ) ────────────────────────────────────
    # Pattern validated: FRT VNI DOWN Exp=-1.47% (17L)
    #                    STB VNI DOWN Exp=-0.22% (16L)
    #                    MCH VNI DOWN Exp=-0.61% (10L)
    # Áp dụng toàn bộ mã — VNI DOWN là context xấu phổ biến
    VNI_DOWN_HARD = {'FRT', 'STB', 'MCH'}   # skip cứng trong Gate 3
    VNI_DOWN_SOFT = {'VND', 'HAH', 'DGC', 'SSI', 'CTS', 'NKG', 'PC1'}  # giảm score

    if vni_chg >= 1.0:
        c1 = 30   # VNI UP rõ ràng — context tốt nhất
        c1_label = f'VNI UP ({vni_chg:+.1f}%)'
    elif vni_chg >= -0.5:
        c1 = 18   # VNI FLAT — trung tính
        c1_label = f'VNI FLAT ({vni_chg:+.1f}%)'
    elif vni_chg >= -2.0:
        c1 = 8    # VNI nhẹ xuống — cảnh báo
        c1_label = f'VNI yếu ({vni_chg:+.1f}%)'
    else:
        # VNI DOWN < -2%: hard sym đã bị Gate 3 block, soft sym giảm score
        if sym in VNI_DOWN_HARD:
            c1 = 0   # đã bị Gate 3 skip — Score B irrelevant nhưng để 0
        elif sym in VNI_DOWN_SOFT:
            c1 = 3   # minimal — context rất xấu
        else:
            c1 = 5   # mã chưa có data — giảm nhẹ
        c1_label = f'VNI DOWN ({vni_chg:+.1f}%) ⚠'
    score_b += c1
    bd['vni'] = {'val': round(vni_chg, 1), 'pts': c1, 'label': c1_label}

    # ── C2: Volume Pattern per-symbol (25đ) ─────────────────────
    # PC1 HIGH_VOL: Exp=+6.44% (n=20L) → boost
    # NKG HIGH_VOL: Exp=+3.10% (n=27L) → boost (vẫn cap 50%)
    # FRT HIGH_VOL: Exp=-0.17% (n=21L) → penalty (Gate 3 skip vol≥2.0)
    # Các mã khác: volume tăng = tiền vào = tốt
    HIGH_VOL_GOOD = {'PC1': 25, 'NKG': 20, 'STB': 18, 'HAH': 18}  # Exp dương rõ
    HIGH_VOL_BAD  = {'FRT'}   # Exp âm — Gate 3 đã xử lý ≥2.0, đây phản ánh <2.0

    if vol_ratio >= 2.0:
        if sym in HIGH_VOL_GOOD:
            c2 = HIGH_VOL_GOOD[sym]   # per-symbol boost
            c2_label = f'HIGH_VOL {vol_ratio:.1f}x — {sym} historically tốt'
        elif sym in HIGH_VOL_BAD:
            c2 = 4    # FRT HIGH_VOL xấu
            c2_label = f'HIGH_VOL {vol_ratio:.1f}x — {sym} historically yếu ⚠'
        else:
            c2 = 15   # mã chưa validate — neutral-positive
            c2_label = f'HIGH_VOL {vol_ratio:.1f}x'
    elif vol_ratio >= 1.2:
        c2 = 18   # vol tăng bình thường — tín hiệu tốt
        c2_label = f'Vol tăng {vol_ratio:.1f}x'
    elif vol_ratio >= 0.8:
        c2 = 10   # vol bình thường
        c2_label = f'Vol bình thường {vol_ratio:.1f}x'
    else:
        c2 = 3    # vol thấp — thanh khoản yếu
        c2_label = f'Vol thấp {vol_ratio:.1f}x ⚠'
    score_b += c2
    bd['vol'] = {'val': round(vol_ratio, 2), 'pts': c2, 'label': c2_label}

    # ── C3: Score Bucket Quality (25đ) ──────────────────────────
    # Từ backtest bucket analysis:
    #   VND  75-84: Exp=-2.21% (17L) — WORST bucket
    #   DGC  85-94: Exp=-0.23% (27L) — bucket âm
    #   FRT  75-84: Exp=+2.27% (16L) — BEST bucket
    #   CTS  75-84: Exp=+5.68% (13L) — BEST bucket (EXPLORE)
    #   PC1  95+  : Exp=+0.02% (32L) — flat, không đủ edge
    #   FRT  95+  : Exp=+0.11% (28L) — flat
    WORST_BUCKETS = {  # (sym, lo, hi): penalty
        'VND': (75, 84),
        'DGC': (85, 94),
    }
    BEST_BUCKETS = {   # (sym, lo, hi): bonus
        'FRT': (75, 84),
        'CTS': (75, 84),
    }
    FLAT_BUCKETS = {   # (sym, lo, hi): score gần flat
        'PC1': (95, 100),
        'FRT': (95, 100),
    }

    sa = int(score_a)
    c3 = 15  # default: bucket chưa validate
    c3_label = f'Score A={sa} (bucket chưa validate)'

    for _sym, (lo, hi) in WORST_BUCKETS.items():
        if sym == _sym and lo <= sa <= hi:
            c3 = 0
            c3_label = f'Score A={sa} trong WORST bucket {lo}-{hi} (Exp âm ⛔)'
            break
    else:
        for _sym, (lo, hi) in BEST_BUCKETS.items():
            if sym == _sym and lo <= sa <= hi:
                c3 = 25
                c3_label = f'Score A={sa} trong BEST bucket {lo}-{hi} (Exp tốt ✅)'
                break
        else:
            for _sym, (lo, hi) in FLAT_BUCKETS.items():
                if sym == _sym and lo <= sa <= hi:
                    c3 = 8
                    c3_label = f'Score A={sa} trong FLAT bucket {lo}-{hi} (edge yếu ⚠)'
                    break
            else:
                # Mã không có bucket đặc biệt — score cao hơn tốt hơn (trong lý)
                if sa >= 85:   c3 = 22
                elif sa >= 75: c3 = 18
                elif sa >= 65: c3 = 14
                else:          c3 = 8
                c3_label = f'Score A={sa}'

    score_b += c3
    bd['bucket'] = {'val': sa, 'pts': c3, 'label': c3_label}

    # ── C4: MA20 Distance (20đ) ──────────────────────────────────
    # MCH: False BK 80% khi dist ≤5% (OK zone nguy hiểm)
    # Extended quá xa (>15%) cũng rủi ro pullback
    # Optimal: 5-15% trên MA20 (momentum confirmed, chưa quá extended)
    MCH_EXTENDED_THRESHOLD = 5.0

    if sym == 'MCH':
        if ma20_dist > MCH_EXTENDED_THRESHOLD:
            c4 = 20   # MCH EXTENDED — safe zone
            c4_label = f'MCH dist MA20 {ma20_dist:.1f}% > 5% (EXTENDED ✅)'
        else:
            c4 = 0    # MCH OK zone — False BK 80%
            c4_label = f'MCH dist MA20 {ma20_dist:.1f}% ≤ 5% (False BK 80% ⛔)'
    elif ma20_dist > 20:
        c4 = 6    # Quá extended — pullback risk
        c4_label = f'Quá extended {ma20_dist:.1f}% vs MA20 ⚠'
    elif ma20_dist > 10:
        c4 = 14   # Extended vừa — momentum tốt nhưng cẩn thận
        c4_label = f'Extended {ma20_dist:.1f}% vs MA20'
    elif ma20_dist >= 2:
        c4 = 20   # Optimal zone — vừa break, chưa extended
        c4_label = f'Optimal zone {ma20_dist:.1f}% vs MA20 ✅'
    elif ma20_dist >= 0:
        c4 = 12   # Gần MA20 — chưa confirm break
        c4_label = f'Gần MA20 {ma20_dist:.1f}% — chưa confirm'
    else:
        c4 = 0    # Dưới MA20 — không hợp lệ cho mua
        c4_label = f'Dưới MA20 {ma20_dist:.1f}% ⛔'
    score_b += c4
    bd['ma20'] = {'val': round(ma20_dist, 1), 'pts': c4, 'label': c4_label}

    score_b = min(100, max(0, score_b))

    # ── Hard cap: bad patterns có Exp âm rõ ràng ────────────────
    # Dù các component khác cho điểm, context xấu đã validate → cap cứng
    cap_reason = None

    # 1. VNI DOWN (<-2%) + mã nhạy cảm VNI — đã validate Exp âm
    VNI_DOWN_HARD = {'FRT', 'STB', 'MCH'}
    if vni_chg < -2.0 and sym in VNI_DOWN_HARD:
        if score_b > 35:
            score_b = 35
            cap_reason = f'{sym} VNI DOWN cap=35 (Exp âm validated)'

    # 2. WORST bucket: VND 75-84 (Exp=-2.21%), DGC 85-94 (Exp=-0.23%)
    worst_hit = (
        (sym == 'VND' and 75 <= sa <= 84) or
        (sym == 'DGC' and 85 <= sa <= 94)
    )
    if worst_hit and score_b > 40:
        score_b = 40
        cap_reason = f'{sym} WORST bucket cap=40 (Exp âm validated)'

    # 3. MCH False BK zone (dist ≤5%) — False BK 80%
    if sym == 'MCH' and ma20_dist <= MCH_EXTENDED_THRESHOLD and score_b > 38:
        score_b = 38
        cap_reason = 'MCH False BK zone cap=38 (80% false breakout)'

    # 4. FRT HIGH_VOL (≥2.0) — Exp=-0.17% (Gate 3 đã skip ≥2.0, đây phòng thủ thêm)
    if sym == 'FRT' and vol_ratio >= 2.0 and score_b > 40:
        score_b = 40
        cap_reason = 'FRT HIGH_VOL cap=40 (Exp=-0.17% validated)'

    if cap_reason:
        bd['cap'] = cap_reason

    # Badge
    if   score_b >= 75: badge = '🔥'   # Context rất tốt
    elif score_b >= 60: badge = '✅'   # Pass — timing hợp lệ
    elif score_b >= 45: badge = '⚠'   # Trung tính — cân nhắc
    else:               badge = '⛔'   # Context xấu — tránh

    return score_b, bd, badge


def fmt_score_b(score_b, bd, badge, compact=True):
    """Format Score B thành string hiển thị Telegram."""
    NL = chr(10)
    if compact:
        pass_str = 'PASS ✅' if score_b >= 60 else ('neutral' if score_b >= 45 else 'SKIP ⛔')
        return f'ScB={score_b} {badge} ({pass_str})'
    else:
        # Full breakdown cho /analyze
        lines = [
            f'{badge} <b>Score B — Context Timing: {score_b}/100</b>',
            f'  VNI Regime : {bd.get("vni",{}).get("pts",0):>2}đ/30  {bd.get("vni",{}).get("label","")}',
            f'  Vol Pattern: {bd.get("vol",{}).get("pts",0):>2}đ/25  {bd.get("vol",{}).get("label","")}',
            f'  Score Bucket:{bd.get("bucket",{}).get("pts",0):>2}đ/25  {bd.get("bucket",{}).get("label","")}',
            f'  MA20 Dist  : {bd.get("ma20",{}).get("pts",0):>2}đ/20  {bd.get("ma20",{}).get("label","")}',
            '',
        ]
        if score_b >= 60:
            lines.append('<i>✅ Context tốt — timing hợp lệ</i>')
        elif score_b >= 45:
            lines.append('<i>⚠ Context trung tính — theo dõi thêm</i>')
        else:
            lines.append('<i>⛔ Context xấu — chờ điều kiện tốt hơn</i>')
        return NL.join(lines)


def handle_scb(sym, chat_id):
    """
    /scb SYM — Tính Score B realtime cho 1 mã.
    Hiển thị full breakdown C1/C2/C3/C4 + hard skip status + verdict.
    Chạy trong thread riêng.
    """
    NL = chr(10)

    def run():
        try:
            data = call_api('/api/analyze/' + sym)
            if not data or data.get('error'):
                send(f'❌ Không lấy được dữ liệu cho <b>{sym}</b>.', chat_id)
                return

            score_a   = int(data.get('score', 0) or 0)
            price     = float(data.get('price', 0) or 0)
            ma20      = float(data.get('ma20', 0) or data.get('sma20', 0) or 0)
            vol_ratio = float(data.get('vol_ratio', 1.0) or 1.0)
            vni_chg   = float(data.get('vni_change_pct', 0) or 0)
            ma20_dist = ((price - ma20) / ma20 * 100) if ma20 > 0 and price > 0 \
                        else float(data.get('dist_ma20_pct', 0) or 0)

            score_b, bd, badge = calc_score_b(data)

            # Tier & watchlist status
            in_wl   = sym in SCB_WATCHLIST
            tier    = ('A' if sym in SCB_WATCHLIST_TIER_A
                       else 'B' if sym in SCB_WATCHLIST_TIER_B
                       else '—')
            wl_note = (f'Tier {tier}' if in_wl else 'Không trong SCB watchlist')

            # Hard skip check (dùng lại logic _scb_format_signal)
            vni_regime, ma20_zone, score_bucket, vol_pat = _scb_get_zones(
                vni_chg, ma20_dist, vol_ratio, score_a
            )

            skip_reasons = []
            # Layer 1 — Universal gate
            if vni_regime == 'DOWN':
                skip_reasons.append('VNI DOWN (<-2%)')
            min_score_a = SCB_SCORE_A_MIN.get(sym, 65) if in_wl else 65
            if score_a < min_score_a:
                skip_reasons.append(f'Score A={score_a} < min {min_score_a}')
            if ma20_zone == 'BELOW':
                skip_reasons.append('Giá dưới MA20')
            # Layer 2 — Hard skip
            if not skip_reasons and in_wl:
                sk, sk_reason = _scb_check_hard_skip(
                    sym, vni_regime, ma20_zone, score_bucket, vol_pat
                )
                if sk:
                    skip_reasons.append(sk_reason)

            # ScB threshold
            scb_pass  = score_b >= SCB_SCORE_B_MIN
            gate_pass = scb_pass and not skip_reasons

            # Badge & verdict line
            if not scb_pass:
                verdict_line = f'⛔ ScB={score_b} < 60 — không đủ context'
            elif skip_reasons:
                verdict_line = '⛔ Skip: ' + ' | '.join(skip_reasons)
            elif score_b >= 75:
                verdict_line = '🔥 STRONG PASS — context rất tốt'
            else:
                verdict_line = '✅ PASS — context đủ điều kiện'

            # Cap note
            cap_note = ''
            if 'cap' in bd:
                cap_note = NL + f'⚠ Cap: {bd["cap"]}'

            # WF/BT stats nếu có trong watchlist
            stats_line = ''
            if in_wl:
                bt  = SCB_BT_STATS.get(sym, {})
                wf  = SCB_WF_STATS.get(sym, {})
                if bt:
                    stats_line = (NL + f'BT: WR={bt.get("wr","?")}%'
                                  f' | Exp=+{bt.get("exp","?")}%'
                                  f' | PF={bt.get("pf","?")}')
                if wf:
                    worst_str = (f'+{wf["worst"]:.2f}%' if wf['worst'] >= 0
                                 else f'{wf["worst"]:.2f}%')
                    stats_line += (NL + f'WF: {wf["wf"]}'
                                   f' | Median OOS: +{wf["median"]:.2f}%'
                                   f' | Worst: {worst_str}')

            msg = (
                f'{badge} <b>Score B — {sym}</b> | {wl_note}' + NL
                + '─' * 28 + NL
                + f'<b>ScB = {score_b}/100</b>  (ngưỡng ≥60)' + NL
                + NL
                + f'C1 VNI Regime : <b>{bd["vni"]["pts"]:>2}/30</b>  {bd["vni"]["label"]}' + NL
                + f'C2 Vol Pattern: <b>{bd["vol"]["pts"]:>2}/25</b>  {bd["vol"]["label"]}' + NL
                + f'C3 Score Bucket:<b>{bd["bucket"]["pts"]:>2}/25</b>  {bd["bucket"]["label"]}' + NL
                + f'C4 MA20 Dist  : <b>{bd["ma20"]["pts"]:>2}/20</b>  {bd["ma20"]["label"]}'
                + cap_note
                + stats_line + NL
                + '─' * 28 + NL
                + f'<b>{verdict_line}</b>'
            )
            send(msg, chat_id)

        except Exception as e:
            logger.error(f'handle_scb {sym}: {e}')
            send(f'❌ Lỗi tính ScB cho {sym}: {str(e)[:120]}', chat_id)

    threading.Thread(target=run, daemon=True).start()


# ══════════════════════════════════════════════════════════════════
# RETRY WRAPPER — load_data với auto-retry khi rate limit / timeout
# Dùng thay cho bt.load_data() trong tất cả handler chính.
# ══════════════════════════════════════════════════════════════════
def _load_data_retry(sym, chat_id, days=None, label=None, max_retry=2):
    """
    Gọi bt.load_data() với retry khi bị rate limit (429) hoặc timeout.
    - max_retry=2: retry 2 lần, wait 30s rồi 60s
    - Gửi thông báo Telegram trước mỗi lần retry
    - Raise exception sau khi hết retry (để outer handler bắt và thông báo)
    """
    import time as _t
    import backtest as bt

    lbl = label or sym
    kw  = {'days': days} if days is not None else {}

    for attempt in range(max_retry + 1):
        try:
            df, meta = bt.load_data(sym, **kw)
            return df, meta
        except Exception as e:
            err_str = str(e).lower()
            is_rate_limit = any(x in err_str for x in
                                ['429', 'rate', 'too many', 'quota', 'limit'])
            is_timeout    = any(x in err_str for x in
                                ['timeout', 'timed out', 'connect', 'read error'])

            if attempt < max_retry:
                wait = 30 * (attempt + 1)   # 30s → 60s
                if is_rate_limit:
                    reason = f'⚠ Rate limit VCI ({lbl}). Đang chờ {wait}s rồi retry ({attempt+1}/{max_retry})...'
                elif is_timeout:
                    reason = f'⚠ Timeout khi tải {lbl}. Đang chờ {wait}s rồi retry ({attempt+1}/{max_retry})...'
                else:
                    reason = f'⚠ Lỗi tải {lbl}: {str(e)[:60]}. Retry {attempt+1}/{max_retry} sau {wait}s...'
                send(reason, chat_id)
                logger.warning(f'_load_data_retry {lbl} attempt {attempt}: {e}')
                _t.sleep(wait)
            else:
                # Hết retry — raise lên để caller xử lý
                logger.error(f'_load_data_retry {lbl} FAILED after {max_retry} retries: {e}')
                raise


def handle_signals(chat_id):
    def _run():
        try:
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
                _t.sleep(20)
                data = call_api('/api/signals')
                if not data:
                    send('&#x274C; Không lấy được tín hiệu. Thử lại sau 1-2 phút.', chat_id)
                    return
    
            # [PROGRESS PING 1/3] — API signals OK, bắt đầu xử lý
            send('&#x23F3; Đang tính B-filter cho ' + str(len(WATCHLIST_META)) + ' mã...', chat_id)
    
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
    
            # ── VNI change% cho per-symbol rules (FRT/STB VNI DOWN gate) ─────────────
            # Tính 1 lần ở đây, dùng cho tất cả mã trong loop phía dưới
            _vni_chg_today = None
            try:
                _market_now = call_api('/api/market') or {}
                _vni_now    = _market_now.get('VNINDEX', {})
                _vni_chg_today = float(_vni_now.get('change_pct', 0))
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
    
            # [PROGRESS PING 2/3] — Bắt đầu B-filter + per-symbol gates
            send('&#x23F3; Đang chạy per-symbol gates...', chat_id)
    
            # Đảm bảo tất cả mã Tier 1 đều được xét dù cache có hay không
            _wl_list = list(WATCHLIST_META.items())

            # ── FIX PERFORMANCE: Pre-load B-filter data song song (2 workers) ──────
            # Thay vì load tuần tự trong loop (10 mã × 4s = 40s),
            # pre-load tất cả song song → ~8-10s tổng
            import concurrent.futures as _cfu
            import backtest as _bt_preload

            def _preload_b(sym_):
                try:
                    df_, _ = _bt_preload.load_data(sym_, days=200)
                    return sym_, df_
                except Exception:
                    return sym_, None

            _wl_syms = [s for s, _ in _wl_list]
            _df_cache = {}
            try:
                with _cfu.ThreadPoolExecutor(max_workers=2) as _pex:
                    _pfuts = {_pex.submit(_preload_b, s): s for s in _wl_syms}
                    for _pf in _cfu.as_completed(_pfuts):
                        _ps, _pdf = _pf.result()
                        _df_cache[_ps] = _pdf
            except Exception:
                pass  # nếu parallel fail, df_cache rỗng → loop dùng None

            for sym, meta in _wl_list:
                item = data_by_sym.get(sym)
                if item is None:
                    # Không có trong cache — gọi trực tiếp
                    fallback = call_api('/api/analyze/' + sym)
                    if fallback and 'error' not in fallback:
                        item = fallback
                    else:
                        logger.warning(f'signals fallback {sym}: no data')
                        continue
                score  = item.get('score', 0)
                action = item.get('action', '')
                item['symbol'] = sym  # đảm bảo có key symbol

                # ── Soft filter (B): dùng df đã pre-load ─────────────────────────
                b_penalty  = 0
                b_warnings = []
                try:
                    mc    = _mc
                    df_b  = _df_cache.get(sym)  # lấy từ cache, không load lại
                    if df_b is not None:
                        ctx_b = mc.build_market_context(df_b, sym,
                                    item.get('price', 0),
                                    item.get('vol_ratio', 1.0), score)
                        _b_delta, _b_flags, _b_dets = _mc.calc_b_adjustment(ctx_b)
                        b_penalty = -_b_delta
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
                                   score_adj, b_warnings, None))
                    continue
    
                # ── Session 10: skip bucket anomaly (VND: bucket 75-84 worst) ─────────
                if action == 'MUA' and is_score_in_skip_bucket(sym, score_adj):
                    _skip_buckets = meta.get('skip_buckets', [])
                    _bucket_str = ', '.join(f'{lo}-{hi}' for lo, hi in _skip_buckets)
                    skipped.append((sym, score, meta['score_min'], meta,
                                   score_adj, b_warnings, f'skip bucket {_bucket_str}'))
                    continue
    
                # ── Session 10: per-symbol rule engine (MCH/FRT/STB/PC1/CTS/VND) ─────
                if action == 'MUA':
                    _ps_skip, _ps_reason, _ps_notes = _check_per_symbol_rules(
                        sym, score_adj, item, _vni_chg_today
                    )
                    if _ps_skip:
                        skipped.append((sym, score, meta['score_min'], meta,
                                       score_adj, b_warnings, _ps_reason))
                        continue
                    # Lưu advisory notes vào item để hiển thị trong signal
                    item['_per_sym_notes'] = _ps_notes
    
                wl_signals.append((item, meta))
    
            # [PROGRESS PING 3/3] — Xong gates, format kết quả
            send('&#x1F4CB; Xong! Đang format kết quả...', chat_id)
    
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
    
                    # Nhóm + ngưỡng score + SL/TP + hold + cap (session 9+10)
                    _hold_d   = meta.get('hold_days', 10)
                    _size_cap = meta.get('size_cap', 1.0)
                    _cap_txt  = f' | Cap {int(_size_cap*100)}%' if _size_cap < 1.0 else ''
                    _bkt_warn = ' | ⚠skip 75-84' if meta.get('skip_buckets') else ''
                    meta_line = (f' &#x1F4CC; {meta["group"]} | '
                                 f'Score&gt;={meta["score_min"]} | '
                                 f'SL={meta["sl"]}% TP={meta["tp"]}% | '
                                 f'Hold={_hold_d}d{_cap_txt}{_bkt_warn}\n')
    
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
    
                    # ── Per-symbol advisory notes (CTS EXPLORE, VND caution...) ──────
                    _ps_note_txt = ''
                    _ps_notes = item.get('_per_sym_notes', [])
                    for _note in _ps_notes:
                        _ps_note_txt += '\n &#x1F4A1; ' + _note
    
                    # ── Compact Scorecard v3 (Advisory) ──────────────────────────────
                    _sc_line  = ''
                    _sc_grade = None   # FIX (S16): lưu grade để guard paper trade
                    if action == 'MUA' and _sc is not None:
                        try:
                            _sc_text, _sc_grade, _sc_err = _run_scorecard_v3_with_grade(
                                sym, score_adj, item, compact=True
                            )
                            if _sc_text:
                                _sc_line = '\n' + _sc_text
                            else:
                                logger.warning(f'scorecard {sym}: {_sc_err}')
                        except Exception as _sc_ex:
                            logger.warning(f'scorecard {sym} exception: {_sc_ex}')

                    # ── Trade Personality — hold style từ 3H analysis ─────────────────
                    _personality_line = ''
                    if action == 'MUA':
                        _personality_line = _fmt_trade_personality(sym, _hold_d)

                    msg += (
                        ae + ' <b>' + sym + '</b> — <b>' + action + '</b> (' + str(score) + '/100)\n'
                        + meta_line
                        + score_note
                        + ' Giá: ' + f'{p:,.0f}' + 'd  RSI: ' + str(item.get('rsi', 0)) + '\n'
                        + ' ' + vb + ' Vol: ' + f'{vr:.1f}' + 'x  ' + is_ + '\n'
                        + (' HT: ' + f'{sups[0]["price"]:,.0f}' if sups else '')
                        + (' KC: ' + f'{ress[0]["price"]:,.0f}' if ress else '') + '\n'
                        + div_txt + tio_txt + entry_warn + _ps_note_txt
                        + _sc_line + _personality_line + '\n\n'
                    )
                    if action == 'MUA':
                        buy_symbols.append({'symbol': sym, 'score': score})
                        item['_meta'] = meta
                        # FIX (S16): Chỉ log paper trade khi scorecard KHÔNG phải SKIP.
                        # Scorecard SKIP có evidence rõ (VNI OVERBOUGHT/BLOWOFF/etc.)
                        # → không nên đếm vào paper stats, làm nhiễu WR/Exp.
                        # Nếu scorecard không load được (_sc_grade=None) → vẫn log
                        # (graceful fallback: không có scorecard thì không chặn).
                        _sc_blocked = (_sc_grade == 'SKIP')
                        if _sc_blocked:
                            logger.info(f'Paper trade BLOCKED by scorecard SKIP: {sym} '
                                        f'grade={_sc_grade} score={score}')
                        else:
                            sl_pct = meta.get('sl', 7)
                            tp_pct = meta.get('tp', 14)
                            ok, result = _add_paper_trade(sym, p, score, sl_pct, tp_pct)
                            if ok:
                                logger.info(f'Paper trade added: {sym} @{p} score={score} '
                                            f'sc_grade={_sc_grade}')
    
            # Mã bị lọc vì score chưa đủ hoặc bucket skip
            if skipped:
                msg += '&#x23F3; <b>Cho ngưỡng score:</b>\n'
                for row in skipped:
                    sym, sc, min_sc, meta = row[0], row[1], row[2], row[3]
                    sc_adj      = row[4] if len(row) > 4 else sc
                    b_warns     = row[5] if len(row) > 5 else []
                    bucket_skip = row[6] if len(row) > 6 else None
                    if bucket_skip:
                        # Session 10: hiển thị lý do bucket anomaly (VND 75-84)
                        msg += (f' &#x26A0; {sym} ({meta["group"]}): '
                                f'Score={sc_adj} — {bucket_skip} (data-driven, Exp âm)\n')
                    elif sc_adj < sc:
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

        except Exception as _sig_err:
            import traceback as _tb
            logger.error('handle_signals uncaught: ' + str(_sig_err))
            logger.error(_tb.format_exc())
            send(
                '&#x274C; <b>/signals gặp lỗi:</b>\n'
                + str(_sig_err)[:150] + '\n'
                + '<i>Thử lại sau 30s hoặc dùng /analyze SYM để xem từng mã.</i>',
                chat_id
            )

    threading.Thread(target=_run, daemon=True).start()

def poll_updates():
    if not TOKEN:
        logger.error('Không co TOKEN')
        return

    # Khởi tạo DB table khi bot start
    _init_db()
    logger.info('Bot V6 S16 polling... (RS+Scanner+SharkV4+AutoShark ready)')
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
                    threading.Thread(target=handle_start, args=(cid,), daemon=True).start()
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

                elif cmd == '/scbscan':
                    threading.Thread(
                        target=handle_scbscan, args=(cid,),
                        daemon=True).start()

                elif cmd == '/scb':
                    _sym = parts[1].upper() if len(parts) > 1 else ''
                    if not _sym:
                        send('Cú pháp: <b>/scb SYM</b>\nVí dụ: <b>/scb BSR</b>\n\n'
                             '<i>Tính Score B realtime cho 1 mã — breakdown C1/C2/C3/C4 '
                             '+ hard skip status + verdict.</i>', cid)
                    else:
                        handle_scb(_sym, cid)

                elif cmd == '/ptreport':
                    # FIX (S16): truyền args để /ptreport scb/ml/open hoạt động
                    _pt_args = list(parts[1:])
                    threading.Thread(
                        target=handle_ptreport, args=(cid, _pt_args),
                        daemon=True).start()


                elif cmd == '/signals':
                    handle_signals(cid)  # spawns own _run() thread internally


                elif cmd == '/sectorbt':
                    threading.Thread(target=handle_sectorbt, args=(parts[1:], cid), daemon=True).start()
                elif cmd == '/sectorscan':
                    handle_sectorscan(parts[1:], cid)

                elif cmd == '/sbtbt':
                    handle_sbtbt(parts[1:], cid)

                elif cmd == '/mlbt':
                    threading.Thread(target=handle_mlbt, args=(parts[1:], cid), daemon=True).start()
                elif cmd == '/mlbtv2':
                    send('&#x26A0; /mlbtv2 da bi xoa. Dung <b>/mlbtv3</b> thay the (chay duoc moi ma).', cid)
                elif cmd == '/mlbtv3':
                    threading.Thread(target=handle_mlbtv3, args=(parts[1:], cid), daemon=True).start()
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

                elif cmd == '/ml_scan':
                    threading.Thread(
                        target=handle_ml_scan, args=(list(parts[1:]), cid),
                        daemon=True).start()


                elif cmd == '/bt':
                    arg = ' '.join(parts[1:]) if len(parts) > 1 else ''
                    threading.Thread(target=handle_bt, args=(arg, cid), daemon=True).start()
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

ML_SCAN_INTERVAL_MIN   = 10    # ML Momentum: 10 phút/lần + sleep(3s)/mã → 20 calls/phút peak

# ── Scanner ON/OFF control — dùng lệnh /scanner on/off <tên> ────────────────
# Tắt scanner để giải phóng quota khi cần chạy /mlbtv3 all hoặc backtest nặng
# Dùng: /scanner off all   → tắt hết trước khi chạy /mlbtv3 all
#        /scanner on all    → bật lại sau khi chạy xong
SCANNER_ENABLED = {
    'signal':   True,   # Signal MUA/BAN (10 phút/lần) — ~6 calls/lần
    'ml':       True,   # ML Momentum (10 phút/lần) — ~12 calls/lần
    'scb':      True,   # Score B scan (10 phút/lần) — 19 mã
}

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


# ══════════════════════════════════════════════════════════════════════════════
# SECTOR ROTATION SCANNER
# Quét top3 mạnh nhất mỗi ngành cuối phiên (15:05), track lịch sử 5 ngày,
# alert khi mã xuất hiện top3 ≥3/5 ngày + vol tăng.
#
# Scoring (0–100):
#   RS 5d vs ngành (40đ) — relative strength ngắn hạn
#   Volume trend  (35đ) — vol hôm nay vs TB20d ngành
#   MA50 slope    (25đ) — xu hướng trung hạn
# ══════════════════════════════════════════════════════════════════════════════

SECTOR_MAP = {
    'Ngân hàng': [
        'VCB','BID','CTG','MBB','TCB','VPB','ACB','HDB','LPB','OCB',
        'MSB','STB','SHB','NAB','SSB','BAB','PGB','KLB','ABB','BVB',
    ],
    'Chứng khoán': [
        'SSI','VND','HCM','VCI','CTS','BSI','MBS','VIX','FTS','ORS',
        'AGR','APS','BMS','TVS','VDS',
    ],
    'Thép - Vật liệu': [
        'HPG','HSG','NKG','TLH','SMC','DTL','VGS','POM','TVN','HMC',
        'BVG','HLA',
    ],
    'Bất động sản': [
        'VHM','VIC','NLG','DXG','KDH','PDR','DIG','CII','NVL','HDG',
        'AGG','BCG','DXS','HUT','LDG','SCR','TDH','IDC','KBC','NRC',
    ],
    'Bán lẻ - Tiêu dùng': [
        'MWG','FRT','PNJ','DGW','MCP','HAX','SVC','DAT',
    ],
    'Hóa chất - Phân bón': [
        'DGC','DCM','DPM','CSV','BFC','LAS','PME','SFG',
    ],
    'Dầu khí': [
        'GAS','PLX','PVD','PVS','PVT','BSR','PVC','PXS','CNG','PCG',
        'PHP','POW',
    ],
    'Điện - Năng lượng': [
        'PC1','REE','HDG','SHP','VSH','GEX','EVF','PPC','NT2','VSI',
        'HND','TBC',
    ],
    'Logistics - Cảng': [
        'GMD','HAH','VSC','STK','DVP','SGP','TMS','VOS','VTP',
    ],
    'Công nghệ': [
        'FPT','CMG','VGI','ICT','SAM','ELC','TST',
    ],
    'FMCG - Thực phẩm': [
        'VNM','MCH','SAB','QNS','KDC','MSN','ANV','FMC','VHC','MPC',
        'IDI','HVN',
    ],
    'Xây dựng - VLXD': [
        'CTD','HBC','VCG','FCN','LCG','HT1','BMP','DRC','PLC','CSV',
        'PHR',
    ],
}

# File lưu lịch sử top3 mỗi ngày
SECTOR_HISTORY_FILE = '/tmp/sector_top3_history.json'
SECTOR_HISTORY_DAYS = 5   # track 5 ngày
SECTOR_TOP3_ALERT_MIN_DAYS = 3   # xuất hiện ≥3/5 ngày → alert


def _load_sector_history():
    """Load lịch sử top3 từ file JSON."""
    import json as _json
    try:
        if os.path.exists(SECTOR_HISTORY_FILE):
            with open(SECTOR_HISTORY_FILE) as f:
                return _json.load(f)
    except Exception:
        pass
    return {}


def _save_sector_history(history):
    """Lưu lịch sử top3 vào file JSON."""
    import json as _json
    try:
        # Chỉ giữ SECTOR_HISTORY_DAYS ngày gần nhất
        dates = sorted(history.keys())
        if len(dates) > SECTOR_HISTORY_DAYS + 2:
            for old_date in dates[:-(SECTOR_HISTORY_DAYS + 2)]:
                del history[old_date]
        with open(SECTOR_HISTORY_FILE, 'w') as f:
            _json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f'sector history save: {e}')


def _calc_sector_rank_score(sym_data, sector_peer_data, vni_5d_ret=0.0):
    """
    Tính Sector Rank Score (0-100) cho 1 mã trong ngành.

    Components (tổng 100đ):
      C1: RS 5d vs TB ngành  30đ — mã mạnh hơn ngành bao nhiêu?
      C2: Vol vs TB ngành    25đ — tiền vào hơn ngành? (cứng: vol_ratio >= 1.2)
      C3: MA50 slope         20đ — xu hướng trung hạn
      C4: RS 5d vs VNI       25đ — mã khoẻ hơn thị trường chung không?

    vni_5d_ret: return 5 ngày của VNINDEX (%), fetch 1 lần bên ngoài vòng lặp.
    """
    score = 0
    bd    = {}

    # ── C1: RS 5d vs TB ngành (30đ) ────────────────────────────────────
    rs5_sym  = float(sym_data.get('rs_5d') or 0)
    peer_rs5 = [float(p.get('rs_5d') or 0) for p in sector_peer_data
                if p.get('rs_5d') is not None]
    sec_avg_rs5 = sum(peer_rs5) / len(peer_rs5) if peer_rs5 else 0
    rs5_diff = rs5_sym - sec_avg_rs5

    if   rs5_diff >= 4.0: c1 = 30
    elif rs5_diff >= 2.0: c1 = 22
    elif rs5_diff >= 0.5: c1 = 15
    elif rs5_diff >= -1.0: c1 = 7
    else:                  c1 = 0
    score += c1
    bd['rs5'] = {'sym': round(rs5_sym, 1), 'sec_avg': round(sec_avg_rs5, 1),
                 'diff': round(rs5_diff, 1), 'pts': c1}

    # ── C2: Volume vs TB ngành (25đ) ────────────────────────────────────
    vol_ratio_sym = float(sym_data.get('vol_ratio') or 1.0)
    peer_vr = [float(p.get('vol_ratio') or 1.0) for p in sector_peer_data
               if p.get('vol_ratio') is not None]
    sec_avg_vr = sum(peer_vr) / len(peer_vr) if peer_vr else 1.0

    if vol_ratio_sym < 1.2:
        c2 = 0
        c2_label = f'Vol {vol_ratio_sym:.1f}x < 1.2 min'
    else:
        vr_diff = vol_ratio_sym - sec_avg_vr
        if   vr_diff >= 0.8: c2 = 25
        elif vr_diff >= 0.4: c2 = 18
        elif vr_diff >= 0.0: c2 = 12
        else:                c2 = 5
        c2_label = f'Vol {vol_ratio_sym:.1f}x vs sec_avg {sec_avg_vr:.1f}x'
    score += c2
    bd['vol'] = {'ratio': round(vol_ratio_sym, 2), 'sec_avg': round(sec_avg_vr, 2),
                 'pts': c2, 'label': c2_label}

    # ── C3: MA50 slope (20đ) ────────────────────────────────────────────
    ma50_slope = float(sym_data.get('ma50_slope') or 0)
    peer_slopes = [float(p.get('ma50_slope') or 0) for p in sector_peer_data
                   if p.get('ma50_slope') is not None]
    sec_avg_slope = sum(peer_slopes) / len(peer_slopes) if peer_slopes else 0
    slope_diff = ma50_slope - sec_avg_slope

    if   ma50_slope >= 1.0 and slope_diff >= 0.3: c3 = 20
    elif ma50_slope >= 0.5:                        c3 = 14
    elif ma50_slope >= 0.0:                        c3 = 7
    else:                                          c3 = 0
    score += c3
    bd['ma50_slope'] = {'sym': round(ma50_slope, 2), 'sec_avg': round(sec_avg_slope, 2),
                        'diff': round(slope_diff, 2), 'pts': c3}

    # ── C4: RS 5d vs VNI (25đ) ──────────────────────────────────────────
    # Đo mã khoẻ hơn thị trường chung không?
    # vni_5d_ret = 0.0 khi không fetch được → C4 neutral (không phạt)
    rs5_vs_vni = rs5_sym - vni_5d_ret

    if   rs5_vs_vni >= 3.0: c4 = 25   # vượt trội vs VNI
    elif rs5_vs_vni >= 1.0: c4 = 18
    elif rs5_vs_vni >= 0.0: c4 = 12   # ít nhất bằng VNI
    elif rs5_vs_vni >= -1.0: c4 = 5   # yếu hơn chút
    else:                    c4 = 0   # thua VNI rõ ràng
    score += c4
    bd['rs_vni'] = {'sym_5d': round(rs5_sym, 1), 'vni_5d': round(vni_5d_ret, 1),
                    'diff': round(rs5_vs_vni, 1), 'pts': c4}

    score = min(100, max(0, score))
    return score, bd

def run_sector_scan(save_history=True, chat_id=None, filter_sectors=None):
    """
    Quét toàn bộ SECTOR_MAP, tính Sector Rank Score mỗi ngành.

    Mới S13: fetch VNI 5d return 1 lần trước vòng lặp ngành để tính C4.

    Args:
        save_history : lưu top3 hôm nay vào history file
        chat_id      : ping tiến độ per-ngành lên Telegram nếu có
        filter_sectors: set tên ngành muốn quét (None = tất cả)
    Returns:
        dict {sector_name: [{'sym', 'score', 'rank', 'bd', ...}, ...]}
    """
    import time as _time
    NL    = chr(10)
    today = datetime.now(VN_TZ).strftime('%Y-%m-%d')

    # ── Fetch VNI 5d return 1 lần trước khi quét ────────────────────────
    vni_5d_ret = 0.0
    try:
        import backtest as _bt
        df_vni, _ = _bt.load_data('VNINDEX', days=10)
        if df_vni is not None and len(df_vni) >= 6:
            cc = _bt.find_col(df_vni, ['close', 'closeprice', 'close_price'])
            if cc:
                closes_vni = df_vni[cc].values
                vni_5d_ret = float((closes_vni[-1] / closes_vni[-6] - 1) * 100)
                logger.info(f'VNI 5d return: {vni_5d_ret:+.2f}%')
    except Exception as e:
        logger.warning(f'run_sector_scan: VNI 5d fetch failed ({e}), C4 neutral')

    results  = {}
    today_top3 = {}

    sector_items = [
        (sec, syms) for sec, syms in SECTOR_MAP.items()
        if filter_sectors is None or sec in filter_sectors
    ]
    n_sectors = len(sector_items)

    for sec_idx, (sector, syms) in enumerate(sector_items, 1):
        if chat_id:
            send(
                f'⏳ [{sec_idx}/{n_sectors}] <b>{sector}</b> — đang tải {len(syms)} mã...',
                chat_id
            )

        sector_data = {}
        for sym in syms:
            try:
                d = call_api_fast('/api/analyze/' + sym, timeout=20)
                if d and 'score' in d and d.get('price', 0) > 0:
                    sector_data[sym] = d
                _time.sleep(1.1)
            except Exception:
                _time.sleep(1.1)

        if len(sector_data) < 2:
            if chat_id:
                send(f'  ↳ {sector}: skip ({len(sector_data)} mã có data)', chat_id)
            continue

        peer_list = list(sector_data.values())

        scored = []
        for sym, d in sector_data.items():
            peers_excl = [p for p in peer_list if p is not d]
            sc, bd = _calc_sector_rank_score(d, peers_excl, vni_5d_ret=vni_5d_ret)
            scored.append({
                'sym':        sym,
                'score':      sc,
                'bd':         bd,
                'price':      d.get('price', 0),
                'rs5':        bd['rs5']['sym'],
                'rs5_diff':   bd['rs5']['diff'],
                'rs_vni':     bd['rs_vni']['diff'],
                'vol_ratio':  bd['vol']['ratio'],
                'ma50_slope': bd['ma50_slope']['sym'],
                'vol_ok':     bd['vol']['ratio'] >= 1.2,
            })

        scored.sort(key=lambda x: -x['score'])
        for i, item in enumerate(scored):
            item['rank']  = i + 1
            item['total'] = len(scored)

        results[sector]    = scored
        today_top3[sector] = [s['sym'] for s in scored[:3]]

        # Ping top3 ngành vừa xong
        if chat_id:
            top3_lines = []
            for r in scored[:3]:
                vol_ico = '🔥' if r['vol_ratio'] >= 1.5 else ('📈' if r['vol_ratio'] >= 1.2 else '➡')
                slp_ico = '↗' if r['ma50_slope'] >= 0.5 else ('→' if r['ma50_slope'] >= 0 else '↘')
                vni_ico = '💪' if r['rs_vni'] >= 1.0 else ('🟰' if r['rs_vni'] >= 0 else '⚠')
                top3_lines.append(
                    f'  {r["rank"]}. <b>{r["sym"]}</b> ScR={r["score"]}'
                    f' | RS↕ngành={r["rs5_diff"]:+.1f}%'
                    f' | RS↕VNI={r["rs_vni"]:+.1f}%{vni_ico}'
                    f' | Vol{vol_ico}{r["vol_ratio"]:.1f}x'
                    f' | MA50{slp_ico}{r["ma50_slope"]:+.1f}%'
                )
            send(
                f'  ↳ <b>{sector}</b> ({len(sector_data)}/{len(syms)} mã OK) — Top3:' + NL
                + NL.join(top3_lines),
                chat_id
            )

    if save_history and today_top3:
        history = _load_sector_history()
        history[today] = today_top3
        _save_sector_history(history)
        logger.info(f'Sector history saved: {today} | VNI_5d={vni_5d_ret:+.2f}%')

    return results

def check_sector_alerts(results):
    """
    Kiểm tra xem mã nào xuất hiện top3 ngành ≥3/5 ngày + vol >= 1.2.
    Returns: list of alert dicts
    """
    history  = _load_sector_history()
    dates    = sorted(history.keys())[-SECTOR_HISTORY_DAYS:]  # 5 ngày gần nhất
    alerts   = []

    for sector, ranked in results.items():
        top3_today = {s['sym'] for s in ranked[:3]}

        for sym_data in ranked[:6]:   # check top6 để không bỏ sót
            sym = sym_data['sym']
            if not sym_data.get('vol_ok'):
                continue   # vol < 1.2 → không alert

            # Đếm số ngày sym xuất hiện top3 trong 5 ngày gần nhất (bao gồm hôm nay)
            count = 0
            for d in dates:
                if sym in history.get(d, {}).get(sector, []):
                    count += 1
            # Cộng hôm nay nếu chưa save
            if sym in top3_today:
                count += 1

            if count >= SECTOR_TOP3_ALERT_MIN_DAYS:
                alerts.append({
                    'sym':     sym,
                    'sector':  sector,
                    'count':   count,
                    'rank':    sym_data['rank'],
                    'score':   sym_data['score'],
                    'rs5':     sym_data['rs5'],
                    'rs5_diff':sym_data['rs5_diff'],
                    'vol':     sym_data['vol_ratio'],
                    'slope':   sym_data['ma50_slope'],
                })

    # Sort: count giảm dần, score giảm dần
    alerts.sort(key=lambda x: (-x['count'], -x['score']))
    return alerts


def format_sector_scan_msg(results, alerts):
    """Format output sector scan cho Telegram."""
    NL  = chr(10)
    now = datetime.now(VN_TZ).strftime('%d/%m %H:%M')

    # ── Header alerts (nếu có) ───────────────────────────────────────
    msg = f'🏆 <b>Sector Rotation Scan — {now}</b>' + NL

    if alerts:
        msg += NL + '🔔 <b>ALERT — Mạnh liên tục ≥3/5 ngày + Vol≥1.2x:</b>' + NL
        msg += '─' * 28 + NL
        for a in alerts:
            msg += (
                f'  🔥 <b>{a["sym"]}</b> [{a["sector"]}]' + NL
                + f'     Top{a["rank"]} ngành | {a["count"]}/{SECTOR_HISTORY_DAYS}ngày'
                + f' | ScR={a["score"]} | RS5={a["rs5"]:+.1f}%'
                + f'(+{a["rs5_diff"]:+.1f}% vs ngành)'
                + f' | Vol={a["vol"]:.1f}x | MA50↗{a["slope"]:+.1f}%' + NL
            )
    else:
        msg += NL + '<i>Chưa có mã nào đủ điều kiện alert hôm nay.</i>' + NL

    # ── Top3 mỗi ngành ───────────────────────────────────────────────
    msg += NL + '<b>Top 3 mỗi ngành hôm nay:</b>' + NL
    msg += '─' * 28 + NL

    for sector, ranked in results.items():
        top3 = ranked[:3]
        if not top3:
            continue
        sec_line = f'<b>{sector}</b>' + NL
        for r in top3:
            vol_icon = '🔥' if r['vol_ratio'] >= 1.5 else ('📈' if r['vol_ratio'] >= 1.2 else '➡')
            slope_icon = '↗' if r['ma50_slope'] >= 0.5 else ('→' if r['ma50_slope'] >= 0 else '↘')
            alert_flag = '⭐' if any(a['sym'] == r['sym'] for a in alerts) else ''
            vni_diff = r.get('rs_vni', 0)
            vni_icon = '💪' if vni_diff >= 1.0 else ('🟰' if vni_diff >= 0 else '⚠')
            sec_line += (
                f'  {r["rank"]}. {alert_flag}<b>{r["sym"]}</b>'
                + f' ScR={r["score"]}'
                + f' | RS↕ngành={r["rs5_diff"]:+.1f}%'
                + f' | RS↕VNI={vni_diff:+.1f}%{vni_icon}'
                + f' | Vol{vol_icon}{r["vol_ratio"]:.1f}x'
                + f' | MA50{slope_icon}{r["ma50_slope"]:+.1f}%' + NL
            )
        msg += sec_line

    msg += NL + '<i>ScR=Sector Rank Score | RS↕ngành=RS 5d vs TB ngành | RS↕VNI=RS 5d vs VNINDEX | Vol vs TB20d ngành</i>'
    msg += NL + '<i>Dùng /sectorscan để quét thủ công bất kỳ lúc nào</i>'
    return msg



def handle_sectorbt(args, chat_id):
    """
    /sectorbt              — Backtest Sector Rotation toàn bộ 12 ngành
    /sectorbt sl=7 tp=15 hold=10 thresh=55 — Custom params
    /sectorbt fast         — Nhanh: chỉ 3 năm gần nhất
    """
    NL = chr(10)
    args = [a.strip() for a in (args or []) if a.strip()]

    # Parse params
    sl       = 0.07
    tp       = 0.15
    hold     = 10
    thresh   = 55
    days     = 2520   # ~7 năm
    fast_mode = False

    for a in args:
        al = a.lower()
        try:
            if al == 'fast':
                fast_mode = True
                days = 1080   # ~3 năm
            elif al.startswith('sl='):     sl     = float(al[3:]) / 100
            elif al.startswith('tp='):     tp     = float(al[3:]) / 100
            elif al.startswith('hold='):   hold   = int(al[5:])
            elif al.startswith('thresh='): thresh = int(al[7:])
            elif al.startswith('days='):   days   = int(al[5:])
        except Exception:
            pass

    n_syms = sum(len(v) for v in SECTOR_MAP.values())
    yr_str  = '3 năm' if fast_mode else '7 năm'
    est_min = 8 if fast_mode else 20   # parallel load estimate

    send(
        f'🏭 <b>Sector Rotation Backtest</b>' + NL
        + f'12 ngành | {n_syms} mã | {yr_str} | hold={hold}d SL={sl*100:.0f}% TP={tp*100:.0f}%' + NL
        + f'Ngưỡng ScR_avg ngành ≥ {thresh} | BEAR regime → skip' + NL
        + f'⏳ Đang load data (~{est_min} phút)...',
        chat_id
    )

    def _run():
        try:
            import backtest as bt

            result = bt.run_sector_backtest(
                sector_map         = dict(SECTOR_MAP),
                sl                 = sl,
                tp                 = tp,
                hold_days          = hold,
                scr_avg_threshold  = thresh,
                days               = days,
                bear_skip          = True,
                shock_skip_pct     = -3.0,
                verbose            = False,
            )

            if 'error' in result:
                send(f'❌ {result["error"]}', chat_id)
                return

            stats        = result['stats']
            yearly       = result.get('yearly', {})
            sector_stats = result.get('sector_stats', {})
            params       = result.get('params', {})
            n_loaded     = params.get('n_syms_loaded', '?')

            # ── Header tổng kết ───────────────────────────────────────────
            n     = stats['total']
            wr    = stats['win_rate']
            avg   = stats['avg_pnl']
            pf    = stats['profit_factor']
            pf_s  = f'{pf:.2f}' if pf != float('inf') else '∞'
            ci_lo = stats.get('ci_low', 0)
            ci_hi = stats.get('ci_high', 0)
            n_tp  = stats.get('tp', 0)
            n_sl  = stats.get('sl', 0)
            n_exp = stats.get('expired', 0)

            # Verdict
            if wr >= 55 and avg >= 1.0 and pf >= 1.5:
                verdict = '✅ <b>CÓ EDGE</b> — Chiến lược hoạt động tốt'
            elif wr >= 50 and avg >= 0.5:
                verdict = '🟡 <b>EDGE YẾU</b> — Chấp nhận nhưng chưa mạnh'
            elif avg < 0:
                verdict = '❌ <b>KHÔNG CÓ EDGE</b> — Chiến lược lỗ'
            else:
                verdict = '⚠ <b>TRUNG BÌNH</b> — Cần thêm filter'

            msg_header = (
                f'📊 <b>Sector Rotation BT — Kết quả {yr_str}</b>' + NL
                + '─' * 32 + NL
                + f'Params: SL={sl*100:.0f}% TP={tp*100:.0f}% Hold={hold}d ScR_avg≥{thresh}' + NL
                + f'Data: {n_loaded} mã loaded | BEAR regime & VNI shock(-3%) → skip' + NL + NL
                + f'<b>Tổng lệnh:</b> {n}L' + NL
                + f'<b>Win Rate:</b> {wr:.1f}% (CI: {ci_lo}–{ci_hi}%)' + NL
                + f'<b>Avg PnL:</b> {avg:+.2f}%  |  <b>PF:</b> {pf_s}' + NL
                + f'<b>Exit:</b> TP={n_tp}L ({n_tp/n*100:.0f}%) | '
                + f'SL={n_sl}L ({n_sl/n*100:.0f}%) | HK={n_exp}L ({n_exp/n*100:.0f}%)' + NL + NL
                + verdict
            )
            send(msg_header, chat_id)

            # ── Yearly breakdown ──────────────────────────────────────────
            if yearly:
                yr_lines = ['📅 <b>Breakdown theo năm:</b>', '─' * 28]
                for yr in sorted(yearly.keys()):
                    y = yearly[yr]
                    pf_y = f'{y["pf"]:.2f}' if y["pf"] != float('inf') else '∞'
                    icon = '✅' if y['avg_pnl'] > 0.5 else ('🟡' if y['avg_pnl'] > 0 else '❌')
                    yr_lines.append(
                        f'{icon} <b>{yr}</b>: {y["n"]}L | '
                        + f'WR={y["wr"]:.0f}% | Avg={y["avg_pnl"]:+.2f}% | PF={pf_y}'
                    )
                send(NL.join(yr_lines), chat_id)

            # ── Per-sector stats ──────────────────────────────────────────
            if sector_stats:
                # Sort: avg_pnl cao nhất lên đầu
                sorted_secs = sorted(sector_stats.items(),
                                     key=lambda x: -x[1]['avg_pnl'])
                sec_lines = ['🏭 <b>Breakdown theo ngành:</b>', '─' * 28]
                for sec, s in sorted_secs:
                    pf_s2 = f'{s["pf"]:.2f}' if s["pf"] != float('inf') else '∞'
                    icon  = '✅' if s['avg_pnl'] > 0.5 else ('🟡' if s['avg_pnl'] > 0 else '❌')
                    sec_lines.append(
                        f'{icon} <b>{sec}</b>' + NL
                        + f'   {s["n"]}L | WR={s["wr"]:.0f}% | Avg={s["avg_pnl"]:+.2f}% | PF={pf_s2}'
                        + f' | TP={s["tp"]} SL={s["sl"]} HK={s["expired"]}'
                    )

                # Gửi theo chunk
                chunk = ''
                for line in sec_lines:
                    if len(chunk) + len(line) + 1 > 3800:
                        send(chunk, chat_id)
                        chunk = line + NL
                    else:
                        chunk += line + NL
                if chunk.strip():
                    send(chunk, chat_id)

            # ── Top/Bottom trades ─────────────────────────────────────────
            df_t = result['trades']
            if len(df_t) > 0:
                top5    = df_t.nlargest(5, 'pnl')[['date','sym','sector','pnl','reason','scr_avg']]
                bot5    = df_t.nsmallest(5, 'pnl')[['date','sym','sector','pnl','reason','scr_avg']]
                tb_lines = ['🏆 <b>Top 5 lệnh tốt nhất:</b>']
                for _, r in top5.iterrows():
                    tb_lines.append(f'  ✅ {r["sym"]} {r["date"]} {r["pnl"]:+.1f}% [{r["reason"]}] ScR_avg={r["scr_avg"]:.0f}')
                tb_lines.append(NL + '❌ <b>Top 5 lệnh tệ nhất:</b>')
                for _, r in bot5.iterrows():
                    tb_lines.append(f'  ❌ {r["sym"]} {r["date"]} {r["pnl"]:+.1f}% [{r["reason"]}] ScR_avg={r["scr_avg"]:.0f}')
                send(NL.join(tb_lines), chat_id)

            send(
                f'✅ <b>Sector BT xong.</b>' + NL
                + 'Dùng /sectorbt fast để test nhanh 3 năm, '
                + 'hoặc /sectorbt thresh=60 để tăng ngưỡng ngành.',
                chat_id
            )

        except Exception as e:
            import traceback
            logger.error(f'handle_sectorbt: {traceback.format_exc()}')
            send(f'❌ Lỗi Sector BT: {str(e)[:150]}', chat_id)

    threading.Thread(target=_run, daemon=True).start()


def handle_sectorscan(args, chat_id):
    """
    /sectorscan             — Quet top3 manh nhat moi nganh + alert
    /sectorscan sector Ngan hang — chi xem 1 nganh
    /sectorscan history     — xem lich su 5 ngay
    """
    NL   = chr(10)
    args = [a.strip() for a in (args or []) if a.strip()]

    # ── History mode ─────────────────────────────────────────────────
    if args and args[0].lower() == 'history':
        history = _load_sector_history()
        if not history:
            send('Chua co lich su — chay /sectorscan truoc.', chat_id)
            return
        dates = sorted(history.keys())[-5:]
        msg = '\U0001f4c5 <b>Sector History — 5 ngay gan nhat</b>' + NL + NL
        for d in reversed(dates):
            msg += f'<b>{d}</b>' + NL
            for sec, top3 in history[d].items():
                msg += f'  {sec}: {" > ".join(top3)}' + NL
            msg += NL
        send(msg, chat_id)
        return

    # ── Single sector mode ───────────────────────────────────────────
    filter_sectors = None
    filter_label   = 'toan bo nganh'
    if len(args) >= 2 and args[0].lower() == 'sector':
        kw = ' '.join(args[1:]).lower()
        matched_names = [k for k in SECTOR_MAP if kw in k.lower()]
        if not matched_names:
            send(
                f'\u274c Khong tim thay nganh "{kw}".' + NL
                + 'Nganh co san: ' + ', '.join(SECTOR_MAP.keys()),
                chat_id
            )
            return
        filter_sectors = set(matched_names)
        filter_label   = ', '.join(matched_names)

    n_syms  = sum(len(v) for k, v in SECTOR_MAP.items()
                  if filter_sectors is None or k in filter_sectors)
    n_sec   = len(filter_sectors) if filter_sectors else len(SECTOR_MAP)
    est_min = max(1, round(n_syms * 1.2 / 60))   # ~1.2s/mã

    send(
        f'\u23f3 <b>Sector Scan bat dau</b> — {filter_label}' + NL
        + f'{n_syms} ma / {n_sec} nganh — uoc tinh ~{est_min} phut' + NL
        + '<i>Se ping tung nganh khi xong, tong ket o cuoi.</i>',
        chat_id
    )

    def _run():
        try:
            results = run_sector_scan(
                save_history=(filter_sectors is None),
                chat_id=chat_id,
                filter_sectors=filter_sectors,
            )
            if not results:
                send('\u274c Khong co nganh nao du data de hien thi.', chat_id)
                return

            alerts  = check_sector_alerts(results)
            msg     = format_sector_scan_msg(results, alerts)

            # Chia nho neu qua dai
            if len(msg) > 4000:
                chunk = ''
                for line in msg.split(NL):
                    if len(chunk) + len(line) + 1 > 3800:
                        if chunk.strip():
                            send(chunk, chat_id)
                        chunk = line + NL
                    else:
                        chunk += line + NL
                if chunk.strip():
                    send(chunk, chat_id)
            else:
                send(msg, chat_id)

        except Exception as e:
            import traceback
            logger.error(f'sectorscan: {traceback.format_exc()}')
            send(f'\u274c Loi sector scan: {str(e)[:150]}', chat_id)

    threading.Thread(target=_run, daemon=True).start()


_ml_cooldown_state = {}  # ML scan cooldown state (replaces _last_ma_alerts)

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

            # ── Sector scan 15:05 — lưu history + alert ─────────────────────
            if weekday < 5 and h == 15 and m == 5 \
                    and not getattr(auto_alert_scanner, '_sector_done_today', '') == datetime.now(VN_TZ).strftime('%Y-%m-%d'):
                auto_alert_scanner._sector_done_today = datetime.now(VN_TZ).strftime('%Y-%m-%d')
                logger.info('Sector scan 15:05')
                def _run_sector_bg():
                    try:
                        _sec_results = run_sector_scan(save_history=True)
                        _sec_alerts  = check_sector_alerts(_sec_results)
                        if _sec_alerts:  # Chỉ broadcast khi có alert
                            _sec_msg = format_sector_scan_msg(_sec_results, _sec_alerts)
                            broadcast(_sec_msg)
                            logger.info(f'Sector alerts: {len(_sec_alerts)} mã')
                        else:
                            logger.info('Sector scan: no alerts today')
                    except Exception as _se:
                        logger.error(f'Sector auto scan: {_se}')
                threading.Thread(target=_run_sector_bg, daemon=True).start()

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
                    # FIX (S16): Update paper trades sau mỗi phiên
                    # Gọi trong thread riêng vì có API calls per-symbol
                    def _run_paper_update():
                        try:
                            n = _update_paper_trades()
                            if n > 0:
                                logger.info(f'EOD paper update: {n} lệnh closed')
                        except Exception as _pe:
                            logger.warning(f'EOD paper update error: {_pe}')
                    threading.Thread(target=_run_paper_update, daemon=True).start()
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
                        sym_last     = _ml_cooldown_state.setdefault(sym, {})
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
                                # Auto paper trade log
                                if price > 0:
                                    _ml_ok, _ = _add_paper_trade(
                                        sym, price,
                                        score=int(data.get('score', 0) or 0),
                                        sl_pct=5.5, tp_pct=16.0,
                                        source='ML',
                                        extra={
                                            'ml_tier':   _ml_tier,
                                            'ml_grade':  _ms.get('grade', ''),
                                            'ml_score':  _ms.get('score', 0),
                                            'pct52w':    _ms.get('pct52w', 0),
                                            'penalties': _ms.get('penalties', []),
                                        }
                                    )
                                    if _ml_ok:
                                        logger.info(f'ML paper trade logged: {sym} tier={_ml_tier}')
                                time.sleep(2)
                    except Exception as e:
                        logger.warning(f'ML scan {sym}: {e}')
                pass  # MA alerts removed

            # ── SCB MOMENTUM SCAN — 10 phút/lần, sleep(2s)/mã ───────────────
            # 19 mã x sleep 2s = ~38s | dùng lại _scb_format_signal()
            scb_scan_slot = total_min // SCAN_INTERVAL_MIN
            if (weekday < 5 and in_session
                    and scb_scan_slot != getattr(auto_alert_scanner, '_last_scb_slot', -1)
                    and not in_ato and not in_atc
                    and SCANNER_ENABLED.get('scb', True)):
                auto_alert_scanner._last_scb_slot = scb_scan_slot
                logger.info('ScB scan (%d ma): ' % len(SCB_WATCHLIST)
                            + now.strftime('%H:%M'))
                try:
                    _vni_data = call_api('/api/market') or {}
                    _vni_chg  = float(
                        (_vni_data.get('VNINDEX') or {}).get('change_pct', 0) or 0
                    )
                except Exception:
                    _vni_chg = 0.0

                for _scb_sym in SCB_WATCHLIST:
                    try:
                        time.sleep(2)
                        _scb_raw = call_api('/api/analyze/' + _scb_sym)
                        if not _scb_raw or not isinstance(_scb_raw, dict):
                            continue
                        _scb_score_a  = int(_scb_raw.get('score', 0) or 0)
                        _scb_score_b, _, _ = calc_score_b(_scb_raw)
                        if _scb_score_b < SCB_SCORE_B_MIN:
                            continue
                        _scb_ma20d = float(_scb_raw.get('dist_ma20_pct') or
                                           _scb_raw.get('ma20_dist', 0) or 0)
                        _scb_vol   = float(_scb_raw.get('vol_ratio', 1.0) or 1.0)
                        _scb_vni   = float(_scb_raw.get('vni_change_pct') or _vni_chg)
                        _verdict, _scb_msg = _scb_format_signal(
                            _scb_sym, _scb_score_a, _scb_score_b,
                            _scb_vni, _scb_ma20d, _scb_vol
                        )
                        if _verdict in ('GO', 'CAUTION'):
                            # Cooldown 90 phút per mã per chiều
                            _cooldown_key = 'scb_' + _scb_sym
                            _last_scb = _last_alerts.get(_cooldown_key)
                            if _last_scb and (time.time() - _last_scb[1]) < 5400:
                                continue
                            _last_alerts[_cooldown_key] = (_verdict, time.time())
                            broadcast(_scb_msg)
                            logger.info(f'ScB alert [{_verdict}]: {_scb_sym}'
                                        f' ScA={_scb_score_a} ScB={_scb_score_b}')
                            # Auto paper trade log — GO và CAUTION
                            if _verdict == 'GO':
                                _scb_price = float(_scb_raw.get('price', 0) or 0)
                                if _scb_price > 0:
                                    _scb_bt = SCB_BT_STATS.get(_scb_sym, {})
                                    _apt_ok, _ = _add_paper_trade(
                                        _scb_sym, _scb_price, _scb_score_a,
                                        sl_pct=7.0, tp_pct=14.0,
                                        source='ScB',
                                        extra={
                                            'score_b':   _scb_score_b,
                                            'vni_chg':   round(_vni_chg, 2),
                                            'ma20_dist': round(_scb_ma20d, 2),
                                            'vol_ratio': round(_scb_vol, 2),
                                            'bt_exp':    _scb_bt.get('exp', 0),
                                            'bt_pf':     _scb_bt.get('pf', 0),
                                            'bt_wr':     _scb_bt.get('wr', 0),
                                            'tier':      'A' if _scb_sym in SCB_WATCHLIST_TIER_A else 'B',
                                        }
                                    )
                                    if _apt_ok:
                                        logger.info(f'ScB auto paper logged: {_scb_sym} @{_scb_price}')
                            # FIX: Log CAUTION với source='ScB_C' để track Tier B + VNI FLAT signals
                            elif _verdict == 'CAUTION':
                                _scb_price_c = float(_scb_raw.get('price', 0) or 0)
                                if _scb_price_c > 0:
                                    _scb_bt_c = SCB_BT_STATS.get(_scb_sym, {})
                                    _apt_c, _ = _add_paper_trade(
                                        _scb_sym, _scb_price_c, _scb_score_a,
                                        sl_pct=7.0, tp_pct=14.0,
                                        source='ScB_C',
                                        extra={
                                            'score_b':   _scb_score_b,
                                            'verdict':   'CAUTION',
                                            'vni_chg':   round(_vni_chg, 2),
                                            'ma20_dist': round(_scb_ma20d, 2),
                                            'vol_ratio': round(_scb_vol, 2),
                                            'bt_exp':    _scb_bt_c.get('exp', 0),
                                            'bt_pf':     _scb_bt_c.get('pf', 0),
                                            'bt_wr':     _scb_bt_c.get('wr', 0),
                                            'tier':      'A' if _scb_sym in SCB_WATCHLIST_TIER_A else 'B',
                                        }
                                    )
                                    if _apt_c:
                                        logger.info(f'ScB_C auto paper logged: {_scb_sym} @{_scb_price_c}')
                    except Exception as _se:
                        logger.warning(f'ScB auto scan {_scb_sym}: {_se}')


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
