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
    # Thử PostgreSQL trước
    conn = _get_db_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute('SELECT chat_id FROM subscribers')
            rows = cur.fetchall()
            for r in rows:
                subs.add(str(r[0]))
            cur.close()
            conn.close()
        except Exception as e:
            err = str(e)
            logger.warning('DB load failed: ' + err)
            conn.close()
            # Auto-init table nếu chưa tồn tại
            if 'does not exist' in err or 'relation' in err:
                logger.info('DB: table missing, running init...')
                _init_db()
    conn = _get_db_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute('SELECT chat_id FROM subscribers')
            for row in cur.fetchall():
                subs.add(str(row[0]))
            cur.close()
            conn.close()
            return subs
        except Exception as e:
            logger.warning('DB load failed: ' + str(e))
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
    SETTLEMENT_DAYS, SYMBOL_CONFIG, SIGNALS_WATCHLIST,
    get_sl_tp, get_sl_tp_pct, get_min_score, get_wf_verdict,
    MIN_SCORE_BUY,
)

API_URL = os.environ.get('API_BASE_URL', 'http://localhost:8080')

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

# ── SL/TP config cho mã ngoài WATCHLIST_META ────────────────────────────────
# Dùng get_sl_tp_pct() từ config.py thay vì hardcode bảng này
# Giữ lại để tương thích với code cũ — tự động build từ SYMBOL_CONFIG
SYMBOL_SL_TP_CONFIG = {
    sym: {'sl_pct': int(cfg['sl']*100), 'tp_pct': int(cfg['tp']*100)}
    for sym, cfg in SYMBOL_CONFIG.items()
    if sym not in SIGNALS_WATCHLIST
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
        return False, 'Da co lenh OPEN cho ' + symbol + ' hom nay'

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





def send(text, chat_id=None):
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
        try:
            r = requests.post(
                'https://api.telegram.org/bot' + TOKEN + '/sendMessage',
                json={'chat_id': cid, 'text': chunk, 'parse_mode': 'HTML'},
                timeout=10
            )
            if r.status_code != 200:
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
    #   /signals    : 30s — gọi cache sẵn, nếu cache miss thì tự compute nhanh
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
                    t = 55   # FIX: tăng từ 25s → 55s (FA compute + rate limit wait)
                elif '/signals' in endpoint:
                    t = 30
                else:
                    t = 15
            else:
                if '/analyze/' in endpoint or '/fairvalue/' in endpoint:
                    t = 90
                elif '/signals' in endpoint:
                    t = 45
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
                ' 🎯 <b>Vao lenh toi uu:</b>' + NL
                + '   Dat Limit: <b>' + f'{entry_opt:,.0f}' + 'd</b>'
                + ' ~ ' + f'{entry_max:,.0f}' + 'd' + NL
                + '   (' + entry_lbl + ')' + NL
                + '   Hoac mua ngay: ' + f'{price:,.0f}' + 'd (chac khop, kem dep hon)' + NL
            )
        else:
            entry_line = (
                ' 🎯 <b>Vao lenh: Mua ngay ' + f'{price:,.0f}' + 'd</b>' + NL
                + '   (HT rat gan, khong can cho pullback)' + NL
            )

        return (
            entry_line
            + ' 🛑 Stop Loss : <b>' + f'{sl:,.0f}' + 'd</b> (' + sl_lbl + ')' + NL
            + ' 💰 Chot loi  : <b>' + f'{tp:,.0f}' + 'd</b> (' + tp_lbl + ')' + NL
            + NL
        )
    elif action == 'BAN':
        NL = chr(10)
        return (
            ' Nen ban o : ' + f'{price:,.0f}' + 'd (gia hien tai)' + NL
            + ' Vung mua lai: ' + f'{tp:,.0f}' + 'd (vung ho tro gan nhat)' + NL
            + ' Neu da mua : Cat lo neu gia tiep tuc giam them -7%' + NL + NL
        )
    else:
        NL = chr(10)
        return (
            ' Theo doi vung: ' + f'{sl:,.0f}' + 'd - ' + f'{tp:,.0f}' + 'd' + NL
            + ' Chua du tin hieu de vao lenh' + NL + NL
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
            lines += '\n<i>Nguong MUA cua ' + sym + ': &gt;= ' + str(score_min) + ' (tu backtest)</i>\n'
    elif not is_watchlist:
        lines += '\n<i>Nguong MUA mac dinh: &gt;= 65 (chua backtest per-symbol)</i>\n'

    lines += build_action_lines(data)
    lines += '<i>Score A: ky thuat | Score A+B: tong hop voi dieu kien TT VN</i>\n'
    lines += '<i>Chi mang tinh tham khao, khong phai tu van dau tu</i>'
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
        '<b>⏰ Dong tien 1H:</b>\n'
        ' ' + emoji + ' ' + escape_html(msg) + '\n'
        '<i>(Chi tham khao — khong anh huong score 1D)</i>\n\n'
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
            return '<b>📊 Dinh gia co ban:</b>\n <i>Khong tinh duoc: ' + escape_html(err[:60]) + '</i>\n\n'
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
        'UNDERVALUED': 'DANG RE (duoi vung hop ly)',
        'FAIR':        'GIA HOP LY',
        'OVERVALUED':  'DANG DAT (tren vung hop ly)',
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
        ta_fa_note = ' ⚠ CANH BAO: KT=MUA nhung gia dang dat hon fair value ' + upside_s + '\n'
    elif ta_action == 'BAN' and valuation == 'UNDERVALUED':
        ta_fa_note = ' ⚠ CANH BAO: KT=BAN nhung gia dang re hon fair value ' + upside_s + '\n'

    lines = (
        '<b>📊 Dinh gia co ban (' + escape_html(method) + '):</b>\n'
        ' Vung gia hop ly: <b>' + f'{fair_low:,.0f}' + 'd — ' + f'{fair_high:,.0f}' + 'd</b>\n'
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
    lines += '<i>Cap nhat 1 lan/ngay luc 8:30 | Khong phai tu van dau tu</i>\n\n'
    return lines


def handle_fv(symbol, chat_id):
    """
    /fv VCB — Xem fair value chi tiết của một mã, refresh cache.
    """
    send('📊 Dang tinh <b>Fair Value</b> cho <b>' + symbol + '</b>...', chat_id)

    def run():
        try:
            fv = call_api('/api/fairvalue/' + symbol)
            if not fv:
                send('❌ ' + symbol + ': Khong ket noi duoc API', chat_id)
                return
            if not fv.get('ok'):
                err = fv.get('error', 'Khong ro loi')
                send('❌ ' + symbol + ': ' + escape_html(err[:200]), chat_id)
                return

            val_emoji = {
                'UNDERVALUED': '🟢',
                'FAIR':        '🟡',
                'OVERVALUED':  '🔴',
            }.get(fv.get('valuation', ''), '❓')

            details = fv.get('details', {})
            detail_lines = ''
            for k, v in details.items():
                if v and v != 0:
                    if isinstance(v, (int, float)) and v > 1000:
                        detail_lines += f'  {k}: {v:,.0f}d\n'
                    else:
                        detail_lines += f'  {k}: {v}\n'

            upside     = fv.get('discount', 0)   # Thực ra là upside% từ giá lên FV
            upside_s   = ('+' if upside >= 0 else '') + f'{upside:.1f}%'
            upside_lbl = 'upside' if upside >= 0 else 'premium so voi FV'
            note       = fv.get('note', '')

            msg = (
                '📊 <b>FAIR VALUE: ' + symbol + '</b>\n'
                + '=' * 28 + '\n\n'
                + '<b>Phuong phap:</b> ' + escape_html(fv.get('method', '')) + '\n'
                + '<b>Nhom:</b> ' + fv.get('group', '') + '\n\n'
                + '<b>Vung gia hop ly:</b>\n'
                + ' Tham chieu thap: <b>' + f'{fv.get("fair_low", 0):,.0f}' + 'd</b>\n'
                + ' Fair value     : <b>' + f'{fv.get("fair_value", 0):,.0f}' + 'd</b>\n'
                + ' Tham chieu cao : <b>' + f'{fv.get("fair_high", 0):,.0f}' + 'd</b>\n\n'
                + '<b>Chi so co ban:</b>\n'
                + detail_lines + '\n'
                + val_emoji + ' <b>' + fv.get('valuation', '') + '</b> '
                + '(' + upside_s + ' ' + upside_lbl + ')\n'
                + (('\n<i>Luu y: ' + escape_html(note) + '</i>\n') if note else '')
                + '\n<i>Cap nhat: vua refresh | Khong phai tu van dau tu</i>'
            )
            send(msg, chat_id)

        except Exception as e:
            logger.error(f'handle_fv {symbol}: {e}')
            send('❌ Loi khi tinh fair value ' + symbol + ': ' + str(e)[:100], chat_id)

    threading.Thread(target=run, daemon=True).start()


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


def _fmt_shark_inline(shark_score):
    """Dòng shark score ngắn cho header analyze."""
    if shark_score >= 80:
        return chr(10) + '&#x1F988;&#x1F988; <b>Shark Score: ' + str(shark_score) + '/100 — GOM MANH!</b>'
    elif shark_score >= 60:
        return chr(10) + '&#x1F988; Shark Score: ' + str(shark_score) + '/100 — Co dau hieu gom hang'
    elif shark_score >= 40:
        return chr(10) + '&#x1F440; Shark Score: ' + str(shark_score) + '/100 — Theo doi them'
    return ''


def handle_shark(symbol, chat_id):
    """Phân tích chi tiết Shark Accumulation cho 1 mã."""
    send('&#x1F988; Dang phan tich Shark v4 <b>' + symbol + '</b>...', chat_id)

    def run():
        try:
            import sys, os, importlib, pandas as pd
            bot_dir = os.path.dirname(os.path.abspath(__file__))
            if bot_dir not in sys.path:
                sys.path.insert(0, bot_dir)
            from shark_detector import calc_shark_score, load_foreign_flow, format_shark_msg
            import backtest as bt
            importlib.reload(bt)

            df, _ = bt.load_data(symbol, days=200)
            if df is None:
                send('&#x274C; Khong tai duoc du lieu ' + symbol, chat_id)
                return

            def to_arr(col_names):
                for c in df.columns:
                    if c.lower() in col_names:
                        return __import__('numpy').array(
                            pd.to_numeric(df[c], errors='coerce').fillna(0)).copy()
                return __import__('numpy').zeros(len(df))

            import numpy as np
            closes  = to_arr({'close','closeprice','close_price'})
            highs   = to_arr({'high','highprice','high_price'})
            lows    = to_arr({'low','lowprice','low_price'})
            volumes = to_arr({'volume','volume_match','klgd','vol'})
            if closes.max() < 1000: closes  *= 1000
            if highs.max()  < 1000: highs   *= 1000
            if lows.max()   < 1000: lows    *= 1000

            foreign_net = None
            try:
                df_fn = load_foreign_flow(symbol, days=60)
                if df_fn is not None and 'net_vol' in df_fn.columns:
                    foreign_net = df_fn['net_vol'].values[-20:].tolist()
            except Exception:
                pass

            score, details = calc_shark_score(
                closes.tolist(), highs.tolist(), lows.tolist(), volumes.tolist(),
                foreign_net=foreign_net, symbol=symbol,
            )
            msg = ('&#x1F988; <b>Shark Accumulation v4 — ' + symbol + '</b>' + chr(10)
                   + '=' * 30 + chr(10) + chr(10)
                   + format_shark_msg(score, details, symbol))
            send(msg, chat_id)
        except Exception as e:
            logger.error('handle_shark ' + symbol + ': ' + str(e))
            send('&#x274C; Loi Shark Detector: ' + str(e)[:100], chat_id)

    threading.Thread(target=run, daemon=True).start()


def handle_shark_backtest(symbol, chat_id):
    """Chạy backtest Shark Score độc lập vs kết hợp A+S."""
    send('&#x1F9EA; Shark Backtest <b>' + symbol + '</b>...' + chr(10)
         + 'So sanh: Shark doc lap | Score A | A+S ket hop (~2-3 phut)', chat_id)

    def run():
        try:
            import sys, os, importlib, io, contextlib, time as _t
            bot_dir = os.path.dirname(os.path.abspath(__file__))
            if bot_dir not in sys.path:
                sys.path.insert(0, bot_dir)
            import backtest_shark as bs
            importlib.reload(bs)

            # Progress update sau 20s để user biết đang chạy
            _started = _t.time()
            def _progress():
                _t.sleep(20)
                elapsed = _t.time() - _started
                send(f'&#x23F3; Dang tinh backtest {symbol}... ({elapsed:.0f}s)' + chr(10)
                     + 'Vui long doi them 1-2 phut.', chat_id)
            threading.Thread(target=_progress, daemon=True).start()

            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                res = bs.run_shark_report(symbol)
            if not res:
                send('&#x274C; Khong du du lieu cho ' + symbol + chr(10)
                     + '(Co the do rate limit vnstock — thu lai sau 1-2 phut)', chat_id)
                return

            # results = {'symbol', 'score_a', 'best', 'all_rows', 'verdict_*'}
            sa   = res.get('score_a', {})
            best = res.get('best', {})   # {mode: (thr, res_list, stats_dict)}
            NL   = chr(10)
            
            # Build bảng kết quả
            rows = ''
            best_mode = None
            best_wr   = 0
            for mode, (thr, _, st) in sorted(best.items()):
                wr  = st.get('win_rate', 0)
                pnl = st.get('avg_pnl', 0)
                n   = st.get('total', 0)
                dwr = wr - sa.get('wr', 0)
                rows += f'  {mode:>5}>={thr}: {n}L | WR={wr:.1f}% | PnL={pnl:+.2f}% | DWR={dwr:+.1f}%' + NL
                if wr > best_wr:
                    best_wr, best_mode = wr, mode
            
            # Verdicts
            vds = [v for k, v in res.items() if k.startswith('verdict_')]
            vd_str = NL.join(vds) if vds else ''
            
            msg = ('&#x1F988; <b>Shark Backtest — ' + symbol + '</b>' + NL
                   + '=' * 30 + NL + NL
                   + 'Score A baseline: '
                   + str(sa.get("n",0)) + 'L | WR=' + f'{sa.get("wr",0):.1f}%'
                   + ' | PnL=' + f'{sa.get("pnl",0):+.2f}%' + NL + NL
                   + '<b>Ket qua tung mode:</b>' + NL
                   + rows + NL
                   + (('<b>Mode tot nhat:</b> ' + best_mode + NL + NL) if best_mode else '')
                   + vd_str + NL
                   + '<i>Backtest 7 nam OHLCV daily</i>')
            send(msg, chat_id)
        except Exception as e:
            import traceback
            logger.error('handle_shark_backtest ' + symbol + ': ' + str(e))
            logger.error(traceback.format_exc())
            send('&#x274C; Loi Shark Backtest: ' + str(e)[:150] + chr(10)
                 + 'Thu: /sharkbt ' + symbol + ' sau 2-3 phut', chat_id)

    threading.Thread(target=run, daemon=True).start()


def handle_market_scan(chat_id):
    """Chạy market scan toàn sàn và gửi kết quả."""
    try:
        n_syms = len(__import__('market_scanner').HOSE_LIQUID)
    except Exception:
        n_syms = 250
    send('&#x1F4E1; <b>Market Scanner v2</b> dang quet ~' + str(n_syms) + ' ma...' + chr(10)
         + 'Loc 3 tang: ADTV&gt;5ty | Gia&gt;MA50&gt;MA200 | RSI&lt;70 | ScoreA&gt;=60' + chr(10)
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
            extra_syms = ['VCB','BID','CTG','VPB','ACB','HPG','FPT','VHM',
                          'GAS','SSI','VND','HCM','DCM','DGC','MBB','HSG','NKG',
                          'TCB','STB','HDB','TPB','MWG','PNJ','FRT','POW','REE']
            all_syms = list(dict.fromkeys(watchlist_syms + extra_syms))

            results = []
            total   = len(all_syms)
            for i, sym in enumerate(all_syms, 1):
                try:
                    r = scan_via_api(sym)
                    if r: results.append(r)
                except Exception:
                    pass
                if i % 10 == 0:
                    pct = int(i/total*100)
                    send('&#x23F3; ' + str(i) + '/' + str(total) + ' ma (' + str(pct) + '%)...', chat_id)
                _time.sleep(0.3)   # 0.3s delay tránh overload Flask

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


def build_analysis_msg(data, prefix='Phan tich', b_ctx=None):
    sym = data.get('symbol', '')
    price = data.get('price', 0)
    score = data.get('score', 50)
    action = data.get('action', 'THEO DOI')

    # ── Tính B-filter penalty nếu có ─────────────────────────────────────
    b_delta    = 0
    b_details  = []
    b_overall  = ''
    if b_ctx:
        import market_context as _mc
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
        tio_line = '\n\nHOI TU 3-TRONG-1: Gia tren MA20 + Vol dot bien + RSI hop le -&gt; Du dieu kien'

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
        ma10_cross_line = '\n⚡ <b>MA10 CROSS UP</b> — Gia vua cat len MA10 hom nay!'

    msg = (
            '<b>' + prefix + ' ' + sym + '</b>\n'
            + '=' * 30 + '\n'
            + 'Gia: <b>' + f'{price:,.0f}' + 'd</b>'
            + ' Diem A: <b>' + str(score) + '/100</b>'
            + (' → A+B: <b>' + str(score_adj) + '/100</b>'
               + (' (+' if b_delta > 0 else ' (') + str(b_delta) + 'd)'
               if b_delta != 0 else '')
            + ' ' + ae + tio_line + div_line + ma10_cross_line
            + (_fmt_shark_inline(data.get('shark_score', 0)) if data.get('shark_score', 0) >= 40 else '')
            + (_fmt_rs_inline(data) if data.get('rs_20d') is not None else '')
            + '\n\n'
            + '<b>1. RSI(14)</b>\n' + (rsi_lines or ' -&gt; Trung tinh') + '\n\n'
            + '<b>2. RSI Phan ky</b>\n' + (div_lines or ' -&gt; Khong phat hien phan ky') + '\n\n'
            + '<b>3. MACD</b>\n'
            + ' Line:' + f'{data.get("macd", 0):+.0f}' + ' Sig:' + f'{data.get("macd_signal", 0):+.0f}\n' + (macd_lines or '') + '\n\n'
            + '<b>4. MA10 / MA20 / MA50</b>\n'
            + ' MA10:' + f'{ma10_val:,.0f}' + ' MA20:' + f'{data.get("ma20", 0):,.0f}' + ' MA50:' + f'{data.get("ma50", 0):,.0f}\n'
            + (ma10_lines or '') + '\n'
            + (ma50_lines or '') + '\n'
            + (ma_lines or '') + '\n\n'
            + '<b>5. Volume (Dong tien)</b>\n'
            + ' Hom nay:' + fmt_vol(data.get('vol_today', 0)) + ' TB20:' + fmt_vol(data.get('vol_tb20', 0)) + '\n'
            + (vol_lines or '') + '\n'
            + _vol_time_note(vr) + '\n\n'
            + '<b>6. Ho tro / Khang cu</b>\n'
            + ' HT: ' + (sup_txt or '(chua xac dinh)') + '\n'
            + ' KC: ' + (res_txt or '(chua xac dinh)') + '\n'
            + (sr_lines or '') + '\n'
            + ' <i>BB: ' + f'{data.get("bb_lower", 0):,.0f}' + '–' + f'{data.get("bb_upper", 0):,.0f}'
            + (' | ' + bb_lines.strip() if bb_lines and bb_lines.strip() else '') + '</i>\n'
            + ' <i>Ichimoku: May ' + f'{cb:,.0f}' + '–' + f'{ct:,.0f}' + ' | ' + ichi_s
            + (' | TK:' + f'{ichi.get("tenkan",0):,.0f}' + ' KJ:' + f'{ichi.get("kijun",0):,.0f}' if ichi.get('tenkan') else '') + '</i>\n'
            + ((' <i>' + ichi_lines.strip() + '</i>\n') if ichi_lines and ichi_lines.strip() else '')
            + '\n'
            + '<b>7. Xu huong Tuan (1W)</b>\n'
            + ' MA10W~MA100D:' + f'{ma100:,.0f}' + ' MA20W~MA200D:' + f'{ma200:,.0f}' + '\n'
            + ' ' + wt_emoji + ' ' + wt_vn + '\n'
            + (weekly_lines or '') + '\n\n'
            + _format_1h_warnings(data.get('warnings_1h', []))
            + _format_fair_value(data.get('fair_value', {}), data.get('action', ''), data.get('score', 50))
            + '<b>KET LUAN</b>\n'
            + _build_conclusion(score, score_adj, b_delta, b_details,
                                b_overall, action, ae, data)
    )
    return msg


def handle_backtest(symbol, chat_id):
    """
    Chạy backtest 5 năm cho 1 mã và gửi kết quả qua Telegram.
    Chạy trong thread riêng để không block polling loop.
    """
    send('📊 Dang chay backtest <b>' + symbol + '</b> tren 7 nam...\n'
         'Vui long cho ~90 giay.', chat_id)

    def run():
        try:
            import sys
            import importlib
            import os

            # Đảm bảo import được backtest.py cùng thư mục
            bot_dir = os.path.dirname(os.path.abspath(__file__))
            if bot_dir not in sys.path:
                sys.path.insert(0, bot_dir)

            import backtest as bt
            importlib.reload(bt)  # Luôn dùng phiên bản mới nhất

            result = bt.run_backtest_symbol(symbol)

            # Không có dữ liệu
            if not result:
                send('❌ ' + symbol + ': Khong du du lieu de backtest '
                     '(can it nhat 120 phien giao dich).', chat_id)
                return

            buy  = result.get('buy',  {})
            sell = result.get('sell', {})
            total = result.get('total_trades', 0)
            trades_df = result.get('trades')

            # Không có lệnh MUA nào
            if not buy:
                send('🟡 ' + symbol + ': Khong co lenh MUA nao duoc phat '
                     'sinh trong 7 nam qua.\n'
                     'Co the ma nay it bien dong hoac di ngang.', chat_id)
                return

            # ── Chỉ số chính ────────────────────────────────────────────────
            wr   = buy.get('win_rate', 0)
            pnl  = buy.get('avg_pnl', 0)
            pf   = buy.get('profit_factor', 0)
            aw   = buy.get('avg_win', 0)
            al   = buy.get('avg_loss', 0)
            ad   = buy.get('avg_days', 0)
            bt_  = buy.get('total', 0)
            tp_  = buy.get('tp', 0)
            sl_  = buy.get('sl', 0)
            hk_  = buy.get('expired', 0)
            pf_s = f'{pf:.2f}' if pf != float('inf') else '&#x221E;'

            # SL/TP/Score/Lookback thực tế dùng cho mã này (từ SYMBOL_CONFIG hoặc default)
            cfg_sl        = result.get('sl', 0.07)
            cfg_tp        = result.get('tp', 0.14)
            cfg_min_score = result.get('min_score', 65)
            cfg_days      = result.get('days', 2555)
            cfg_yrs       = round(cfg_days / 365, 0)
            cfg_note = (f'SL=-{cfg_sl*100:.0f}% TP=+{cfg_tp*100:.0f}% '
                        f'Score&gt;={cfg_min_score} | '
                        f'{cfg_yrs:.0f} nam | Max 10 phien')

            # ── Đánh giá tổng thể ───────────────────────────────────────────
            if wr >= 60 and pnl >= 3 and pf >= 1.8:
                verdict      = '✅ TIN HIEU DANG TIN CAY'
                verdict_note = 'Win rate, PnL va Profit Factor deu tot. Co the tin tuong tin hieu MUA tren ma nay.'
                verdict_bar  = '🟢🟢🟢'
            elif wr >= 55 and pnl >= 1 and pf >= 1.3:
                verdict      = '🟡 CHAP NHAN DUOC'
                verdict_note = 'Ket qua on nhung chua xuat sac. Nen ket hop them phan tich tay truoc khi vao lenh.'
                verdict_bar  = '🟢🟢🟡'
            elif wr >= 50 and pnl >= 0:
                verdict      = '🟡 TRUNG BINH'
                verdict_note = 'Bot hoat dong khong on dinh tren ma nay. Chi dung de tham khao, khong nen phu thuoc.'
                verdict_bar  = '🟢🟡🟡'
            else:
                verdict      = '🔴 CAN THAN - KEM HIEU QUA'
                verdict_note = 'Tin hieu tren ma nay co ti le thua cao hon thang. Nen tim ma khac hoc lai weight.'
                verdict_bar  = '🔴🔴🔴'

            # ── TIME SLICE: kết quả từng năm ────────────────────────────────
            yearly_data = result.get('yearly', {}).get('yearly', {})
            bull_bias   = result.get('yearly', {}).get('bull_bias', 'N/A')
            consistency = result.get('yearly', {}).get('consistency', '')

            time_slice_lines = ''
            if yearly_data:
                PHASE = {
                    2020: 'Covid/Phuc hoi',
                    2021: 'Bull Run (+130%)',
                    2022: 'Bear Market (-50%)',
                    2023: 'Phuc hoi sideway',
                    2024: 'Tang truong on dinh',
                    2025: 'Bien dong DCP',
                    2026: '2026',
                }
                for yr in sorted(yearly_data.keys()):
                    if yr == 0:
                        continue
                    d = yearly_data[yr]
                    yr_wr  = d.get('win_rate', 0)
                    yr_pnl = d.get('avg_pnl', 0)
                    yr_tp  = d.get('tp', 0)
                    yr_sl  = d.get('sl', 0)
                    yr_hk  = d.get('expired', 0)
                    yr_n   = d.get('total', 0)
                    if   yr_wr >= 60 and yr_pnl >= 0: icon = '✅'
                    elif yr_wr >= 50:                 icon = '🟡'
                    else:                             icon = '❌'
                    phase = PHASE.get(yr, str(yr))
                    time_slice_lines += (
                        f' {icon} <b>{yr}</b> ({phase}): '
                        f'WR=<b>{yr_wr:.0f}%</b> PnL={yr_pnl:+.1f}% '
                        f'| TP:{yr_tp} SL:{yr_sl} HK:{yr_hk} ({yr_n}L)\n'
                    )
                # Bull bias note
                if bull_bias == 'NGHIÊM TRỌNG':
                    time_slice_lines += '\n⚠ <b>CANH BAO:</b> Ket qua bi thoi phong boi bull run 2021\n'
                elif bull_bias == 'TRUNG BINH':
                    time_slice_lines += '\n⚠ Bull Bias trung binh — xem ket qua 2022+ de danh gia thuc te\n'
                # Consistency
                if consistency:
                    time_slice_lines += f'&#x1F4CC; {consistency[:60]}\n'

            # ── Ngưỡng score tối ưu ────────────────────────────────────────
            thresh_data = result.get('thresh', {}).get('results', {})
            best_thr    = result.get('thresh', {}).get('best_threshold', 65)
            thresh_lines = ''
            for thr in [65, 70, 75, 80]:
                t = thresh_data.get(thr, {})
                if not t:
                    continue
                t_wr  = t.get('win_rate', 0)
                t_pnl = t.get('avg_pnl', 0)
                t_n   = t.get('total', 0)
                flag  = ' &#x2B50;' if thr == best_thr else ''
                thresh_lines += f' Score&gt;={thr}: {t_n}L | WR={t_wr:.0f}% | PnL={t_pnl:+.1f}%{flag}\n'

            # ── 3 lệnh gần nhất ────────────────────────────────────────────
            recent_lines = ''
            if trades_df is not None and len(trades_df) > 0:
                buy_df = trades_df[trades_df['action'] == 'MUA'].tail(3)
                for _, row in buy_df.iterrows():
                    icon = '✅' if row['pnl'] > 0 else '❌'
                    recent_lines += (
                        ' ' + icon + ' ' + str(row['date']) + ' @'
                        + f'{row["price"]:,.0f}d'
                        + ' S=' + str(row['score'])
                        + ' PnL=' + f'{row["pnl"]:+.1f}%'
                        + ' (' + row['reason'] + ', ' + str(row['days']) + 'p)\n'
                    )

            # ── Lệnh BAN (nếu có) ──────────────────────────────────────────
            sell_line = ''
            if sell and sell.get('total', 0) > 0:
                sell_line = (
                    '\n<b>Tin hieu BAN (tranh mua):</b>\n'
                    + ' Tong: ' + str(sell.get('total', 0)) + ' lenh'
                    + ' | Chinh xac: ' + str(sell.get('win_rate', 0)) + '%\n'
                )

            # ── CI 95% ─────────────────────────────────────────────────────
            conf = result.get('conf', {})
            ci_lo = conf.get('ci_low', 0)
            ci_hi = conf.get('ci_high', 100)
            ci_line = f'CI 95%: [{ci_lo}% – {ci_hi}%] ({bt_} lenh)'

            # ── Build message ───────────────────────────────────────────────
            msg = (
                '📊 <b>BACKTEST ' + symbol + ' (5 NAM)</b>\n'
                + '=' * 30 + '\n\n'

                + '<b>Tong quan:</b>\n'
                + ' Lenh MUA: ' + str(bt_)
                + ' | TP: ' + str(tp_)
                + ' | SL: ' + str(sl_)
                + ' | Het ky: ' + str(hk_) + '\n'
                + ' WR: <b>' + str(wr) + '%</b>'
                + ' | PnL TB: <b>' + f'{pnl:+.2f}%</b>\n'
                + ' PF: <b>' + pf_s + '</b>'
                + ' | TB thang: ' + f'{aw:+.1f}%'
                + ' | TB thua: ' + f'{al:+.1f}%\n'
                + ' ' + ci_line + '\n\n'

                + ('<b>&#x1F4C5; Time Slice — Ket qua theo tung nam:</b>\n'
                   + time_slice_lines + '\n' if time_slice_lines else '')

                + ('<b>Nguong score toi uu (MUA):</b>\n'
                   + thresh_lines + '\n' if thresh_lines else '')

                + ('<b>3 lenh MUA gan nhat:</b>\n'
                   + recent_lines + '\n' if recent_lines else '')

                + sell_line

                + '<b>Danh gia:</b> ' + verdict_bar + '\n'
                + ' ' + verdict + '\n'
                + ' ' + verdict_note + '\n\n'

                + '<i>' + cfg_note + '\n'
                + 'Khong tinh phi giao dich (~0.3%/khu vong).\n'
                + 'Ket qua qua khu khong dam bao tuong lai.</i>'
            )
            send(msg, chat_id)

        except ImportError:
            send('❌ Loi: Khong tim thay file backtest.py.\n'
                 'Hay dam bao backtest.py nam cung thu muc voi telegram_bot.py.', chat_id)
        except Exception as e:
            logger.error('handle_backtest ' + symbol + ': ' + str(e))
            import traceback
            logger.error(traceback.format_exc())
            send('❌ Loi khi chay backtest ' + symbol + ': ' + str(e)[:120], chat_id)

    # Chạy trong thread riêng — không block polling
    threading.Thread(target=run, daemon=True).start()


def handle_wf(symbol, chat_id):
    """
    Walk-Forward Analysis cho 1 mã — gửi kết quả qua Telegram.
    IS=2năm → OOS=1năm, cuộn theo từng năm.
    """
    send(
        '📊 Dang chay <b>Walk-Forward</b> cho <b>' + symbol + '</b>\n'
        'IS=2nam OOS=1nam | Du kien ~2 phut...',
        chat_id
    )

    def run():
        try:
            import sys, os, importlib
            bot_dir = os.path.dirname(os.path.abspath(__file__))
            if bot_dir not in sys.path:
                sys.path.insert(0, bot_dir)
            import backtest as bt
            importlib.reload(bt)

            res = bt.run_walk_forward(symbol, verbose=False)

            if not res:
                send('❌ ' + symbol + ': Khong du du lieu walk-forward (can 3+ nam).', chat_id)
                return

            windows    = res['windows']
            verdict    = res['verdict']
            verdict_txt = res['verdict_txt']
            decay_wr   = res['decay_wr']
            avg_is_wr  = res['avg_is_wr']
            avg_oos_wr = res['avg_oos_wr']
            avg_is_pnl = res['avg_is_pnl']
            avg_oos_pnl= res['avg_oos_pnl']
            thresholds = res['thresholds']
            thr_stable = res['thr_stable']
            _sl        = res['sl']
            _tp        = res['tp']

            # ── Icon verdict ─────────────────────────────────────────────
            if   verdict == 'V': v_icon = '✅'
            elif verdict == '~': v_icon = '🟡'
            elif verdict == '!': v_icon = '⚠'
            else:                v_icon = '❌'

            # ── Bảng từng cửa sổ ─────────────────────────────────────────
            win_lines = ''
            for w in windows:
                if w['oos_wr'] is None:
                    continue
                decay_w = w['is_wr'] - w['oos_wr']
                if   decay_w <=  5: flag = '✅'
                elif decay_w <= 15: flag = '🟡'
                elif decay_w <= 25: flag = '⚠'
                else:               flag = '❌'

                win_lines += (
                    f'{flag} <b>OOS {w["oos_label"]}</b> '
                    f'(IS={w["is_label"]} nguong&gt;={w["best_thr"]})\n'
                    f'   IS  : WR={w["is_wr"]:.1f}%  PnL={w["is_pnl"]:+.2f}%  ({w["is_n"]}L)\n'
                    f'   OOS : WR=<b>{w["oos_wr"]:.1f}%</b>  '
                    f'PnL=<b>{w["oos_pnl"]:+.2f}%</b>  ({w["oos_n"]}L)  '
                    f'decay={decay_w:+.1f}%\n\n'
                )

            # ── Ngưỡng score có nhất quán không ──────────────────────────
            def _safe(s):
                return str(s).replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')
            thr_note = (
                '✅ Nguong score on dinh: ' + _safe(thresholds)
                if thr_stable else
                '⚠ Nguong score bien dong: ' + _safe(thresholds)
                + '\n   → He thong nhay cam voi thi truong, nen de dat khi dung'
            )

            # ── Decay assessment ──────────────────────────────────────────
            if   decay_wr <=  5: decay_txt = 'Rat on dinh (decay &lt; 5%)'
            elif decay_wr <= 10: decay_txt = 'On dinh (decay &lt; 10%)'
            elif decay_wr <= 20: decay_txt = 'Chap nhan duoc (decay &lt; 20%)'
            elif decay_wr <= 30: decay_txt = 'Canh bao (decay &gt; 20%)'
            else:                decay_txt = 'NGUY HIEM — Co the overfit nghiem trong'

            msg = (
                '📊 <b>WALK-FORWARD: ' + symbol + '</b>\n'
                + f'SL={_sl*100:.0f}% TP={_tp*100:.0f}% | IS=2nam OOS=1nam\n'
                + '=' * 30 + '\n\n'
                + win_lines
                + '📋 <b>Tong ket:</b>\n'
                + f' IS  TB: WR={avg_is_wr:.1f}%  PnL={avg_is_pnl:+.2f}%\n'
                + f' OOS TB: WR=<b>{avg_oos_wr:.1f}%</b>  PnL=<b>{avg_oos_pnl:+.2f}%</b>\n'
                + f' Decay WR: <b>{decay_wr:+.1f}%</b> — {decay_txt}\n'
                + f' {thr_note}\n\n'
                + f'{v_icon} <b>{verdict_txt.replace("<","&lt;").replace(">","&gt;")}</b>\n\n'
                + '<i>Walk-forward kiem tra he thong co hoat dong nhat quan\n'
                + 'tren du lieu chua tung thay (OOS) hay khong.\n'
                + 'Decay cao = co the overfit tren du lieu lich su.</i>'
            )
            send(msg, chat_id)

        except Exception as e:
            logger.error(f'handle_wf {symbol}: {e}')
            import traceback
            logger.error(traceback.format_exc())
            send('❌ Loi walk-forward ' + symbol + ': ' + str(e)[:120], chat_id)

    threading.Thread(target=run, daemon=True).start()

def handle_dual(symbol, chat_id):
    """
    /dual VCB — Chạy backtest 2 mode Entry=T vs Entry=T+1, so sánh kết quả.
    """
    send(
        '📊 Dang chay <b>Dual Backtest</b> cho <b>' + symbol + '</b>\n'
        'Entry=T (backtest chuan) vs Entry=T+1 (thuc te)\n'
        'Du kien ~3 phut...',
        chat_id
    )

    def run():
        try:
            import sys, os, importlib
            bot_dir = os.path.dirname(os.path.abspath(__file__))
            if bot_dir not in sys.path:
                sys.path.insert(0, bot_dir)
            import backtest as bt
            importlib.reload(bt)

            res = bt.run_backtest_dual(symbol, verbose=False)
            if not res:
                send('❌ ' + symbol + ': Khong du du lieu.', chat_id)
                return

            mt   = res['mode_T']
            mt1  = res['mode_T1']
            wd   = res['wr_diff']
            pd_  = res['pnl_diff']
            flag = res['bias_flag']
            bias = res['bias_level']
            rec  = res['recommend']

            # Icons
            vmap  = {'V': '✅', '~': '🟡', '!': '⚠'}
            v_icon = vmap.get(flag, '❓')

            # PnL diff color
            wd_s  = f'{wd:+.1f}%'
            pd_s  = f'{pd_:+.2f}%'

            pf_t_s  = f"{mt['pf']:.2f}"  if mt['pf']  != float('inf') else 'inf'
            pf_t1_s = f"{mt1['pf']:.2f}" if mt1['pf'] != float('inf') else 'inf'

            # Yearly comparison nếu có
            yr_lines = ''
            if res.get('res_t') and res.get('res_t1'):
                yr_t  = res['res_t'].get('yearly', {}).get('yearly', {})
                yr_t1 = res['res_t1'].get('yearly', {}).get('yearly', {})
                years = sorted(set(list(yr_t.keys()) + list(yr_t1.keys())))
                for yr in years[-4:]:  # 4 năm gần nhất
                    d_t  = yr_t.get(yr, {})
                    d_t1 = yr_t1.get(yr, {})
                    if not d_t or not d_t1:
                        continue
                    wr_diff_yr  = d_t1.get('win_rate', 0) - d_t.get('win_rate', 0)
                    icon = '⬆' if wr_diff_yr >= 0 else '⬇'
                    yr_lines += (
                        f' {icon} {yr}: T={d_t.get("win_rate",0):.0f}%'
                        f' → T+1={d_t1.get("win_rate",0):.0f}%'
                        f' ({wr_diff_yr:+.0f}%)\n'
                    )

            # Dùng giá trị đã format sẵn — tránh format specifier > trong f-string HTML
            n_t_s   = str(mt["n"])
            wr_t_s  = f'{mt["wr"]:.1f}%'
            pnl_t_s = f'{mt["pnl"]:+.2f}%'
            n_t1_s  = str(mt1["n"])
            wr_t1_s = f'{mt1["wr"]:.1f}%'
            pnl_t1s = f'{mt1["pnl"]:+.2f}%'
            wd_str  = f'{wd:+.1f}%'
            pd_str  = f'{pd_:+.2f}%'

            msg = (
                '📊 <b>DUAL BACKTEST: ' + symbol + '</b>\n'
                + '=' * 28 + '\n\n'

                + '<b>Mode</b>\n'
                + ' Entry T   (BT) : ' + n_t_s + 'L | WR=' + wr_t_s
                + ' | PnL=' + pnl_t_s + ' | PF=' + pf_t_s + '\n'
                + ' Entry T+1 (TT) : ' + n_t1_s + 'L | WR=<b>' + wr_t1_s
                + '</b> | PnL=<b>' + pnl_t1s + '</b> | PF=' + pf_t1_s + '\n'
                + ' Chenh lech     : WR=' + wd_str + ' | PnL=' + pd_str + '\n\n'

                + ('<b>WR theo nam (T → T+1):</b>\n' + yr_lines + '\n' if yr_lines else '')

                + v_icon + ' <b>Entry Bias: ' + bias + '</b>\n'
                + '<i>' + rec + '</i>\n\n'

                + '<i>Entry T  : mua tai gia dong cua ngay phat hieu (backtest chuan)\n'
                + 'Entry T+1 : mua dau phien ngay hom sau (sat thuc te hon)\n'
                + 'Khong phai tu van dau tu</i>'
            )
            send(msg, chat_id)

        except Exception as e:
            logger.error(f'handle_dual {symbol}: {e}')
            import traceback
            logger.error(traceback.format_exc())
            send('❌ Loi dual backtest ' + symbol + ': ' + str(e)[:120], chat_id)

    threading.Thread(target=run, daemon=True).start()


def handle_btest_b(symbol, chat_id):
    """
    /btest_b VCB — Backtest 7 năm so sánh Mode A (kỹ thuật) vs Mode A+B (kỹ thuật + B-filter).
    Kiểm chứng xem B-filter có cải thiện WR/PnL thực sự không.
    """
    send(
        '📊 Dang chay <b>B-Filter Comparison</b> cho <b>' + symbol + '</b>\n'
        'Mode A (ky thuat) vs Mode A+B (+ Wyckoff/Liquidity filter)\n'
        'Du kien ~4 phut...',
        chat_id
    )

    def run():
        try:
            import sys, os, importlib
            bot_dir = os.path.dirname(os.path.abspath(__file__))
            if bot_dir not in sys.path: sys.path.insert(0, bot_dir)
            import backtest as bt
            importlib.reload(bt)

            res = bt.run_b_filter_comparison(symbol, verbose=False)
            if not res:
                send('❌ ' + symbol + ': Khong du du lieu.', chat_id)
                return

            ma  = res['mode_A']
            mab = res['mode_AB']
            wd  = res['wr_diff']
            pd_ = res['pnl_diff']
            nf  = res['n_filtered']
            flag = res['flag']
            verdict = res['verdict']

            fmap = {'V': '✅', '~': '🟡', '-': '➡', '!': '⚠'}
            v_icon = fmap.get(flag, '❓')

            pf_a_s  = f"{ma['pf']:.2f}"  if ma['pf']  != float('inf') else 'inf'
            pf_ab_s = f"{mab['pf']:.2f}" if mab['pf'] != float('inf') else 'inf'

            wd_s  = f'{wd:+.1f}%'
            pd_s  = f'{pd_:+.2f}%'
            pct_f = f'{nf/ma["n"]*100:.1f}%' if ma['n'] > 0 else '0%'

            msg = (
                '📊 <b>B-FILTER COMPARISON: ' + symbol + '</b>\n'
                + '=' * 28 + '\n\n'

                + '<b>Mode</b>\n'
                + ' A   (KT thuan) : ' + str(ma['n']) + 'L | WR=' + f'{ma["wr"]:.1f}%'
                + ' | PnL=' + f'{ma["pnl"]:+.2f}%' + ' | PF=' + pf_a_s + '\n'
                + ' A+B (KT+BF)   : ' + str(mab['n']) + 'L | WR=<b>' + f'{mab["wr"]:.1f}%'
                + '</b> | PnL=<b>' + f'{mab["pnl"]:+.2f}%' + '</b> | PF=' + pf_ab_s + '\n'
                + ' Chenh lech    : ' + str(-nf) + 'L | WR=' + wd_s + ' | PnL=' + pd_s + '\n\n'

                + 'Lenh bi loc boi B-filter: ' + str(nf) + ' (' + pct_f + ' tong lenh)\n\n'

                + v_icon + ' <b>' + verdict + '</b>\n\n'
                + '<i>Mode A  : chi dung score ky thuat (RSI/MA/Vol...)\n'
                + 'Mode A+B : them penalty Wyckoff+Liquidity\n'
                + 'Khong phai tu van dau tu</i>'
            )
            send(msg, chat_id)

            # Walk-forward comparison
            send('&#x1F504; Dang chay <b>Walk-Forward</b> cho B-filter (~3 phut them)...', chat_id)
            wfres = bt.run_b_filter_walkforward(symbol, verbose=False)
            if wfres:
                da  = wfres['decay_a']
                dab = wfres['decay_ab']
                oa  = wfres['oos_a']
                oab = wfres['oos_ab']
                wfa = wfres['wf_a']
                wfb = wfres['wf_ab']

                if dab < da - 2:
                    wf_icon = '✅'
                    wf_txt  = 'B-filter GIAM DECAY - robustness cao hon'
                elif abs(dab - da) <= 3:
                    wf_icon = '🟡'
                    wf_txt  = 'B-filter TRUNG TINH - decay tuong duong'
                else:
                    wf_icon = '⚠'
                    wf_txt  = 'B-filter TANG DECAY - nen xem lai penalty'

                wf_msg = (
                    '&#x1F504; <b>WALK-FORWARD: ' + symbol + '</b>\n'
                    + '=' * 28 + '\n\n'
                    + '<b>Mode</b>\n'
                    + ' A  : OOS WR=' + f'{oa:.1f}%' + ' | Decay=' + f'{da:+.1f}%'
                    + ' | ' + wfa.get('verdict_txt','')[:20] + '\n'
                    + ' A+B: OOS WR=<b>' + f'{oab:.1f}%' + '</b> | Decay=<b>' + f'{dab:+.1f}%'
                    + '</b> | ' + wfb.get('verdict_txt','')[:20] + '\n\n'
                    + wf_icon + ' <b>' + wf_txt + '</b>\n\n'
                    + '<i>IS=2nam OOS=1nam | Decay thap = robustness cao\n'
                    + 'Khong phai tu van dau tu</i>'
                )
                send(wf_msg, chat_id)

        except Exception as e:
            logger.error(f'handle_btest_b {symbol}: {e}')
            import traceback
            logger.error(traceback.format_exc())
            send('❌ Loi btest_b ' + symbol + ': ' + str(e)[:120], chat_id)

    threading.Thread(target=run, daemon=True).start()


def handle_btest_b_all(chat_id):
    """
    /btest_b all — Chay B-filter comparison cho 8 ma watchlist.
    8 ma chay tuan tu ~8 phut, gui ket qua tung ma ngay khi xong,
    cuoi cung gui tong hop A vs A+B.
    """
    wl_list = list(WATCHLIST_META.keys())
    n       = len(wl_list)

    send(
        '📊 <b>B-Filter Comparison — ' + str(n) + ' ma watchlist</b>\n'
        'So sanh: <b>A</b> (ky thuat thuan) vs <b>A+B</b> (+ B-filter)\n'
        'Du kien ~' + str(n) + '-' + str(n + 2) + ' phut. '
        'Gui ket qua tung ma ngay khi xong.',
        chat_id
    )

    def run():
        import sys, os, importlib, concurrent.futures
        bot_dir = os.path.dirname(os.path.abspath(__file__))
        if bot_dir not in sys.path:
            sys.path.insert(0, bot_dir)
        import backtest as bt
        importlib.reload(bt)

        fmap        = {'V': '✅', '~': '🟡', '-': '➡', '!': '⚠'}
        all_results = []

        # Chay tung ma, gui tin ngay khi xong
        for i, sym in enumerate(wl_list, 1):
            try:
                r = bt.run_b_filter_comparison(sym, verbose=False)
                if not r:
                    send('⚠ ' + sym + ': Khong du du lieu.', chat_id)
                    continue

                all_results.append(r)
                ma  = r['mode_A']
                mab = r['mode_AB']
                fi  = fmap.get(r['flag'], '❓')
                wd  = ('+' if r['wr_diff']  >= 0 else '') + str(round(r['wr_diff'],  1)) + '%'
                pd_ = ('+' if r['pnl_diff'] >= 0 else '') + str(round(r['pnl_diff'], 2)) + '%'
                nf  = r['n_filtered']
                pct = str(round(nf / ma['n'] * 100, 1)) + '%' if ma['n'] > 0 else '0%'

                pf_a_s  = str(round(ma['pf'],  2)) if ma['pf']  != float('inf') else 'inf'
                pf_ab_s = str(round(mab['pf'], 2)) if mab['pf'] != float('inf') else 'inf'

                send(
                    fi + ' [' + str(i) + '/' + str(n) + '] <b>' + sym + '</b>\n'
                    + ' A  : ' + str(ma['n'])  + 'L | WR=' + str(round(ma['wr'],  1)) + '%'
                    + ' | PnL=' + str(round(ma['pnl'],  2)) + '% | PF=' + pf_a_s + '\n'
                    + ' A+B: ' + str(mab['n']) + 'L | WR=<b>' + str(round(mab['wr'], 1)) + '%</b>'
                    + ' | PnL=<b>' + str(round(mab['pnl'], 2)) + '%</b> | PF=' + pf_ab_s + '\n'
                    + ' Chenh: WR=' + wd + ' | PnL=' + pd_ + ' | Loc: ' + str(nf) + 'L (' + pct + ')',
                    chat_id
                )
            except Exception as ex:
                logger.error('btest_b_all ' + sym + ': ' + str(ex))
                send('❌ ' + sym + ': loi - ' + str(ex)[:80], chat_id)

        if not all_results:
            send('❌ Khong co ket qua nao.', chat_id)
            return

        # ── Tong hop ─────────────────────────────────────────────────────────
        n_total    = len(all_results)
        n_improved = sum(1 for r in all_results if r['flag'] in ('V', '~'))
        n_neutral  = sum(1 for r in all_results if r['flag'] == '-')
        n_harmful  = sum(1 for r in all_results if r['flag'] == '!')
        avg_wr_a   = sum(r['mode_A']['wr']   for r in all_results) / n_total
        avg_wr_ab  = sum(r['mode_AB']['wr']  for r in all_results) / n_total
        avg_pnl_a  = sum(r['mode_A']['pnl']  for r in all_results) / n_total
        avg_pnl_ab = sum(r['mode_AB']['pnl'] for r in all_results) / n_total
        wr_diff    = avg_wr_ab  - avg_wr_a
        pnl_diff   = avg_pnl_ab - avg_pnl_a
        avg_filt   = sum(r['n_filtered'] for r in all_results) / n_total

        # Verdict tong
        if n_improved >= n_total * 0.6 and wr_diff >= 1:
            overall_flag = 'V'
            overall      = 'TIN CAY CAO — A+B tot hon A tren da so ma'
        elif n_improved >= n_total * 0.4 and wr_diff >= 0:
            overall_flag = '~'
            overall      = 'CHAP NHAN — A+B co ich, xem dieu chinh penalty cho ma kem'
        elif n_harmful >= n_total * 0.4:
            overall_flag = '!'
            overall      = 'CAN XEM LAI — B-filter lam hai nhieu ma, giam penalty'
        else:
            overall_flag = '-'
            overall      = 'TRUNG TINH — B-filter khong co tac dong dang ke'

        bad_syms = [r['symbol'] for r in all_results if r['flag'] == '!']
        bad_txt  = ''
        if bad_syms:
            bad_txt = '⚠ <b>Can xem lai penalty:</b> ' + ', '.join(bad_syms) + '\n\n'

        # Bang so sanh ngang
        tbl = ''
        for r in sorted(all_results, key=lambda x: x['wr_diff'], reverse=True):
            fi  = fmap.get(r['flag'], '❓')
            wd  = ('+' if r['wr_diff']  >= 0 else '') + str(round(r['wr_diff'],  1)) + '%'
            pd_ = ('+' if r['pnl_diff'] >= 0 else '') + str(round(r['pnl_diff'], 2)) + '%'
            tbl += (fi + ' <b>' + r['symbol'] + '</b>: '
                    + str(round(r['mode_A']['wr'], 1)) + '% → '
                    + '<b>' + str(round(r['mode_AB']['wr'], 1)) + '%</b>'
                    + ' (WR' + wd + ' PnL' + pd_ + ')\n')

        v_icon = fmap.get(overall_flag, '❓')
        wd_s   = ('+' if wr_diff  >= 0 else '') + str(round(wr_diff,  1)) + '%'
        pd_s   = ('+' if pnl_diff >= 0 else '') + str(round(pnl_diff, 2)) + '%'

        send(
            '📊 <b>TONG HOP — ' + str(n_total) + ' MA</b>\n'
            + '=' * 28 + '\n\n'

            + '<b>He thong A (ky thuat thuan):</b>\n'
            + ' WR trung binh : ' + str(round(avg_wr_a,  1)) + '%\n'
            + ' PnL trung binh: ' + str(round(avg_pnl_a, 2)) + '%\n\n'

            + '<b>He thong A+B (+ B-filter):</b>\n'
            + ' WR trung binh : <b>' + str(round(avg_wr_ab,  1)) + '%</b>\n'
            + ' PnL trung binh: <b>' + str(round(avg_pnl_ab, 2)) + '%</b>\n\n'

            + '<b>Chenh lech A+B vs A:</b>\n'
            + ' WR : ' + wd_s + '\n'
            + ' PnL: ' + pd_s + '\n'
            + ' Lenh bi loc TB: ' + str(round(avg_filt, 1)) + ' lenh/ma\n\n'

            + '<b>Ket qua tung ma (sap xep theo WR chenh lech):</b>\n'
            + tbl + '\n'

            + '<b>Phan loai:</b>\n'
            + ' Co ich (V+~): ' + str(n_improved) + '/' + str(n_total) + ' ma\n'
            + ' Trung tinh  : ' + str(n_neutral)  + '/' + str(n_total) + ' ma\n'
            + ' Co hai (!)  : ' + str(n_harmful)  + '/' + str(n_total) + ' ma\n\n'

            + bad_txt
            + v_icon + ' <b>' + overall + '</b>\n\n'
            + '<i>B-filter chi dieu chinh signal cua A, khong tao signal doc lap\n'
            + 'score_min cua tung ma da duoc toi uu qua backtest 7 nam\n'
            + 'Khong phai tu van dau tu</i>',
            chat_id
        )

    threading.Thread(target=run, daemon=True).start()

def handle_bt(args, chat_id):
    """
    /bt <MA>          — Backtest + Walk-Forward gộp, output compact
    /bt <MA> full     — Thêm Entry bias (T vs T+1) + B-filter check
    /bt all           — Toàn bộ watchlist, tóm tắt 1 dòng/mã
    /bt all full      — Watchlist + đầy đủ các checks

    Gộp 6 lệnh cũ: /backtest /wf /dual /btest_b /btest_b_all /lookahead /ma
    """
    import sys, os, importlib
    bot_dir = os.path.dirname(os.path.abspath(__file__))
    if bot_dir not in sys.path:
        sys.path.insert(0, bot_dir)

    parts   = [a.strip().upper() for a in args.split() if a.strip()]
    symbol  = parts[0] if parts else ''
    is_full = 'FULL' in parts
    is_all  = symbol == 'ALL'

    if not symbol:
        send(
            '&#x1F4CA; <b>Lenh /bt — Backtest &amp; Walk-Forward</b>\n'
            '&#x3D;&#x3D;&#x3D;&#x3D;&#x3D;&#x3D;&#x3D;&#x3D;&#x3D;&#x3D;\n\n'
            '<b>Cu phap:</b>\n'
            ' /bt MBB        — Backtest + WF compact (~3 phut)\n'
            ' /bt MBB full   — Them Entry bias + B-filter (~7 phut)\n'
            ' /bt all        — Tom tat 1 dong/ma (~15 phut)\n'
            ' /bt all full   — Watchlist day du (~30 phut)\n\n'
            '<b>Giai thich ket qua:</b>\n'
            ' WR  = Win Rate — ti le lenh thang\n'
            ' OOS = Out-of-Sample — hieu qua thuc te ngoai mau\n'
            ' PF  = Profit Factor — tong loi / tong lo\n'
            ' Decay = IS WR - OOS WR (thap = tot, bot khong overfit)',
            chat_id
        )
        return

    if is_all:
        _handle_bt_all(is_full, chat_id)
    else:
        _handle_bt_symbol(symbol, is_full, chat_id)


def _fmt_verdict(wr, pnl, pf, n, ci_lo=0):
    """Trả về (icon, text) đánh giá tổng thể."""
    if n < 20:
        return '&#x26A0;', 'IT LENH — Chua du de ket luan tin cay'
    if wr >= 60 and pnl >= 2 and pf >= 1.8:
        return '&#x2705;', 'TIN CAY CAO'
    elif wr >= 55 and pnl >= 1 and pf >= 1.3:
        return '&#x1F7E1;', 'CHAP NHAN — On nhung chua xuat sac'
    elif wr >= 50 and pnl >= 0:
        return '&#x1F7E1;', 'TRUNG BINH — Chi tham khao'
    else:
        return '&#x274C;', 'KEM HIEU QUA — Nen xem lai'


def _fmt_decay(decay):
    if decay <= 5:   return '&#x2705; Rat on dinh'
    if decay <= 10:  return '&#x1F7E2; On dinh'
    if decay <= 20:  return '&#x1F7E1; Chap nhan'
    if decay <= 30:  return '&#x26A0; Canh bao overfit'
    return '&#x274C; Co the overfit nghiem trong'


def _handle_bt_symbol(symbol, full_mode, chat_id):
    """Chạy BT+WF cho 1 mã, gửi output compact."""
    mode_txt = ' (full mode)' if full_mode else ''
    eta = '~7 phut' if full_mode else '~3 phut'
    send(
        '&#x1F504; Dang chay <b>Backtest + Walk-Forward ' + symbol + '</b>'
        + mode_txt + '\n'
        + '<i>' + eta + ', vui long cho...</i>',
        chat_id
    )

    def run():
        try:
            import sys, os, importlib   # FIX: import trong thread, không dùng scope ngoài
            bot_dir = os.path.dirname(os.path.abspath(__file__))
            if bot_dir not in sys.path:
                sys.path.insert(0, bot_dir)
            import backtest as bt
            importlib.reload(bt)

            # ── BACKTEST ─────────────────────────────────────────────────────
            res = bt.run_backtest_symbol(symbol, verbose=False)
            if not res or not res.get('buy'):
                send('&#x274C; ' + symbol + ': Khong du du lieu hoac khong co lenh MUA.', chat_id)
                return

            buy   = res['buy']
            cfg_sl    = res.get('sl', 0.07)
            cfg_tp    = res.get('tp', 0.14)
            cfg_score = res.get('min_score', 65)
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
                if yr_n == 0: continue
                icon = '&#x2705;' if yr_wr >= 60 else ('&#x1F7E1;' if yr_wr >= 50 else '&#x274C;')
                ph   = PHASE_SHORT.get(yr, str(yr))
                yr_lines += (
                    f' {icon} <b>{yr}</b> ({ph}): '
                    f'WR={yr_wr:.0f}% PnL={yr_pnl:+.1f}% ({yr_n}L)\n'
                )

            bull_bias   = res.get('yearly', {}).get('bull_bias', 'N/A')
            consistency = res.get('yearly', {}).get('consistency', '')

            # Ngưỡng tối ưu
            best_thr   = res.get('thresh', {}).get('best_threshold', cfg_score)
            thr_note   = (f'Nguong hien tai ({cfg_score}) la toi uu &#x2713;'
                         if best_thr == cfg_score
                         else f'Nguong toi uu la <b>{best_thr}</b> (hien tai {cfg_score})')

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

                + '<b>Tong quan:</b>\n'
                + f' Lenh: {n} | TP: {tp_} | SL: {sl_} | HK: {hk_}\n'
                + f' WR: <b>{wr}%</b> | PnL TB: <b>{pnl:+.2f}%</b>\n'
                + f' PF: <b>{pf_s}</b> | Thang TB: {aw:+.1f}% | Thua TB: {al:+.1f}%\n'
                + f' CI 95%: [{ci_lo}% – {ci_hi}%]\n\n'

                + '<b>Ket qua theo nam:</b>\n'
                + (yr_lines or ' (khong co du lieu)') + '\n'

                + (f'&#x26A0; Bull Bias: <b>{bull_bias}</b>\n' if bull_bias not in ('KHÔNG', 'N/A', '') else '')
                + (f'&#x1F4CC; {consistency[:80]}\n' if consistency else '')
                + '\n'

                + '<b>Nguong score:</b> ' + thr_note + '\n'
                + f' SL=-{cfg_sl*100:.0f}% TP=+{cfg_tp*100:.0f}% | Score&gt;={cfg_score}\n\n'

                + ('<b>3 lenh MUA gan nhat:</b>\n' + recent + '\n' if recent else '')

                + v_icon + ' <b>' + v_txt + '</b>\n'
                + '<i>Chua tinh phi GD ~0.3%. QK khong dam bao TL.</i>'
            )
            send(msg_bt, chat_id)

            # ── WALK-FORWARD ─────────────────────────────────────────────────
            send('&#x1F504; Dang chay <b>Walk-Forward</b> ' + symbol + '...', chat_id)
            wf = bt.run_walk_forward(symbol, verbose=False)

            if not wf:
                send('&#x26A0; ' + symbol + ': Khong du du lieu Walk-Forward (can 3+ nam).', chat_id)
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

                thr_s = ('&#x2705; Nguong on dinh: ' if stable else '&#x26A0; Nguong bien dong: ') + str(thrs)
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
                send('&#x1F504; Dang chay <b>B-Filter Check</b> ' + symbol + '...', chat_id)
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
            send('&#x274C; Loi khi chay BT ' + symbol + ': ' + str(e)[:120], chat_id)

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
            importlib.reload(bt)

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
                    + '<i>Tot = WR&gt;=55% va PnL&gt;0 | Yeu = WR&lt;50% hoac PnL&lt;0\n'
                    + 'Robust = decay WF &lt;= 10%</i>'
                )
                send(summary, chat_id)

        except Exception as e:
            logger.error(f'_handle_bt_all: {e}')
            import traceback
            logger.error(traceback.format_exc())
            send('&#x274C; Loi khi chay BT all: ' + str(e)[:120], chat_id)

    threading.Thread(target=run, daemon=True).start()

def handle_paper(sub_cmd, chat_id):
    """
    /paper         → Danh mục đang mở + P&L thực tế
    /paper report  → Báo cáo tổng kết 2 tháng
    /paper reset   → Xóa toàn bộ (xác nhận trước)
    """
    data   = _update_paper_prices()
    trades = data.get('trades', [])
    today  = datetime.now(VN_TZ).strftime('%Y-%m-%d')

    if sub_cmd == 'report':
        # ── Báo cáo tổng kết ─────────────────────────────────────────────
        closed = [t for t in trades if t['status'] != 'OPEN']
        opened = [t for t in trades if t['status'] == 'OPEN']

        if not trades:
            send('📋 Chua co lenh paper trade nao. '
                 'Lenh se tu dong ghi nhan khi /signals phat MUA.', chat_id)
            return

        tp_list  = [t for t in closed if t['status'] == 'TP']
        sl_list  = [t for t in closed if t['status'] == 'SL']
        exp_list = [t for t in closed if t['status'] == 'EXPIRED']
        n_closed = len(closed)
        wr       = len(tp_list) / n_closed * 100 if n_closed > 0 else 0
        pnls     = [t['pnl_pct'] for t in closed if t['pnl_pct'] is not None]
        avg_pnl  = sum(pnls) / len(pnls) if pnls else 0

        if   wr >= 60 and avg_pnl > 0: verdict = '✅ Hieu qua tot'
        elif wr >= 50 and avg_pnl > 0: verdict = '🟡 Chap nhan duoc'
        else:                           verdict = '❌ Can xem lai'

        # Bảng chi tiết lệnh đóng
        rows = ''
        for t in sorted(closed, key=lambda x: x['entry_date'], reverse=True)[:10]:
            icon = '✅' if t['pnl_pct'] and t['pnl_pct'] > 0 else '❌'
            pnl_s = f"{t['pnl_pct']:+.1f}%" if t['pnl_pct'] is not None else '--'
            rows += (f" {icon} <b>{t['symbol']}</b> "
                     f"{t['entry_date']} @{t['entry_price']:,.0f}d "
                     f"→ {t['exit_reason']} {pnl_s}\n")

        msg = (
            '📊 <b>PAPER TRADING — Bao Cao 2 Thang</b>\n'
            + '=' * 30 + '\n\n'
            + f'<b>Tong lenh:</b> {len(trades)} | Dong: {n_closed} | Mo: {len(opened)}\n'
            + f'<b>Ket qua dong lenh:</b>\n'
            + f' TP: {len(tp_list)} | SL: {len(sl_list)} | Het han: {len(exp_list)}\n'
            + f' Win Rate: <b>{wr:.1f}%</b>\n'
            + f' PnL TB  : <b>{avg_pnl:+.2f}%</b>\n\n'
            + (f'<b>10 lenh gan nhat:</b>\n{rows}\n' if rows else '')
            + f'{verdict}\n\n'
            + f'<i>Paper trading | Khong tinh phi GD | Khong phai tu van dau tu</i>'
        )
        send(msg, chat_id)

    elif sub_cmd == 'reset':
        _save_paper({'trades': [], 'created': datetime.now(VN_TZ).isoformat()})
        send('&#x1F5D1; Da xoa toan bo paper trades.', chat_id)

    else:
        # ── Danh mục đang mở ─────────────────────────────────────────────
        opened = [t for t in trades if t['status'] == 'OPEN']

        if not opened:
            closed_count = len([t for t in trades if t['status'] != 'OPEN'])
            send(
                '📋 <b>Paper Trading — Danh Muc</b>\n\n'
                '🟡 Khong co lenh OPEN nao.\n'
                f'Da dong: {closed_count} lenh\n\n'
                'Lenh tu dong ghi nhan khi /signals phat MUA &gt;= nguong score.\n'
                'Dung /paper report xem bao cao tong ket.',
                chat_id
            )
            return

        rows = ''
        total_pnl = 0
        for t in sorted(opened, key=lambda x: x['entry_date']):
            cur    = t.get('current_price', t['entry_price'])
            pnl    = t.get('pnl_pct', 0) or 0
            total_pnl += pnl
            icon   = '🟢' if pnl >= 0 else '🔴'
            days   = (datetime.now(VN_TZ).date() -
                      datetime.strptime(t['entry_date'], '%Y-%m-%d').date()).days
            settle = t.get('settlement_date', '')
            today_str = datetime.now(VN_TZ).strftime('%Y-%m-%d')
            can_sell  = today_str >= settle if settle else True
            sell_note = '' if can_sell else ' &#x23F3;T+2:' + settle
            rows += (
                f" {icon} <b>{t['symbol']}</b> Score={t['score']} | {days}ng{sell_note}\n"
                f"    Vao: {t['entry_price']:,.0f}d | Hien: {cur:,.0f}d\n"
                f"    PnL: <b>{pnl:+.1f}%</b> | "
                f"SL: {t['sl_price']:,.0f} TP: {t['tp_price']:,.0f}\n"
                f"    Het han: {t['expire_date']}\n\n"
            )

        avg_pnl = total_pnl / len(opened)
        msg = (
            f'📋 <b>Paper Trading — {len(opened)} Lenh Mo</b>\n'
            + f'Cap nhat: {datetime.now(VN_TZ).strftime("%d/%m %H:%M")}\n'
            + '=' * 28 + '\n\n'
            + rows
            + f'PnL TB hien tai: <b>{avg_pnl:+.2f}%</b>\n\n'
            + '<i>Dung /paper report xem bao cao day du</i>'
        )
        send(msg, chat_id)


def handle_lookahead(symbol, chat_id):
    """
    /lookahead VCB — Kiểm tra lookahead bias cho 1 mã.
    """
    send(
        '&#x1F50E; Dang kiem tra <b>Lookahead Bias</b> cho <b>' + symbol + '</b>\n'
        'Kiem tra 200 diem ngau nhien trong 7 nam... (~30 giay)',
        chat_id
    )

    def run():
        try:
            import sys, os, importlib
            bot_dir = os.path.dirname(os.path.abspath(__file__))
            if bot_dir not in sys.path:
                sys.path.insert(0, bot_dir)
            import backtest as bt
            importlib.reload(bt)

            res = bt.run_lookahead_check(symbol, verbose=False)
            if not res:
                send('❌ ' + symbol + ': Khong du du lieu de kiem tra.', chat_id)
                return

            # Icon verdict
            vmap = {'V': '✅', '~': '🟡', '!': '⚠', 'X': '❌'}
            v_icon = vmap.get(res['verdict'], '❓')

            issues     = res.get('issues', [])
            vmap = {'V': '✅', '~': '🟡', '!': '⚠', 'X': '❌'}
            v_icon2 = vmap.get(res['verdict'], '❓')

            # Check 1: Signal bias
            s_ok   = res.get('signal_ok_rate', 0)
            s_icon = '✅' if res.get('signal_bias', 0) == 0 else '❌'

            # Check 2: Entry price bias
            e_flip = res.get('entry_flip_rate', 0)
            e_diff = res.get('avg_entry_diff', 0)
            e_icon = '✅' if e_flip <= 20 else '⚠'

            # Check 3: Indicator bias
            ind_bias = res.get('indicator_bias', 0)
            i_icon   = '✅' if ind_bias == 0 else '❌'

            issues_txt = ''
            for iss in issues:
                issues_txt += f'  ⚠ {iss}\n'

            msg = (
                '&#x1F50E; <b>LOOKAHEAD BIAS: ' + symbol + '</b>\n'
                + '=' * 30 + '\n\n'

                + f'{s_icon} <b>[1] Signal Bias</b>\n'
                + f'   Score phu thuoc closes[idx]: {res.get("signal_total",0) - res.get("signal_bias",0)}'
                + f'/{res.get("signal_total",0)} diem ({s_ok:.1f}%)\n'
                + ('   ✅ Score luon phan ung voi gia — SACH\n' if res.get('signal_bias',0) == 0
                   else f'   ❌ {res.get("signal_bias",0)} diem khong phan ung — CO VAN DE\n')
                + '\n'

                + f'{e_icon} <b>[2] Entry Price Bias</b> (mua T vs T+1)\n'
                + f'   So lenh MUA test: {res.get("entry_total",0)}\n'
                + f'   PnL diff TB: {e_diff:.2f}% | Flip W/L: {e_flip:.1f}%\n'
                + ('   ✅ Flip thap (&lt;=20%) — structural bias khong dang ke\n' if e_flip <= 20
                   else f'   ⚠ {e_flip:.1f}% lenh doi ket qua khi mua T+1 — nen chay /dual de kiem tra\n')
                + '\n'

                + f'{i_icon} <b>[3] Indicator Bias</b> (EMA/MA)\n'
                + f'   Kiem tra {res.get("indicator_total",0)} diem\n'
                + ('   ✅ EMA nhat quan full vs cut array — SACH\n' if ind_bias == 0
                   else f'   ❌ {ind_bias} diem EMA sai lech — CO LOOKAHEAD BUG\n')
                + '\n'

                + (f'<b>Van de phat hien:</b>\n{issues_txt}\n' if issues else '')
                + f'{v_icon2} <b>{res["verdict_txt"]}</b>\n\n'

                + '<i>[1] Signal: score co phu thuoc gia hom nay?\n'
                + '[2] Entry: mua cuoi ngay T hay dau ngay T+1?\n'
                + '[3] Indicator: EMA/MA co bi leak data?\n'
                + 'Khong phai tu van dau tu</i>'
            )
            send(msg, chat_id)

        except Exception as e:
            logger.error(f'handle_lookahead {symbol}: {e}')
            import traceback
            logger.error(traceback.format_exc())
            send('❌ Loi lookahead check ' + symbol + ': ' + str(e)[:120], chat_id)

    threading.Thread(target=run, daemon=True).start()


def handle_start(chat_id):
    msg = (
        '<b>VN Trader Bot v4.4</b> - Chao mung!\n\n'
        'Bo chi so 8 lop thuc chien:\n'
        '1. RSI(14) - Wilder Smoothing\n'
        '2. RSI Phan ky\n'
        '3. MACD\n'
        '4. MA20 & MA50 + Golden/Death Cross\n'
        '5. Bollinger Bands\n'
        '6. Volume thong minh (Ca map)\n'
        '7. Ichimoku\n'
        '8. Ho tro & Khang cu\n\n'
        '<b>Lenh:</b>\n'
        '/price VCB - Gia hien tai\n'
        '/analyze FPT - Phan tich day du 8 lop\n'
        '/whatif VCB 59000 - Neu VCB ve 59k thi sao?\n'
        '/check VCB 85000 - Kiem tra vi the mua tai 85k\n'
        '/bt MBB       — Backtest + WalkForward gop (~3 phut)\n'
        '/bt MBB full  — Them EntryBias + BFilter (~7 phut)\n'
        '/bt all       — Ca watchlist tom tat (~15 phut)\n'
        '/volscan      — Quet vol dot bien 28 ma\n'
        '/volscan TOP  — Top 5 ma vol cao nhat\n'
        '/volscan MBB  — Chi tiet vol 1 ma\n'
        '/fv VCB       — Refresh cache Fair Value (da co trong /analyze)\n'
        '/macro        — Systemic Risk Score (VN market)\n'
        '/paper        — Danh muc paper trading\n'
        '/paper report — Bao cao tong ket\n'
        '/signals      — Top tin hieu hom nay\n'
        '/market       — Chi so thi truong\n\n'
        '<b>Smart Money (Shark Detector v4):</b>\n'
        '/shark DGC    — Phan tich Smart Money: Wyckoff VSA + A/D + Spring + Foreign\n'
        '/sharkbt DGC  — Backtest Shark Score vs Score A (tim nguong toi uu)\n'
        '/scan         — Market Scanner ~250 ma HOSE/HNX (top 10 tiem nang)\n\n'
        '<i>Lenh dev (giu de tuong thich cu): /backtest /wf /dual /btest_b /lookahead /ma\n'
        'Khong phai tu van dau tu</i>'
    )
    send(msg, chat_id)


def handle_price(symbol, chat_id):
    send('Dang lay gia ' + symbol + '...', chat_id)
    d = call_api('/api/price/' + symbol)
    if d.get('price', 0) > 0:
        chg = d.get('change_pct', 0)
        arr = '+' if chg >= 0 else ''
        send('<b>' + symbol + '</b>\nGia: <b>' + f'{d["price"]:,.0f}' + 'd</b>\nThay doi: ' + arr + f'{chg:.2f}%', chat_id)
    else:
        send(symbol + ': ' + d.get('error', 'Khong lay duoc gia'), chat_id)


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
        import market_context as mc

        # Lấy data từ vnstock để tính Wyckoff + Wick
        from backtest import load_data
        import importlib, backtest as bt
        importlib.reload(bt)
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
            + '<i>Phan tich theo Blueprint VN Trading Signal\n'
            + 'Liquidity Tier | Wick Filter | Weekend Rule | Wyckoff Phase</i>'
        )
        send(msg, chat_id)

    except Exception as e:
        logger.error(f'market_context {symbol}: {e}')
        import traceback
        logger.error(traceback.format_exc())


def handle_analyze(symbol, chat_id):
    send('Dang phan tich <b>' + symbol + '</b>...', chat_id)
    d = call_api('/api/analyze/' + symbol)
    if 'error' in d:
        send(symbol + ': ' + d['error'], chat_id)
        return

    def run():
        try:
            import sys, os, importlib, traceback
            bot_dir = os.path.dirname(os.path.abspath(__file__))
            if bot_dir not in sys.path: sys.path.insert(0, bot_dir)
            import market_context as mc
            import backtest as bt
            importlib.reload(bt)

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
                    import market_context as _mc_r
                    b_delta, _bf, b_details = _mc_r.calc_b_adjustment(b_ctx)
                    score_adj = max(0, min(100, score + b_delta))
                except Exception:
                    pass

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
                    '&#x2139; <b>TT VN ' + symbol + ':</b> Khong tai duoc du lieu B-filter\n'
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


def handle_whatif(symbol, target, chat_id):
    send('Dang tinh: Neu <b>' + symbol + '</b> ve <b>' + f'{target:,.0f}' + 'd</b>...', chat_id)
    d = call_api('/api/whatif/' + symbol + '/' + str(int(target)))
    if 'error' in d:
        send(d['error'], chat_id)
        return

    actual = d.get('price', 0)
    if actual > 0:
        dp = (target - actual) / actual * 100
        if dp < -0.5:
            send('Gia hien tai ' + f'{actual:,.0f}' + 'd -&gt; can giam them ' + f'{abs(dp):.1f}%', chat_id)
        elif dp > 0.5:
            send('Gia hien tai ' + f'{actual:,.0f}' + 'd -&gt; da vuot muc nay ' + f'{dp:.1f}%', chat_id)

    send(build_analysis_msg(d, prefix='What-If @' + f'{target:,.0f}' + 'd -'), chat_id)


def handle_check(symbol, buy_price, chat_id):
    send('Dang kiem tra vi the <b>' + symbol + '</b> mua tai <b>' + f'{buy_price:,.0f}' + 'd</b>...', chat_id)
    data = call_api('/api/analyze/' + symbol)
    if not data or 'error' in data:
        send('Khong lay duoc du lieu ' + symbol, chat_id)
        return

    price  = data.get('price', 0)
    ma20   = data.get('ma20', 0)
    ma50   = data.get('ma50', 0)
    score  = data.get('score', 50)
    action = data.get('action', '')
    sups   = data.get('supports', [])
    ress   = data.get('resistances', [])

    # Lấy SL/TP từ WATCHLIST_META nếu có, sau đó từ SYMBOL_SL_TP_CONFIG, cuối là default
    sym_cfg = WATCHLIST_META.get(symbol, {})
    sl_pct  = sym_cfg.get('sl_pct', SYMBOL_SL_TP_CONFIG.get(symbol, {}).get('sl_pct', 7))
    tp_pct  = sym_cfg.get('tp_pct', SYMBOL_SL_TP_CONFIG.get(symbol, {}).get('tp_pct', 14))

    pnl_pct   = (price - buy_price) / buy_price * 100 if buy_price > 0 else 0
    pnl_emoji = '🟢' if pnl_pct >= 0 else '🔴'
    pnl_sign  = '+' if pnl_pct >= 0 else ''

    if buy_price > ma20 and buy_price > ma50:
        ma_pos   = 'Mua tren ca MA20 va MA50 (vung an toan)'
        ma_emoji = '✅'
    elif buy_price > ma20:
        ma_pos   = 'Mua tren MA20 nhung duoi MA50'
        ma_emoji = '⚠'
    elif buy_price > ma50:
        ma_pos   = 'Mua tren MA50 nhung duoi MA20'
        ma_emoji = '⚠'
    else:
        ma_pos   = 'Mua duoi ca MA20 va MA50 (vung rui ro)'
        ma_emoji = '❌'

    ht_txt = 'Chua xac dinh'
    kc_txt = 'Chua xac dinh'
    if sups:
        ht      = sups[0]['price']
        ht_dist = (buy_price - ht) / buy_price * 100
        ht_txt  = f'{ht:,.0f}d ({ht_dist:.1f}% duoi gia mua)'
    if ress:
        kc      = ress[0]['price']
        kc_dist = (kc - buy_price) / buy_price * 100
        kc_txt  = f'{kc:,.0f}d ({kc_dist:.1f}% tren gia mua)'

    sl = round(buy_price * (1 - sl_pct / 100), 0)
    tp = round(buy_price * (1 + tp_pct / 100), 0)

    if action == 'BAN' or score <= 35:
        if pnl_pct > 0:
            rec = '🔴 Nen CHOT LOI - Tin hieu yeu, dang co lai ' + pnl_sign + f'{pnl_pct:.1f}%'
        elif pnl_pct > -sl_pct:
            rec = '🔴 Can nhac CAT LO - Tin hieu xau, lo ' + f'{pnl_pct:.1f}%'
        else:
            rec = '&#x1F198; Da lo qua SL -' + str(sl_pct) + '%, nen THOAT NGAY'
    elif action == 'MUA' or score >= 65:
        rec = '🟢 GIU - Tin hieu con tot (' + str(score) + '/100)'
    else:
        if pnl_pct >= tp_pct:
            rec = '🟡 Gan muc CHOT LOI +' + str(tp_pct) + '%, xem xet ban mot phan'
        elif pnl_pct <= -sl_pct:
            rec = '&#x1F198; Da cham muc CAT LO -' + str(sl_pct) + '%, nen THOAT'
        else:
            rec = '🟡 THEO DOI - Chua co tin hieu ro rang'

    msg = (
        '📋 <b>KIEM TRA VI THE ' + symbol + '</b>\n'
        + '=' * 30 + '\n\n'
        + '<b>Gia mua      :</b> ' + f'{buy_price:,.0f}' + 'd\n'
        + '<b>Gia hien tai :</b> ' + f'{price:,.0f}' + 'd\n'
        + pnl_emoji + ' <b>Lai/Lo       :</b> ' + pnl_sign + f'{pnl_pct:.1f}' + '%\n\n'
        + '<b>Vi tri so voi MA:</b>\n'
        + ' ' + ma_emoji + ' ' + ma_pos + '\n'
        + ' MA20: ' + f'{ma20:,.0f}' + 'd  MA50: ' + f'{ma50:,.0f}' + 'd\n\n'
        + '<b>Ho tro / Khang cu:</b>\n'
        + ' HT gan nhat: ' + ht_txt + '\n'
        + ' KC gan nhat: ' + kc_txt + '\n\n'
        + '<b>SL/TP tu gia mua (' + symbol + '):</b>\n'
        + ' Cat lo (-' + str(sl_pct) + '%): ' + f'{sl:,.0f}' + 'd'
        + (' (da vuot qua)' if price < sl else ' (con ' + f'{price - sl:,.0f}' + 'd dem)') + '\n'
        + ' Chot loi (+' + str(tp_pct) + '%): ' + f'{tp:,.0f}' + 'd'
        + (' (da dat)' if price >= tp else ' (con ' + f'{tp - price:,.0f}' + 'd nua)') + '\n\n'
        + '<b>Tin hieu hien tai:</b> ' + str(score) + '/100 - ' + action + '\n\n'
        + '&#x1F4A1; <b>Khuyen nghi:</b>\n ' + rec + '\n\n'
        + '<i>SL/TP lay tu cau hinh rieng cua ' + symbol
        + ' (SL=-' + str(sl_pct) + '% TP=+' + str(tp_pct) + '%)\n'
        + 'Chi mang tinh tham khao, khong phai tu van dau tu</i>'
    )
    send(msg, chat_id)


def handle_signals(chat_id):
    send('Dang quet tin hieu thi truong...', chat_id)
    data = call_api('/api/signals')
    if not data:
        send('Khong lay duoc tin hieu. Thu lai sau hoac dung:\n/analyze HPG\n/analyze FPT\n/analyze VCB', chat_id)
        return

    # Tính Macro Risk Score nhanh để gắn vào đầu signals
    _macro_prefix = ''
    try:
        import sys, os
        bot_dir = os.path.dirname(os.path.abspath(__file__))
        if bot_dir not in sys.path: sys.path.insert(0, bot_dir)
        import market_context as mc
        import backtest as bt, importlib, numpy as np, pandas as pd
        importlib.reload(bt)
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
        watchlist_scores = [{'symbol': x.get('symbol',''), 'score': x.get('score',50), 'action': x.get('action','')} for x in data]
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
    data_by_sym = {d.get('symbol', ''): d for d in (data or [])}

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
            import market_context as mc
            import backtest as bt_mod, importlib
            importlib.reload(bt_mod)
            df_b, _ = bt_mod.load_data(sym, days=200)
            if df_b is not None:
                ctx_b = mc.build_market_context(df_b, sym,
                            item.get('price', 0),
                            item.get('vol_ratio', 1.0), score)
                # Dùng hàm chung calc_b_adjustment (cộng/trừ nhất quán)
                import market_context as _mc2
                _b_delta, _b_flags, _b_dets = _mc2.calc_b_adjustment(ctx_b)
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
    msg += '(' + str(len(WATCHLIST_META)) + ' ma | Score &gt;= nguong BT | B-filter ON)\n'
    if _macro_prefix:
        msg += _macro_prefix
    msg += '\n'
    buy_symbols = []

    if not wl_signals:
        msg += '🟡 Hom nay chua co tin hieu hop le trong watchlist.\n'
        msg += '(Cac ma co the dang o THEO DOI hoac score chua du nguong)\n'
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
                    f' ✅ Score {score_adj}{adj_txt} &gt;= {meta["score_min"]} (dat nguong)\n'
                    if score_adj >= meta['score_min'] else
                    f' ⚠ Score {score_adj}{adj_txt} (nguong: {meta["score_min"]})\n'
                )
                score_note += f' {b_icon} B-filter: {", ".join(b_warn)}\n'
            else:
                # Không có điều chỉnh B (neutral)
                score_note = (
                    f' ✅ Score {score} &gt;= {meta["score_min"]} (dat nguong)\n'
                    if score >= meta['score_min'] else
                    f' ⚠ Score {score} (nguong: {meta["score_min"]})\n'
                )

            msg += (
                ae + ' <b>' + sym + '</b> — <b>' + action + '</b> (' + str(score) + '/100)\n'
                + meta_line
                + score_note
                + ' Gia: ' + f'{p:,.0f}' + 'd  RSI: ' + str(item.get('rsi', 0)) + '\n'
                + ' ' + vb + ' Vol: ' + f'{vr:.1f}' + 'x  ' + is_ + '\n'
                + (' HT: ' + f'{sups[0]["price"]:,.0f}' if sups else '')
                + (' KC: ' + f'{ress[0]["price"]:,.0f}' if ress else '') + '\n'
                + div_txt + tio_txt + '\n\n'
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
        msg += '&#x23F3; <b>Cho nguong score:</b>\n'
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

    msg += '<i>Khong phai tu van dau tu</i>'
    send(msg, chat_id)

    # ── Market context (B) cho các mã MUA trong watchlist ────────────────────
    if wl_signals:
        mua_items = [(item, meta) for item, meta in wl_signals if item.get('action') == 'MUA']
        if mua_items:
            def send_wl_context():
                try:
                    import sys, os
                    bot_dir = os.path.dirname(os.path.abspath(__file__))
                    if bot_dir not in sys.path: sys.path.insert(0, bot_dir)
                    import market_context as mc
                    import backtest as bt, importlib
                    importlib.reload(bt)

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

    # ── Nếu có tín hiệu MUA → chạy backtest nền, gửi thêm tin nhắn context ──
    if buy_symbols:
        def run_bt_context():
            try:
                import sys, os, importlib
                bot_dir = os.path.dirname(os.path.abspath(__file__))
                if bot_dir not in sys.path:
                    sys.path.insert(0, bot_dir)
                import backtest as bt
                importlib.reload(bt)

                send(
                    '&#x1F50D; Dang kiem tra do tin cay backtest 7 nam cho '
                    + str(len(buy_symbols)) + ' ma MUA... (~'
                    + str(len(buy_symbols) * 25) + 'giay)',
                    chat_id
                )

                bt_lines = ''
                for item in buy_symbols:
                    sym   = item['symbol']
                    score = item['score']
                    try:
                        res = bt.run_backtest_symbol(sym, verbose=False)
                        if not res or not res.get('buy'):
                            bt_lines += f' ❓ <b>{sym}</b>: Chua du du lieu backtest\n\n'
                            continue

                        buy     = res['buy']
                        wr      = buy.get('win_rate', 0)
                        pnl     = buy.get('avg_pnl', 0)
                        pf      = buy.get('profit_factor', 0)
                        total   = buy.get('total', 0)
                        pf_s    = f'{pf:.2f}' if pf != float('inf') else 'inf'

                        # Ngưỡng tối ưu
                        best_thr  = res.get('thresh', {}).get('best_threshold', 65)
                        thr_data  = res.get('thresh', {}).get('results', {}).get(best_thr, {})
                        thr_wr    = thr_data.get('win_rate', wr)
                        thr_pnl   = thr_data.get('avg_pnl', pnl)
                        thr_total = thr_data.get('total', total)

                        # Tín hiệu hiện tại có đạt ngưỡng tối ưu?
                        score_ok = score >= best_thr
                        score_flag = (
                            f'✅ Score {score} &gt;= nguong toi uu {best_thr}'
                            if score_ok else
                            f'⚠ Score {score} &lt; nguong toi uu {best_thr} (WR cao hon o &gt;={best_thr})'
                        )

                        # Time slice: tóm tắt năm tốt/xấu
                        yearly   = res.get('yearly', {}).get('yearly', {})
                        good_yrs = res.get('yearly', {}).get('good_years', [])
                        bad_yrs  = res.get('yearly', {}).get('bad_years', [])
                        bull_bias = res.get('yearly', {}).get('bull_bias', 'N/A')
                        yr_summary = ''
                        if yearly:
                            # Chỉ hiện 3 năm gần nhất
                            recent_yrs = sorted(yearly.keys())[-3:]
                            for yr in recent_yrs:
                                d = yearly[yr]
                                y_wr = d.get('win_rate', 0)
                                y_pnl = d.get('avg_pnl', 0)
                                ico = '✅' if y_wr >= 55 and y_pnl >= 0 else ('❌' if y_wr < 45 else '🟡')
                                yr_summary += f'  {ico} {yr}: WR={y_wr:.0f}% PnL={y_pnl:+.1f}%\n'
                            yr_ok_str  = ', '.join(str(y) for y in good_yrs[-3:]) if good_yrs else 'Khong co'
                            yr_bad_str = ', '.join(str(y) for y in bad_yrs[-3:])  if bad_yrs  else 'Khong co'

                        # Verdict backtest
                        if wr >= 58 and pnl >= 2 and pf >= 1.5:
                            bt_verdict = '✅ TIN CAY CAO — Backtest xac nhan tin hieu nay'
                        elif wr >= 52 and pnl >= 0:
                            bt_verdict = '🟡 CHAP NHAN — Backtest ung ho nhung khong manh'
                        else:
                            bt_verdict = '❌ CANH BAO — Backtest 7 nam cho thay tin hieu KEM tin cay'

                        bt_lines += (
                            f'📊 <b>{sym}</b> (Score hom nay: {score}/100)\n'
                            + f' WR tong the : <b>{wr:.1f}%</b> ({total} lenh / 7 nam)\n'
                            + f' PnL TB      : <b>{pnl:+.2f}%</b> | PF: {pf_s}\n'
                        )
                        if best_thr != 65:
                            bt_lines += (
                                f' Nguong toi uu: <b>&gt;={best_thr}</b> → '
                                f'WR={thr_wr:.0f}% PnL={thr_pnl:+.1f}% ({thr_total}L)\n'
                            )
                        bt_lines += f' {score_flag}\n'
                        if yr_summary:
                            bt_lines += f' 3 nam gan nhat:\n{yr_summary}'
                            bt_lines += (
                                f' Nam tot: {yr_ok_str} | '
                                f'Nam xau: {yr_bad_str}\n'
                            )
                        if bull_bias == 'NGHIÊM TRỌNG':
                            bt_lines += ' 🔴 Bull Bias: ket qua bi thoi phong boi 2021\n'
                        bt_lines += f' {bt_verdict}\n\n'

                    except Exception as e:
                        bt_lines += f' ❓ <b>{sym}</b>: Loi backtest ({str(e)[:60]})\n\n'
                        logger.error(f'signals backtest {sym}: {e}')

                if bt_lines:
                    send(
                        '🧪 <b>BACKTEST CONTEXT — Do Tin Cay 5 Nam</b>\n'
                        + '(Chi danh cho tin hieu MUA hom nay)\n'
                        + '=' * 30 + '\n\n'
                        + bt_lines
                        + '<i>Nguong toi uu = score co WR/PnL cao nhat trong backtest 7 nam\n'
                        + 'Khong phai tu van dau tu</i>',
                        chat_id
                    )

            except Exception as e:
                logger.error(f'signals bt_context: {e}')
                import traceback
                logger.error(traceback.format_exc())

        threading.Thread(target=run_bt_context, daemon=True).start()


def handle_market(chat_id):
    send('Dang lay chi so...', chat_id)
    data = call_api('/api/market')
    msg = '<b>Chi so thi truong</b>\n\n'

    for key, val in data.items():
        if isinstance(val, dict):
            p = val.get('price', 0)
            chg = val.get('change_pct', 0)
            arr = '+' if chg >= 0 else ''
            msg += '<b>' + val.get('name', key) + '</b>: ' + f'{p:,.2f}' + ' (' + arr + f'{chg:.2f}%)\n'

    if msg == '<b>Chi so thi truong</b>\n\n':
        msg += 'Khong lay duoc du lieu.'
    send(msg, chat_id)


def handle_macro(chat_id):
    """
    /macro — Systemic Risk Score: đánh giá rủi ro vĩ mô thị trường VN.
    Kết hợp VNINDEX trend + market breadth + volatility + weekend risk.
    """
    send('📊 Dang tinh <b>Macro Risk Score</b>...', chat_id)
    try:
        import sys, os
        bot_dir = os.path.dirname(os.path.abspath(__file__))
        if bot_dir not in sys.path: sys.path.insert(0, bot_dir)
        import market_context as mc

        # Lấy VNINDEX data
        market_data = call_api('/api/market')
        vn_raw = market_data.get('VNINDEX', {}) if market_data else {}

        # Lấy VNINDEX MA20 từ backtest module
        import backtest as bt, importlib
        importlib.reload(bt)
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
        watchlist_scores = [
            {'symbol': x.get('symbol',''), 'score': x.get('score',50), 'action': x.get('action','')}
            for x in signals_data
        ] if signals_data else []

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
            + 'Khong phai tu van dau tu</i>'
        )
        send(msg, chat_id)

    except Exception as e:
        logger.error(f'handle_macro: {e}')
        import traceback
        logger.error(traceback.format_exc())
        send('❌ Loi macro risk: ' + str(e)[:120], chat_id)


def poll_updates():
    if not TOKEN:
        logger.error('Khong co TOKEN')
        return

    # Khởi tạo DB table khi bot start
    _init_db()
    logger.info('Bot v4.4 polling... (RS+Scanner+SharkV4+AutoShark ready)')
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
                        send('⚠ Ban la owner, khong the huy.', cid)
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
                elif cmd == '/analyze':
                    handle_analyze(parts[1].upper() if len(parts) > 1 else 'VCB', cid)
                elif cmd == '/shark':
                    handle_shark(parts[1].upper() if len(parts) > 1 else 'DGC', cid)
                elif cmd == '/sharkbt':
                    handle_shark_backtest(parts[1].upper() if len(parts) > 1 else 'DGC', cid)
                elif cmd == '/scan':
                    handle_market_scan(cid)
                elif cmd == '/whatif':
                    if len(parts) < 3:
                        send('Cu phap: <b>/whatif MA GIA</b>\nVi du: <b>/whatif VCB 59000</b>', cid)
                    else:
                        try:
                            # FIX: Parse price đúng cách, hỗ trợ cả "59.5k", "59500", "59,500"
                            raw = parts[2].replace(',', '')
                            # Xử lý hậu tố k/K (nghìn đồng)
                            if raw.lower().endswith('k'):
                                target = float(raw[:-1]) * 1000
                            else:
                                target = float(raw)
                            # Nếu nhập dạng đơn vị nghìn (< 1000) thì nhân 1000
                            if target < 1000:
                                target *= 1000
                            handle_whatif(parts[1].upper(), target, cid)
                        except ValueError:
                            send('Gia khong hop le.\nVi du: <b>/whatif VCB 59000</b> hoac <b>/whatif VCB 59k</b>', cid)
                elif cmd == '/check':
                    if len(parts) >= 3:
                        try:
                            raw = parts[2].replace(',', '')
                            if raw.lower().endswith('k'):
                                buy_price = float(raw[:-1]) * 1000
                            else:
                                buy_price = float(raw)
                            if buy_price < 1000:
                                buy_price *= 1000
                            handle_check(parts[1].upper(), buy_price, cid)
                        except ValueError:
                            send('Gia khong hop le. VD: <b>/check VCB 85000</b> hoac <b>/check VCB 85k</b>', cid)
                    else:
                        send('Cu phap: <b>/check MA GIA_MUA</b>\nVi du: <b>/check VCB 85000</b>', cid)
                elif cmd == '/signals':
                    handle_signals(cid)
                elif cmd == '/market':
                    handle_market(cid)
                elif cmd == '/macro':
                    handle_macro(cid)
                elif cmd == '/bt':
                    arg = ' '.join(parts[1:]) if len(parts) > 1 else ''
                    handle_bt(arg, cid)
                elif cmd == '/backtest':
                    if len(parts) < 2 or not parts[1].strip():
                        send('Cu phap: <b>/backtest MA</b>\n'
                             'Vi du: <b>/backtest VCB</b>\n\n'
                             'Bot se kiem tra tin hieu 7 nam qua cho 1 ma.', cid)
                    else:
                        handle_backtest(parts[1].upper(), cid)
                elif cmd == '/wf':
                    if len(parts) < 2 or not parts[1].strip():
                        send('Cu phap: <b>/wf MA</b>\n'
                             'Vi du: <b>/wf VCB</b>\n\n'
                             'Walk-Forward: kiem tra he thong co hoat dong tot\n'
                             'tren du lieu chua tung thay (OOS) hay khong.\n'
                             'IS=2nam → OOS=1nam | ~2 phut.', cid)
                    else:
                        handle_wf(parts[1].upper(), cid)
                elif cmd == '/dual':
                    if len(parts) < 2 or not parts[1].strip():
                        send('Cu phap: <b>/dual MA</b>\n'
                             'Vi du: <b>/dual VCB</b>\n\n'
                             'Chay backtest 2 mode song song:\n'
                             '  Entry=T   : mua tai close[i] (backtest chuan)\n'
                             '  Entry=T+1 : mua tai close[i+1] (thuc te hon)\n'
                             'Du kien ~3 phut.', cid)
                    else:
                        handle_dual(parts[1].upper(), cid)
                elif cmd == '/btest_b':
                    if len(parts) < 2 or not parts[1].strip():
                        send('Cu phap: <b>/btest_b MA</b> hoac <b>/btest_b all</b>\n'
                             'Vi du: <b>/btest_b VCB</b> — 1 ma (~7 phut)\n'
                             '        <b>/btest_b all</b> — 28 ma (~25 phut)\n\n'
                             'So sanh Mode A (ky thuat thuan) vs Mode A+B (+ B-filter)\n'
                             'Kiem chung B-filter co gia tri thuc su khong.', cid)
                    elif parts[1].lower() == 'all':
                        handle_btest_b_all(cid)
                    else:
                        handle_btest_b(parts[1].upper(), cid)
                elif cmd == '/paper':
                    sub = parts[1].lower() if len(parts) > 1 else ''
                    handle_paper(sub, cid)
                elif cmd == '/lookahead':
                    if len(parts) < 2 or not parts[1].strip():
                        send('Cu phap: <b>/lookahead MA</b>\n'
                             'Vi du: <b>/lookahead VCB</b>\n\n'
                             'Kiem tra lookahead bias: phat hien bot co\n'
                             '"nhin truoc" du lieu tuong lai khong (~30 giay).', cid)
                    else:
                        handle_lookahead(parts[1].upper(), cid)
                elif cmd == '/fv':
                    if len(parts) < 2 or not parts[1].strip():
                        send('&#x1F4CA; <b>Fair Value</b> da duoc tich hop vao <b>/analyze</b>\n\n'
                             'Xem FV: <b>/analyze MBB</b> (phan cuoi ket qua)\n\n'
                             'Neu muon <b>refresh cache</b> FV: <b>/fv MBB</b>\n'
                             '(Cap nhat tu BCTC VCI, xu ly 15-30 giay)', cid)
                    else:
                        # /fv giờ chỉ dùng để force-refresh cache
                        send('&#x1F504; Dang refresh Fair Value <b>' + parts[1].upper() + '</b>...', cid)
                        handle_fv(parts[1].upper(), cid)
                elif cmd == '/ma':
                    if len(parts) < 2 or not parts[1].strip():
                        send('Cu phap: <b>/ma MA</b>\n'
                             'Vi du: <b>/ma MBB</b>\n\n'
                             'Backtest chien luoc MA doc lap:\n'
                             '  MA10 Cross: Mua khi gia cat len MA10, TP 8%\n'
                             '  MA50 Trend: Mua khi gia len tren MA50, TP 25%\n'
                             '  Combined  : MA10 cross + dang tren MA50, TP 10%\n\n'
                             'Ket qua doc lap, khong anh huong score hien tai.', cid)
                    else:
                        handle_ma_backtest(parts[1].upper(), cid)
                elif cmd == '/volscan':
                    # /volscan        → quét 28 mã (watchlist + extended)
                    # /volscan top    → chỉ hiện top spike
                    # /volscan <MA>   → chi tiết 1 mã
                    arg = parts[1].upper() if len(parts) > 1 else ''
                    handle_volscan(arg, cid)
                else:
                    send('Lenh khong nhan ra. Go /help de xem danh sach.', cid)

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
_last_shark_alerts = {}  # Cooldown riêng cho shark alerts


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
                'nen cho xac nhan sau 11:00 truoc khi vao lenh</i>'
            )
        else:
            return (
                '\n⚠ <i>' + time_label + ': Vol chua du tin cay '
                '(' + f'{vol_ratio:.1f}' + 'x &lt; ' + f'{threshold:.1f}' + 'x can thiet). '
                'Cho den sau 11:00 de xac nhan</i>'
            )
    elif h < 14:
        threshold = 1.8
        time_label = '&#x23F3; Vol giua phien (' + f'{h:02d}:{m:02d}' + ')'
        if vol_ratio >= threshold:
            return (
                '\n🟡 <i>' + time_label + ': Vol ' + f'{vol_ratio:.1f}' + 'x — '
                'kha manh, co the vao lenh nhung chua toan dien. '
                'Vol cuoi phien se chac chan hon</i>'
            )
        else:
            return (
                '\n⚠ <i>' + time_label + ': Vol chua day du '
                '(' + f'{vol_ratio:.1f}' + 'x &lt; ' + f'{threshold:.1f}' + 'x can thiet). '
                'Tin hieu volume chua dang tin — cho them</i>'
            )
    else:
        # Sau 14:00 — nến gần đóng, ngưỡng bình thường
        if vol_ratio >= 2.5:
            return '\n✅ <i>Vol dot bien ' + f'{vol_ratio:.1f}' + 'x — tin hieu manh nhat trong ngay</i>'
        elif vol_ratio >= 1.5:
            return '\n&#x2139; <i>Vol ' + f'{vol_ratio:.1f}' + 'x — xac nhan tot</i>'
        else:
            return ''   # Vol thấp không cần ghi chú thêm


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
        confirms.append('Phan ky RSI &#x1F514;')
    if confirms:
        lines.append('&#x1F4A1; ' + ' | '.join(confirms))

    # RSI ngắn gọn
    lines.append(f'RSI: {rsi:.0f}')

    # Hỗ trợ/kháng cự
    if is_buy and sups:
        lines.append('&#x2795; HT: ' + f'{sups[0]["price"]:,.0f}' + 'd')
    if not is_buy and ress:
        lines.append('&#x2796; KC: ' + f'{ress[0]["price"]:,.0f}' + 'd')

    lines += ['', '&#x1F50E; /analyze ' + sym + ' de xem chi tiet',
              '<i>Chi mang tinh tham khao, khong phai tu van</i>']

    return NL.join(lines)


# ── Cấu hình Scanner ────────────────────────────────────────────────────────
SCAN_INTERVAL_MIN      = 10    # Quét signal MUA/BAN mỗi 10 phút trong giờ giao dịch
ALERT_COOLDOWN_SEC     = 5400  # Không alert lại cùng mã
SHARK_SCAN_INTERVAL    = 60    # Quét Shark Score mỗi 60 phút cho watchlist
SHARK_ALERT_THRESHOLD  = 60    # Shark >= 60 → alert subscribers
SHARK_COOLDOWN_SEC     = 14400 # Không alert lại cùng mã trong 4 giờ trong 90 phút

# ── Vol Spike 1H alert config ────────────────────────────────────────────────
VOL_SPIKE_SCAN_MIN     = 15    # Quét vol spike 1H mỗi 15 phút (đủ nhạy, không spam)
VOL_SPIKE_COOLDOWN_SEC = 3600  # Không alert vol spike cùng mã trong 60 phút
_last_vol_spike        = {}    # {symbol: (direction, timestamp)}

# ── MA10 / MA50 Scanner config ───────────────────────────────────────────────
MA_SCAN_INTERVAL_MIN  = 10
MA_CROSS_COOLDOWN_SEC = 14400
_MA_ALERTS_FILE       = '/tmp/ma_alerts_state.json'

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
WATCHLIST_EXTENDED = [
    # Ngân hàng / Chứng khoán
    'VCB', 'BID', 'TCB', 'VPB', 'SSI', 'VND',
    # Bất động sản
    'VHM', 'VIC', 'NVL', 'PDR',
    # Công nghệ
    'FPT', 'CMG',
    # Thép
    'HPG', 'HSG', 'NKG',
    # Dầu khí / Điện
    'GAS', 'PVD', 'PVS', 'POW', 'REE',
    # Tiêu dùng
    'MWG', 'FRT', 'VNM', 'MSN',
    # KCN / Khác
    'KBC', 'SZC',
]
# Cooldown dài hơn tầng 1 để giảm noise từ mã chưa được calibrate
MA_EXT_COOLDOWN_SEC = 28800  # 8 tiếng cho mã tầng 2

def handle_ma_backtest(symbol, chat_id):
    """
    /ma MBB — Chạy backtest 3 chiến lược MA độc lập và gửi kết quả.
    Chạy trong thread riêng vì mất ~30 giây.
    """
    send('📊 Dang chay <b>MA Strategy Backtest</b> cho <b>'
         + symbol + '</b>... (~30 giay)', chat_id)

    def run():
        try:
            import backtest as bt
            results = {}
            configs = [
                ('MA10',     0.08, 0.05, 15, 'Cat len MA10, TP 8% SL 5%'),
                ('MA50',     0.25, 0.08, 40, 'Tren MA50 doc len, TP 25% SL 8%'),
                ('COMBINED', 0.10, 0.05, 15, 'MA10 cross + tren MA50, TP 10% SL 5%'),
            ]
            for strat, tp, sl, hold, desc in configs:
                r = bt.run_ma_strategy_backtest(
                    symbol, strat, tp_pct=tp, sl_pct=sl,
                    hold_days=hold, verbose=False
                )
                results[strat] = (r, desc)

            # So sánh với hệ thống score hiện tại
            base = bt.run_backtest_symbol(symbol, verbose=False)
            base_wr  = base['buy']['win_rate']  if base and base.get('buy') else 0
            base_pnl = base['buy']['avg_pnl']   if base and base.get('buy') else 0
            base_n   = base['buy']['total']      if base and base.get('buy') else 0

            msg = ('📊 <b>MA STRATEGY: ' + symbol + '</b>\n'
                   + '=' * 30 + '\n\n')

            for strat, (r, desc) in results.items():
                if r and r.get('stats'):
                    st = r['stats']
                    pf_s = f"{st['profit_factor']:.2f}" if st['profit_factor'] != float('inf') else 'inf'
                    flag = '✅' if st['win_rate'] >= 55 else ('❌' if st['win_rate'] < 45 else '🟡')
                    msg += (flag + ' <b>' + strat + '</b>: ' + desc + '\n'
                            + '  WR=' + f"{st['win_rate']:.1f}%" + ' PnL=' + f"{st['avg_pnl']:+.2f}%"
                            + ' PF=' + pf_s + ' (' + str(st['total']) + 'L)\n\n')
                else:
                    msg += '❓ <b>' + strat + '</b>: Khong du tin hieu\n\n'

            # Hệ thống hiện tại
            flag_base = '✅' if base_wr >= 55 else ('❌' if base_wr < 45 else '🟡')
            msg += (flag_base + ' <b>Score tong hop (hien tai)</b>:\n'
                    + '  WR=' + f"{base_wr:.1f}%" + ' PnL=' + f"{base_pnl:+.2f}%"
                    + ' (' + str(base_n) + 'L)\n\n')

            # Kết luận
            all_wrs = [(strat, r['stats']['win_rate'])
                       for strat, (r, _) in results.items()
                       if r and r.get('stats')]
            all_wrs.append(('Score', base_wr))
            if all_wrs:
                best = max(all_wrs, key=lambda x: x[1])
                msg += ('🏆 <b>Hieu qua nhat:</b> ' + best[0]
                        + ' (' + f"{best[1]:.1f}%" + ')\n')
            msg += '\n<i>Ket qua doc lap — chua tinh phi GD. Khong phai tu van dau tu.</i>'
            send(msg, chat_id)

        except Exception as e:
            logger.error(f'handle_ma_backtest {symbol}: {e}')
            send('❌ Loi khi chay MA backtest ' + symbol + ': ' + str(e)[:100], chat_id)

    threading.Thread(target=run, daemon=True).start()


def format_vol_spike_alert(sym, warning, score, price):
    """Format tin nhắn alert Volume Spike 1H."""
    level   = warning.get('level', 'MEDIUM')
    msg     = warning.get('message', '')
    emoji   = '🚨💰' if level == 'HIGH' else '⚠💰'
    score_s = str(score) + '/100' if score else '—'
    return (
        emoji + ' <b>VOL SPIKE 1H: ' + sym + '</b>\n'
        + '=' * 28 + '\n'
        + ' Gia: <b>' + f'{price:,.0f}' + 'd</b>'
        + ' | Score 1D: <b>' + score_s + '</b>\n\n'
        + ' 📊 ' + escape_html(msg) + '\n\n'
        + 'Dung /analyze ' + sym + ' de xem phan tich day du\n'
        + '<i>Canh bao dong tien 1H — chi tham khao</i>'
    )


def format_ma_alert(sym, event, price, score, ma10, ma50, ma50_slope_up, tier=1):
    """
    Format alert MA10 cross hoặc MA50 uptrend.
    event: 'MA10_CROSS_UP' | 'MA10_CROSS_DOWN' | 'MA50_UPTREND' | 'MA50_LOST'
    tier:  1 = watchlist chính, 2 = watchlist mở rộng (chưa backtest)
    """
    tier_note = '' if tier == 1 else '\n<i>⚠ Chua backtest — chi tham khao them</i>'

    if event == 'MA10_CROSS_UP':
        emoji  = '⚡🟢'
        title  = 'MA10 CROSS UP — NGAN HAN'
        detail = (f'Gia ({price:,.0f}d) vua cat LEN MA10 ({ma10:,.0f}d) hom nay\n'
                  f' -> Momentum ngan han phuc hoi\n'
                  f' -> TP tham khao: +7% den +10%')
    elif event == 'MA10_CROSS_DOWN':
        emoji  = '⚡🔴'
        title  = 'MA10 CROSS DOWN — MAT DONG LUC'
        detail = (f'Gia ({price:,.0f}d) vua cat XUONG MA10 ({ma10:,.0f}d) hom nay\n'
                  f' -> Dong luc ngan han giam\n'
                  f' -> Neu dang so huu: can than, xem xet chot loi')
    elif event == 'MA50_UPTREND':
        dist = (price - ma50) / ma50 * 100 if ma50 > 0 else 0
        emoji  = '📈🟢'
        title  = 'MA50 UPTREND — TRUNG HAN'
        detail = (f'Gia ({price:,.0f}d) tren MA50 ({ma50:,.0f}d) +{dist:.1f}% + MA50 doc len\n'
                  f' -> Uptrend trung han xac nhan\n'
                  f' -> TP tham khao: +25% den +30%')
    elif event == 'MA50_LOST':
        emoji  = '📉🔴'
        title  = 'MA50 LOST — TRUNG HAN SUY YEU'
        detail = (f'Gia ({price:,.0f}d) vua ro XUONG MA50 ({ma50:,.0f}d)\n'
                  f' -> Uptrend trung han co the ket thuc\n'
                  f' -> Nen xem xet giam vi the hoac dat SL chat hon')
    else:
        return None

    score_s = str(score) + '/100' if score else '—'
    return (
        emoji + ' <b>' + title + ': ' + sym + '</b>\n'
        + '=' * 28 + '\n'
        + ' Score 1D: <b>' + score_s + '</b>\n\n'
        + ' ' + escape_html(detail)
        + tier_note + '\n\n'
        + 'Dung /analyze ' + sym + ' de xem phan tich day du\n'
        + '<i>Chi tham khao — khong phai tu van dau tu</i>'
    )


def handle_volscan(arg, chat_id):
    """
    /volscan        — Quét volume đột biến toàn bộ 28 mã trong phiên vừa rồi
    /volscan top    — Chỉ hiện top 5 mã spike cao nhất
    /volscan <MA>   — Chi tiết volume 1 mã cụ thể
    """
    # Xác định chế độ
    all_syms = list(WATCHLIST_META.keys()) + [
        s for s in WATCHLIST_EXTENDED if s not in WATCHLIST_META
    ]

    # Nếu là tên mã cụ thể
    if arg and arg not in ('TOP', ''):
        send('&#x1F4CA; Dang quet volume <b>' + arg + '</b>...', chat_id)
        _volscan_single(arg, chat_id)
        return

    top_only = (arg == 'TOP')
    label = 'Top Vol Spike' if top_only else 'Quet Volume Dot Bien'
    send('&#x1F50E; <b>' + label + ' — ' + str(len(all_syms)) + ' ma...</b>\n'
         '<i>(~20-40 giay)</i>', chat_id)

    def run():
        try:
            results = []
            # Bước 1: Gọi /api/volscan bulk — đọc cache 28 mã, < 1s
            # Endpoint này không trigger FA, không gọi vnstock mới
            bulk_map = {}
            try:
                bulk = call_api('/api/volscan')
                if bulk and isinstance(bulk, list):
                    for item in bulk:
                        s2 = item.get('symbol', '')
                        if s2:
                            bulk_map[s2] = item
                logger.info(f'volscan: bulk /api/volscan got {len(bulk_map)} symbols')
            except Exception as e:
                logger.warning(f'volscan bulk failed: {e}')

            # Bước 2: Với mã chưa có trong bulk, gọi /api/vol/<sym> (nhẹ, không FA)
            for sym in all_syms:
                try:
                    if sym in bulk_map:
                        d = bulk_map[sym]  # Từ cache — tức thì
                    else:
                        # /api/vol/<sym>: trả vol data nhanh, không trigger FA
                        d = call_api('/api/vol/' + sym)
                        if not d or d.get('error'):
                            # Fallback cuối: /api/analyze nếu thực sự cần
                            d = call_api('/api/analyze/' + sym)
                    if not d or d.get('error'):
                        continue
                    vr    = d.get('vol_ratio', 0)
                    vol   = d.get('vol_today', 0)
                    tb20  = d.get('vol_tb20', 0)
                    price = d.get('price', 0)
                    score = d.get('score', 0)
                    action = d.get('action', '')
                    wt    = d.get('weekly_trend', '')
                    is_wl = sym in WATCHLIST_META
                    results.append({
                        'sym': sym, 'vr': vr, 'vol': vol, 'tb20': tb20,
                        'price': price, 'score': score, 'action': action,
                        'wt': wt, 'is_wl': is_wl,
                    })
                    # Không sleep cho mã từ cache
                    # Sleep nhỏ chỉ khi gọi API mới tránh rate limit
                    if sym not in bulk_map:
                        time.sleep(0.5)
                except requests.exceptions.Timeout:
                    logger.warning(f'volscan {sym}: timeout, bo qua')
                    continue
                except Exception as e:
                    logger.warning(f'volscan {sym}: {e}')

            if not results:
                send('&#x274C; Khong lay duoc du lieu.', chat_id)
                return

            # Sắp xếp theo vol_ratio giảm dần
            results.sort(key=lambda x: x['vr'], reverse=True)

            # Top 5 nếu top_only
            if top_only:
                results = results[:5]

            # Build message
            now_str = datetime.now(VN_TZ).strftime('%H:%M %d/%m')
            msg = '&#x1F4CA; <b>Vol Scan — ' + now_str + '</b>\n'
            msg += '=' * 30 + '\n'

            # Nhóm theo mức độ
            spike_high  = [r for r in results if r['vr'] >= 2.5]
            spike_med   = [r for r in results if 1.5 <= r['vr'] < 2.5]
            spike_norm  = [r for r in results if 1.2 <= r['vr'] < 1.5]
            below_avg   = [r for r in results if r['vr'] < 0.5]

            def fmt_row(r):
                vr = r['vr']
                if vr >= 2.5:
                    v_icon = '&#x1F525;&#x1F525;'   # 🔥🔥 rất cao
                elif vr >= 1.5:
                    v_icon = '&#x1F525;'             # 🔥 cao
                elif vr >= 1.2:
                    v_icon = '&#x2B06;'              # ⬆ trên TB
                elif vr >= 0.8:
                    v_icon = '&#x27A1;'              # ➡ bình thường
                else:
                    v_icon = '&#x2B07;'              # ⬇ yếu

                wl_mark = '&#x2B50;' if r['is_wl'] else ''  # ⭐ watchlist
                wt_map = {'STRONG_UP':'&#x1F7E2;','UP':'&#x1F7E2;',
                          'WEAK_UP':'&#x1F7E1;','PULLBACK':'&#x1F7E1;','DOWN':'&#x1F534;'}
                wt_icon = wt_map.get(r['wt'], '')

                return (f" {v_icon} <b>{r['sym']}</b>{wl_mark} "
                        f"{wt_icon} {r['vr']:.1f}x | "
                        f"{r['price']:,.0f}d | "
                        f"Score {r['score']} {r['action']}\n")

            if spike_high:
                msg += '\n&#x1F525;&#x1F525; <b>DOT BIEN MANH (&#x2265;2.5x):</b>\n'
                for r in spike_high:
                    msg += fmt_row(r)

            if spike_med:
                msg += '\n&#x1F525; <b>TANG MANH (1.5-2.5x):</b>\n'
                for r in spike_med:
                    msg += fmt_row(r)

            if spike_norm and not top_only:
                msg += '\n&#x2B06; <b>TREN TRUNG BINH (1.2-1.5x):</b>\n'
                for r in spike_norm:
                    msg += fmt_row(r)

            if below_avg and not top_only:
                msg += '\n&#x2B07; <b>VOL YEU (&#x3C;0.5x):</b>\n'
                for r in below_avg[:5]:  # Chỉ hiện 5 mã yếu nhất
                    msg += fmt_row(r)
                if len(below_avg) > 5:
                    msg += f' ... va {len(below_avg)-5} ma khac\n'

            if not spike_high and not spike_med:
                msg += '\n<i>Khong co ma nao co vol dot bien dang ke trong phien nay.</i>\n'

            # Tóm tắt
            msg += '\n' + '─' * 28 + '\n'
            msg += (f'&#x2B50; Watchlist (5) | &#x25CB; Mo rong ({len(WATCHLIST_EXTENDED)})\n'
                    f'Vol >2.5x: {len(spike_high)} ma | >1.5x: {len(spike_med)} ma\n'
                    f'<i>Dung /volscan TOP xem nhanh 5 ma cao nhat</i>\n'
                    f'<i>Dung /volscan MBB de xem chi tiet 1 ma</i>')

            send(msg, chat_id)

        except Exception as e:
            logger.error(f'handle_volscan: {e}')
            send('&#x274C; Loi khi quet volume: ' + str(e)[:100], chat_id)

    threading.Thread(target=run, daemon=True).start()


def _volscan_single(symbol, chat_id):
    """Chi tiết volume 1 mã: vol hôm nay vs TB5, TB10, TB20."""
    def run():
        try:
            d = call_api('/api/analyze/' + symbol)
            if not d or d.get('error'):
                send('&#x274C; Khong lay duoc du lieu ' + symbol, chat_id)
                return

            vr     = d.get('vol_ratio', 0)
            vol    = d.get('vol_today', 0)
            tb20   = d.get('vol_tb20', 0)
            price  = d.get('price', 0)
            score  = d.get('score', 0)
            action = d.get('action', '')
            wt     = escape_html(d.get('weekly_trend_vn', ''))
            signals = d.get('signals', [])

            # Vol signal từ signals
            vol_sig = next((txt for g, t, txt in signals if g == 'VOL'), '')

            if vr >= 2.5:
                level_txt = '&#x1F525;&#x1F525; DOT BIEN MANH'
                level_color = 'cao bat thuong'
            elif vr >= 1.5:
                level_txt = '&#x1F525; TANG MANH'
                level_color = 'tren trung binh ro ret'
            elif vr >= 1.2:
                level_txt = '&#x2B06; TREN TB'
                level_color = 'nhe'
            elif vr >= 0.8:
                level_txt = '&#x27A1; BINH THUONG'
                level_color = 'trong vung binh thuong'
            else:
                level_txt = '&#x2B07; THAP'
                level_color = 'duoi trung binh'

            msg = (
                '&#x1F4CA; <b>VOL DETAIL: ' + symbol + '</b>\n'
                + '=' * 28 + '\n\n'
                + ' Vol hom nay : <b>' + fmt_vol(vol) + '</b>\n'
                + ' TB20 phien  : <b>' + fmt_vol(tb20) + '</b>\n'
                + ' Ti le Vol   : <b>' + f'{vr:.2f}x</b> — ' + level_color + '\n'
                + ' Muc do      : ' + level_txt + '\n\n'
                + ' Gia hien tai: <b>' + f'{price:,.0f}d</b>\n'
                + ' Score       : <b>' + str(score) + '/100</b> ' + action + '\n'
                + ' Xu huong TT : ' + escape_html(wt) + '\n'
                + ('\n <i>' + escape_html(vol_sig) + '</i>\n' if vol_sig else '')
                + '\n<i>Dung /analyze ' + symbol + ' de xem day du</i>'
            )
            send(msg, chat_id)

        except Exception as e:
            logger.error(f'_volscan_single {symbol}: {e}')
            send('&#x274C; Loi: ' + str(e)[:80], chat_id)

    threading.Thread(target=run, daemon=True).start()


def _format_shark_alert(symbol, shark_score, shark_details, score_a, price):
    """Format shark alert message để gửi subscribers."""
    NL    = chr(10)
    emoji = shark_details.get('emoji', '&#x1F988;')
    label = shark_details.get('label', '')
    comp  = shark_details.get('components', {})

    # Phân loại kết hợp Score A + Shark
    if score_a >= 65 and shark_score >= 60:
        combo = '&#x1F525; <b>DOUBLE CONFIRM</b> — Score A + Shark cung xac nhan!'
    elif 60 <= score_a < 65 and shark_score >= 60:
        combo = '&#x1F440; <b>SAP MUA</b> — Score A sap dat nguong, Shark dang gom'
    else:
        combo = '&#x1F988; Smart money gom truoc ky thuat'

    bar = '&#x2588;' * round(shark_score/10) + '&#x2591;' * (10 - round(shark_score/10))

    msg  = f'{emoji} <b>Shark Alert — {symbol}</b>' + NL
    msg += '=' * 28 + NL
    msg += f'{bar} Shark: <b>{shark_score}/100</b> — {label}' + NL
    msg += f'{combo}' + NL + NL
    msg += f'Gia: <b>{price:,.0f}d</b> | Score A: <b>{score_a}/100</b>' + NL

    # Chi tiết TẤT CẢ thành phần — hiển thị cả điểm 0 để minh bạch
    raw_shown  = shark_details.get('raw', 0)
    max_shown  = shark_details.get('max_raw', 110)
    
    for key, ic, name, max_pts in [
        ('vsa',     '&#x1F4CA;', 'Wyckoff VSA',       30),
        ('spring',  '&#x1F30A;', 'Spring/Shakeout',   20),
        ('ad_line', '&#x1F4C8;', 'Chaikin A/D',       25),
        ('supply',  '&#x23F3;',  'Supply Exhaustion', 15),
        ('foreign', '&#x1F30F;', 'Foreign Flow',      20),
        ('sector',  '&#x1F3ED;', 'Sector Strength',   20),
    ]:
        d   = shark_details.get(key, {})
        s   = d.get('score', 0)
        lbl = d.get('label', '')
        # Ẩn Foreign nếu không có data
        if key == 'foreign' and not d.get('available', False):
            msg += f'{ic} {name}: khong co data' + NL
            continue
        # Ẩn Sector nếu không nhận diện được
        if key == 'sector' and s == 0 and 'Chua co' in lbl:
            msg += f'{ic} {name}: {lbl}' + NL
            continue
        # Hiển thị tất cả component kể cả score=0
        sc_str = f'<b>{s}/{max_pts}d</b>' if s > 0 else f'0/{max_pts}d'
        msg += f'{ic} {name} {sc_str}: {lbl}' + NL

    # Dòng minh bạch: raw / max → normalize
    msg += NL + f'<i>Raw: {raw_shown}/{max_shown} → normalize = {shark_score}/100</i>'
    msg += NL + '<i>Day chi la canh bao theo doi, khong phai lenh MUA</i>'
    return msg


def shark_watchlist_scanner():
    """
    Background scanner: Tính Shark Score cho 13 mã watchlist mỗi 60 phút.
    Gửi alert cho subscribers khi Shark Score >= SHARK_ALERT_THRESHOLD.
    Chạy song song với auto_alert_scanner (không conflict).
    """
    import time as _time
    logger.info('Shark watchlist scanner started')
    _last_shark_slot = -1

    while True:
        try:
            now      = datetime.now(VN_TZ)
            h, m     = now.hour, now.minute
            total_min= h * 60 + m
            weekday  = now.weekday()

            # Chỉ chạy trong giờ giao dịch + 1 giờ sau phiên (để bắt tích lũy cuối ngày)
            in_window = (weekday < 5 and
                        ((9 <= h < 15) or (h == 15 and m <= 30) or (h == 16)))

            shark_slot = total_min // SHARK_SCAN_INTERVAL

            if in_window and shark_slot != _last_shark_slot:
                _last_shark_slot = shark_slot
                logger.info(f'Shark scan: {now.strftime("%H:%M")} — queting {len(SIGNALS_WATCHLIST)} ma')

                for sym in SIGNALS_WATCHLIST:
                    try:
                        import sys, os, importlib, pandas as pd, numpy as np
                        bot_dir = os.path.dirname(os.path.abspath(__file__))
                        if bot_dir not in sys.path:
                            sys.path.insert(0, bot_dir)
                        from shark_detector import calc_shark_score, load_foreign_flow
                        import backtest as bt
                        importlib.reload(bt)

                        # Load data
                        df, _ = bt.load_data(sym, days=200)
                        if df is None:
                            continue

                        def to_arr(col_names):
                            for c in df.columns:
                                if c.lower() in col_names:
                                    arr = np.array(pd.to_numeric(df[c], errors='coerce').fillna(0)).copy()
                                    if arr.max() < 1000 and arr.max() > 0:
                                        arr *= 1000
                                    return arr
                            return np.zeros(len(df))

                        closes  = to_arr({'close','closeprice','close_price'})
                        highs   = to_arr({'high','highprice','high_price'})
                        lows    = to_arr({'low','lowprice','low_price'})
                        volumes = to_arr({'volume','volume_match','klgd','vol'})
                        price   = float(closes[-1]) if len(closes) > 0 else 0

                        # Foreign flow
                        foreign_net = None
                        try:
                            df_fn = load_foreign_flow(sym, days=60)
                            if df_fn is not None and 'net_vol' in df_fn.columns:
                                foreign_net = df_fn['net_vol'].values[-20:].tolist()
                        except Exception:
                            pass

                        # Tính Shark Score
                        shark_score, shark_details = calc_shark_score(
                            closes.tolist(), highs.tolist(), lows.tolist(),
                            volumes.tolist(), foreign_net=foreign_net, symbol=sym,
                        )

                        if shark_score < SHARK_ALERT_THRESHOLD:
                            continue

                        # Cooldown check
                        last_shark = _last_shark_alerts.get(sym)
                        if last_shark:
                            last_score, last_time = last_shark
                            if (_time.time() - last_time) < SHARK_COOLDOWN_SEC:
                                continue   # Còn trong cooldown 4 giờ

                        # Lấy Score A từ cache API
                        score_a = 50
                        try:
                            api_data = call_api('/api/analyze/' + sym)
                            if api_data and 'score' in api_data:
                                score_a = api_data.get('score', 50)
                        except Exception:
                            pass

                        # Gửi alert
                        _last_shark_alerts[sym] = (shark_score, _time.time())
                        alert_msg = _format_shark_alert(sym, shark_score, shark_details,
                                                        score_a, price)
                        broadcast(alert_msg)
                        logger.info(f'Shark alert sent: {sym} score={shark_score}')
                        _time.sleep(1)

                    except Exception as ex:
                        logger.warning(f'Shark scan {sym}: {ex}')
                        continue

            _time.sleep(30)

        except Exception as e:
            logger.error(f'shark_watchlist_scanner: {e}')
            _time.sleep(60)


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
            theo_doi.append('&#x1F7E1; ' + line + ' (gan nguong)')

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
    msg += '<i>Dung /signals hoac /analyze MA de xem chi tiet</i>'
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
        msg += '&#x1F7E1; Khong co tin hieu MUA/BAN dat nguong hom nay' + NL

    # Top RS (mã đang dẫn dắt thị trường)
    rs_list = [(item.get('rs_20d') or 0, item.get('symbol','')) 
               for item in (data or []) if item.get('rs_20d')]
    rs_list = sorted(rs_list, reverse=True)[:3]
    if rs_list:
        rs_str = ', '.join(f'{sym} {rs:+.1f}%' for rs, sym in rs_list if rs > 0)
        if rs_str:
            msg += '&#x1F680; <i>Dan dau TT: ' + rs_str + '</i>' + NL

    msg += '&#x2500;' * 22 + NL
    msg += '<i>Dung /signals de xem day du hoac /scan de tim ma moi</i>'
    return msg


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

            # Vol spike 1H vẫn scan trong ATC — giờ ATC có vol spike quan trọng nhất
            in_ato_only = in_ato  # Chỉ bỏ ATO cho vol spike, không bỏ ATC
            scan_slot = total_min // SCAN_INTERVAL_MIN

            if weekday < 5 and in_session and scan_slot != _last_scan_slot and not in_ato and not in_atc:
                _last_scan_slot = scan_slot
                logger.info('Scanner tick: ' + now.strftime('%H:%M %a'))

                data = call_api('/api/signals')
                if not data:
                    time.sleep(30)
                    continue

                for item in data:
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

            # ── Quét Volume Spike 1H mỗi 15 phút ────────────────────────────
            # Độc lập với scan 1D — chạy cho toàn bộ WATCHLIST_META
            # Bỏ qua ATO và ATC vì volume 1H chưa ổn định
            vol_spike_slot = total_min // VOL_SPIKE_SCAN_MIN
            if not hasattr(auto_alert_scanner, '_last_vol_slot'):
                auto_alert_scanner._last_vol_slot = -1

            if (weekday < 5 and in_session
                    and vol_spike_slot != auto_alert_scanner._last_vol_slot
                    and not in_ato_only):   # Fix: giữ ATC, chỉ bỏ ATO
                auto_alert_scanner._last_vol_slot = vol_spike_slot
                logger.info('Vol spike 1H scan: ' + now.strftime('%H:%M'))

                for sym in list(WATCHLIST_META.keys()):
                    try:
                        w_list = call_api('/api/warnings_1h/' + sym)
                        if not w_list:
                            continue
                        warnings = w_list.get('warnings_1h', [])
                        if not warnings:
                            logger.info(f'Vol spike {sym}: no spike detected')
                            continue

                        w = warnings[0]
                        level = w.get('level', 'MEDIUM')

                        # Alert: HIGH (≥2.5x) luôn gửi, MEDIUM (1.8-2.5x) chỉ gửi cho tier 1
                        if level == 'MEDIUM':
                            continue  # Bỏ MEDIUM để tránh spam — chỉ HIGH

                        # Xác định hướng từ message
                        direction = 'BAN' if 'BAN LON' in w.get('message', '') else 'MUA'

                        # Cooldown: không alert cùng mã + cùng hướng trong 60 phút
                        last_spike = _last_vol_spike.get(sym)
                        if last_spike:
                            last_dir, last_ts = last_spike
                            if last_dir == direction and (time.time() - last_ts) < VOL_SPIKE_COOLDOWN_SEC:
                                continue

                        # Lấy giá và score hiện tại từ cache để đính kèm vào alert
                        price_info = call_api('/api/price/' + sym)
                        price = price_info.get('price', 0) if price_info else 0
                        score_info = call_api('/api/analyze/' + sym)
                        score = score_info.get('score', 0) if score_info else 0

                        _last_vol_spike[sym] = (direction, time.time())
                        msg = format_vol_spike_alert(sym, w, score, price)
                        broadcast(msg)
                        logger.info(f'Vol spike alert sent: {sym} {direction}')
                        time.sleep(3)  # Tránh rate limit giữa các mã

                    except Exception as e:
                        logger.warning(f'vol_spike scan {sym}: {e}')

            # ── Quét MA10 / MA50 cross mỗi 10 phút ──────────────────────────
            # Cùng tần suất scan 1D — lấy dữ liệu từ /api/analyze (đã cache)
            # Bỏ qua ATO/ATC vì giá chưa ổn định
            ma_scan_slot = total_min // MA_SCAN_INTERVAL_MIN
            if not hasattr(auto_alert_scanner, '_last_ma_slot'):
                auto_alert_scanner._last_ma_slot = -1

            if (weekday < 5 and in_session
                    and ma_scan_slot != auto_alert_scanner._last_ma_slot
                    and not in_ato and not in_atc):
                auto_alert_scanner._last_ma_slot = ma_scan_slot
                logger.info('MA10/MA50 scan: ' + now.strftime('%H:%M'))

                for sym in list(WATCHLIST_META.keys()):
                    try:
                        data = call_api('/api/analyze/' + sym)
                        if not data or data.get('error'):
                            continue

                        price        = data.get('price', 0)
                        score        = data.get('score', 50)
                        ma10_val     = data.get('ma10', 0)
                        ma50_val     = data.get('ma50', 0)
                        cross_up     = data.get('ma10_cross_up', False)
                        cross_down   = data.get('ma10_cross_down', False)
                        above_ma50   = data.get('above_ma50', False)
                        ma50_slope   = data.get('ma50_slope_up', False)

                        sym_last = _last_ma_alerts.setdefault(sym, {})
                        now_ts   = time.time()

                        # ── MA10 Cross UP ─────────────────────────────────
                        # Filter score >= 50: tránh gửi khi mã đang downtrend mạnh
                        if cross_up and score >= 50:
                            last_ma10 = sym_last.get('ma10', ('', 0))
                            if (last_ma10[0] != 'MA10_CROSS_UP'
                                    or now_ts - last_ma10[1] > MA_CROSS_COOLDOWN_SEC):
                                sym_last['ma10'] = ('MA10_CROSS_UP', now_ts)
                                msg = format_ma_alert(sym, 'MA10_CROSS_UP',
                                                      price, score, ma10_val, ma50_val, ma50_slope)
                                if msg:
                                    broadcast(msg)
                                    logger.info(f'MA10 cross up alert: {sym}')
                                    time.sleep(2)

                        # ── MA10 Cross DOWN ───────────────────────────────
                        elif cross_down:
                            last_ma10 = sym_last.get('ma10', ('', 0))
                            if (last_ma10[0] != 'MA10_CROSS_DOWN'
                                    or now_ts - last_ma10[1] > MA_CROSS_COOLDOWN_SEC):
                                sym_last['ma10'] = ('MA10_CROSS_DOWN', now_ts)
                                msg = format_ma_alert(sym, 'MA10_CROSS_DOWN',
                                                      price, score, ma10_val, ma50_val, ma50_slope)
                                if msg:
                                    broadcast(msg)
                                    logger.info(f'MA10 cross down alert: {sym}')
                                    time.sleep(2)

                        # ── MA50 Uptrend mới xác nhận ─────────────────────
                        # Chỉ alert khi vừa chuyển từ dưới lên trên MA50
                        last_ma50 = sym_last.get('ma50', ('', 0))
                        if above_ma50 and ma50_slope:
                            if (last_ma50[0] != 'MA50_UPTREND'
                                    or now_ts - last_ma50[1] > MA_CROSS_COOLDOWN_SEC):
                                sym_last['ma50'] = ('MA50_UPTREND', now_ts)
                                # Chỉ alert MA50 khi vừa bắt đầu (lần đầu trong cooldown)
                                if last_ma50[0] != 'MA50_UPTREND':
                                    msg = format_ma_alert(sym, 'MA50_UPTREND',
                                                          price, score, ma10_val, ma50_val, ma50_slope)
                                    if msg:
                                        broadcast(msg)
                                        logger.info(f'MA50 uptrend alert: {sym}')
                                        time.sleep(2)
                        elif not above_ma50:
                            # Mất MA50 — alert nếu trước đó đang uptrend
                            if last_ma50[0] == 'MA50_UPTREND':
                                sym_last['ma50'] = ('MA50_LOST', now_ts)
                                msg = format_ma_alert(sym, 'MA50_LOST',
                                                      price, score, ma10_val, ma50_val, ma50_slope)
                                if msg:
                                    broadcast(msg)
                                    logger.info(f'MA50 lost alert: {sym}')
                                    time.sleep(2)

                    except Exception as e:
                        logger.warning(f'MA scan {sym}: {e}')
                # Save state sau mỗi lần scan để persist qua restart
                _save_ma_alerts(_last_ma_alerts)
            # Cùng slot với MA tầng 1 (mỗi 10 phút), nhưng cooldown 8 tiếng
            # Chỉ alert MA10 cross UP/DOWN và Vol spike — không MA50, không Signal 1D
            # Dùng cùng ma_scan_slot đã tính ở trên
            if (weekday < 5 and in_session
                    and ma_scan_slot != getattr(auto_alert_scanner, '_last_ext_slot', -1)
                    and not in_ato and not in_atc):
                auto_alert_scanner._last_ext_slot = ma_scan_slot
                logger.info('Extended MA10/Vol scan (tier2): ' + now.strftime('%H:%M'))

                for sym in WATCHLIST_EXTENDED:
                    # Bỏ qua nếu đã có trong WATCHLIST_META (tránh alert trùng)
                    if sym in WATCHLIST_META:
                        continue
                    try:
                        data = call_api('/api/analyze/' + sym)
                        if not data or data.get('error'):
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

                        # MA10 Cross UP — tầng 2: chỉ gửi khi score >= 55
                        if cross_up and score >= 55:
                            last = sym_last.get('ma10_ext', ('', 0))
                            if (last[0] != 'MA10_CROSS_UP'
                                    or now_ts - last[1] > MA_EXT_COOLDOWN_SEC):
                                sym_last['ma10_ext'] = ('MA10_CROSS_UP', now_ts)
                                msg = format_ma_alert(sym, 'MA10_CROSS_UP',
                                                      price, score, ma10_val, ma50_val,
                                                      ma50_slope, tier=2)
                                if msg:
                                    broadcast(msg)
                                    logger.info(f'[Tier2] MA10 cross up: {sym}')
                                    time.sleep(2)

                        # MA10 Cross DOWN — tầng 2: BỎ
                        # Mã tier 2 chưa backtest → CROSS DOWN chỉ là noise
                        # elif cross_down: pass

                        # Vol Spike 1H — tầng 2 (cùng logic tầng 1, cooldown 8h)
                        try:
                            w_list = call_api('/api/warnings_1h/' + sym)
                            if w_list:
                                warnings = w_list.get('warnings_1h', [])
                                if warnings and warnings[0].get('level') == 'HIGH':
                                    w = warnings[0]
                                    direction = 'BAN' if 'BAN LON' in w.get('message', '') else 'MUA'
                                    last_vs = _last_vol_spike.get(sym + '_ext')
                                    if not last_vs or (
                                            last_vs[0] != direction
                                            or now_ts - last_vs[1] > MA_EXT_COOLDOWN_SEC):
                                        _last_vol_spike[sym + '_ext'] = (direction, now_ts)
                                        # Thêm note tier 2 vào vol spike alert
                                        vol_msg = format_vol_spike_alert(sym, w, score, price)
                                        if vol_msg:
                                            vol_msg += '\n<i>⚠ Tier 2 — chua backtest, chi tham khao</i>'
                                            broadcast(vol_msg)
                                            logger.info(f'[Tier2] Vol spike: {sym} {direction}')
                                            time.sleep(2)
                        except Exception:
                            pass

                    except Exception as e:
                        logger.warning(f'Extended scan {sym}: {e}')
                    time.sleep(1)  # Throttle nhẹ giữa các mã tầng 2

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
        # Shark watchlist scanner — chạy song song, quét mỗi 60 phút
        t_shark = threading.Thread(target=shark_watchlist_scanner, daemon=True)
        t_shark.start()
        logger.info('Shark watchlist scanner started (interval=60min)')
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
