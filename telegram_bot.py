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
    SETTLEMENT_DAYS, SYMBOL_CONFIG, SIGNALS_WATCHLIST, SIGNALS_MANUAL,
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
                    t = 55   # FIX: tăng từ 25s → 55s (FA compute + rate limit wait)
                elif '/signals' in endpoint:
                    t = 90   # cold start warmup cần ~73s
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


def _fmt_shark_verdict(symbol: str, shark_score: int) -> str:
    """Trả về dòng kết luận backtest để append vào /shark message."""
    NL = chr(10)
    try:
        from config import get_shark_config
        cfg     = get_shark_config(symbol)
        verdict = cfg.get('verdict', '')
        note    = cfg.get('note', '')
        pnl_ok  = cfg.get('pnl_ok', True)
        mn      = cfg.get('min_score', 0)
        warn    = cfg.get('warn_score', 0)
        hard    = cfg.get('shark_min_hard', 0)
    except Exception:
        return ''

    if not verdict:
        return ''

    out  = NL + '─' * 28 + NL
    out += '<b>Ket luan backtest:</b>' + NL
    out += verdict + NL
    if note:
        out += f'<i>{note}</i>' + NL

    # Cảnh báo trực tiếp theo score hiện tại
    if mn and shark_score < mn:
        out += NL + f'⚠ Shark hien tai ({shark_score}) chua dat nguong ({mn}) — chua co tin hieu' + NL
    elif warn and shark_score >= warn:
        out += NL + f'🚫 Shark hien tai ({shark_score}) >= {warn} — TRANH vao lenh' + NL
    elif hard and shark_score < hard:
        out += NL + f'🚫 Shark hien tai ({shark_score}) < {hard} — KHONG vao du Score A du' + NL
    elif mn and shark_score >= mn:
        out += NL + f'✅ Shark hien tai ({shark_score}) dat nguong ({mn}) — du dieu kien' + NL

    return out


def _fmt_regime_compare(result_no, result_yes):
    """So sanh backtest co/khong co regime filter cho /bt output."""
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
        emoji, verdict = '&#x26A0;', 'Regime filter CO HAI — block lenh tot (ma counter-cyclical?)'
    else:
        emoji, verdict = '&#x1F7E1;', 'Regime filter it tac dong'

    pf_no_s  = f'{pf_no:.2f}'  if pf_no  != float('inf') else 'inf'
    pf_yes_s = f'{pf_yes:.2f}' if pf_yes != float('inf') else 'inf'

    out  = '&#x1F4CA; <b>Market Regime Filter:</b>' + NL
    out += chr(9472) * 24 + NL
    out += f'  Khong regime: {n_no}L | WR={wr_no}% | PnL={pnl_no:+.2f}% | PF={pf_no_s}' + NL
    out += f'  Co regime:    {n_yes}L | WR={wr_yes}% | PnL={pnl_yes:+.2f}% | PF={pf_yes_s}' + NL
    sign_wr = '+' if dwr >= 0 else ''
    sign_pnl= '+' if dpnl>= 0 else ''
    sign_pf = '+' if dpf >= 0 else ''
    out += f'  Delta: WR={sign_wr}{dwr}% | PnL={sign_pnl}{dpnl}% | PF={sign_pf}{dpf} | Block={block}L' + NL
    out += f'{emoji} {verdict}' + NL
    return out


def _fmt_sector_rs(data):
    """Hien thi Intra-Sector RS trong /score."""
    sr = data.get('sector_rs', {})
    if not sr or not sr.get('available'):
        return ''
    NL = chr(10)
    grp   = sr.get('group', '')
    rank  = sr.get('rank', 0)
    total = sr.get('total', 0)
    pct   = sr.get('percentile', 0)
    ret   = sr.get('symbol_ret', 0)
    label = sr.get('label', '')
    peer_rets = sr.get('peer_rets', {})

    if pct >= 80:   icon = '&#x1F525;'  # 🔥
    elif pct >= 60: icon = '&#x1F4C8;'  # 📈
    elif pct >= 40: icon = '&#x27A1;'   # ➡
    elif pct >= 20: icon = '&#x1F4C9;'  # 📉
    else:           icon = '&#x2B07;'   # ⬇

    line = (NL + icon + ' <b>Sector RS</b> [' + grp + '] '
            + f'Hang {rank}/{total} ({pct:.0f}%ile)'
            + f' | {ret:+.1f}% vs peers')

    # Hiển thị peer comparison ngắn gọn
    if peer_rets:
        peer_str = ' | '.join(
            f'{p}:{r:+.1f}%' for p, r in
            sorted(peer_rets.items(), key=lambda x: -x[1])[:3]
        )
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
            return NL + '&#x26AA; Regime BEAR nhung ma nay MIEN TRU (counter-cyclical)'
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
                   + format_shark_msg(score, details, symbol)
                   + _fmt_shark_verdict(symbol, score))
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

            # Progress update mỗi 15s để user biết đang chạy
            _started = _t.time()
            _stop_progress = [False]
            def _progress():
                for wait_s in [15, 40, 80]:
                    _t.sleep(15)
                    if _stop_progress[0]: return
                    elapsed = _t.time() - _started
                    send(f'&#x23F3; Dang tinh backtest {symbol}... ({elapsed:.0f}s)' + chr(10)
                         + 'He thong dang tai 7 nam OHLCV — vui long cho them.', chat_id)
            threading.Thread(target=_progress, daemon=True).start()

            # Đọc use_regime từ SYMBOL_CONFIG per-symbol
            try:
                from config import SYMBOL_CONFIG as _sc_shark
                _use_reg_shark = _sc_shark.get(symbol.upper(), {}).get('use_regime', True)
            except Exception:
                _use_reg_shark = True

            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                res = bs.run_shark_report(symbol, use_regime=_use_reg_shark)
            if not res:
                send('&#x274C; Khong du du lieu cho ' + symbol + chr(10)
                     + '(Co the do rate limit vnstock — thu lai sau 1-2 phut)', chat_id)
                return

            # results = {'symbol', 'score_a', 'best', 'all_rows', 'verdict_*'}
            sa   = res.get('score_a', {})
            best = res.get('best', {})   # {mode: (thr, res_list, stats_dict)}
            NL   = chr(10)
            
            # Build bảng đầy đủ TẤT CẢ ngưỡng đã test (all_rows)
            rows = ''
            best_mode = None
            best_dwr  = -999
            wr_a      = sa.get('wr', 0)

            # Nhóm all_rows theo mode để hiển thị bảng đầy đủ
            from collections import defaultdict
            rows_by_mode = defaultdict(list)
            for (mode, thr, n, wr, pnl, pf, dwr, dpnl) in sorted(
                    res.get('all_rows', []), key=lambda x: (x[0], x[1])):
                rows_by_mode[mode].append((thr, n, wr, pnl, dwr))

            if rows_by_mode:
                # Hiển thị bảng đầy đủ mọi ngưỡng
                for mode in sorted(rows_by_mode):
                    for (thr, n, wr, pnl, dwr) in rows_by_mode[mode]:
                        warn = ' &#x26A0;' if n < 15 else ''   # cảnh báo ít lệnh
                        rows += f'  {mode:>4}>={thr}: {n:>3}L | WR={wr:.0f}% | PnL={pnl:+.2f}% | DWR={dwr:+.1f}%{warn}' + NL
                        if dwr > best_dwr and n >= 10:
                            best_dwr, best_mode = dwr, mode
            else:
                # Fallback: dùng best dict nếu all_rows trống
                for mode, (thr, _, st) in sorted(best.items()):
                    wr  = st.get('win_rate', 0)
                    pnl = st.get('avg_pnl', 0)
                    n   = st.get('total', 0)
                    dwr = wr - wr_a
                    warn = ' &#x26A0;' if n < 15 else ''
                    rows += f'  {mode:>4}>={thr}: {n:>3}L | WR={wr:.0f}% | PnL={pnl:+.2f}% | DWR={dwr:+.1f}%{warn}' + NL
                    if dwr > best_dwr and n >= 10:
                        best_dwr, best_mode = dwr, mode
            
            # Verdicts
            vds = [v for k, v in res.items() if k.startswith('verdict_')]
            vd_str = NL.join(vds) if vds else ''
            
            # Tổng kết: Shark có ích hay không?
            # Dùng all_rows để check toàn bộ ngưỡng, không chỉ best per mode
            _wr_a = sa.get('wr', 0)
            any_better = any(
                wr > _wr_a + 2 and n >= 10
                for (_, _, n, wr, _, _, _, _) in res.get('all_rows', [])
            ) or any(
                st.get('win_rate',0) > _wr_a + 2
                for _, (_, _, st) in best.items()
                if st.get('total', 0) >= 10
            )
            if any_better:
                summary = '&#x2705; <b>Shark co ich voi ' + symbol + '</b> — co mode tot hon baseline'
            else:
                summary = '&#x274C; <b>Shark chua tot hon Score A don</b> — nen dung Score A la chinh'

            msg = ('&#x1F988; <b>Shark Backtest — ' + symbol + '</b>' + NL
                   + '=' * 30 + NL + NL
                   + 'Score A baseline: '
                   + str(sa.get("n",0)) + 'L | WR=' + f'{sa.get("wr",0):.1f}%'
                   + ' | PnL=' + f'{sa.get("pnl",0):+.2f}%' + NL + NL
                   + '<b>Ket qua tung mode (so voi baseline):</b>' + NL
                   + rows + NL
                   + summary + NL
                   + ((NL + '<b>Mode co DWR tot nhat:</b> ' + best_mode
                       + f' (DWR={best_dwr:+.1f}%)' + NL) if best_mode and best_dwr > 0 else '')
                   + NL + '<i>Backtest 7 nam OHLCV daily</i>')
            _stop_progress[0] = True
            send(msg, chat_id)
        except Exception as e:
            _stop_progress[0] = True
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
            + _fmt_regime_inline(data) + _fmt_vwap(data) + _fmt_sector_rs(data)
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
            + _fmt_wf_summary(data) + '\n'
            + '<b>KET LUAN</b>\n'
            + _build_conclusion(score, score_adj, b_delta, b_details,
                                b_overall, action, ae, data)
    )
    # Thêm conviction block vào cuối
    _conv_block, _, _ = _build_conviction_block(data, score_adj=score_adj)
    msg += _conv_block
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
            ' /bt all        — Toan watchlist (~15 phut)\n\n'
            '<b>Cu phap tuy chinh:</b>\n'
            ' /bt DGC s=60           — Score threshold = 60\n'
            ' /bt DGC sl=5 tp=20     — SL=5% TP=20%\n'
            ' /bt DGC s=55 sl=7 tp=20 hold=7 — Full custom\n\n'
            '<b>Giai thich:</b>\n'
            ' s=   Score threshold (mac dinh 65)\n'
            ' sl=  Stop Loss % (mac dinh 7)\n'
            ' tp=  Take Profit % (mac dinh 14)\n'
            ' hold= So phien giu lenh (mac dinh 10)',
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
        return '&#x1F7E1;', 'TRUNG BINH — Chi tham khao'
    else:
        return '&#x274C;', 'KEM HIEU QUA — Nen xem lai'


def _fmt_decay(decay):
    if decay <= 5:   return '&#x2705; Rat on dinh'
    if decay <= 10:  return '&#x1F7E2; On dinh'
    if decay <= 20:  return '&#x1F7E1; Chap nhan'
    if decay <= 30:  return '&#x26A0; Canh bao overfit'
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
        '&#x1F504; Dang chay <b>Backtest + Walk-Forward ' + symbol + '</b>'
        + custom_label + mode_txt + '\n'
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

            # ── BACKTEST ─────────────────────────────────────────────────────
            res       = bt.run_backtest_symbol(symbol, verbose=False, use_regime=False,
                            sl=custom_sl, tp=custom_tp,
                            hold_days=custom_hold, min_score=custom_score)  # baseline
            res_regime= bt.run_backtest_symbol(symbol, verbose=False, use_regime=None,
                            sl=custom_sl, tp=custom_tp,
                            hold_days=custom_hold, min_score=custom_score)   # per-symbol config
            if not res or not res.get('buy'):
                send('&#x274C; ' + symbol + ': Khong du du lieu hoac khong co lenh MUA.', chat_id)
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
            # Thêm regime compare block vào cuối msg_bt
            msg_bt += chr(10) + _fmt_regime_compare(res, res_regime)
            send(msg_bt, chat_id)

            # ── WALK-FORWARD ─────────────────────────────────────────────────
            send('&#x1F504; Dang chay <b>Walk-Forward</b> ' + symbol + '...', chat_id)
            wf = bt.run_walk_forward(symbol, verbose=False)

            if not wf:
                send('&#x26A0; ' + symbol + ': Khong du du lieu Walk-Forward.\n'
                     '<i>API vnstock co the gioi han so rows tra ve.\n'
                     'Bot da chay Backtest don (khong co WF).</i>', chat_id)
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


def _handle_trigbt(symbol_or_all, chat_id):
    """So sanh 3 trigger mode: score_primary / filter_confirm / filter_led."""
    from config import SYMBOL_CONFIG, BACKTEST_WATCHLIST
    NL = chr(10)
    ALL_SYMS = [
        'DGC','DCM','SSI','NKG','MBB','HSG','FRT','VND','HCM','PDR',
        'NVL','VIC','BID','KBC','FPT','SZC','KDH','GAS','PVS','POW',
        'HPG','TCB','VPB','VCB','MWG','CMG','PVD','REE',
    ]
    MODES = [
        ('score_primary',  65, 0,   'Score>=65'),
        ('filter_confirm', 55, 2.0, 'Score>=55+conv>=2'),
        ('filter_confirm', 55, 1.5, 'Score>=55+conv>=1.5'),
        ('filter_led',     45, 0,   'Score>=45+R+V'),
        ('filter_led',     50, 0,   'Score>=50+R+V'),
    ]

    if symbol_or_all == 'ALL':
        syms = ALL_SYMS
        send('&#x1F50D; Trigger Backtest <b>28 ma</b>...'
             + NL + 'Uoc tinh ~25 phut', chat_id)
    else:
        syms = [symbol_or_all]
        send('&#x1F50D; Trigger Backtest <b>' + symbol_or_all + '</b>...', chat_id)

    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from backtest import run_backtest_symbol, load_data, LOOKBACK_DAYS

        summary = []
        for sym in syms:
            cfg        = SYMBOL_CONFIG.get(sym.upper(), {})
            use_regime = cfg.get('use_regime', True)
            use_vwap   = cfg.get('use_vwap', True)
            wf         = cfg.get('wf_verdict', '?')

            _days = cfg.get('days', LOOKBACK_DAYS)
            df, _ = load_data(sym, days=_days)
            if df is None:
                continue

            mode_results = {}
            for tmode, tscore, tconv, label in MODES:
                r = run_backtest_symbol(
                    sym, verbose=False,
                    use_regime=use_regime, use_vwap=use_vwap,
                    _df_cache=df,
                    trigger_mode=tmode, trigger_score=tscore,
                    min_conviction=tconv,
                )
                st = r.get('buy', {}) if r else {}
                mode_results[(tmode, tscore, tconv)] = {
                    'n': st.get('total', 0), 'wr': st.get('win_rate', 0),
                    'pnl': st.get('avg_pnl', 0), 'label': label,
                }

            # Baseline
            base = mode_results.get(('score_primary', 65, 0), {})
            b_wr, b_pnl = base.get('wr', 0), base.get('pnl', 0)

            # Tìm best
            best_key, best_val = ('score_primary', 65, 0), -99
            for k, r in mode_results.items():
                if r['n'] >= 20:
                    val = (r['wr'] - b_wr)*0.5 + (r['pnl'] - b_pnl)*10*0.3
                    if val > best_val:
                        best_val, best_key = val, k

            best   = mode_results[best_key]
            dwr    = round(best['wr']  - b_wr,  1)
            dpnl   = round(best['pnl'] - b_pnl, 2)
            better = best_key != ('score_primary', 65, 0) and best_val > 0

            # Per-symbol message
            if len(syms) == 1:
                rows = [f'&#x1F50D; <b>Trigger Backtest — {sym}</b>',
                        f'WF={wf} | Regime={use_regime} | VWAP={use_vwap}', '']
                for k, r in mode_results.items():
                    stat = '&#x2705;' if r['n'] >= 30 else ('&#x1F7E1;' if r['n'] >= 20 else '&#x274C;')
                    best_m = ' &#x1F3AF;' if k == best_key and better else ''
                    base_m = ' (hien tai)' if k == ('score_primary', 65, 0) else ''
                    rows.append(f'{stat} {r["label"]}: {r["n"]}L WR={r["wr"]:.1f}% PnL={r["pnl"]:+.2f}%{base_m}{best_m}')
                rows.append('')
                if better:
                    rows.append(f'&#x1F3AF; <b>Nen dung: {best["label"]}</b>')
                    rows.append(f'   dWR={dwr:+.1f}% dPnL={dpnl:+.2f}% so voi baseline')
                    rows.append(f'   → Score A qua strict voi {sym}?')
                else:
                    rows.append(f'&#x2705; Score>=65 van tot nhat voi {sym}')
                send(NL.join(rows), chat_id)

            summary.append({
                'sym': sym, 'wf': wf,
                'better': better, 'best_label': best.get('label', '?'),
                'base_n': base.get('n', 0), 'base_wr': b_wr, 'base_pnl': b_pnl,
                'best_n': best.get('n', 0), 'best_wr': best.get('wr', 0),
                'best_pnl': best.get('pnl', 0), 'dwr': dwr, 'dpnl': dpnl,
            })

        # Tổng kết all
        if len(summary) > 1:
            improved  = [r for r in summary if r['better'] and r['dwr'] >= 1]
            same_best = [r for r in summary if not r['better']]
            mixed     = [r for r in summary if r['better'] and r['dwr'] < 1]

            lines = ['&#x1F50D; <b>Trigger Backtest — Tong ket</b>', '']
            lines.append(f'{"Ma":<6} {"Base":>14} {"Best":>14} {"dWR":>6} {"dPnL":>6}')
            lines.append('─' * 50)
            for r in summary:
                b_s = f'{r["base_n"]}L {r["base_wr"]:.0f}%'
                k_s = f'{r["best_n"]}L {r["best_wr"]:.0f}%'
                icon = '&#x2705;' if r['dwr'] >= 1 and r['dpnl'] >= 0 else (
                       '&#x1F7E1;' if r['dwr'] >= 0 else '&#x274C;')
                lines.append(f'{r["sym"]:<6} {b_s:>14} {k_s:>14} {r["dwr"]:>+5.1f}% {r["dpnl"]:>+5.2f}% {icon}')

            lines.append('')
            lines.append(f'&#x2705; Co loi khi ha nguong: {len(improved)} ma')
            if improved:
                lines.append('  ' + ', '.join(r["sym"] for r in improved))
            lines.append(f'&#x274C; Score>=65 van tot nhat: {len(same_best)} ma')
            if same_best:
                lines.append('  ' + ', '.join(r["sym"] for r in same_best))
            lines.append('')
            avg_dwr  = sum(r['dwr']  for r in summary) / len(summary)
            avg_dpnl = sum(r['dpnl'] for r in summary) / len(summary)
            lines.append(f'Avg dWR={avg_dwr:+.1f}% | Avg dPnL={avg_dpnl:+.2f}%')
            send(NL.join(lines), chat_id)

    except Exception as e:
        send('&#x274C; Loi Trigger Backtest: ' + str(e)[:200], chat_id)


def _handle_optimize(symbol, chat_id):
    """Tim SL/TP/Hold/Score toi uu voi Walk-Forward validation."""
    NL = chr(10)
    send('&#x1F50D; Dang chay <b>Optimize ' + symbol + '</b>...'
         + NL + '<i>Grid search 144 combos + WF validation, ~3-5 phut</i>',
         chat_id)
    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from backtest import run_optimize_symbol

        results = run_optimize_symbol(symbol, verbose=False)
        if not results:
            send('&#x274C; ' + symbol + ': Khong du du lieu hoac khong co combo hop le', chat_id)
            return

        per_score = results.get('per_score', [])
        ob        = results.get('overall', {})

        st_icon = {
            'ROBUST':  '&#x2705;',
            'THIN':    '&#x26A0; it lenh',
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

        for r in per_score:
            is_s  = f"{r['n_is']}L {r['wr_is']:.0f}% {r['pnl_is']:+.1f}%"
            oos_s = (f"{r['n_oos']}L {r['wr_oos']:.0f}% {r['pnl_oos']:+.1f}%"
                     if r['n_oos'] > 0 else 'N/A')
            st    = st_icon.get(r['oos_status'], '?')
            win   = ' &#x1F3AF;' if (ob and r['score'] == ob['score']
                                     and r['sl'] == ob['sl']) else ''
            lines.append(
                f"{r['score']:2d}  {r['sl']:2d}%  {r['tp']:2d}%  {r['hold']:2d}p"
                f" | {is_s:15} | {oos_s} {st}{win}"
            )

        lines.append('')

        if ob:
            if ob['oos_status'] == 'ROBUST':
                lines.append(
                    f'&#x1F4A1; <b>Khuyen nghi: S&gt;={ob["score"]}'
                    f' SL={ob["sl"]}% TP={ob["tp"]}% Hold={ob["hold"]}p</b>'
                )
                lines.append(f'   OOS: {ob["n_oos"]}L WR={ob["wr_oos"]}% PnL={ob["pnl_oos"]:+.2f}%')
                lines.append(
                    f'   Test: /bt {symbol}'
                    f' s={ob["score"]} sl={ob["sl"]} tp={ob["tp"]} hold={ob["hold"]}'
                )
            elif ob['oos_status'] in ('THIN', 'NO_WF'):
                lines.append(f'&#x26A0; OOS qua it lenh ({ob["n_oos"]}L) — dung IS lam tham khao')
                lines.append(f'   IS tot nhat: S&gt;={ob["score"]} SL={ob["sl"]}% TP={ob["tp"]}% Hold={ob["hold"]}p')
            else:
                lines.append('&#x274C; Tat ca combos overfit — giu config mac dinh')

        lines.append('')
        lines.append('<i>/bt SYM s=X sl=X tp=X hold=X de test thu</i>')
        send(NL.join(lines), chat_id)

    except Exception as e:
        import traceback
        send('&#x274C; Loi Optimize: ' + str(e)[:200], chat_id)


def _handle_workflow(chat_id):
    """Huong dan quy trinh su dung bot khoa hoc."""
    NL = chr(10)
    msg = (
        '<b>&#x1F4CB; QUY TRINH SU DUNG BOT</b>' + NL
        + '<i>Funnel: Thi truong → Ma → Lenh</i>' + NL
        + NL
        + '&#x2501;' * 22 + NL
        + '<b>[T1] SANG SOM 8:30 — ~15 phut</b>' + NL
        + NL
        + '<b>B1.</b> /macro' + NL
        + '  Ket qua VERY HIGH → dung lai, khong GD hom nay' + NL
        + NL
        + '<b>B2.</b> /market' + NL
        + '  Xac dinh Regime: BULL / NEUTRAL / BEAR' + NL
        + NL
        + '<b>B3.</b> /signals' + NL
        + '  Lay danh sach 3-8 ma ung vien hom nay' + NL
        + NL
        + '&#x2501;' * 22 + NL
        + '<b>[T2] PHAN TICH SAU 9:00-11:00</b>' + NL
        + '<i>Cho tung ma tu /signals, 5-10 phut/ma</i>' + NL
        + NL
        + '<b>B4.</b> /score SYM' + NL
        + '  Xem <b>Xac nhan da chieu</b> o cuoi:' + NL
        + '  — Duoi 2/4: BỎ QUA, xet ma khac' + NL
        + '  — Tu 3/4 tro len: TIEP TUC B5' + NL
        + NL
        + '<b>B5.</b> /shark SYM  <i>(neu ma co Shark score)</i>' + NL
        + '  Shark thap + Score khong noi bat → bo qua' + NL
        + '  Shark &gt;= nguong → xac nhan tang [4]' + NL
        + NL
        + '<b>B6.</b> /fv SYM  <i>(tuy chon)</i>' + NL
        + '  Dinh gia re/dat → quyet dinh size vi the' + NL
        + NL
        + '&#x2501;' * 22 + NL
        + '<b>[T3] VAO LENH 11:00-14:00</b>' + NL
        + '<i>Cho ma da qua T2, ~10 phut</i>' + NL
        + NL
        + '<b>B7.</b> /volscan SYM' + NL
        + '  Vol &lt; 1.0x TB: CHO, chua vao' + NL
        + '  Vol &gt;= 1.5x TB: tin hieu manh &#x2705;' + NL
        + NL
        + '<b>B8.</b> /whatif SYM GIA' + NL
        + '  R:R &lt; 1:2 → bo qua, gia chua hop ly' + NL
        + '  R:R &gt;= 1:2 → VAO LENH' + NL
        + NL
        + '<b>B9.</b> /paper SYM  <i>(theo doi sau khi vao)</i>' + NL
        + NL
        + '&#x2501;' * 22 + NL
        + '<b>[T4] THEO DOI — Tu dong</b>' + NL
        + NL
        + 'Bot tu gui alert khi:' + NL
        + '  • Vol dot bien → xem /score ngay' + NL
        + '  • MA10 cat MA50 → xem xet add/thoat' + NL
        + NL
        + 'Khi dang hold: /check SYM GIA_MUA' + NL
        + NL
        + '&#x2501;' * 22 + NL
        + '<b>[T5] RESEARCH — Cuoi tuan</b>' + NL
        + '<i>KHONG dung trong gio giao dich</i>' + NL
        + NL
        + '/bt /vwapbt /convbt /trigbt /scan' + NL
        + NL
        + '&#x2501;' * 22 + NL
        + '<b>&#x26A1; NGUYEN TAC CUNG:</b>' + NL
        + '(1) /macro truoc — VERY HIGH = dung han' + NL
        + '(2) /signals cho list — khong chon ma cam tinh' + NL
        + '(3) /score truoc /shark — score la gate' + NL
        + '(4) Vol confirm truoc khi vao lenh' + NL
        + '(5) R:R &gt;= 1:2 hoac khong vao' + NL
        + NL
        + '<i>Thoi gian chu dong: ~1-1.5h/ngay</i>'
    )
    send(msg, chat_id)


def _handle_convbt(symbol_or_all, chat_id):
    """Backtest Conviction Filter: tim nguong K toi uu per-symbol."""
    from config import SYMBOL_CONFIG, BACKTEST_WATCHLIST
    NL = chr(10)
    ALL_SYMS = [
        'DGC','DCM','SSI','NKG','MBB','HSG','FRT','VND','HCM','PDR',
        'NVL','VIC','BID','KBC','FPT','SZC','KDH','GAS','PVS','POW',
        'HPG','TCB','VPB','VCB','MWG','CMG','PVD','REE',
    ]
    K_LEVELS = [1.0, 1.5, 2.0, 2.5, 3.0]

    if symbol_or_all == 'ALL':
        syms = ALL_SYMS
        send('&#x1F50D; Dang chay Conviction Backtest <b>toan watchlist</b>...'
             + NL + 'Uoc tinh ~20 phut', chat_id)
    else:
        syms = [symbol_or_all]
        send('&#x1F50D; Dang chay Conviction Backtest <b>' + symbol_or_all + '</b>...', chat_id)

    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from backtest import run_backtest_symbol, load_data, LOOKBACK_DAYS

        summary = []
        for sym in syms:
            cfg = SYMBOL_CONFIG.get(sym.upper(), {})
            use_regime = cfg.get('use_regime', True)
            use_vwap   = cfg.get('use_vwap', True)
            wf         = cfg.get('wf_verdict', '?')

            _days = cfg.get('days', LOOKBACK_DAYS)
            df, _ = load_data(sym, days=_days)
            if df is None:
                continue

            results_k = {}
            for k in K_LEVELS:
                r = run_backtest_symbol(
                    sym, verbose=False,
                    use_regime=use_regime, use_vwap=use_vwap,
                    _df_cache=df, min_conviction=k
                )
                st = r.get('buy', {}) if r else {}
                results_k[k] = {
                    'n': st.get('total', 0), 'wr': st.get('win_rate', 0),
                    'pnl': st.get('avg_pnl', 0), 'pf': st.get('profit_factor', 0),
                }

            base = results_k.get(1.0, {})
            base_wr, base_pnl = base.get('wr', 0), base.get('pnl', 0)

            # Tìm best K (n>=20, maximize dWR*0.6 + dPnL*10*0.4)
            best_k, best_score = 1.0, -99
            for k in K_LEVELS:
                r = results_k[k]
                if r['n'] >= 20:
                    s = (r['wr'] - base_wr) * 0.6 + (r['pnl'] - base_pnl) * 10 * 0.4
                    if s > best_score:
                        best_score, best_k = s, k

            best = results_k.get(best_k, {})
            dwr  = round(best.get('wr', 0)  - base_wr,  1)
            dpnl = round(best.get('pnl', 0) - base_pnl, 2)

            # Per-symbol output (chi tiet)
            if len(syms) == 1:
                rows = [f'&#x1F50D; <b>Conviction Backtest — {sym}</b>',
                        f'WF={wf} | Regime={use_regime} | VWAP={use_vwap}', '']
                for k in K_LEVELS:
                    r = results_k[k]
                    stat = '&#x2705;' if r['n'] >= 30 else ('&#x1F7E1;' if r['n'] >= 20 else '&#x274C;')
                    best_mark = ' &#x1F3AF;' if k == best_k else ''
                    rows.append(f'{stat} K={k:.1f}: {r["n"]}L WR={r["wr"]:.1f}% PnL={r["pnl"]:+.2f}%{best_mark}')
                rows += ['',
                    f'&#x1F3AF; <b>Best K={best_k:.1f}</b>: {best.get("n",0)}L WR={best.get("wr",0):.1f}% PnL={best.get("pnl",0):+.2f}%',
                    f'Delta vs K=1.0: dWR={dwr:+.1f}% dPnL={dpnl:+.2f}%']
                send(NL.join(rows), chat_id)

            summary.append({
                'sym': sym, 'wf': wf, 'best_k': best_k,
                'base_n': base.get('n', 0), 'best_n': best.get('n', 0),
                'base_wr': base_wr, 'best_wr': best.get('wr', 0),
                'base_pnl': base_pnl, 'best_pnl': best.get('pnl', 0),
                'dwr': dwr, 'dpnl': dpnl,
            })

        # Summary cho all
        if len(summary) > 1:
            from collections import Counter
            k_dist = Counter(r['best_k'] for r in summary)
            lines_out = ['&#x1F50D; <b>Conviction Backtest — Tong ket</b>', '']
            lines_out.append(f'{"Ma":<6} {"K*":>4} {"Base":>12} {"Best":>12} {"dWR":>7} {"dPnL":>7}')
            lines_out.append('─' * 48)
            for r in summary:
                icon = '&#x2705;' if r['dwr'] >= 1 and r['dpnl'] >= 0 else ('&#x1F7E1;' if r['dwr'] >= 0 else '&#x274C;')
                b_str = f'{r["base_n"]}L {r["base_wr"]:.0f}%'
                k_str = f'{r["best_n"]}L {r["best_wr"]:.0f}%'
                lines_out.append(f'{r["sym"]:<6} K={r["best_k"]:.1f} {b_str:>12} {k_str:>12} {r["dwr"]:>+6.1f}% {r["dpnl"]:>+6.2f}% {icon}')
            lines_out.append('')
            lines_out.append('&#x1F4CA; Phan bo Best K:')
            for k in sorted(k_dist):
                syms_k = [r['sym'] for r in summary if r['best_k'] == k]
                lines_out.append(f'  K={k:.1f} ({k_dist[k]} ma): {", ".join(syms_k)}')
            avg_dwr  = sum(r['dwr']  for r in summary) / len(summary)
            avg_dpnl = sum(r['dpnl'] for r in summary) / len(summary)
            lines_out.append(f'')
            lines_out.append(f'Avg dWR={avg_dwr:+.1f}% | Avg dPnL={avg_dpnl:+.2f}%')
            send(NL.join(lines_out), chat_id)

    except Exception as e:
        import traceback
        send('&#x274C; Loi Conviction Backtest: ' + str(e)[:200], chat_id)


def _handle_vwapbt(symbol_or_all, chat_id):
    """Backtest VWAP: so sanh Score A co/khong co VWAP bonus."""
    from config import SYMBOL_CONFIG, BACKTEST_WATCHLIST
    NL = chr(10)

    if symbol_or_all == 'ALL':
        syms = BACKTEST_WATCHLIST
        send('&#x1F4CA; Dang chay VWAP backtest <b>toan watchlist</b> (28 ma)...'
             + NL + 'Uoc tinh ~15 phut', chat_id)
    else:
        syms = [symbol_or_all]
        send('&#x1F4CA; Dang chay VWAP backtest <b>' + symbol_or_all + '</b>...', chat_id)

    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from backtest import run_backtest_symbol

        results = []
        for sym in syms:
            cfg = SYMBOL_CONFIG.get(sym.upper(), {})
            use_regime = cfg.get('use_regime', True)

            # Load data 1 lần, tái sử dụng cho cả 2 runs (tránh 2x API calls)
            from backtest import load_data, LOOKBACK_DAYS
            _cfg = SYMBOL_CONFIG.get(sym.upper(), {})
            _days = _cfg.get('days', LOOKBACK_DAYS)
            _df, _ = load_data(sym, days=_days)
            if _df is None:
                send('&#x274C; Khong tai duoc data ' + sym, chat_id)
                continue

            r_no  = run_backtest_symbol(sym, verbose=False,
                                        use_regime=use_regime, use_vwap=False,
                                        _df_cache=_df)
            r_yes = run_backtest_symbol(sym, verbose=False,
                                        use_regime=use_regime, use_vwap=True,
                                        _df_cache=_df)

            st_no  = r_no.get('buy', {})  if r_no  else {}
            st_yes = r_yes.get('buy', {}) if r_yes else {}

            wr_no   = st_no.get('win_rate', 0)
            wr_yes  = st_yes.get('win_rate', 0)
            pnl_no  = st_no.get('avg_pnl', 0)
            pnl_yes = st_yes.get('avg_pnl', 0)
            n_no    = st_no.get('total', 0)
            n_yes   = st_yes.get('total', 0)
            dwr     = round(wr_yes - wr_no, 1)
            dpnl    = round(pnl_yes - pnl_no, 2)

            if dwr >= 1.0 and dpnl >= 0:   flag = '&#x2705; CO ICH'
            elif dwr < -1.0 and dpnl < 0:  flag = '&#x274C; CO HAI'
            elif dwr >= 0 or dpnl >= 0:    flag = '&#x1F7E1; CO ICH NHE'
            else:                           flag = '&#x3030; TRUNG TINH'

            results.append({'sym': sym, 'dwr': dwr, 'dpnl': dpnl,
                             'wr_no': wr_no, 'pnl_no': pnl_no,
                             'wr_yes': wr_yes, 'pnl_yes': pnl_yes,
                             'n_no': n_no, 'n_yes': n_yes, 'flag': flag})

            # Per-symbol output
            if len(syms) == 1:
                msg = (NL + '&#x1F4CA; VWAP Backtest — <b>' + sym + '</b>' + NL
                       + '─' * 30 + NL
                       + f'  Khong VWAP: {n_no}L | WR={wr_no:.1f}% | PnL={pnl_no:+.2f}%' + NL
                       + f'  Co VWAP:    {n_yes}L | WR={wr_yes:.1f}% | PnL={pnl_yes:+.2f}%' + NL
                       + f'  Delta: dWR={dwr:+.1f}% | dPnL={dpnl:+.2f}%' + NL
                       + flag)
                send(msg, chat_id)

        # Summary cho all
        if len(results) > 1:
            avg_dwr  = sum(r['dwr']  for r in results) / len(results)
            avg_dpnl = sum(r['dpnl'] for r in results) / len(results)
            pos = [r for r in results if r['dwr'] >= 1.0 and r['dpnl'] >= 0]
            neg = [r for r in results if r['dwr'] < -1.0 and r['dpnl'] < 0]

            lines = ['&#x1F4CA; <b>VWAP Backtest — Tong ket 28 ma</b>', '']
            lines.append(f'{"Ma":<6} {"dWR":>6} {"dPnL":>7}  Ket qua')
            lines.append('─' * 35)
            for r in results:
                icon = '&#x2705;' if 'CO ICH' in r['flag'] and 'NHE' not in r['flag'] else (
                       '&#x274C;' if 'CO HAI' in r['flag'] else '&#x1F7E1;')
                lines.append(f'{r["sym"]:<6} {r["dwr"]:>+5.1f}%  {r["dpnl"]:>+5.2f}%  {icon}')
            lines.append('')
            lines.append(f'Avg dWR  = {avg_dwr:+.1f}%')
            lines.append(f'Avg dPnL = {avg_dpnl:+.2f}%')
            lines.append(f'CO ICH: {len(pos)} ma | CO HAI: {len(neg)} ma')

            verdict = ('&#x2705; VWAP CO ICH — giu use_vwap=True'
                       if avg_dwr >= 1.0 else
                       '&#x1F7E1; VWAP TRUNG TINH — xem xet per-symbol'
                       if avg_dwr >= 0 else
                       '&#x274C; VWAP CO HAI — can xem xet tat per-symbol')
            lines.append(verdict)
            send(NL.join(lines), chat_id)

    except Exception as e:
        send('&#x274C; Loi VWAP backtest: ' + str(e)[:200], chat_id)


def handle_start(chat_id):
    # Kick Flask warmup ngay khi user bắt đầu dùng bot
    try:
        call_api('/api/warmup')
    except Exception:
        pass
    msg = (
        '<b>VN Trader Bot v4.4</b> — Chao mung!\n\n'
        '<b>&#x1F4CB; QUY TRINH (doc truoc khi dung):</b>\n'
        'T1 8:30 /macro → /market → /signals\n'
        'T2 9:00 /score SYM → /shark SYM (neu can)\n'
        'T3 11:00 /volscan SYM → /whatif SYM GIA → lenh\n'
        'T4 Auto alerts (khong can lam gi)\n'
        'T5 Cuoi tuan: /bt /convbt /trigbt (research)\n\n'
        '/workflow — Xem chi tiet quy trinh 5 tang\n\n'
        '<b>Lenh thuong dung:</b>\n'
        '/workflow     — Quy trinh su dung bot (doc truoc)\n'
        '/price VCB - Gia hien tai\n'
        '/analyze FPT - Phan tich day du 8 lop\n'
        '/score FPT    - Tuong tu /analyze (alias)\n'
        '/whatif VCB 59000 - Neu VCB ve 59k thi sao?\n'
        '/check VCB 85000 - Kiem tra vi the mua tai 85k\n'
        '/bt MBB              — Backtest compact (~3 phut)\n'
        '/bt MBB s=60 sl=5 tp=20 — Custom score/SL/TP\n'
        '/bt all              — Ca watchlist (~15 phut)\n'
        '/optimize MBB        — Tim SL/TP/Hold toi uu (~5 phut)\n'
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
        '<b>VWAP &amp; Backtest:</b>\n'
        '/vwapbt DGC   — Backtest VWAP bonus (1 ma, ~30 giay)\n'
        '/vwapbt all   — VWAP backtest toan watchlist (~15 phut)\n\n'
        '<b>Quan ly theo doi:</b>\n'
        '/subscribe    — Dang ky nhan alert tu dong\n'
        '/unsubscribe  — Huy dang ky alert\n'
        '/subscribers  — Danh sach nguoi theo doi (admin)\n\n'
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
    # Regime warning ở đầu signals
    try:
        from backtest import get_market_regime
        _reg = get_market_regime()
        if _reg.get('regime') == 'BEAR':
            send(
                '&#x1F534; <b>CANH BAO: BEAR MARKET</b>' + chr(10)
                + 'VNI=' + f'{_reg["vni"]:,.0f}' + ' duoi MA200=' + f'{_reg["ma200"]:,.0f}' + chr(10)
                + '<i>He thong da cap score xuong 58 — uu tien THEO DOI, giam size.</i>',
                chat_id
            )
        elif _reg.get('regime') == 'BULL':
            send(
                '&#x1F7E2; <b>BULL MARKET</b> — VNI>MA50>MA200' + chr(10)
                + '<i>+3 bonus cho ma score 62-64 sat nguong.</i>',
                chat_id
            )
    except Exception:
        pass
    data = call_api('/api/signals')
    if not data:
        # Cold start: Flask cần ~60-90s warmup lần đầu
        import time as _t
        send('&#x23F3; He thong dang khoi dong (lan dau chay mat ~60s).\n'
             'Dang thu lai tu dong...', chat_id)
        _t.sleep(30)
        data = call_api('/api/signals')
    if not data:
        send('&#x274C; Chua lay duoc tin hieu sau 2 lan thu.\n'
             'Vui long thu lai sau 1-2 phut hoac dung:\n'
             '/analyze VCB\n/analyze DGC', chat_id)
        return

    # Tính Macro Risk Score nhanh để gắn vào đầu signals
    _macro_prefix = ''
    try:
        import sys, os
        bot_dir = os.path.dirname(os.path.abspath(__file__))
        if bot_dir not in sys.path: sys.path.insert(0, bot_dir)
        import market_context as mc
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

            # Entry timing warning cho mã có entry bias mạnh
            entry_warn = ''
            cfg_sym = SYMBOL_CONFIG.get(sym, {})
            if action == 'MUA' and cfg_sym.get('entry'):
                entry_t    = cfg_sym['entry']
                entry_note = cfg_sym.get('entry_note', '')
                entry_warn = '\n &#x23F0; <b>Vao lenh: ' + entry_t + '</b>'
                if entry_note:
                    entry_warn += ' — ' + entry_note

            msg += (
                ae + ' <b>' + sym + '</b> — <b>' + action + '</b> (' + str(score) + '/100)\n'
                + meta_line
                + score_note
                + ' Gia: ' + f'{p:,.0f}' + 'd  RSI: ' + str(item.get('rsi', 0)) + '\n'
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
    NL   = chr(10)
    msg  = '<b>Chi so thi truong</b>' + NL + NL

    for key, val in data.items():
        if isinstance(val, dict):
            p   = val.get('price', 0)
            chg = val.get('change_pct', 0)
            arr = '+' if chg >= 0 else ''
            msg += '<b>' + val.get('name', key) + '</b>: ' + f'{p:,.2f}' + ' (' + arr + f'{chg:.2f}%)' + NL

    # ── Market Regime block ──────────────────────────────────────
    try:
        reg = call_api('/api/regime')
        regime  = reg.get('regime', 'UNKNOWN')
        label   = reg.get('label', '')
        vni     = reg.get('vni', 0)
        ma50    = reg.get('ma50', 0)
        ma200   = reg.get('ma200', 0)

        regime_emoji = {'BULL': '&#x1F7E2;', 'NEUTRAL': '&#x1F7E1;', 'BEAR': '&#x1F534;'}.get(regime, '&#x2753;')
        msg += NL + '─' * 24 + NL
        msg += '<b>Market Regime:</b> ' + regime_emoji + ' ' + regime + NL
        msg += label + NL
        msg += 'VNI=' + f'{vni:,.0f}' + ' | MA50=' + f'{ma50:,.0f}' + ' | MA200=' + f'{ma200:,.0f}' + NL

        if regime == 'BEAR':
            msg += NL + '<i>&#x26A0; Score A da bi cap xuong 58 — uu tien THEO DOI, giam size.</i>' + NL
        elif regime == 'BULL':
            msg += NL + '<i>Score 62-64 duoc cong +3 bonus — dieu kien vao lenh de hon.</i>' + NL
        else:
            msg += NL + '<i>Thi truong binh thuong — Score A khong bi dieu chinh.</i>' + NL
    except Exception as _e:
        msg += NL + '<i>Khong lay duoc Market Regime.</i>' + NL

    if msg.strip() == '<b>Chi so thi truong</b>':
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
                elif cmd == '/workflow':
                    _handle_workflow(cid)
                elif cmd in ('/analyze', '/score'):
                    handle_analyze(parts[1].upper() if len(parts) > 1 else 'VCB', cid)
                elif cmd == '/shark':
                    handle_shark(parts[1].upper() if len(parts) > 1 else 'DGC', cid)
                elif cmd == '/sharkbt':
                    handle_shark_backtest(parts[1].upper() if len(parts) > 1 else 'DGC', cid)
                elif cmd == '/vwapbt':
                    _sym = parts[1].upper() if len(parts) > 1 else 'all'
                    threading.Thread(target=lambda s=_sym, c=cid: _handle_vwapbt(s, c),
                                     daemon=True).start()
                elif cmd == '/convbt':
                    _sym = parts[1].upper() if len(parts) > 1 else 'all'
                    threading.Thread(target=lambda s=_sym, c=cid: _handle_convbt(s, c),
                                     daemon=True).start()
                elif cmd == '/optimize':
                    _sym = parts[1].upper() if len(parts) > 1 else ''
                    if not _sym:
                        send('Cu phap: <b>/optimize SYM</b>\nVi du: <b>/optimize DGC</b>', cid)
                    else:
                        threading.Thread(target=lambda s=_sym, c=cid: _handle_optimize(s, c),
                                         daemon=True).start()
                elif cmd == '/trigbt':
                    _sym = parts[1].upper() if len(parts) > 1 else 'all'
                    threading.Thread(target=lambda s=_sym, c=cid: _handle_trigbt(s, c),
                                     daemon=True).start()
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
        checks.append((True,  f'Score A: <b>{score}/100</b> (&gt;={min_score}) — Ky thuat xac nhan'))
    elif score >= 60:
        checks.append((None,  f'Score A: <b>{score}/100</b> — Gan nguong, chua du {min_score}'))
    else:
        checks.append((False, f'Score A: <b>{score}/100</b> — Chua du nguong {min_score}'))

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
        checks.append((None, 'VWAP: Khong ap dung cho ma nay'))
    elif pct_w >= 0 and pct_m >= 0:
        checks.append((True,  f'VWAP: P&gt;W({pct_w:+.1f}%) P&gt;M({pct_m:+.1f}%) — Dong tien ung ho &#x1F4C8;'))
    elif pct_w >= 0 or pct_m >= 0:
        pct_w_s = f'P&gt;W({pct_w:+.1f}%)' if pct_w >= 0 else f'P&lt;W({pct_w:+.1f}%)'
        pct_m_s = f'P&gt;M({pct_m:+.1f}%)' if pct_m >= 0 else f'P&lt;M({pct_m:+.1f}%)'
        checks.append((None,  f'VWAP: {pct_w_s} {pct_m_s} — Tin hieu pha tron'))
    else:
        checks.append((False, f'VWAP: P&lt;W({pct_w:+.1f}%) P&lt;M({pct_m:+.1f}%) — Dong tien yeu &#x1F4C9;'))

    # [4] Shark Score
    shark      = data.get('shark_score', 0)
    shark_mode = cfg.get('shark_mode', 'none')
    shark_min  = cfg.get('shark_min', 55)
    if shark_mode == 'none':
        checks.append((None, 'Shark: Khong ap dung cho ma nay'))
    elif shark >= shark_min:
        checks.append((True,  f'Shark: <b>{shark}/100</b> (&gt;={shark_min}) — Tich luy ro &#x1F988;'))
    elif shark >= 50:
        checks.append((None,  f'Shark: <b>{shark}/100</b> — Dang tich luy, chua du {shark_min}'))
    else:
        checks.append((False, f'Shark: <b>{shark}/100</b> — Chua co tin hieu tich luy'))

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
    if passed == 4:  remark = 'Tat ca 4 tang xac nhan — do tin cay cao nhat.'
    elif passed == 3:remark = f'3/4 xac nhan. {" ".join(miss)} chua ho tro — co the vao lenh.'
    elif passed == 2:remark = 'Tin hieu pha tron. Nen doi them xac nhan.'
    else:            remark = 'Tin hieu yeu. Nen cho co hoi tot hon.'

    wf = cfg.get('wf_verdict', '')
    if wf == 'TOT':  remark += ' WF=TOT: He thong robust.'
    elif wf == 'YEU':remark += ' WF=YEU: Giam size de phong ngua.'

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

    # Conviction block
    conv_block, conv_passed, conv_level = _build_conviction_block(item)
    lines.append(conv_block)

    lines += ['', '&#x1F50E; /analyze ' + sym + ' de xem chi tiet',
              '<i>Chi mang tinh tham khao, khong phai tu van</i>']

    return NL.join(lines)


# ── Cấu hình Scanner ────────────────────────────────────────────────────────
SCAN_INTERVAL_MIN      = 10    # Quét signal MUA/BAN mỗi 10 phút trong giờ giao dịch
ALERT_COOLDOWN_SEC     = 5400  # Không alert lại cùng mã
SHARK_SCAN_INTERVAL    = 60    # Quét Shark Score mỗi 60 phút cho watchlist
SHARK_ALERT_THRESHOLD  = 60    # Shark >= 60 → alert subscribers
SHARK_COOLDOWN_SEC     = 14400 # Không alert lại cùng mã trong 4 giờ

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
# WATCHLIST_EXTENDED: Tự động build từ SYMBOL_CONFIG
# Loại mã đã có trong SIGNALS_WATCHLIST hoặc SIGNALS_MANUAL (tránh alert trùng)
# Chỉ lấy mã có wf_verdict tốt (không phải rỗng hoặc YEU)
_EXTENDED_BASE = [
    'VCB', 'TCB', 'VPB', 'VHM', 'VIC',       # Ngân hàng / BĐS lớn
    'FPT', 'CMG',                               # Công nghệ
    'HPG',                                      # Thép
    'GAS', 'PVD', 'PVS', 'POW', 'REE',        # Dầu khí / Điện
    'MWG', 'VNM', 'MSN',                       # Tiêu dùng
    'KBC', 'SZC',                              # KCN
]
# Loại bỏ mã đã có trong watchlist chính để tránh alert trùng
WATCHLIST_EXTENDED = [
    s for s in _EXTENDED_BASE
    if s not in set(SIGNALS_WATCHLIST) | set(SIGNALS_MANUAL)
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

    # Lấy kết luận backtest từ config
    try:
        from config import get_shark_config as _gsc
        _scfg   = _gsc(symbol)
        _verdict = _scfg.get('verdict', '')
        _note    = _scfg.get('note', '')
        _pnl_ok  = _scfg.get('pnl_ok', True)
    except Exception:
        _verdict = ''; _note = ''; _pnl_ok = True

    msg  = f'{emoji} <b>Shark Alert — {symbol}</b>' + NL
    msg += '=' * 28 + NL
    msg += f'{bar} Shark: <b>{shark_score}/100</b> — {label}' + NL
    msg += f'{combo}' + NL + NL
    msg += f'Gia: <b>{price:,.0f}d</b> | Score A: <b>{score_a}/100</b>' + NL
    # Kết luận backtest
    if _verdict:
        msg += NL + f'<b>Ket luan backtest:</b>' + NL
        msg += f'{_verdict}' + NL
        if _note:
            msg += f'<i>{_note}</i>' + NL

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
