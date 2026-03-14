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
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')
API_URL = os.environ.get('API_BASE_URL', 'http://localhost:8080')

# ── Watchlist chính thức — 21 mã đạt chuẩn backtest 7 năm ───────────────────
# tier: 1=Vàng / 2=Bạc / 3=Đồng / 0=Theo dõi
# pf/wr: kết quả backtest thực tế | score_min: ngưỡng MUA tối ưu
WATCHLIST_META = {
    'HCM': {'score_min': 78, 'sl': 5, 'tp':  9, 'group': 'Chung khoan'},
    'PC1': {'score_min': 68, 'sl': 7, 'tp': 14, 'group': 'Dien'},
    'VCB': {'score_min': 78, 'sl': 5, 'tp':  9, 'group': 'Ngan hang'},
    'MBB': {'score_min': 68, 'sl': 5, 'tp':  9, 'group': 'Ngan hang'},
    'NKG': {'score_min': 78, 'sl': 7, 'tp': 14, 'group': 'Thep'},
    'VND': {'score_min': 75, 'sl': 5, 'tp':  9, 'group': 'Chung khoan'},
    'DGC': {'score_min': 68, 'sl': 7, 'tp': 14, 'group': 'Hoa chat'},
    'DCM': {'score_min': 70, 'sl': 7, 'tp': 14, 'group': 'Hoa chat'},
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
    expire   = (datetime.now(VN_TZ) + timedelta(days=PAPER_MONTHS * 30)).strftime('%Y-%m-%d')

    trade = {
        'id':         len(data['trades']) + 1,
        'symbol':     symbol,
        'entry_date': today,
        'entry_price':price,
        'score':      score,
        'sl_price':   sl_price,
        'tp_price':   tp_price,
        'sl_pct':     sl_pct,
        'tp_pct':     tp_pct,
        'expire_date':expire,
        'status':     'OPEN',
        'exit_date':  None,
        'exit_price': None,
        'pnl_pct':    None,
        'exit_reason':None,
    }
    data['trades'].append(trade)
    _save_paper(data)
    return True, trade

def _update_paper_prices():
    """
    Cập nhật giá hiện tại cho tất cả lệnh OPEN, kiểm tra TP/SL/EXPIRED.
    Gọi từ background scanner.
    """
    data = _load_paper()
    changed = False
    today   = datetime.now(VN_TZ).strftime('%Y-%m-%d')

    for t in data['trades']:
        if t['status'] != 'OPEN':
            continue

        # Kiểm tra hết hạn
        if today >= t['expire_date']:
            t['status']     = 'EXPIRED'
            t['exit_date']  = today
            t['exit_price'] = t['entry_price']  # Chưa có giá thực
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
    # FIX: Ưu tiên API_URL (Railway URL) trước, fallback về localhost
    bases = [API_URL, 'http://localhost:8080', 'http://127.0.0.1:8080']
    # Loại bỏ duplicate nếu API_URL là localhost
    seen = set()
    ordered = []
    for b in bases:
        if b not in seen:
            seen.add(b)
            ordered.append(b)

    for base in ordered:
        try:
            r = requests.get(base + endpoint, timeout=45)
            if r.status_code == 200:
                return r.json()
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
        return '&#x1F7E2;'
    if 'BAN' in action:
        return '&#x1F534;'
    return '&#x1F7E1;'


def sig_emoji(typ):
    if typ == 'bull':
        return '&#x1F4C8;'
    if typ == 'bear':
        return '&#x1F4C9;'
    return '&#x27A1;'


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
    action = data.get('action', '')
    price = data.get('price', 0)
    sl = data.get('stop_loss', 0)
    tp = data.get('take_profit', 0)
    sl_lbl = data.get('sl_label', '')
    tp_lbl = data.get('tp_label', '')

    if action == 'MUA':
        sups = data.get('supports', [])
        if sups:
            buy_zone = sups[0]['price']
            buy_zone_line = ' Cho gia ve : ' + f'{buy_zone:,.0f}' + 'd (vung HT - an toan hon)'
        else:
            buy_zone_line = ''

        return (
                ' Mua ngay : ' + f'{price:,.0f}' + 'd (neu tin hieu du manh)\n'
                + buy_zone_line + '\n'
                + ' Stop Loss : ' + f'{sl:,.0f}' + 'd (' + sl_lbl + ')\n'
                + ' Chot loi : ' + f'{tp:,.0f}' + 'd (' + tp_lbl + ')\n'
                + ' R:R = 1:2\n\n'
        )
    elif action == 'BAN':
        return (
                ' Nen ban o : ' + f'{price:,.0f}' + 'd (gia hien tai)\n'
                + ' Vung mua lai: ' + f'{tp:,.0f}' + 'd (vung ho tro gan nhat)\n'
                + ' Neu da mua : Cat lo neu gia tiep tuc giam them -7%\n\n'
        )
    else:
        return (
                ' Theo doi vung: ' + f'{sl:,.0f}' + 'd - ' + f'{tp:,.0f}' + 'd\n'
                + ' Chua du tin hieu de vao lenh\n\n'
        )


def build_analysis_msg(data, prefix='Phan tich'):
    sym = data.get('symbol', '')
    price = data.get('price', 0)
    score = data.get('score', 50)
    action = data.get('action', 'THEO DOI')
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

    vol_bar = '&#x1F525;' if vr >= 1.5 else ('&#x2B06;' if vr >= 1.0 else '&#x2B07;')
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

    rsi_lines = get_group(sigs, 'RSI')
    div_lines = get_group(sigs, 'DIV')
    macd_lines = get_group(sigs, 'MACD')
    ma_lines = get_group(sigs, 'MA')
    bb_lines = get_group(sigs, 'BB')
    vol_lines = get_group(sigs, 'VOL')
    ichi_lines = get_group(sigs, 'ICHI')
    sr_lines = get_group(sigs, 'SR')

    msg = (
            '<b>' + prefix + ' ' + sym + '</b>\n'
            + '=' * 30 + '\n'
            + 'Gia: <b>' + f'{price:,.0f}' + 'd</b> Diem: <b>' + str(score) + '/100</b> ' + ae + tio_line + div_line + '\n\n'
            + '<b>1. RSI(14)</b>\n' + (rsi_lines or ' -&gt; Trung tinh') + '\n\n'
            + '<b>2. RSI Phan ky</b>\n' + (div_lines or ' -&gt; Khong phat hien phan ky') + '\n\n'
            + '<b>3. MACD</b>\n'
            + ' Line:' + f'{data.get("macd", 0):+.0f}' + ' Sig:' + f'{data.get("macd_signal", 0):+.0f}\n' + (macd_lines or '') + '\n\n'
            + '<b>4. MA20 & MA50</b>\n'
            + ' MA20:' + f'{data.get("ma20", 0):,.0f}' + ' MA50:' + f'{data.get("ma50", 0):,.0f}\n' + (ma_lines or '') + '\n\n'
            + '<b>5. Bollinger Bands</b>\n'
            + ' BB:' + f'{data.get("bb_lower", 0):,.0f}' + '-' + f'{data.get("bb_upper", 0):,.0f}\n' + (bb_lines or '') + '\n\n'
            + '<b>6. Volume (Dong tien)</b>\n'
            + ' Hom nay:' + fmt_vol(data.get('vol_today', 0)) + ' TB20:' + fmt_vol(data.get('vol_tb20', 0)) + '\n' + (vol_lines or '') + '\n\n'
            + '<b>7. Ichimoku</b>\n'
            + ' Tenkan:' + f'{ichi.get("tenkan", 0):,.0f}' + ' Kijun:' + f'{ichi.get("kijun", 0):,.0f}\n'
            + ' May:' + f'{cb:,.0f}' + '-' + f'{ct:,.0f}' + ' ' + ichi_s + '\n'
            + (ichi_lines or '') + '\n\n'
            + '<b>8. Ho tro & Khang cu</b>\n'
            + ' HT: ' + sup_txt + '\n'
            + ' KC: ' + res_txt + '\n'
            + (sr_lines or '') + '\n\n'
            + '<b>KET LUAN</b>\n'
            + ' ' + ae + ' <b>' + action + '</b> (' + str(score) + '/100)\n'
            + build_action_lines(data)
            + '<i>Chi mang tinh tham khao, khong phai tu van dau tu</i>'
    )
    return msg


def handle_backtest(symbol, chat_id):
    """
    Chạy backtest 5 năm cho 1 mã và gửi kết quả qua Telegram.
    Chạy trong thread riêng để không block polling loop.
    """
    send('&#x1F4CA; Dang chay backtest <b>' + symbol + '</b> tren 7 nam...\n'
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
                send('&#x274C; ' + symbol + ': Khong du du lieu de backtest '
                     '(can it nhat 120 phien giao dich).', chat_id)
                return

            buy  = result.get('buy',  {})
            sell = result.get('sell', {})
            total = result.get('total_trades', 0)
            trades_df = result.get('trades')

            # Không có lệnh MUA nào
            if not buy:
                send('&#x1F7E1; ' + symbol + ': Khong co lenh MUA nao duoc phat '
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
                verdict      = '&#x2705; TIN HIEU DANG TIN CAY'
                verdict_note = 'Win rate, PnL va Profit Factor deu tot. Co the tin tuong tin hieu MUA tren ma nay.'
                verdict_bar  = '&#x1F7E2;&#x1F7E2;&#x1F7E2;'
            elif wr >= 55 and pnl >= 1 and pf >= 1.3:
                verdict      = '&#x1F7E1; CHAP NHAN DUOC'
                verdict_note = 'Ket qua on nhung chua xuat sac. Nen ket hop them phan tich tay truoc khi vao lenh.'
                verdict_bar  = '&#x1F7E2;&#x1F7E2;&#x1F7E1;'
            elif wr >= 50 and pnl >= 0:
                verdict      = '&#x1F7E1; TRUNG BINH'
                verdict_note = 'Bot hoat dong khong on dinh tren ma nay. Chi dung de tham khao, khong nen phu thuoc.'
                verdict_bar  = '&#x1F7E2;&#x1F7E1;&#x1F7E1;'
            else:
                verdict      = '&#x1F534; CAN THAN - KEM HIEU QUA'
                verdict_note = 'Tin hieu tren ma nay co ti le thua cao hon thang. Nen tim ma khac hoc lai weight.'
                verdict_bar  = '&#x1F534;&#x1F534;&#x1F534;'

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
                    if   yr_wr >= 60 and yr_pnl >= 0: icon = '&#x2705;'
                    elif yr_wr >= 50:                 icon = '&#x1F7E1;'
                    else:                             icon = '&#x274C;'
                    phase = PHASE.get(yr, str(yr))
                    time_slice_lines += (
                        f' {icon} <b>{yr}</b> ({phase}): '
                        f'WR=<b>{yr_wr:.0f}%</b> PnL={yr_pnl:+.1f}% '
                        f'| TP:{yr_tp} SL:{yr_sl} HK:{yr_hk} ({yr_n}L)\n'
                    )
                # Bull bias note
                if bull_bias == 'NGHIÊM TRỌNG':
                    time_slice_lines += '\n&#x26A0; <b>CANH BAO:</b> Ket qua bi thoi phong boi bull run 2021\n'
                elif bull_bias == 'TRUNG BINH':
                    time_slice_lines += '\n&#x26A0; Bull Bias trung binh — xem ket qua 2022+ de danh gia thuc te\n'
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
                    icon = '&#x2705;' if row['pnl'] > 0 else '&#x274C;'
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
                '&#x1F4CA; <b>BACKTEST ' + symbol + ' (5 NAM)</b>\n'
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
            send('&#x274C; Loi: Khong tim thay file backtest.py.\n'
                 'Hay dam bao backtest.py nam cung thu muc voi telegram_bot.py.', chat_id)
        except Exception as e:
            logger.error('handle_backtest ' + symbol + ': ' + str(e))
            import traceback
            logger.error(traceback.format_exc())
            send('&#x274C; Loi khi chay backtest ' + symbol + ': ' + str(e)[:120], chat_id)

    # Chạy trong thread riêng — không block polling
    threading.Thread(target=run, daemon=True).start()


def handle_wf(symbol, chat_id):
    """
    Walk-Forward Analysis cho 1 mã — gửi kết quả qua Telegram.
    IS=2năm → OOS=1năm, cuộn theo từng năm.
    """
    send(
        '&#x1F4CA; Dang chay <b>Walk-Forward</b> cho <b>' + symbol + '</b>\n'
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
                send('&#x274C; ' + symbol + ': Khong du du lieu walk-forward (can 3+ nam).', chat_id)
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
            if   verdict == 'V': v_icon = '&#x2705;'
            elif verdict == '~': v_icon = '&#x1F7E1;'
            elif verdict == '!': v_icon = '&#x26A0;'
            else:                v_icon = '&#x274C;'

            # ── Bảng từng cửa sổ ─────────────────────────────────────────
            win_lines = ''
            for w in windows:
                if w['oos_wr'] is None:
                    continue
                decay_w = w['is_wr'] - w['oos_wr']
                if   decay_w <=  5: flag = '&#x2705;'
                elif decay_w <= 15: flag = '&#x1F7E1;'
                elif decay_w <= 25: flag = '&#x26A0;'
                else:               flag = '&#x274C;'

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
                '&#x2705; Nguong score on dinh: ' + _safe(thresholds)
                if thr_stable else
                '&#x26A0; Nguong score bien dong: ' + _safe(thresholds)
                + '\n   &#x2192; He thong nhay cam voi thi truong, nen de dat khi dung'
            )

            # ── Decay assessment ──────────────────────────────────────────
            if   decay_wr <=  5: decay_txt = 'Rat on dinh (decay &lt; 5%)'
            elif decay_wr <= 10: decay_txt = 'On dinh (decay &lt; 10%)'
            elif decay_wr <= 20: decay_txt = 'Chap nhan duoc (decay &lt; 20%)'
            elif decay_wr <= 30: decay_txt = 'Canh bao (decay &gt; 20%)'
            else:                decay_txt = 'NGUY HIEM &#x2014; Co the overfit nghiem trong'

            msg = (
                '&#x1F4CA; <b>WALK-FORWARD: ' + symbol + '</b>\n'
                + f'SL={_sl*100:.0f}% TP={_tp*100:.0f}% | IS=2nam OOS=1nam\n'
                + '=' * 30 + '\n\n'
                + win_lines
                + '&#x1F4CB; <b>Tong ket:</b>\n'
                + f' IS  TB: WR={avg_is_wr:.1f}%  PnL={avg_is_pnl:+.2f}%\n'
                + f' OOS TB: WR=<b>{avg_oos_wr:.1f}%</b>  PnL=<b>{avg_oos_pnl:+.2f}%</b>\n'
                + f' Decay WR: <b>{decay_wr:+.1f}%</b> &#x2014; {decay_txt}\n'
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
            send('&#x274C; Loi walk-forward ' + symbol + ': ' + str(e)[:120], chat_id)

    threading.Thread(target=run, daemon=True).start()

def handle_dual(symbol, chat_id):
    """
    /dual VCB — Chạy backtest 2 mode Entry=T vs Entry=T+1, so sánh kết quả.
    """
    send(
        '&#x1F4CA; Dang chay <b>Dual Backtest</b> cho <b>' + symbol + '</b>\n'
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
                send('&#x274C; ' + symbol + ': Khong du du lieu.', chat_id)
                return

            mt   = res['mode_T']
            mt1  = res['mode_T1']
            wd   = res['wr_diff']
            pd_  = res['pnl_diff']
            flag = res['bias_flag']
            bias = res['bias_level']
            rec  = res['recommend']

            # Icons
            vmap  = {'V': '&#x2705;', '~': '&#x1F7E1;', '!': '&#x26A0;'}
            v_icon = vmap.get(flag, '&#x2753;')

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
                    icon = '&#x2B06;' if wr_diff_yr >= 0 else '&#x2B07;'
                    yr_lines += (
                        f' {icon} {yr}: T={d_t.get("win_rate",0):.0f}%'
                        f' &#x2192; T+1={d_t1.get("win_rate",0):.0f}%'
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
                '&#x1F4CA; <b>DUAL BACKTEST: ' + symbol + '</b>\n'
                + '=' * 28 + '\n\n'

                + '<b>Mode</b>\n'
                + ' Entry T   (BT) : ' + n_t_s + 'L | WR=' + wr_t_s
                + ' | PnL=' + pnl_t_s + ' | PF=' + pf_t_s + '\n'
                + ' Entry T+1 (TT) : ' + n_t1_s + 'L | WR=<b>' + wr_t1_s
                + '</b> | PnL=<b>' + pnl_t1s + '</b> | PF=' + pf_t1_s + '\n'
                + ' Chenh lech     : WR=' + wd_str + ' | PnL=' + pd_str + '\n\n'

                + ('<b>WR theo nam (T &#x2192; T+1):</b>\n' + yr_lines + '\n' if yr_lines else '')

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
            send('&#x274C; Loi dual backtest ' + symbol + ': ' + str(e)[:120], chat_id)

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
            send('&#x1F4CB; Chua co lenh paper trade nao. '
                 'Lenh se tu dong ghi nhan khi /signals phat MUA.', chat_id)
            return

        tp_list  = [t for t in closed if t['status'] == 'TP']
        sl_list  = [t for t in closed if t['status'] == 'SL']
        exp_list = [t for t in closed if t['status'] == 'EXPIRED']
        n_closed = len(closed)
        wr       = len(tp_list) / n_closed * 100 if n_closed > 0 else 0
        pnls     = [t['pnl_pct'] for t in closed if t['pnl_pct'] is not None]
        avg_pnl  = sum(pnls) / len(pnls) if pnls else 0

        if   wr >= 60 and avg_pnl > 0: verdict = '&#x2705; Hieu qua tot'
        elif wr >= 50 and avg_pnl > 0: verdict = '&#x1F7E1; Chap nhan duoc'
        else:                           verdict = '&#x274C; Can xem lai'

        # Bảng chi tiết lệnh đóng
        rows = ''
        for t in sorted(closed, key=lambda x: x['entry_date'], reverse=True)[:10]:
            icon = '&#x2705;' if t['pnl_pct'] and t['pnl_pct'] > 0 else '&#x274C;'
            pnl_s = f"{t['pnl_pct']:+.1f}%" if t['pnl_pct'] is not None else '--'
            rows += (f" {icon} <b>{t['symbol']}</b> "
                     f"{t['entry_date']} @{t['entry_price']:,.0f}d "
                     f"&#x2192; {t['exit_reason']} {pnl_s}\n")

        msg = (
            '&#x1F4CA; <b>PAPER TRADING — Bao Cao 2 Thang</b>\n'
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
                '&#x1F4CB; <b>Paper Trading — Danh Muc</b>\n\n'
                '&#x1F7E1; Khong co lenh OPEN nao.\n'
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
            icon   = '&#x1F7E2;' if pnl >= 0 else '&#x1F534;'
            days   = (datetime.now(VN_TZ).date() -
                      datetime.strptime(t['entry_date'], '%Y-%m-%d').date()).days
            rows += (
                f" {icon} <b>{t['symbol']}</b> Score={t['score']} | {days}ng\n"
                f"    Vao: {t['entry_price']:,.0f}d | Hien: {cur:,.0f}d\n"
                f"    PnL: <b>{pnl:+.1f}%</b> | "
                f"SL: {t['sl_price']:,.0f} TP: {t['tp_price']:,.0f}\n"
                f"    Het han: {t['expire_date']}\n\n"
            )

        avg_pnl = total_pnl / len(opened)
        msg = (
            f'&#x1F4CB; <b>Paper Trading — {len(opened)} Lenh Mo</b>\n'
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
                send('&#x274C; ' + symbol + ': Khong du du lieu de kiem tra.', chat_id)
                return

            # Icon verdict
            vmap = {'V': '&#x2705;', '~': '&#x1F7E1;', '!': '&#x26A0;', 'X': '&#x274C;'}
            v_icon = vmap.get(res['verdict'], '&#x2753;')

            issues     = res.get('issues', [])
            vmap = {'V': '&#x2705;', '~': '&#x1F7E1;', '!': '&#x26A0;', 'X': '&#x274C;'}
            v_icon2 = vmap.get(res['verdict'], '&#x2753;')

            # Check 1: Signal bias
            s_ok   = res.get('signal_ok_rate', 0)
            s_icon = '&#x2705;' if res.get('signal_bias', 0) == 0 else '&#x274C;'

            # Check 2: Entry price bias
            e_flip = res.get('entry_flip_rate', 0)
            e_diff = res.get('avg_entry_diff', 0)
            e_icon = '&#x2705;' if e_flip <= 20 else '&#x26A0;'

            # Check 3: Indicator bias
            ind_bias = res.get('indicator_bias', 0)
            i_icon   = '&#x2705;' if ind_bias == 0 else '&#x274C;'

            issues_txt = ''
            for iss in issues:
                issues_txt += f'  &#x26A0; {iss}\n'

            msg = (
                '&#x1F50E; <b>LOOKAHEAD BIAS: ' + symbol + '</b>\n'
                + '=' * 30 + '\n\n'

                + f'{s_icon} <b>[1] Signal Bias</b>\n'
                + f'   Score phu thuoc closes[idx]: {res.get("signal_total",0) - res.get("signal_bias",0)}'
                + f'/{res.get("signal_total",0)} diem ({s_ok:.1f}%)\n'
                + ('   &#x2705; Score luon phan ung voi gia — SACH\n' if res.get('signal_bias',0) == 0
                   else f'   &#x274C; {res.get("signal_bias",0)} diem khong phan ung — CO VAN DE\n')
                + '\n'

                + f'{e_icon} <b>[2] Entry Price Bias</b> (mua T vs T+1)\n'
                + f'   So lenh MUA test: {res.get("entry_total",0)}\n'
                + f'   PnL diff TB: {e_diff:.2f}% | Flip W/L: {e_flip:.1f}%\n'
                + ('   &#x2705; Flip thap (&lt;=20%) — structural bias khong dang ke\n' if e_flip <= 20
                   else f'   &#x26A0; {e_flip:.1f}% lenh doi ket qua khi mua T+1 — nen chay /dual de kiem tra\n')
                + '\n'

                + f'{i_icon} <b>[3] Indicator Bias</b> (EMA/MA)\n'
                + f'   Kiem tra {res.get("indicator_total",0)} diem\n'
                + ('   &#x2705; EMA nhat quan full vs cut array — SACH\n' if ind_bias == 0
                   else f'   &#x274C; {ind_bias} diem EMA sai lech — CO LOOKAHEAD BUG\n')
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
            send('&#x274C; Loi lookahead check ' + symbol + ': ' + str(e)[:120], chat_id)

    threading.Thread(target=run, daemon=True).start()


def handle_start(chat_id):
    msg = (
        '<b>VN Trader Bot v4.1</b> - Chao mung!\n\n'
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
        '/backtest VCB - Kiem tra do tin cay tin hieu 7 nam\n'
                             + '/wf VCB - Walk-Forward Analysis (IS=2y OOS=1y)\n'
                             + '/dual VCB - So sanh Entry=T vs T+1 (entry bias)\n'
                             + '/paper - Danh muc paper trading (2 thang)\n'
                             + '/paper report - Bao cao tong ket\n'
                             + '/lookahead VCB - Kiem tra lookahead bias\n'
        '/signals - Top tin hieu hom nay\n'
        '/market - Chi so thi truong\n\n'
        '<i>Khong phai tu van dau tu</i>'
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


def handle_analyze(symbol, chat_id):
    send('Dang phan tich <b>' + symbol + '</b> (8 chi so)...', chat_id)
    d = call_api('/api/analyze/' + symbol)
    if 'error' in d:
        send(symbol + ': ' + d['error'], chat_id)
        return
    send(build_analysis_msg(d), chat_id)


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

    price = data.get('price', 0)
    ma20 = data.get('ma20', 0)
    ma50 = data.get('ma50', 0)
    score = data.get('score', 50)
    action = data.get('action', '')
    sups = data.get('supports', [])
    ress = data.get('resistances', [])

    pnl_pct = (price - buy_price) / buy_price * 100 if buy_price > 0 else 0
    pnl_emoji = '&#x1F7E2;' if pnl_pct >= 0 else '&#x1F534;'
    pnl_sign = '+' if pnl_pct >= 0 else ''

    if buy_price > ma20 and buy_price > ma50:
        ma_pos = 'Mua tren ca MA20 va MA50 (vung an toan)'
        ma_emoji = '&#x2705;'
    elif buy_price > ma20:
        ma_pos = 'Mua tren MA20 nhung duoi MA50'
        ma_emoji = '&#x26A0;'
    elif buy_price > ma50:
        ma_pos = 'Mua tren MA50 nhung duoi MA20'
        ma_emoji = '&#x26A0;'
    else:
        ma_pos = 'Mua duoi ca MA20 va MA50 (vung rui ro)'
        ma_emoji = '&#x274C;'

    ht_txt = 'Chua xac dinh'
    kc_txt = 'Chua xac dinh'
    if sups:
        ht = sups[0]['price']
        ht_dist = (buy_price - ht) / buy_price * 100
        ht_txt = f'{ht:,.0f}d ({ht_dist:.1f}% duoi gia mua)'
    if ress:
        kc = ress[0]['price']
        kc_dist = (kc - buy_price) / buy_price * 100
        kc_txt = f'{kc:,.0f}d ({kc_dist:.1f}% tren gia mua)'

    sl = round(buy_price * 0.93, 0)
    tp = round(buy_price * 1.14, 0)

    if action == 'BAN' or score <= 35:
        if pnl_pct > 0:
            rec = '&#x1F534; Nen CHOT LOI - Tin hieu yeu, dang co lai ' + pnl_sign + f'{pnl_pct:.1f}%'
        elif pnl_pct > -7:
            rec = '&#x1F534; Can nhac CAT LO - Tin hieu xau, lo ' + f'{pnl_pct:.1f}%'
        else:
            rec = '&#x1F198; Da lo qua SL -7%, nen THOAT NGAY'
    elif action == 'MUA' or score >= 65:
        rec = '&#x1F7E2; GIU - Tin hieu con tot (' + str(score) + '/100)'
    else:
        if pnl_pct >= 14:
            rec = '&#x1F7E1; Gan muc CHOT LOI +14%, xem xet ban mot phan'
        elif pnl_pct <= -7:
            rec = '&#x1F198; Da cham muc CAT LO -7%, nen THOAT'
        else:
            rec = '&#x1F7E1; THEO DOI - Chua co tin hieu ro rang'

    msg = (
            '&#x1F4CB; <b>KIEM TRA VI THE ' + symbol + '</b>\n'
            + '=' * 30 + '\n\n'
            + '<b>Gia mua :</b> ' + f'{buy_price:,.0f}' + 'd\n'
            + '<b>Gia hien tai:</b> ' + f'{price:,.0f}' + 'd\n'
            + pnl_emoji + ' <b>Lai/Lo :</b> ' + pnl_sign + f'{pnl_pct:.1f}' + '%\n\n'
            + '<b>Vi tri so voi MA:</b>\n'
            + ' ' + ma_emoji + ' ' + ma_pos + '\n'
            + ' MA20: ' + f'{ma20:,.0f}' + 'd MA50: ' + f'{ma50:,.0f}' + 'd\n\n'
            + '<b>Ho tro / Khang cu:</b>\n'
            + ' HT gan nhat: ' + ht_txt + '\n'
            + ' KC gan nhat: ' + kc_txt + '\n\n'
            + '<b>SL/TP tu gia mua:</b>\n'
            + ' Cat lo (-7%): ' + f'{sl:,.0f}' + 'd'
            + (' (da vuot qua)' if price < sl else ' (con ' + f'{price - sl:,.0f}' + 'd dem)') + '\n'
            + ' Chot loi (+14%): ' + f'{tp:,.0f}' + 'd'
            + (' (da dat)' if price >= tp else ' (con ' + f'{tp - price:,.0f}' + 'd nua)') + '\n\n'
            + '<b>Tin hieu hien tai:</b> ' + str(score) + '/100 - ' + action + '\n\n'
            + '&#x1F4A1; <b>Khuyen nghi:</b>\n ' + rec + '\n\n'
            + '<i>Chi mang tinh tham khao, khong phai tu van dau tu</i>'
    )
    send(msg, chat_id)


def handle_signals(chat_id):
    send('Dang quet tin hieu thi truong...', chat_id)
    data = call_api('/api/signals')
    if not data:
        send('Khong lay duoc tin hieu. Thu lai sau hoac dung:\n/analyze HPG\n/analyze FPT\n/analyze VCB', chat_id)
        return

    # Lọc chỉ mã trong WATCHLIST_META + kiểm tra score >= score_min
    wl_signals  = []  # Tín hiệu hợp lệ trong watchlist
    skipped     = []  # Mã watchlist có tín hiệu nhưng score chưa đủ

    for item in data:
        sym    = item.get('symbol', '')
        score  = item.get('score', 0)
        action = item.get('action', '')
        meta   = WATCHLIST_META.get(sym)
        if not meta:
            continue  # Bỏ qua mã ngoài watchlist
        if action == 'MUA' and score < meta['score_min']:
            skipped.append((sym, score, meta['score_min'], meta))
            continue
        wl_signals.append((item, meta))

    now_str = datetime.now(VN_TZ).strftime('%d/%m %H:%M')
    msg = f'<b>&#x1F4CB; Tin Hieu Watchlist — {now_str}</b>\n'
    msg += f'({len(WATCHLIST_META)} ma chuan | Chi hien thi score &gt;= nguong toi uu)\n\n'
    buy_symbols = []

    if not wl_signals:
        msg += '&#x1F7E1; Hom nay chua co tin hieu hop le trong watchlist.\n'
        msg += '(Cac ma co the dang o THEO DOI hoac score chua du nguong)\n'
    else:
        for item, meta in wl_signals:
            sym    = item.get('symbol', '')
            action = item.get('action', '')
            score  = item.get('score', 0)
            ae     = action_emoji(action)
            vr     = item.get('vol_ratio', 1.0)
            vb     = '&#x1F525;' if vr >= 1.5 else ('&#x2B06;' if vr >= 1.0 else '&#x2B07;')
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

            # Score vs ngưỡng
            score_note = (f' &#x2705; Score {score} &gt;= {meta["score_min"]} (dat nguong)\n'
                          if score >= meta['score_min'] else
                          f' &#x26A0; Score {score} (nguong: {meta["score_min"]})\n')

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
                # Tự động ghi paper trade
                sl_pct = meta.get('sl', 7)
                tp_pct = meta.get('tp', 14)
                ok, result = _add_paper_trade(sym, p, score, sl_pct, tp_pct)
                if ok:
                    logger.info(f'Paper trade added: {sym} @{p} score={score}')

    # Mã bị lọc vì score chưa đủ
    if skipped:
        msg += '&#x23F3; <b>Cho nguong score:</b>\n'
        for sym, sc, min_sc, meta in skipped:
            msg += f' &#x1F4CC; {sym} ({meta["group"]}): Score={sc} (can &gt;={min_sc})\n'
        msg += '\n'

    msg += '<i>Khong phai tu van dau tu</i>'
    send(msg, chat_id)

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
                            bt_lines += f' &#x2753; <b>{sym}</b>: Chua du du lieu backtest\n\n'
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
                            f'&#x2705; Score {score} &gt;= nguong toi uu {best_thr}'
                            if score_ok else
                            f'&#x26A0; Score {score} &lt; nguong toi uu {best_thr} (WR cao hon o &gt;={best_thr})'
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
                                ico = '&#x2705;' if y_wr >= 55 and y_pnl >= 0 else ('&#x274C;' if y_wr < 45 else '&#x1F7E1;')
                                yr_summary += f'  {ico} {yr}: WR={y_wr:.0f}% PnL={y_pnl:+.1f}%\n'
                            yr_ok_str  = ', '.join(str(y) for y in good_yrs[-3:]) if good_yrs else 'Khong co'
                            yr_bad_str = ', '.join(str(y) for y in bad_yrs[-3:])  if bad_yrs  else 'Khong co'

                        # Verdict backtest
                        if wr >= 58 and pnl >= 2 and pf >= 1.5:
                            bt_verdict = '&#x2705; TIN CAY CAO — Backtest xac nhan tin hieu nay'
                        elif wr >= 52 and pnl >= 0:
                            bt_verdict = '&#x1F7E1; CHAP NHAN — Backtest ung ho nhung khong manh'
                        else:
                            bt_verdict = '&#x274C; CANH BAO — Backtest 7 nam cho thay tin hieu KEM tin cay'

                        bt_lines += (
                            f'&#x1F4CA; <b>{sym}</b> (Score hom nay: {score}/100)\n'
                            + f' WR tong the : <b>{wr:.1f}%</b> ({total} lenh / 7 nam)\n'
                            + f' PnL TB      : <b>{pnl:+.2f}%</b> | PF: {pf_s}\n'
                        )
                        if best_thr != 65:
                            bt_lines += (
                                f' Nguong toi uu: <b>&gt;={best_thr}</b> &#x2192; '
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
                            bt_lines += ' &#x1F534; Bull Bias: ket qua bi thoi phong boi 2021\n'
                        bt_lines += f' {bt_verdict}\n\n'

                    except Exception as e:
                        bt_lines += f' &#x2753; <b>{sym}</b>: Loi backtest ({str(e)[:60]})\n\n'
                        logger.error(f'signals backtest {sym}: {e}')

                if bt_lines:
                    send(
                        '&#x1F9EA; <b>BACKTEST CONTEXT — Do Tin Cay 5 Nam</b>\n'
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


def poll_updates():
    if not TOKEN:
        logger.error('Khong co TOKEN')
        return

    logger.info('Bot v4.1 polling...')
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

                logger.info('CMD: ' + text)
                parts = text.split()
                cmd = parts[0].lower().split('@')[0]

                if cmd in ('/start', '/help'):
                    handle_start(cid)
                elif cmd == '/price':
                    handle_price(parts[1].upper() if len(parts) > 1 else 'VCB', cid)
                elif cmd == '/analyze':
                    handle_analyze(parts[1].upper() if len(parts) > 1 else 'VCB', cid)
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
_last_alerts = {}


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


def format_alert(item):
    action = item.get('action', '')
    sym = item.get('symbol', '')
    score = item.get('score', 50)
    price = item.get('price', 0)
    vr = item.get('vol_ratio', 1.0)
    div = item.get('rsi_divergence', {})
    tio = item.get('three_in_one', False)
    sups = item.get('supports', [])
    ress = item.get('resistances', [])

    if score >= SCORE_STRONG_BUY:
        header = '&#x1F7E2;&#x1F6A8; TIN HIEU MUA MANH'
    else:
        header = '&#x1F534;&#x1F6A8; TIN HIEU BAN MANH'

    vol_line = ''
    if vr >= 1.5:
        vol_line = '\n &#x1F4B0; DONG TIEN LON: Vol ' + f'{vr:.1f}' + 'x TB20'

    div_line = ''
    if div.get('type') != 'none' and div.get('message'):
        div_line = '\n &#x1F514; PHAN KY RSI phat hien!'

    tio_line = ''
    if tio:
        tio_line = '\n &#x2705; HOI TU 3-TRONG-1 du dieu kien!'

    sr_line = ''
    if sups and score >= SCORE_STRONG_BUY:
        sr_line = '\n HT: ' + f'{sups[0]["price"]:,.0f}' + 'd'
    if ress and score <= SCORE_STRONG_SELL:
        sr_line = '\n KC: ' + f'{ress[0]["price"]:,.0f}' + 'd'

    msg = (
            header + '\n'
            + '=' * 28 + '\n'
            + '<b>' + sym + '</b> ' + f'{price:,.0f}' + 'd <b>' + str(score) + '/100</b>\n'
            + ' SL: ' + f'{item.get("stop_loss", 0):,.0f}' + 'd (-7%)'
            + ' TP: ' + f'{item.get("take_profit", 0):,.0f}' + 'd (+14%)'
            + vol_line + div_line + tio_line + sr_line
            + '\n\nDung /analyze ' + sym + ' de xem chi tiet'
            + '\n<i>Chi mang tinh tham khao</i>'
    )
    return msg


def auto_alert_scanner():
    if not CHAT_ID:
        return
    logger.info('Auto alert scanner started')
    while True:
        try:
            now = datetime.now(VN_TZ)
            in_trading = is_trading_hours()
            if now.weekday() < 5 and (in_trading or (now.hour == 8 and now.minute >= 30)):
                logger.info('Scanner tick: ' + now.strftime('%H:%M %a'))
                data = call_api('/api/signals')
                if data:
                    for item in data:
                        sym = item.get('symbol', '')
                        score = item.get('score', 50)

                        if score < SCORE_STRONG_BUY and score > SCORE_STRONG_SELL:
                            continue

                        last = _last_alerts.get(sym)
                        if last:
                            last_score, last_time = last
                            same_direction = (last_score >= SCORE_STRONG_BUY) == (score >= SCORE_STRONG_BUY)
                            if same_direction and (time.time() - last_time) < 5400:
                                continue

                        _last_alerts[sym] = (score, time.time())
                        send(format_alert(item), CHAT_ID)
                        time.sleep(2)

            if now.weekday() < 5 and now.hour == 8 and now.minute == 45:
                send('<b>Bao cao 8:45 - Chuan bi phien giao dich</b>', CHAT_ID)
                handle_signals(CHAT_ID)
                time.sleep(70)

            if now.weekday() < 5 and now.hour == 15 and now.minute == 5:
                send('<b>Tong ket phien - Top tin hieu cuoi ngay</b>', CHAT_ID)
                handle_signals(CHAT_ID)
                time.sleep(70)

        except Exception as e:
            logger.error('Scanner error: ' + str(e))
            time.sleep(300)


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
