import os
import logging
import time
import threading
import requests
from datetime import datetime
import pytz

VN_TZ = pytz.timezone('Asia/Ho_Chi_Minh')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')
API_URL = os.environ.get('API_BASE_URL', 'http://localhost:8080')

# ── Watchlist chính thức — 20 mã đạt chuẩn backtest 7 năm ───────────────────
# tier: 1=Vàng / 2=Bạc / 3=Đồng / 0=Theo dõi
# pf/wr: kết quả backtest thực tế | score_min: ngưỡng MUA tối ưu
WATCHLIST_META = {
    # ── Tier 1 — Vàng ────────────────────────────────────────────────────────
    'DCM': {'tier': 1, 'pf': 2.22, 'wr': 57.1, 'score_min': 65, 'sl': 7, 'tp': 14, 'group': 'Hoa chat'},
    'SZC': {'tier': 1, 'pf': 2.18, 'wr': 60.5, 'score_min': 65, 'sl': 7, 'tp': 14, 'group': 'BDS KCN'},
    'DGC': {'tier': 1, 'pf': 2.16, 'wr': 61.7, 'score_min': 65, 'sl': 7, 'tp': 14, 'group': 'Hoa chat'},
    'FPT': {'tier': 1, 'pf': 2.01, 'wr': 61.5, 'score_min': 70, 'sl': 5, 'tp':  9, 'group': 'Cong nghe'},
    'GAS': {'tier': 1, 'pf': 1.96, 'wr': 56.9, 'score_min': 65, 'sl': 7, 'tp': 14, 'group': 'Nang luong'},
    # ── Tier 2 — Bạc ────────────────────────────────────────────────────────
    'KDH': {'tier': 2, 'pf': 1.99, 'wr': 58.9, 'score_min': 80, 'sl': 7, 'tp': 14, 'group': 'BDS mid'},
    'HSG': {'tier': 2, 'pf': 1.98, 'wr': 56.0, 'score_min': 70, 'sl': 7, 'tp': 14, 'group': 'Thep'},
    'PDR': {'tier': 2, 'pf': 1.82, 'wr': 56.0, 'score_min': 65, 'sl': 7, 'tp': 14, 'group': 'BDS'},
    'SSI': {'tier': 2, 'pf': 1.77, 'wr': 53.3, 'score_min': 70, 'sl': 5, 'tp':  9, 'group': 'Chung khoan'},
    'MBB': {'tier': 2, 'pf': 1.76, 'wr': 60.0, 'score_min': 70, 'sl': 5, 'tp':  9, 'group': 'Ngan hang'},
    'PC1': {'tier': 2, 'pf': 1.69, 'wr': 52.7, 'score_min': 65, 'sl': 7, 'tp': 14, 'group': 'Dien'},
    'NKG': {'tier': 2, 'pf': 1.62, 'wr': 53.1, 'score_min': 65, 'sl': 7, 'tp': 14, 'group': 'Thep'},
    'VIC': {'tier': 2, 'pf': 1.64, 'wr': 42.0, 'score_min': 65, 'sl': 7, 'tp': 14, 'group': 'BDS'},
    'BID': {'tier': 2, 'pf': 1.58, 'wr': 58.5, 'score_min': 70, 'sl': 5, 'tp':  9, 'group': 'Ngan hang'},
    # ── Tier 3 — Đồng ───────────────────────────────────────────────────────
    'HCM': {'tier': 3, 'pf': 1.47, 'wr': 51.1, 'score_min': 75, 'sl': 5, 'tp':  9, 'group': 'Chung khoan'},
    'VND': {'tier': 3, 'pf': 1.40, 'wr': 47.0, 'score_min': 75, 'sl': 5, 'tp':  9, 'group': 'Chung khoan'},
    'KBC': {'tier': 3, 'pf': 1.39, 'wr': 54.5, 'score_min': 65, 'sl': 7, 'tp': 14, 'group': 'BDS KCN'},
    'NVL': {'tier': 3, 'pf': 1.36, 'wr': 52.2, 'score_min': 70, 'sl': 7, 'tp': 14, 'group': 'BDS'},
    'VCB': {'tier': 3, 'pf': 1.32, 'wr': 58.6, 'score_min': 80, 'sl': 5, 'tp':  9, 'group': 'Ngan hang'},
    'PVS': {'tier': 3, 'pf': 1.28, 'wr': 53.8, 'score_min': 70, 'sl': 7, 'tp': 14, 'group': 'Dau khi'},
  
}

TIER_ICON = {1: '&#x1F947;', 2: '&#x1F948;', 3: '&#x1F949;', 0: '&#x1F7E1;'}
TIER_LABEL = {1: 'Tier 1', 2: 'Tier 2', 3: 'Tier 3', 0: 'Theo doi'}



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
                        f'Score>={cfg_min_score} | '
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
                thresh_lines += f' Score>={thr}: {t_n}L | WR={t_wr:.0f}% | PnL={t_pnl:+.1f}%{flag}\n'

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
    msg += f'(21 ma chuan | Chi hien thi score >= nguong toi uu)\n\n'
    buy_symbols = []

    if not wl_signals:
        msg += '&#x1F7E1; Hom nay chua co tin hieu hop le trong watchlist.\n'
        msg += '(Cac ma co the dang o THEO DOI hoac score chua du nguong)\n'
    else:
        for item, meta in wl_signals:
            sym    = item.get('symbol', '')
            action = item.get('action', '')
            score  = item.get('score', 0)
            tier   = meta['tier']
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

            # Dòng Tier + nhóm + PF/WR từ backtest
            tier_icon  = TIER_ICON.get(tier, '&#x1F7E1;')
            tier_label = TIER_LABEL.get(tier, '')
            meta_line  = (f' {tier_icon} {tier_label} | {meta["group"]} | '
                          f'PF={meta["pf"]:.2f} WR={meta["wr"]:.1f}% '
                          f'(BT 7nam)\n')

            # Score vs ngưỡng tối ưu
            score_note = (f' &#x2705; Score {score} >= {meta["score_min"]} (nguong toi uu ma nay)\n'
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
                + f' SL: {meta["sl"]}%  TP: {meta["tp"]}%'
                + div_txt + tio_txt + '\n\n'
            )
            if action == 'MUA':
                buy_symbols.append({'symbol': sym, 'score': score})

    # Mã bị lọc vì score chưa đủ
    if skipped:
        msg += '&#x23F3; <b>Cho nguong score:</b>\n'
        for sym, sc, min_sc, meta in skipped:
            tier_icon = TIER_ICON.get(meta['tier'], '')
            msg += f' {tier_icon} {sym}: Score={sc} (can >={min_sc})\n'
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
                            f'&#x2705; Score {score} >= nguong toi uu {best_thr}'
                            if score_ok else
                            f'&#x26A0; Score {score} < nguong toi uu {best_thr} (WR cao hon o >={best_thr})'
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
                                f' Nguong toi uu: <b>>={best_thr}</b> → '
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
