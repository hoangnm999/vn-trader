import os
import logging
import time
import threading
import requests
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')
API_URL = os.environ.get('API_BASE_URL', 'http://localhost:8080')

def send(text, chat_id=None):
    cid = chat_id or CHAT_ID
    if not TOKEN or not cid:
        return False
    MAX = 3800
    chunks = []
    if len(text) <= MAX:
        chunks =[text]
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
    for base in['http://localhost:8080', 'http://127.0.0.1:8080', API_URL]:
        try:
            r = requests.get(base + endpoint, timeout=45)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            logger.warning('api ' + base + endpoint + ': ' + str(e))
    return {}

def fmt_vol(v):
    if v >= 1000000:
        return f'{v/1000000:.1f}M'
    if v >= 1000:
        return f'{v/1000:.0f}K'
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
    # Dam bao khong co HTML tag nao lam loi Telegram HTML mode
    return (str(txt)
        .replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
        .replace('&amp;lt;', '&lt;') # Tranh double-escape
        .replace('&amp;gt;', '&gt;')
        .replace('&amp;amp;', '&amp;')
    )

def get_group(signals, key):
    lines =[]
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
        sups = data.get('supports',[])
        if sups:
            buy_zone = sups[0]['price']
            buy_zone_line = ' Cho gia ve : ' + f'{buy_zone:,.0f}' + 'd (vung HT - an toan hon)\n'
        else:
            buy_zone_line = ''
        return (
            ' Mua ngay : ' + f'{price:,.0f}' + 'd (neu tin hieu du manh)\n'
            + buy_zone_line
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
    sigs = data.get('signals',[])
    ichi = data.get('ichimoku', {})
    sups = data.get('supports',[])
    ress = data.get('resistances',[])
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
    
    cross_line = ''
    if gc:
        cross_line = '\n GOLDEN CROSS vua xuat hien!'
    if dc:
        cross_line = '\n DEATH CROSS vua xuat hien!'
        
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
        + 'Gia: <b>' + f'{price:,.0f}' + 'd</b> Diem: <b>' + str(score) + '/100</b> ' + ae + tio_line + div_line + cross_line + '\n\n'
        + '<b>1. RSI(14)</b>\n' + (rsi_lines or ' -&gt; Trung tinh') + '\n\n'
        + '<b>2. RSI Phan ky</b>\n' + (div_lines or ' -&gt; Khong phat hien phan ky') + '\n\n'
        + '<b>3. MACD</b>\n'
        + ' Line:' + f'{data.get("macd", 0):+.0f}' + ' Sig:' + f'{data.get("macd_signal", 0):+.0f}' + '\n'
        + (macd_lines or '') + '\n\n'
        + '<b>4. MA20 & MA50</b>\n'
        + ' MA20:' + f'{data.get("ma20", 0):,.0f}' + ' MA50:' + f'{data.get("ma50", 0):,.0f}' + '\n'
        + (ma_lines or '') + '\n\n'
        + '<b>5. Bollinger Bands</b>\n'
        + ' BB:' + f'{data.get("bb_lower", 0):,.0f}' + '-' + f'{data.get("bb_upper", 0):,.0f}' + '\n'
        + (bb_lines or '') + '\n\n'
        + '<b>6. Volume (Dong tien)</b>\n'
        + ' Hom nay:' + fmt_vol(data.get('vol_today', 0)) + ' TB20:' + fmt_vol(data.get('vol_avg_20', 0)) + '\n'
        + (vol_lines or '') + '\n\n'
        + '<b>7. Ichimoku</b>\n'
        + ' Tenkan:' + f'{ichi.get("tenkan", 0):,.0f}' + ' Kijun:' + f'{ichi.get("kijun", 0):,.0f}' + '\n'
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

def handle_start(chat_id):
    msg = (
        '<b>VN Trader Bot v4</b> - Chao mung!\n\n'
        'Bo chi so 8 lop thuc chien:\n'
        '1. RSI(14)\n'
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

def handle_signals(chat_id):
    send('Dang quet tin hieu HPG, FPT, VCB...', chat_id)
    data = call_api('/api/signals')
    if not data:
        send('Khong lay duoc tin hieu. Thu lai sau hoac dung:\n/analyze HPG\n/analyze FPT', chat_id)
        return
    msg = '<b>Top Tin Hieu Hom Nay</b>\n\n'
    for item in data:
        action = item.get('action', '')
        ae = action_emoji(action)
        vr = item.get('vol_ratio', 1.0)
        vb = '&#x1F525;' if vr >= 1.5 else ('&#x2B06;' if vr >= 1.0 else '&#x2B07;')
        div = item.get('rsi_divergence', {})
        tio = item.get('three_in_one', False)
        ichi = item.get('ichimoku', {})
        p = item.get('price', 0)
        ct = ichi.get('cloud_top', 0)
        cb = ichi.get('cloud_bottom', 0)
        
        if p > ct:
            is_ = 'Tren may'
        elif p < cb:
            is_ = 'Duoi may'
        else:
            is_ = 'Trong may'
            
        sups = item.get('supports',[])
        ress = item.get('resistances', [])
        div_txt = '\n PHAN KY: ' + escape_html(div['message']) if div.get('type') != 'none' else ''
        tio_txt = '\n HOI TU 3-TRONG-1!' if tio else ''
        
        msg += (
            ae + ' <b>' + item.get('symbol', '') + '</b> - <b>' + action + '</b> (' + str(item.get('score', 0)) + '/100)\n'
            + ' Gia: ' + f'{p:,.0f}' + 'd RSI: ' + str(item.get('rsi', 0)) + '\n'
            + ' ' + vb + ' Vol: ' + f'{vr:.1f}' + 'x ' + is_ + '\n'
            + (' HT: ' + f'{sups[0]["price"]:,.0f}' if sups else '')
            + (' KC: ' + f'{ress[0]["price"]:,.0f}' if ress else '') + '\n'
            + ' SL: ' + f'{item.get("stop_loss", 0):,.0f}' + ' TP: ' + f'{item.get("take_profit", 0):,.0f}' + '\n'
            + div_txt + tio_txt + '\n\n'
        )
    msg += '<i>Khong phai tu van dau tu</i>'
    send(msg, chat_id)

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
    logger.info('Bot v4 polling...')
    offset = 0
    while True:
        try:
            resp = requests.get(
                'https://api.telegram.org/bot' + TOKEN + '/getUpdates',
                params={'offset': offset, 'timeout': 30},
                timeout=35
            )
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
                            target = float(parts[2].replace(',', '').replace('.', ''))
                            if target < 100:
                                target *= 1000
                            handle_whatif(parts[1].upper(), target, cid)
                        except ValueError:
                            send('Gia khong hop le. VD: <b>/whatif VCB 59000</b>', cid)
                elif cmd == '/signals':
                    handle_signals(cid)
                elif cmd == '/market':
                    handle_market(cid)
                else:
                    send('Lenh khong nhan ra. Go /help de xem danh sach.', cid)
        except requests.exceptions.Timeout:
            pass
        except Exception as e:
            logger.error('Polling: ' + str(e))
            time.sleep(5)

# ── Cấu hình alert ──────────────────────────────────────────────────────────
SCORE_STRONG_BUY = 72 # >= 72 -> MUA manh
SCORE_STRONG_SELL = 28 # <= 28 -> BAN manh
ALERT_INTERVAL = 30 # phut
TRADING_HOURS = ((9, 0), (15, 0)) # 9:00 - 15:00 gio VN
_last_alerts = {} # sym -> (score, timestamp) - tranh gui lap

def is_trading_hours():
    now = datetime.now()
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
    ress = item.get('resistances',[])
    
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
            now = datetime.now()
            in_trading = is_trading_hours()
            # Chi chay trong gio giao dich hoac 8:30-9:00 chuan bi
            if now.weekday() < 5 and (in_trading or (now.hour == 8 and now.minute >= 30)):
                data = call_api('/api/signals')
                if data:
                    for item in data:
                        sym = item.get('symbol', '')
                        score = item.get('score', 50)
                        # Chi alert khi tin hieu MANH
                        if score < SCORE_STRONG_BUY and score > SCORE_STRONG_SELL:
                            continue
                        # Tranh gui lại trong vòng 90 phút với cùng mã + hướng
                        last = _last_alerts.get(sym)
                        if last:
                            last_score, last_time = last
                            same_direction = (last_score >= SCORE_STRONG_BUY) == (score >= SCORE_STRONG_BUY)
                            if same_direction and (time.time() - last_time) < 5400:
                                continue
                        _last_alerts[sym] = (score, time.time())
                        send(format_alert(item), CHAT_ID)
                        time.sleep(2)
            # 8:45 sang - bao cao dau ngay
            if now.weekday() < 5 and now.hour == 8 and now.minute == 45:
                send('<b>Bao cao 8:45 - Chuan bi phien giao dich</b>', CHAT_ID)
                handle_signals(CHAT_ID)
                time.sleep(70)
            # 15:05 - tong ket cuoi ngay
            if now.weekday() < 5 and now.hour == 15 and now.minute == 5:
                send('<b>Tong ket phien - Top tin hieu cuoi ngay</b>', CHAT_ID)
                handle_signals(CHAT_ID)
                time.sleep(70)
        except Exception as e:
            logger.error('Scanner error: ' + str(e))
        # Nghi 30 phut
        time.sleep(ALERT_INTERVAL * 60)

def main():
    threading.Thread(target=auto_alert_scanner, daemon=True).start()
    poll_updates()

if __name__ == '__main__':
    main()
