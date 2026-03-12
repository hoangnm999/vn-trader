import os
import logging
import time
import threading
import requests
from datetime import datetime

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cấu hình biến môi trường
TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')
API_URL = os.environ.get('API_BASE_URL', 'http://localhost:5000')

def send(text, chat_id=None):
    cid = chat_id or CHAT_ID
    if not TOKEN or not cid: 
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={'chat_id': cid, 'text': text, 'parse_mode': 'HTML'},
            timeout=10
        )
        return r.status_code == 200
    except Exception as e:
        logger.error(f"send: {e}")
        return False

def api(endpoint):
    # Thử localhost trước (nhanh hơn, tránh HTTPS overhead)
    for base in ['http://localhost:8080', 'http://127.0.0.1:8080', API_URL]:
        try:
            r = requests.get(f"{base}{endpoint}", timeout=45)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            logger.warning(f"api {base}{endpoint}: {e}")
    return {}

def fmt_vol(v):
    if v >= 1_000_000: return f"{v/1_000_000:.1f}M"
    if v >= 1_000: return f"{v/1_000:.0f}K"
    return str(int(v))

def ae(action):
    return '🟢' if 'MUA' in action else ('🔴' if 'BÁN' in action else '🟡')

def se(typ):
    return '🔼' if typ == 'bull' else ('🔽' if typ == 'bear' else '⚪')

def grp(signals, key):
    return '\n'.join(f" {se(t)} {txt}" for grp_key, t, txt in signals if grp_key == key) or ""

def build_msg(d, prefix="🔍 Phân tích"):
    sym = d.get('symbol','')
    price = d.get('price', 0)
    score = d.get('score', 50)
    act = d.get('action', 'THEO DÕI')
    sigs = d.get('signals', [])
    ichi = d.get('ichimoku', {})
    sups = d.get('supports', [])
    ress = d.get('resistances', [])
    div = d.get('rsi_divergence', {})
    vr = d.get('vol_ratio', 1.0)
    tio = d.get('three_in_one', False)
    gc = d.get('golden_cross', False)
    dc = d.get('death_cross', False)
    
    vol_bar = '🔥' if vr >= 1.5 else ('✅' if vr >= 1.0 else '⚠️')
    ct = ichi.get('cloud_top', 0)
    cb = ichi.get('cloud_bottom', 0)
    
    if price > ct: ichi_s = ' Trên mây ↑'
    elif price < cb: ichi_s = ' Dưới mây ↓'
    else: ichi_s = ' Trong mây'
    
    sup_txt = ', '.join(f"{s['price']:,.0f}({s['count']}x)" for s in sups[:2]) if sups else 'Không rõ'
    res_txt = ', '.join(f"{r['price']:,.0f}({r['count']}x)" for r in ress[:2]) if ress else 'Không rõ'
    
    cross_line = ''
    if gc: cross_line = '\n🚀 GOLDEN CROSS vừa xuất hiện!'
    if dc: cross_line = '\n💀 DEATH CROSS vừa xuất hiện!'
    
    div_line = f"\n\n🚨 PHÂN KỲ RSI:\n {div['message']}" if div.get('type') != 'none' and div.get('message') else ''
    tio_line = '\n\n💎 HỘI TỤ "3 TRONG 1": Giá trên MA20 + Volume đột biến + RSI hợp lý' if tio else ''
    
    msg = (
        f"<b>{prefix} {sym}</b>\n"
        f"{'─'*30}\n"
        f"💰 Giá: {price:,.0f}đ | {score}/100 {ae(act)} {act}\n"
        f"{tio_line}"
        f"{div_line}\n\n"
        f"━━ RSI(14) ━━━━━━━━━━━━━━━━\n"
        f"{grp(sigs,'RSI') or ' Trung tính'}\n\n"
        f"━━ RSI PHÂN KỲ ━━━━━━━━━━━━\n"
        f"{grp(sigs,'DIV') or ' Không phát hiện phân kỳ'}\n\n"
        f"━━ MACD ━━━━━━━━━━━━━━━━━━\n"
        f"Line: {d.get('macd',0):+.0f} Signal: {d.get('macd_signal',0):+.0f}\n"
        f"{grp(sigs,'MACD') or ''}\n\n"
        f"━━ MA20 & MA50 ━━━━━━━━━━━\n"
        f"MA20: {d.get('ma20',0):,.0f} | MA50: {d.get('ma50',0):,.0f}{cross_line}\n"
        f"{grp(sigs,'MA') or ''}\n\n"
        f"━━ BOLLINGER BANDS ━━━━━━━\n"
        f"BB: {d.get('bb_lower',0):,.0f} ─ {d.get('bb_upper',0):,.0f}\n"
        f"{grp(sigs,'BB') or ''}\n\n"
        f"━━ VOLUME (DÒNG TIỀN) ━━━━\n"
        f"Hôm nay: {fmt_vol(d.get('vol_today',0))} | TB20: {fmt_vol(d.get('vol_ma20',0))} {vol_bar}\n"
        f"{grp(sigs,'VOL') or ''}\n\n"
        f"━━ ICHIMOKU ━━━━━━━━━━━━━━\n"
        f"Tenkan: {ichi.get('tenkan',0):,.0f} | Kijun: {ichi.get('kijun',0):,.0f}\n"
        f"Mây: {cb:,.0f} – {ct:,.0f} {ichi_s}\n"
        f"{grp(sigs,'ICHI') or ''}\n\n"
        f"━━ HỖ TRỢ & KHÁNG CỰ ━━━━\n"
        f"🏠 Hỗ trợ : {sup_txt}\n"
        f"🚧 Kháng cự: {res_txt}\n"
        f"{grp(sigs,'SR') or ''}\n\n"
        f"━━ KẾT LUẬN ━━━━━━━━━━━━━━━━\n"
        f"{ae(act)} Khuyến nghị: <b>{act}</b> ({score}/100)\n"
        f"📍 Vào lệnh : {d.get('entry',0):,.0f}đ\n"
        f"🛡️ Stop Loss : {d.get('stop_loss',0):,.0f}đ (-7%)\n"
        f"🎯 Take Profit: {d.get('take_profit',0):,.0f}đ (+14%)\n"
        f"📊 Tỷ lệ R:R = 1:2\n\n"
        f"<i>Chỉ mang tính tham khảo, không phải tư vấn đầu tư</i>"
    )
    return msg

# --- HANDLERS ---

def handle_start(chat_id):
    msg = (
        "📊 VN Trader Bot v3 — Chào mừng!\n\n"
        "Bộ chỉ số 8 lớp thực chiến thị trường VN:\n"
        "1. RSI(14) — Động lượng\n"
        "2. RSI Phân kỳ — Cảnh báo đảo chiều\n"
        "3. MACD — Momentum\n"
        "4. MA20 & MA50 + Cắt nhau\n"
        "5. Bollinger Bands — Biên độ\n"
        "6. Volume thông minh — Cá mập\n"
        "7. Ichimoku — Xu hướng dài\n"
        "8. Hỗ trợ & Kháng cự cứng\n\n"
        "Lệnh:\n"
        "/price MÃ — Giá hiện tại\n"
        "/analyze MÃ — Phân tích 8 lớp\n"
        "/whatif MÃ GIÁ — Giả lập kịch bản\n"
        "/signals — Top tín hiệu tốt\n"
        "/market — Chỉ số thị trường\n"
    )
    send(msg, chat_id)

def handle_price(symbol, chat_id):
    send(f"⏳ Đang lấy giá {symbol}...", chat_id)
    d = api(f"/api/price/{symbol}")
    if d.get('price', 0) > 0:
        chg = d.get('change_pct', 0)
        arr = '📈' if chg >= 0 else '📉'
        send(f"🏷️ {symbol}\nGiá: {d['price']:,.0f}đ\nThay đổi: {arr} {chg:+.2f}%", chat_id)
    else:
        send(f"❌ {symbol}: {d.get('error','Không lấy được giá')}", chat_id)

def handle_analyze(symbol, chat_id):
    send(f"⏳ Đang phân tích {symbol} (8 chỉ số)...", chat_id)
    d = api(f"/api/analyze/{symbol}")
    if 'error' in d:
        send(f"❌ {symbol}: {d['error']}", chat_id)
        return
    send(build_msg(d), chat_id)

def handle_whatif(symbol, target, chat_id):
    send(f"⏳ Đang tính: Nếu {symbol} về {target:,.0f}đ...", chat_id)
    d = api(f"/api/whatif/{symbol}/{int(target)}")
    if 'error' in d:
        send(f"❌ {d['error']}", chat_id)
        return
    actual = d.get('price', 0)
    if actual > 0:
        dp = (target - actual) / actual * 100
        note = (f"📍 Giá hiện tại {actual:,.0f}đ → cần giảm {abs(dp):.1f}%" if dp < -0.5 
                else f"📍 Giá hiện tại {actual:,.0f}đ → đã vượt {dp:.1f}%" if dp > 0.5 
                else "📍 Đúng bằng giá hiện tại")
        send(note, chat_id)
    send(build_msg(d, prefix=f"🔮 What-If @ {target:,.0f}đ —"), chat_id)

def handle_signals(chat_id):
    send(f"⏳ Đang quét tín hiệu HPG, FPT, VCB...", chat_id)
    data = api("/api/signals")
    if not data:
        send("❌ Không lấy được tín hiệu.", chat_id)
        return
    
    msg = "🏆 <b>Top Tín Hiệu Hôm Nay</b>\n\n"
    for item in data:
        act = item.get('action','')
        a = ae(act)
        vr = item.get('vol_ratio', 1.0)
        vb = '🔥' if vr >= 1.5 else ('✅' if vr >= 1.0 else '⚠️')
        p = item.get('price', 0)
        
        msg += (
            f"{a} <b>{item.get('symbol','')}</b> — {act} ({item.get('score',0)}/100)\n"
            f"💰 {p:,.0f}đ | Vol: {vr:.1f}x {vb}\n"
            f"🎯 SL: {item.get('stop_loss',0):,.0f} TP: {item.get('take_profit',0):,.0f}\n\n"
        )
    send(msg, chat_id)

def handle_market(chat_id):
    data = api("/api/market")
    msg = "🌍 <b>Chỉ số thị trường</b>\n\n"
    for key, val in data.items():
        if isinstance(val, dict):
            p = val.get('price', 0)
            chg = val.get('change_pct', 0)
            arr = '🔼' if chg >= 0 else '🔽'
            msg += f"{arr} {val.get('name', key)}: {p:,.2f} ({chg:+.2f}%)\n"
    send(msg, chat_id)

# --- BOT CORE ---

def poll_updates():
    if not TOKEN:
        logger.error("Không có TOKEN"); return
    logger.info("Bot v3 polling...")
    offset = 0
    while True:
        try:
            resp = requests.get(f"https://api.telegram.org/bot{TOKEN}/getUpdates", 
                                params={'offset': offset, 'timeout': 30}, timeout=35)
            for upd in resp.json().get('result', []):
                offset = upd['update_id'] + 1
                msg = upd.get('message', {})
                if not msg: continue
                cid = str(msg.get('chat', {}).get('id', ''))
                text = msg.get('text', '').strip()
                if not text: continue
                
                parts = text.split()
                cmd = parts[0].lower().split('@')[0]
                
                if cmd in ('/start', '/help'):
                    handle_start(cid)
                elif cmd == '/price' and len(parts) > 1:
                    handle_price(parts[1].upper(), cid)
                elif cmd == '/analyze' and len(parts) > 1:
                    handle_analyze(parts[1].upper(), cid)
                elif cmd == '/whatif' and len(parts) > 2:
                    try:
                        t = float(parts[2].replace(',',''))
                        if t < 100: t *= 1000
                        handle_whatif(parts[1].upper(), t, cid)
                    except:
                        send("⚠️ Lỗi giá. VD: /whatif VCB 90000", cid)
                elif cmd == '/signals':
                    handle_signals(cid)
                elif cmd == '/market':
                    handle_market(cid)
        except Exception as e:
            logger.error(f"Polling: {e}")
            time.sleep(5)

def morning_scheduler():
    if not CHAT_ID: return
    while True:
        now = datetime.now()
        if now.weekday() < 5 and now.hour == 8 and now.minute == 45:
            send("☀️ <b>Báo cáo buổi sáng — Đang quét tín hiệu...</b>", CHAT_ID)
            handle_signals(CHAT_ID)
            time.sleep(70)
        time.sleep(30)

def main():
    threading.Thread(target=morning_scheduler, daemon=True).start()
    poll_updates()

if __name__ == '__main__':
    main()
