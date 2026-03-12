import os, logging, time, threading, requests
from datetime import datetime

logging.basicConfig(level=logging.INFO, format=’%(asctime)s - %(levelname)s - %(message)s’)
logger = logging.getLogger(**name**)

TOKEN   = os.environ.get(‘TELEGRAM_BOT_TOKEN’, ‘’)
CHAT_ID = os.environ.get(‘TELEGRAM_CHAT_ID’, ‘’)
API_URL = os.environ.get(‘API_BASE_URL’, ‘http://localhost:5000’)

def send(text, chat_id=None):
cid = chat_id or CHAT_ID
if not TOKEN or not cid: return False
try:
r = requests.post(
f”https://api.telegram.org/bot{TOKEN}/sendMessage”,
json={‘chat_id’: cid, ‘text’: text, ‘parse_mode’: ‘HTML’},
timeout=10
)
return r.status_code == 200
except Exception as e:
logger.error(f”send: {e}”); return False

def api(endpoint):
# Thử localhost trước (nhanh hơn, tránh HTTPS overhead)
for base in [‘http://localhost:8080’, ‘http://127.0.0.1:8080’, API_URL]:
try:
r = requests.get(f”{base}{endpoint}”, timeout=45)
if r.status_code == 200:
return r.json()
except Exception as e:
logger.warning(f”api {base}{endpoint}: {e}”)
return {}

def fmt_vol(v):
if v >= 1_000_000: return f”{v/1_000_000:.1f}M”
if v >= 1_000:     return f”{v/1_000:.0f}K”
return str(int(v))

def ae(action):
return ‘🟢’ if ‘MUA’ in action else (‘🔴’ if ‘BÁN’ in action else ‘🟡’)

def se(typ):
return ‘📈’ if typ == ‘bull’ else (‘📉’ if typ == ‘bear’ else ‘➡️’)

def grp(signals, key):
return ‘\n’.join(f”  {se(t)} {txt}” for grp_key, t, txt in signals if grp_key == key) or ‘’

def build_msg(d, prefix=“📊 Phân tích”):
sym   = d.get(‘symbol’,’’)
price = d.get(‘price’, 0)
score = d.get(‘score’, 50)
act   = d.get(‘action’, ‘THEO DÕI’)
sigs  = d.get(‘signals’, [])
ichi  = d.get(‘ichimoku’, {})
sups  = d.get(‘supports’, [])
ress  = d.get(‘resistances’, [])
div   = d.get(‘rsi_divergence’, {})
vr    = d.get(‘vol_ratio’, 1.0)
tio   = d.get(‘three_in_one’, False)
gc    = d.get(‘golden_cross’, False)
dc    = d.get(‘death_cross’, False)

```
vol_bar = '🔥' if vr >= 1.5 else ('⬆️' if vr >= 1.0 else '⬇️')
ct = ichi.get('cloud_top', 0); cb = ichi.get('cloud_bottom', 0)
if price > ct:   ichi_s = '☁️ Trên mây ↑'
elif price < cb: ichi_s = '☁️ Dưới mây ↓'
else:            ichi_s = '☁️ Trong mây'

sup_txt = ', '.join(f"{s['price']:,.0f}({s['count']}x)" for s in sups[:2]) if sups else 'Chưa xác định'
res_txt = ', '.join(f"{r['price']:,.0f}({r['count']}x)" for r in ress[:2]) if ress else 'Chưa xác định'

cross_line = ''
if gc: cross_line = '\n  🌟 GOLDEN CROSS vừa xuất hiện!'
if dc: cross_line = '\n  💀 DEATH CROSS vừa xuất hiện!'

div_line = ''
if div.get('type') != 'none' and div.get('message'):
    div_line = f"\n\n🔔 <b>PHÂN KỲ RSI:</b>\n  {div['message']}"

tio_line = ''
if tio:
    tio_line = '\n\n✅ <b>HỘI TỤ "3 TRONG 1":</b> Giá trên MA20 + Volume đột biến + RSI hợp lệ → Đủ điều kiện vào lệnh!'

msg = (
    f"{prefix} <b>{sym}</b>\n"
    f"{'─'*30}\n"
    f"💰 Giá: <b>{price:,.0f}đ</b>  ⚡ <b>{score}/100</b>  {ae(act)} <b>{act}</b>\n"
    f"{tio_line}"
    f"{div_line}\n\n"

    f"━━ 1️⃣ RSI(14) ━━━━━━━━━━━━━━━━\n"
    f"{grp(sigs,'RSI') or '  ➡️ Trung tính'}\n\n"

    f"━━ 2️⃣ RSI PHÂN KỲ ━━━━━━━━━━━━\n"
    f"{grp(sigs,'DIV') or '  ➡️ Không phát hiện phân kỳ'}\n\n"

    f"━━ 3️⃣ MACD ━━━━━━━━━━━━━━━━━━\n"
    f"  Line: {d.get('macd',0):+.0f}  Signal: {d.get('macd_signal',0):+.0f}  Hist: {d.get('macd_hist',0):+.0f}\n"
    f"{grp(sigs,'MACD') or ''}\n\n"

    f"━━ 4️⃣ MA20 & MA50 ━━━━━━━━━━━\n"
    f"  MA20: {d.get('ma20',0):,.0f}  |  MA50: {d.get('ma50',0):,.0f}{cross_line}\n"
    f"{grp(sigs,'MA') or ''}\n\n"

    f"━━ 5️⃣ BOLLINGER BANDS ━━━━━━━\n"
    f"  BB: {d.get('bb_lower',0):,.0f} ─ {d.get('bb_upper',0):,.0f}  ({d.get('bb_pct',50):.0f}% trong dải)\n"
    f"{grp(sigs,'BB') or ''}\n\n"

    f"━━ 6️⃣ VOLUME (DÒNG TIỀN) ━━━━\n"
    f"  Hôm nay: {fmt_vol(d.get('vol_today',0))}  TB20: {fmt_vol(d.get('vol_ma20',0))}  {vol_bar} {vr:.1f}x\n"
    f"{grp(sigs,'VOL') or ''}\n\n"

    f"━━ 7️⃣ ICHIMOKU ━━━━━━━━━━━━━━\n"
    f"  Tenkan: {ichi.get('tenkan',0):,.0f}  Kijun: {ichi.get('kijun',0):,.0f}\n"
    f"  Mây: {cb:,.0f} – {ct:,.0f}  {ichi_s}\n"
    f"{grp(sigs,'ICHI') or ''}\n\n"

    f"━━ 8️⃣ HỖ TRỢ & KHÁNG CỰ ━━━━\n"
    f"  🟩 Hỗ trợ : {sup_txt}\n"
    f"  🟥 Kháng cự: {res_txt}\n"
    f"{grp(sigs,'SR') or ''}\n\n"

    f"━━ KẾT LUẬN ━━━━━━━━━━━━━━━━\n"
    f"  {ae(act)} Khuyến nghị: <b>{act}</b>  ({score}/100)\n"
    f"  Vào lệnh  : {d.get('entry',0):,.0f}đ\n"
    f"  Stop Loss : {d.get('stop_loss',0):,.0f}đ  <b>(-7%)</b>\n"
    f"  Take Profit: {d.get('take_profit',0):,.0f}đ  (+14%)\n"
    f"  Tỷ lệ R:R  = 1:2\n\n"
    f"⚠️ <i>Chỉ mang tính tham khảo, không phải tư vấn đầu tư</i>"
)
return msg
```

def handle_start(chat_id):
send((
“📈 <b>VN Trader Bot v3</b> — Chào mừng!\n\n”
“🔬 Bộ chỉ số <b>8 lớp</b> thực chiến thị trường VN:\n”
“1️⃣ RSI(14) — Động lượng\n”
“2️⃣ RSI Phân kỳ — Cảnh báo đảo chiều sớm\n”
“3️⃣ MACD — Xu hướng & momentum\n”
“4️⃣ MA20 & MA50 + Golden/Death Cross\n”
“5️⃣ Bollinger Bands — Biên độ\n”
“6️⃣ Volume thông minh — Dòng tiền cá mập\n”
“7️⃣ Ichimoku — Mây hỗ trợ/kháng cự\n”
“8️⃣ Hỗ trợ & Kháng cự ngang\n\n”
“<b>Lệnh:</b>\n”
“/price VCB — Giá hiện tại\n”
“/analyze FPT — Phân tích đầy đủ 8 lớp\n”
“/whatif VCB 59000 — Nếu VCB về 59k thì sao?\n”
“/signals — Top tín hiệu hôm nay\n”
“/market — Chỉ số thị trường\n\n”
“⚠️ <i>Không phải tư vấn đầu tư</i>”
), chat_id)

def handle_price(symbol, chat_id):
send(f”🔄 Đang lấy giá {symbol}…”, chat_id)
d = api(f”/api/price/{symbol}”)
if d.get(‘price’, 0) > 0:
chg = d.get(‘change_pct’, 0); arr = ‘🔺’ if chg >= 0 else ‘🔻’
send(f”💹 <b>{symbol}</b>\nGiá: <b>{d[‘price’]:,.0f}đ</b>\nThay đổi: {arr} {chg:+.2f}%”, chat_id)
else:
send(f”❌ {symbol}: {d.get(‘error’,‘Không lấy được giá’)}”, chat_id)

def handle_analyze(symbol, chat_id):
send(f”🔬 Đang phân tích <b>{symbol}</b> (8 chỉ số)…”, chat_id)
d = api(f”/api/analyze/{symbol}”)
if ‘error’ in d:
send(f”❌ {symbol}: {d[‘error’]}”, chat_id); return
send(build_msg(d), chat_id)

def handle_whatif(symbol, target, chat_id):
send(f”🔮 Đang tính: Nếu <b>{symbol}</b> về <b>{target:,.0f}đ</b>…”, chat_id)
d = api(f”/api/whatif/{symbol}/{int(target)}”)
if ‘error’ in d:
send(f”❌ {d[‘error’]}”, chat_id); return
actual = d.get(‘price’, 0)
if actual > 0:
dp = (target - actual) / actual * 100
note = (f”📍 Giá hiện tại {actual:,.0f}đ → cần giảm {abs(dp):.1f}% nữa” if dp < -0.5
else f”📍 Giá hiện tại {actual:,.0f}đ → đã vượt {dp:.1f}% rồi” if dp > 0.5
else “📍 Đúng bằng giá hiện tại”)
send(note, chat_id)
send(build_msg(d, prefix=f”🔮 What-If @ {target:,.0f}đ —”), chat_id)

def handle_signals(chat_id):
send(“🔄 Đang quét tín hiệu HPG, FPT, VCB…”, chat_id)
data = api(”/api/signals”)
if not data:
send(“❌ Không lấy được tín hiệu.\n\nThử lại sau ít phút hoặc dùng:\n/analyze HPG\n/analyze FPT\n/analyze VCB”, chat_id); return
msg = “🔔 <b>Top Tín Hiệu Hôm Nay</b>\n\n”
for item in data:
act  = item.get(‘action’,’’); a = ae(act)
vr   = item.get(‘vol_ratio’, 1.0)
vb   = ‘🔥’ if vr >= 1.5 else (‘⬆️’ if vr >= 1.0 else ‘⬇️’)
div  = item.get(‘rsi_divergence’, {})
tio  = item.get(‘three_in_one’, False)
ichi = item.get(‘ichimoku’, {})
p    = item.get(‘price’, 0)
ct   = ichi.get(‘cloud_top’, 0); cb = ichi.get(‘cloud_bottom’, 0)
is_  = ‘↑Trên mây’ if p > ct else (‘↓Dưới mây’ if p < cb else ‘~Trong mây’)
sups = item.get(‘supports’, []); ress = item.get(‘resistances’, [])
div_txt = f”\n  🔔 {div[‘message’]}” if div.get(‘type’) != ‘none’ and div.get(‘message’) else ‘’
tio_txt = “\n  ✅ Hội tụ 3-trong-1!” if tio else ‘’
msg += (
f”{a} <b>{item.get(‘symbol’,’’)}</b> — <b>{act}</b>  ({item.get(‘score’,0)}/100)\n”
f”  💰 {p:,.0f}đ  |  RSI: {item.get(‘rsi’,0)}\n”
f”  {vb} Vol: {vr:.1f}x  |  {is_}\n”
f”  🟩 HT: {sups[0][‘price’]:,.0f}” + (f”  🟥 KC: {ress[0][‘price’]:,.0f}” if ress else ‘’) + “\n”
f”  SL: {item.get(‘stop_loss’,0):,.0f}  TP: {item.get(‘take_profit’,0):,.0f}”
f”{div_txt}{tio_txt}\n\n”
)
msg += “⚠️ <i>Không phải tư vấn đầu tư</i>”
send(msg, chat_id)

def handle_market(chat_id):
send(“🔄 Đang lấy chỉ số…”, chat_id)
data = api(”/api/market”)
msg  = “🏦 <b>Chỉ số thị trường</b>\n\n”
for key, val in data.items():
if isinstance(val, dict):
p = val.get(‘price’, 0); chg = val.get(‘change_pct’, 0)
arr = ‘🔺’ if chg >= 0 else ‘🔻’
msg += f”{arr} <b>{val.get(‘name’, key)}</b>: {p:,.2f} ({chg:+.2f}%)\n”
if msg == “🏦 <b>Chỉ số thị trường</b>\n\n”:
msg += “❌ Không lấy được dữ liệu.”
send(msg, chat_id)

def poll_updates():
if not TOKEN:
logger.error(“Không có TOKEN”); return
logger.info(“🤖 Bot v3 polling…”)
offset = 0
while True:
try:
resp = requests.get(f”https://api.telegram.org/bot{TOKEN}/getUpdates”,
params={‘offset’: offset, ‘timeout’: 30}, timeout=35)
for upd in resp.json().get(‘result’, []):
offset  = upd[‘update_id’] + 1
msg     = upd.get(‘message’, {})
if not msg: continue
cid  = str(msg.get(‘chat’, {}).get(‘id’, ‘’))
text = msg.get(‘text’, ‘’).strip()
if not text: continue
logger.info(f”CMD: {text}”)
parts = text.split()
cmd   = parts[0].lower().split(’@’)[0]
if cmd in (’/start’, ‘/help’):
handle_start(cid)
elif cmd == ‘/price’:
handle_price(parts[1].upper() if len(parts) > 1 else ‘VCB’, cid)
elif cmd == ‘/analyze’:
handle_analyze(parts[1].upper() if len(parts) > 1 else ‘VCB’, cid)
elif cmd == ‘/whatif’:
if len(parts) < 3:
send(“❓ Cú pháp: <b>/whatif MÃ GIÁ</b>\nVí dụ: <b>/whatif VCB 59000</b>”, cid)
else:
try:
t = float(parts[2].replace(’,’,’’).replace(’.’,’’))
if t < 100: t *= 1000
handle_whatif(parts[1].upper(), t, cid)
except ValueError:
send(“❓ Giá không hợp lệ. VD: <b>/whatif VCB 59000</b>”, cid)
elif cmd == ‘/signals’:
handle_signals(cid)
elif cmd == ‘/market’:
handle_market(cid)
else:
send(“❓ Lệnh không nhận ra. Gõ /help xem danh sách.”, cid)
except requests.exceptions.Timeout:
pass
except Exception as e:
logger.error(f”Polling: {e}”); time.sleep(5)

def morning_scheduler():
if not CHAT_ID: return
while True:
now = datetime.now()
if now.weekday() < 5 and now.hour == 8 and now.minute == 45:
send(“🌅 <b>Báo cáo buổi sáng</b> — Đang quét…”, CHAT_ID)
handle_signals(CHAT_ID)
time.sleep(70)
time.sleep(30)

def main():
threading.Thread(target=morning_scheduler, daemon=True).start()
poll_updates()

if **name** == ‘**main**’:
main()
