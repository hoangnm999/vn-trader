Content is user-generated and unverified.
6
import os
import logging
import time
import threading
import requests
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TOKEN   = os.environ.get('TELEGRAM_BOT_TOKEN', '')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')
API_URL = os.environ.get('API_BASE_URL', 'http://localhost:5000')

def send_message(text: str, chat_id: str = None):
    if not TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN chưa được set")
        return False
    cid = chat_id or CHAT_ID
    if not cid:
        logger.warning("TELEGRAM_CHAT_ID chưa được set")
        return False
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        resp = requests.post(url, json={
            'chat_id': cid,
            'text': text,
            'parse_mode': 'HTML'
        }, timeout=10)
        return resp.status_code == 200
    except Exception as e:
        logger.error(f"Send message error: {e}")
        return False

def get_api(endpoint: str) -> dict:
    try:
        r = requests.get(f"{API_URL}{endpoint}", timeout=15)
        return r.json()
    except Exception as e:
        logger.error(f"API error {endpoint}: {e}")
        return {}

def handle_start(chat_id: str):
    msg = (
        "📈 <b>VN Trader Bot</b> — Chào mừng!\n\n"
        "🤖 Bot phân tích kỹ thuật cổ phiếu HOSE/HNX\n\n"
        "<b>Lệnh có thể dùng:</b>\n"
        "/price VCB — Giá cổ phiếu VCB\n"
        "/analyze FPT — Phân tích kỹ thuật FPT\n"
        "/signals — Top tín hiệu MUA/BÁN\n"
        "/market — Chỉ số thị trường\n"
        "/help — Xem lại hướng dẫn\n\n"
        "⚠️ <i>Bot chỉ phân tích kỹ thuật, không phải tư vấn đầu tư</i>"
    )
    send_message(msg, chat_id)

def handle_price(symbol: str, chat_id: str):
    send_message(f"🔄 Đang lấy giá {symbol}...", chat_id)
    data = get_api(f"/api/price/{symbol}")
    if data.get('price', 0) > 0:
        p = data['price']
        chg = data.get('change_pct', 0)
        arrow = '🔺' if chg >= 0 else '🔻'
        msg = (
            f"💹 <b>{symbol}</b>\n"
            f"Giá: <b>{p:,.0f} đ</b>\n"
            f"Thay đổi: {arrow} {chg:+.2f}%\n"
            f"Nguồn: {data.get('source','TCBS')}"
        )
    else:
        err = data.get('error', 'Không lấy được giá')
        msg = f"❌ <b>{symbol}</b>: {err}\n<i>Thị trường có thể đang đóng cửa</i>"
    send_message(msg, chat_id)

def handle_analyze(symbol: str, chat_id: str):
    send_message(f"🔬 Đang phân tích {symbol}...", chat_id)
    data = get_api(f"/api/analyze/{symbol}")
    if 'error' in data:
        send_message(f"❌ {symbol}: {data['error']}", chat_id)
        return

    score  = data.get('score', 50)
    action = data.get('action', 'THEO DÕI')
    action_emoji = '🟢' if action == 'MUA' else ('🔴' if action == 'BÁN' else '🟡')

    signals_text = '\n'.join([f"  • {s}" for s in data.get('signals', [])]) or '  • Không có tín hiệu rõ'

    msg = (
        f"📊 <b>Phân tích {symbol}</b>\n\n"
        f"💰 Giá: <b>{data.get('price',0):,.0f} đ</b>\n\n"
        f"📈 Chỉ báo:\n"
        f"  RSI(14): {data.get('rsi',0)}\n"
        f"  MACD: {data.get('macd',0):+.1f}\n"
        f"  EMA20: {data.get('ema20',0):,.0f}\n"
        f"  BB: {data.get('bb_lower',0):,.0f} – {data.get('bb_upper',0):,.0f}\n\n"
        f"🎯 Tín hiệu:\n{signals_text}\n\n"
        f"⚡ Điểm tổng hợp: <b>{score}/100</b>\n"
        f"{action_emoji} Khuyến nghị: <b>{action}</b>\n\n"
        f"📌 Tham chiếu:\n"
        f"  Vào lệnh: {data.get('entry',0):,.0f}\n"
        f"  Stop Loss: {data.get('stop_loss',0):,.0f} (-5%)\n"
        f"  Take Profit: {data.get('take_profit',0):,.0f} (+10%)\n\n"
        f"⚠️ <i>Chỉ mang tính tham khảo, không phải tư vấn đầu tư</i>"
    )
    send_message(msg, chat_id)

def handle_signals(chat_id: str):
    send_message("🔄 Đang quét tín hiệu...", chat_id)
    data = get_api("/api/signals")
    if not data:
        send_message("❌ Không lấy được tín hiệu. Thử lại sau.", chat_id)
        return

    msg = "🔔 <b>Top Tín Hiệu Hôm Nay</b>\n\n"
    for item in data:
        action = item.get('action', '')
        emoji = '🟢' if action == 'MUA' else ('🔴' if action == 'BÁN' else '🟡')
        msg += (
            f"{emoji} <b>{item.get('symbol','')}</b> — {action}\n"
            f"   Giá: {item.get('price',0):,.0f}đ | Score: {item.get('score',0)}/100\n"
            f"   SL: {item.get('stop_loss',0):,.0f} | TP: {item.get('take_profit',0):,.0f}\n\n"
        )
    msg += "⚠️ <i>Không phải tư vấn đầu tư</i>"
    send_message(msg, chat_id)

def handle_market(chat_id: str):
    send_message("🔄 Đang lấy chỉ số...", chat_id)
    data = get_api("/api/market")
    msg = "🏦 <b>Chỉ số thị trường</b>\n\n"
    for key, val in data.items():
        if isinstance(val, dict):
            p   = val.get('price', 0)
            chg = val.get('change_pct', 0)
            arr = '🔺' if chg >= 0 else '🔻'
            name = val.get('name', key)
            msg += f"{arr} <b>{name}</b>: {p:,.2f} ({chg:+.2f}%)\n"
    if msg == "🏦 <b>Chỉ số thị trường</b>\n\n":
        msg += "❌ Không lấy được dữ liệu. Thị trường có thể đang đóng cửa."
    send_message(msg, chat_id)

# ── Polling loop ──────────────────────────────────────────────────────────────
def poll_updates():
    if not TOKEN:
        logger.error("Không có TELEGRAM_BOT_TOKEN — bot không thể chạy")
        return

    logger.info("🤖 Telegram bot bắt đầu polling...")
    offset = 0

    while True:
        try:
            url  = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
            resp = requests.get(url, params={'offset': offset, 'timeout': 30}, timeout=35)
            updates = resp.json().get('result', [])

            for upd in updates:
                offset = upd['update_id'] + 1
                msg = upd.get('message', {})
                if not msg:
                    continue

                chat_id = str(msg.get('chat', {}).get('id', ''))
                text    = msg.get('text', '').strip()
                if not text:
                    continue

                logger.info(f"Nhận lệnh từ {chat_id}: {text}")
                parts = text.split()
                cmd   = parts[0].lower().split('@')[0]  # bỏ @botname nếu có

                if cmd in ('/start', '/help'):
                    handle_start(chat_id)
                elif cmd == '/price':
                    sym = parts[1].upper() if len(parts) > 1 else 'VCB'
                    handle_price(sym, chat_id)
                elif cmd == '/analyze':
                    sym = parts[1].upper() if len(parts) > 1 else 'VCB'
                    handle_analyze(sym, chat_id)
                elif cmd == '/signals':
                    handle_signals(chat_id)
                elif cmd == '/market':
                    handle_market(chat_id)
                else:
                    send_message(
                        "❓ Lệnh không nhận ra.\nGõ /help để xem danh sách lệnh.",
                        chat_id
                    )

        except requests.exceptions.Timeout:
            pass  # timeout bình thường với long polling
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(5)

# ── Gửi báo cáo buổi sáng tự động ────────────────────────────────────────────
def morning_report_scheduler():
    if not CHAT_ID:
        return
    logger.info("📅 Morning report scheduler started")
    while True:
        now = datetime.now()
        # Gửi lúc 8:45 sáng các ngày trong tuần
        if now.weekday() < 5 and now.hour == 8 and now.minute == 45:
            logger.info("📨 Gửi báo cáo buổi sáng...")
            send_message("🌅 <b>Báo cáo buổi sáng</b> — Đang quét tín hiệu...", CHAT_ID)
            handle_signals(CHAT_ID)
            time.sleep(70)  # tránh gửi lại trong cùng phút
        time.sleep(30)

def main():
    # Chạy morning report trong thread riêng
    t1 = threading.Thread(target=morning_report_scheduler, daemon=True)
    t1.start()
    # Chạy polling (blocking)
    poll_updates()

if __name__ == '__main__':
    main()
