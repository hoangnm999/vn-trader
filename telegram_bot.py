"""
VN Trader Telegram Bot
Báo tín hiệu MUA/BÁN tự động qua Telegram
Chạy song song với Flask server
"""

import os
import time
import threading
import requests
from datetime import datetime

# ── Cấu hình ─────────────────────────────────────────────────────────────────
BOT_TOKEN    = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID      = os.environ.get("TELEGRAM_CHAT_ID",  "")
API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:5000")

SCAN_INTERVAL_MINUTES = 15  # Quét tín hiệu mỗi 15 phút
MARKET_OPEN_HOUR   = 9
MARKET_CLOSE_HOUR  = 15

# ── Helpers ───────────────────────────────────────────────────────────────────
def send_message(text: str, parse_mode: str = "HTML"):
    if not BOT_TOKEN or not CHAT_ID:
        print(f"[BOT] {text}")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id":    CHAT_ID,
            "text":       text,
            "parse_mode": parse_mode,
        }, timeout=10)
    except Exception as e:
        print(f"[BOT ERROR] {e}")


def format_price(p: float) -> str:
    return f"{p:,.0f}".replace(",", ".")


def is_market_open() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:   # Thứ 7, CN
        return False
    return MARKET_OPEN_HOUR <= now.hour < MARKET_CLOSE_HOUR


# ── Signal Scanner ────────────────────────────────────────────────────────────
alerted_signals = {}  # Tránh báo trùng cùng tín hiệu trong ngày

def scan_and_alert():
    """Quét tín hiệu và gửi Telegram nếu có MUA/BÁN mạnh."""
    try:
        resp = requests.get(f"{API_BASE_URL}/api/signals", timeout=30)
        if resp.status_code != 200:
            return
        signals = resp.json()
    except Exception as e:
        print(f"[SCAN ERROR] {e}")
        return

    today = datetime.now().strftime("%Y-%m-%d")

    for sig in signals:
        sym     = sig["symbol"]
        signal  = sig["signal"]
        score   = sig["score"]
        price   = sig["price"]
        chg_pct = sig["change_pct"]
        rsi     = sig["rsi"]

        key = f"{today}_{sym}_{signal}"
        if key in alerted_signals:
            continue   # Đã báo rồi

        if score < 65:  # Chỉ báo tín hiệu có độ tin cậy >= 65
            continue

        alerted_signals[key] = True

        arrow  = "🟢" if signal == "MUA" else "🔴"
        chg_str = f"+{chg_pct:.1f}%" if chg_pct >= 0 else f"{chg_pct:.1f}%"

        # Lấy thông tin phân tích đầy đủ
        try:
            detail = requests.get(f"{API_BASE_URL}/api/analyze/{sym}", timeout=20).json()
            ind    = detail.get("indicators", {})
            sl     = ind.get("sl", 0)
            tp     = ind.get("tp", 0)
            rr     = ind.get("rr_ratio", 0)
            sl_pct = ind.get("sl_pct", 5)
            tp_pct = ind.get("tp_pct", 10)
        except:
            sl, tp, rr, sl_pct, tp_pct = 0, 0, 0, 5, 10

        msg = f"""
{arrow} <b>TÍN HIỆU {signal}: {sym}</b>
━━━━━━━━━━━━━━━━━━━━
💰 Giá hiện tại: <b>{format_price(price)} đ</b> ({chg_str})
📊 Điểm: {score}/100 · RSI: {rsi}
━━━━━━━━━━━━━━━━━━━━
🎯 Vào lệnh: {format_price(price)} đ
✅ Mục tiêu:  {format_price(tp)} đ (+{tp_pct}%)
❌ Dừng lỗ:  {format_price(sl)} đ (-{sl_pct}%)
⚖️  Tỷ lệ R:R: 1:{rr}
━━━━━━━━━━━━━━━━━━━━
🕐 {datetime.now().strftime("%H:%M:%S %d/%m/%Y")}
⚠️ <i>Không phải khuyến nghị đầu tư</i>
"""
        send_message(msg.strip())
        time.sleep(1)  # Tránh spam


# ── Daily Report ──────────────────────────────────────────────────────────────
def send_daily_report():
    """Gửi báo cáo tổng hợp lúc 8:45 sáng."""
    try:
        resp = requests.get(f"{API_BASE_URL}/api/signals", timeout=30)
        signals = resp.json() if resp.status_code == 200 else []
    except:
        signals = []

    buys  = [s for s in signals if s["signal"] == "MUA"][:5]
    sells = [s for s in signals if s["signal"] == "BÁN"][:3]

    msg_lines = [
        "📋 <b>BÁO CÁO BUỔI SÁNG</b>",
        f"🗓 {datetime.now().strftime('%d/%m/%Y')} · Phiên HOSE 09:00–15:00",
        "━━━━━━━━━━━━━━━━━━━━",
    ]

    if buys:
        msg_lines.append("🟢 <b>Cổ phiếu đáng chú ý (MUA):</b>")
        for s in buys:
            chg = f"+{s['change_pct']:.1f}%" if s['change_pct'] >= 0 else f"{s['change_pct']:.1f}%"
            msg_lines.append(f"  • <b>{s['symbol']}</b> | {format_price(s['price'])}đ | RSI {s['rsi']} | Score {s['score']}")

    if sells:
        msg_lines.append("🔴 <b>Cổ phiếu cảnh báo (BÁN):</b>")
        for s in sells:
            msg_lines.append(f"  • <b>{s['symbol']}</b> | {format_price(s['price'])}đ | RSI {s['rsi']}")

    msg_lines += [
        "━━━━━━━━━━━━━━━━━━━━",
        "⚠️ <i>Phân tích kỹ thuật, không phải tư vấn đầu tư</i>",
        "📱 Dùng /analyze [MÃ] để xem chi tiết",
    ]

    send_message("\n".join(msg_lines))


# ── Telegram Command Handler ──────────────────────────────────────────────────
def handle_commands():
    """Xử lý lệnh người dùng gửi vào bot."""
    if not BOT_TOKEN:
        return
    offset = None
    while True:
        try:
            url    = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
            params = {"timeout": 30, "offset": offset}
            resp   = requests.get(url, params=params, timeout=35).json()

            for update in resp.get("result", []):
                offset = update["update_id"] + 1
                msg    = update.get("message", {})
                text   = msg.get("text", "").strip()
                cid    = msg.get("chat", {}).get("id")
                if not text or not cid: continue

                parts  = text.split()
                cmd    = parts[0].lower()

                if cmd in ("/start", "/help"):
                    send_message("""
🤖 <b>VN Trader Bot</b>
━━━━━━━━━━━━━━━━━━━━
Các lệnh có thể dùng:

/analyze VCB — Phân tích cổ phiếu
/price VCB — Giá hiện tại
/signals — Tín hiệu nổi bật
/market — Chỉ số thị trường
/help — Trợ giúp
━━━━━━━━━━━━━━━━━━━━
⚠️ <i>Không phải tư vấn đầu tư</i>
""".strip())

                elif cmd == "/price" and len(parts) >= 2:
                    sym  = parts[1].upper()
                    data = requests.get(f"{API_BASE_URL}/api/price/{sym}", timeout=15).json()
                    p    = data.get("price", 0)
                    chg  = data.get("change_pct", 0)
                    src  = data.get("source", "")
                    chg_str = f"+{chg:.2f}%" if chg >= 0 else f"{chg:.2f}%"
                    send_message(f"💰 <b>{sym}</b>: {format_price(p)} đ ({chg_str})\n📡 Nguồn: {src}")

                elif cmd == "/analyze" and len(parts) >= 2:
                    sym  = parts[1].upper()
                    data = requests.get(f"{API_BASE_URL}/api/analyze/{sym}", timeout=20).json()
                    if "error" in data:
                        send_message(f"❌ Lỗi: {data['error']}")
                        continue
                    ind  = data.get("indicators", {})
                    p    = data.get("price", 0)
                    chg  = data.get("change_pct", 0)
                    chg_str = f"+{chg:.2f}%" if chg >= 0 else f"{chg:.2f}%"
                    sig  = ind.get("signal", "THEO DÕI")
                    icon = "🟢" if sig == "MUA" else ("🔴" if sig == "BÁN" else "🟡")
                    send_message(f"""
{icon} <b>Phân tích: {sym}</b>
━━━━━━━━━━━━━━━━━━━━
💰 Giá: <b>{format_price(p)} đ</b> ({chg_str})
📊 RSI(14): {ind.get('rsi','--')} | MACD: {ind.get('macd','--')}
📐 EMA20: {format_price(ind.get('ema20',0))} | SMA50: {format_price(ind.get('sma50',0))}
━━━━━━━━━━━━━━━━━━━━
🎯 Tín hiệu: <b>{sig}</b> (Score: {ind.get('score','--')}/100)
✅ TP: {format_price(ind.get('tp',0))} đ (+{ind.get('tp_pct',0)}%)
❌ SL: {format_price(ind.get('sl',0))} đ (-{ind.get('sl_pct',0)}%)
⚖️  R:R = 1:{ind.get('rr_ratio',0)}
""".strip())

                elif cmd == "/signals":
                    data = requests.get(f"{API_BASE_URL}/api/signals", timeout=20).json()
                    lines = ["⚡ <b>Tín hiệu nổi bật:</b>"]
                    for s in data[:6]:
                        icon = "🟢" if s["signal"] == "MUA" else "🔴"
                        lines.append(f"{icon} <b>{s['symbol']}</b> | {format_price(s['price'])}đ | Score {s['score']}")
                    send_message("\n".join(lines))

                elif cmd == "/market":
                    data = requests.get(f"{API_BASE_URL}/api/market", timeout=20).json()
                    lines = ["📈 <b>Chỉ số thị trường:</b>"]
                    for sym, d in data.items():
                        chg  = d.get("change_pct", 0)
                        icon = "▲" if chg > 0 else ("▼" if chg < 0 else "→")
                        chg_str = f"{'+' if chg >= 0 else ''}{chg:.2f}%"
                        lines.append(f"{icon} <b>{sym}</b>: {format_price(d.get('price',0))} ({chg_str})")
                    send_message("\n".join(lines))

        except Exception as e:
            print(f"[CMD ERROR] {e}")
            time.sleep(5)


# ── Background Scheduler ──────────────────────────────────────────────────────
def scheduler():
    """Chạy nền: quét tín hiệu + gửi báo cáo buổi sáng."""
    last_daily_report = None

    while True:
        now = datetime.now()

        # Báo cáo buổi sáng lúc 8:45
        if (now.hour == 8 and now.minute >= 45 and
                last_daily_report != now.strftime("%Y-%m-%d") and
                now.weekday() < 5):
            send_daily_report()
            last_daily_report = now.strftime("%Y-%m-%d")

        # Quét tín hiệu trong giờ giao dịch
        if is_market_open():
            scan_and_alert()

        time.sleep(SCAN_INTERVAL_MINUTES * 60)


def start_bot():
    """Khởi động bot trong thread riêng."""
    if not BOT_TOKEN:
        print("[BOT] TELEGRAM_BOT_TOKEN chưa được cấu hình, bỏ qua bot.")
        return
    print("[BOT] Khởi động Telegram Bot...")
    send_message("🚀 <b>VN Trader Bot đã khởi động!</b>\nGõ /help để xem hướng dẫn.")
    t1 = threading.Thread(target=handle_commands, daemon=True)
    t2 = threading.Thread(target=scheduler,        daemon=True)
    t1.start()
    t2.start()


# Khởi động bot khi import từ app.py
start_bot()
