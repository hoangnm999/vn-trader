import os
import threading
import logging
from app import app
from telegram_bot import main as bot_main

# Cấu hình logging để theo dõi tiến trình
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    # Khởi chạy Telegram Bot trong một luồng (thread) riêng
    t = threading.Thread(target=bot_main, daemon=True)
    t.start()
    
    # Khởi chạy Flask App trên luồng chính
    # Mặc định dùng port 8080 nếu không có biến môi trường PORT
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
