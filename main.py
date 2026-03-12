import os
import threading
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from app import app
from telegram_bot import main as bot_main

def create_app():
    """Entry point cho gunicorn — cũng khởi động bot trong background"""
    # Chạy bot trong thread daemon (tự tắt khi server tắt)
    bot_thread = threading.Thread(target=bot_main, daemon=True, name="TelegramBot")
    bot_thread.start()
    logger.info("✅ Telegram bot thread started")
    return app

# Cho phép gunicorn gọi: gunicorn "main:create_app()"
application = create_app()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

