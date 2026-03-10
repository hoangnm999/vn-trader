"""Main entry point - khởi động Flask API + Telegram Bot cùng lúc."""
from app import app
import telegram_bot  # import để kích hoạt start_bot()

def create_app():
    return app

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
