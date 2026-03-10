import os
import threading
from app import app

def run_bot():
    try:
        from telegram_bot import main as bot_main
        bot_main()
    except Exception as e:
        print(f"Bot error: {e}")

if __name__ == "__main__":
    # Chạy bot trong thread riêng
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()
    
    # Chạy Flask
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
