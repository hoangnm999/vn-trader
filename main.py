import os import threading import logging import time
logging.basicConfig(level=logging.INFO) logger = logging.getLogger(__name__)
def warmup_cache():     time.sleep(10)     try:
        import requests         r = requests.get("http://localhost:8080/api/warmup", timeout=5)         logger.info("Cache warmup done")     except Exception as e:         logger.warning("Warmup failed")
def start_bot():     try:
        from telegram_bot import main as bot_main         bot_main()     except Exception as e:
        logger.error("Bot crashed")
if __name__ == "__main__":
    threading.Thread(target=start_bot, daemon=True).start()     threading.Thread(target=warmup_cache, daemon=True).start()     from app import app     port = int(os.environ.get("PORT", 8080))     app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
