import os
import threading
import logging
from app import app
from telegram_bot import main as bot_main
logging.basicConfig(level=logging.INFO)
if __name__ == "__main__":
 t = threading.Thread(target=bot_main, daemon=True)
 t.start()
 port = int(os.environ.get("PORT", 8080))
 app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
