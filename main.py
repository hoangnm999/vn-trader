import os
import threading
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(**name**)

def warmup_cache():
time.sleep(10)  # chờ Flask sẵn sàng
try:
import requests
r = requests.get(‘http://localhost:8080/api/warmup’, timeout=5)
logger.info(f”Cache warmup: {r.json()}”)
except Exception as e:
logger.warning(f”Warmup failed: {e}”)

def start_bot():
try:
from telegram_bot import main as bot_main
logger.info(“🤖 Starting Telegram bot…”)
bot_main()
except Exception as e:
logger.error(f”Bot crashed: {e}”)

if **name** == ‘**main**’:
# Thread 1: Telegram bot
threading.Thread(target=start_bot, daemon=True).start()
logger.info(“✅ Telegram bot thread started”)

```
# Thread 2: Warmup cache sau 10 giây
threading.Thread(target=warmup_cache, daemon=True).start()

# Flask server
from app import app
port = int(os.environ.get('PORT', 8080))
logger.info(f"🌐 Starting Flask on port {port}")
app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
```
