import os
import requests
from datetime import datetime

class AlertManager:
    def __init__(self, token=None, chat_id=None):
        self.token = token or os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")

    def notify(self, message):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] {message}")
        if self.token and self.chat_id:
            try:
                requests.post(
                    f"https://api.telegram.org/bot{self.token}/sendMessage",
                    json={"chat_id": self.chat_id, "text": message},
                    timeout=5
                )
            except:
                pass