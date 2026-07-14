from SmartApi import SmartConnect
from datetime import datetime, timezone, timedelta
import pyotp
import time
import os 
import csv
from pymongo import MongoClient
from datetime import datetime

# CONFIG
API_KEY = "ijiEoo6t"
CLIENT_CODE = "AACA085039"
PASSWORD = "1211"
TOTP_SECRET = "6AU4DOG3HZWSRYJAOZJDBV7674"

MARKET_TZ = timezone(timedelta(hours=5, minutes=30))

DB_NAME = "autotrader"
COLLECTION_NAME = "order_history"

MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://ankitarrow:ankitarrow@cluster0.zcajdur.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
).strip();


mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
orders_col = db[COLLECTION_NAME]


def get_smartapi_client():
    smartApi = SmartConnect(api_key=API_KEY)

    totp = pyotp.TOTP(TOTP_SECRET).now()
    session = smartApi.generateSession(
        clientCode=CLIENT_CODE,
        password=PASSWORD,
        totp=totp
    )

    if not session or not session.get("status"):
        raise RuntimeError("Angel One login failed")

    return smartApi

# # ====== FETCH TODAY'S INTRADAY ORDERS ======
def fetch_todays_intraday_orders():
    # ====== LOGIN ======
    smartApi = get_smartapi_client()
    resp = smartApi.orderBook()

    if not resp or not resp.get("status"):
        print("[WARN] orderBook failed:", resp)
        return []

    orders = []
    today = datetime.now(MARKET_TZ).date()

    for o in resp.get("data", []):
        try:
            # Intraday check
            if o.get("producttype", "").upper() != "INTRADAY":
                continue

            # Parse date safely (Angel format)
            ut = o.get("updatetime") or o.get("ordertime")
            if not ut:
                continue

            order_date = datetime.strptime(ut, "%d-%b-%Y %H:%M:%S").date()
            if order_date != today:
                continue

            orders.append({
                "order_id": o.get("orderid"),
                "symbol": o.get("tradingsymbol"),
                "side": o.get("transactiontype"),
                "qty": int(o.get("quantity", 0)),
                "filled_qty": int(o.get("filledshares", 0)),
                "avg_price": float(o.get("averageprice") or 0),
                "status": o.get("status"),
                "order_time": o.get("ordertime"),
                "update_time": o.get("updatetime"),
                "exchange_order_id": o.get("exchangeorderid"),
                "rejection_reason": o.get("text"),
            })

        except Exception as e:
            print("[PARSE ERROR]", e, o)

    return orders


def save_orders_to_csv(orders):
    if not orders:
        print("[WARN] No orders to save")
        return None

    today_str = datetime.now(MARKET_TZ).strftime("%Y-%m-%d")
    filename = f"angel_orders_{today_str}.csv"

    filepath = os.path.join(os.getcwd(), filename)

    headers = [
        "order_id",
        "symbol",
        "side",
        "qty",
        "filled_qty",
        "avg_price",
        "status",
        "product",
        "order_time",
        "update_time",
        "exchange_order_id",
        "rejection_reason",
    ]

    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()

        for o in orders:
            writer.writerow({
                "order_id": o.get("order_id"),
                "symbol": o.get("symbol"),
                "side": o.get("side"),
                "qty": o.get("qty"),
                "filled_qty": o.get("filled_qty"),
                "avg_price": o.get("avg_price"),
                "status": o.get("status"),
                "product": o.get("product"),
                "order_time": o.get("order_time"),
                "update_time": o.get("update_time"),
                "exchange_order_id": o.get("exchange_order_id"),
                "rejection_reason": o.get("rejection_reason"),
            })

    print(f"[OK] CSV saved successfully Ã¢â€ â€™ {filepath}")
    return filepath


# ====== MAIN ======
if __name__ == "__main__":
    orders = fetch_todays_intraday_orders()
    print(f"[INFO] Found {len(orders)} intraday orders today")
    
    csv_path = save_orders_to_csv(orders)

    if csv_path:
        print("[DOWNLOAD READY]", csv_path)
    for o in orders:
        print(o)