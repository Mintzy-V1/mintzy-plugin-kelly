import csv
from pathlib import Path
from threading import Lock

# Thread-safe CSV writes
_csv_lock = Lock()

CSV_HEADERS = [
    "timestamp",
    "session_id",
    "cycle",
    "symbol",
    "curr_price",
    "next_pred",
    "return_pct",
    "side",
    "signal",
    "action",
    "unrealized_pnl",
    "cash_balance",
    "realized_pnl",
    "total_equity",
]

def append_snapshot_rows(
    logs_dir: Path,
    session_id: str,
    cycle: int,
    snapshot: dict,
    rows: list[dict],
):
    logs_dir.mkdir(exist_ok=True)
    csv_path = logs_dir / "live_data.csv"

    with _csv_lock:
        file_exists = csv_path.exists()

        with csv_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)

            if not file_exists:
                writer.writeheader()

            for r in rows:
                writer.writerow({
                    "timestamp": snapshot["timestamp"],
                    "session_id": session_id,
                    "cycle": cycle,
                    "symbol": r.get("symbol"),
                    "curr_price": r.get("curr_price"),
                    "next_pred": r.get("next_pred"),
                    "return_pct": r.get("return_pct"),
                    "side": r.get("side"),
                    "signal": r.get("signal"),
                    "action": r.get("action"),
                    "unrealized_pnl": r.get("unrealized_pnl"),
                    "cash_balance": snapshot["cash_balance"],
                    "realized_pnl": snapshot["realized_pnl"],
                    "total_equity": snapshot["total_equity"],
                })