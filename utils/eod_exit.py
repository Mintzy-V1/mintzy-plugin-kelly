"""EOD idempotency helpers and session-scoped exit preparation."""
import os
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple

from utils.session_ledger import (
    build_eod_exit_plan,
    ledger_has_open_positions,
)


def log_eod_config() -> None:
    print(
        "[EOD-CONFIG] "
        f"EXIT_ONLY_SESSION_SYMBOLS={os.environ.get('EXIT_ONLY_SESSION_SYMBOLS', 'true')} "
        f"EOD_USE_SESSION_LEDGER={os.environ.get('EOD_USE_SESSION_LEDGER', 'true')} "
        f"EOD_STRICT_REDIS_META={os.environ.get('EOD_STRICT_REDIS_META', 'false')}"
    )


def _ensure_eod_state(trader) -> None:
    if not hasattr(trader, "_eod_exit_done"):
        trader._eod_exit_done = False
    if not hasattr(trader, "_eod_exit_in_progress"):
        trader._eod_exit_in_progress = False
    if not hasattr(trader, "_eod_exit_lock"):
        trader._eod_exit_lock = threading.Lock()


def try_begin_eod_exit(trader) -> str:
    """
    Acquire EOD execution slot.

    Returns: 'proceed' | 'done' | 'busy'
    """
    _ensure_eod_state(trader)
    with trader._eod_exit_lock:
        if trader._eod_exit_done:
            print("[EOD] Square-off already completed — skipping duplicate exit")
            return "done"
        if trader._eod_exit_in_progress:
            print("[EOD] Square-off already in progress — will retry later")
            return "busy"
        trader._eod_exit_in_progress = True
        return "proceed"


def mark_eod_exit_done(trader) -> None:
    _ensure_eod_state(trader)
    with trader._eod_exit_lock:
        trader._eod_exit_done = True
        trader._eod_exit_in_progress = False


def release_eod_exit_in_progress(trader) -> None:
    _ensure_eod_state(trader)
    with trader._eod_exit_lock:
        trader._eod_exit_in_progress = False


def finalize_eod_shutdown(
    trader,
    *,
    clear_positions: bool = True,
    sync_broker: bool = False,
) -> None:
    """Clear internal books and optionally sync cash with broker after EOD."""
    if clear_positions:
        positions = getattr(trader, "positions", None)
        if positions is not None:
            try:
                positions.clear()
            except Exception as e:
                print(f"[EOD] positions clear failed: {e}")

        lock = getattr(trader, "_session_open_qty_lock", None)
        ledger = getattr(trader, "_session_open_qty", None)
        if lock is not None and ledger is not None:
            try:
                with lock:
                    ledger.clear()
            except Exception as e:
                print(f"[EOD] session ledger clear failed: {e}")

    if sync_broker and hasattr(trader, "_sync_cash_with_broker"):
        try:
            trader._sync_cash_with_broker()
        except Exception as e:
            print(f"[EOD] broker sync failed: {e}")


def prepare_eod_exit_plan(
    trader,
    broker_positions: List[Dict[str, Any]],
    fallback_symbols: Optional[List[str]] = None,
    on_skip_summary: Optional[Callable[[int, List[str], str], None]] = None,
) -> Tuple[List[Dict[str, Any]], bool, str]:
    """
    Returns (exit_plan, should_mark_done_without_orders, status_message).

    should_mark_done_without_orders is False when ledger is open but plan is empty
    (caller must NOT mark EOD done — retry later).
    """
    exit_plan = build_eod_exit_plan(
        trader,
        broker_positions,
        fallback_symbols=fallback_symbols,
        on_skip_summary=on_skip_summary,
    )

    if exit_plan:
        return exit_plan, False, f"{len(exit_plan)} session position(s) to exit"

    if ledger_has_open_positions(trader):
        msg = (
            "EOD CRITICAL: session ledger has open qty but exit plan is empty "
            "(broker flat / allow-list mismatch) — will retry"
        )
        print(f"[EOD] {msg}")
        return [], False, msg

    return [], True, "No session positions to exit"


def exit_plan_to_broker_positions(exit_plan: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert exit plan rows to broker-position-shaped dicts for existing EOD loops."""
    rows: List[Dict[str, Any]] = []
    for row in exit_plan or []:
        rows.append({
            "symbol": row["symbol"],
            "side": row["side"],
            "qty": row["qty"],
            "ltp": row.get("ltp", 0.0),
            "ledger_qty": row.get("ledger_qty"),
            "broker_qty": row.get("broker_qty"),
        })
    return rows
