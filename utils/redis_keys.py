"""
Canonical Redis key builders for autotrader session + control keys.

IMPORTANT: Changing string formats here changes production Redis layout.
Run scripts/verify_redis_keys.py after any edit.
"""
from typing import Union

SESSION_PREFIX = "autotrader:session:"
SESSION_META_SUFFIX = ":meta"
SESSION_ORDER_IDS_SUFFIX = ":order_ids"
SESSION_SYMBOLS_TRADED_SUFFIX = ":symbols_traded"

SIMULATION_STOP_PREFIX = "autotrader:simulation_stop:"
PYRAMID_RESULT_PREFIX = "autotrader:pyramid_result:"
STOP_JOB_PREFIX = "autotrader:stop_job:"
EXIT_REQUEST_PREFIX = "autotrader:exit_request:"
EXIT_RESULT_PREFIX = "autotrader:exit_result:"
EXIT_STATUS_PREFIX = "autotrader:exit_status:"
RMS_EXITED_PREFIX = "autotrader:rms_exited:"
LIVE_PNL_PREFIX = "live_pnl:"

SESSION_REDIS_TTL = 86400


def session_pid_key(session_id: str) -> str:
    return f"{SESSION_PREFIX}{session_id}"


def session_meta_key(session_id: str) -> str:
    return f"{SESSION_PREFIX}{session_id}{SESSION_META_SUFFIX}"


def session_order_ids_key(session_id: str) -> str:
    return f"{SESSION_PREFIX}{session_id}{SESSION_ORDER_IDS_SUFFIX}"


def session_symbols_traded_key(session_id: str) -> str:
    return f"{SESSION_PREFIX}{session_id}{SESSION_SYMBOLS_TRADED_SUFFIX}"


def simulation_stop_key(session_id: str) -> str:
    return f"{SIMULATION_STOP_PREFIX}{session_id}"


def pyramid_result_key(session_id: str) -> str:
    return f"{PYRAMID_RESULT_PREFIX}{session_id}"


def stop_job_key(session_id: str) -> str:
    return f"{STOP_JOB_PREFIX}{session_id}"


def exit_request_key(session_id: str) -> str:
    return f"{EXIT_REQUEST_PREFIX}{session_id}"


def exit_result_key(session_id: str, symbol: str) -> str:
    sym = (symbol or "").upper().replace("-EQ", "").strip()
    return f"{EXIT_RESULT_PREFIX}{session_id}:{sym}"


def exit_status_key(session_id: str) -> str:
    return f"{EXIT_STATUS_PREFIX}{session_id}"


def rms_exited_key(session_id: str) -> str:
    return f"{RMS_EXITED_PREFIX}{session_id}"


def live_pnl_key(session_id: str) -> str:
    return f"{LIVE_PNL_PREFIX}{session_id}"


def session_cleanup_keys(session_id: str) -> tuple:
    """Keys removed when a session worker is torn down (paper stop / terminate)."""
    return (
        session_pid_key(session_id),
        session_meta_key(session_id),
        session_order_ids_key(session_id),
        session_symbols_traded_key(session_id),
    )
