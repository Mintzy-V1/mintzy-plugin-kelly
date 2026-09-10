"""Session-scoped symbol helpers for EOD exit (Redis meta + broker position filter)."""
import json
import os
from typing import Any, Callable, Dict, List, Optional, Set

from utils.redis_keys import (
    SESSION_REDIS_TTL,
    session_meta_key,
    session_symbols_traded_key,
)


def normalize_symbol(sym: str) -> str:
    return (sym or "").upper().replace("-EQ", "").strip()


def exit_only_session_symbols_enabled() -> bool:
    return os.environ.get("EXIT_ONLY_SESSION_SYMBOLS", "true").lower() in (
        "1",
        "true",
        "yes",
    )


def _merge_symbol_fallbacks(
    fallback_symbols: Optional[List[str]] = None,
    ledger_symbols: Optional[List[str]] = None,
) -> Set[str]:
    merged = {
        normalize_symbol(s) for s in (fallback_symbols or []) if s
    }
    merged.update(normalize_symbol(s) for s in (ledger_symbols or []) if s)
    return merged


def _symbols_traded_from_redis(redis_client, session_id: str) -> Set[str]:
    if redis_client is None:
        return set()
    try:
        members = redis_client.smembers(session_symbols_traded_key(session_id))
        if not members:
            return set()
        return {normalize_symbol(m) for m in members if m}
    except Exception as e:
        print(f"[EOD] symbols_traded read failed: {e}")
        return set()


def get_session_symbols_from_redis(
    redis_client,
    session_id: Optional[str],
    fallback_symbols: Optional[List[str]] = None,
    ledger_symbols: Optional[List[str]] = None,
) -> Set[str]:
    fallback = _merge_symbol_fallbacks(fallback_symbols, ledger_symbols)

    if not session_id:
        print("[EOD] No session_id — cannot load session symbols from Redis")
        return fallback

    if redis_client is None:
        print("[EOD] No redis_client — using fallback symbol sources")
        return fallback

    traded = _symbols_traded_from_redis(redis_client, session_id)
    merged_with_traded = set(fallback) | traded

    try:
        raw = redis_client.get(session_meta_key(session_id))
        if not raw:
            print(
                f"[EOD] Redis meta missing for {session_id} — "
                "using fallback + symbols_traded"
            )
            return merged_with_traded if merged_with_traded else fallback

        meta = json.loads(raw)
        symbols = meta.get("symbols") or []
        normalized = {normalize_symbol(s) for s in symbols if s}
        if normalized:
            result = normalized | traded
            print(
                f"[EOD] Session symbols from Redis ({session_id}): "
                f"{sorted(result)}"
            )
            return result

        print(
            f"[EOD] Redis meta has no symbols for {session_id} — "
            "using fallback + symbols_traded"
        )
        return merged_with_traded if merged_with_traded else fallback
    except Exception as e:
        print(f"[EOD] Redis meta read failed: {e} — using fallback symbol sources")
        return merged_with_traded if merged_with_traded else fallback


def filter_broker_positions_for_session(
    broker_positions: List[Dict[str, Any]],
    session_id: Optional[str],
    redis_client,
    fallback_symbols: Optional[List[str]] = None,
    on_skip: Optional[Callable[[str], None]] = None,
) -> List[Dict[str, Any]]:
    if not exit_only_session_symbols_enabled():
        print("[EOD] EXIT_ONLY_SESSION_SYMBOLS=false — exiting ALL broker positions (legacy)")
        return list(broker_positions or [])

    allowed = get_session_symbols_from_redis(
        redis_client,
        session_id,
        fallback_symbols=fallback_symbols,
    )
    if not allowed:
        print(
            "[EOD] No session symbols resolved — filter returns nothing "
            "(use build_eod_exit_plan for ledger-aware EOD)"
        )
        return []

    to_exit: List[Dict[str, Any]] = []
    skipped = 0
    for pos in broker_positions or []:
        sym = normalize_symbol(pos.get("symbol", ""))
        if sym in allowed:
            to_exit.append(pos)
        else:
            skipped += 1
            label = pos.get("symbol", sym)
            print(
                f"[EOD] SKIP {label} — not in session symbol list "
                "(manual/other app position)"
            )
            if on_skip:
                try:
                    on_skip(label)
                except Exception as e:
                    print(f"[EOD] on_skip callback failed: {e}")

    print(
        f"[EOD] Broker positions={len(broker_positions or [])} "
        f"exit={len(to_exit)} skip={skipped}"
    )
    return to_exit
