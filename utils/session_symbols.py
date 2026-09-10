"""Session-scoped symbol helpers for EOD exit (Redis meta + broker position filter)."""
import json
import os
from typing import Any, Callable, Dict, List, Optional, Set

REDIS_SESSION_PREFIX = "autotrader:session:"
REDIS_META_SUFFIX = ":meta"


def normalize_symbol(sym: str) -> str:
    return (sym or "").upper().replace("-EQ", "").strip()


def session_meta_redis_key(session_id: str) -> str:
    return f"{REDIS_SESSION_PREFIX}{session_id}{REDIS_META_SUFFIX}"


def exit_only_session_symbols_enabled() -> bool:
    return os.environ.get("EXIT_ONLY_SESSION_SYMBOLS", "true").lower() in (
        "1",
        "true",
        "yes",
    )


def get_session_symbols_from_redis(
    redis_client,
    session_id: Optional[str],
    fallback_symbols: Optional[List[str]] = None,
) -> Set[str]:
    fallback = {
        normalize_symbol(s) for s in (fallback_symbols or []) if s
    }

    if not session_id:
        print("[EOD] No session_id — cannot load session symbols from Redis")
        return fallback

    if redis_client is None:
        print("[EOD] No redis_client — using symbol_allocations fallback")
        return fallback

    try:
        raw = redis_client.get(session_meta_redis_key(session_id))
        if not raw:
            print(
                f"[EOD] Redis meta missing for {session_id} — "
                "using symbol_allocations fallback"
            )
            return fallback

        meta = json.loads(raw)
        symbols = meta.get("symbols") or []
        normalized = {normalize_symbol(s) for s in symbols if s}
        if normalized:
            print(
                f"[EOD] Session symbols from Redis ({session_id}): "
                f"{sorted(normalized)}"
            )
            return normalized

        print(
            f"[EOD] Redis meta has no symbols for {session_id} — "
            "using symbol_allocations fallback"
        )
        return fallback
    except Exception as e:
        print(f"[EOD] Redis meta read failed: {e} — using symbol_allocations fallback")
        return fallback


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
        print("[EOD] No session symbols resolved — exiting NOTHING (safe default)")
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
                except Exception:
                    pass

    print(
        f"[EOD] Broker positions={len(broker_positions or [])} "
        f"exit={len(to_exit)} skip={skipped}"
    )
    return to_exit
