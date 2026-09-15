"""Engine-attributed position ledger for session-scoped EOD exit."""
import os
from typing import Any, Callable, Dict, List, Optional, Set

from utils.redis_keys import SESSION_REDIS_TTL, session_order_ids_key
from utils.session_symbols import (
    exit_only_session_symbols_enabled,
    get_session_symbols_from_redis,
    normalize_symbol,
)

EXIT_ACTIONS = frozenset({
    "EXIT_LONG",
    "COVER_SHORT",
    "STOP_LOSS",
    "MARKET_CLOSE_EXIT",
    "FLIP_TO_LONG",
    "FLIP_TO_SHORT",
    "RMS_TICKER_EXIT",
    "PORTFOLIO_RMS_EXIT",
    "MANUAL_EXIT",
})


def eod_use_session_ledger_enabled() -> bool:
    return os.environ.get("EOD_USE_SESSION_LEDGER", "true").lower() in (
        "1",
        "true",
        "yes",
    )


def record_engine_order(
    redis_client,
    session_id: Optional[str],
    order_id: Optional[str],
    ttl: int = SESSION_REDIS_TTL,
) -> None:
    if not redis_client or not session_id or not order_id:
        return
    try:
        key = session_order_ids_key(session_id)
        redis_client.sadd(key, str(order_id))
        redis_client.expire(key, ttl)
    except Exception as e:
        print(f"[SESSION-LEDGER] order_id record failed: {e}")


def record_symbol_traded(
    redis_client,
    session_id: Optional[str],
    symbol: str,
    ttl: int = SESSION_REDIS_TTL,
) -> None:
    if not redis_client or not session_id:
        return
    sym = normalize_symbol(symbol)
    if not sym:
        return
    try:
        from utils.redis_keys import session_symbols_traded_key
        key = session_symbols_traded_key(session_id)
        redis_client.sadd(key, sym)
        redis_client.expire(key, ttl)
    except Exception as e:
        print(f"[SESSION-LEDGER] symbols_traded record failed: {e}")


def get_trader_session_id(trader) -> Optional[str]:
    return getattr(trader, "ui_session_id", None) or getattr(trader, "session_id", None)


def get_trader_redis(trader):
    market_client = getattr(trader, "market_client", None)
    return getattr(market_client, "redis_client", None) if market_client else None


def get_session_ledger(trader) -> Dict[str, int]:
    ledger = getattr(trader, "_session_open_qty", None)
    if ledger is None:
        return {}
    return dict(ledger)


def ledger_has_open_positions(trader) -> bool:
    return any(int(q or 0) != 0 for q in get_session_ledger(trader).values())


_BUY_FILL_ACTIONS = frozenset({
    "OPEN_LONG",
    "EXPAND_LONG",
    "FLIP_TO_LONG",
    "COVER_SHORT",
    "TREND_VETO_EXIT_SHORT",
})
_SELL_FILL_ACTIONS = frozenset({
    "OPEN_SHORT",
    "EXPAND_SHORT",
    "FLIP_TO_SHORT",
    "EXIT_LONG",
    "TREND_VETO_EXIT_LONG",
    "SINGLE_EXIT",
})


def _signed_fill_delta(fill: int, side: str, action_type: str) -> Optional[int]:
    """BUY adds, SELL subtracts. Action used only if side is missing."""
    order_side = (side or "").upper()
    if order_side in {"BUY", "B"}:
        return fill
    if order_side in {"SELL", "S"}:
        return -fill

    action = (action_type or "").upper()
    if action in _BUY_FILL_ACTIONS:
        return fill
    if action in _SELL_FILL_ACTIONS:
        return -fill
    return None


def _compute_ledger_qty_after_fill(
    current: int,
    fill_qty: int,
    action_type: str,
    side: str = "",
) -> Optional[int]:
    """
    Signed session ledger: positive = long, negative = short, zero = flat.

    One rule: BUY += fill, SELL -= fill. Flip is just a 2x fill of that side.
    """
    fill = max(int(fill_qty or 0), 0)
    if fill <= 0:
        return current
    delta = _signed_fill_delta(fill, side, action_type)
    if delta is None:
        return None
    return current + delta


def apply_fill_to_session_ledger(
    trader,
    symbol: str,
    qty: int,
    action_type: str,
    side: str = "",
) -> None:
    """Update engine-only ledger on confirmed fills (never from broker sync)."""
    if not eod_use_session_ledger_enabled():
        return

    sym = normalize_symbol(symbol)
    fill_qty = int(qty or 0)
    if not sym or fill_qty <= 0:
        return

    lock = getattr(trader, "_session_open_qty_lock", None)
    if lock is None:
        return

    with lock:
        ledger = getattr(trader, "_session_open_qty", None)
        if ledger is None:
            return
        current = int(ledger.get(sym, 0) or 0)
        new_qty = _compute_ledger_qty_after_fill(current, fill_qty, action_type, side)
        if new_qty is None:
            print(
                f"[SESSION-LEDGER] {sym} action={action_type} side={side or '-'} "
                f"fill={fill_qty} skipped — unknown fill side"
            )
            return
        if new_qty == 0:
            ledger.pop(sym, None)
        else:
            ledger[sym] = new_qty
        print(
            f"[SESSION-LEDGER] {sym} action={action_type} side={side or '-'} "
            f"fill={fill_qty} before={current} after={new_qty}"
        )

    session_id = get_trader_session_id(trader)
    record_symbol_traded(get_trader_redis(trader), session_id, sym)


def record_engine_order_for_trader(trader, order_id: Optional[str]) -> None:
    record_engine_order(
        get_trader_redis(trader),
        get_trader_session_id(trader),
        order_id,
    )


def resolve_session_allow_list(
    trader,
    fallback_symbols: Optional[List[str]] = None,
) -> Set[str]:
    session_id = get_trader_session_id(trader)
    redis_client = get_trader_redis(trader)
    ledger_keys = [
        sym for sym, qty in get_session_ledger(trader).items() if int(qty or 0) != 0
    ]

    allowed = get_session_symbols_from_redis(
        redis_client,
        session_id,
        fallback_symbols=fallback_symbols,
        ledger_symbols=ledger_keys,
    )
    return allowed


def broker_positions_to_map(
    broker_positions: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for pos in broker_positions or []:
        sym = normalize_symbol(pos.get("symbol", ""))
        if sym:
            out[sym] = pos
    return out


def build_eod_exit_plan(
    trader,
    broker_positions: List[Dict[str, Any]],
    fallback_symbols: Optional[List[str]] = None,
    on_skip_summary: Optional[Callable[[int, List[str], str], None]] = None,
) -> List[Dict[str, Any]]:
    """
    Build EOD exit rows with session-attributed qty.

    on_skip_summary(count, sample_symbols, reason) called once at end if any skips.
    reason: 'ledger_zero' | 'ledger_fallback'
    """
    skipped_manual_on_session_symbol: List[str] = []

    if not exit_only_session_symbols_enabled():
        plan: List[Dict[str, Any]] = []
        for pos in broker_positions or []:
            sym = normalize_symbol(pos.get("symbol", ""))
            if not sym:
                continue
            side = pos.get("side", "BUY")
            qty = int(pos.get("qty", 0) or 0)
            if qty <= 0:
                continue
            plan.append({
                "symbol": sym,
                "side": side,
                "qty": qty,
                "exit_side": "SELL" if side == "BUY" else "BUY",
                "ltp": float(pos.get("ltp") or 0.0),
                "ledger_qty": qty,
                "broker_qty": qty,
            })
        return plan

    allowed = resolve_session_allow_list(trader, fallback_symbols=fallback_symbols)
    ledger = get_session_ledger(trader)
    broker_map = broker_positions_to_map(broker_positions)

    if not allowed and ledger_has_open_positions(trader):
        allowed = {
            normalize_symbol(s) for s in ledger.keys() if int(ledger.get(s, 0) or 0) != 0
        }
        print(
            f"[EOD] Allow-list empty but ledger open — using ledger keys: "
            f"{sorted(allowed)}"
        )
        if on_skip_summary:
            try:
                on_skip_summary(0, [], "ledger_fallback")
            except Exception as e:
                print(f"[EOD] on_skip_summary callback failed: {e}")

    plan: List[Dict[str, Any]] = []

    if eod_use_session_ledger_enabled():
        symbols_to_check = set(allowed) | {
            s for s, q in ledger.items() if int(q or 0) != 0
        }
        for sym in sorted(symbols_to_check):
            ledger_qty = int(ledger.get(sym, 0) or 0)
            broker_pos = broker_map.get(sym)
            broker_qty = int(broker_pos.get("qty", 0) or 0) if broker_pos else 0
            broker_side = (broker_pos or {}).get("side", "BUY")

            if sym not in allowed:
                if ledger_qty != 0:
                    print(
                        f"[EOD-SKIP] {sym} ledger={ledger_qty} — not in session allow-list"
                    )
                continue

            if ledger_qty == 0:
                if broker_qty > 0:
                    print(
                        f"[EOD-SKIP] {sym} broker_net={broker_qty} ledger=0 "
                        "(manual/other on session symbol)"
                    )
                    skipped_manual_on_session_symbol.append(sym)
                continue

            if ledger_qty > 0:
                expected_broker_side = "BUY"
                exit_side = "SELL"
                ledger_abs = ledger_qty
            else:
                expected_broker_side = "SELL"
                exit_side = "BUY"
                ledger_abs = abs(ledger_qty)

            if broker_qty <= 0:
                print(
                    f"[EOD-WARN] {sym} ledger={ledger_qty} broker_net=0 — skip over-sell"
                )
                continue

            if broker_side != expected_broker_side:
                print(
                    f"[EOD-WARN] {sym} ledger={ledger_qty} broker_side={broker_side} "
                    f"expected={expected_broker_side} — side mismatch, skip"
                )
                continue

            exit_qty = min(ledger_abs, broker_qty)
            if exit_qty <= 0:
                continue

            plan.append({
                "symbol": sym,
                "side": broker_side,
                "qty": exit_qty,
                "exit_side": exit_side,
                "ltp": float((broker_pos or {}).get("ltp") or 0.0),
                "ledger_qty": ledger_qty,
                "broker_qty": broker_qty,
            })
            print(
                f"[EOD-PLAN] {sym} allowed=Y ledger={ledger_qty} "
                f"broker_net={broker_qty} ({broker_side}) exit_qty={exit_qty} "
                f"exit_side={exit_side}"
            )

        if skipped_manual_on_session_symbol and on_skip_summary:
            try:
                on_skip_summary(
                    len(skipped_manual_on_session_symbol),
                    skipped_manual_on_session_symbol[:5],
                    "ledger_zero",
                )
            except Exception as e:
                print(f"[EOD] on_skip_summary callback failed: {e}")
        return plan

    for sym in sorted(allowed):
        broker_pos = broker_map.get(sym)
        if not broker_pos:
            continue
        qty = int(broker_pos.get("qty", 0) or 0)
        if qty <= 0:
            continue
        side = broker_pos.get("side", "BUY")
        plan.append({
            "symbol": sym,
            "side": side,
            "qty": qty,
            "exit_side": "SELL" if side == "BUY" else "BUY",
            "ltp": float(broker_pos.get("ltp") or 0.0),
            "ledger_qty": int(ledger.get(sym, 0) or 0),
            "broker_qty": qty,
        })
    return plan
