def insert_trading_snapshot(
    trading_logs_collection,
    session_id: str,
    cycle: int,
    snapshot: dict,
    rows: list[dict],
    simulation_logs: bool = False,
):
    if not rows:
        return

    def as_float(value, default=0.0):
        try:
            if value is None:
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    portfolio_pnl = snapshot.get("pnl")
    if portfolio_pnl is None:
        portfolio_pnl = round(
            as_float(snapshot.get("realized_pnl")) + as_float(snapshot.get("unrealized_pnl")),
            2,
        )

    docs = []
    for r in rows:
        symbol_unrealized_pnl = r.get("symbol_unrealized_pnl", r.get("unrealized_pnl"))
        symbol_realized_pnl = r.get("symbol_realized_pnl", 0.0)
        symbol_pnl = r.get("symbol_pnl", r.get("pnl"))
        if symbol_pnl is None:
            symbol_pnl = round(as_float(symbol_realized_pnl) + as_float(symbol_unrealized_pnl), 2)

        docs.append({
            "session_id": session_id,
            "cycle": cycle,
            "timestamp": snapshot["timestamp"],
            "simulation_logs": bool(simulation_logs),

            "symbol": r.get("symbol"),
            "curr_price": r.get("curr_price"),
            "return_pct": r.get("return_pct"),
            "side": r.get("side"),
            "signal": r.get("signal"),
            "action": r.get("action"),
            "qty": r.get("qty", 0),
            "unrealized_pnl": r.get("unrealized_pnl"),
            "symbol_unrealized_pnl": symbol_unrealized_pnl,
            "symbol_realized_pnl": symbol_realized_pnl,
            "symbol_pnl": symbol_pnl,

            "cash_balance": snapshot["cash_balance"],
            "realized_pnl": snapshot["realized_pnl"],
            "pnl": symbol_pnl,
            "total_equity": snapshot["total_equity"],
            "portfolio_cash_balance": snapshot["cash_balance"],
            "portfolio_realized_pnl": snapshot["realized_pnl"],
            "portfolio_unrealized_pnl": snapshot["unrealized_pnl"],
            "portfolio_pnl": portfolio_pnl,
            "portfolio_total_equity": snapshot["total_equity"],
        })

    trading_logs_collection.insert_many(docs)
