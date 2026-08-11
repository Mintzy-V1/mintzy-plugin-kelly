
import sys
from client import Client
from auto_trader import AutoTrader
from alerts import AlertManager

def ask_int(prompt, min_v=1, max_v=None):
    while True:
        try:
            v = int(input(prompt).strip())
            if v < min_v:
                print(f"Value must be >= {min_v}")
                continue
            if max_v is not None and v > max_v:
                print(f"Value must be <= {max_v}")
                continue
            return v
        except Exception:
            print("Please enter an integer (e.g. 5).")

def ask_float(prompt, min_v=0.0):
    while True:
        try:
            v = float(input(prompt).strip())
            if v < min_v:
                print(f"Value must be >= {min_v}")
                continue
            return v
        except Exception:
            print("Please enter a number ")

def ask_symbol(prompt):
    while True:
        s = input(prompt).strip().upper()
        if s == "":
            print("Symbol cannot be blank.")
            continue
        if " " in s or len(s) > 30:
            print("Symbol looks invalid. Try again (e.g. RELIANCE, TCS).")
            continue
        return s

def flatten_numeric_values(obj, numbers):
    """Recursively collect numeric-looking values from nested dict/list into numbers list."""
    if obj is None:
        return
    if isinstance(obj, dict):
        for v in obj.values():
            flatten_numeric_values(v, numbers)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            flatten_numeric_values(v, numbers)
    else:
        try:
            if isinstance(obj, (int, float)):
                numbers.append(float(obj))
            else:
                s = str(obj).replace(",", "").replace("â‚¹", "").strip()
                if s.replace(".", "", 1).lstrip("+-").isdigit():
                    numbers.append(float(s))
        except Exception:
            pass

def guess_free_cash_from_resp(resp):
    if resp is None:
        return None
    numbers = []
    candidates = []
    if isinstance(resp, dict):
        if "data" in resp:
            candidates.append(resp["data"])
        if "raw" in resp:
            candidates.append(resp["raw"])
        candidates.append(resp)
    else:
        candidates.append(resp)

    for c in candidates:
        flatten_numeric_values(c, numbers)

    pos = [n for n in numbers if n is not None and n > 0 and n < 1e9]
    if not pos:
        return None
    pos_sorted = sorted(pos)
    large = [n for n in pos_sorted if n >= 100]
    if large:
        return float(large[-1])
    return float(pos_sorted[-1])

def main():
    print("=" * 80)
    print("    ðŸ¤– AutoTrader Interactive Setup (REAL BROKER CONNECTION)")
    print("=" * 80)
    print("\nðŸ“Œ This will connect to your Angel One account via BrokerConnector.")
    print("ðŸ“Œ Ensure your API credentials are set in environment variables or be ready to enter them.\n")

    # Initialize components
    client = Client(api_key="XeyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9")
    alerts = AlertManager()
    trader = AutoTrader(client=client, alerts=alerts)

    # Link to broker and fetch free cash
    print("ðŸ”— Connecting to broker to read available cash/margin...\n")
    try:
        trader._link_broker()
        print("âœ… Broker linked successfully!")
    except Exception as e:
        print(f"âŒ Failed to link broker: {e}")
        print("âš ï¸  Ensure ANGEL_API_KEY, ANGEL_CLIENT_CODE and ANGEL_PASSWORD are set.")
        sys.exit(1)

    # Attempt to obtain free cash automatically
    free_cash = None
    try:
        broker = trader.broker
        session = trader.session

        # Prefer explicit get_account_balance (new helper in BrokerConnector)
        if hasattr(broker, "get_account_balance"):
            try:
                resp = broker.get_account_balance(session)
                if isinstance(resp, dict) and resp.get("status") == "success" and resp.get("free_cash") is not None:
                    free_cash = float(resp.get("free_cash"))
                    print(f"âœ… Free cash detected from broker: â‚¹{free_cash:,.2f}")
                    print(f"   Source: {resp.get('source', 'unknown')}\n")
                    
                    # Show raw response sample for debugging
                    if resp.get("raw") is not None:
                        print("ðŸ“„ Raw response sample (first 500 chars):")
                        try:
                            raw_s = str(resp.get("raw"))
                            print(raw_s[:500])
                            print()
                        except Exception:
                            pass
                else:
                    # Fallback to heuristic on returned payload
                    free_cash = guess_free_cash_from_resp(resp)
                    if free_cash:
                        print(f"âœ… Free cash guessed from response: â‚¹{free_cash:,.2f}\n")
            except Exception as e:
                print(f"âš ï¸  get_account_balance call failed: {e}")

        # Fallback: try get_holdings/get_positions
        if free_cash is None and hasattr(broker, "get_holdings"):
            try:
                resp = broker.get_holdings(session)
                free_cash = guess_free_cash_from_resp(resp)
                if free_cash:
                    print(f"âœ… Free cash guessed from holdings: â‚¹{free_cash:,.2f}\n")
            except Exception:
                pass
        
        if free_cash is None and hasattr(broker, "get_positions"):
            try:
                resp = broker.get_positions(session)
                free_cash = guess_free_cash_from_resp(resp)
                if free_cash:
                    print(f"âœ… Free cash guessed from positions: â‚¹{free_cash:,.2f}\n")
            except Exception:
                pass

        # Final fallback: try SmartConnect funds() if present
        if free_cash is None and hasattr(broker, "obj") and broker.obj:
            try:
                if hasattr(broker.obj, "funds"):
                    resp = broker.obj.funds()
                    free_cash = guess_free_cash_from_resp(resp)
                    if free_cash:
                        print(f"âœ… Free cash guessed from funds(): â‚¹{free_cash:,.2f}\n")
            except Exception:
                pass

    except Exception as e:
        print(f"âš ï¸  Error while trying to fetch account balance: {e}\n")

    # If we couldn't determine free_cash, ask user to type it manually
    if free_cash is None or free_cash <= 0:
        print("âš ï¸  Could not automatically determine available free cash from broker response.")
        print("ðŸ“ Please enter the amount of free cash / usable margin available in your Angel One account.\n")
        free_cash = ask_float("ðŸ’° Enter available cash (â‚¹): ", min_v=1.0)

    # Validate minimum cash requirement
    if free_cash is None or free_cash <= 0:
        print(f"\nâŒ Detected available cash = â‚¹{free_cash}. This is insufficient to proceed.")
        print("ðŸ’¡ Please fund your account and try again.")
        sys.exit(1)

    print("\n" + "=" * 80)
    print(f"ðŸ’° Available Free Cash Confirmed: â‚¹{free_cash:,.2f}")
    print("=" * 80 + "\n")

    # Ask how many stocks the user wants to trade
    n = ask_int("ðŸ“Š How many stocks do you want to trade? Enter a number (1-25): ", min_v=1, max_v=25)

    allocations = {}
    total_allocated = 0.0

    for i in range(1, n + 1):
        print(f"\n{'â”€' * 80}")
        print(f"ðŸ“ˆ Stock #{i}")
        print(f"{'â”€' * 80}")
        
        while True:
            sym = ask_symbol("ðŸ”¤ Symbol (e.g. RELIANCE): ")
            cap = ask_float(f"ðŸ’µ Capital to allocate to {sym} (â‚¹): ", min_v=0.0)
            sl_pct = ask_float(f"ðŸ›¡ï¸  Stop-loss for {sym} (as decimal, e.g. 0.05 for 5%): ", min_v=0.0)
            
            tentative_total = total_allocated + cap
            
            if tentative_total > free_cash:
                print(f"\nâŒ ERROR: Total allocations would exceed available free cash!")
                print(f"   Allocated so far: â‚¹{total_allocated:,.2f}")
                print(f"   Trying to add: â‚¹{cap:,.2f}")
                print(f"   Total would be: â‚¹{tentative_total:,.2f}")
                print(f"   Available cash: â‚¹{free_cash:,.2f}")
                print(f"   âš ï¸  Please re-enter this stock with a lower amount.\n")
                continue
            
            # Accept the allocation
            allocations[sym] = {"capital": cap, "stop_loss": sl_pct}
            total_allocated += cap
            
            print(f"\nâœ… Added: {sym}")
            print(f"   Capital: â‚¹{cap:,.2f}")
            print(f"   Stop-loss: {sl_pct*100:.2f}%")
            print(f"   Total allocated so far: â‚¹{total_allocated:,.2f} / â‚¹{free_cash:,.2f}")
            break

    # Show allocation summary
    print("\n" + "=" * 80)
    print("ðŸ“‹ ALLOCATION SUMMARY")
    print("=" * 80)
    
    for s, v in allocations.items():
        print(f"  {s:.<20} Capital: â‚¹{v['capital']:>12,.2f}  |  SL: {v['stop_loss']*100:>6.2f}%")
    
    print("â”€" * 80)
    print(f"  {'TOTAL ALLOCATION':.<20} â‚¹{total_allocated:>12,.2f}")
    print(f"  {'AVAILABLE CASH':.<20} â‚¹{free_cash:>12,.2f}")
    print(f"  {'REMAINING CASH':.<20} â‚¹{free_cash - total_allocated:>12,.2f}")
    print("=" * 80 + "\n")

    # Final validation (should not happen, but safety check)
    if total_allocated > free_cash:
        print("âŒ ERROR: Total allocation exceeds available free cash â€“ aborting.")
        sys.exit(1)

    # Confirm before proceeding
    proceed = input("â–¶ï¸  Proceed to start the trader with these allocations? (yes/no): ").strip().lower()
    if proceed not in ("y", "yes"):
        print("ðŸ›‘ Aborting per user request.")
        sys.exit(0)

    print("\n" + "=" * 80)
    print("ðŸš€ STARTING AUTOTRADER...")
    print("=" * 80 + "\n")

    # Start the AutoTrader with the chosen allocations
    try:
        trader.start(
            symbols=list(allocations.keys()),
            time_frame="3 hours",
            initial_allocations=allocations,
            use_broker_cash_as_capital=True,  # âœ… Use actual broker cash
            stop_on_insufficient=True,
            min_required_cash=total_allocated
        )
    except KeyboardInterrupt:
        print("\n\nðŸ›‘ User interrupted. Stopping trader gracefully...")
        print("=" * 80)
        sys.exit(0)
    except Exception as e:
        print(f"\nâŒ Fatal error while running trader: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()