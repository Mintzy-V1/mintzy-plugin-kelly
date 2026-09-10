"""Assert Redis key builders match legacy production formats."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils.redis_keys import (  # noqa: E402
    exit_request_key,
    exit_result_key,
    exit_status_key,
    live_pnl_key,
    pyramid_result_key,
    rms_exited_key,
    session_meta_key,
    session_order_ids_key,
    session_pid_key,
    session_symbols_traded_key,
    simulation_stop_key,
    stop_job_key,
)

SID = "session_20260910123456_abc123"


def main() -> int:
    checks = [
        (session_pid_key(SID), f"autotrader:session:{SID}"),
        (session_meta_key(SID), f"autotrader:session:{SID}:meta"),
        (session_order_ids_key(SID), f"autotrader:session:{SID}:order_ids"),
        (session_symbols_traded_key(SID), f"autotrader:session:{SID}:symbols_traded"),
        (simulation_stop_key(SID), f"autotrader:simulation_stop:{SID}"),
        (pyramid_result_key(SID), f"autotrader:pyramid_result:{SID}"),
        (stop_job_key(SID), f"autotrader:stop_job:{SID}"),
        (exit_request_key(SID), f"autotrader:exit_request:{SID}"),
        (exit_result_key(SID, "ITC-EQ"), f"autotrader:exit_result:{SID}:ITC"),
        (exit_status_key(SID), f"autotrader:exit_status:{SID}"),
        (rms_exited_key(SID), f"autotrader:rms_exited:{SID}"),
        (live_pnl_key(SID), f"live_pnl:{SID}"),
    ]
    failed = 0
    for got, expected in checks:
        if got != expected:
            print(f"FAIL expected={expected!r} got={got!r}")
            failed += 1
        else:
            print(f"OK   {got}")
    if failed:
        print(f"\n{failed} assertion(s) failed")
        return 1
    print("\nAll Redis key assertions passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
