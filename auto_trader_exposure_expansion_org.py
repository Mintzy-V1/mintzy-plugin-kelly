import pandas as pd
import time
import csv
import os
import json
import requests
from datetime import datetime, time as dt_time, timedelta, timezone
from alerts import AlertManager
from broker_angle import BrokerConnector
import numpy as np
from orderbook import fetch_todays_intraday_orders
from concurrent.futures import ThreadPoolExecutor, as_completed


# ==================== PARALLEL EXECUTION IMPORTS ====================
import threading
from queue import Queue, Empty, Full
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import threading
import logging
import traceback
from trading_snapshot import insert_trading_snapshot
from utils.session_ledger import (
    apply_fill_to_session_ledger,
    record_engine_order_for_trader,
)
from utils.eod_exit import (
    try_begin_eod_exit,
    mark_eod_exit_done,
    release_eod_exit_in_progress,
    prepare_eod_exit_plan,
    exit_plan_to_broker_positions,
    finalize_eod_shutdown,
)
from utils.redis_keys import exit_status_key, live_pnl_key, rms_exited_key

# ====================================================================
from trading_state import trading_snapshot

print("TRADER snapshot id:", id(trading_snapshot))



# ==================== TIMING LOGGER ====================
class TimingLogger:
    """
    Thread-safe CSV logger for per-cycle timing probes.
    One row per timed event: timestamp, cycle, candle_key, event_label, elapsed_sec.
    File rotates daily: timing_log_YYYY-MM-DD.csv  (stored in self.log_dir).
    Usage:
        tlog = TimingLogger(log_dir)
        tlog.start_cycle(cycle_count, candle_key)
        t0 = time.time(); ...; tlog.record("LTP_FETCH", t0)
    """
    _lock = threading.Lock()
 
    def __init__(self, log_dir: str):
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        self._cycle = 0
        self._candle_key = ""
        self._cycle_start = None
 
    def start_cycle(self, cycle: int, candle_key: str):
        self._cycle = cycle
        self._candle_key = candle_key
        self._cycle_start = time.time()
        self._write("CYCLE_START", 0.0, note="")
 
    def record(self, label: str, t0: float, note: str = ""):
        elapsed = round(time.time() - t0, 3)
        self._write(label, elapsed, note)
        print(f"[TIMING] {label:<45} {elapsed:>7.3f}s  {note}")
        return elapsed
 
    def record_since_cycle_start(self, label: str, note: str = ""):
        if self._cycle_start is None:
            return
        elapsed = round(time.time() - self._cycle_start, 3)
        self._write(label, elapsed, note)
        print(f"[TIMING] {label:<45} {elapsed:>7.3f}s (since cycle start)  {note}")
 
    def _write(self, label: str, elapsed: float, note: str):
        date_str = datetime.now().strftime("%Y-%m-%d")
        path = os.path.join(self.log_dir, f"timing_log_{date_str}.csv")
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        with self._lock:
            file_exists = os.path.exists(path)
            with open(path, "a", newline="") as f:
                w = csv.writer(f)
                if not file_exists:
                    w.writerow(["timestamp", "cycle", "candle_key", "event", "elapsed_sec", "note"])
                w.writerow([now_str, self._cycle, self._candle_key, label, elapsed, note])
# ========================================================


# Market timezone: IST (UTC+5:30)
MARKET_TZ = timezone(timedelta(hours=5, minutes=30))

# Hard square-off window (IST)
MARKET_EXIT_TIME = dt_time(15, 0)       # 3:00 PM IST — exit all open positions
MARKET_EXIT_WARN_TIME = dt_time(14, 55) # 2:55 PM IST — warning before auto exit

# 14:15 IST stop-lock — exit losers, continue with green symbols
STOP_LOCK_TIME = dt_time(14, 15)


def load_json(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}


# ==================== PARALLEL ORDER EXECUTOR CLASSES ====================
@dataclass
class OrderRequest:
    symbol: str
    side: str
    qty: int
    order_type: str = "MARKET"
    product_type: str = "INTRADAY"
    price: Optional[float] = None
    stop_loss: Optional[float] = None
    trigger_price: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class OrderResult:
    symbol: str
    success: bool
    order_id: Optional[str] = None
    filled: bool = False
    avg_price: float = 0.0
    filled_qty: int = 0
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class RateLimiter:
    def __init__(self, max_calls: int, time_window: float):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = []
        self.lock = threading.Lock()
    
    def acquire(self):
        with self.lock:
            now = time.time()
            self.calls = [call_time for call_time in self.calls 
                         if now - call_time < self.time_window]
            
            if len(self.calls) >= self.max_calls:
                oldest_call = self.calls[0]
                sleep_time = self.time_window - (now - oldest_call)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.time()
                    self.calls = [call_time for call_time in self.calls 
                                 if now - call_time < self.time_window]
            
            self.calls.append(now)


class ParallelOrderExecutor:   
    def __init__(self, broker, session, 
                 max_workers: int = 5,
                 order_rate_limit: int = 10,
                 order_rate_window: float = 1.0,
                 status_rate_limit: int = 20,
                 status_rate_window: float = 1.0):
        self.broker = broker
        self.session = session
        self.max_workers = max_workers
        
        self.order_limiter = RateLimiter(order_rate_limit, order_rate_window)
        self.status_limiter = RateLimiter(status_rate_limit, status_rate_window)
        
        self.order_queue = Queue()
        self.result_queue = Queue()
        self.workers = []
        self.stop_flag = threading.Event()
    
    def _worker(self):
        while not self.stop_flag.is_set():
            try:
                try:
                    order_req = self.order_queue.get(timeout=0.5)
                except:
                    continue
                
                if order_req is None:
                    break
                
                result = self._execute_single_order(order_req)
                self.result_queue.put(result)
                self.order_queue.task_done()

            except Exception as e:
                print(f"[Worker Error] {e}")
                logging.exception("Order worker crashed")
                self.result_queue.put(
                  OrderResult(
                  symbol="UNKNOWN",
                  success=False,
                  error=str(e)
                    )
                      )

    def _execute_single_order(self, order_req: OrderRequest) -> OrderResult:
        try:
            self.order_limiter.acquire()
            
            placed_at_ts = datetime.now()
            print(
                f"\n[DEBUG-ORDER] [{placed_at_ts.strftime('%H:%M:%S.%f')[:-3]}] Submitting {order_req.side} order for "
                f"symbol={order_req.symbol} "
                f"qty={order_req.qty} "
                f"type={order_req.order_type} "
                f"product={order_req.product_type} "
                f"price={order_req.price} "
                f"sl={order_req.stop_loss} "
                f"trigger={order_req.trigger_price}"
            )
            
            order_response = self.broker.place_order(
                session=self.session,
                symbol=order_req.symbol,
                side=order_req.side,
                qty=order_req.qty,
                order_type=order_req.order_type,
                product_type=order_req.product_type,
                price=order_req.price,
                stop_loss=order_req.stop_loss,
                trigger_price=order_req.trigger_price,
                wait_for_confirmation=False
            )
            
            print("\n[EXECUTOR] place_order raw response:")
            print(order_response)
            
            order_id = None
            if isinstance(order_response, dict):
                order_id = order_response.get("order_id")
                if order_id:
                    received_at_ts = datetime.now()
                    print(f"[DEBUG-ORDER] [{received_at_ts.strftime('%H:%M:%S.%f')[:-3]}] Received order_id {order_id} for {order_req.symbol}")
                
                if order_response.get("status") == "error":
                    return OrderResult(
                        symbol=order_req.symbol,
                        success=False,
                        error=order_response.get("error", "Unknown error"),
                        metadata=order_req.metadata
                    )
            
            if not order_id:
                return OrderResult(
                    symbol=order_req.symbol,
                    success=False,
                    error="No order ID received",
                    metadata=order_req.metadata
                )
            
            #  PHASE A: fire-and-forget  return immediately, reconciler checks status
            # orderBook() is intentionally NOT called here  reconcile thread handles it
            return OrderResult(
                symbol=order_req.symbol,
                success=True,          
                order_id=order_id,
                filled=False,          
                avg_price=0.0,
                filled_qty=0,
                error=None,
                metadata=order_req.metadata
            )
            
        except Exception as e:
            return OrderResult(
                symbol=order_req.symbol,
                success=False,
                error=str(e),
                metadata=order_req.metadata
            )
    
    def start(self):
        self.stop_flag.clear()
        self.workers = []
        
        for i in range(self.max_workers):
            worker = threading.Thread(target=self._worker, name=f"OrderWorker-{i}")
            worker.daemon = True
            worker.start()
            self.workers.append(worker)
    
    def stop(self):
        self.stop_flag.set()
        for _ in self.workers:
            self.order_queue.put(None)
        for worker in self.workers:
            worker.join(timeout=5)
        self.workers = []

    def submit_orders(self, orders: List[OrderRequest]) -> List[OrderResult]:
        if not self.workers:
            self.start()
        
        for order in orders:
            self.order_queue.put(order)
        
        results = []
        for i, order in enumerate(orders):        #  enumerate so we know which order
            try:
                result = self.result_queue.get(timeout=10)  #  reduced from 30s to 10s
            except Exception:
                print(f"[TIMEOUT] Order {i+1}/{len(orders)} timed out: "
                    f"{order.symbol} {order.side} {order.qty}")
                result = OrderResult(
                    symbol=order.symbol,           #  now we know the symbol
                    success=False,
                    error="Order execution timeout",
                    metadata=order.metadata        #  preserve metadata for pending_orders
                )
                
            results.append(result)
        
        self.order_queue.join()
        return results
    
class OrderBatcher:    
    def __init__(self ,tlog=None):
        self.buy_orders = []
        self.sell_orders = []
        self.cover_orders = []
        self.exit_orders = []
        self.tlog = tlog
    
    def add_order(self, order: OrderRequest, order_category: str = "general"):
        t0 = time.time()
        category = order_category.lower()
        
        if category == "buy" or order.side.upper() == "BUY":
            self.buy_orders.append(order)
        elif category in ["sell", "short"] or order.side.upper() == "SELL":
            self.sell_orders.append(order)
        elif category == "cover":
            self.cover_orders.append(order)
        elif category == "exit":
            self.exit_orders.append(order)
        else:
            if order.side.upper() == "BUY":
                self.buy_orders.append(order)
            else:
                self.sell_orders.append(order)
        elapsed = time.time() - t0

        print(f"[TRACE] ADD_ORDER {order.symbol}: {elapsed:.6f}s")   
        if self.tlog:  #  guard added
            self.tlog.record("order batcher add order timing", t0, note="order batcher add order")    
    
    def get_all_orders(self, priority: str = "exit_first") -> List[OrderRequest]:
        if priority == "exit_first":
            return (self.exit_orders + self.cover_orders + 
                   self.sell_orders + self.buy_orders)
        else:
            return (self.buy_orders + self.sell_orders + 
                   self.cover_orders + self.exit_orders)
    
    def clear(self):
        self.buy_orders = []
        self.sell_orders = []
        self.cover_orders = []
        self.exit_orders = []
    
    def is_empty(self) -> bool:
        return not (self.buy_orders or self.sell_orders or 
                   self.cover_orders or self.exit_orders)
    
    def get_count(self) -> int:
        return (len(self.buy_orders) + len(self.sell_orders) + 
                len(self.cover_orders) + len(self.exit_orders))

# ==================== END PARALLEL ORDER EXECUTOR ====================


# ==================== ASYNC CSV LOGGER (non-blocking writes) ====================
class AsyncCsvLogger:
    """
    Background CSV writer used by the WS tick path. Callers do `write(path, row)`
    which is a non-blocking enqueue (microseconds). A daemon thread drains the
    queue, batches rows per file, and flushes every FLUSH_INTERVAL_SEC or when
    a file's batch reaches BATCH_SIZE — whichever comes first.

    Why this exists: synchronous open-append-close inside on_ltp_tick stalls
    the WS thread under high tick volume (25+ symbols at active hours can
    exceed 100 ticks/s, each potentially writing to multiple CSVs).
    """

    BATCH_SIZE = 100
    FLUSH_INTERVAL_SEC = 0.5
    QUEUE_MAXSIZE = 20000

    def __init__(self, name: str = "AsyncCsvLogger"):
        self._queue: Queue = Queue(maxsize=self.QUEUE_MAXSIZE)
        self._stop = threading.Event()
        self._registered: set = set()
        self._dropped_count = 0
        self._last_drop_warn_ts = 0.0
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)
        self._thread.start()
        print(f"[ASYNC-CSV] writer thread started (batch={self.BATCH_SIZE}, "
              f"flush_interval={self.FLUSH_INTERVAL_SEC}s, max_queue={self.QUEUE_MAXSIZE})")

    def register(self, path: str, header: list) -> None:
        """Create the file with header if it doesn't exist. Safe to call repeatedly."""
        if path in self._registered:
            return
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            if not os.path.exists(path):
                with open(path, "w", newline="") as f:
                    csv.writer(f).writerow(header)
                print(f"[ASYNC-CSV] created {path} with header={header}")
            self._registered.add(path)
        except Exception as e:
            print(f"[ASYNC-CSV] register({path}) failed: {e}")

    def write(self, path: str, row: list) -> None:
        """Non-blocking enqueue. Drops the row if the queue is full."""
        try:
            self._queue.put_nowait((path, row))
        except Full:
            self._dropped_count += 1
            now = time.time()
            if now - self._last_drop_warn_ts > 5.0:   # warn at most every 5s
                print(f"[ASYNC-CSV] queue full, dropped {self._dropped_count} rows "
                      f"(latest target={path})")
                self._last_drop_warn_ts = now

    def stop(self, drain_timeout_sec: float = 5.0) -> None:
        """Drain the queue and stop the writer thread."""
        print("[ASYNC-CSV] stop requested, draining queue...")
        deadline = time.time() + drain_timeout_sec
        while not self._queue.empty() and time.time() < deadline:
            time.sleep(0.05)
        self._stop.set()
        self._thread.join(timeout=2.0)
        print(f"[ASYNC-CSV] stopped. total_dropped={self._dropped_count}")

    def _run(self) -> None:
        from collections import defaultdict
        buffers = defaultdict(list)
        last_flush = time.time()

        while not self._stop.is_set():
            try:
                path, row = self._queue.get(timeout=0.1)
                buffers[path].append(row)
                if len(buffers[path]) >= self.BATCH_SIZE:
                    self._flush_one(path, buffers)
            except Empty:
                pass

            if time.time() - last_flush >= self.FLUSH_INTERVAL_SEC:
                for path in list(buffers.keys()):
                    self._flush_one(path, buffers)
                last_flush = time.time()

        # Final drain on shutdown
        try:
            while True:
                path, row = self._queue.get_nowait()
                buffers[path].append(row)
        except Empty:
            pass
        for path in list(buffers.keys()):
            self._flush_one(path, buffers)

    def _flush_one(self, path: str, buffers: dict) -> None:
        rows = buffers.get(path)
        if not rows:
            return
        try:
            with open(path, "a", newline="") as f:
                csv.writer(f).writerows(rows)
            buffers[path] = []
        except Exception as e:
            print(f"[ASYNC-CSV] flush failed for {path}: {e}")
# ==================== END ASYNC CSV LOGGER ====================


class AutoTrader:
    def __init__(self, prediction_client, market_client, broker=None, alerts=None,
                 initial_capital=196000,
                 get_access_token=None,
                 log_dir=None,
                 trading_logs_collection=None):
        self._last_executed_candle = None
        self.pred_client = prediction_client
        self.market_client = market_client
        self.broker = broker
        self.alerts = alerts if alerts is not None else AlertManager()
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.cash_balance = initial_capital
        self.get_access_token = get_access_token
        self.trading_logs_collection = trading_logs_collection
        self.config_db_name = os.environ.get("MONGO_CONFIG_DB_NAME", "test")
        self.configuration_id = None
        self.leverage_multiplier = None
        self.simulation_logs = False
        self.max_exposure_pct = 1.00
        self.reserved_exposure = {}  
        self.symbol_locks = {}        
        self.broker_pos_lock = threading.Lock()
        self._broker_positions_cache = []
        # ====== CANDLE TIMESTAMP (for correct logging) ======
        self.current_cycle_ts = None
        self.current_cycle_ts_str = None
        # ================ RISK/EXPOSURE MANAGEMENT ================
        self.min_trade_pct = 0.05
        self.max_trade_pct = 0.15
        # ===================================================
        
        default_log_dir = os.environ.get("MINTZY_LOGS_DIR", "logs")
        self.log_dir = os.path.abspath(log_dir or default_log_dir)
        os.makedirs(self.log_dir, exist_ok=True)

        self.log_path = os.path.join(self.log_dir, "trade_log.csv")
        self.portfolio_log = os.path.join(self.log_dir, "portfolio_log.csv")

        self.positions = {}
        self.symbol_allocations = {}
        
        from collections import defaultdict
        self.pending_orders = defaultdict(list)

        self.pending_lock = threading.Lock()
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.total_profit = 0.0
        self.total_loss = 0.0
        self.unrealized_pnl = 0.0
        self.realized_pnl = 0.0
        from collections import defaultdict as _dd
        self.realized_pnl_by_symbol = _dd(float)  # cumulative realized PnL per symbol
        self.trade_history = []
        self.stop_event = threading.Event()
        self._exit_warning_sent = False
        self._stoplock_done = False
        self._eod_exit_done = False
        self._eod_exit_in_progress = False
        self._eod_exit_lock = threading.Lock()
        self._session_open_qty = {}
        self._session_open_qty_lock = threading.Lock()
        self.positions_lock = threading.Lock()   #  ADD THIS LINE
        self._exited_symbols = set()  # Symbols manually exited Ã¢â‚¬â€ excluded from future cycles

        # ==================== RMS: RISK MANAGEMENT SYSTEM ====================
        self.rms_triggered = False          # True once daily loss limit is hit
        self.rms_loss_limit = 0.0           # Set dynamically from capital at session start
        self.portfolio_max_loss_pct = 0.005 # 0.5% of total allocated capital -> halt ALL trading
        # ======================================================================

        # qty locked at first entry per symbol — reused for entire session
        self.symbol_qty: dict = {}

        # ==================== PARALLEL EXECUTION SETUP ====================
        self.parallel_executor = None
        self.use_parallel_execution = True  # Set False to disable parallel execution
        # ==================================================================

        # ==================== LIVE LTP STREAM (observational) ====================
        # Populated by on_ltp_tick() from a background WS thread.
        # NOT used by the existing PnL math — pure side-channel for the UI.
        self.live_pnl: dict = {}                 # {symbol: {ltp, pnl, qty, entry, side, ts}}
        self.live_pnl_lock = threading.Lock()
        self._live_pnl_last_write: dict = {}     # throttle CSV writes per symbol
        self._live_pnl_last_redis_write: float = 0.0   # throttle Redis writes (max 1/sec globally)
        self.live_pnl_log = os.path.join(self.log_dir, "live_pnl_log.csv")
        self.portfolio_pnl_log = os.path.join(self.log_dir, "portfolio_pnl_log.csv")
        self.rms_events_log = os.path.join(self.log_dir, "rms_events_log.csv")

        # Single async writer for all tick-driven CSVs — keeps the WS thread fast.
        self._csv_logger = AsyncCsvLogger(name=f"AsyncCsv-{getattr(self, 'session_id', 'trader')}")
        self._csv_logger.register(
            self.live_pnl_log,
            ["Timestamp", "Symbol", "Side", "Qty", "Entry_Price", "LTP", "PnL"],
        )
        self._csv_logger.register(
            self.portfolio_pnl_log,
            ["Timestamp", "Total_PnL", "Realized_PnL", "Live_Unrealized",
             "Limit", "Usage_Pct", "Open_Symbols"],
        )
        self._csv_logger.register(
            self.rms_events_log,
            ["Timestamp", "Event_Type", "Symbol", "PnL", "Realized",
             "Unrealized", "Threshold", "Action"],
        )

        # Per-ticker RMS layer (independent of portfolio-level RMS).
        # Trips when gross PnL (realized + unrealized) <= -(1% of entry_price * qty).
        # self.rms_per_ticker_loss_per_share = 9.8
        self.rms_per_ticker_loss_pct = 0.01
        self._rms_exit_inflight: set = set()

        # Tick-driven portfolio RMS — uses the SAME rms_loss_limit set by the
        # existing cycle-level path; only the trigger source differs (live ticks
        # vs end-of-cycle aggregation). Dedup flag distinct from rms_triggered.
        self._live_portfolio_rms_inflight = False

        # Diagnostics for tick-flow visibility (throttled).
        self._tick_first_seen_in_trader: set = set()
        self._last_portfolio_print_ts = 0.0
        self._portfolio_print_interval_sec = 30.0
        self._last_per_ticker_print_ts: dict = {}    # per-symbol last print
        self._per_ticker_print_interval_sec = 30.0
        if not os.path.exists(self.live_pnl_log):
            with open(self.live_pnl_log, "w", newline="") as f:
                csv.writer(f).writerow(
                    ["Timestamp", "Symbol", "Side", "Qty", "Entry_Price", "LTP", "PnL"]
                )
        # =========================================================================

         # ==================== TIMING LOGGER ====================
        # Initialised here so every method can call self.tlog.record(...)
        # The actual log_dir may not exist yet  TimingLogger creates it.
        self.tlog = TimingLogger(self.log_dir)
        # ========================================================

        if not os.path.exists(self.log_path):
            with open(self.log_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "Timestamp", "Symbol", "Signal", "Change(%)",
                    "Action_Status", "Price",
                    "P&L", "Total_Capital", "Return(%)"
                ])

        if not os.path.exists(self.portfolio_log):
            with open(self.portfolio_log, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "Timestamp", "Symbol", "Action", "Qty", "Entry_Price",
                    "Exit_Price", "P&L", "Cumulative_P&L", "Total_Capital",
                    "Return_On_Trade(%)", "Portfolio_Return(%)"
                ])
    
    # ---------- LIVE LTP TICK (background WS thread) ----------
    def on_ltp_tick(self, symbol: str, ltp: float, ts_epoch: float) -> None:
        """
        Called from LiveLTPStream's WS thread on every tick.
        Reads from self.positions (snapshot), writes to self.live_pnl.
        Does NOT mutate any field used by the existing PnL pipeline.
        """
        try:
            pos = self.positions.get(symbol)
            if not pos:
                if symbol not in self._tick_first_seen_in_trader:
                    self._tick_first_seen_in_trader.add(symbol)
                    print(f"[TRADER-TICK] {symbol} tick received but no position yet (ltp={ltp:.2f})")
                return
            qty = int(pos.get("qty") or 0)
            entry = float(pos.get("entry_price") or 0.0)
            side = (pos.get("side") or "BUY").upper()
            if qty <= 0 or entry <= 0:
                return

            pnl = (ltp - entry) * qty if side == "BUY" else (entry - ltp) * qty

            if symbol + ":pos" not in self._tick_first_seen_in_trader:
                self._tick_first_seen_in_trader.add(symbol + ":pos")
                print(
                    f"[TRADER-TICK] first tick-with-position for {symbol}: "
                    f"side={side} qty={qty} entry={entry:.2f} ltp={ltp:.2f} pnl={pnl:.2f}"
                )

            with self.live_pnl_lock:
                self.live_pnl[symbol] = {
                    "ltp": ltp,
                    "pnl": pnl,
                    "qty": qty,
                    "entry": entry,
                    "side": side,
                    "ts": ts_epoch,
                }
                last = self._live_pnl_last_write.get(symbol, 0.0)
                if ts_epoch - last >= 1.0:
                    self._live_pnl_last_write[symbol] = ts_epoch
                    write_row = True
                else:
                    write_row = False

            if write_row:
                self._csv_logger.write(
                    self.live_pnl_log,
                    [
                        datetime.fromtimestamp(ts_epoch).strftime("%Y-%m-%d %H:%M:%S"),
                        symbol, side, qty, f"{entry:.2f}",
                        f"{ltp:.2f}", f"{pnl:.2f}",
                    ],
                )

            # ----- Per-ticker RMS halt (independent of portfolio RMS) -----
            # Uses realized (closed trades for this symbol) + unrealized (open
            # position live PnL). Threshold = 1% of entry_price * qty (dynamic per open).
            realized_for_sym = float(self.realized_pnl_by_symbol.get(symbol, 0.0))
            total_for_sym = realized_for_sym + pnl
            # loss_threshold = -self.rms_per_ticker_loss_per_share * qty
            loss_threshold = -(self.rms_per_ticker_loss_pct * entry * qty)

            # Throttled per-ticker PnL log (every ~30s per symbol) — mirrors
            # the [RMS-LIVE-PORTFOLIO] line so you can watch each symbol's
            # headroom against its own threshold.
            now = time.time()
            last_print = self._last_per_ticker_print_ts.get(symbol, 0.0)
            if now - last_print >= self._per_ticker_print_interval_sec:
                self._last_per_ticker_print_ts[symbol] = now
                usage_pct = (total_for_sym / loss_threshold * 100.0) if loss_threshold else 0.0
                print(
                    f"[RMS-TICKER] {symbol} total={total_for_sym:.2f} "
                    f"(realized={realized_for_sym:.2f} + unrealized={pnl:.2f}) "
                    f"threshold={loss_threshold:.2f} usage={usage_pct:.1f}% "
                    f"qty={qty} side={side} ltp={ltp:.2f}"
                )

            if (total_for_sym <= loss_threshold
                    and symbol not in self._exited_symbols
                    and symbol not in self._rms_exit_inflight):
                self._rms_exit_inflight.add(symbol)
                print(
                    f"[RMS-TICKER] {symbol} breached: total={total_for_sym:.2f} "
                    f"(realized={realized_for_sym:.2f} + unrealized={pnl:.2f}) "
                    f"<= threshold={loss_threshold:.2f} (qty={qty}). Exiting."
                )
                self._csv_logger.write(
                    self.rms_events_log,
                    [
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "PER_TICKER_BREACH", symbol,
                        f"{total_for_sym:.2f}", f"{realized_for_sym:.2f}",
                        f"{pnl:.2f}", f"{loss_threshold:.2f}",
                        f"EXIT_SINGLE qty={qty}",
                    ],
                )
                self._notify_rms_exit_to_api(symbol, total_for_sym)
                threading.Thread(
                    target=self._rms_exit_worker,
                    args=(symbol,),
                    name=f"RMSExit-{symbol}",
                    daemon=True,
                ).start()

            # ----- Portfolio RMS halt disabled -----
            # Keep the layer available, but do not let portfolio-level loss
            # exit all stocks. Per-ticker RMS above remains active.
            # self._check_live_portfolio_rms()

            # ----- Push live PnL snapshot to Redis (max 1 write/sec) -----
            now = time.time()
            if now - self._live_pnl_last_redis_write >= 1.0:
                self._live_pnl_last_redis_write = now
                self._push_live_pnl_to_redis()

        except Exception as e:
            print(f"[LIVE-PNL] tick handler error for {symbol}: {e}")

    def _push_live_pnl_to_redis(self) -> None:
        """
        Builds a per-symbol + portfolio PnL snapshot and writes it to Redis
        so the API server (separate process) can serve it tick-by-tick.
        Key: live_pnl:{session_id}   TTL: 5s (auto-expires if trader dies)
        """
        try:
            sid = getattr(self, "session_id", None) or getattr(self, "ui_session_id", None)
            rc = getattr(self.market_client, "redis_client", None)
            if not sid or rc is None:
                return

            # Snapshot live_pnl and realized_pnl_by_symbol safely
            with self.live_pnl_lock:
                live_snapshot = dict(self.live_pnl)

            realized_by_sym = dict(self.realized_pnl_by_symbol)

            # Build per-symbol response — merge open positions + closed-only symbols
            all_symbols = set(live_snapshot.keys()) | set(realized_by_sym.keys())
            symbols_out = {}
            live_unrealized_total = 0.0
            exited_symbols = set(getattr(self, "_exited_symbols", set()) or set())

            for sym in all_symbols:
                live = live_snapshot.get(sym, {})
                is_exited = sym in exited_symbols or str(live.get("status") or "").upper() == "EXITED"
                unrealized = 0.0 if is_exited else round(float(live.get("pnl", 0.0)), 2)
                realized = round(float(realized_by_sym.get(sym, 0.0)), 2)
                live_unrealized_total += unrealized
                symbols_out[sym] = {
                    "ltp": round(float(live.get("ltp", 0.0)), 2),
                    "unrealized_pnl": unrealized,
                    "realized_pnl": realized,
                    "total_pnl": round(unrealized + realized, 2),
                    "qty": 0 if is_exited else int(live.get("qty", 0)),
                    "entry": round(float(live.get("entry", 0.0)), 2),
                    "side": live.get("side", ""),
                    "position_status": "EXITED" if is_exited else "OPEN",
                    "exit_reason": live.get("exit_reason") if is_exited else None,
                }

            realized_total = round(float(self.realized_pnl), 2)
            live_unrealized_total = round(live_unrealized_total, 2)

            payload = {
                "realized_pnl": realized_total,
                "live_unrealized_pnl": live_unrealized_total,
                "total_pnl": round(realized_total + live_unrealized_total, 2),
                "symbols": symbols_out,
                "ts": time.time(),
            }

            rc.setex(live_pnl_key(sid), 5, json.dumps(payload))

        except Exception as e:
            print(f"[LIVE-PNL] Redis push error: {e}")

    def _check_realized_portfolio_rms(self) -> None:
        """
        Called after every trade close (_close_position).
        Checks realized PnL alone — no live unrealized needed.
        Catches the case where all positions close in one batch and no
        further WS ticks arrive to trigger _check_live_portfolio_rms.
        Mirror of break_1's register_pnl() halt check.
        """
        # Portfolio RMS layer is intentionally disabled.
        # Keep this function available, but never let portfolio loss exit all stocks.
        return

        if self._live_portfolio_rms_inflight or self.rms_triggered:
            return
        if not self.rms_loss_limit or self.rms_loss_limit >= 0:
            return

        total_realized = float(self.realized_pnl or 0.0)
        if total_realized > self.rms_loss_limit:
            return

        self._live_portfolio_rms_inflight = True
        self.rms_triggered = True
        print(
            f"\n[RMS-REALIZED-PORTFOLIO] BREACH: realized PnL Rs{total_realized:.2f} "
            f"<= limit Rs{self.rms_loss_limit:.2f}. Exiting all positions."
        )
        self._csv_logger.write(
            self.rms_events_log,
            [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "REALIZED_PORTFOLIO_BREACH", "ALL",
                f"{total_realized:.2f}", f"{total_realized:.2f}",
                "0.00", f"{self.rms_loss_limit:.2f}",
                "EXIT_ALL realized_only",
            ],
        )
        try:
            self.alerts.notify(
                f"RMS REALIZED PORTFOLIO HALT: realized PnL Rs{total_realized:.2f} "
                f"<= Rs{self.rms_loss_limit:.2f} — exiting all positions"
            )
        except Exception:
            pass

        try:
            with self.positions_lock:
                for sym in list(self.positions.keys()):
                    self._exited_symbols.add(sym)
        except Exception as e:
            print(f"[RMS-REALIZED-PORTFOLIO] mark exited failed: {e}")

        all_syms = set(self.positions.keys())
        for sym in all_syms:
            self._notify_rms_exit_to_api(sym, self.live_pnl.get(sym, {}).get("pnl", 0.0))

        def _do_exit():
            try:
                self._exit_all_positions_and_stop()
                print("Done for the day all positions exitted")
            finally:
                self.stop_event.set()

        threading.Thread(
            target=_do_exit,
            name="RMSRealizedPortfolioExit",
            daemon=True,
        ).start()

    def _check_live_portfolio_rms(self) -> None:
        """
        Sum live_pnl across all symbols. If the running total breaches
        self.rms_loss_limit (already set by the cycle-level RMS init),
        exit every open position. Dedup'd via _live_portfolio_rms_inflight.
        """
        # Portfolio RMS layer is intentionally disabled.
        # Keep this function available, but never let portfolio loss exit all stocks.
        return

        if self._live_portfolio_rms_inflight or self.rms_triggered:
            return
        if not self.rms_loss_limit or self.rms_loss_limit >= 0:
            return  # limit not initialised yet (set in trader.start)

        with self.live_pnl_lock:
            total_live_pnl = sum(float(v.get("pnl") or 0.0) for v in self.live_pnl.values())
            symbols_snapshot = list(self.live_pnl.keys())

        # Realized + currently-unrealized across the portfolio.
        total_realized = float(self.realized_pnl or 0.0)
        total_pnl = total_realized + total_live_pnl

        # Throttled portfolio-PnL log (every ~30s) so we can see RMS headroom.
        now = time.time()
        usage_pct = (total_pnl / self.rms_loss_limit * 100.0) if self.rms_loss_limit else 0.0
        if now - self._last_portfolio_print_ts >= self._portfolio_print_interval_sec:
            self._last_portfolio_print_ts = now
            print(
                f"[RMS-LIVE-PORTFOLIO] total={total_pnl:.2f} "
                f"(realized={total_realized:.2f} + live_unrealized={total_live_pnl:.2f}) "
                f"limit={self.rms_loss_limit:.2f} usage={usage_pct:.1f}% "
                f"open_syms={len(symbols_snapshot)}"
            )
            self._csv_logger.write(
                self.portfolio_pnl_log,
                [
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    f"{total_pnl:.2f}", f"{total_realized:.2f}",
                    f"{total_live_pnl:.2f}", f"{self.rms_loss_limit:.2f}",
                    f"{usage_pct:.2f}", len(symbols_snapshot),
                ],
            )

        if total_pnl > self.rms_loss_limit:
            return

        self._live_portfolio_rms_inflight = True
        self.rms_triggered = True  # gate the cycle-level path so it doesn't double-fire
        print(
            f"\n[RMS-LIVE-PORTFOLIO] BREACH: total PnL Rs{total_pnl:.2f} "
            f"(realized={total_realized:.2f} + unrealized={total_live_pnl:.2f}) "
            f"<= limit Rs{self.rms_loss_limit:.2f}. Exiting all positions."
        )
        self._csv_logger.write(
            self.rms_events_log,
            [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "PORTFOLIO_BREACH", "ALL",
                f"{total_pnl:.2f}", f"{total_realized:.2f}",
                f"{total_live_pnl:.2f}", f"{self.rms_loss_limit:.2f}",
                f"EXIT_ALL syms={len(symbols_snapshot)}",
            ],
        )
        try:
            self.alerts.notify(
                f"RMS LIVE PORTFOLIO HALT: PnL Rs{total_pnl:.2f} "
                f"<= Rs{self.rms_loss_limit:.2f} — exiting all positions"
            )
        except Exception:
            pass

        # Mark every open position as permanently exited so any in-flight
        # cycle skips them on its next iteration.
        try:
            with self.positions_lock:
                for sym in list(self.positions.keys()):
                    self._exited_symbols.add(sym)
        except Exception as e:
            print(f"[RMS-LIVE-PORTFOLIO] mark exited failed: {e}")

        # Notify api_server to clear the symbols payload by reusing the
        # per-ticker drain channel — push every live symbol onto it.
        all_syms = set(symbols_snapshot) | set(self.positions.keys())
        for sym in all_syms:
            self._notify_rms_exit_to_api(sym, self.live_pnl.get(sym, {}).get("pnl", 0.0))

        def _do_exit():
            try:
                self._exit_all_positions_and_stop()
                print("Done for the day all positions exitted")
            finally:
                self.stop_event.set()

        threading.Thread(
            target=_do_exit,
            name="RMSLivePortfolioExit",
            daemon=True,
        ).start()

    def _rms_exit_worker(self, symbol: str) -> None:
        """Run exit_single_position off the WS thread."""
        sym = self._normalize_config_symbol(symbol)
        print(f"[RMS-TICKER] worker start symbol={sym} reason=RMS_TICKER_EXIT")
        try:
            result = self.exit_single_position(sym, exit_reason="RMS_TICKER_EXIT")
            print(
                f"[RMS-TICKER] worker done symbol={sym} "
                f"success={result.get('success')} order_id={result.get('order_id')} "
                f"message={result.get('message')}"
            )
        except Exception as e:
            print(f"[RMS-TICKER] exit failed for {sym}: {e}")
        finally:
            # exit_single_position adds to _exited_symbols on success;
            # drop the inflight marker either way so a retry is possible if it failed.
            self._rms_exit_inflight.discard(sym)

    def _notify_rms_exit_to_api(self, symbol: str, pnl: float) -> None:
        """
        Push the RMS-exited symbol to a Redis list so api_server (different
        process) can prune it from sessions_store[sid]["symbols"].
        """
        try:
            sid = getattr(self, "session_id", None) or getattr(self, "ui_session_id", None)
            rc = getattr(self.market_client, "redis_client", None)
            if not sid or rc is None:
                return
            rc.rpush(
                rms_exited_key(sid),
                json.dumps({"symbol": symbol, "pnl": pnl, "ts": time.time()}),
            )
            rc.expire(rms_exited_key(sid), 86400)
            print(f"[RMS-TICKER] redis notify queued session={sid} symbol={symbol} pnl={float(pnl or 0.0):.2f}")
        except Exception as e:
            print(f"[RMS-TICKER] redis notify failed for {symbol}: {e}")

    def _get_session_id_for_db(self) -> str:
        sid = getattr(self, "ui_session_id", None) or getattr(self, "session_id", None)
        return str(sid or "").strip()

    def _get_source_mode(self) -> str:
        return "live"

    def _get_exit_cycle_numbers(self) -> tuple:
        actual_cycle = int(
            getattr(self, "_cycle_count", 0)
            or getattr(self, "_current_cycle_count", 0)
            or getattr(self, "_restored_cycle_count", 0)
            or 0
        )
        return actual_cycle, max(actual_cycle + 1, 1)

    def _get_exited_symbols_collection(self):
        base = self.trading_logs_collection
        if base is None:
            return None
        db_name = getattr(self, "config_db_name", None) or os.environ.get("MONGO_CONFIG_DB_NAME", "test")
        coll = (
            base.database.client[db_name]["exited_symbols"]
            if db_name != base.database.name
            else base.database["exited_symbols"]
        )
        if not getattr(self, "_exited_symbols_index_ready", False):
            try:
                coll.create_index(
                    [("session_id", 1), ("symbol", 1)],
                    unique=True,
                    name="uniq_session_symbol_exit",
                )
                coll.create_index(
                    [("session_id", 1), ("status", 1)],
                    name="idx_session_exit_status",
                )
            except Exception as exc:
                print(f"[EXITED-SYMBOLS] index ensure failed: {exc}")
            self._exited_symbols_index_ready = True
        return coll

    def _snapshot_position_for_exit(self, symbol: str, fallback: Optional[dict] = None) -> dict:
        sym = self._normalize_config_symbol(symbol)
        with self.positions_lock:
            pos = self.positions.get(sym)
            if pos:
                return {
                    "symbol": sym,
                    "side": pos.get("side"),
                    "qty": int(pos.get("qty") or 0),
                    "entry_price": float(pos.get("entry_price") or 0.0),
                    "ltp": float(pos.get("ltp") or 0.0),
                }

        with self.broker_pos_lock:
            for pos in self._broker_positions_cache or []:
                if self._normalize_config_symbol(pos.get("symbol")) == sym:
                    return {
                        "symbol": sym,
                        "side": pos.get("side"),
                        "qty": int(pos.get("qty") or 0),
                        "entry_price": float(pos.get("avg_price") or pos.get("entry_price") or 0.0),
                        "ltp": float(pos.get("ltp") or 0.0),
                    }

        fallback = fallback or {}
        return {
            "symbol": sym,
            "side": fallback.get("side"),
            "qty": int(fallback.get("qty") or 0),
            "entry_price": float(fallback.get("entry_price") or fallback.get("avg_price") or 0.0),
            "ltp": float(fallback.get("ltp") or 0.0),
        }

    def _exit_action_label(self, exit_reason: str) -> str:
        labels = {
            "RMS_TICKER_EXIT": "RMS EXITED",
            "STOP_LOSS_EXIT": "STOP LOSS EXITED",
            "STOP_LOCK_EXIT": "STOP LOCK EXITED",
            "MANUAL_EXIT": "MANUAL EXITED",
            "PORTFOLIO_RMS_EXIT": "PORTFOLIO RMS EXITED",
        }
        return labels.get(exit_reason or "", "EXITED")

    def _resolve_exit_reason(self, action_type: str, ctx: Optional[dict] = None) -> str:
        ctx = ctx or {}
        explicit = ctx.get("exit_reason")
        if explicit:
            return str(explicit).upper()
        if action_type == "STOP_LOSS":
            return "STOP_LOSS_EXIT"
        if action_type in ("EXIT_LONG", "COVER_SHORT"):
            return "MANUAL_EXIT"
        return "EXIT"

    def _clear_symbol_live_pnl_after_exit(
        self,
        symbol: str,
        *,
        exit_price: float = 0.0,
        entry_price: float = 0.0,
        side: str = "",
        exit_reason: str = "",
    ) -> None:
        sym = self._normalize_config_symbol(symbol)
        self._exited_symbols.add(sym)
        with self.live_pnl_lock:
            old = self.live_pnl.get(sym, {})
            self.live_pnl[sym] = {
                "ltp": float(exit_price or old.get("ltp") or 0.0),
                "pnl": 0.0,
                "qty": 0,
                "entry": float(entry_price or old.get("entry") or 0.0),
                "side": side or old.get("side", ""),
                "ts": time.time(),
                "status": "EXITED",
                "exit_reason": exit_reason,
            }
        print(
            f"[EXITED-SYMBOLS] live pnl cleared session={self._get_session_id_for_db()} "
            f"symbol={sym} reason={exit_reason} exit_price={float(exit_price or 0.0):.2f}"
        )

    def _persist_exit_pending(
        self,
        symbol: str,
        *,
        exit_reason: str,
        exit_side: str,
        qty: int,
        entry_price: float,
        exit_order_id: Optional[str],
    ) -> None:
        sid = self._get_session_id_for_db()
        coll = self._get_exited_symbols_collection()
        if not sid or coll is None:
            print(
                f"[EXITED-SYMBOLS] pending persist skipped symbol={symbol} "
                f"session_present={bool(sid)} collection_present={coll is not None}"
            )
            return

        actual_cycle, display_cycle = self._get_exit_cycle_numbers()
        now_market = self._now_market_time()
        now_utc = datetime.now(timezone.utc)
        sym = self._normalize_config_symbol(symbol)
        try:
            existing = coll.find_one({"session_id": sid, "symbol": sym}, {"status": 1})
            if existing and str(existing.get("status") or "").upper() in {"EXITED", "RMS_EXITED", "CLOSED"}:
                return
        except Exception:
            pass

        pending_unrealized = self._get_symbol_unrealized_pnl(sym)
        doc = {
            "session_id": sid,
            "configuration_id": getattr(self, "configuration_id", None),
            "symbol": sym,
            "status": "EXIT_PENDING",
            "position_status": "EXIT_PENDING",
            "exit_reason": exit_reason,
            "entry_side": "BUY" if exit_side == "SELL" else "SELL",
            "exit_side": exit_side,
            "qty": int(qty or 0),
            "entry_price": round(float(entry_price or 0.0), 2),
            "exit_price": None,
            "realized_pnl": 0.0,
            "unrealized_pnl": pending_unrealized,
            "total_pnl": pending_unrealized,
            "exit_actual_cycle": actual_cycle,
            "display_cycle": display_cycle,
            "exit_order_id": exit_order_id,
            "exit_order_status": "PENDING",
            "source": self._get_source_mode(),
            "exit_time": None,
            "exit_time_utc": None,
            "updated_at": now_utc.isoformat(),
        }
        try:
            coll.update_one(
                {"session_id": sid, "symbol": sym},
                {"$set": doc, "$setOnInsert": {"created_at": now_utc.isoformat(), "requested_at": now_market.strftime("%Y-%m-%d %H:%M:%S")}},
                upsert=True,
            )
            print(
                f"[EXITED-SYMBOLS] pending saved session={sid} symbol={sym} "
                f"reason={exit_reason} order_id={exit_order_id} "
                f"actual_cycle={actual_cycle} display_cycle={display_cycle} "
                f"unrealized={pending_unrealized:.2f}"
            )
        except Exception as exc:
            print(f"[EXITED-SYMBOLS] pending persist failed for {sym}: {exc}")

    def _persist_exit_trading_log(self, exit_doc: dict) -> None:
        coll = self.trading_logs_collection
        if coll is None:
            print(f"[TRADING-LOGS] exit row skipped symbol={exit_doc.get('symbol')} collection_present=False")
            return

        sid = exit_doc.get("session_id")
        sym = exit_doc.get("symbol")
        if not sid or not sym:
            print(f"[TRADING-LOGS] exit row skipped missing session/symbol session={sid} symbol={sym}")
            return

        display_cycle = int(exit_doc.get("display_cycle") or 1)
        realized = round(float(exit_doc.get("realized_pnl") or 0.0), 2)
        total = round(float(exit_doc.get("total_pnl") or realized), 2)
        timestamp = exit_doc.get("exit_time") or self._now_market_time().strftime("%Y-%m-%d %H:%M:%S")
        portfolio_unrealized = round(float(getattr(self, "unrealized_pnl", 0.0) or 0.0), 2)
        portfolio_realized = round(float(getattr(self, "realized_pnl", 0.0) or 0.0), 2)
        portfolio_pnl = round(portfolio_realized + portfolio_unrealized, 2)
        total_equity = round(float(self.cash_balance or 0.0) + portfolio_realized + portfolio_unrealized, 2)

        row = {
            "session_id": sid,
            "configuration_id": exit_doc.get("configuration_id"),
            "cycle": display_cycle,
            "timestamp": timestamp,
            "simulation_logs": bool(getattr(self, "simulation_logs", False)),
            "symbol": sym,
            "curr_price": exit_doc.get("exit_price"),
            "return_pct": 0.0,
            "trajectory_pct": None,
            "side": exit_doc.get("entry_side"),
            "signal": None,
            "action": self._exit_action_label(exit_doc.get("exit_reason")),
            "qty": int(exit_doc.get("qty") or 0),
            "unrealized_pnl": 0.0,
            "symbol_unrealized_pnl": 0.0,
            "symbol_realized_pnl": realized,
            "exit_realized_pnl": exit_doc.get("exit_realized_pnl", realized),
            "symbol_pnl": total,
            "cash_balance": round(float(self.cash_balance or 0.0), 2),
            "realized_pnl": portfolio_realized,
            "pnl": total,
            "total_equity": total_equity,
            "portfolio_cash_balance": round(float(self.cash_balance or 0.0), 2),
            "portfolio_realized_pnl": portfolio_realized,
            "portfolio_unrealized_pnl": portfolio_unrealized,
            "portfolio_pnl": portfolio_pnl,
            "portfolio_total_equity": total_equity,
            "is_exit_row": True,
            "source": "exited_symbols",
            "status": exit_doc.get("status"),
            "position_status": exit_doc.get("position_status") or exit_doc.get("status"),
            "exit_reason": exit_doc.get("exit_reason"),
            "exit_side": exit_doc.get("exit_side"),
            "entry_price": exit_doc.get("entry_price"),
            "exit_price": exit_doc.get("exit_price"),
            "exit_order_id": exit_doc.get("exit_order_id"),
            "exit_order_status": exit_doc.get("exit_order_status"),
            "exit_actual_cycle": exit_doc.get("exit_actual_cycle"),
            "display_cycle": display_cycle,
            "exit_time": exit_doc.get("exit_time"),
            "exit_time_utc": exit_doc.get("exit_time_utc"),
        }
        try:
            coll.update_one(
                {"session_id": sid, "symbol": sym, "is_exit_row": True},
                {"$set": row, "$setOnInsert": {"created_at": datetime.now(timezone.utc).isoformat()}},
                upsert=True,
            )
            print(
                f"[TRADING-LOGS] exit row upserted session={sid} symbol={sym} "
                f"cycle={display_cycle} actual_cycle={exit_doc.get('exit_actual_cycle')} "
                f"reason={exit_doc.get('exit_reason')} realized={realized:.2f} "
                f"unrealized=0.00 order_id={exit_doc.get('exit_order_id')}"
            )
        except Exception as exc:
            print(f"[TRADING-LOGS] exit row persist failed for {sym}: {exc}")

    def _persist_symbol_exit(
        self,
        symbol: str,
        *,
        exit_reason: str,
        action_type: str,
        exit_side: str,
        qty: int,
        entry_price: float,
        exit_price: float,
        realized_pnl: float,
        exit_order_id: Optional[str],
        exit_order_status: str = "FILLED",
    ) -> dict:
        sid = self._get_session_id_for_db()
        actual_cycle, display_cycle = self._get_exit_cycle_numbers()
        sym = self._normalize_config_symbol(symbol)
        now_market = self._now_market_time()
        now_utc = datetime.now(timezone.utc)
        exit_realized_pnl = round(float(realized_pnl or 0.0), 2)
        cumulative_realized_pnl = round(
            float(self.realized_pnl_by_symbol.get(sym, exit_realized_pnl) or exit_realized_pnl),
            2,
        )
        total_pnl = cumulative_realized_pnl
        entry_side = "BUY" if exit_side == "SELL" else "SELL"
        doc = {
            "session_id": sid,
            "configuration_id": getattr(self, "configuration_id", None),
            "symbol": sym,
            "status": "EXITED",
            "position_status": "EXITED",
            "exit_reason": exit_reason,
            "action_type": action_type,
            "entry_side": entry_side,
            "exit_side": exit_side,
            "qty": int(qty or 0),
            "entry_price": round(float(entry_price or 0.0), 2),
            "exit_price": round(float(exit_price or 0.0), 2),
            "exit_realized_pnl": exit_realized_pnl,
            "realized_pnl": cumulative_realized_pnl,
            "unrealized_pnl": 0.0,
            "total_pnl": total_pnl,
            "exit_actual_cycle": actual_cycle,
            "display_cycle": display_cycle,
            "exit_time": now_market.strftime("%Y-%m-%d %H:%M:%S"),
            "exit_time_utc": now_utc.isoformat(),
            "source": self._get_source_mode(),
            "exit_order_id": exit_order_id,
            "exit_order_status": exit_order_status,
            "updated_at": now_utc.isoformat(),
        }

        coll = self._get_exited_symbols_collection()
        if sid and coll is not None:
            try:
                coll.update_one(
                    {"session_id": sid, "symbol": sym},
                    {"$set": doc, "$setOnInsert": {"created_at": now_utc.isoformat()}},
                    upsert=True,
                )
                print(
                    f"[EXITED-SYMBOLS] final saved session={sid} symbol={sym} "
                    f"reason={exit_reason} status=EXITED order_id={exit_order_id} "
                    f"entry={entry_price:.2f} exit={exit_price:.2f} qty={int(qty or 0)} "
                    f"exit_realized={exit_realized_pnl:.2f} cumulative_realized={cumulative_realized_pnl:.2f} "
                    f"display_cycle={display_cycle}"
                )
            except Exception as exc:
                print(f"[EXITED-SYMBOLS] persist failed for {sym}: {exc}")
        else:
            print(
                f"[EXITED-SYMBOLS] final persist skipped symbol={sym} "
                f"session_present={bool(sid)} collection_present={coll is not None}"
            )

        self._persist_exit_trading_log(doc)
        return doc

    def _get_exited_symbol_doc(self, symbol: str) -> Optional[dict]:
        sid = self._get_session_id_for_db()
        coll = self._get_exited_symbols_collection()
        sym = self._normalize_config_symbol(symbol)
        if not sid or coll is None or not sym:
            return None
        try:
            doc = coll.find_one({"session_id": sid, "symbol": sym})
            if doc and str(doc.get("status") or "").upper() in {"EXITED", "RMS_EXITED", "CLOSED"}:
                print(
                    f"[EXITED-SYMBOLS] found closed symbol session={sid} symbol={sym} "
                    f"status={doc.get('status')} reason={doc.get('exit_reason')} "
                    f"realized={doc.get('realized_pnl')} unrealized={doc.get('unrealized_pnl')}"
                )
                return doc
        except Exception as exc:
            print(f"[EXITED-SYMBOLS] lookup failed for {sym}: {exc}")
        return None

    def _sync_exited_symbols_from_db(self) -> None:
        sid = self._get_session_id_for_db()
        coll = self._get_exited_symbols_collection()
        if not sid or coll is None:
            return
        try:
            cursor = coll.find(
                {"session_id": sid, "status": {"$in": ["EXIT_PENDING", "EXITED", "RMS_EXITED", "CLOSED"]}},
                {"symbol": 1},
            )
            synced = set()
            for doc in cursor:
                sym = self._normalize_config_symbol(doc.get("symbol"))
                if sym:
                    synced.add(sym)
            if synced:
                before = len(self._exited_symbols)
                self._exited_symbols.update(synced)
                if len(self._exited_symbols) > before:
                    print(f"[EXITED-SYMBOLS] Synced from DB: {sorted(synced)}")
        except Exception as exc:
            print(f"[EXITED-SYMBOLS] sync failed: {exc}")

    def _finalize_symbol_exit_fill(self, symbol: str, broker_pos: dict, ctx: dict) -> dict:
        sym = self._normalize_config_symbol(symbol)
        action_type = ctx.get("action_type", "")
        exit_reason = self._resolve_exit_reason(action_type, ctx)
        exit_price = float(broker_pos.get("avg_price") or 0.0)
        exit_qty = int(broker_pos.get("qty") or ctx.get("qty") or 0)
        exit_side = (ctx.get("side") or broker_pos.get("side") or "").upper()
        pre_exit = ctx.get("pre_exit_position") or self._snapshot_position_for_exit(sym)
        entry_price = float(pre_exit.get("entry_price") or 0.0)
        entry_side = pre_exit.get("side") or ("BUY" if exit_side == "SELL" else "SELL")
        print(
            f"[EXIT-FINALIZE] start symbol={sym} action={action_type} reason={exit_reason} "
            f"entry_side={entry_side} exit_side={exit_side} qty={exit_qty} "
            f"entry={entry_price:.2f} exit={exit_price:.2f} order_id={ctx.get('order_id')}"
        )

        pnl = self._close_position(
            self.session,
            sym,
            exit_price,
            exit_qty,
            position_snapshot=pre_exit,
        )
        print(f"[P&L REALIZED] {sym} | Action: {action_type} | Reason: {exit_reason} | Realized: {pnl:.2f}")

        self._log_trade(
            sym,
            action_type,
            0.0,
            "closed",
            exit_price,
            exit_qty,
            pnl,
        )

        remaining_qty = self._get_symbol_position_qty(sym)
        if remaining_qty <= 0:
            self._clear_symbol_live_pnl_after_exit(
                sym,
                exit_price=exit_price,
                entry_price=entry_price,
                side=entry_side,
                exit_reason=exit_reason,
            )

        exit_doc = self._persist_symbol_exit(
            sym,
            exit_reason=exit_reason,
            action_type=action_type,
            exit_side=exit_side,
            qty=exit_qty,
            entry_price=entry_price,
            exit_price=exit_price,
            realized_pnl=pnl,
            exit_order_id=ctx.get("order_id"),
            exit_order_status="FILLED",
        )
        print(
            f"[EXIT-FINALIZE] done symbol={sym} reason={exit_reason} "
            f"realized={exit_doc.get('realized_pnl')} unrealized={exit_doc.get('unrealized_pnl')} "
            f"trading_log_cycle={exit_doc.get('display_cycle')}"
        )
        return exit_doc

    def _notify_eod_exit_status_to_api(self, reason: str = "MARKET_CLOSE_15:00_IST") -> None:
        """
        Publish EOD exit status to Redis so api_server (separate process) can
        serve accurate exit_initiated / exit_time on /api/trading/exit-status.
        """
        try:
            sid = getattr(self, "session_id", None) or getattr(self, "ui_session_id", None)
            rc = getattr(self.market_client, "redis_client", None)
            if not sid or rc is None:
                print("[EOD] exit_status redis notify skipped (no session_id or redis)")
                return
            exit_time = self._now_market_time().isoformat()
            payload = {
                "exit_initiated": True,
                "exit_time": exit_time,
                "reason": reason,
                "ts": time.time(),
            }
            key = exit_status_key(sid)
            rc.setex(key, 86400, json.dumps(payload))
            print(f"[EOD] exit_status published to redis key={key} reason={reason}")
        except Exception as e:
            print(f"[EOD] exit_status redis notify failed: {e}")

    # ---------- TIME HELPERS ----------
    
    def _now_market_time(self):
        return datetime.now(MARKET_TZ)
    
    # ---------- SYMBOL LOCK (thread-safe exposure updates) -----------

    def _get_symbol_lock(self, symbol):
        t0 = time.time()

        if symbol not in self.symbol_locks:
            self.symbol_locks.setdefault(symbol, threading.Lock())

        elapsed = time.time() - t0

        print(f"[TRACE] SYMBOL_LOCK {symbol}: {elapsed:.6f}s")
        self.tlog.record("symbol lock timing ", t0, note=symbol)
        return self.symbol_locks[symbol]

    # ---------- RESERVED (PENDING) EXPOSURE READ --------------
    
    def _reserved_exposure(self, symbol):
        return self.reserved_exposure.get(symbol, 0.0)

    @staticmethod
    def _coerce_leverage_multiplier(value, source: str = "") -> Optional[float]:
        if value is None:
            return None
        try:
            mult = float(value)
            if mult > 0:
                return mult
            print(f"[LEVERAGE] Ignoring non-positive {source}: {value!r}")
        except (TypeError, ValueError):
            print(f"[LEVERAGE] Invalid {source}: {value!r}")
        return None

    def _normalize_configuration_root(self, config_doc: dict) -> dict:
        root = config_doc.get("configuration", config_doc)
        if isinstance(root, str):
            try:
                root = json.loads(root)
            except Exception as exc:
                print(f"[LEVERAGE] configuration JSON parse failed: {exc}")
                return {}
        if not isinstance(root, dict):
            return {}
        return root

    def _leverage_from_config_doc(self, config_doc: dict) -> Optional[float]:
        if not isinstance(config_doc, dict):
            return None
        for key in ("leverage_multiplier", "leverage", "pyramid_leverage_multiplier"):
            val = self._coerce_leverage_multiplier(
                config_doc.get(key), f"SavedTradingConfiguration.{key}"
            )
            if val is not None:
                return val
        root = self._normalize_configuration_root(config_doc)
        for key in ("leverage_multiplier", "leverage", "pyramid_leverage_multiplier"):
            val = self._coerce_leverage_multiplier(root.get(key), f"configuration.{key}")
            if val is not None:
                return val
        return None

    def _fetch_saved_trading_configuration_for_leverage(self) -> Optional[dict]:
        configuration_id = getattr(self, "configuration_id", None)
        coll = self.trading_logs_collection
        if not configuration_id or coll is None:
            return None

        config_db_name = getattr(self, "config_db_name", None) or os.environ.get(
            "MONGO_CONFIG_DB_NAME", "test"
        )
        collection_names = (
            "savedtradingconfigurations",
            "SavedTradingConfiguration",
        )
        mongo_client = coll.database.client
        config_db = mongo_client[config_db_name]

        queries = [{"configuration_id": configuration_id}]
        try:
            from bson import ObjectId

            if ObjectId.is_valid(configuration_id):
                oid = ObjectId(configuration_id)
                queries.insert(0, {"_id": oid})
                queries.append({"configuration_id": str(oid)})
        except Exception:
            pass

        for coll_name in collection_names:
            if coll_name not in config_db.list_collection_names():
                continue
            config_coll = config_db[coll_name]
            for query in queries:
                try:
                    doc = config_coll.find_one(query)
                except Exception as exc:
                    print(f"[LEVERAGE] SavedTradingConfiguration lookup failed: {exc}")
                    continue
                if doc:
                    return doc
        return None

    def _resolve_leverage_multiplier(self, config_doc=None, default: float = 4.0) -> float:
        """
        Resolve intraday leverage: session/request -> SavedTradingConfiguration -> default.
        Live exposure cap defaults to 4.0 (legacy behavior) when unset.
        """
        val = self._coerce_leverage_multiplier(
            getattr(self, "leverage_multiplier", None), "session"
        )
        if val is not None:
            self.leverage_multiplier = val
            return val

        doc = config_doc
        if doc is None:
            doc = self._fetch_saved_trading_configuration_for_leverage()

        if doc:
            val = self._leverage_from_config_doc(doc)
            if val is not None:
                self.leverage_multiplier = val
                print(f"[LEVERAGE] Using leverage_multiplier={val} from SavedTradingConfiguration")
                return val

        self.leverage_multiplier = float(default)
        print(f"[LEVERAGE] Falling back to leverage_multiplier={default}")
        return float(default)

    # ---------- TOTAL SYMBOL EXPOSURE (FILLED + RESERVED) -------------
    
    def _total_symbol_exposure(self, symbol):
        return self._stock_exposure(symbol) + self._reserved_exposure(symbol)

    # ---------- EXPOSURE CAP CHECK (ATOMIC) ----------------

    def _can_reserve_exposure(self, symbol, order_value):
        t0 = time.time()
        leverage_mult = self._resolve_leverage_multiplier(default=4.0)
        leveraged_capital = leverage_mult * self.initial_capital

        result = (self._total_symbol_exposure(symbol) + order_value) <= (self.max_exposure_pct * leveraged_capital)

        elapsed = time.time() - t0

        print(f"[TRACE] CAN_RESERVE {symbol}: {elapsed:.6f}s")

        self.tlog.record("CAN_RESERVE_EXPOSURE", t0, note=symbol)

        return result

    # ---------- RESERVE EXPOSURE (BEFORE ORDER SUBMIT) -------------

    def _reserve_exposure(self, symbol, order_value):
        t0 = time.time()

        self.reserved_exposure[symbol] = (
            self.reserved_exposure.get(symbol, 0.0) + order_value
        )

        elapsed = time.time() - t0

        print(f"[TRACE] RESERVE_EXPOSURE {symbol}: {elapsed:.6f}s")

        self.tlog.record("RESERVE_EXPOSURE", t0, note=symbol)

    # ---------- RELEASE RESERVED EXPOSURE (AFTER RESULT) --------------
    
    def _release_exposure(self, symbol, order_value):
        if symbol in self.reserved_exposure:
            self.reserved_exposure[symbol] -= order_value
            if self.reserved_exposure[symbol] <= 0:
                self.reserved_exposure.pop(symbol, None)

    # ---------- STOCK EXPOSURE ------------
    
    def _stock_exposure(self, symbol):
        with self.broker_pos_lock:
            broker_positions = list(self._broker_positions_cache or [])

        for p in broker_positions:
            if p["symbol"] == symbol:
                qty = abs(p.get("qty", 0))
                avg = p.get("avg_price", 0.0)

                if qty <= 0 or avg <= 0:
                    return 0.0

                return qty * avg

        return 0.0

    def _get_fill_price_from_orderbook(self, order_id, symbol):
        try:
            ob = self.session["obj"].orderBook()

            if not isinstance(ob, dict):
                return 0.0
            if not ob.get("status"):
                return 0.0

            orders = ob.get("data", [])
            if not isinstance(orders, list):
                return 0.0

            for order in orders:
                if str(order.get("orderid")) != str(order_id):
                    continue

                #  Pehle orderstatus check karo
                order_status = order.get("orderstatus", "").lower()

                if order_status == "cancelled":
                    print(f"[FILL PRICE] {symbol}: order {order_id} CANCELLED hai  fill price nahi milegi")
                    return 0.0

                if order_status == "rejected":
                    print(f"[FILL PRICE] {symbol}: order {order_id} REJECTED hai  fill price nahi milegi")
                    return 0.0

                if order_status not in ("complete", "filled"):
                    # open, pending, trigger pending etc.
                    print(f"[FILL PRICE] {symbol}: order {order_id} abhi {order_status} hai  wait karo")
                    return 0.0

                #  Order complete hai  ab averageprice lo
                # fill_price = float(order.get("averageprice") or 0.0)

                #  Order complete hai  ab averageprice lo
                # fill_price = float(order.get("price") or 0.0)
                # avg_price_field = float(order.get("averageprice") or 0.0)

                #  averageprice = actual execution price (AngelOne dashboard bhi yahi use karta hai)
                avg_price_field = float(order.get("averageprice") or 0.0)
                fill_price = float(order.get("price") or 0.0)
            
                print(
                    f"[FILL PRICE] {symbol}: order {order_id} | "
                    f"price={fill_price:.2f} | averageprice={avg_price_field:.2f}"
                )

                #  filledshares bhi check karo
                filled_shares = int(order.get("filledshares") or 0)

                # if fill_price > 0 and filled_shares > 0:
                #         print(f"[FILL PRICE] {symbol}: order {order_id} complete @ Ãƒâ€šÃ‚Â¹{fill_price:.2f} (price field) | averageprice=Ãƒâ€šÃ‚Â¹{avg_price_field:.2f} ({filled_shares} shares)")
                #         return fill_price
                # elif avg_price_field > 0 and filled_shares > 0:
                #         print(f"[FILL PRICE] {symbol}: price=0 fallback to averageprice=Ãƒâ€šÃ‚Â¹{avg_price_field:.2f}")
                #         return avg_price_field
                # else:
                #         print(f"[FILL PRICE] {symbol}: order complete but both price=0 and averageprice=0 or filledshares=0")
                #         return 0.0
                if avg_price_field > 0 and filled_shares > 0:
                    print(f"[FILL PRICE] {symbol}: averageprice=Ãƒâ€šÃ‚Â¹{avg_price_field:.2f} ({filled_shares} shares)")
                    return avg_price_field
                elif fill_price > 0 and filled_shares > 0:
                    print(f"[FILL PRICE] {symbol}: averageprice=0, fallback to price=Ãƒâ€šÃ‚Â¹{fill_price:.2f}")
                    return fill_price
                else:
                    print(f"[FILL PRICE] {symbol}: both 0 or filledshares=0")
                    return 0.0

            print(f"[FILL PRICE] {symbol}: order {order_id} order book mein nahi mila")
            return 0.0

        except Exception as e:
            print(f"[FILL PRICE ERROR] {symbol}: {e}")
            return 0.0   
    
    def _track_engine_fill(self, symbol, broker_pos, ctx) -> None:
        record_engine_order_for_trader(self, ctx.get("order_id"))
        apply_fill_to_session_ledger(
            self,
            symbol,
            int(broker_pos.get("qty", 0) or 0),
            ctx.get("action_type", ""),
        )

    # ------- HANDLE FILLED -------- 

    def _handle_filled(self, symbol, broker_pos, ctx):
        """
        Jab order fill confirm ho jaaye tab ye function call hota hai.

        Entry pe   self.positions mein position daalo (sahi entry price ke saath)
        Exit pe    P&L calculate karo, phir position hatao
        """
        action_type = ctx.get("action_type", "")

        # Agar qty hi nahi hai toh kuch mat karo
        if broker_pos["qty"] <= 0:
            return

        # ================================================================
        # EXIT ACTIONS Pehle P&L calculate karo, phir position hatao
        # ================================================================
        if action_type in {
            "EXIT_LONG",
            "COVER_SHORT",
            "STOP_LOSS",
            "MARKET_CLOSE_EXIT",
            "FLIP_TO_LONG",
            "FLIP_TO_SHORT"
        }:
            # Step 1: Exit price lo broker_pos se aayegi (reconciliation ne set ki hogi)
            exit_price = float(broker_pos.get("avg_price") or 0.0)

            # Step 2: P&L calculate karo (self.positions abhi bhi exist karti hai)
            if (
                action_type in {"EXIT_LONG", "COVER_SHORT", "STOP_LOSS"}
                and exit_price > 0
                and (symbol in self.positions or ctx.get("pre_exit_position"))
            ):
                exit_qty = broker_pos.get("qty", 0)
                self._finalize_symbol_exit_fill(
                    symbol,
                    {
                        "side": ctx.get("side") or broker_pos.get("side"),
                        "qty": exit_qty,
                        "avg_price": exit_price,
                    },
                    ctx,
                )
                self._track_engine_fill(symbol, broker_pos, ctx)
                return

            if exit_price > 0 and symbol in self.positions:
                exit_qty = broker_pos.get("qty", 0)
                pnl = self._close_position(self.session, symbol, exit_price, exit_qty)
                print(f"[P&L REALIZED] {symbol} | Action: {action_type} | Realized: {pnl:.2f}")
                self._log_trade(
                    symbol,
                    action_type,
                    0.0,
                    "closed",
                    exit_price,
                    exit_qty,
                    pnl
                )
                self._track_engine_fill(symbol, broker_pos, ctx)
                return
            elif action_type in ("EXIT_LONG", "COVER_SHORT", "STOP_LOSS", "MARKET_CLOSE_EXIT"):
                # Missing price/position for normal exit
                print(f"[WARN] {symbol}: exit price nahi mili ya position exist nahi karti sirf pop kar rahe hain")
                self.positions.pop(symbol, None)
                self._track_engine_fill(symbol, broker_pos, ctx)
                return

            return

        # ================================================================
        # ENTRY ACTIONS  Position save karo sahi entry price ke saath
        # ================================================================

        #  Step 1: Broker se jo avg_price aaya wo lo
        entry_price = float(broker_pos.get("avg_price") or 0.0)

        #  Step 2: Agar broker ne 0 diya (same candle issue) toh
        #    order book se actual fill price nikalo
        if entry_price <= 0:
            order_id = ctx.get("order_id")
            if order_id:
                entry_price = self._get_fill_price_from_orderbook(order_id, symbol)
                if entry_price > 0:
                    print(f"[ENTRY PRICE] {symbol}: order book se mili Ãƒâ€šÃ‚Â¹{entry_price:.2f}")

        #  Step 3: Order book se bhi nahi mili toh position save mat karo
        #    Next reconciliation cycle mein phir try hoga
        if entry_price <= 0:
            print(f"[WARN] {symbol}: entry price nahi mili  position set nahi hua, next cycle mein retry hoga")
            return

        #  Step 4: Sahi entry price ke saath position save karo
        # self.positions[symbol] = {
        #     "side": broker_pos["side"],
        #     "qty": broker_pos["qty"],
        #     "entry_price": entry_price        #  actual fill price 
        # }
        with self.positions_lock:
            if symbol in self.positions:
                old_qty = self.positions[symbol]["qty"]
                old_avg = self.positions[symbol]["entry_price"]
                new_qty = broker_pos["qty"]
                
                blended_avg = ((old_qty * old_avg) + (new_qty * entry_price)) / (old_qty + new_qty)
                self.positions[symbol]["entry_price"] = blended_avg
                self.positions[symbol]["qty"] += new_qty
            else:
                self.positions[symbol] = {
                    "side": broker_pos["side"],
                    "qty": broker_pos["qty"],
                    "entry_price": entry_price
                }

        print(
            f"[POSITION SET] {symbol}: "
            f"{broker_pos['side']} {broker_pos['qty']} @ Ãƒâ€šÃ‚Â¹{entry_price:.2f}"
        )
        self._track_engine_fill(symbol, broker_pos, ctx)

    # -------- HANDLE REJECTED ---------

    def _handle_rejected(self, symbol, ctx):
        print(f" {symbol}: ORDER REJECTED / CANCELLED ({ctx.get('action_type')})")

    # ---------- BROKER / SESSION ----------

    def _link_broker(self, force_relink=False):
        if not force_relink and getattr(self, "broker", None) and getattr(self, "session", None):
            return

        if getattr(self, "broker", None) is None or force_relink:
            self.broker = BrokerConnector()

        try:
            self.session = self.broker.get_session()
        except Exception as e:
            self.session = None
            raise

    def _ensure_session(self):
        """Ensure self.session is present and valid; attempt relink if missing."""
        if getattr(self, "session", None) and isinstance(self.session, dict) and "obj" in self.session:
            return True
        try:
            self._link_broker(force_relink=True)
            return True
        except Exception as e:
            self.alerts.notify(f"Failed to (re)link broker session: {e}")
            return False
    
    # ==================== PARALLEL EXECUTOR HELPER ====================
    def _ensure_parallel_executor(self):
        """Ensure parallel executor is initialized with current session"""
        if self.parallel_executor is None and hasattr(self, 'session') and self.session:
            self.parallel_executor = ParallelOrderExecutor(
                broker=self.broker,
                session=self.session,
                max_workers=5,
                order_rate_limit=10,
                order_rate_window=1.0,
                status_rate_limit=20,
                status_rate_window=1.0
            )
            self.parallel_executor.start()
            print("[INFO] Parallel order executor initialized with 4 workers")
            print("[INFO]  Rate limits: 8 orders/sec, 18 status checks/sec")
    # ==================================================================
    
    def _seed_realized_pnl_from_broker(self):
        """
        On a fresh session start, pull today's already-realized PnL from the
        broker so the RMS layers don't believe the day starts at zero.

        Why: self.realized_pnl and self.realized_pnl_by_symbol are in-memory
        only — every new AutoTrader instance resets them. If a user stops and
        restarts mid-day, the previous day's closed-trade losses/profits are
        invisible to RMS unless we re-seed from the broker's position book.

        Source of truth:
          - Per-symbol: position book entries (open AND closed positions both
            carry a 'realised' field with today's realized PnL for that name).
          - Total cross-check: rmsLimit -> data.m2mrealized.
        """
        seeded_total = 0.0
        seeded_by_symbol: dict = {}

        try:
            resp = self.broker.get_positions(self.session)
            if (isinstance(resp, dict) and resp.get("status")
                    and isinstance(resp.get("raw"), dict)):
                data = resp["raw"].get("data") or []
                if isinstance(data, list):
                    for p in data:
                        try:
                            sym = (p.get("tradingsymbol") or "").replace("-EQ", "").upper()
                            if not sym:
                                continue
                            # Angel returns 'realised' (sometimes 'realized' or 'pnl')
                            r = (p.get("realised")
                                 or p.get("realized")
                                 or p.get("pnl")
                                 or 0.0)
                            r = float(r or 0.0)
                            if r != 0.0:
                                seeded_by_symbol[sym] = seeded_by_symbol.get(sym, 0.0) + r
                                seeded_total += r
                        except Exception as inner:
                            print(f"[SEED-REALIZED] skip row: {inner}")
                            continue
        except Exception as e:
            print(f"[SEED-REALIZED] get_positions failed: {e}")

        # Cross-check vs rmsLimit (broker's own aggregate)
        broker_total = None
        try:
            ab = self.broker.get_account_balance(self.session)
            if isinstance(ab, dict) and ab.get("status") == "success":
                broker_total = float(ab.get("m2m_realized") or 0.0)
        except Exception as e:
            print(f"[SEED-REALIZED] rmsLimit cross-check failed: {e}")

        # Apply
        for sym, val in seeded_by_symbol.items():
            self.realized_pnl_by_symbol[sym] = val
        self.realized_pnl = seeded_total

        print(
            f"[SEED-REALIZED] today's realized PnL loaded from broker: "
            f"total=Rs{seeded_total:.2f} per_symbol={dict(seeded_by_symbol)} "
            f"broker_m2m_realized=Rs{broker_total if broker_total is not None else 'N/A'}"
        )
        if broker_total is not None and abs(broker_total - seeded_total) > 1.0:
            print(
                f"[SEED-REALIZED] WARNING: per-symbol sum (Rs{seeded_total:.2f}) "
                f"differs from broker total (Rs{broker_total:.2f}) by "
                f"Rs{broker_total - seeded_total:.2f} — using per-symbol sum"
            )

    def _get_broker_positions(self):
        try:
            resp = self.broker.get_positions(self.session)

            if not isinstance(resp, dict) or not resp.get("status"):
                return []

            raw = resp.get("raw")
            if not isinstance(raw, dict):
                return []

            data = raw.get("data")
            if not isinstance(data, list):
                return []

            positions = []

            for p in data:
                try:
                    # Exit path must never square off CNC/DELIVERY/carry positions
                    product = str(p.get("producttype") or p.get("productType") or "").upper()
                    if product not in ("INTRADAY", "MIS"):
                        continue

                    net_qty = int(p.get("netqty", 0))
                    if net_qty == 0:
                        continue

                    symbol = (
                        p.get("tradingsymbol", "")
                        .replace("-EQ", "")
                        .upper()
                    )

                    side = "BUY" if net_qty > 0 else "SELL"

                    positions.append({
                        "symbol": symbol,
                        "side": side,
                        "qty": abs(net_qty),
                        "product_type": product or "INTRADAY",
                        "avg_price": float(
                            p.get("averageprice")
                            or p.get("avg_price")
                            or 0.0
                        ),
                        "ltp": float(
                            p.get("ltp")
                            or p.get("lastprice")
                            or p.get("last_price")
                            or 0.0
                        )
                    })

                except Exception:
                    continue

            return positions

        except Exception as e:
            self.alerts.notify(f"Broker positions fetch failed: {e}")
            return []

    def _get_candle_key(self, now, candle):
        candle = str(candle or "5m").lower().strip()
        step = 5  # Hardcoded to 5m boundaries to prevent gap issues with larger timeframes

        # e.g. now=09:21:05  floor to 09:20  closed candle key
        elapsed = (now.hour * 60 + now.minute) - (9 * 60 + 15)

        if elapsed < 0:
            # Before market open  use 09:15 as key
            return now.replace(hour=9, minute=15, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")

        floored_offset = (elapsed // step) * step
        closed_candle_dt = now.replace(
            hour=9, minute=15, second=0, microsecond=0
        ) + timedelta(minutes=floored_offset)

        return closed_candle_dt.strftime("%Y-%m-%d %H:%M")
        
    def _shared_ltp_bar(self, now):
        """15m slot from 09:30 IST (same grid as csv15m / plugin cycles)."""
        local = now
        if getattr(local, "tzinfo", None) is not None:
            local = local.replace(tzinfo=None)
        start = datetime.combine(local.date(), dt_time(9, 30))
        end = datetime.combine(local.date(), dt_time(15, 0))
        if local < start:
            return start
        asof = local if local <= end else end
        slot = (int((asof - start).total_seconds() // 60) // 15) * 15
        bar = start + timedelta(minutes=slot)
        if bar > end:
            bar = end
        return bar

    def _parse_shared_ltp(self, cached):
        if cached is None:
            return None
        try:
            val = float(cached)
            return val if val > 0 else None
        except (ValueError, TypeError):
            pass
        try:
            data = json.loads(cached)
            val = float(data["price"])
            return val if val > 0 else None
        except Exception:
            return None

    def _get_live_price_redis(self, symbol: str, candle: str) -> Optional[float]:
        """
        Shared LTP for all accounts: GET price:ltp:{SYM}.NS:{date}:{HHMM}.
        Written by csv15m (09:30–10:15, …) and /predict append (10:30, 11:45, …) via SET NX.
        First trader caller after a miss also SET NX; losers must not use their own fetch.
        """
        t_ltp_sym = time.time()
        redis_client = getattr(self.market_client, "redis_client", None)
        now = self._now_market_time()
        bar = self._shared_ltp_bar(now)
        redis_key = (
            f"price:ltp:{symbol}.NS:{bar.strftime('%Y-%m-%d')}:{bar.strftime('%H%M')}"
        )
        print(
            f"[LTP FUNC] symbol={symbol} candle={candle} key={redis_key} "
            f"redis={'YES' if redis_client else 'NO'}",
            flush=True
        )
        if redis_client is None:
            return None
        try:
            cached = redis_client.get(redis_key)
            val = self._parse_shared_ltp(cached)
            if val is not None:
                print(f"[LTP SHARED] GET hit {redis_key} price={val:.4f}")
                return val

            for _ in range(8):
                time.sleep(0.15)
                cached = redis_client.get(redis_key)
                val = self._parse_shared_ltp(cached)
                if val is not None:
                    print(f"[LTP SHARED] GET wait-hit {redis_key} price={val:.4f}")
                    return val

            if not hasattr(self.market_client, "fetch_price"):
                return None
            ticker = f"{symbol}.NS"
            bar_dt = bar
            if now.tzinfo is not None and bar_dt.tzinfo is None:
                bar_dt = bar_dt.replace(tzinfo=now.tzinfo)
            tlog_fetch_price_start = time.time()
            print("calling market_client.fetch_price inside the get_live_price_redis", ticker, bar_dt, candle)
            px = self.market_client.fetch_price(
                ticker=ticker,
                target_datetime=bar_dt,
                candle=candle,
            )
            self.tlog.record(
                "fetch_Price_total_time",
                tlog_fetch_price_start,
                note=f"fetch_price_total_time={time.time() - tlog_fetch_price_start}",
            )
            if not px or float(px.get("Close") or 0) <= 0:
                cached = redis_client.get(redis_key)
                return self._parse_shared_ltp(cached)
            live = float(px["Close"])
            payload = json.dumps({
                "price": live,
                "bar": bar.strftime("%H:%M"),
                "source": "trader_setnx",
            })
            created = redis_client.set(redis_key, payload, nx=True, ex=5400)
            print(
                f"[LTP SHARED] SET NX {redis_key} price={live:.4f} created={bool(created)}"
            )
            if not created:
                for _ in range(12):
                    cached = redis_client.get(redis_key)
                    val = self._parse_shared_ltp(cached)
                    if val is not None:
                        elapsed_ltp = round(time.time() - t_ltp_sym, 3)
                        print(
                            f"[LTP SHARED] winner price after SET NX lost "
                            f"{redis_key} price={val:.4f}"
                        )
                        print(f"[TIMING] LTP_PER_SYMBOL {symbol:<15} {elapsed_ltp:>7.3f}s  source=SHARED_LTP")
                        return val
                    time.sleep(0.15)
                print(
                    f"[LTP SHARED] SET NX lost and GET empty — refusing local fetch "
                    f"for {redis_key}"
                )
                return None
            elapsed_ltp = round(time.time() - t_ltp_sym, 3)
            print(f"[TIMING] LTP_PER_SYMBOL {symbol:<15} {elapsed_ltp:>7.3f}s  source=SHARED_LTP")
            return live
        except Exception as e:
            print(f"[LIVE PRICE REDIS ERROR] {symbol}: {e}")
            return None


    def _sleep_until_next_candle(self, candle):
      
        candle = str(candle or "5m").lower().strip()
        step = int(candle[:-1]) if candle.endswith("m") else 5

        now = self._now_market_time()

        # Get the current locked candle base calculation
        candle_key_str = self._get_candle_key(now, candle)
        current_boundary = datetime.strptime(candle_key_str, "%Y-%m-%d %H:%M").replace(tzinfo=MARKET_TZ)

        # Target: NEXT candle boundary (base + step) + 60 seconds
        next_candle_close = current_boundary + timedelta(minutes=step)
        next_run = next_candle_close + timedelta(seconds=60)

        # If we're already past next_run, advance to the next cycle
        if next_run <= now:
            missed = int((now - next_run).total_seconds() // (step * 60)) + 1
            next_run += timedelta(minutes=missed * step)

        # Cap sleep so we wake at/before 15:00 IST for forced square-off
        today_exit = now.replace(
            hour=MARKET_EXIT_TIME.hour,
            minute=MARKET_EXIT_TIME.minute,
            second=0,
            microsecond=0,
        )
        if now < today_exit < next_run:
            next_run = today_exit
            print(
                f"[SCHEDULER] capping sleep to MARKET_EXIT "
                f"{today_exit.strftime('%H:%M:%S')} IST"
            )

        sleep_seconds = max(1, (next_run - now).total_seconds())
        print(
            f"[SCHEDULER] now={now.strftime('%H:%M:%S')} "
            f"next={next_run.strftime('%H:%M:%S')} "
            f"sleep={sleep_seconds:.1f}s"
    )


        deadline = time.time() + sleep_seconds
        while time.time() < deadline:
            if self.stop_event.is_set():
                print("[SCHEDULER] Stop signal mila neend mein  uth raha hoon!")
                return   #  neend se uthta hai, loop pe wapas jaata hai
            if self._now_market_time().time() >= MARKET_EXIT_TIME:
                print("[SCHEDULER] 15:00 IST reached during sleep — waking for square-off")
                return
            time.sleep(1)


        print(
            f"[SCHEDULER] now={now.strftime('%H:%M:%S')} "
            f"next_candle_close={next_candle_close.strftime('%H:%M:%S')} "
            f"firing_at={next_run.strftime('%H:%M:%S')} "
            f"sleep={sleep_seconds:.1f}s"
        )

        # time.sleep(sleep_seconds)

    def _exit_all_positions_and_stop(self):
        begin = try_begin_eod_exit(self)
        if begin == "done":
            return True
        if begin == "busy":
            return False

        try:
            angel_orders = fetch_todays_intraday_orders(self.broker)
            self._generate_final_merged_tradebook(angel_orders=angel_orders)
        except Exception as e:
            print(f"[EOD MERGE ERROR] {e}")

        try:
            return self._run_eod_exit_body()
        except Exception as e:
            print(f"[EOD] Exit failed: {e}")
            self.alerts.notify(f"EOD exit failed: {e}")
            release_eod_exit_in_progress(self)
            return False

    def _run_eod_exit_body(self):
        print("\n" + "=" * 80)
        print("  MARKET CLOSE (3:00 PM IST) - EXITING SESSION POSITIONS")
        print("=" * 80)

        self.alerts.notify("3:00 PM IST - Initiating exit of session positions")
        self._notify_eod_exit_status_to_api(reason="MARKET_CLOSE_15:00_IST")

        with self.broker_pos_lock:
            self._broker_positions_cache = self._get_broker_positions()
            raw_broker_positions = list(self._broker_positions_cache or [])

        def _on_eod_skip_summary(count: int, sample: list, reason: str) -> None:
            if reason == "ledger_fallback":
                self.alerts.notify("EOD: using session ledger keys (Redis meta missing)")
            elif reason == "ledger_zero" and count:
                suffix = f" e.g. {', '.join(sample[:3])}" if sample else ""
                self.alerts.notify(
                    f"EOD skipped {count} manual qty on session symbol(s){suffix}"
                )

        exit_plan, mark_done_empty, status_msg = prepare_eod_exit_plan(
            self,
            raw_broker_positions,
            fallback_symbols=list((self.symbol_allocations or {}).keys()),
            on_skip_summary=_on_eod_skip_summary,
        )

        if not exit_plan:
            if mark_done_empty:
                print(f"[INFO]  {status_msg}")
                self.alerts.notify(" No session positions to exit - Auto trader stopped")
                finalize_eod_shutdown(self, sync_broker=True)
                mark_eod_exit_done(self)
                return True
            print(f"[EOD] {status_msg}")
            self.alerts.notify(status_msg)
            release_eod_exit_in_progress(self)
            return False

        broker_positions = exit_plan_to_broker_positions(exit_plan)
        print(f"[INFO] {status_msg}")

        eod_exit_records = {}
        for pos in broker_positions:
            sym = pos["symbol"]
            position_side = pos["side"]
            exit_side = "SELL" if position_side == "BUY" else "BUY"
            eod_exit_records[sym] = {
                "symbol": sym,
                "position_side": position_side,
                "exit_side": exit_side,
                "qty": pos.get("qty", 0),
                "price": float(pos.get("ltp") or 0.0),
            }
        
        # Collect all exit orders
        exit_orders = []
        
        for pos in broker_positions:
            sym = pos["symbol"]
            side = pos["side"]
            qty = pos["qty"]
            curr_price = pos.get("ltp", 0.0)
            
            # Determine exit side
            exit_side = "SELL" if side == "BUY" else "BUY"
            
            print(f"[EXIT] {sym}: Closing {side} position (qty={qty}) {curr_price:.2f}")
            
            exit_orders.append(
                OrderRequest(
                    symbol=sym,
                    side=exit_side,
                    qty=qty,
                    metadata={
                        "signal": "MARKET_CLOSE_EXIT",
                        "action_type": "MARKET_CLOSE_EXIT",
                        "curr_price": curr_price,
                        "side": exit_side,
                        "qty": qty,
                        "order_value": curr_price * qty,
                        "original_side": side
                    }
                )
            )
        
        # Execute all exit orders in parallel
        if exit_orders:
            print(f"\n[PARALLEL] Executing {len(exit_orders)} exit orders...")
            
            self._ensure_parallel_executor()
            results = self.parallel_executor.submit_orders(exit_orders)
            
            # Process results
            successful_exits = 0
            failed_exits = 0
            
            for result in results:
                sym = result.symbol
                metadata = result.metadata or {}
                original_side = metadata.get("original_side", "UNKNOWN")

                if sym in eod_exit_records:
                    if result.avg_price and float(result.avg_price) > 0:
                        eod_exit_records[sym]["price"] = float(result.avg_price)
                    elif metadata.get("curr_price"):
                        eod_exit_records[sym]["price"] = float(metadata["curr_price"])
                
                if result.success:
                    successful_exits += 1
                    record_engine_order_for_trader(self, result.order_id)

                    # Add to pending for reconciliation
                    with self.pending_lock:
                        self.pending_orders[sym].append({
                            "order_id": result.order_id,
                            "action_type": "MARKET_CLOSE_EXIT",
                            "side": metadata.get("side"),
                            "qty": metadata.get("qty"),
                            "order_value": metadata.get("order_value", 0.0),
                            "placed_at": time.time(),
                        })
                    
                    print(f" {sym}: Exit order sent (closing {original_side} position)")
                else:
                    failed_exits += 1
                    error = result.error or "Unknown error"
                    print(f" {sym}: Exit order failed - {error}")
                    self.alerts.notify(f" Failed to exit {sym}: {error}")
            
            print(f"\n[SUMMARY] Exit orders: {successful_exits} sent, {failed_exits} failed")
            
            # Wait for orders to fill (max 60 seconds)
            print("\n[WAIT] Waiting for exit orders to fill (max 60s)...")
            max_wait = 60
            start_wait = time.time()
            
            while (time.time() - start_wait) < max_wait:
                with self.pending_lock:
                    if not self.pending_orders:
                        print(" All exit orders filled")
                        break
                
                time.sleep(2)
                elapsed = int(time.time() - start_wait)
                remaining = max_wait - elapsed
                print(f"[WAIT] {remaining}s remaining... (pending: {len(self.pending_orders)} symbols)", end='\r')
            
            # Check final status
            with self.pending_lock:
                if self.pending_orders:
                    print(f"\n  Warning: {len(self.pending_orders)} positions still pending after 60s")
                    for sym in self.pending_orders.keys():
                        print(f"  - {sym}: Position may not be fully closed")
                        self.alerts.notify(f" {sym} exit order pending - check manually")
        
        # Final sync
        print("\n[FINAL SYNC] Syncing with broker...")
        self._sync_cash_with_broker()

        try:
            self._persist_eod_exit_trading_logs(list(eod_exit_records.values()))
        except Exception as e:
            print(f"[EOD-LOG] trading_logs insert failed: {e}")
        
        finalize_eod_shutdown(self, clear_positions=True, sync_broker=False)

        print("\n" + "=" * 80)
        print(" ALL POSITIONS EXITED - AUTO TRADER STOPPED")
        print("=" * 80)
        
        # Print final summary
        print(f"\nFinal Summary:")
        print(f"  Cash Balance:{self.cash_balance:,.2f}")
        print(f"  Realized P&L:{self.realized_pnl:,.2f}")
        print(f"  Total Equity:{self.current_capital:,.2f}")
        
        self.alerts.notify(
            f" Auto Trader Stopped\n"
            f"Final Equity:{self.current_capital:,.2f}\n"
            f"Realized P&L:{self.realized_pnl:,.2f}"
        )

        mark_eod_exit_done(self)
        return True

    def exit_single_position(self, symbol: str, log_signal: str = None, exit_reason: str = None) -> dict:
        """
        Exit a single symbol's position.
        - Fetches broker positions for this symbol
        - If position exists Ã¢â€ â€™ places exit order
        - Adds to pending_orders for reconciliation
        - Returns result dict (success/failure)
        """
        symbol = symbol.upper().replace("-EQ", "")
        print(f"\n[SINGLE EXIT] Request to exit position: {symbol}")

        try:
            # 1) Refresh broker positions
            with self.broker_pos_lock:
                self._broker_positions_cache = self._get_broker_positions()
                broker_positions = list(self._broker_positions_cache or [])

            # 2) Find the target symbol
            target_pos = None
            for pos in broker_positions:
                if pos["symbol"] == symbol:
                    target_pos = pos
                    break

            if not target_pos:
                msg = f"No open position found for {symbol}"
                print(f"[SINGLE EXIT] {msg}")
                return {"success": False, "symbol": symbol, "message": msg}

            side = target_pos["side"]
            qty = target_pos["qty"]
            curr_price = target_pos.get("ltp", 0.0)
            exit_side = "SELL" if side == "BUY" else "BUY"
            exit_reason = exit_reason or ("STOP_LOCK_EXIT" if log_signal == "STOP_LOCK" else "MANUAL_EXIT")
            pre_exit_position = self._snapshot_position_for_exit(symbol, target_pos)

            print(f"[SINGLE EXIT] {symbol}: Closing {side} position (qty={qty}) @ {curr_price:.2f}")

            # 3) Place exit order
            exit_order = OrderRequest(
                symbol=symbol,
                side=exit_side,
                qty=qty,
                metadata={
                    "signal": "SINGLE_EXIT",
                    "action_type": "EXIT_LONG" if side == "BUY" else "COVER_SHORT",
                    "curr_price": curr_price,
                    "side": exit_side,
                    "qty": qty,
                    "order_value": curr_price * qty,
                    "original_side": side,
                    "position_side": side,
                    "pre_exit_position": pre_exit_position,
                    "exit_reason": exit_reason,
                }
            )

            self._ensure_parallel_executor()
            results = self.parallel_executor.submit_orders([exit_order])

            if not results:
                msg = f"No result from order executor for {symbol}"
                print(f"[SINGLE EXIT] {msg}")
                return {"success": False, "symbol": symbol, "message": msg}

            result = results[0]
            metadata = result.metadata or {}

            if result.success:
                exit_ctx = {
                    "order_id": result.order_id,
                    "action_type": metadata.get("action_type", "EXIT_LONG"),
                    "side": metadata.get("side"),
                    "qty": metadata.get("qty"),
                    "order_value": metadata.get("order_value", 0.0),
                    "position_side": metadata.get("position_side"),
                    "pre_exit_position": metadata.get("pre_exit_position") or pre_exit_position,
                    "exit_reason": metadata.get("exit_reason") or exit_reason,
                    "placed_at": time.time(),
                }

                if result.filled and float(result.avg_price or 0.0) > 0:
                    self._finalize_symbol_exit_fill(
                        symbol,
                        {
                            "side": metadata.get("side"),
                            "qty": int(result.filled_qty or metadata.get("qty") or 0),
                            "avg_price": float(result.avg_price or 0.0),
                        },
                        exit_ctx,
                    )
                    msg = f"Exit order filled for {symbol} (closed {side} position, qty={qty})"
                    print(f"[SINGLE EXIT] {msg}")
                    return {"success": True, "symbol": symbol, "message": msg, "order_id": result.order_id}

                # Add to pending for reconciliation
                with self.pending_lock:
                    self.pending_orders[symbol].append(exit_ctx)

                self._persist_exit_pending(
                    symbol,
                    exit_reason=exit_ctx["exit_reason"],
                    exit_side=exit_ctx["side"],
                    qty=exit_ctx["qty"],
                    entry_price=pre_exit_position.get("entry_price", 0.0),
                    exit_order_id=result.order_id,
                )

                # Mark symbol as exited Ã¢â‚¬â€ will be excluded from next trading cycle
                self._exited_symbols.add(symbol)
                print(f"[SINGLE EXIT] {symbol} added to _exited_symbols Ã¢â‚¬â€ will be skipped in future cycles")

                if False and log_signal:
                    session_id = getattr(self, "ui_session_id", None) or getattr(self, "session_id", None)
                    if session_id:
                        symbol_unrealized_pnl = round(self._get_symbol_unrealized_pnl(symbol), 2)
                        symbol_realized_pnl = round(float(self.realized_pnl_by_symbol.get(symbol, 0.0)), 2)
                        symbol_pnl = round(symbol_realized_pnl + symbol_unrealized_pnl, 2)
                        self.current_cycle_ts_str = self._now_market_time().strftime("%Y-%m-%d %H:%M:%S")
                        self._update_ui_snapshot(
                            session_id,
                            getattr(self, "_cycle_count", 0) + 1,
                            [{
                                "symbol": symbol,
                                "curr_price": round(float(curr_price), 2),
                                "return_pct": 0.0,
                                "side": side,
                                "signal": log_signal,
                                "action": f"{log_signal} EXIT SENT",
                                "qty": qty,
                                "unrealized_pnl": symbol_unrealized_pnl,
                                "symbol_unrealized_pnl": symbol_unrealized_pnl,
                                "symbol_realized_pnl": symbol_realized_pnl,
                                "symbol_pnl": symbol_pnl,
                                "pnl": symbol_pnl,
                            }],
                        )

                msg = f"Exit order sent for {symbol} (closing {side} position, qty={qty})"
                print(f"[SINGLE EXIT] Ã¢Å“â€¦ {msg}")
                
                return {"success": True, "symbol": symbol, "message": msg, "order_id": result.order_id}
            else:
                msg = f"Exit order failed for {symbol}: {result.error}"
                print(f"[SINGLE EXIT] Ã¢ÂÅ’ {msg}")
                return {"success": False, "symbol": symbol, "message": msg}

        except Exception as e:
            msg = f"Exception during single exit for {symbol}: {e}"
            print(f"[SINGLE EXIT] Ã¢ÂÅ’ {msg}")
            return {"success": False, "symbol": symbol, "message": msg}

    def _reconcile_pending_orders(self):
        while not self.stop_event.is_set():
            t_reconcile_start = time.time()
            pending_snapshot = []                    #Ãƒâ€šÃ‚Â¦ initialise here so always defined


            with self.pending_lock:
                if not self.pending_orders:
                    pass # Handled below
                else:
                    # pending_orders: symbol -> list[ctx]
                    pending_snapshot = list(self.pending_orders.items())

            if not pending_snapshot:
                time.sleep(1)
                continue

            new_positions = self._get_broker_positions()
            with self.broker_pos_lock:
                self._broker_positions_cache = new_positions
                broker_positions = list(self._broker_positions_cache or [])

            for sym, ctx_list in pending_snapshot:
                # iterate over a COPY so we can safely remove
                for ctx in list(ctx_list):

                    expected_side = ctx.get("side")
                    expected_qty = ctx.get("qty", 0)
                    action_type = ctx.get("action_type", "")
                    order_id       = ctx.get("order_id")

                    # Defensive: phantom entry with no order_id (order was rejected
                    # by Angel before an order_id was issued). Release its exposure
                    # and drop it so the symbol isn't locked out of future cycles.
                    if not order_id:
                        try:
                            self._release_exposure(sym, ctx.get("order_value", 0.0))
                        except Exception as _re:
                            print(f"[RECONCILE] {sym}: release_exposure failed: {_re}")
                        with self.pending_lock:
                            try:
                                ctx_list.remove(ctx)
                            except ValueError:
                                pass
                            if not ctx_list:
                                self.pending_orders.pop(sym, None)
                        print(f"[RECONCILE] dropped phantom pending entry for {sym} ({action_type})")
                        continue

                    is_exit = action_type in {
                        "EXIT_LONG",
                        "COVER_SHORT",
                        # "TREND_VETO_EXIT_LONG",
                        # "TREND_VETO_EXIT_SHORT",
                        "STOP_LOSS",
                        "MARKET_CLOSE_EXIT"
                    }
                    
                    is_flip = action_type in {
                        "FLIP_TO_LONG",
                        "FLIP_TO_SHORT"
                    }
                    
                    # ----------------------------------------
                    # FLIP HANDLING (NETTED POSITIONS CHANGE)
                    # ----------------------------------------
                    if is_flip:
                        exit_price = 0.0
                        if order_id:
                            check_ts = datetime.now()
                            print(f"[DEBUG-RECONCILE] [{check_ts.strftime('%H:%M:%S.%f')[:-3]}] Checking flip pending order_id {order_id} for {sym}")
                            exit_price = self._get_fill_price_from_orderbook(order_id, sym)
                        
                        if exit_price <= 0:
                            print(f"[DEBUG-RECONCILE] Flip Order {order_id} not executed yet. Retaining in pending list.")
                            continue
                            
                        # If executed, definitively call handle_filled
                        self._handle_filled(
                            sym,
                            {
                                "side": "BUY" if "LONG" in action_type else "SELL",
                                "qty": expected_qty,
                                "avg_price": exit_price,
                            },
                            ctx,
                        )

                        order_value = ctx.get("order_value", 0.0)
                        self._release_exposure(sym, order_value)

                        with self.pending_lock:
                            ctx_list.remove(ctx)
                            if not ctx_list:
                                self.pending_orders.pop(sym, None)

                        continue
                    
                    # -----------------------
                    # ENTRY / SAME-SIDE MATCH
                    # -----------------------
                    pos = None
                    broker_qty = 0
                    for p in broker_positions:
                        if p.get("symbol") != sym:
                            continue
                        if p.get("side") != expected_side:
                            continue

                        broker_qty = p.get("qty", 0)
                        if broker_qty <= 0:
                            continue

                        pos = p
                        break

                    if pos:
                        # ---- FILLED (or partially filled) ----
                        filled_qty = min(broker_qty, expected_qty)
                        avg_price = float(pos.get("avg_price") or 0.0)

                        if avg_price <= 0 and order_id:
                            avg_price = self._get_fill_price_from_orderbook(order_id, sym)
                            if avg_price > 0:
                                print(f"[RECONCILE] {sym}: avg_price order book se mili Ãƒâ€šÃ‚Â¹{avg_price:.2f}")
                            else:
                                print(f"[RECONCILE] {sym}: avg_price abhi bhi 0  next cycle mein retry hoga")

                        self._handle_filled(
                            sym,
                            {
                                "side": pos["side"],
                                "qty": filled_qty,
                                "avg_price": avg_price
                            },
                            ctx
                        )
                        order_value = ctx.get("order_value", 0.0)
                        self._release_exposure(sym, order_value)

                        with self.pending_lock:
                            ctx_list.remove(ctx)
                            if not ctx_list:
                                self.pending_orders.pop(sym, None)
                        continue
                    
                    # -----------------------
                    # EXIT HANDLING (POSITION GONE)
                    # -----------------------
                    if is_exit:
                        still_exists = False
                        for p in broker_positions:
                            if p.get("symbol") != sym:
                                continue
                            position_side = ctx.get("position_side", expected_side)
                            if p.get("side") == position_side:
                                still_exists = True
                                break

                        if not still_exists:
                            exit_price = 0.0
                            if order_id:
                                check_ts = datetime.now()
                                print(f"[DEBUG-RECONCILE] [{check_ts.strftime('%H:%M:%S.%f')[:-3]}] Checking pending order_id {order_id} for {sym}")
                                exit_price = self._get_fill_price_from_orderbook(order_id, sym)
                                ret_ts = datetime.now()
                                print(f"[DEBUG-RECONCILE] [{ret_ts.strftime('%H:%M:%S.%f')[:-3]}] Orderbook API returned exit_price: {exit_price} for {sym} (order_id {order_id})")

                            # Fallback removed - we MUST wait for the true execution price
                            if exit_price <= 0:
                                print(f"[DEBUG-RECONCILE] Order {order_id} not executed yet. Retaining in pending list.")
                                continue

                            self._handle_filled(
                                sym,
                                {
                                    "side": expected_side,
                                    "qty": expected_qty,
                                    "avg_price": exit_price  # price already realized
                                },
                                ctx
                            )
                            order_value = ctx.get("order_value", 0.0)
                            self._release_exposure(sym, order_value)
                            with self.pending_lock:
                                ctx_list.remove(ctx)
                                if not ctx_list:
                                    self.pending_orders.pop(sym, None)

                            continue
                        
                    # ---- RECOIL / TIMEOUT ----
                    if time.time() - ctx.get("placed_at", 0) > 120:
                        self._handle_rejected(sym, ctx)

                        order_value = ctx.get("order_value", 0.0)
                        self._release_exposure(sym, order_value)

                        with self.pending_lock:
                            ctx_list.remove(ctx)
                            if not ctx_list:
                                self.pending_orders.pop(sym, None)

            time.sleep(2) 
            elapsed_rec = round(time.time() - t_reconcile_start, 3)
            print(f"[TIMING] RECONCILE_CYCLE_TOTAL              {elapsed_rec:>7.3f}s  pending_syms={len(pending_snapshot)}")
            try:
                self.tlog.record("RECONCILE_CYCLE_TOTAL", t_reconcile_start, note=f"pending_syms={len(pending_snapshot)}")
            except Exception:
                pass 
    
    # ---------- CASH / BALANCE ----------

    def _get_free_cash(self):
        try:
            bal = self.broker.get_account_balance(self.session)
        except Exception as e:
            self.alerts.notify(f"Failed to fetch account balance: {e}")
            return None

        if not isinstance(bal, dict) or bal.get("status") != "success":
            self.alerts.notify(
                f"Could not read account balance: {bal.get('error') if isinstance(bal, dict) else bal}"
            )
            return None

        if "free_cash" in bal:
            try:
                free_cash = float(bal["free_cash"])
                if free_cash >= 0:
                    print(f"[INFO] Free cash detected: {free_cash:,.2f}")
                    return free_cash
            except Exception:
                pass

        if "data" in bal:
            try:
                data = bal["data"]
                if isinstance(data, dict):
                    for key in ["availablecash", "available_cash", "availableCash", "net", "cash"]:
                        if key in data:
                            try:
                                free_cash = float(data[key])
                                if free_cash >= 0:
                                    print(f"[INFO] Free cash from data.{key}: {free_cash:,.2f}")
                                    return free_cash
                            except Exception:
                                pass
            except Exception:
                pass

        raw = bal.get("raw")
        free_cash = None

        try:
            if isinstance(raw, dict):
                candidate = raw.get("data") if "data" in raw else raw
                for key in (
                    "available_cash", "availableCash", "available_balance", "availableBalance",
                    "cash", "equity", "netEquity", "availableMargin", "available_margin",
                    "availablecash", "net"
                ):
                    if isinstance(candidate, dict) and key in candidate:
                        try:
                            free_cash = float(candidate[key])
                            if free_cash >= 0:
                                print(f"[INFO] Free cash from raw.{key}: {free_cash:,.2f}")
                                return free_cash
                        except Exception:
                            try:
                                free_cash = float(str(candidate[key]).replace(",", ""))
                                if free_cash >= 0:
                                    print(f"[INFO] Free cash from raw.{key} (parsed): {free_cash:,.2f}")
                                    return free_cash
                            except Exception:
                                pass

                if free_cash is None:
                    for k, v in (candidate.items() if isinstance(candidate, dict) else []):
                        try:
                            if isinstance(v, (int, float)) and v >= 0:
                                if (
                                    "available" in k.lower() or
                                    "free" in k.lower() or
                                    "cash" in k.lower() or
                                    "net" in k.lower()
                                ):
                                    free_cash = float(v)
                                    print(f"[INFO] Free cash from scanning {k}: {free_cash:,.2f}")
                                    return free_cash
                        except Exception:
                            continue
        except Exception as e:
            print(f"[WARN] Error parsing raw response: {e}")
            free_cash = None

        print(f"[ERROR] Could not extract free cash from response. Available keys: {list(bal.keys())}")
        return free_cash
    
    # trading snapshot update karne ka function
    def _update_ui_snapshot(self, session_id, cycle, rows):
        snapshot = {
            "cycle": cycle,
            "timestamp": (self.current_cycle_ts_str or self._now_market_time().strftime("%Y-%m-%d %H:%M:%S")),
            "cash_balance": round(self.cash_balance, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "unrealized_pnl": round(self.unrealized_pnl, 2),
            "pnl":round(self.unrealized_pnl+self.realized_pnl , 2),
            "total_equity": round(
                self.cash_balance + self.realized_pnl + self.unrealized_pnl, 2
            ),
            "symbols": rows,
            "rms_triggered": self.rms_triggered,
            "rms_message": "Done for the day all positions exitted" if self.rms_triggered else None,
            "simulation_logs": bool(getattr(self, "simulation_logs", False)),
        }

    # LIVE UI (FAST)
        trading_snapshot[session_id] = snapshot

    # DB LOGGING (HISTORY)
        try:
            insert_trading_snapshot(
            trading_logs_collection=self.trading_logs_collection,
            session_id=session_id,
            cycle=cycle,
            snapshot=snapshot,
            rows=rows,
            simulation_logs=bool(getattr(self, "simulation_logs", False)),
        )
        except Exception as e:
            print(f"[DB ERROR] Trading snapshot insert failed: {e}")

    def _persist_eod_exit_trading_logs(self, exit_records: list) -> None:
        """Save 15:00 / shutdown square-off rows to trading_logs for the frontend."""
        if not exit_records:
            return
        session_id = getattr(self, "ui_session_id", None) or getattr(self, "session_id", None)
        if not session_id:
            print("[EOD-LOG] No session_id — skipping trading_logs insert")
            return

        rows = []
        for rec in exit_records:
            sym = rec["symbol"]
            exit_side = rec["exit_side"]  # actual order placed: BUY or SELL
            position_side = rec["position_side"]  # position we had: BUY (long) or SELL (short)
            qty = int(rec.get("qty") or 0)
            price = float(rec.get("price") or 0.0)
            symbol_realized = round(float(self.realized_pnl_by_symbol.get(sym, 0.0)), 2)
            rows.append({
                "symbol": sym,
                "curr_price": round(price, 2),
                "return_pct": 0.0,
                "side": position_side,
                "signal": "",
                "action": exit_side,
                "qty": qty,
                "unrealized_pnl": 0.0,
                "symbol_unrealized_pnl": 0.0,
                "symbol_realized_pnl": symbol_realized,
                "symbol_pnl": symbol_realized,
                "pnl": symbol_realized,
            })

        self.current_cycle_ts_str = self._now_market_time().strftime("%Y-%m-%d %H:%M:%S")
        self.unrealized_pnl = 0.0
        cycle = getattr(self, "_cycle_count", 0) + 1
        print(f"[EOD-LOG] Persisting {len(rows)} square-off row(s) to trading_logs (cycle={cycle})")
        self._update_ui_snapshot(session_id, cycle, rows)

    def _sync_cash_with_broker(self):
        print("[SYNC] Syncing cash balance with broker...")
        free_cash = self._get_free_cash()
        if free_cash is not None:
            old_balance = self.cash_balance
            self.cash_balance = free_cash
            print(f"[SYNC]  Cash balance updated: {old_balance:,.2f} -> {free_cash:,.2f}")
            self.alerts.notify(f"Cash synced with broker: {free_cash:,.2f}")
            return True
        else:
            print("[SYNC] Failed to sync cash balance")
            self.alerts.notify("Warning: Could not sync cash balance with broker")
            return False
    
    def _analyze(self, df, swing_interval, user_positions,session_trends=None):
        signals = {}
        for symbol, group in df.groupby("Ticker"):
            if "Timestamp" in group.columns:
                group["Timestamp"] = pd.to_datetime(group["Timestamp"])
                group = group.sort_values("Timestamp")

            predicted_path = group["Predicted Price"].values

            # Need current + 3 future points
            if len(predicted_path) < 4:
                continue

            # -------------------------------
            # CURRENT PRICE ANCHOR
            # -------------------------------
            live_price = self._get_live_price_redis(symbol, swing_interval) 

            print(
                f"[LIVE RESULT] {symbol} "
                f"live_price={live_price} "
                f"({'FALLBACK predicted[0]' if live_price is None else 'USING SHARED LTP'})",
                flush=True
            )
            
            # Last resort only if Redis SET NX / GET both failed (keeps Trade logs row)
            current_price = float(live_price) if live_price else float(predicted_path[0])
            
            if live_price is None:
                print(f"\n[DEBUG-VERIFY] {symbol}: shared LTP missing — using predicted_path[0]: {current_price}")

            print(
                f"[CURR PRICE FINAL] {symbol} curr_price={current_price}",
                flush=True
            )          
            
            # -------------------------------
            traj_col = group["trajectory_pct"].values if "trajectory_pct" in group.columns else None
            regime_col = group["risk_regime"].values if "risk_regime" in group.columns else None

            if traj_col is not None and len(traj_col) > 0 and not np.isnan(traj_col[0]):
                # Use slot 0's trajectory ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â the most current signal
                trajectory_pct = float(traj_col[0])
                risk_regime     = int(regime_col[0]) if regime_col is not None else (
                    0 if abs(trajectory_pct) < self.min_trade_pct else 1
                )
            else:
                # Fallback: recompute from raw prices
                first_candle   = predicted_path[0]
                last_candle    = predicted_path[3]
                trajectory_pct = ((last_candle - first_candle) / first_candle) * 100 if first_candle != 0 else 0.0
                abs_move       = abs(trajectory_pct)
                risk_regime    = 0 if abs_move < self.min_trade_pct else 1 

            # -------------------------------
            # POSITION CONTEXT
            # -------------------------------
            position = user_positions.get(symbol, {})
            position_side = position.get("side", "NONE")

            # # ===============================
            # # SESSION TREND VETO (HARD RULE)
            # # ===============================
            # session_direction = None
            # session_open = None

            # if session_trends and symbol in session_trends:
            #     session_direction = session_trends[symbol].get("direction")
            #     session_open = session_trends[symbol].get("session_open")

            # # If LONG but session trend is DOWN FORCE EXIT
            # if position_side == "BUY" and session_direction == -1 and session_open and current_price < session_open:
            #     signal = "SELL (Trend Veto Exit)"
            #     signals[symbol] = {
            #         "signal": signal,
            #         "change_pct": trajectory_pct,
            #         "curr_price": current_price,
            #         "side": position_side,
            #         "interval": swing_interval,
            #         "risk_regime": risk_regime
            #     }
            #     continue

            # # If SHORT but session trend is UP FORCE COVER
            # if position_side == "SELL" and session_direction == 1 and session_open and current_price > session_open:
            #     signal = "BUY (Trend Veto Exit)"
            #     signals[symbol] = {
            #         "signal": signal,
            #         "change_pct": trajectory_pct,
            #         "curr_price": current_price,
            #         "side": position_side,
            #         "interval": swing_interval,
            #         "risk_regime": risk_regime
            #     }
            #     continue

            signal = "HOLD"
            
            # =========================================================
            # STRICT NO-EXPANSION SIGNAL LOGIC 
            # =========================================================
            if position_side == "NONE":
                if trajectory_pct > 0:
                    signal = "BUY"
                elif trajectory_pct < 0:
                    signal = "SELL"
                else:
                    signal = "HOLD"

            elif position_side == "BUY":
                if trajectory_pct < 0:
                    signal = "SELL" 
                else:
                    # signal = "HOLD"
                    signal = "BUY"   # allow adding to winning longs

            elif position_side == "SELL":
                if trajectory_pct > 0:
                    signal = "BUY"  
                else:
                    # signal = "HOLD"
                    signal = "SELL" # allow adding to winning shorts

            # ===============================
            # OUTPUT
            # ===============================
            signals[symbol] = {
                "signal": signal,
                "change_pct": trajectory_pct,
                "curr_price": current_price,
                "side": position_side,
                "interval": swing_interval,
                "risk_regime": risk_regime
            }
        return signals 
    
    def _calculate_pnl(self, symbol, ltp):
        if ltp is None or ltp <= 0:
            return 0.0

        #  ONLY source of truth  set by _handle_filled with actual fill price
        # if symbol not in self.positions:
        #     return 0.0  # No tracked position  no PnL to compute

        with self.positions_lock:
            pos = self.positions.get(symbol)

        if not pos:
            return 0.0

        pos = self.positions.get(symbol)
        try:
            entry_price = float(pos.get("entry_price") or 0.0)
            qty = int(pos.get("qty") or 0)
            side = pos.get("side")
        except Exception:
            return 0.0

        # ================================================================
        # Fallback: entry_price still 0 after _handle_filled?
        # This means _handle_filled returned early (couldn't get fill price).
        # Fix it NOW from broker cache  and permanently patch self.positions
        # so next cycle doesn't repeat this.
        # ================================================================
        if entry_price <= 0:
            with self.broker_pos_lock:
                broker_positions = list(self._broker_positions_cache or [])
            for p in broker_positions:
                if p.get("symbol") != symbol:
                    continue
                fresh_price = float(p.get("avg_price", 0.0))
                if fresh_price > 0:
                    with self.positions_lock:
                        if symbol in self.positions:
                            self.positions[symbol]["entry_price"] = fresh_price
                    entry_price = fresh_price
                    print(f"[ENTRY PRICE PATCH] {symbol}: Ãƒâ€šÃ‚Â¹{fresh_price:.2f} (broker cache fallback  should be rare)")
                break

        # Validation
        if entry_price <= 0:
            print(f"[PNL WARN] {symbol}: entry_price nahi mili  P&L = 0")
            return 0.0
        if qty <= 0:
            return 0.0
        if side not in ("BUY", "SELL"):
            return 0.0

        # P&L Formula
        pnl = (ltp - entry_price) * qty if side == "BUY" else (entry_price - ltp) * qty

        # Sanity check
        if abs(pnl) > (entry_price * qty):
            print(f"[P&L SANITY BREACH] {symbol} | pnl={pnl:.2f}, entry={entry_price:.2f}, qty={qty}, ltp={ltp:.2f}")
            return 0.0

        return round(pnl, 2)

    @staticmethod
    def _normalize_config_symbol(symbol: str) -> str:
        return (symbol or "").upper().replace("-EQ", "").strip()

    def _get_symbol_position_qty(self, symbol: str) -> int:
        symbol = self._normalize_config_symbol(symbol)
        with self.positions_lock:
            pos = self.positions.get(symbol)
            if pos:
                return int(pos.get("qty") or 0)
        with self.broker_pos_lock:
            for pos in self._broker_positions_cache or []:
                if pos.get("symbol") == symbol:
                    return int(pos.get("qty") or 0)
        return 0

    def _get_symbol_unrealized_pnl(self, symbol: str) -> float:
        symbol = self._normalize_config_symbol(symbol)
        if symbol in self._exited_symbols or self._get_exited_symbol_doc(symbol):
            print(f"[PNL-GUARD] {symbol}: exited symbol -> unrealized_pnl forced to 0.00")
            return 0.0

        with self.live_pnl_lock:
            tick = self.live_pnl.get(symbol)
            if tick is not None:
                return float(tick.get("pnl") or 0.0)

        cache = getattr(self, "_cycle_ltp_cache", {}) or {}
        ltp = cache.get(symbol)
        if ltp is not None and float(ltp) > 0:
            return float(self._calculate_pnl(symbol, float(ltp)))

        with self.broker_pos_lock:
            for pos in self._broker_positions_cache or []:
                if pos.get("symbol") == symbol:
                    ltp = float(pos.get("ltp") or 0.0)
                    if ltp > 0:
                        return float(self._calculate_pnl(symbol, ltp))
        return 0.0

    def _run_stoplock_exits(self, active_symbols: list) -> None:
        """At 14:15 IST exit open symbols with unrealized_pnl < 0; continue with the rest."""
        now = self._now_market_time()
        print(
            f"[STOPLOCK] Check at {now.strftime('%Y-%m-%d %H:%M:%S')} IST "
            f"(trigger>={STOP_LOCK_TIME.strftime('%H:%M')})"
        )

        symbols_to_check = {
            self._normalize_config_symbol(s) for s in (active_symbols or []) if s
        }

        with self.broker_pos_lock:
            self._broker_positions_cache = self._get_broker_positions()
            for pos in self._broker_positions_cache or []:
                sym = self._normalize_config_symbol(pos.get("symbol"))
                if sym:
                    symbols_to_check.add(sym)

        exited = []
        continuing = []
        for sym_key in sorted(symbols_to_check):
            if sym_key in self._exited_symbols:
                continue

            qty = self._get_symbol_position_qty(sym_key)
            if qty == 0:
                continuing.append(f"{sym_key}(flat)")
                continue

            unrealized = self._get_symbol_unrealized_pnl(sym_key)
            if unrealized < 0:
                print(f"[STOPLOCK] {sym_key} unrealized={unrealized:.2f} — placing exit")
                result = self.exit_single_position(sym_key, log_signal="STOP_LOCK")
                if result.get("success"):
                    exited.append(sym_key)
                else:
                    print(
                        f"[STOPLOCK] {sym_key} exit failed: "
                        f"{result.get('message', 'unknown error')}"
                    )
            else:
                continuing.append(f"{sym_key}(unrealized={unrealized:.2f})")

        summary = (
            f"14:15 stop-lock complete — exited: {exited or 'none'}; "
            f"continuing: {continuing}"
        )
        print(f"[STOPLOCK] {summary}")
        self.alerts.notify(summary)

    def convert_candle_to_seconds(self, c):
        c = str(c).lower().strip()

        if c.endswith("m"):
            return int(c[:-1]) * 60

        return 300

    def _close_position(self, session, symbol, exit_price, exit_qty, position_snapshot: Optional[dict] = None):
        # -------------------------------
        # Atomic fetch and update (thread-safe)
        # -------------------------------
        with self.positions_lock:
            pos = self.positions.get(symbol)
            position_was_live = bool(pos)
            if not pos and position_snapshot:
                pos = dict(position_snapshot)
            if not pos:
                return 0.0

            # -------------------------------
            # Extract values
            # -------------------------------
            try:
                current_qty = int(pos.get("qty") or 0)
                entry = float(pos.get("entry_price") or 0.0)
                side = pos.get("side")
                exit_price = float(exit_price or 0.0)
                exit_qty = int(exit_qty or 0)
            except Exception:
                print(f"[CLOSE ERROR] {symbol}: invalid position data {pos}")
                return 0.0

            if current_qty <= 0 or entry <= 0 or exit_price <= 0 or exit_qty <= 0 or side not in ("BUY", "SELL"):
                return 0.0

            # -------------------------------
            # P&L calculation based ONLY on exited quantity
            # -------------------------------
            realized_qty = min(current_qty, exit_qty)
            
            if side == "BUY":
                profit = (exit_price - entry) * realized_qty
                self.cash_balance += exit_qty * exit_price
            else:
                profit = (entry - exit_price) * realized_qty

            # Handle partial exits cleanly and position flips
            remaining_qty = exit_qty - current_qty
            
            if position_was_live and remaining_qty > 0:
                pos["side"] = "SELL" if side == "BUY" else "BUY"
                pos["qty"] = remaining_qty
                pos["entry_price"] = exit_price
                print(f"[POSITION FLIP] {symbol}: Flipped to {pos['side']} {remaining_qty} @ {exit_price}")
            elif position_was_live and remaining_qty == 0:
                self.positions.pop(symbol, None)
            elif position_was_live:
                pos["qty"] -= exit_qty

        # -------------------------------
        # Update realized PnL
        # -------------------------------
        self.realized_pnl += profit
        self.realized_pnl_by_symbol[symbol] += profit

        # Check if realized PnL alone has breached the portfolio limit.
        # Mirrors break_1's register_pnl() — catches breach when all positions
        # close in one batch and no further WS ticks arrive.
        # Portfolio RMS halt disabled.
        # Keep the layer available, but do not let realized portfolio loss
        # exit all stocks. Per-ticker RMS remains active.
        # self._check_realized_portfolio_rms()

        return profit

    def _is_position_settled(self, broker_pos):
        return (
            broker_pos is not None
            and broker_pos.get("qty", 0) > 0
            and broker_pos.get("avg_price", 0) > 0
            and broker_pos.get("side") in ("BUY", "SELL")
        )
        
    # ---------- LOGGING ----------
    def _log_portfolio_action(self, symbol, action, qty, entry_price, exit_price, pnl):
        cumulative_pnl = self.realized_pnl
        portfolio_return = (cumulative_pnl / self.initial_capital) * 100
        trade_return = (pnl / (entry_price * qty)) * 100 if entry_price * qty else 0.0
        with open(self.portfolio_log, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                self._now_market_time().strftime("%Y-%m-%d %H:%M:%S"),
                symbol, action, qty,
                f"{entry_price:.2f}", f"{exit_price:.2f}", f"{pnl:.2f}",
                f"{cumulative_pnl:.2f}", f"{self.current_capital:.2f}",
                f"{trade_return:.2f}", f"{portfolio_return:.2f}"
            ])

    def _log_trade(self, symbol, signal, change, status, price, qty, pnl):

        #  Always use live price from redis cache as price source

        # live = self._get_live_price_redis(symbol, getattr(self, "_current_candle", "5m"))
        
        live = getattr(self, "_cycle_ltp_cache", {}).get(symbol)


        if live is not None and live > 0:
            price = float(live)
        elif price is None or price <= 0:
            price = 0.0

        #  Calculate live unrealized PnL from cache if not explicitly passed
        if pnl == 0.0 and price > 0 and status in ("hold", "pending", "wait"):
            unrealized = self._calculate_pnl(symbol, price)
        else:
            unrealized = pnl

        candle_time = getattr(self, "current_cycle_ts_str", None)
        if not candle_time:
            candle_time = self._now_market_time().strftime("%Y-%m-%d %H:%M:%S")

        logged_at = self._now_market_time().strftime("%Y-%m-%d %H:%M:%S")

        total_equity = round(self.cash_balance + self.unrealized_pnl, 2)
        portfolio_return = round(
            ((total_equity - self.initial_capital) / self.initial_capital) * 100, 4
        ) if self.initial_capital else 0.0

        log_entry = {
            "time": candle_time,
            "logged_at": logged_at,
            "symbol": symbol,
            "signal": signal,
            "change_pct": round(change, 6),
            "status": status,
            "price": price,
            "qty": qty,
            "pnl": round(unrealized, 2),
            "cash_balance": round(self.cash_balance, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "total_equity": total_equity,
        }

        self.trade_history.append(log_entry)

        #  WRITE TO CSV IMMEDIATELY  bar by bar, every cycle
        try:
            with open(self.log_path, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    candle_time,
                    symbol,
                    signal,
                    round(change, 6),
                    status,
                    price,
                    qty,
                    round(unrealized, 2),
                    round(self.cash_balance, 2),
                    round(self.realized_pnl, 2),
                    total_equity,
                    portfolio_return
                ])
        except Exception as e:
            print(f"[CSV ERROR] Trade log append failed: {e}")
    


    
    def _generate_final_merged_tradebook(self ,angel_orders=None) -> pd.DataFrame:
        internal_df = pd.DataFrame(self.trade_history)
        if not internal_df.empty:
            internal_df["source"] = "internal"

        try:
            # resp = self.broker.get_order_book(self.session)
            # if resp.get("status") == "success":
            #     angel_df = pd.DataFrame(resp["raw"].get("data", []))
            # else:
            #     angel_df = pd.DataFrame()

            # angel_df = pd.DataFrame(self.broker.orderBook())

              angel_df = pd.DataFrame(angel_orders or [])
        except Exception as e:
            print(f"[ORDERBOOK] Fetch failed: {e}")
            angel_df = pd.DataFrame()

        if not angel_df.empty:
            angel_df["source"] = "angel"

        final_df = pd.concat(
            [internal_df, angel_df],
            ignore_index=True,
            sort=False
        )

        date_str = self._now_market_time().strftime("%Y-%m-%d")
        output_path = os.path.join(
            self.log_dir,
            f"final_tradebook_{date_str}.csv"
        )

        final_df.to_csv(output_path, index=False)

        print(f"[EOD] Final tradebook saved: {output_path}")
        self.alerts.notify(f"Final tradebook generated: {output_path}")

        return final_df

    # ---------- MAIN LOOP ----------
    def start(self, symbols, time_frame="5 minutes", candle_for_client=None,
              parameters=["close"], user_positions=None, initial_allocations = None,
              min_required_cash=0.0, stop_on_insufficient=True,
              use_broker_cash_as_capital=True, leverage_multiplier=None):
        
        if leverage_multiplier is not None:
            coerced = self._coerce_leverage_multiplier(leverage_multiplier, "start_kwarg")
            if coerced is not None:
                self.leverage_multiplier = coerced
        resolved_leverage = self._resolve_leverage_multiplier(default=4.0)
        print(f"[LEVERAGE] Active session leverage multiplier: x{resolved_leverage}")

        # ==================== CANDLE NORMALIZATION ====================
        candle = (candle_for_client or "5m").lower().strip()

        if not candle.endswith("m"):
            candle = "5m"

        step = int(candle[:-1])
        # ==============================================================

        # ---- EARLY MARKET-HOURS GUARD (IST) ----
        market_now = self._now_market_time()
        now_time = market_now.time()

        # NSE cash market typical intraday window — start only strictly before square-off
        market_open = dt_time(9, 15)   # 9:15 AM IST

        # Block weekends or at/after 15:00 IST (no new sessions in auto-exit window)
        if market_now.weekday() >= 5 or not (market_open <= now_time < MARKET_EXIT_TIME):
            msg = (
                f"Market closed in IST. Now: "
                f"{market_now.strftime('%Y-%m-%d %H:%M:%S')} AutoTrader will not start."
            )
            print("[INFO]", msg)
            self.alerts.notify(msg)
            return
        # ----------------------------------------

        sess = getattr(self, "session", None)
        has_live = (
            isinstance(sess, dict)
            and sess.get("obj") is not None
            and getattr(self, "broker", None) is not None
        )
        if not has_live:
            try:
                self._link_broker()
            except Exception as e:
                print(f"Failed to link broker during start(): {e}")
                self.alerts.notify("Failed to link broker during start()")
                return

        free_cash = self._get_free_cash()
        if free_cash is None:
            print("WARNING: Could not determine account free cash/margin from broker. Aborting start() for safety.")
            self.alerts.notify("Could not determine account balance. Stopping AutoTrader for safety.")
            return

        print(f"Broker free cash / available margin: {free_cash:,.2f}")
        self.alerts.notify(f"Broker free cash / available margin: {free_cash:,.2f}")

        if use_broker_cash_as_capital:
            self.initial_capital = free_cash
            self.current_capital = free_cash
            self.cash_balance = free_cash
            print(f"[INFO]  Using broker cash as initial capital: {free_cash:,.2f}")
            self.alerts.notify(f"Initial Capital set to broker cash: {free_cash:,.2f}")
        else:
            print(f"[INFO] Using configured initial capital: {self.initial_capital:,.2f} (Broker has {free_cash:,.2f})")
            self.alerts.notify(f"Starting Capital: {self.initial_capital:,.2f}")

        if stop_on_insufficient and free_cash < float(min_required_cash):
            self.alerts.notify(
                f"Insufficient funds to start trading: available {free_cash:,.2f} < required {min_required_cash:,.2f}. Halting."
            )
            print(f"Insufficient funds: required {min_required_cash:.2f}, available {free_cash:.2f}. Exiting.")
            return

        batch_size = 3
        symbol_batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
        
        if initial_allocations:
            self.symbol_allocations = {
                sym: {
                    "capital": alloc["capital"],
                    "stop_loss": alloc.get("stop_loss")
                }
                for sym, alloc in initial_allocations.items()
            }

        # RMS loss limit based on total capital allocated across all symbols
        total_allocated = sum(a["capital"] for a in self.symbol_allocations.values()) if self.symbol_allocations else self.initial_capital
        # self.rms_loss_limit = -(2303.0 / 1_000_000) * total_allocated
        self.rms_loss_limit = -(self.portfolio_max_loss_pct * total_allocated)
        print(f"[RMS] Total allocated capital: Rs{total_allocated:,.2f} | Loss limit: Rs{self.rms_loss_limit:.2f} ({self.portfolio_max_loss_pct*100:.2f}% of capital)")
        self.alerts.notify(f"RMS Loss Limit: Rs{self.rms_loss_limit:.2f} ({self.portfolio_max_loss_pct*100:.2f}% of Rs{total_allocated:,.2f} allocated)")

        # Seed today's already-realized PnL from broker so a same-day restart
        # carries forward prior closed-trade PnL into the RMS calculations.
        self._seed_realized_pnl_from_broker()

        cycle_count = 0
        self._cycle_count = 0
        sync_counter = 0
        
        # ---- START BACKGROUND RECONCILIATION THREAD ----
        if not hasattr(self, "_reconcile_thread"):
            self._reconcile_thread = threading.Thread(
                target=self._reconcile_pending_orders,
                daemon=True
            )
            self._reconcile_thread.start()

        self._last_executed_candle = None

        while True:
            try: 
                if self.stop_event.is_set():
                    print("[AUTO_TRADER] Stop signal mila  shutdown ho raha hoon...")
                    self.shutdown()   #  shutdown call karo, woh khud exit karega
                    break    

                # ==================== FILTER EXITED SYMBOLS ====================
                # If any symbols were manually exited, remove them from the active list
                self._sync_exited_symbols_from_db()
                if self._exited_symbols:
                    before_count = len(symbols)
                    symbols = [
                        s for s in symbols
                        if self._normalize_config_symbol(s) not in self._exited_symbols
                    ]
                    symbol_batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
                    removed = self._exited_symbols.copy()
                    # Don't clear _exited_symbols Ã¢â‚¬â€ keep them excluded permanently
                    if len(symbols) < before_count:
                        print(f"[SINGLE EXIT] Removed {removed} from active symbols. Remaining: {symbols}")
                
                if not symbols:
                    print("[AUTO_TRADER] All symbols have been exited Ã¢â‚¬â€ no more symbols to trade. Stopping.")
                    self.stop_event.set()
                    break
                # ==============================================================

                print("pending orders:", self.pending_orders)
                # =====================================================
                # DOUBLE-EXECUTION GUARD (ONE EXECUTION PER CANDLE)
                # =====================================================
                now = self._now_market_time()

                if not self._stoplock_done and now.time() >= STOP_LOCK_TIME:
                    self._run_stoplock_exits(symbols)
                    self._stoplock_done = True

                #temp change 
                if now.time() >= MARKET_EXIT_WARN_TIME and not self._exit_warning_sent:
                    msg = "2:55 PM IST - Market closing in 5 minutes. All positions will be exited at 3:00 PM IST."
                    print(f"\n{msg}")
                    self.alerts.notify(msg)
                    self._exit_warning_sent = True
                
                # EXIT ALL POSITIONS AT 3:00 PM IST
                if now.time() >= MARKET_EXIT_TIME:
                    print(f"\n[MARKET CLOSE] Current time: {now.strftime('%H:%M:%S')} - Initiating shutdown")
                    
                    # Exit all positions
                    self._exit_all_positions_and_stop()
                    
                    # Stop the trader
                    self.stop_event.set()
                    break
                # -------- HARD CANDLE BOUNDARY GATE --------
            
                # We only execute when minute is exactly on the boundary AND we're within the first few seconds
                # -------- CANDLE-KEY EXECUTION GATE (NO SKIP IF LATE) --------

                candle_key = self._get_candle_key(now, candle)

                print(
                    f"[DEBUG] now={now.strftime('%H:%M:%S')} "
                    f"candle_key={candle_key} "
                    f"candle={candle}"
                )

                # Prevent executing same candle twice
                if self._last_executed_candle == candle_key:
                    print(f"[SKIP] Candle {candle_key} already executed")
                    self._sleep_until_next_candle(candle)
                    continue

                if self.stop_event.is_set():
                    print(f"[SKIP] Stop requested — skipping candle {candle_key}")
                    break

                #  LOCK CANDLE IMMEDIATELY (IMPORTANT)
                self._last_executed_candle = candle_key

                # Compute lateness (drift) for debugging
                try:
                    candle_dt = datetime.strptime(candle_key, "%Y-%m-%d %H:%M").replace(tzinfo=MARKET_TZ)
                    lateness = (now - candle_dt).total_seconds()
                    if lateness > step * 60:
                        print(f"[DRIFT] Late candle execution: {lateness:.1f}s behind for {candle_key}")
                except Exception:
                    pass

                # ====== CANDLE TIME FIX (this will be used for ALL logs in this cycle) ======
                try:
                    candle_dt = datetime.strptime(candle_key, "%Y-%m-%d %H:%M")
                    self.current_cycle_ts = candle_dt.replace(tzinfo=MARKET_TZ)
                    self.current_cycle_ts_str = self.current_cycle_ts.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    self.current_cycle_ts = None
                    self.current_cycle_ts_str = None
                # ==========================================================================

                print(f"[EXECUTE] Candle {candle_key} at {now.strftime('%H:%M:%S')}")
         # ==================== CYCLE TIMING START ====================
                t_cycle_start = time.time()
                self.tlog.start_cycle(cycle_count + 1, candle_key)
                # ============================================================
                if user_positions:
                    for sym, pos in user_positions.items():
                        if sym not in self.positions and pos:
                            self.positions[sym] = pos.copy()
                            
                cycle_count += 1
                self._cycle_count = cycle_count
                sync_counter += 1

                if sync_counter >= 5 and len(self.positions) == 0:
                    self._sync_cash_with_broker()
                    sync_counter = 0
                
                # ====== DEBUG START ======
                print(f"\n{'='*80}")
                print(f"[AUTO_TRADER] PREDICTION CYCLE START")
                print(f"[AUTO_TRADER] Candle: {candle_key}")
                print(f"[AUTO_TRADER] symbols input: {symbols}")
                print(f"[AUTO_TRADER] len(symbols): {len(symbols)}")
                print(f"[AUTO_TRADER] symbol_batches: {symbol_batches}")
                print(f"[AUTO_TRADER] len(symbol_batches): {len(symbol_batches)}")
                print(f"[AUTO_TRADER] time_frame: {time_frame}")
                print(f"[AUTO_TRADER] candle: {candle}")
                print(f"[AUTO_TRADER] parameters: {parameters}")
                print(f"{'='*80}\n")
                # ====== DEBUG END ======

                merged_df_list = []
                t_pred_start = time.time()
                print(f"[TIMING] PREDICTION_BATCH_START  at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
                #parallel order execution for batches of symbols
                

                candle_step = int(candle[:-1]) if candle.endswith("m") else 5
                prediction_candle = candle if candle_step <= 5 else "5m"
                if prediction_candle != candle:
                    print(f"[CANDLE] candle={candle} > 5m, using prediction_candle={prediction_candle} for _run_batch")
                
                def _run_batch(batch):
                    print("inside the run batch function being called from for loop")
                    t0 = time.time()
                    result = self.pred_client.get_prediction_once(
                        batch, time_frame,
                        parameters=parameters,
                        candle=prediction_candle,
                        single_run=True,
                        debug=False
                    )
                    print("prediction call returned from function now check the response time")
                    print(f"[_run_batch] {batch}  completed in {time.time() - t0:.2f}s")
                    self.tlog.record(
                        f"BATCH_PREDICTION",
                        t0,
                        note=f"symbols={batch}"
                        )
                    return result

                with ThreadPoolExecutor(max_workers=len(symbol_batches)) as ex:
                    print("calling run batch function for batches")
                    futures = {ex.submit(_run_batch, b): b for b in symbol_batches}
                    for fut in as_completed(futures):
                        batch = futures[fut]
                        try:
                            t_start = time.time()
                            print(f"[BATCH TIMER] {batch}  waiting for result... ({datetime.now().strftime('%H:%M:%S')})")
                            df = fut.result(timeout=300)
                            t_end = time.time()
                            elapsed = t_end - t_start
                            print(f"[BATCH TIMER] {batch}  got response in {elapsed:.2f}s ({elapsed/60:.2f} min)")
                            print("response coming from prediction service df with timeout 300", df)
                            if df is None:
                                print("response is none from predicton service df is none")
                                self.alerts.notify(f"No response for batch {batch}")
                                continue
                            if isinstance(df, pd.DataFrame) and "Error" in df.columns:
                                print("response has error column from predicton service ")
                                self.alerts.notify(f"Prediction error for batch {batch}")
                                continue
                            print("df response coming from the predicton service ", df)
                            merged_df_list.append(df)
                            print("merged df list is : ",merged_df_list)
                        except Exception as e:
                            t_end = time.time()
                            elapsed = t_end - t_start
                            print(f"[BATCH TIMER] {batch}  FAILED after {elapsed:.2f}s  {e}")

                            self.alerts.notify(f"[ERROR] Batch {batch} failed: {e}")
                            logging.exception("Batch failed")
                

                if not merged_df_list:
                    print("merged df list is empty continue now it will skip the current candle and call the function sleep until next candle")
                    self.alerts.notify("No valid prediction data returned; retrying next cycle...")
                    print("calling sleep_until_next_candle")
                    self.tlog.record("PREDICTION_BATCH_TOTAL", t_pred_start, note="EMPTY_RESULT")

                    self._sleep_until_next_candle(candle)
                    continue

                df = pd.concat(merged_df_list, axis=0)

                # =====================================
                # LOAD SESSION TRENDS FROM REDIS
                # =====================================
                session_trends = {}

                t_trend_total = time.time()
                for sym in symbols:
                    try:
                        t_sym = time.time()
                        redis_key = f"TREND:{sym}"  
                        t_redis = time.time()
                        raw = self.market_client.redis_client.get(redis_key)
                        redis_time = time.time() - t_redis
                        print(
                            f"[TREND REDIS] {sym} | "
                            f"latency={redis_time:.4f}s | "
                            f"status={'MISS' if not raw else 'HIT'}"
                        )
                        self.tlog.record(
                                "TREND_REDIS_GET",
                                t_redis,
                                note=f"{sym}|{'MISS' if not raw else 'HIT'}"
                            )

                        if redis_time > 0.05:
                            print(f"ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¨ SLOW REDIS: {sym} took {redis_time:.3f}s")
                        
                        if not raw:
                            continue

                        trend = json.loads(raw)

                        session_trends[sym] = {
                            "direction": int(trend.get("direction", 0)),
                            "session_open": float(trend.get("session_open", 0.0)),
                            "last_price": float(trend.get("last_price", 0.0)),
                        }
                        total_sym_time = time.time() - t_sym
                        print(
                            f"[TREND TOTAL] {sym} | total_time={total_sym_time:.4f}s"
                        )

                        self.tlog.record(
                            "TREND_PER_SYMBOL_TOTAL",
                            t_sym,
                            note=sym
                        )

                    except Exception as e:
                        print(f"[REDIS WARN] {sym}: {e}")



                print(f"[TREND TOTAL] {sym} | total_time={t_trend_total:.4f}s")

                self.tlog.record(
                                    "TREND_TOTAL",
                                    t_trend_total,
                                    note=sym
                                )
                        
                #  Store candle so _log_trade can fetch correct redis price
                self._current_candle = candle

                #  ONE fresh broker call per cycle  BEFORE all PnL calcs
                t_broker_pos = time.time()
                with self.broker_pos_lock:
                    self._broker_positions_cache = self._get_broker_positions()
                    broker_positions = list(self._broker_positions_cache)
                self.tlog.record("BROKER_POS_FETCH", t_broker_pos, note=f"positions={len(broker_positions)}")

                print(f"[CACHE] {len(broker_positions)} open positions refreshed for cycle")
  
                t_analyze = time.time()
                signals = self._analyze(
                    df, candle,
                    self.positions,
                    session_trends=session_trends
                )
                self._cycle_ltp_cache = {
                    sym: float(info["curr_price"])
                    for sym, info in signals.items()
                    if info.get("curr_price")
                }
                self.tlog.record("ANALYZE_SIGNALS", t_analyze, note=f"symbols={len(signals)}")

                market_now = self._now_market_time()
                t_after_brp_call = time.time()
                print(f"\nCYCLE #{cycle_count} - {market_now.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"Cash Balance: {self.cash_balance:,.2f}")
                print("=" * 120)
                print(
                    f"{'Symbol':<10} {'Curr_Price':>12} {'Trajectory%':>12} "
                    f" {'Side':>6} {'Signal':<35} {'Action Taken':<40}"
                )
                print("-" * 120)

                session_id = getattr(self, "ui_session_id", "default")
                ui_rows = []

                # ========== PARALLEL ORDER EXECUTION - PHASE 1: COLLECT ORDERS ==========
                order_batcher = OrderBatcher()
                
                # ---- BROKER TRUTH POSITION CHECK ----
                t_lock_wait = time.time()

                 # ----- broker_pos_lock wait timer (second cache read) -----
                t_lock_wait = time.time()
                with self.broker_pos_lock:
                    _lock_elapsed = round(time.time() - t_lock_wait, 4)
                    _ = self._broker_positions_cache  # just access
                if _lock_elapsed > 0.05:
                    print(f"[TIMING] BROKER_POS_LOCK_WAIT (signal loop)     {_lock_elapsed:>7.3f}s  >50ms  potential contention")
                    self.tlog._write("BROKER_POS_LOCK_WAIT", _lock_elapsed, note="signal_loop")

                print("starting the for loop having 400 lines of code in between")
                
                
                for sym, info in signals.items():
                    t_sym_loop = time.time()
                    symbol_action_taken = False
                    try:   
                        has_broker_pos = False
                        broker_pos = None
                        
                        print(f"[DEBUG TOP] sym={sym} has_broker_pos={has_broker_pos}")

                        broker_pos = next(
                            (p for p in broker_positions if p["symbol"] == sym),
                            None
                        )

                        has_broker_pos = bool(broker_pos and broker_pos.get("qty", 0) > 0)

                        # Force internal state to match broker
                        if not has_broker_pos and sym not in self.pending_orders:
                            self.positions.pop(sym, None)  
                            
                        print(f"[DEBUG TOP] sym={sym} has_broker_pos={has_broker_pos}")
                        
                        sig = info["signal"]
                        change_pct = info["change_pct"]
                        curr_price = info["curr_price"]
                        side = broker_pos["side"] if has_broker_pos else "NONE"
                        
                        # ===============================
                        # RISK VETO GUARD (EXIT ONLY)
                        # ===============================
                        risk_veto = info.get("_risk_veto", False)

                        # If risk veto is active, allow ONLY exit / cover actions
                        if risk_veto:
                            # Block all OPEN / REVERSE logic
                            if any(k in sig for k in (
                                "Start Long",
                                "Start Short",
                                "Reverse to Long",
                                "Reverse to Short",
                                "OPEN_LONG",
                                "OPEN_SHORT"
                            )):
                                continue
                        
                        # Avoiding noise.
                        risk_regime = info.get("risk_regime", 0)
                        if risk_regime == 0:
                            continue
                        
                        # SCENARIO 0: Hard Stop Loss (Still handled sequentially for safety)
                        if (
                                has_broker_pos
                                and sym in self.symbol_allocations
                                and self._is_position_settled(broker_pos)
                                and sym not in self.pending_orders
                            ):
                            sl_pct = self.symbol_allocations[sym].get("stop_loss")
                            if sl_pct is not None and sl_pct > 0:
                                entry_price = broker_pos["avg_price"]
                                qty = broker_pos["qty"]
                                position_side = broker_pos["side"]
                                if entry_price <= 0 or qty <= 0 or not position_side:
                                    continue
                                stop_hit = False
                                exit_side = None
                                if position_side == "BUY":
                                    stop_price = entry_price * (1 - sl_pct)
                                    if curr_price <= stop_price:
                                        stop_hit = True
                                        exit_side = "SELL"
                                elif position_side == "SELL":
                                    stop_price = entry_price * (1 + sl_pct)
                                    if curr_price >= stop_price:
                                        stop_hit = True
                                        exit_side = "BUY"
                                if position_side == "BUY" and curr_price > entry_price:
                                    stop_hit = False
                                if position_side == "SELL" and curr_price < entry_price:
                                    stop_hit = False

                                if stop_hit:
                                    print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                                    order_batcher.add_order(
                                        OrderRequest(
                                            sym,
                                            exit_side,
                                            qty,
                                            metadata={
                                                "signal": "STOP_LOSS",
                                                "action_type": "STOP_LOSS",
                                                "curr_price": curr_price,
                                                "side": exit_side,
                                                "qty": qty,
                                                "order_value": curr_price * qty,
                                                "position_side": position_side,
                                                "pre_exit_position": self._snapshot_position_for_exit(sym, broker_pos),
                                                "exit_reason": "STOP_LOSS_EXIT",
                                            }
                                        ),
                                        "exit"
                                    )
                                    symbol_action_taken = True
                                    action_taken = "STOP-LOSS ORDER SENT"
                                    symbol_unrealized_pnl = 0.0
                                    symbol_realized_pnl = round(float(self.realized_pnl_by_symbol.get(sym, 0.0)), 2)
                                    symbol_pnl = round(symbol_realized_pnl + symbol_unrealized_pnl, 2)
                                    ui_rows.append({
                                        "symbol": sym,
                                        "curr_price": round(curr_price, 2),
                                        "return_pct": round(info["change_pct"], 4),
                                        "side": "NONE",
                                        "signal": "STOP-LOSS",
                                        "action": action_taken,
                                        "unrealized_pnl": symbol_unrealized_pnl,
                                        "symbol_unrealized_pnl": symbol_unrealized_pnl,
                                        "symbol_realized_pnl": symbol_realized_pnl,
                                        "symbol_pnl": symbol_pnl,
                                        "pnl": symbol_pnl,
                                    })
                                    
                                    print(f"{sym:<10} {curr_price:>12.2f} {info['change_pct']:>12.6f} "
                                        f"{position_side:>6} {'STOP-LOSS':<35} {action_taken:<40}")
                        
                        print(f"[DEBUG TOP] symbol_action_taken : {symbol_action_taken}")
                        # Ensuring One signal per cycle
                        if symbol_action_taken:
                            continue
                        
                        print(f"[DEBUG] risk_veto={risk_veto} sig={sig}")

                        # # SCENARIO 6A: Trend Veto Exit LONG
                        # if "SELL (Trend Veto Exit)" in sig:
                        #     long_pos = next(
                        #         (p for p in broker_positions if p["symbol"] == sym and p["side"] == "BUY"),
                        #         None
                        #     )

                        #     if long_pos:
                        #         print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                        #         order_batcher.add_order(
                        #             OrderRequest(
                        #                 sym,
                        #                 "SELL",
                        #                 long_pos["qty"],
                        #                 metadata={
                        #                     "signal": sig,
                        #                     "change_pct": change_pct,
                        #                     "action_type": "TREND_VETO_EXIT_LONG",
                        #                     "curr_price": curr_price,
                        #                     "side": "SELL",
                        #                     "position_side": "BUY",
                        #                     "qty": long_pos["qty"],
                        #                     "order_value": curr_price * long_pos["qty"]
                        #                 }
                        #             ),
                        #             "exit"
                        #         )
                        #         continue            
                            
                        # # SCENARIO 6B: Trend Veto Exit SHORT
                        # if "BUY (Trend Veto Exit)" in sig:
                        #         short_pos = next(
                        #             (p for p in broker_positions if p["symbol"] == sym and p["side"] == "SELL"),
                        #             None
                        #         )

                        #         if short_pos:
                        #             print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                        #             order_batcher.add_order(
                        #                 OrderRequest(
                        #                     sym,
                        #                     "BUY",
                        #                     short_pos["qty"],
                        #                     metadata={
                        #                         "signal": sig,
                        #                         "change_pct": change_pct,
                        #                         "action_type": "TREND_VETO_EXIT_SHORT",
                        #                         "curr_price": curr_price,
                        #                         "side": "BUY",
                        #                         "position_side": "SELL",
                        #                         "qty": short_pos["qty"],
                        #                         "order_value": curr_price * short_pos["qty"]
                        #                     }
                        #                 ),
                        #                 "exit"
                        #             )
                        #             continue
                        
                        print(f"[DEBUG] before OPEN LONG: symbol_action_taken={symbol_action_taken}")
               
                        # SCENARIO 1: OPEN LONG
                        if sig == "BUY" and not has_broker_pos and sym not in self.pending_orders:
                            scenario_name = "BUY (Fresh Long Entry)"

                            if sym not in self.symbol_qty:
                                if sym in self.symbol_allocations:
                                    capital = self.symbol_allocations[sym]["capital"]
                                else:
                                    capital = self.cash_balance * 0.1
                                self.symbol_qty[sym] = int(capital / curr_price)

                            qty = self.symbol_qty[sym] # lock the price 
                            if qty <= 0:
                                continue

                            order_value = curr_price * qty
                            lock = self._get_symbol_lock(sym)

                            with lock:
                                if not self._can_reserve_exposure(sym, order_value):
                                    print("[DEBUG] exposure blocked", sym, order_value)
                                    continue
                                self._reserve_exposure(sym, order_value)
                                
                            print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                            order_batcher.add_order(
                                OrderRequest(
                                    sym,
                                    "BUY",
                                    qty,
                                    metadata={
                                        "signal": scenario_name,
                                        "action_type": "OPEN_LONG",
                                        "curr_price": curr_price,
                                        "side": "BUY",
                                        "qty": qty,
                                        "order_value": order_value
                                    }
                                ),
                                "buy"
                            )
                            continue
                        
                        # SCENARIO 2: OPEN SHORT
                        if sig == "SELL" and not has_broker_pos and sym not in self.pending_orders:
                            scenario_name = "SELL (Fresh Short Entry)"

                            if sym not in self.symbol_qty:
                                if sym in self.symbol_allocations:
                                    capital = self.symbol_allocations[sym]["capital"]
                                else:
                                    capital = self.cash_balance * 0.1
                                self.symbol_qty[sym] = int(capital / curr_price)

                            qty = self.symbol_qty[sym]
                            if qty <= 0:
                                continue

                            order_value = curr_price * qty
                            lock = self._get_symbol_lock(sym)

                            with lock:
                                if not self._can_reserve_exposure(sym, order_value):
                                    continue
                                self._reserve_exposure(sym, order_value)
                            
                            print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                            order_batcher.add_order(
                                OrderRequest(
                                    sym,
                                    "SELL",
                                    qty,
                                    metadata={
                                        "signal": scenario_name,
                                        "action_type": "OPEN_SHORT",
                                        "curr_price": curr_price,
                                        "side": "SELL",
                                        "qty": qty,
                                        "order_value": order_value
                                    }
                                ),
                                "sell"
                            )
                            continue
                        
                        # SCENARIO 3: EXIT LONG & REVERSE TO SHORT
                        if sig == "SELL" and has_broker_pos and broker_pos["side"] == "BUY" and sym not in self.pending_orders:
                            scenario_name = "SELL (Flip Long to Short)"
                            qty = broker_pos["qty"]
                            inverted_qty = qty*2                 # EXIT LONG -> OPEN SHORT (same qty)
                            print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                            # OPEN SHORT (1 QTY)
                            order_batcher.add_order(
                                OrderRequest(
                                    sym,
                                    "SELL",
                                    inverted_qty,
                                    metadata={
                                        "signal": scenario_name,
                                        "action_type": "FLIP_TO_SHORT",
                                        "curr_price": curr_price,
                                        "side": "SELL",
                                        "qty": inverted_qty,
                                        "order_value": curr_price * inverted_qty
                                    }
                                ),
                                "sell"
                            )
                            continue
                        
                        # SCENARIO 4: EXIT SHORT & REVERSE TO LONG
                        if sig == "BUY" and has_broker_pos and broker_pos["side"] == "SELL" and sym not in self.pending_orders:
                            scenario_name = "BUY (Flip Short to Long)"
                            qty = broker_pos["qty"]
                            inverted_qty = qty*2                # EXIT SHORT -> OPEN LONG (same qty)
                            print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")
                    
                            # OPEN LONG (1 QTY)
                            order_batcher.add_order(
                                OrderRequest(
                                    sym,
                                    "BUY",
                                    inverted_qty,
                                    metadata={
                                        "signal": scenario_name,
                                        "action_type": "FLIP_TO_LONG",
                                        "curr_price": curr_price,
                                        "side": "BUY",
                                        "qty": inverted_qty,
                                        "order_value": curr_price * inverted_qty
                                    }
                                ),
                                "buy"
                            )
                            continue
                        
                        # ================================
                        # SCENARIO 5: LONG EXPANSION
                        # ================================
                        if sig == "BUY" and has_broker_pos and broker_pos["side"] == "BUY" and sym not in self.pending_orders and risk_regime == 2:
                            scenario_name = "BUY (Position Expansion Long)"
                            qty = self.symbol_qty.get(sym, 0)
                            if qty <= 0:
                                continue
                            
                            order_value = curr_price * qty
                            lock = self._get_symbol_lock(sym)

                            with lock:
                                if not self._can_reserve_exposure(sym, order_value):
                                    continue
                                self._reserve_exposure(sym, order_value)
                            
                            print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                            order_batcher.add_order(
                                OrderRequest(
                                    sym,
                                    "BUY",
                                    qty,
                                    metadata={
                                        "signal": scenario_name,
                                        "action_type": "EXPAND_LONG",
                                        "curr_price": curr_price,
                                        "side": "BUY",
                                        "qty": qty,
                                        "order_value": order_value
                                    }
                                ),
                                "buy"
                            )
                            continue
                        
                        # ================================
                        # SCENARIO 7: SHORT EXPANSION
                        # ================================
                        if sig == "SELL" and has_broker_pos and broker_pos["side"] == "SELL" and sym not in self.pending_orders and risk_regime == 2:
                            scenario_name = "SELL (Position Expansion Short)"
                            qty = self.symbol_qty.get(sym, 0)
                            if qty <= 0:
                                continue

                            order_value = curr_price * qty
                            lock = self._get_symbol_lock(sym)

                            with lock:
                                if not self._can_reserve_exposure(sym, order_value):
                                    continue
                                self._reserve_exposure(sym, order_value)
                                
                            print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                            order_batcher.add_order(
                                OrderRequest(
                                    sym,
                                    "SELL",
                                    qty,
                                    metadata={
                                        "signal": scenario_name,
                                        "action_type": "EXPAND_SHORT",
                                        "curr_price": curr_price,
                                        "side": "SELL",
                                        "qty": qty,
                                        "order_value": order_value
                                    }
                                ),
                                "sell"
                            )
                            elapsed = time.time() - t_sym_loop

                            print(f"[TRACE] SYMBOL_LOOP {sym}: {elapsed:.4f}s")

                            self.tlog.record("SYMBOL_LOOP_TOTAL", t_sym_loop, note=sym)

                            continue
      
                    except Exception as e:
                        self.alerts.notify(f"[SYMBOL ERROR] {sym}: {e}")
                        continue
                
                # ========== PHASE 2: EXECUTE ALL ORDERS IN PARALLEL ==========
                if not order_batcher.is_empty() and self.use_parallel_execution:
                    print(f"\n[PARALLEL] Executing {order_batcher.get_count()} orders concurrently...")
                    
                    t_parallel_exec = time.time()
                    
                    self._ensure_parallel_executor()
                    all_orders = order_batcher.get_all_orders(priority="exit_first")
                    results = self.parallel_executor.submit_orders(all_orders)
                    
                    self.tlog.record("PARALLEL_ORDER_EXEC", t_parallel_exec, note=f"orders={len(all_orders)}")


                    symbols_to_fetch = [req.symbol for req in all_orders]
                    def _fetch_ltp_safe(sym):
                        try:
                            price = self._get_live_price_redis(sym, candle)
                            print(f"[LTP] inside _fetch_ltp_safe {sym}: {price}")  
                            return sym, price
                        except Exception:
                            return sym, None

                    ltp_cache = {}
                    print('before the thrreadpool executer of fetchltp')
                    tlog_fetch_ltp_start = time.time()  
                    with ThreadPoolExecutor(max_workers=len(symbols_to_fetch)) as pool:
                        print('inside the thrreadpool executer of fetchltp now fetch ltp will be called')
                        futures = {pool.submit(_fetch_ltp_safe, sym): sym for sym in symbols_to_fetch}
                        for future in as_completed(futures):
                            sym, price = future.result()
                            if price:
                                ltp_cache[sym] = price
                            

                    tlog_fetch_ltp_end = time.time()  
                    print("time taken in fetchltp_price",tlog_fetch_ltp_end-tlog_fetch_ltp_start )
                    self.tlog.record("FETCH_LTP", tlog_fetch_ltp_end, note=f"symbols={len(symbols_to_fetch)}")  


                                
                    # ========== PHASE 3: PROCESS RESULTS ==========
                    for result in results:
                        t_result = time.time()
                        sym = result.symbol.upper().replace("-EQ", "")
                        metadata = result.metadata or {}
                        requested_value = metadata.get("order_value", 0.0)
            
                        action_type = metadata.get("action_type", "")
                        sig = metadata.get("signal", "")
                        change_pct = metadata.get("change_pct", 0.0)
                        curr_price = metadata.get("curr_price", 0.0)
                        
                        if result.success and result.filled:
                            avg_price = result.avg_price
                            filled_qty = result.filled_qty
                            pnl = 0.0
                            action_taken = ""
                            
                            if action_type == "EXIT_LONG":
                                exit_doc = self._finalize_symbol_exit_fill(
                                    sym,
                                    {
                                        "side": metadata.get("side"),
                                        "qty": filled_qty,
                                        "avg_price": avg_price,
                                    },
                                    {
                                        **metadata,
                                        "order_id": result.order_id,
                                        "qty": filled_qty,
                                    },
                                )
                                pnl = float(exit_doc.get("realized_pnl") or 0.0)
                                action_taken = f"CLOSED LONG ({filled_qty}) {avg_price:.2f} | P&L:{pnl:,.2f}"
                            
                            elif action_type == "OPEN_SHORT":
                                action_taken = f"OPEN SHORT ORDER SENT ({filled_qty})"
                                self._log_trade(
                                    sym,
                                    "OPEN_SHORT",
                                    change_pct,
                                    "filled",
                                    avg_price,
                                    filled_qty,
                                    0.0
                                )
                            
                            elif action_type in ["COVER_SHORT"]:
                                exit_doc = self._finalize_symbol_exit_fill(
                                    sym,
                                    {
                                        "side": metadata.get("side"),
                                        "qty": filled_qty,
                                        "avg_price": avg_price,
                                    },
                                    {
                                        **metadata,
                                        "order_id": result.order_id,
                                        "qty": filled_qty,
                                    },
                                )
                                pnl = float(exit_doc.get("realized_pnl") or 0.0)
                                action_taken = f"COVERED SHORT ({filled_qty}) @ {avg_price:.2f} | P&L: {pnl:,.2f}"
                            
                            elif action_type == "STOP_LOSS":
                                exit_doc = self._finalize_symbol_exit_fill(
                                    sym,
                                    {
                                        "side": metadata.get("side"),
                                        "qty": filled_qty,
                                        "avg_price": avg_price,
                                    },
                                    {
                                        **metadata,
                                        "order_id": result.order_id,
                                        "qty": filled_qty,
                                    },
                                )
                                pnl = float(exit_doc.get("realized_pnl") or 0.0)
                                action_taken = f"STOP LOSS EXITED ({filled_qty}) @ {avg_price:.2f} | P&L: {pnl:,.2f}"
                            
                            elif action_type == "OPEN_LONG":
                                action_taken = f"OPEN LONG ORDER SENT ({filled_qty})"
                                self._log_trade(
                                    sym,
                                    "OPEN_LONG",
                                    change_pct,
                                    "filled",
                                    avg_price,
                                    filled_qty,
                                    0.0
                                )
                                                          
                        else:
                            # Order failed at Angel before getting an order_id
                            # (validation rejects like AB1019 / AB4036, RMS rejects, etc).
                            # Do NOT add to pending_orders — there is nothing to reconcile.
                            # Release the reserved exposure so the symbol can trade again
                            # in later cycles. Log it and move on to the next order.
                            if not result.order_id:
                                order_value = metadata.get("order_value", 0.0)
                                try:
                                    self._release_exposure(sym, order_value)
                                except Exception as _re:
                                    print(f"[REJECT] {sym}: release_exposure failed: {_re}")

                                live_price = ltp_cache.get(sym) or curr_price or 0.0
                                if not live_price:
                                    try:
                                        live_price = self._get_live_price_redis(sym, candle) or 0.0
                                    except Exception:
                                        live_price = 0.0

                                err_msg = result.error or "unknown error"
                                print(f"[REJECT] {sym} {action_type}: {err_msg} — skipping, cycle continues")
                                try:
                                    self.alerts.notify(f"Order rejected for {sym} ({action_type}): {err_msg}")
                                except Exception:
                                    pass
                                try:
                                    self._log_trade(sym, action_type, change_pct, "rejected",
                                                    live_price, metadata.get("qty", 0), 0.0)
                                except Exception as _le:
                                    print(f"[REJECT] {sym}: log_trade failed: {_le}")
                                continue

                            with self.pending_lock:
                                self.pending_orders[sym].append({
                                    "order_id": result.order_id,
                                    "action_type": action_type,
                                    "side": metadata.get("side"),
                                    "qty": metadata.get("qty"),
                                    "order_value": metadata.get("order_value", 0.0),
                                    "position_side": metadata.get("position_side"),
                                    "pre_exit_position": metadata.get("pre_exit_position"),
                                    "exit_reason": metadata.get("exit_reason"),
                                    "placed_at": time.time(),
                                    # "metadata": metadata   # redundant
                                })
                            if action_type in {"EXIT_LONG", "COVER_SHORT", "STOP_LOSS"}:
                                pre_exit = metadata.get("pre_exit_position") or self._snapshot_position_for_exit(sym)
                                self._persist_exit_pending(
                                    sym,
                                    exit_reason=self._resolve_exit_reason(action_type, metadata),
                                    exit_side=metadata.get("side"),
                                    qty=metadata.get("qty"),
                                    entry_price=pre_exit.get("entry_price", 0.0),
                                    exit_order_id=result.order_id,
                                )

                            # Log with live redis price instead of 0.0
                            t_ltp = time.time()

                            live_price = ltp_cache.get(sym) or curr_price
                            if live_price is None:
                                live_price = self._get_live_price_redis(sym, candle) or curr_price

                            ltp_time = time.time() - t_ltp

                            print(f"[TRACE] RESULT_LTP from get_live_price_redis {sym}: {ltp_time:.3f}s")

                            self.tlog.record("RESULT_LTP_FETCH", t_ltp, note=sym)
                            
                            t_log = time.time()

                            self._log_trade(sym, action_type, change_pct, "pending", live_price, metadata.get("qty", 0), 0.0)
                            log_time = time.time() - t_log

                            print(f"[TRACE] LOG_TRADE {sym}: {log_time:.3f}s")

                            self.tlog.record("LOG_TRADE_TIME", t_log, note=sym)

                            print(f"  {sym}: {action_type} PENDING (order sent) @ {live_price:.2f}")

                            result_total = time.time() - t_result

                            print(f"[TRACE] RESULT_TOTAL {sym}: {result_total:.3f}s")

                            self.tlog.record("RESULT_PROCESS_TOTAL", t_result, note=sym)


                with self.pending_lock:
                    pending_syms = set(self.pending_orders.keys())


                # ========== CONTINUE WITH HOLD POSITIONS ==========
                for sym, info in signals.items(): 
                    t_sym = time.time()            
                    broker_pos = next(
                            (p for p in broker_positions if p["symbol"] == sym),
                            None
                        )

                    has_broker_pos = bool(broker_pos and broker_pos.get("qty", 0) > 0)
                    
                    sig = info["signal"]
                    change_pct = info["change_pct"]
                    curr_price = info["curr_price"]
                    side = broker_pos["side"] if has_broker_pos else "NONE"

                    if has_broker_pos:
                        if broker_pos["side"] == "BUY":
                            action_taken = "HOLD (Continue Long)"
                        else:
                            action_taken = "HOLD (Continue Short)"
                    else:
                        action_taken = "HOLD (Flat)"
                    
                    #  Calculate live PnL once here for all hold paths
                    t_pnl = time.time()

                    live_pnl = self._calculate_pnl(sym, curr_price) if has_broker_pos else 0.0
                    
                    pnl_time = time.time() - t_pnl

                    print(f"[TRACE] PNL {sym}: {pnl_time:.4f}s")

                    self.tlog.record("PNL_CALC", t_pnl, note=sym)
                    held_qty = broker_pos.get("qty", 0) if has_broker_pos else 0

                    # SCENARIO 8 : WAIT NO POSITION
                    if not has_broker_pos and sym not in pending_syms and self._stock_exposure(sym) == 0:
                        action_taken = "WAIT (no position)"
                        self._log_trade(sym, sig, change_pct, "wait", curr_price, 0, 0.0)
                    
                    # SCENARIO 9 : PENDING STATUS
                    elif sym in pending_syms:
                        action_taken = "PENDING (order sent)"
                        self._log_trade(sym, sig, change_pct, "pending", curr_price, held_qty, live_pnl)

                    #  SCENARIO 10 : HOLD WITH OPEN POSITION  log with live PnL
                    elif has_broker_pos:
                        # self._log_trade(sym, sig, change_pct, "hold", curr_price, held_qty, live_pnl)
                        t_log = time.time()

                        self._log_trade(sym, sig, change_pct, "hold", curr_price, held_qty, live_pnl)

                        log_time = time.time() - t_log

                        print(f"[TRACE] LOG_TRADE {sym}: {log_time:.3f}s")

                        self.tlog.record("HOLD_LOG_TRADE", t_log, note=sym) 
                    
                    symbol_unrealized_pnl = round(live_pnl, 2)
                    symbol_realized_pnl = round(float(self.realized_pnl_by_symbol.get(sym, 0.0)), 2)
                    symbol_pnl = round(symbol_realized_pnl + symbol_unrealized_pnl, 2)

                    ui_rows.append({
                        "symbol": sym,
                        "curr_price": round(curr_price, 2),
                        "return_pct": round(change_pct, 4),
                        "side": side,
                        "signal": sig,
                        "action": action_taken,
                        "unrealized_pnl": symbol_unrealized_pnl,
                        "symbol_unrealized_pnl": symbol_unrealized_pnl,
                        "symbol_realized_pnl": symbol_realized_pnl,
                        "symbol_pnl": symbol_pnl,
                        "pnl": symbol_pnl,
                    })


                    elapsed = time.time() - t_sym

                    print(f"[TRACE] HOLD_LOOP {sym}: {elapsed:.3f}s")

                    self.tlog.record("HOLD_LOOP_PER_SYMBOL", t_sym, note=sym)
                    
                    print(f"{sym:<10} {curr_price:>12.2f} {change_pct:>12.6f} "
                        f"{side:>6} {sig:<35} {action_taken:<40}")

                t_pnl_loop = time.time()

                self.unrealized_pnl = 0.0

                for sym, info in signals.items():
                    t_pnl = time.time()

                    curr_price = info["curr_price"]
                    pnl = self._calculate_pnl(sym, curr_price)

                    self.unrealized_pnl += pnl

                    pnl_time = time.time() - t_pnl

                    print(f"[TRACE] PNL_LOOP {sym}: {pnl_time:.4f}s")

                    self.tlog.record("PNL_PER_SYMBOL", t_pnl, note=sym)

                # total loop
                print(f"[TRACE] PNL_LOOP_TOTAL: {time.time() - t_pnl_loop:.3f}s")

                self.tlog.record("PNL_LOOP_TOTAL", t_pnl_loop)
                self.current_capital = self.cash_balance + self.unrealized_pnl

                # ==================== RMS: DAILY LOSS LIMIT CHECK ====================
                # Cycle-level portfolio RMS removed — handled by tick-driven
                # _check_live_portfolio_rms() in on_ltp_tick (LiveLTPStream thread).
                # rms_loss_limit / rms_triggered are still set/used by that path.
                # ======================================================================

                t_ui = time.time()

                
                self._update_ui_snapshot(
                    session_id=session_id,
                    cycle=cycle_count,
                    rows=ui_rows,
                )

                ui_time = time.time() - t_ui

                print(f"[TRACE] UI_SNAPSHOT: {ui_time:.3f}s")

                self.tlog.record("UI_SNAPSHOT_TOTAL", t_ui)
                
                print(f"Realized PnL: {self.realized_pnl:.2f}")
                print(f"Unrealized PnL: {self.unrealized_pnl:.2f}")
                print(f"Total Equity: {self.current_capital:.2f}")


                # ==================== CYCLE TOTAL TIME ====================
                self.tlog.record("CYCLE_TOTAL", t_cycle_start, note=f"cycle={cycle_count}")
                total_cycle_sec = round(time.time() - t_cycle_start, 2)
                candle_budget_sec = step * 60
                if total_cycle_sec > candle_budget_sec * 0.8:
                    print(
                        f"[TIMING WARNING] Cycle took {total_cycle_sec:.1f}s / budget {candle_budget_sec}s "
                        f"({100*total_cycle_sec/candle_budget_sec:.0f}%)  RISK OF CANDLE SKIP!"
                    )

                # ==============================
                # BACKUP EXIT AT 3:00 PM IST (end-of-cycle safety net)
                # ==============================
                market_now = self._now_market_time()
                now_time = market_now.time()
                print("time after analyse after second broker api call : ", time.time()- t_after_brp_call)
                self.tlog.record("time after analyse after second broker api call" ,t_after_brp_call , note="time analysis of delay")

                if now_time >= MARKET_EXIT_TIME:
                    self.alerts.notify("Backup market close triggered (3:00 PM IST)")
                    print("\n" + "=" * 70)
                    print("BACKUP MARKET CLOSE (3:00 PM IST) - AUTO-TRADING STOPPED")
                    print("=" * 70)

                    print("\n[BACKUP EXIT] Attempting to exit remaining positions...")
                    self._exit_all_positions_and_stop()

                    self.stop_event.set()
                    break
                else:
                    self._sleep_until_next_candle(candle)
                    
            except RuntimeError as e:
                if "SESSION_EXPIRED_RELOGIN_REQUIRED" in str(e):
                    self.alerts.notify("Broker session expired. Manual restart required.")

                    self.stop_event.set()
                    break
                # Any other RuntimeError: log it, keep the cycle alive.
                print(f"[CYCLE WARN] RuntimeError (non-auth): {e}")
                try:
                    print(traceback.format_exc())
                except Exception:
                    pass
                try:
                    self.alerts.notify(f"Cycle warning (continuing): {e}")
                except Exception:
                    pass
                try:
                    self._sleep_until_next_candle(candle)
                except Exception:
                    time.sleep(1)
                continue
            except Exception as e:
                # Catch-all: never let the trading thread die because of a transient
                # parse / network / data error. Log, alert, sleep to next candle.
                print(f"[CYCLE WARN] Unhandled {type(e).__name__}: {e}")
                try:
                    print(traceback.format_exc())
                except Exception:
                    pass
                try:
                    self.alerts.notify(f"Cycle warning (continuing): {type(e).__name__}: {e}")
                except Exception:
                    pass
                try:
                    self._sleep_until_next_candle(candle)
                except Exception:
                    time.sleep(1)
                continue

    def shutdown(self):
        self.stop_event.set()
        print("[SHUTDOWN] Pehle open positions exit kar raha hoon...")
        try:
            self._exit_all_positions_and_stop()  #  sirf yahan, ek baar
        except Exception as e:
            print(f"[SHUTDOWN] Exit failed: {e}")
        
        if hasattr(self, 'parallel_executor') and self.parallel_executor:
            self.parallel_executor.stop()
        self.reserved_exposure.clear()
        self.symbol_locks.clear()
        # Drain async CSV writer so no in-flight rows are lost
        if hasattr(self, '_csv_logger') and self._csv_logger:
            try:
                self._csv_logger.stop()
            except Exception as e:
                print(f"[SHUTDOWN] AsyncCsvLogger stop failed: {e}")


        
