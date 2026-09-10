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
from queue import Queue
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import threading
import logging
from trading_snapshot import insert_trading_snapshot
from utils.session_symbols import filter_broker_positions_for_session

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
    """Represents the result of an order execution"""
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
    """Token bucket rate limiter for API calls"""
    
    def __init__(self, max_calls: int, time_window: float):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = []
        self.lock = threading.Lock()
    
    def acquire(self):
        """Wait until a call can be made within rate limits"""
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
    """Executes orders in parallel while respecting API rate limits"""
    
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
        """Worker thread that processes orders from the queue"""
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
        """Execute a single order with rate limiting"""
        try:
            self.order_limiter.acquire()
            
            print(
                f"\n[EXECUTOR] Sending order ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ "
                f"symbol={order_req.symbol} "
                f"side={order_req.side} "
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
            
            # Fetch order status after brief delay    
            time.sleep(0.5)
            try:
                ob = self.session["obj"].orderBook()
                if isinstance(ob, dict) and ob.get("status") and ob.get("data"):
                    for o in ob["data"]:
                        if str(o.get("orderid")) == str(order_id):
                            print("\n[ANGEL RMS DECISION]")
                            print("status         :", o.get("orderstatus"))
                            print("rejectionreason:", o.get("rejectionreason"))
                            print("statusmessage  :", o.get("statusmessage"))
                            print("text           :", o.get("text"))
            except Exception as e:
                print("[EXECUTOR] orderBook error:", e)
                
            # PHASE A: fire-and-forget (do NOT wait for confirmation)
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
        """Start worker threads"""
        self.stop_flag.clear()
        self.workers = []
        
        for i in range(self.max_workers):
            worker = threading.Thread(target=self._worker, name=f"OrderWorker-{i}")
            worker.daemon = True
            worker.start()
            self.workers.append(worker)
    
    def stop(self):
        """Stop worker threads gracefully"""
        self.stop_flag.set()
        for _ in self.workers:
            self.order_queue.put(None)
        for worker in self.workers:
            worker.join(timeout=5)
        self.workers = []
    
    def submit_orders(self, orders: List[OrderRequest]) -> List[OrderResult]:
        """Submit multiple orders for parallel execution"""
        if not self.workers:
            self.start()
        
        for order in orders:
            self.order_queue.put(order)
        
        results = []
        for _ in orders:
            try:
              result = self.result_queue.get(timeout=30)
            except Exception:
              result = OrderResult(
                  symbol="UNKNOWN",
                  success=False,
                  error="Order execution timeout"
              )
            results.append(result)
        
        self.order_queue.join()
        return results


class OrderBatcher:
    """Helper class to batch orders by type for efficient parallel execution"""
    
    def __init__(self):
        self.buy_orders = []
        self.sell_orders = []
        self.cover_orders = []
        self.exit_orders = []
    
    def add_order(self, order: OrderRequest, order_category: str = "general"):
        """Add order to appropriate batch"""
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
    
    def get_all_orders(self, priority: str = "exit_first") -> List[OrderRequest]:
        """Get all orders in optimal execution order"""
        if priority == "exit_first":
            return (self.exit_orders + self.cover_orders + 
                   self.sell_orders + self.buy_orders)
        else:
            return (self.buy_orders + self.sell_orders + 
                   self.cover_orders + self.exit_orders)
    
    def clear(self):
        """Clear all batches"""
        self.buy_orders = []
        self.sell_orders = []
        self.cover_orders = []
        self.exit_orders = []
    
    def is_empty(self) -> bool:
        """Check if all batches are empty"""
        return not (self.buy_orders or self.sell_orders or 
                   self.cover_orders or self.exit_orders)
    
    def get_count(self) -> int:
        """Get total number of orders"""
        return (len(self.buy_orders) + len(self.sell_orders) + 
                len(self.cover_orders) + len(self.exit_orders))

# ==================== END PARALLEL ORDER EXECUTOR ====================

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
        # ====================================================
        
        
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
        self.trade_history = []
        self.stop_event = threading.Event()
        self._exit_warning_sent = False


        # ==================== PARALLEL EXECUTION SETUP ====================
        self.parallel_executor = None
        self.use_parallel_execution = True  # Set False to disable parallel execution
        # ==================================================================

            # ==================== TIMING LOGGER ====================
        # Initialised here so every method can call self.tlog.record(...)
        # The actual log_dir may not exist yet â€” TimingLogger creates it.
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
    
    
    # ---------- TIME HELPERS ----------
    
    def _now_market_time(self):
        """Return current market datetime in IST."""
        return datetime.now(MARKET_TZ)
    
    # ---------- SYMBOL LOCK (thread-safe exposure updates) -----------
    
    def _get_symbol_lock(self, symbol):
        if symbol not in self.symbol_locks:
            self.symbol_locks.setdefault(symbol, threading.Lock())
        return self.symbol_locks[symbol]

    def _get_bulk_broker_ltp(self, symbols):
        """
        Read closed candle price from Redis â€” the exact same price
        prediction service wrote after fetching the closed candle.
        Redis key: price:live:{ticker}.NS  (set by prediction_service.py)
        Falls back to broker LTP if Redis has nothing.
        """
        ltp_map = {}
        t_func_start = time.time()

        # âœ… Get Redis client from market_client (same Redis prediction service uses)
        redis_client = getattr(self.market_client, "redis_client", None)

        for sym in symbols:
            try:
                ticker = f"{sym}.NS"
                redis_key = f"price:live:{ticker}"   # matches redis_key_live_price(ticker) in prediction_service.py

                # âœ… Step 1: Try Redis first â€” this is what prediction service wrote
                if redis_client is not None:
                    cached = redis_client.get(redis_key)
                    if cached:
                        try:
                            data = json.loads(cached)
                            price = float(data["price"])
                            ts    = data.get("ts", "unknown")
                            if price > 0:
                                ltp_map[sym] = price
                                print(f"[LTP REDIS] {sym} = {price:.2f} (candle close @ {ts})")
                                continue
                        except Exception as e:
                            print(f"[LTP REDIS PARSE ERROR] {sym}: {e}")

                # âœ… Step 2: Fallback â€” broker LTP if Redis miss
                print(f"[LTP REDIS] {sym}: cache miss, falling back to broker LTP")
                if not self._ensure_session():
                    continue

                instruments = [f"NSE|{sym}-EQ"]
                resp = self.broker.get_bulk_ltp(self.session, instruments)

                if not isinstance(resp, dict):
                    continue

                data = resp.get("data") or resp.get("raw", {}).get("data")
                if not isinstance(data, list):
                    continue

                for row in data:
                    ltp = row.get("ltp")
                    if ltp is not None and float(ltp) > 0:
                        ltp_map[sym] = float(ltp)
                        print(f"[LTP BROKER FALLBACK] {sym} = {float(ltp):.2f}")
                        break

            except Exception as e:
                print(f"[LTP ERROR] {sym}: {e}")
                continue

        print(f"[LTP FINAL] {ltp_map}")
        self.tlog.record("LTP_BULK_FETCH_TOTAL", t_func_start, note=f"symbols={len(symbols)} hits={len(ltp_map)}")

        return ltp_map

    # ---------- RESERVED (PENDING) EXPOSURE READ --------------
    
    def _reserved_exposure(self, symbol):
        return self.reserved_exposure.get(symbol, 0.0)


    # ---------- TOTAL SYMBOL EXPOSURE (FILLED + RESERVED) -------------
    
    def _total_symbol_exposure(self, symbol):
        return self._stock_exposure(symbol) + self._reserved_exposure(symbol)


    # ---------- EXPOSURE CAP CHECK (ATOMIC) ----------------
    
    def _can_reserve_exposure(self, symbol, order_value):
        max_allowed = self.max_exposure_pct * self.initial_capital
        return (self._total_symbol_exposure(symbol) + order_value) <= max_allowed


    # ---------- RESERVE EXPOSURE (BEFORE ORDER SUBMIT) -------------
    
    def _reserve_exposure(self, symbol, order_value):
        self.reserved_exposure[symbol] = (
            self.reserved_exposure.get(symbol, 0.0) + order_value
        )


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

                # âœ… Pehle orderstatus check karo
                order_status = order.get("orderstatus", "").lower()

                if order_status == "cancelled":
                    print(f"[FILL PRICE] {symbol}: order {order_id} CANCELLED hai â€” fill price nahi milegi")
                    return 0.0

                if order_status == "rejected":
                    print(f"[FILL PRICE] {symbol}: order {order_id} REJECTED hai â€” fill price nahi milegi")
                    return 0.0

                if order_status not in ("complete", "filled"):
                    # open, pending, trigger pending etc.
                    print(f"[FILL PRICE] {symbol}: order {order_id} abhi {order_status} hai â€” wait karo")
                    return 0.0

                # âœ… Order complete hai â€” ab averageprice lo
                # fill_price = float(order.get("price") or 0.0)
                fill_price = float(order.get("price") or 0.0)
                avg_price_field = float(order.get("averageprice") or 0.0)
                print(
                    f"[FILL PRICE] {symbol}: order {order_id} | "
                    f"price={fill_price:.2f} | averageprice={avg_price_field:.2f}"
                )

                # âœ… filledshares bhi check karo
                filled_shares = int(order.get("filledshares") or 0)

                # if fill_price > 0 and filled_shares > 0:
                #     print(f"[FILL PRICE] {symbol}: order {order_id} complete @ â‚¹{fill_price:.2f} ({filled_shares} shares)")
                #     return fill_price
                # else:
                #     print(f"[FILL PRICE] {symbol}: order complete but averageprice=0 ya filledshares=0")
                #     return 0.0


                if fill_price > 0 and filled_shares > 0:
                    print(f"[FILL PRICE] {symbol}: order {order_id} complete @ â‚¹{fill_price:.2f} (price field) | averageprice=â‚¹{avg_price_field:.2f} ({filled_shares} shares)")
                    return fill_price
                elif avg_price_field > 0 and filled_shares > 0:
                    print(f"[FILL PRICE] {symbol}: price=0 fallback to averageprice=â‚¹{avg_price_field:.2f}")
                    return avg_price_field
                else:
                    print(f"[FILL PRICE] {symbol}: order complete but both price=0 and averageprice=0 or filledshares=0")
                    return 0.0
                
            print(f"[FILL PRICE] {symbol}: order {order_id} order book mein nahi mila")
            return 0.0

        except Exception as e:
            print(f"[FILL PRICE ERROR] {symbol}: {e}")
            return 0.0

    
    # ------- HANDLE FILLED -------- 
       
    # def _handle_filled(self, symbol, broker_pos, ctx):
    #     action_type = ctx.get("action_type", "")
    #     if broker_pos["qty"] <= 0:
    #         return
    #     if action_type in {
    #         "EXIT_LONG",
    #         "COVER_SHORT",
    #         "TREND_VETO_EXIT_LONG",
    #         "TREND_VETO_EXIT_SHORT",
    #         "STOP_LOSS",
    #         "MARKET_CLOSE_EXIT"
    #     }:
    #         self.positions.pop(symbol, None)
    #         return    
    #     self.positions[symbol] = {
    #         "side": broker_pos["side"],
    #         "qty": broker_pos["qty"],
    #         "entry_price": broker_pos["avg_price"]
    #     }
    #     print(
    #         f"{symbol}: POSITION SET"
    #         f"{broker_pos['side']} {broker_pos['qty']} @ {broker_pos['avg_price']}"
    #     )


    

# ----------------------------------------------------------------
# FUNCTION 1 â€” _handle_filled (FIXED)
# ----------------------------------------------------------------

    def _handle_filled(self, symbol, broker_pos, ctx):
        """
        Jab order fill confirm ho jaaye tab ye function call hota hai.

        Entry pe  â†’ self.positions mein position daalo (sahi entry price ke saath)
        Exit pe   â†’ P&L calculate karo, phir position hatao
        """
        action_type = ctx.get("action_type", "")

        # Agar qty hi nahi hai toh kuch mat karo
        if broker_pos["qty"] <= 0:
            return

        # ================================================================
        # EXIT ACTIONS â€” Pehle P&L calculate karo, phir position hatao
        # ================================================================
        if action_type in {
            "EXIT_LONG",
            "COVER_SHORT",
            "TREND_VETO_EXIT_LONG",
            "TREND_VETO_EXIT_SHORT",
            "STOP_LOSS",
            "MARKET_CLOSE_EXIT"
        }:
            # âœ… Step 1: Exit price lo â€” broker_pos se aayegi (reconciliation ne set ki hogi)
            exit_price = float(broker_pos.get("avg_price") or 0.0)

            # âœ… Step 2: Agar exit price nahi mili toh LTP cache fallback
            # (LTP bilkul sahi nahi hoti exit price ke liye, but better than 0)
            if exit_price <= 0:
                exit_price = getattr(self, "_cycle_ltp_cache", {}).get(symbol, 0.0)
                if exit_price > 0:
                    print(f"[WARN] {symbol}: exit price broker se nahi mili, LTP use kar rahe hain: â‚¹{exit_price:.2f}")

            # âœ… Step 3: P&L calculate karo (self.positions abhi bhi exist karti hai)
            if exit_price > 0 and symbol in self.positions:
                pnl = self._close_position(self.session, symbol, exit_price)
                # _close_position andar se self.positions.pop() bhi karta hai
                print(f"[P&L REALIZED] {symbol} | Action: {action_type} | Realized: â‚¹{pnl:.2f}")
                self._log_trade(
                    symbol,
                    action_type,
                    0.0,
                    "closed",
                    exit_price,
                    broker_pos["qty"],
                    pnl
                )
            else:
                # Exit price nahi mili ya position nahi thi â€” bas hatao
                print(f"[WARN] {symbol}: exit price nahi mili ya position exist nahi karti â€” sirf pop kar rahe hain")
                self.positions.pop(symbol, None)

            return

        # ================================================================
        # ENTRY ACTIONS â€” Position save karo sahi entry price ke saath
        # ================================================================

        # âœ… Step 1: Broker se jo avg_price aaya wo lo
        entry_price = float(broker_pos.get("avg_price") or 0.0)

        # âœ… Step 2: Agar broker ne 0 diya (same candle issue) toh
        #    order book se actual fill price nikalo
        if entry_price <= 0:
            order_id = ctx.get("order_id")
            if order_id:
                entry_price = self._get_fill_price_from_orderbook(order_id, symbol)
                if entry_price > 0:
                    print(f"[ENTRY PRICE] {symbol}: order book se mili â‚¹{entry_price:.2f}")

        # âœ… Step 3: Order book se bhi nahi mili toh position save mat karo
        #    Next reconciliation cycle mein phir try hoga
        if entry_price <= 0:
            print(f"[WARN] {symbol}: entry price nahi mili â€” position set nahi hua, next cycle mein retry hoga")
            return

        # âœ… Step 4: Sahi entry price ke saath position save karo
        self.positions[symbol] = {
            "side": broker_pos["side"],
            "qty": broker_pos["qty"],
            "entry_price": entry_price        # â† actual fill price âœ…
        }

        print(
            f"[POSITION SET] {symbol}: "
            f"{broker_pos['side']} {broker_pos['qty']} @ â‚¹{entry_price:.2f}"
        )
               
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

        if candle.endswith("m"):
            step = int(candle[:-1])
        else:
            step = 5  # safe default

        # Round DOWN to nearest candle boundary
        minute_bucket = (now.minute // step) * step

        return now.replace(
            minute=minute_bucket,
            second=0,
            microsecond=0
        ).strftime("%Y-%m-%d %H:%M")
        
    def _get_live_price_redis(self, symbol: str, candle: str) -> Optional[float]:
        """
        Worker-independent LIVE price cache using Redis.
        Keyed by candle boundary so it auto-refreshes every candle.
        """
        
        print(
            f"[LTP FUNC] symbol={symbol} candle={candle} "
            f"redis={'YES' if getattr(self.market_client, 'redis_client', None) else 'NO'} "
            f"fetch_price={'YES' if hasattr(self.market_client, 'fetch_price') else 'NO'}",
            flush=True
        )

        try:
            # Ensure redis exists
            redis_client = getattr(self.market_client, "redis_client", None)
            if redis_client is None:
                return None

            now = self._now_market_time()
            candle_key = self._get_candle_key(now, candle)

            # Unique key per candle = no stale bleed into next candle
            # redis_key = f"LTP:{symbol}"
            
            ticker_ns = f"{symbol}.NS"
            redis_key = f"price:live:{ticker_ns}" 


            # 1) Try redis cache first
            cached = redis_client.get(redis_key)
            # if cached:
            #     try:
            #         return float(cached)
            #     except Exception:
            #         pass
            if cached:
                try:
                    return float(cached)
                except (ValueError, TypeError):
                    pass
                try:
                    data = json.loads(cached)
                    return float(data["price"])
                except Exception:
                    pass


            # 2) Fetch live price using client.fetch_price (your existing Upstox function)
            if not hasattr(self.market_client, "fetch_price"):
                return None

            ticker = f"{symbol}.NS"
            px = self.market_client.fetch_price(
                ticker=ticker,
                target_datetime=now,
                candle=candle
            )

            if not px:
                return None

            live = px.get("Close")
            if live is None:
                return None

            live = float(live)
            if live <= 0:
                return None

            # 3) Cache in Redis: short TTL (safe)
            # Since key is candle-specific, TTL is just to clean up memory.
            
            # redis_client.setex(redis_key, 5, str(live))
            
            candle_ttl = 60 if candle == "1m" else 310   # was: hardcoded 5
            redis_client.setex(redis_key, candle_ttl, str(live))

            return live

        except Exception as e:
            print(f"[LIVE PRICE REDIS ERROR] {symbol}: {e}")
            return None

    def _sleep_until_next_candle(self, candle):

       

        candle = str(candle or "5m").lower().strip()
        step = int(candle[:-1]) if candle.endswith("m") else 5

        now = self._now_market_time()
        print("printing now: ", now)

        # Floor to current candle boundary
        minute_bucket = (now.minute // step) * step
        current_boundary = now.replace(
            minute=minute_bucket,
            second=0,
            microsecond=0
        )
        print("printing current_boundary: ", current_boundary)
        # Always target NEXT candle
        next_run = current_boundary + timedelta(minutes=step)
        print("Next run calculated as: ", next_run)

        # next_run = current_boundary + timedelta(minutes=step) + timedelta(seconds=60)


        # If we're already late, skip missed candles safely
        if next_run <= now:
            missed = int((now - next_run).total_seconds() // (step * 60)) + 1
            next_run += timedelta(minutes=missed * step)

        sleep_seconds = max(1, (next_run - now).total_seconds())

        print(
            f"[SCHEDULER] now={now.strftime('%H:%M:%S')} "
            f"next={next_run.strftime('%H:%M:%S')} "
            f"sleep={sleep_seconds:.1f}s"
    )



        deadline = time.time() + sleep_seconds
        while time.time() < deadline:
            if self.stop_event.is_set():
                print("[SCHEDULER] Stop signal mila neend mein â€” uth raha hoon!")
                return   # â† neend se uthta hai, loop pe wapas jaata hai
            time.sleep(1)

        print(
            f"[SCHEDULER] now={now.strftime('%H:%M:%S')} "
            f"next={next_run.strftime('%H:%M:%S')} "
            f"sleep={sleep_seconds:.1f}s"
        )

        # time.sleep(sleep_seconds)

    def _exit_all_positions_and_stop(self):
        try:
            print("exiting all postiions from market")
            angel_orders = fetch_todays_intraday_orders()
            self._generate_final_merged_tradebook(angel_orders=angel_orders)
        except Exception as e:
            print(f"[EOD MERGE ERROR] {e}")
        
        print("\n" + "=" * 80)
        print("  MARKET CLOSE APPROACHING - EXITING SESSION POSITIONS")
        print("=" * 80)
        
        self.alerts.notify(" 1:30 PM - Initiating exit of session positions")
        
        # Get current broker positions
        with self.broker_pos_lock:
            self._broker_positions_cache = self._get_broker_positions()
            broker_positions = list(self._broker_positions_cache or [])

        session_id = getattr(self, "ui_session_id", None) or getattr(self, "session_id", None)
        redis_client = getattr(self.market_client, "redis_client", None)
        broker_positions = filter_broker_positions_for_session(
            broker_positions,
            session_id,
            redis_client,
            fallback_symbols=list((self.symbol_allocations or {}).keys()),
            on_skip=lambda sym: self.alerts.notify(
                f"EOD skip {sym} (not a session symbol)"
            ),
        )
        
        if not broker_positions:
            print("[INFO]  No session positions to exit")
            self.alerts.notify(" No session positions to exit - Auto trader stopped")
            return True
        
        print(f"[INFO] Found {len(broker_positions)} session position(s) to exit")
        
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
                
                if result.success:
                    successful_exits += 1
                    
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
                    live_price = getattr(self, "_cycle_ltp_cache", {}).get(sym) or curr_price
                    self._log_trade(
                        sym,
                        "MARKET_CLOSE_EXIT",   
                        0.0,                   
                        "pending",
                        live_price,
                        metadata.get("qty", 0),
                        0.0
                    )
                    print(f"  {sym}: MARKET_CLOSE_EXIT PENDING (order sent) @ {live_price:.2f}")
                    
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
        
        # Clear internal positions
        self.positions.clear()
        
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
        
        return True
    

    def _reconcile_pending_orders(self):
        while not self.stop_event.is_set():
            with self.pending_lock:
                if not self.pending_orders:
                    time.sleep(1)
                    continue    

                # pending_orders: symbol -> list[ctx]
                pending_snapshot = list(self.pending_orders.items())

            t_reconcile_start = time.time()

           
            with self.broker_pos_lock:

                self._broker_positions_cache = self._get_broker_positions()

                broker_positions = list(self._broker_positions_cache or [])

            for sym, ctx_list in pending_snapshot:
                # iterate over a COPY so we can safely remove
                for ctx in list(ctx_list):

                    expected_side = ctx.get("side")
                    expected_qty = ctx.get("qty", 0)
                    action_type = ctx.get("action_type", "")
                    order_id       = ctx.get("order_id")

                    is_exit = action_type in {
                        "EXIT_LONG",
                        "COVER_SHORT",
                        "TREND_VETO_EXIT_LONG",
                        "TREND_VETO_EXIT_SHORT",
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
                        flipped_pos = None
                        for p in broker_positions:
                            if p.get("symbol") != sym:
                                continue
                            if p.get("side") != expected_side:
                                continue
                            if p.get("qty", 0) <= 0:
                                continue
                            flipped_pos = p
                            break

                        if flipped_pos:
                            fill_price = float(flipped_pos.get("avg_price") or 0.0)
                            if fill_price <= 0 and order_id:
                                fill_price = self._get_fill_price_from_orderbook(order_id, sym)
                            self._handle_filled(
                                sym,
                                {
                                    "side": flipped_pos["side"],
                                    "qty": flipped_pos["qty"],       
                                    "avg_price": flipped_pos["avg_price"],
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
                                print(f"[RECONCILE] {sym}: avg_price order book se mili â‚¹{avg_price:.2f}")
                            else:
                                print(f"[RECONCILE] {sym}: avg_price abhi bhi 0 â€” next cycle mein retry hoga")

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
                                exit_price = self._get_fill_price_from_orderbook(order_id, sym)

                            # âœ… Fallback â€” LTP cache se lo (better than 0)
                            if exit_price <= 0:
                                exit_price = getattr(self, "_cycle_ltp_cache", {}).get(sym, 0.0)
                                if exit_price > 0:
                                    print(f"[WARN] {sym}: exit price order book se nahi mili, LTP use kar rahe hain â‚¹{exit_price:.2f}")

                            self._handle_filled(
                                sym,
                                {
                                    "side": expected_side,
                                    "qty": expected_qty,
                                    "avg_price": None  # price already realized
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
            "total_equity": round(
                self.cash_balance + self.realized_pnl + self.unrealized_pnl, 2
            ),
            "symbols": rows
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
        )
        except Exception as e:
            print(f"[DB ERROR] Trading snapshot insert failed: {e}")

    

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
            live_price = getattr(self, "_cycle_ltp_cache", {}).get(symbol)

            print(
                f"[LIVE RESULT] {symbol} "
                f"live_price={live_price} "
                f"({'FALLBACK  predicted[0]' if live_price is None else 'USING LIVE'})",
                flush=True
            )
            
            # fallback to predicted first point if live fetch fails
            current_price = float(live_price) if live_price else float(predicted_path[0])
            
            print(
                f"[CURR PRICE FINAL] {symbol} curr_price={current_price}",
                flush=True
            )          
            
            # -------------------------------
            traj_col = group["trajectory_pct"].values if "trajectory_pct" in group.columns else None
            regime_col = group["risk_regime"].values if "risk_regime" in group.columns else None

            if traj_col is not None and len(traj_col) > 0 and not np.isnan(traj_col[0]):
                # Use slot 0's trajectory Ã¢â‚¬â€ the most current signal
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

            # ===============================
            # SESSION TREND VETO (HARD RULE)
            # ===============================
            session_direction = None
            session_open = None

            if session_trends and symbol in session_trends:
                session_direction = session_trends[symbol].get("direction")
                session_open = session_trends[symbol].get("session_open")

            # If LONG but session trend is DOWN FORCE EXIT
            if position_side == "BUY" and session_direction == -1 and session_open and current_price < session_open:
                signal = "SELL (Trend Veto Exit)"
                signals[symbol] = {
                    "signal": signal,
                    "change_pct": trajectory_pct,
                    "curr_price": current_price,
                    "side": position_side,
                    "interval": swing_interval,
                    "risk_regime": risk_regime
                }
                continue

            # If SHORT but session trend is UP FORCE COVER
            if position_side == "SELL" and session_direction == 1 and session_open and current_price > session_open:
                signal = "BUY (Trend Veto Exit)"
                signals[symbol] = {
                    "signal": signal,
                    "change_pct": trajectory_pct,
                    "curr_price": current_price,
                    "side": position_side,
                    "interval": swing_interval,
                    "risk_regime": risk_regime
                }
                continue

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
                    signal = "HOLD"

            elif position_side == "SELL":
                if trajectory_pct > 0:
                    signal = "BUY"  
                else:
                    signal = "HOLD"


            # -------------------------------
            # OUTPUT
            # -------------------------------
            signals[symbol] = {
                "signal": signal,
                "change_pct": trajectory_pct,
                "curr_price": current_price,
                "side": position_side,
                "interval": swing_interval,
                "risk_regime": risk_regime
            }

        return signals        
    # ---------- P&L / POSITIONS ----------
    def _calculate_pnl(self, symbol, ltp): 
        if ltp is None or ltp <= 0:
            return 0.0
        entry_price = 0.0
        qty = 0
        side = None

        if symbol in self.positions:
            pos = self.positions[symbol]
            entry_price = float(pos.get("entry_price", 0.0))
            qty = int(pos.get("qty", 0))
            side = pos.get("side")

             # âœ… Agar entry_price galat save hua tha (0), broker cache se update karne ki koshish
            if entry_price <= 0:
                with self.broker_pos_lock:
                    broker_positions = list(self._broker_positions_cache or [])

                for p in broker_positions:
                    if p.get("symbol") != symbol:
                        continue
                    fresh_price = float(p.get("avg_price", 0.0))
                    if fresh_price > 0:
                        # âœ… Broker ne ab sahi price de di â€” update karo
                        self.positions[symbol]["entry_price"] = fresh_price
                        entry_price = fresh_price
                        print(f"[ENTRY PRICE UPDATE] {symbol}: â‚¹{fresh_price:.2f} (broker cache se fix hua)")
                    break


        
        # ================================================================
        # Priority 2 â€” broker cache fallback (agar self.positions mein nahi)
        # ================================================================
        else:
            with self.broker_pos_lock:
                broker_positions = list(self._broker_positions_cache or [])

            for p in broker_positions:
                if p.get("symbol") != symbol:
                    continue

                qty = int(p.get("qty", 0))
                if qty <= 0:
                    return 0.0

                side = p.get("side")
                entry_price = float(p.get("avg_price", 0.0))
                break
            # ================================================================
        # Validation
        # ================================================================
        if entry_price <= 0:
            print(f"[PNL WARN] {symbol}: entry_price nahi mili â€” P&L = 0")
            return 0.0

        if qty <= 0:
            return 0.0

        if side not in ("BUY", "SELL"):
            return 0.0

            # ================================================================
        # P&L Formula
        # ================================================================
        if side == "BUY":
            pnl = (ltp - entry_price) * qty
        else:
            pnl = (entry_price - ltp) * qty

        # ================================================================
        # Sanity Check â€” P&L kabhi total investment se zyada nahi honi chahiye
        # ================================================================


        if abs(pnl) > (entry_price * qty):
            print(
                f"[P&L SANITY BREACH] {symbol} | "
                f"pnl={pnl:.2f}, entry={entry_price:.2f}, qty={qty}, ltp={ltp:.2f}"
            )
            if not hasattr(self, "pnl_sanity_errors"):
                self.pnl_sanity_errors = []
            self.pnl_sanity_errors.append({
                "symbol": symbol,
                "pnl": pnl,
                "entry_price": entry_price,
                "qty": qty,
                "ltp": ltp,
                "timestamp": datetime.now().isoformat()
            })
            return 0.0

        return round(pnl, 2)
        

    def convert_candle_to_seconds(self, c):
        c = str(c).lower().strip()

        if c.endswith("m"):
            return int(c[:-1]) * 60

        return 300

    def _close_position(self, session, symbol, exit_price):
        if symbol not in self.positions:
            return 0.0

        pos = self.positions[symbol]
        qty = pos["qty"]
        entry = pos["entry_price"]
        side = pos["side"]

        # Realized PnL
        if side == "BUY":
            profit = (exit_price - entry) * qty
        else:
            profit = (entry - exit_price) * qty

        if side == "BUY":
            self.cash_balance += qty * exit_price
        else:
            pass

        self.realized_pnl += profit
        self.positions.pop(symbol, None)

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

        #  Always use live LTP cache as price source
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

        # âœ… WRITE TO CSV IMMEDIATELY â€” bar by bar, every cycle
        try:
            with open(self.log_path, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    candle_time,
                    symbol,
                    signal,
                    round(change, 6),
                    status,
                    round(price, 2),
                    round(unrealized, 2),
                    total_equity,
                    portfolio_return,
                ])
        except Exception as e:
            print(f"[LOG ERROR] Failed to write trade log: {e}")
    
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
              use_broker_cash_as_capital=True):
        
        # ==================== CANDLE NORMALIZATION ====================
        candle = (candle_for_client or "5m").lower().strip()

        if not candle.endswith("m"):
            candle = "5m"

        step = int(candle[:-1])
        # ==============================================================

        # ---- EARLY MARKET-HOURS GUARD (IST) ----
        market_now = self._now_market_time()
        now_time = market_now.time()

        # NSE cash market typical intraday window
        market_open  = dt_time(9, 15)   # 9:15 AM IST
        market_close = dt_time(15, 20)  # 3:20 PM IST (your existing cutoff)

        # Block weekends or outside this time window #temp block 
        if market_now.weekday() >= 5 or not (market_open <= now_time <= market_close):
            msg = (
                f"Market closed in IST. Now: "
                f"{market_now.strftime('%Y-%m-%d %H:%M:%S')} AutoTrader will not start."
            )
            print("[INFO]", msg)
            self.alerts.notify(msg)
            return
        # ----------------------------------------

        if not getattr(self, "session", None) or not getattr(self, "broker", None):
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

        cycle_count = 0
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
                    print("[AUTO_TRADER] Stop signal mila â€” shutdown ho raha hoon...")
                    self.shutdown()   # â† shutdown call karo, woh khud exit karega
                    break

                print("pending_orders:", self.pending_orders)  # Debug print

                if self.stop_event.is_set():
                    print("[AUTO_TRADER] Stop signal mila â€” loop band kar raha hoon...")
                    break
                
                # =====================================================
                # DOUBLE-EXECUTION GUARD (ONE EXECUTION PER CANDLE)
                # =====================================================
                now = self._now_market_time()
                # warning_time = dt_time(15, 25)  # 3:25 PM IST
                # #temp block 
                # # print("[DEBUG] Temp Bocked time")
                # if now.time() >= warning_time and not self._exit_warning_sent:
                #     msg = " 2:25 PM - Market closing in 5 minutes. All positions will be exited at 1:30 PM."
                #     print(f"\n{msg}")
                #     self.alerts.notify(msg)
                #     self._exit_warning_sent = True
                
                # # EXIT ALL POSITIONS AT 3:30 PM IST
                # market_exit_time = dt_time(15, 30) # 3:30 PM IST
                
                # if now.time() >= market_exit_time:
                #     print(f"\n[MARKET CLOSE] Current time: {now.strftime('%H:%M:%S')} - Initiating shutdown")
                    
                #     # Exit all positions
                #     self._exit_all_positions_and_stop()
                    
                #     # Stop the trader
                #     self.stop_event.set()
                #     break
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

                # ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬â„¢ LOCK CANDLE IMMEDIATELY (IMPORTANT)
                self._last_executed_candle = candle_key

                # Compute lateness (drift) for debugging
                try:
                    candle_dt = datetime.strptime(candle_key, "%Y-%m-%d %H:%M").replace(tzinfo=MARKET_TZ)
                    lateness = (now - candle_dt).total_seconds()
                    if lateness > step * 60:
                        print(f"[DRIFT] Late candle execution: {lateness:.1f}s behind for {candle_key}")
                except Exception:
                    pass

                # # LOCK this candle immediately
                # self._last_executed_candle = candle_key
                # -------------------------------------------------------------


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
                print("intialised empty merged df list")
                merged_df_list = []

                  # ---- PREDICTION BATCH START ----
                t_pred_start = time.time()
                print(f"[TIMING] PREDICTION_BATCH_START  at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")


                #serial executer 
                # for batch_idx, batch in enumerate(symbol_batches):
                #     # ====== DEBUG START ======
                #     print(f"\n[AUTO_TRADER] >>> BATCH {batch_idx+1}/{len(symbol_batches)}: {batch}")
                #     # ====== DEBUG END ======
                #     try:
                #         print(
                #             f"[PRED CALL] batch={batch} "
                #             f"time_frame={time_frame} candle={candle}",
                #             flush=True
                #         )
                #         print(f"[AUTO_TRADER] Calling pred_client.get_prediction_once()...")
                #         df = self.pred_client.get_prediction_once(
                #             batch,
                #             time_frame,
                #             parameters=parameters,
                #             candle=candle,
                #             single_run=True,
                #             debug=False
                #         )
                        
                #         print(f"[AUTO_TRADER] Prediction call returned")
                #         print(f"[AUTO_TRADER]   df is None: {df is None}")
                        
                #         if df is None:
                #             print(f"[AUTO_TRADER] df is None, skipping batch")
                #             self.alerts.notify(
                #                 f"No response from prediction API for batch {batch}; skipping this batch."
                #             )
                #             continue
                        
                #         if isinstance(df, pd.DataFrame) and "Error" in df.columns and df.iloc[0].get("Error"):
                #             print(f"[AUTO_TRADER] df has Error column, skipping batch")
                #             self.alerts.notify(
                #                 f"Prediction API error for batch {batch}: {df.iloc[0].get('Error')}"
                #             )
                #             continue
                        
                #         print(f"[AUTO_TRADER] ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬ ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¦ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â¦ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œ Valid df, appending to merged_df_list")
                #         merged_df_list.append(df)
                #         # #locked candle 
                #         # self._last_executed_candle = candle_key


                #     except Exception as e:
                #         self._last_executed_candle = None
                #         self.current_cycle_ts = None
                #         self.current_cycle_ts_str = None
                #         self.alerts.notify(f"[ERROR] Cycle failed: {e}")
                #         logging.exception("Main loop crashed")
                #         self._sleep_until_next_candle(candle)
                #         continue
                # lock candle AFTER all batches completed successfully

                #parallel executer for predictions 
 
                def _run_batch(batch):
                    print("inside the run batch function being called from for loop")
                    t0 = time.time()
                    result = self.pred_client.get_prediction_once(
                        batch, time_frame,
                        parameters=parameters,
                        candle=candle,
                        single_run=True,
                        debug=False
                    )
                    print("prediction call returned from function now check the response time")
                    print(f"[_run_batch] {batch} â†’ completed in {time.time() - t0:.2f}s")
                    self.tlog.record(
                        f"BATCH_PREDICTION",
                        t0,
                        note=f"symbols={batch}"
                        )
                    return result

                # with ThreadPoolExecutor(max_workers=len(symbol_batches)) as ex:
                #     print("calling run batch function for batches")
                #     futures = {ex.submit(_run_batch, b): b for b in symbol_batches}
                #     for fut in as_completed(futures):
                #         batch = futures[fut]
                #         try:
                #             df = fut.result(timeout=240)
                #             print("response coming from prediction service df with timeout 240",df)

                #             if df is None:
                #                 print("df is none continue")
                #                 self.alerts.notify(f"No response for batch {batch}")
                #                 continue
                #             if isinstance(df, pd.DataFrame) and "Error" in df.columns:
                #                 print("error in columns of df")
                #                 self.alerts.notify(f"Prediction error for batch {batch}")
                #                 continue
                #             print("mergin df list")
                #             merged_df_list.append(df)
                #         except Exception as e:
                #             print("error occured in exception block  ")
                #             self.alerts.notify(f"[ERROR] Batch {batch} failed: {e}")
                #             logging.exception("Batch failed")
                with ThreadPoolExecutor(max_workers=len(symbol_batches)) as ex:
                    print("calling run batch function for batches")
                    futures = {ex.submit(_run_batch, b): b for b in symbol_batches}
                    for fut in as_completed(futures):
                        batch = futures[fut]
                        try:
                            t_start = time.time()
                            print(f"[BATCH TIMER] {batch} â†’ waiting for result... ({datetime.now().strftime('%H:%M:%S')})")

                            df = fut.result(timeout=300)

                            t_end = time.time()
                            elapsed = t_end - t_start
                            print(f"[BATCH TIMER] {batch} â†’ got response in {elapsed:.2f}s ({elapsed/60:.2f} min)")
                            print("response coming from prediction service df with timeout 300", df)

                            if df is None:
                                print("df is none continue")
                                self.alerts.notify(f"No response for batch {batch}")
                                continue
                            if isinstance(df, pd.DataFrame) and "Error" in df.columns:
                                print("error in columns of df")
                                self.alerts.notify(f"Prediction error for batch {batch}")
                                continue
                            print("mergin df list")
                            merged_df_list.append(df)
                        except Exception as e:
                            t_end = time.time()
                            elapsed = t_end - t_start
                            print(f"[BATCH TIMER] {batch} â†’ FAILED after {elapsed:.2f}s â€” {e}")
                            self.alerts.notify(f"[ERROR] Batch {batch} failed: {e}")
                            logging.exception("Batch failed")

                if not merged_df_list:
                    print("merged df list is empty continue now it will skip the current candle and call the function sleep until next candle")
                    self.alerts.notify("No valid prediction data returned; retrying next cycle...")
                    print("calling sleep_until_next_candle")
                    self.tlog.record("PREDICTION_BATCH_TOTAL", t_pred_start, note="EMPTY_RESULT")
                    print( "total batch prediction me itna time laga : ",time.time()-t_pred_start)
                    self._sleep_until_next_candle(candle)
                    continue
                self.tlog.record("PREDICTION_BATCH_TOTAL", t_pred_start, note=f"batches={len(symbol_batches)}")

                df = pd.concat(merged_df_list, axis=0)

                # =====================================
                # LOAD SESSION TRENDS FROM REDIS
                # =====================================
                session_trends = {}

                for sym in symbols:
                    try:
                        redis_key = f"TREND:{sym}"  
                        raw = self.market_client.redis_client.get(redis_key)

                        if not raw:
                            continue

                        trend = json.loads(raw)

                        session_trends[sym] = {
                            "direction": int(trend.get("direction", 0)),
                            "session_open": float(trend.get("session_open", 0.0)),
                            "last_price": float(trend.get("last_price", 0.0)),
                        }

                    except Exception as e:
                        print(f"[REDIS WARN] {sym}: {e}")

                print("[LTP] Fetching BULK broker prices...")
                t_ltp = time.time()


                self._cycle_ltp_cache = self._get_bulk_broker_ltp(symbols)

                print("[LTP] Received:", self._cycle_ltp_cache)

                # âœ… ONE fresh broker call per cycle â€” BEFORE analyze and BEFORE all PnL calcs
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
                self.tlog.record("ANALYZE_SIGNALS", t_analyze, note=f"symbols={len(signals)}")

                with self.broker_pos_lock:
                    self._broker_positions_cache = self._get_broker_positions()

                market_now = self._now_market_time()
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
                
                # ---- BROKER TRUTH POSITION CHECK (already refreshed above) ----
                # broker_positions already set fresh above â€” reuse it here


               
                # ----- broker_pos_lock wait timer (second cache read) -----
                t_lock_wait = time.time()
                with self.broker_pos_lock:
                    _lock_elapsed = round(time.time() - t_lock_wait, 4)
                    _ = self._broker_positions_cache  # just access
                if _lock_elapsed > 0.05:
                    print(f"[TIMING] BROKER_POS_LOCK_WAIT (signal loop)     {_lock_elapsed:>7.3f}s  >50ms â€” potential contention")
                    self.tlog._write("BROKER_POS_LOCK_WAIT", _lock_elapsed, note="signal_loop")
                # -----------------------------------------------------------

                for sym, info in signals.items():
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
                                # Skip this signal entirely
                                continue
                        
                        # Avoiding noise
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
                                                "order_value": curr_price * qty
                                            }
                                        ),
                                        "exit"
                                    )
                                    symbol_action_taken = True
                                    action_taken = "STOP-LOSS ORDER SENT"
                                    ui_rows.append({
                                        "symbol": sym,
                                        "curr_price": round(curr_price, 2),
                                        "return_pct": round(info["change_pct"], 4),
                                        "side": "NONE",
                                        "signal": "STOP-LOSS",
                                        "action": action_taken,
                                        "unrealized_pnl": 0.0
                                    })
                                    
                                    print(f"{sym:<10} {curr_price:>12.2f} {info['change_pct']:>12.6f} "
                                        f"{position_side:>6} {'STOP-LOSS':<35} {action_taken:<40}")
                        
                        print(f"[DEBUG TOP] symbol_action_taken : {symbol_action_taken}")
                        # Ensuring One signal per cycle
                        if symbol_action_taken:
                            continue
                        
                        print(f"[DEBUG] risk_veto={risk_veto} sig={sig}")

                        # SCENARIO 6A: Trend Veto Exit LONG
                        if "SELL (Trend Veto Exit)" in sig:
                            long_pos = next(
                                (p for p in broker_positions if p["symbol"] == sym and p["side"] == "BUY"),
                                None
                            )

                            if long_pos:
                                print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                                order_batcher.add_order(
                                    OrderRequest(
                                        sym,
                                        "SELL",
                                        long_pos["qty"],
                                        metadata={
                                            "signal": sig,
                                            "change_pct": change_pct,
                                            "action_type": "TREND_VETO_EXIT_LONG",
                                            "curr_price": curr_price,
                                            "side": "SELL",
                                            "position_side": "BUY",
                                            "qty": long_pos["qty"],
                                            "order_value": curr_price * long_pos["qty"]
                                        }
                                    ),
                                    "exit"
                                )
                                continue            
                            
                        # SCENARIO 6B: Trend Veto Exit SHORT
                        if "BUY (Trend Veto Exit)" in sig:
                                short_pos = next(
                                    (p for p in broker_positions if p["symbol"] == sym and p["side"] == "SELL"),
                                    None
                                )

                                if short_pos:
                                    print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                                    order_batcher.add_order(
                                        OrderRequest(
                                            sym,
                                            "BUY",
                                            short_pos["qty"],
                                            metadata={
                                                "signal": sig,
                                                "change_pct": change_pct,
                                                "action_type": "TREND_VETO_EXIT_SHORT",
                                                "curr_price": curr_price,
                                                "side": "BUY",
                                                "position_side": "SELL",
                                                "qty": short_pos["qty"],
                                                "order_value": curr_price * short_pos["qty"]
                                            }
                                        ),
                                        "exit"
                                    )
                                    continue
                        
                        print(f"[DEBUG] before OPEN LONG: symbol_action_taken={symbol_action_taken}")
               
                        # SCENARIO 1: OPEN LONG
                        if sig == "BUY" and not has_broker_pos and sym not in self.pending_orders:
                            scenario_name = "BUY (Fresh Long Entry)"

                            if sym in self.symbol_allocations:
                                capital = self.symbol_allocations[sym]["capital"]
                            else:
                                capital = self.cash_balance * 0.1

                            qty = int(capital / curr_price)
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

                            if sym in self.symbol_allocations:
                                capital = self.symbol_allocations[sym]["capital"]
                            else:
                                capital = self.cash_balance * 0.1

                            qty = int(capital / curr_price)
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
                            inverted_qty = qty*2                 # EXIT LONG -> OPEN SHORT (DOUBLE QTY)
                            
                            print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                            # OPEN SHORT (DOUBLE QTY)
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
                            inverted_qty = qty*2              # EXIT SHORT -> OPEN LONG (DOUBLE QTY)
                            
                            print(f"[ORDER QUEUED] {sym} {sig} qty={qty}")

                            # OPEN LONG (DOUBLE QTY)
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
                        
                        # ==================================================
                        # SAME-SIDE SIGNAL NO-OP                        
                        # ==================================================
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
                    # ========== PHASE 3: PROCESS RESULTS ==========
                    for result in results:
                        sym = result.symbol
                        metadata = result.metadata or {}
                        requested_value = metadata.get("order_value", 0.0)

                        # filled_value = (
                        #     result.avg_price * result.filled_qty
                        #     if result.success and result.filled
                        #     else 0.0
                        # )
            
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
                                pnl = self._close_position(self.session, sym, avg_price)
                                action_taken = f"CLOSED LONG ({filled_qty}) {avg_price:.2f} | P&L:{pnl:,.2f}"
                                self._log_trade(sym, "CLOSE_LONG", change_pct, "filled", avg_price, filled_qty, pnl)
                            
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
                                pnl = self._close_position(self.session, sym, avg_price)
                                action_taken = f"COVERED SHORT ({filled_qty}) @ {avg_price:.2f} | P&L: {pnl:,.2f}"
                                self._log_trade(sym, "CLOSE_SHORT", change_pct, "filled", avg_price, filled_qty, pnl)
                            
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
                            with self.pending_lock:
                                self.pending_orders[sym].append({
                                    "order_id": result.order_id,
                                    "action_type": action_type,
                                    "side": metadata.get("side"),
                                    "qty": metadata.get("qty"),
                                    "order_value": metadata.get("order_value", 0.0),
                                    "placed_at": time.time(),
                                    # "metadata": metadata   # redundant
                                })

                            # âœ… live price log
                            live_price = getattr(self, "_cycle_ltp_cache", {}).get(sym) or curr_price
                            self._log_trade(sym, action_type, change_pct, "pending", live_price, metadata.get("qty", 0), 0.0)
                            print(f"  {sym}: {action_type} PENDING (order sent) @ {live_price:.2f}")
                
                with self.pending_lock:
                    pending_syms = set(self.pending_orders.keys())


                # ========== CONTINUE WITH HOLD POSITIONS ==========
                for sym, info in signals.items():             
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
                    
                    # âœ… Calculate live PnL once here for all hold paths
                    live_pnl = self._calculate_pnl(sym, curr_price) if has_broker_pos else 0.0
                    held_qty = broker_pos.get("qty", 0) if has_broker_pos else 0

                    # SCENARIO 9 : WAIT NO POSITION
                    if not has_broker_pos and sym not in pending_syms and self._stock_exposure(sym) == 0:
                        action_taken = "WAIT (no position)"
                        self._log_trade(sym, sig, change_pct, "wait", curr_price, 0, 0.0)
                    
                    # SCENARIO 10 : PENDING STATUS
                    elif sym in pending_syms:
                        action_taken = "PENDING (order sent)"
                        self._log_trade(sym, sig, change_pct, "pending", curr_price, held_qty, live_pnl)

                    # âœ… SCENARIO 11 : HOLD WITH OPEN POSITION â€” log with live PnL
                    elif has_broker_pos:
                        self._log_trade(sym, sig, change_pct, "hold", curr_price, held_qty, live_pnl)
       
                    ui_rows.append({
                        "symbol": sym,
                        "curr_price": round(curr_price, 2),
                        "return_pct": round(change_pct, 4),
                        "side": side,
                        "signal": sig,
                        "action": action_taken,
                        "unrealized_pnl": round(self._calculate_pnl(sym, curr_price), 2),
                    })
                    
                    print(f"{sym:<10} {curr_price:>12.2f} {change_pct:>12.6f} "
                        f"{side:>6} {sig:<35} {action_taken:<40}")

                # ---- BROKER-ALIGNED UNREALIZED PNL (SINGLE SOURCE OF TRUTH) ----
                self.unrealized_pnl = 0.0
                for sym, info in signals.items():
                    curr_price = info["curr_price"]
                    self.unrealized_pnl += self._calculate_pnl(sym, curr_price)


                self.current_capital = self.cash_balance + self.unrealized_pnl
                
                self._update_ui_snapshot(
                    session_id=session_id,
                    cycle=cycle_count,
                    rows=ui_rows,
                )
                
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
                        f"({100*total_cycle_sec/candle_budget_sec:.0f}%) â€” RISK OF CANDLE SKIP!"
                    )

                # ==============================
                # BACKUP EXIT AT 3:20 PM CHECK
                # ==============================
                market_now = self._now_market_time()
                now_time = market_now.time()
                cutoff_time = dt_time(15, 20)

               

                if now_time >= cutoff_time:
                    self.alerts.notify("Backup market close triggered (3:20 PM) - This shouldn't happen!")
                    print("\n" + "=" * 70)
                    print("BACKUP MARKET CLOSE - AUTO-TRADING STOPPED")
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
                raise                       
    # ==================== CLEANUP METHOD ====================
    # def shutdown(self):
    #     """Cleanup method - call this when stopping the bot"""
    #     if hasattr(self, 'parallel_executor') and self.parallel_executor:
    #         print("[CLEANUP] Stopping parallel order executor...")
    #         self.parallel_executor.stop()
    #         print("[CLEANUP]  Parallel executor stopped")
    #     # ---- RESET IN-MEMORY RISK STATE ----
    #     self.reserved_exposure.clear()
    #     self.symbol_locks.clear()
    #     # Optional: prevent reuse without re-init
    #     self.stop_event.set()                
    # ========================================================

    def shutdown(self):
        print("[SHUTDOWN] Pehle open positions exit kar raha hoon...")
        try:
            self._exit_all_positions_and_stop()  # â† sirf yahan, ek baar
        except Exception as e:
            print(f"[SHUTDOWN] Exit failed: {e}")
        
        if hasattr(self, 'parallel_executor') and self.parallel_executor:
            self.parallel_executor.stop()
        self.reserved_exposure.clear()
        self.symbol_locks.clear()
        self.stop_event.set()