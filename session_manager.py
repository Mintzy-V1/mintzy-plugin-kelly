
import os
import time
import json
import redis
import threading
import multiprocessing as mp
from multiprocessing import Process, Queue, Event, Manager
from queue import Empty
from typing import Dict, Any, Optional
from datetime import datetime
import traceback

from auto_trader import AutoTrader as AutoTraderA
from auto_trader_exposure_expansion import AutoTrader as AutoTraderB
from alerts import AlertManager
from client import PredictionClient, MarketClient
from broker_angle import BrokerConnector
from live_ltp_ws import LiveLTPStream
import signal

SIMULATION_STOP_PREFIX = "autotrader:simulation_stop:"
SIMULATION_STOP_TTL = 300



class WorkerProcess:
    """Wrapper for a trader process with health monitoring"""
    
    def __init__(self, session_id: str, process: Process, 
                 stop_event: Event, health_queue: Queue):
        self.session_id = session_id
        self.process = process
        self.stop_event = stop_event
        self.health_queue = health_queue
        self.last_heartbeat = time.time()
        self.started_at = time.time()
        
    def is_alive(self) -> bool:
        return self.process.is_alive()
    
    def is_healthy(self, timeout: int = 60) -> bool:
        """Check if worker sent heartbeat recently"""
        return (time.time() - self.last_heartbeat) < timeout
    
    def update_heartbeat(self):
        self.last_heartbeat = time.time()


def _trader_worker(
    session_id: str,
    strategy: str,
    symbols: list,
    allocations: dict,
    time_frame: str,
    candle: str,
    broker_config: dict,
    trading_logs_collection_name: str,
    stop_event: Event,
    health_queue: Queue,
    mongo_uri: str,
    mongo_db_name: str,
    configuration_id: Optional[str] = None,
):
    """
    Worker process that runs a single AutoTrader instance.
    Isolated from other traders - has its own Python interpreter and memory space.
    """
    import signal
    def handle_sigterm(signum, frame):
        print(f"[Worker-{session_id}] SIGTERM mila Ã¢â‚¬â€ graceful shutdown...")
        stop_event.set()   # Ã¢â€ Â stop_event set kar do Ã¢â‚¬â€ baaki sab automatically hoga
    
    signal.signal(signal.SIGTERM, handle_sigterm)

    try:
        # Set up environment for this worker
        os.environ["ANGEL_API_KEY"] = broker_config["api_key"]
        os.environ["ANGEL_CLIENT_CODE"] = broker_config["client_code"]
        os.environ["ANGEL_PASSWORD"] = broker_config["password"]
        
        # Initialize broker
        broker = BrokerConnector(require_totp=False)
        restored = broker.restore_session(broker_config["broker_session"])
        
        # Clean up env immediately
        for k in ["ANGEL_API_KEY", "ANGEL_CLIENT_CODE", "ANGEL_PASSWORD"]:
            os.environ.pop(k, None)
        
        if not restored or not restored.get("token"):
            health_queue.put({
                "session_id": session_id,
                "status": "error",
                "error": "Failed to restore broker session"
            })
            return
        
        # Set up MongoDB connection (each process gets its own connection)
        from pymongo import MongoClient
        mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        mongo_db = mongo_client[mongo_db_name]
        trading_logs_collection = mongo_db[trading_logs_collection_name]
        config_db_name = os.environ.get("MONGO_CONFIG_DB_NAME", mongo_db_name)
        if config_db_name != mongo_db_name:
            print(
                f"[Worker-{session_id}] SavedTradingConfiguration DB: {config_db_name} "
                f"(sessions/logs: {mongo_db_name})"
            )
        
        # Initialize clients
        prediction_client = PredictionClient(
            api_key="XeyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
            base_url=os.environ.get("PREDICTION_BASE_URL", "http://54.204.215.28:8000/predict")
        )
        market_client = MarketClient()
        
        # Redis client for inter-process messaging (single-symbol exit requests)
        exit_redis_client = getattr(market_client, 'redis_client', None)
        
        # Select trader class (C is lazy-imported so api_server boots even if org module differs on disk)
        TRADER_MAP = {"A": AutoTraderA, "B": AutoTraderB}
        if strategy == "C":
            try:
                from auto_trader_exposure_expansion_org import AutoTrader as AutoTraderC
                TRADER_MAP["C"] = AutoTraderC
            except ImportError as e:
                health_queue.put({
                    "session_id": session_id,
                    "status": "error",
                    "error": (
                        f"Strategy C (auto_trader_exposure_expansion_org) unavailable: {e}. "
                        "Ensure auto_trader_exposure_expansion_org.py defines class AutoTrader."
                    ),
                })
                return
        TraderClass = TRADER_MAP.get(strategy)
        
        if not TraderClass:
            health_queue.put({
                "session_id": session_id,
                "status": "error",
                "error": f"Invalid strategy: {strategy}"
            })
            return
        
        # Create trader instance
        trader = TraderClass(
            prediction_client=prediction_client,
            market_client=market_client,
            broker=broker,
            alerts=AlertManager(),
            trading_logs_collection=trading_logs_collection
        )
        trader.config_db_name = config_db_name
        
        # Configure trader
        trader.session_id = session_id
        trader.session = restored
        trader.ui_session_id = session_id
        trader.symbol_allocations = {k: v["capital"] for k, v in allocations.items()}
        trader.initial_allocations = allocations
        trader.configuration_id = configuration_id
        
        # Signal that we're healthy and starting
        health_queue.put({
            "session_id": session_id,
            "status": "starting",
            "timestamp": time.time()
        })
        
        # Start heartbeat thread
        def heartbeat_loop():
            while not stop_event.is_set():
                try:
                    health_queue.put({
                        "session_id": session_id,
                        "status": "running",
                        "timestamp": time.time()
                    }, timeout=1)
                except:
                    pass
                time.sleep(10)  # Heartbeat every 10 seconds
        
        heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        
        # Run the trader with monitoring
        print(f"[Worker-{session_id}] Starting trader with {len(symbols)} symbols")
        
        # Wrap trader.start in a monitoring loop
        trader_thread = threading.Thread(
            target=trader.start,
            args=(symbols, time_frame, candle),
            kwargs={"initial_allocations": allocations},
            daemon=False  # Don't make daemon - we want proper cleanup
        )
        trader_thread.start()

        # ---------- Background LTP stream (independent of PnL flow) ----------
        ltp_stream = None
        try:
            print(
                f"[Worker-{session_id}] Wiring LiveLTPStream -> trader.on_ltp_tick "
                f"(callable={callable(getattr(trader, 'on_ltp_tick', None))}) "
                f"symbols={list(allocations.keys())}"
            )
            ltp_stream = LiveLTPStream(broker, trader.on_ltp_tick)
            ltp_stream.start(list(allocations.keys()))
            print(f"[Worker-{session_id}] LiveLTPStream started for {len(allocations)} symbols")
        except Exception as e:
            print(f"[Worker-{session_id}] LiveLTPStream failed to start: {e}")

        # Monitor for stop signal AND single-symbol exit requests
        exit_queue_key = f"autotrader:exit_request:{session_id}"
        while trader_thread.is_alive() and not stop_event.is_set():
            # Check for single-symbol exit requests from Redis
            if exit_redis_client:
                try:
                    exit_req_raw = exit_redis_client.lpop(exit_queue_key)
                    if exit_req_raw:
                        data = json.loads(exit_req_raw)
                        exit_symbol = data.get("symbol", "")
                        print(f"[Worker-{session_id}] Exit request received for symbol: {exit_symbol}")
                        result = trader.exit_single_position(exit_symbol)
                        # Push result back to Redis for the API to read
                        result_key = f"autotrader:exit_result:{session_id}:{exit_symbol}"
                        exit_redis_client.setex(result_key, 60, json.dumps(result))
                        print(f"[Worker-{session_id}] Exit result for {exit_symbol}: {result}")
                except Exception as e:
                    print(f"[Worker-{session_id}] Exit queue check error: {e}")
            
            trader_thread.join(timeout=1)
        
        # If stop was requested, shutdown trader
        if stop_event.is_set():
            print(f"[Worker-{session_id}] Stop requested, shutting down...")
            trader.shutdown() # but inside the shutdown we have not write the logic of exiting the orders and all and in the while loop we have not checked the stop_event flag
            print("shutdown called successfully")
            trader_thread.join(timeout=60)

        if ltp_stream is not None:
            try:
                ltp_stream.stop()
            except Exception as e:
                print(f"[Worker-{session_id}] LiveLTPStream stop error: {e}")


        simulation_stop = False
        try:
            if exit_redis_client:
                sim_key = f"{SIMULATION_STOP_PREFIX}{session_id}"
                simulation_stop = bool(exit_redis_client.get(sim_key))
                if simulation_stop:
                    exit_redis_client.delete(sim_key)
        except Exception as e:
            print(f"[Worker-{session_id}] Simulation stop flag check error: {e}")

        if simulation_stop:
            health_queue.put({
                "session_id": session_id,
                "status": "simulation_stopped",
                "timestamp": time.time()
            })
            try:
                sessions_collection = mongo_db["plugin_sessions"]
                sessions_collection.update_one(
                    {"session_id": session_id},
                    {
                        "$set": {
                            "trading_status": "simulation_stopped",
                            "simulation_stopped_at": datetime.utcnow(),
                            "last_updated": datetime.utcnow()
                        }
                    }
                )
                print(f"[Worker-{session_id}] Simulation stopped in Mongo (session auth unchanged)")
            except Exception as e:
                print(f"[Worker-{session_id}] Failed to mark simulation stopped in Mongo: {e}")
        else:
            health_queue.put({
                "session_id": session_id,
                "status": "stopped",
                "timestamp": time.time()
            })

            try:
                sessions_collection = mongo_db["plugin_sessions"]
                sessions_collection.update_one(
                    {"session_id": session_id},
                    {
                        "$set": {
                            "status": "stopped",
                            "trading_status": "stopped",
                            "stopped_at": datetime.utcnow(),
                            "last_updated": datetime.utcnow()
                        }
                    }
                )
                print(f"[Worker-{session_id}] Session status marked stopped in Mongo")
            except Exception as e:
                print(f"[Worker-{session_id}] Failed to mark session stopped in Mongo: {e}")
        
        # Cleanup
        mongo_client.close()
        
    except Exception as e:
        error_msg = f"Worker error: {str(e)}\n{traceback.format_exc()}"
        print(f"[Worker-{session_id}] {error_msg}")
        health_queue.put({
            "session_id": session_id,
            "status": "error",
            "error": error_msg,
            "timestamp": time.time()
        })


class SessionManager:
    """
    Optimized session manager using multiprocessing for true parallelism.
    Each trader runs in its own process with isolated resources.
    """
    
    _workers: Dict[str, WorkerProcess] = {}
    _manager = Manager()
    _health_queue = _manager.Queue()
    _monitor_thread = None
    _monitor_stop = threading.Event()
   

    _market_client: MarketClient = MarketClient()
    
    @classmethod
    def _redis(cls):
        """Convenience accessor for MarketClient's Redis connection."""
        return cls._market_client.redis_client

    REDIS_KEY_PREFIX = "autotrader:session:"
    SIMULATION_STOP_PREFIX = SIMULATION_STOP_PREFIX
    SIMULATION_STOP_TTL = SIMULATION_STOP_TTL

    @classmethod
    def _simulation_stop_key(cls, session_id: str) -> str:
        return f"{cls.SIMULATION_STOP_PREFIX}{session_id}"

    @classmethod
    def _mark_simulation_stop(cls, session_id: str) -> None:
        try:
            cls._redis().setex(
                cls._simulation_stop_key(session_id),
                cls.SIMULATION_STOP_TTL,
                "1",
            )
            print(f"[SessionManager] Simulation stop flag set for {session_id}")
        except Exception as e:
            print(f"[SessionManager] Failed to set simulation stop flag: {e}")

    # Configuration
    MAX_WORKERS = int(os.environ.get("MAX_TRADER_WORKERS", "6"))  # Limit concurrent processes
    HEALTH_CHECK_INTERVAL = 30  # seconds
    WORKER_TIMEOUT = 120  # seconds without heartbeat = dead
    
    @classmethod
    def _start_monitor(cls):
        """Start background thread to monitor worker health"""
        if cls._monitor_thread and cls._monitor_thread.is_alive():
            return
        
        def monitor_loop():
            print("[SessionManager] Health monitor started")
            while not cls._monitor_stop.is_set():
                try:
                    # Process health updates
                    while True:
                        try:
                            msg = cls._health_queue.get(timeout=0.1)
                            session_id = msg.get("session_id")
                            status = msg.get("status")
                            
                            if session_id in cls._workers:
                                worker = cls._workers[session_id]
                                worker.update_heartbeat()
                                
                                if status == "error":
                                    print(f"[SessionManager] Worker {session_id} reported error: {msg.get('error')}")
                                elif status == "stopped":
                                    print(f"[SessionManager] Worker {session_id} stopped gracefully")
                                elif status == "simulation_stopped":
                                    print(f"[SessionManager] Worker {session_id} simulation stopped (session auth kept)")
                        
                        except Empty:
                            break
                    
                    # Check for unhealthy workers
                    dead_workers = []
                    for session_id, worker in cls._workers.items():
                        if not worker.is_alive():
                            print(f"[SessionManager] Worker {session_id} process died")
                            dead_workers.append(session_id)
                        elif not worker.is_healthy(cls.WORKER_TIMEOUT):
                            print(f"[SessionManager] Worker {session_id} stopped responding (timeout)")
                            dead_workers.append(session_id)
                    
                    # Clean up dead workers
                    for session_id in dead_workers:
                        cls._cleanup_worker(session_id)
                    
                except Exception as e:
                    print(f"[SessionManager] Monitor error: {e}")
                
                time.sleep(cls.HEALTH_CHECK_INTERVAL)
        
        cls._monitor_stop.clear()
        cls._monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        cls._monitor_thread.start()
    
    @classmethod
    def _cleanup_worker(cls, session_id: str):
        """Clean up a dead or stopped worker"""
        worker = cls._workers.get(session_id)
        if not worker:
            return
        
        try:
            if worker.is_alive():
                worker.stop_event.set()
                worker.process.join(timeout=5)
                
                if worker.process.is_alive():
                    print(f"[SessionManager] Force terminating worker {session_id}")
                    worker.process.terminate()
                    worker.process.join(timeout=2)
                    
                    if worker.process.is_alive():
                        worker.process.kill()
        except Exception as e:
            print(f"[SessionManager] Error cleaning up worker {session_id}: {e}")
        finally:
            cls._workers.pop(session_id, None)
    
    @classmethod
    def start_session(cls, session_id: str, session_doc: dict, trading_logs_collection):
        """Start a new trading session in an isolated process"""
        
        # Start monitor if not running
        cls._start_monitor()
        
        # Check if session already exists
        if session_id in cls._workers:
            print(f"[SessionManager] Session already running: {session_id}")
            raise RuntimeError(f"Session {session_id} is already running")
        
        # Check worker limit
        active_workers = sum(1 for w in cls._workers.values() if w.is_alive())
        if active_workers >= cls.MAX_WORKERS:
            raise RuntimeError(
                f"Maximum concurrent sessions ({cls.MAX_WORKERS}) reached. "
                f"Stop a session or increase MAX_TRADER_WORKERS environment variable."
            )
        
        # Parse session configuration
        strategy = session_doc.get("strategy", "A")
        raw_symbols = session_doc.get("symbols", [])
        
        symbols = []
        allocations = {}
        
        for s in raw_symbols:
            if not isinstance(s, dict):
                continue
            
            sym = s.get("symbol")
            cap = float(s.get("capital", 0))
            sl = float(s.get("stop_loss", 0.02))
            
            if sym:
                symbols.append(sym)
                allocations[sym] = {
                    "capital": cap,
                    "stop_loss": sl
                }
        
        if not symbols:
            raise RuntimeError("CRITICAL: Empty symbols list")
        
        print(f"[SessionManager] Starting session {session_id}")
        print(f"  Strategy: {strategy}")
        print(f"  Symbols: {symbols}")
        print(f"  Allocations: {allocations}")
        
        time_frame = session_doc.get("time_frame", "5 minutes")
        candle = session_doc.get("candle", "5m")
        configuration_id = session_doc.get("configuration_id")
        
        # Prepare broker config
        broker_config = {
            "api_key": session_doc["api_key"],
            "client_code": session_doc["client_code"],
            "password": session_doc["password"],
            "broker_session": session_doc["broker_session"]
        }
        
        # Get MongoDB connection details
        mongo_uri = os.environ.get(
            "MONGO_URI",
            "mongodb+srv://mintzy01ai_db_user:zTqQRkovgKbLXQdp@cluster0.cztcxpr.mongodb.net/?appName=Cluster0"
        )
        mongo_db_name = os.environ.get("MONGO_DB_NAME", "mintzy_plugin")
        
        # Create stop event and health queue for this worker
        stop_event = mp.Event()
        
        # Create worker process
        process = Process(
            target=_trader_worker,
            args=(
                session_id,
                strategy,
                symbols,
                allocations,
                time_frame,
                candle,
                broker_config,
                trading_logs_collection.name,
                stop_event,
                cls._health_queue,
                mongo_uri,
                mongo_db_name,
                configuration_id,
            ),
            daemon=False  # Not daemon - we want proper cleanup
        )
        
        # Start process
        process.start()

        try:
            cls._redis().setex(
                f"{cls.REDIS_KEY_PREFIX}{session_id}",
                86400,   # 24 hour TTL
                str(process.pid)
            )
            print(f"[SessionManager] Redis mein save kiya Ã¢â‚¬â€ session={session_id} pid={process.pid}")
        except Exception as e:
            print(f"[SessionManager] Redis save failed: {e}")
            
        # Register worker
        worker = WorkerProcess(session_id, process, stop_event, cls._health_queue)
        cls._workers[session_id] = worker
        
        print(f"[SessionManager] Session {session_id} started in process {process.pid}")
        
        return {
            "session_id": session_id,
            "pid": process.pid,
            "started_at": worker.started_at
        }
    
    # @classmethod
    # def stop_session(cls, session_id: str):
    #     """Stop a trading session gracefully"""
        
    #     worker = cls._workers.get(session_id)
        
    #     if not worker:
    #         print(f"[SessionManager] No active session {session_id}")
    #         return False
        
    #     print(f"[SessionManager] Stopping session {session_id} (PID: {worker.process.pid})")
        
    #     # Signal worker to stop
    #     worker.stop_event.set()
        
    #     # Wait for graceful shutdown
    #     worker.process.join(timeout=15)
        
    #     # Force cleanup if still alive
    #     if worker.is_alive():
    #         print(f"[SessionManager] Force terminating session {session_id}")
    #         worker.process.terminate()
    #         worker.process.join(timeout=3)
            
    #         if worker.is_alive():
    #             worker.process.kill()
        
    #     # Remove from registry
    #     cls._workers.pop(session_id, None)
        
    #     print(f"[SessionManager] Session {session_id} stopped")
    #     return True


    @classmethod
    def stop_session(cls, session_id: str):
        print(f"[SessionManager] Stop request aaya: '{session_id}'")
        print(f"[SessionManager] Current workers: {list(cls._workers.keys())}")

        worker = cls._workers.get(session_id)

        # Ã¢â€ Â AGAR LOCAL MEMORY MEIN NAHI MILA Ã¢â‚¬â€ REDIS CHECK KARO
        if not worker:
            print(f"[SessionManager] Local memory mein nahi mila Ã¢â‚¬â€ Redis check kar raha hoon...")
            
            try:
                pid_str = cls._redis().get(f"{cls.REDIS_KEY_PREFIX}{session_id}")
                
                if not pid_str:
                    print(f"[SessionManager] Ã¢ÂÅ’ Redis mein bhi nahi mila Ã¢â‚¬â€ session already stopped hoga")
                    return False
                
                pid = int(pid_str)
                print(f"[SessionManager] Ã¢Å“â€¦ Redis mein mila Ã¢â‚¬â€ PID={pid} Ã¢â‚¬â€ kill kar raha hoon...")
                
                # Process ko SIGTERM bhejo Ã¢â‚¬â€ graceful shutdown
                try:
                    os.kill(pid, signal.SIGTERM)
                    print(f"[SessionManager] Ã¢Å“â€¦ SIGTERM bheja PID={pid} ko")
                    
                    # 30 sec wait karo graceful shutdown ke liye
                    import psutil
                    try:
                        proc = psutil.Process(pid)
                        proc.wait(timeout=60)
                        print(f"[SessionManager] Ã¢Å“â€¦ Process {pid} gracefully band ho gaya")
                    except psutil.TimeoutExpired:
                        print(f"[SessionManager] Ã¢Å¡ Ã¯Â¸Â 30 sec baad bhi alive Ã¢â‚¬â€ SIGKILL bhej raha hoon...")
                        os.kill(pid, signal.SIGKILL)
                    except psutil.NoSuchProcess:
                        print(f"[SessionManager] Ã¢Å“â€¦ Process {pid} already band ho gaya")
                        
                except ProcessLookupError:
                    print(f"[SessionManager] Ã¢Å¡ Ã¯Â¸Â PID={pid} already exist nahi karta Ã¢â‚¬â€ already band tha")
                
                # Redis se hata do
                cls._redis().delete(f"{cls.REDIS_KEY_PREFIX}{session_id}")
                print(f"[SessionManager] Ã¢Å“â€¦ Session {session_id} stopped via Redis")
                return True
                
            except Exception as e:
                print(f"[SessionManager] Ã¢ÂÅ’ Redis stop failed: {e}")
                return False

        # Ã¢â€ Â NORMAL FLOW Ã¢â‚¬â€ local memory mein mila
        print(f"[SessionManager] Local memory mein mila Ã¢â‚¬â€ normal shutdown...")
        
        worker.stop_event.set()
        worker.process.join(timeout=60)
        
        if worker.is_alive():
            print(f"[SessionManager] Ã¢Å¡ Ã¯Â¸Â Force terminating {session_id}")
            worker.process.terminate()
            worker.process.join(timeout=5)
            if worker.is_alive():
                worker.process.kill()
        
        # Redis se bhi hata do
        try:
            cls._redis().delete(f"{cls.REDIS_KEY_PREFIX}{session_id}")
        except Exception as e:
            print(f"[SessionManager] Redis delete failed: {e}")
        
        cls._workers.pop(session_id, None)
        print(f"[SessionManager] Ã¢Å“â€¦ Session {session_id} stopped")
        return True

    @classmethod
    def stop_simulation_session(cls, session_id: str):
        """
        Stop the paper/simulation worker without marking the session as stopped.
        Sets a Redis flag so the worker exit handler keeps status=authenticated in Mongo.
        """
        print(f"[SessionManager] Simulation stop request: '{session_id}'")
        cls._mark_simulation_stop(session_id)
        return cls.stop_session(session_id)
    
    @classmethod
    def exit_symbol_for_session(cls, session_id: str, symbol: str) -> dict:
        """
        Send a single-symbol exit request to the trader worker process via Redis queue.
        Works across gunicorn workers because Redis is shared.
        """
        print(f"[SessionManager] Exit request: session={session_id} symbol={symbol}")
        
        # Verify session exists (local or Redis)
        worker = cls._workers.get(session_id)
        if not worker:
            # Check Redis for PID (cross-worker case)
            try:
                pid_str = cls._redis().get(f"{cls.REDIS_KEY_PREFIX}{session_id}")
                if not pid_str:
                    return {
                        "success": False,
                        "symbol": symbol,
                        "message": f"Session {session_id} not found (not running)"
                    }
            except Exception as e:
                return {
                    "success": False,
                    "symbol": symbol,
                    "message": f"Redis error checking session: {e}"
                }
        
        # Push exit request to Redis queue
        try:
            exit_queue_key = f"autotrader:exit_request:{session_id}"
            payload = json.dumps({"symbol": symbol.upper()})
            cls._redis().rpush(exit_queue_key, payload)
            # Set TTL on the queue key so it auto-cleans (5 minutes)
            cls._redis().expire(exit_queue_key, 300)
            
            print(f"[SessionManager] Ã¢Å“â€¦ Exit request pushed to Redis for {symbol}")
            
            # Wait briefly for the result (max 10 seconds)
            result_key = f"autotrader:exit_result:{session_id}:{symbol.upper()}"
            for _ in range(20):  # 20 Ãƒâ€” 0.5s = 10s
                time.sleep(0.5)
                result_raw = cls._redis().get(result_key)
                if result_raw:
                    cls._redis().delete(result_key)  # cleanup
                    return json.loads(result_raw)
            
            # Timeout Ã¢â‚¬â€ request was sent but no result yet
            return {
                "success": True,
                "symbol": symbol,
                "message": f"Exit request sent for {symbol}. Order is being processed (reconciliation will handle it)."
            }
            
        except Exception as e:
            return {
                "success": False,
                "symbol": symbol,
                "message": f"Failed to send exit request: {e}"
            }
    

  
    @classmethod
    def get_session_status(cls, session_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a trading session"""
        
        worker = cls._workers.get(session_id)
        
        if not worker:
            return None
        
        return {
            "session_id": session_id,
            "pid": worker.process.pid,
            "is_alive": worker.is_alive(),
            "is_healthy": worker.is_healthy(),
            "started_at": worker.started_at,
            "uptime": time.time() - worker.started_at,
            "last_heartbeat": worker.last_heartbeat
        }
    
    @classmethod
    def list_sessions(cls) -> Dict[str, Dict[str, Any]]:
        """List all active sessions"""
        return {
            session_id: cls.get_session_status(session_id)
            for session_id in cls._workers.keys()
        }
    
    @classmethod
    def stop_all_sessions(cls):
        """Stop all trading sessions"""
        
        print("[SessionManager] Stopping all sessions...")
        
        session_ids = list(cls._workers.keys())
        
        for session_id in session_ids:
            cls.stop_session(session_id)
        
        # Stop monitor
        cls._monitor_stop.set()
        if cls._monitor_thread:
            cls._monitor_thread.join(timeout=5)
        
        print("[SessionManager] All sessions stopped")
