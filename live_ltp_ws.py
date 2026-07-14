"""
Background Angel One LTP streamer.

Runs SmartWebSocketV2 on its own daemon thread and pushes each LTP tick to a
user-supplied callback. Completely independent of the REST flow in
broker_angle.py and the PnL math in auto_trader_exposure_expansion.py.
"""

import threading
import time
from typing import Callable, Dict, Iterable, Optional

from SmartApi.smartWebSocketV2 import SmartWebSocketV2


# Angel exchangeType codes
EXCHANGE_TYPE = {
    "NSE": 1,
    "NFO": 2,
    "BSE": 3,
    "BFO": 4,
    "MCX": 5,
    "CDS": 13,
}

MODE_LTP = 1
MODE_QUOTE = 2
MODE_SNAP_QUOTE = 3       # includes top-5 market depth â†’ fixes stale-symbol problem
CORRELATION_ID = "ltp_stream"

# Source labels for diagnostic logging â€” each tick reports which path produced
# the price so we can verify book-depth coverage in production.
SRC_MID = "mid"
SRC_BID = "bid"
SRC_ASK = "ask"
SRC_LTP = "ltp"


class LiveLTPStream:
    """
    on_tick signature: on_tick(symbol: str, ltp: float, ts_epoch: float) -> None
    The callback runs on the WS thread â€” keep it cheap and thread-safe.
    """

    def __init__(self, broker, on_tick: Callable[[str, float, float], None],
                 exchange: str = "NSE", mode: int = MODE_SNAP_QUOTE):
        self.broker = broker
        self.on_tick = on_tick
        self.exchange_type = EXCHANGE_TYPE.get(exchange.upper(), 1)
        self.mode = mode

        self._sws: Optional[SmartWebSocketV2] = None
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._token_to_symbol: Dict[str, str] = {}
        self._subscribed_tokens: set = set()
        self._connected = threading.Event()
        self._stop = threading.Event()
        self._first_tick_seen: set = set()       # symbols whose first tick we already logged
        self._tick_count_total = 0
        self._tick_count_last_print = 0
        self._tick_print_every = 200             # one summary line every N ticks
        self._last_summary_ts = time.time()
        self._price_source_counts: Dict[str, int] = {
            SRC_MID: 0, SRC_BID: 0, SRC_ASK: 0, SRC_LTP: 0
        }
        self._first_payload_logged = False       # dump first mode-3 payload for verification
        self._fallback_warned: set = set()       # symbols we've already warned about

    # ---------- public ----------

    def start(self, symbols: Iterable[str]) -> None:
        sym_list = list(symbols)
        print(f"[LTP-WS] start() requested for {len(sym_list)} symbols: {sym_list}")
        creds = self.broker.get_ws_credentials()
        self._sws = SmartWebSocketV2(
            auth_token=creds["auth_token"],
            api_key=creds["api_key"],
            client_code=creds["client_code"],
            feed_token=creds["feed_token"],
        )
        self._sws.on_open = self._on_open
        self._sws.on_data = self._on_data
        self._sws.on_error = self._on_error
        self._sws.on_close = self._on_close

        # Resolve symbols -> tokens BEFORE connect so on_open can subscribe.
        for sym in sym_list:
            self._register(sym)
        print(
            f"[LTP-WS] resolved {len(self._token_to_symbol)}/{len(sym_list)} tokens; "
            f"map={self._token_to_symbol}"
        )

        self._thread = threading.Thread(
            target=self._run, name="LiveLTPStream", daemon=True
        )
        self._thread.start()

    def subscribe(self, symbol: str) -> None:
        token = self._register(symbol)
        if token and self._connected.is_set():
            self._send_subscribe([token])

    def unsubscribe(self, symbol: str) -> None:
        sym_u = symbol.upper()
        token = next((t for t, s in self._token_to_symbol.items() if s == sym_u), None)
        if not token:
            return
        with self._lock:
            self._subscribed_tokens.discard(token)
            self._token_to_symbol.pop(token, None)
        if self._connected.is_set() and self._sws:
            try:
                self._sws.unsubscribe(CORRELATION_ID, MODE_LTP,
                                      [{"exchangeType": self.exchange_type,
                                        "tokens": [token]}])
            except Exception as e:
                print(f"[LTP-WS] unsubscribe error for {symbol}: {e}")

    def stop(self) -> None:
        self._stop.set()
        if self._sws:
            try:
                self._sws.close_connection()
            except Exception:
                pass

    # ---------- internals ----------

    def _register(self, symbol: str) -> Optional[str]:
        sym_u = symbol.upper()
        token = self.broker.get_symbol_token(sym_u)
        if not token:
            print(f"[LTP-WS] token missing for {sym_u}, skipping")
            return None
        with self._lock:
            self._token_to_symbol[str(token)] = sym_u
        return str(token)

    def _send_subscribe(self, tokens) -> None:
        try:
            tok_list = list(tokens)
            self._sws.subscribe(CORRELATION_ID, self.mode,
                                [{"exchangeType": self.exchange_type,
                                  "tokens": tok_list}])
            with self._lock:
                self._subscribed_tokens.update(tok_list)
            mode_name = {1: "LTP", 2: "QUOTE", 3: "SNAP_QUOTE"}.get(self.mode, str(self.mode))
            print(f"[LTP-WS] subscribe sent mode={self.mode}({mode_name}) "
                  f"tokens={len(tok_list)}: {tok_list}")
        except Exception as e:
            print(f"[LTP-WS] subscribe error: {e}")

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._sws.connect()  # blocking
            except Exception as e:
                print(f"[LTP-WS] connect crashed: {e}")
            if self._stop.is_set():
                break
            self._connected.clear()
            print("[LTP-WS] disconnected, reconnecting in 5s")
            time.sleep(5)

    def _on_open(self, wsapp):
        print("[LTP-WS] connected")
        self._connected.set()
        with self._lock:
            tokens = list(self._token_to_symbol.keys())
        if tokens:
            self._send_subscribe(tokens)

    def _on_data(self, wsapp, message):
        try:
            if not isinstance(message, dict):
                return
            token = str(message.get("token") or "")
            if not token:
                return
            symbol = self._token_to_symbol.get(token)
            if not symbol:
                return

            # One-shot dump of the first payload â€” confirms field names match
            # what we expect from the SDK before relying on book extraction.
            if not self._first_payload_logged:
                self._first_payload_logged = True
                print(f"[LTP-WS] FIRST PAYLOAD ({symbol}) keys={list(message.keys())}")
                print(f"[LTP-WS] FIRST PAYLOAD raw={message}")

            price, source = self._extract_price(message, symbol)
            if price is None:
                if symbol not in self._fallback_warned:
                    self._fallback_warned.add(symbol)
                    print(f"[LTP-WS] {symbol}: no usable price in payload, dropping tick")
                return

            ts = (message.get("exchange_timestamp") or
                  message.get("last_traded_timestamp") or
                  int(time.time() * 1000)) / 1000.0

            # ---- Diagnostics (throttled) ----
            self._tick_count_total += 1
            self._price_source_counts[source] = self._price_source_counts.get(source, 0) + 1
            if symbol not in self._first_tick_seen:
                self._first_tick_seen.add(symbol)
                print(f"[LTP-WS] first tick for {symbol} @ Rs{price:.2f} (source={source})")
            if (self._tick_count_total - self._tick_count_last_print) >= self._tick_print_every:
                now = time.time()
                rate = (self._tick_count_total - self._tick_count_last_print) / max(now - self._last_summary_ts, 0.001)
                print(
                    f"[LTP-WS] ticks={self._tick_count_total} "
                    f"symbols_seen={len(self._first_tick_seen)}/{len(self._token_to_symbol)} "
                    f"rate~{rate:.1f}/s sources={self._price_source_counts}"
                )
                self._tick_count_last_print = self._tick_count_total
                self._last_summary_ts = now

            self.on_tick(symbol, price, ts)
        except Exception as e:
            print(f"[LTP-WS] on_data error: {e}")

    def _extract_price(self, message: dict, symbol: str):
        """
        Mode-3 (Snap Quote) price selection with graceful fallbacks:
          1. mid = (best_bid + best_ask) / 2   â† freshest, fixes stale-symbol issue
          2. best_bid alone (if ask missing/zero)
          3. best_ask alone (if bid missing/zero)
          4. last_traded_price                 â† mode-1 equivalent fallback
        Returns (price_in_rupees, source_label) or (None, None) if nothing usable.
        """
        # Angel SmartWebSocketV2 mode-3 puts top-of-book in best_5_buy_data /
        # best_5_sell_data â€” each is a list of {price, quantity, no of orders}
        # in PAISE. SDK key names have varied across versions; check both.
        def _best_price(side_keys):
            for k in side_keys:
                arr = message.get(k)
                if isinstance(arr, list) and arr:
                    first = arr[0]
                    if isinstance(first, dict):
                        p = first.get("price") or first.get("Price")
                        if p:
                            return float(p)
            return None

        best_bid = _best_price(["best_5_buy_data", "best5BuyData", "best_5_buy"])
        best_ask = _best_price(["best_5_sell_data", "best5SellData", "best_5_sell"])

        if best_bid and best_ask and best_bid > 0 and best_ask > 0:
            return ((best_bid + best_ask) / 2.0) / 100.0, SRC_MID

        if best_bid and best_bid > 0:
            if symbol not in self._fallback_warned:
                self._fallback_warned.add(symbol)
                print(f"[LTP-WS] {symbol}: ask side empty, using bid only")
            return best_bid / 100.0, SRC_BID

        if best_ask and best_ask > 0:
            if symbol not in self._fallback_warned:
                self._fallback_warned.add(symbol)
                print(f"[LTP-WS] {symbol}: bid side empty, using ask only")
            return best_ask / 100.0, SRC_ASK

        ltp_paise = message.get("last_traded_price")
        if ltp_paise:
            if symbol not in self._fallback_warned:
                self._fallback_warned.add(symbol)
                print(
                    f"[LTP-WS] {symbol}: book empty, falling back to LTP "
                    f"(stale-symbol risk for this name)"
                )
            return float(ltp_paise) / 100.0, SRC_LTP

        return None, None

    def _on_error(self, wsapp, error):
        print(f"[LTP-WS] error: {error}")
        self._connected.clear()

    def _on_close(self, wsapp):
        print("[LTP-WS] socket closed")
        self._connected.clear()