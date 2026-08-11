import os
import getpass
import json
import time
from datetime import datetime
from SmartApi import SmartConnect
import requests
from tabulate import tabulate

class BrokerConnector:

    def __init__(self, instruments_path="NSE.json", require_totp=False):
        self.api_key = os.getenv("ANGEL_API_KEY") or input("Enter your Angel One API Key: ").strip()
        self.client_code = os.getenv("ANGEL_CLIENT_CODE") or input("Enter your Client Code: ").strip()
        self.password = os.getenv("ANGEL_PASSWORD") or getpass.getpass("Enter your Password: ").strip()
        # self.totp = os.getenv("ANGEL_TOTP") or input("Enter your TOTP (if enabled; press Enter to skip): ").strip()
        if require_totp:
            self.totp = os.getenv("ANGEL_TOTP") or input("Enter TOTP: ").strip()
        else:
            self.totp = None
        self.instruments_path = instruments_path

        self.obj = None
        self.access_token = None
        self.refresh_token = None
        self.feed_token = None
        self.user = None

        if not os.path.exists(self.instruments_path):
            self._instruments_sample = []
        else:
            try:
                with open(self.instruments_path, "r") as f:
                    self._instruments_sample = json.load(f)
            except Exception:
                self._instruments_sample = []
    
    def _get_bulk_broker_ltp(self, symbols):
        """
        Angel SmartAPI has NO bulk LTP.
        We loop symbol-by-symbol.
        """

        out = {}

        try:
            if not self.obj:
                print("[BROKER LTP] SmartConnect not ready")
                return {}

            for sym in symbols:
                try:
                    token = self.get_symbol_token(sym)
                    if not token:
                        print(f"[LTP] token missing for {sym}")
                        continue

                    resp = self.obj.ltpData("NSE", f"{sym}-EQ", token)

                    if (
                        isinstance(resp, dict)
                        and resp.get("status")
                        and resp.get("data")
                    ):
                        ltp = resp["data"].get("ltp")
                        if ltp:
                            out[sym.upper()] = float(ltp)

                except Exception as e:
                    print(f"[LTP FAIL] {sym}: {e}")

            return out

        except Exception as e:
            print("[BROKER BULK LOOP ERROR]", e)
            return {}


    def get_order_status(self, session, order_id):
        """
        Fetch order status from Angel One for a given order_id.
        """
        try:
            url = "https://apiconnect.angelone.in/rest/secure/angelbroking/order/v1/details"
            headers = {
                "Content-type": "application/json",
                "X-ClientLocalIP": "127.0.0.1",
                "X-ClientPublicIP": "34.56.133.232",
                "X-MACAddress": "42:01:0a:80:00:04",
                "Accept": "application/json",
                "X-PrivateKey": self.api_key,
                "X-UserType": "USER",
                "X-SourceID": "WEB",
                "Authorization": f"Bearer {session.get('jwt_token') if isinstance(session, dict) else session}"
            }
            payload = {"orderid": str(order_id)}
            r = requests.post(url, headers=headers, json=payload, timeout=10)
            data = r.json()
            if not data.get("status"):
                return {"status": "error", "error": data.get("message", "unknown error")}
            return data.get("data", {})
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def _create_session(self):
        """Create SmartAPI session and store SmartConnect instance and token."""
        try:
            obj = SmartConnect(api_key=self.api_key)
            resp = obj.generateSession(self.client_code, self.password, self.totp)
            
            if not resp or not isinstance(resp, dict):
                raise RuntimeError(f"Invalid response from generateSession: {resp}")
            
            if resp.get('status') == False:
                raise RuntimeError(f"Login failed: {resp.get('message', resp)}")
            
            if "data" in resp:
                data = resp["data"]
                self.access_token = data.get("jwtToken")
                self.refresh_token = data.get("refreshToken")
                self.feed_token = data.get("feedToken")
                
                if not self.access_token:
                    raise RuntimeError(f"Failed to retrieve jwtToken from login response: {resp}")
                
                if self.refresh_token:
                    try:
                        obj.generateToken(self.refresh_token)
                    except Exception:
                        pass
                
                self.obj = obj
                self.user = self.client_code
                
                print("ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“ Angel One account linked successfully.")
                
                return {
                    "user": self.user,
                    "token": self.access_token,
                    "refresh_token": self.refresh_token,
                    "feed_token": self.feed_token,
                    "obj": self.obj
                }
            else:
                raise RuntimeError(f"No 'data' field in response: {resp}")
            
        except Exception as ex:
            raise RuntimeError(f"Login failed: {ex}")


    # def restore_session(self, broker_session: dict):
    #     print("Restoring Angel One broker session from stored data...")
    #     print("DEBUG restore_session input:", broker_session)

    #     token = broker_session.get("token")

    #     if not token or not isinstance(token, str):
    #         raise RuntimeError(
    #             f"Invalid broker session data: token missing. "
    #             f"Keys present: {list(broker_session.keys())}"
    #         )

    #     self.access_token = token
    #     self.refresh_token = broker_session.get("refresh_token")
    #     self.feed_token = broker_session.get("feed_token")

    #     self.obj = SmartConnect(api_key=self.api_key)
    #     self.obj.setAccessToken(self.access_token)

    #     self.user = self.client_code

    #     return {
    #         "user": self.user,
    #         "token": self.access_token,
    #         "refresh_token": self.refresh_token,
    #         "feed_token": self.feed_token,
    #         "obj": self.obj
    #     }


    def restore_session(self, broker_session):
        # self.access_token = broker_session.get("token")

        token = broker_session.get("token")
        if token.startswith("Bearer "):
            token = token.replace("Bearer ", "", 1)
        self.access_token = token        
        self.refresh_token = broker_session.get("refresh_token")
        self.feed_token = broker_session.get("feed_token")

        # 1) recreate object
        self.obj = SmartConnect(api_key=self.api_key)

        # 2) set the old access token
        self.obj.setAccessToken(self.access_token)

        # 3) refresh token to revive backend session
        try:
            new_tokens = self.obj.generateToken(self.refresh_token)
            # self.access_token = new_tokens["data"]["jwtToken"]
            # self.obj.setAccessToken(self.access_token)  # apply new token

            if isinstance(new_tokens, dict) and new_tokens.get("data"):
                new_jwt = new_tokens["data"].get("jwtToken")
                if new_jwt:
                    self.access_token = new_jwt
                    self.obj.setAccessToken(self.access_token)
            else:
                print("Refresh failed:", new_tokens)    
        except Exception as e:
            print("Refresh token failed:", e)

        # print("broker is restored succcessfully", broker)
        print("self.obj in restore session", self.obj)
        return {
            "user": self.client_code,
            "token": self.access_token,
            "refresh_token": self.refresh_token,
            "feed_token": self.feed_token,
            "obj": self.obj
        }


        
    def get_ws_credentials(self):
        token = self.access_token or ""
        if token.startswith("Bearer "):
            token = token[len("Bearer "):]
        creds = {
            "auth_token": token,
            "feed_token": self.feed_token,
            "api_key": self.api_key,
            "client_code": self.client_code,
        }
        print(
            f"[BROKER WS-CREDS] client={self.client_code} "
            f"auth_token={'OK' if token else 'MISSING'} "
            f"feed_token={'OK' if self.feed_token else 'MISSING'} "
            f"api_key={'OK' if self.api_key else 'MISSING'}"
        )
        return creds

    def get_session(self):
        """Return active session dict. Will create session if not already active."""
        if self.obj and self.access_token:
            return {
                "user": self.user,
                "token": self.access_token,
                "refresh_token": self.refresh_token,
                "feed_token": self.feed_token,
                "obj": self.obj
            }
        return self._create_session()

    def refresh_session(self):
        """Refresh the session by generating a new token."""
        return self._create_session()

    # def _is_auth_error(self, err):
    #     """Check if error is authentication related."""
    #     try:
    #         if isinstance(err, Exception):
    #             text = str(err).lower()
    #         elif isinstance(err, dict):
    #             text = json.dumps(err).lower()
    #         else:
    #             text = str(err or "").lower()

    #         probes = [
    #             "401", "unauthorized", "invalid token", "token expired",
    #             "jwt", "authentication failed", "session expired", "access denied",
    #             "invalid session", "invalid credentials", "ag8001"
    #         ]
    #         return any(p in text for p in probes)
    #     except Exception:
    #         return False


    def _is_auth_error(self, err):
        """
        STRICT auth failure detection with DEBUG logging.
        """
        try:
            if isinstance(err, dict):
                msg = (err.get("message") or "").lower()
                code = (err.get("errorcode") or "").lower()
                status = err.get("status")
            else:
                msg = str(err).lower()
                code = ""
                status = None

            # Real auth expiry indicators
            real_auth_markers = [
                "invalid token",
                "token expired",
                "jwt expired",
                "authentication failed",
                "invalid session",
            ]

        # Angel One hard auth error codes
            hard_auth_codes = {"ab8050", "ag8001"}

        # ---- DEBUG PRINT ----
            if msg or code:
             print("\n[AUTH DEBUG] Evaluating auth error")
             print(f"[AUTH DEBUG] status={status}")
             print(f"[AUTH DEBUG] message='{msg}'")
             print(f"[AUTH DEBUG] errorcode='{code}'")

        # Check hard error codes first
            if code in hard_auth_codes:
             print(f"[AUTH DEBUG] ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ MATCH: hard auth error code '{code}'")
             return True

        # Check message markers
            for marker in real_auth_markers:
                if marker in msg:
                    print(f"[AUTH DEBUG] ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ MATCH: message contains '{marker}'")
                    return True

            print("[AUTH DEBUG] ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬ ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ NOT an auth error")
            return False

        except Exception as e:
            print(f"[AUTH DEBUG] Exception while detecting auth error: {e}")
            return False


    # def _call_api(self, fn, *args, **kwargs):
    #     """Call API with automatic retry on auth errors."""
    #     try:
    #         resp = fn(*args, **kwargs)
    #         if isinstance(resp, dict) and self._is_auth_error(resp):
    #             try:
    #                 self.refresh_session()
    #             except Exception as re:
    #                 raise RuntimeError(f"Session refresh failed: {re}")
    #             return fn(*args, **kwargs)
    #         return resp
    #     except Exception as e:
    #         if self._is_auth_error(e):
    #             try:
    #                 self.refresh_session()
    #                 return fn(*args, **kwargs)
    #             except Exception:
    #                 raise
    #         else:
    #             raise

    def _call_api(self, fn, *args, **kwargs):
        """
        Call Angel One API ONCE.
        Retry is handled by caller.
        NEVER auto re-login.
        """
        resp = fn(*args, **kwargs)

        if isinstance(resp, dict) and self._is_auth_error(resp):
            raise RuntimeError("SESSION_EXPIRED_RELOGIN_REQUIRED")

        return resp     

    def _load_instruments(self):
        """Load and return instruments JSON."""
        try:
            with open(self.instruments_path, "r") as f:
                return json.load(f)
        except Exception:
            return []

    def get_symbol_token(self, tradingsymbol, prefer_field="exchange_token"):
        """Get token for a trading symbol."""
        # Normalize symbol (append '-EQ' if missing)
        t = tradingsymbol.upper()
        if not t.endswith("-EQ"):
            t = f"{t}-EQ"

        instruments = self._load_instruments()
        if not instruments:
            return None

        for item in instruments:
            if not isinstance(item, dict):
                continue  # skip malformed entries

            asset = (
            item.get("asset_symbol") or
            item.get("underlying_symbol") or
            item.get("tradingsymbol") or
            item.get("trading_symbol") or
            item.get("symbol") or ""
            ).upper()

            if asset == t:
                token = (
                item.get("exchange_token") or
                item.get("exchangeToken") or
                item.get("symboltoken")
                )
                if token:
                    return str(token)
                ik = item.get("instrument_key") or item.get("instrumentKey") or ""
                if isinstance(ik, str) and "|" in ik:
                    parts = ik.split("|")
                    if parts[-1].isdigit():
                        return parts[-1]
                token2 = item.get("token") or item.get("instrument_token")
                if token2:
                    return str(token2)
        return None


    def _extract_best_cash_value(self, resp):
        """Extract cash value from response."""
        nums = []

        def walk(o):
            if o is None:
                return
            if isinstance(o, (int, float)):
                nums.append(float(o))
            elif isinstance(o, str):
                s = o.replace(",", "").replace("ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¹", "").strip()
                try:
                    if s.replace(".", "", 1).lstrip("+-").isdigit():
                        nums.append(float(s))
                except Exception:
                    pass
            elif isinstance(o, dict):
                for v in o.values():
                    walk(v)
            elif isinstance(o, (list, tuple)):
                for v in o:
                    walk(v)

        walk(resp)
        candidates = [n for n in nums if n is not None and n > 0 and n < 1e10]
        if not candidates:
            return None
        large = [n for n in sorted(candidates) if n >= 100]
        if large:
            return float(large[-1])
        return float(sorted(candidates)[-1]) if candidates else None

    def get_account_balance(self, session):
        """Get account balance using SmartAPI SDK methods."""
        if not session or "obj" not in session:
            return {"status": "error", "error": "No active session object provided."}

        try:
            rms_resp = self._call_api(session["obj"].rmsLimit)
            
            if isinstance(rms_resp, dict) and rms_resp.get("status") and "data" in rms_resp:
                data = rms_resp["data"]
                
                available_cash = float(data.get("availablecash") or data.get("net") or 0)
                collateral = float(data.get("collateral") or 0)
                m2m_realized = float(data.get("m2mrealized") or 0)
                m2m_unrealized = float(data.get("m2munrealized") or 0)
                
                print("\n========== ACCOUNT SUMMARY ==========")
                print(f"Available Cash: ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¹{available_cash:,.2f}")
                print(f"Collateral: ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¹{collateral:,.2f}")
                print(f"M2M Realized: ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¹{m2m_realized:,.2f}")
                print(f"M2M Unrealized: ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¹{m2m_unrealized:,.2f}")
                print("=====================================\n")
                
                try:
                    holdings_resp = self._call_api(session["obj"].holding)
                    positions_resp = self._call_api(session["obj"].position)
                    
                    holdings = []
                    if holdings_resp.get("status") and holdings_resp.get("data"):
                        holdings = holdings_resp["data"]
                        if holdings:
                            try:
                                import pandas as pd
                                df = pd.DataFrame(holdings)
                                if not df.empty:
                                    cols = ["tradingsymbol", "quantity", "averageprice", "ltp", "pnl"]
                                    available_cols = [c for c in cols if c in df.columns]
                                    if available_cols:
                                        print("ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“ÃƒÆ’Ã¢â‚¬Â¦  Holdings:")
                                        print(tabulate(df[available_cols], headers="keys", tablefmt="psql", showindex=False))
                                        print()
                            except ImportError:
                                pass
                    
                    positions = []
                    if positions_resp.get("status") and positions_resp.get("data"):
                        positions = positions_resp["data"]
                        if positions:
                            try:
                                import pandas as pd
                                df = pd.DataFrame(positions)
                                if not df.empty:
                                    cols = ["tradingsymbol", "netqty", "avgprice", "ltp", "pnl"]
                                    available_cols = [c for c in cols if c in df.columns]
                                    if available_cols:
                                        print("ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“ÃƒÆ’Ã¢â‚¬Â¹ÃƒÂ¢Ã¢â€šÂ¬  Positions:")
                                        print(tabulate(df[available_cols], headers="keys", tablefmt="psql", showindex=False))
                                        print()
                            except ImportError:
                                pass
                except Exception:
                    pass
                
                return {
                    "status": "success",
                    "free_cash": available_cash,
                    "collateral": collateral,
                    "m2m_realized": m2m_realized,
                    "m2m_unrealized": m2m_unrealized,
                    "holdings": holdings,
                    "positions": positions,
                    "raw": rms_resp,
                    "data": data,
                    "source": "SMARTAPI_SDK"
                }
            else:
                extracted = self._extract_best_cash_value(rms_resp)
                if extracted:
                    return {
                        "status": "success",
                        "free_cash": extracted,
                        "raw": rms_resp,
                        "source": "SMARTAPI_SDK_EXTRACTED"
                    }
                return {
                    "status": "error",
                    "error": f"RMS API call failed: {rms_resp}",
                    "raw": rms_resp,
                    "source": "SMARTAPI_SDK"
                }
                
        except Exception as ex:
            return {
                "status": "error",
                "error": str(ex),
                "source": "SMARTAPI_SDK"
            }

    def _wait_for_order_confirmation(self, session, order_id, max_wait_seconds=10, check_interval=1):
        """
        Wait for order to be filled/rejected and return final status.
        Returns: dict with 'filled' (bool), 'status', 'avg_price', 'message'
        """
        if not order_id:
            return {"filled": False, "status": "NO_ORDER_ID", "message": "No order ID provided"}
        
        elapsed = 0
        while elapsed < max_wait_seconds:
            try:
                 order_book_resp = None

                # order_book_resp = self._call_api(session["obj"].orderBook)
                 for attempt in range(3):
                    try:
                        order_book_resp = self._call_api(session["obj"].orderBook)
                        break
                    except RuntimeError as e:
                        if "SESSION_EXPIRED_RELOGIN_REQUIRED" in str(e):
                            raise
                        print(f"[WARN] orderBook failed (attempt {attempt + 1}/3): {e}")
                        time.sleep(check_interval)

                 if order_book_resp is None:
                     return {
                         "filled": False,
                         "status": "API_ERROR",
                         "avg_price": 0,
                         "message": "orderBook failed after retries"
                     }
                
                 if order_book_resp.get("status") and order_book_resp.get("data"):
                    orders = order_book_resp["data"]
                    
                    for order in orders:
                        if str(order.get("orderid")) == str(order_id):
                            order_status = order.get("orderstatus", "").upper()
                            order_type = order.get("ordertype", "")
                            avg_price = float(order.get("averageprice") or order.get("price") or 0)
                            
                            # Check if order is completed
                            if order_status in ["COMPLETE", "EXECUTED"]:
                                return {
                                    "filled": True,
                                    "status": order_status,
                                    "avg_price": avg_price,
                                    "message": f"Order {order_id} filled at ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¹{avg_price:.2f}"
                                }
                            
                            # Check if order failed
                            if order_status in ["REJECTED", "CANCELLED"]:
                                return {
                                    "filled": False,
                                    "status": order_status,
                                    "avg_price": 0,
                                    "message": f"Order {order_id} {order_status.lower()}"
                                }
                            
                            # Order still pending
                            if order_status in ["OPEN", "TRIGGER_PENDING", "PENDING"]:
                                time.sleep(check_interval)
                                elapsed += check_interval
                                continue
                
                 time.sleep(check_interval)
                 elapsed += check_interval
                
            except Exception as e:
                print(f"[WARN] Error checking order status: {e}")
                time.sleep(check_interval)
                elapsed += check_interval
        
        # Timeout - order status unknown
        return {
            "filled": False,
            "status": "TIMEOUT",
            "avg_price": 0,
            "message": f"Order {order_id} status check timed out after {max_wait_seconds}s"
        }

    def place_order(self, session, symbol, side, qty = None, quantity = None,
                    price=None,
                   order_type="MARKET",
                   product_type="INTRADAY",
                   exchange="NSE",
                   variety="NORMAL",
                   lot_based=False,
                   stop_loss=None,
                   trigger_price=None,
                   wait_for_confirmation=True,
                   scrip_consent="YES",
                   **kwargs):
        #  Support both qty and quantity arguments
        qty = qty or quantity
        if qty is None:
            return {"status": "error", "error": "Missing quantity/qty argument", "filled": False}
        
        """Place an order with live price and stop-loss support."""
        if not session or "obj" not in session:
            return {"status": "error", "error": "No active session object provided.", "filled": False}

        side_upper = side.upper().replace("_", "").replace(" ", "")
        if side_upper in ["BUY", "LONG"]:
            api_side = "BUY"
        elif side_upper in ["SELL", "SHORT", "SHORTSELL", "SELLSHORT"]:
            api_side = "SELL"
        elif side_upper in ["BUYCOVER", "COVER"]:
            api_side = "BUY"
        else:
            return {"status": "error", "error": f"Invalid side: {side}", "filled": False}

        symbol_token = self.get_symbol_token(symbol)
        if not symbol_token:
            return {"status": "error", "error": f"Symbol token not found for {symbol}", "filled": False}

        if lot_based:
            instruments = self._load_instruments()
            lot_size = 1
            for item in instruments:
                asset = (item.get("asset_symbol") or item.get("underlying_symbol") or "").upper()
                if asset == symbol.upper():
                    lot_size = item.get("lot_size") or item.get("lotSize") or 1
                    break
            qty = int(qty) * int(lot_size)

        price_value = "0" if order_type.upper() == "MARKET" else str(price or "0")
        stoploss_value = str(stop_loss or "0")
        trigger_value = str(trigger_price or "0")

        orderparams = {
            "variety": variety,
            "tradingsymbol": f"{symbol}-EQ" if not symbol.endswith("-EQ") else symbol,
            "symboltoken": str(symbol_token),
            "transactiontype": api_side,
            "exchange": exchange,
            "ordertype": order_type.upper(),
            "producttype": product_type.upper(),
            "duration": "DAY",
            "price": price_value,
            "quantity": str(int(qty)),
            "squareoff": "0",
            "stoploss": stoploss_value,
            "triggerprice": trigger_value,
            "scripconsent": (scrip_consent or "YES").upper()
        }

        try:
            print(f"[ORDER] Placing {api_side} order: {symbol} x {qty} @ {price_value} | SL: {stoploss_value} | Type: {order_type}")
            resp = self._call_api(session["obj"].placeOrder, orderparams)

            order_id = None

            # Handle both dict and raw string responses from Angel API
            if isinstance(resp, dict):
                if resp.get("status") is False:
                    return {"status": "error", "error": resp.get("message", "Order failed"), "filled": False, "raw": resp}
                data = resp.get("data") or {}
                if isinstance(data, dict):
                    order_id = data.get("orderid") or data.get("orderId")
            elif isinstance(resp, str):
                # Sometimes Angel API returns only the order ID as plain text
                order_id = resp.strip()

            if not order_id:
                return {"status": "error", "error": "No order ID in response", "filled": False, "raw": resp}

            if wait_for_confirmation:
                confirmation = self._wait_for_order_confirmation(session, order_id)
                if confirmation.get("filled"):
                    print(f"[ORDER] Filled: {symbol} @ {confirmation['avg_price']:.2f}")
                    return {"status": "success", "order_id": order_id, "filled": True,
                            "avg_price": confirmation["avg_price"], "raw": resp}
                else:
                    return {"status": "error", "order_id": order_id, "filled": False,
                            "error": confirmation["message"], "raw": resp}
            else:
                return {"status": "success", "order_id": order_id, "filled": None, "raw": resp}

        except Exception as e:
            return {"status": "error", "error": str(e), "filled": False}

    def modify_order(self, session, order_id, new_price=None, new_qty=None, **kwargs):
        """Modify an existing order."""
        if not session or "obj" not in session:
            return {"status": "error", "error": "No active session object provided."}
        
        params = {
            "variety": kwargs.get("variety", "NORMAL"),
            "orderid": str(order_id)
        }
        
        if new_price is not None:
            params["price"] = str(new_price)
        if new_qty is not None:
            params["quantity"] = str(new_qty)
            
        for key in ["ordertype", "producttype", "duration", "tradingsymbol", "symboltoken", "exchange"]:
            if key in kwargs:
                params[key] = str(kwargs[key])
                
        try:
            resp = self._call_api(session["obj"].modifyOrder, params)
            return {"status": "success", "raw": resp}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def cancel_order(self, session, order_id, variety="NORMAL"):
        """Cancel an order."""
        if not session or "obj" not in session:
            return {"status": "error", "error": "No active session object provided."}
        try:
            resp = self._call_api(session["obj"].cancelOrder, order_id, variety)
            return {"status": "success", "raw": resp}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def get_order_book(self, session):
        """Get order book."""
        if not session or "obj" not in session:
            return {"status": "error", "error": "No active session object provided."}
        try:
            resp = self._call_api(session["obj"].orderBook)
            return {"status": "success", "raw": resp}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def get_ltp(self, session, exchange, trading_symbol, symbol_token):
        """Get LTP (Last Traded Price)."""
        if not session or "obj" not in session:
            return {"status": "error", "error": "No active session object provided."}
        try:
            resp = self._call_api(session["obj"].ltpData, exchange, trading_symbol, symbol_token)
            return {"status": "success", "raw": resp}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def get_positions(self, session):
        """Get positions."""
        if not session or "obj" not in session:
            return {"status": "error", "error": "No active session object provided."}
        try:
            resp = self._call_api(session["obj"].position)
            return {"status": "success", "raw": resp}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def get_holdings(self, session):
        """Get holdings."""
        if not session or "obj" not in session:
            return {"status": "error", "error": "No active session object provided."}
        try:
            resp = self._call_api(session["obj"].holding)
            return {"status": "success", "raw": resp}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def get_trade_book(self, session):
        """Get trade book."""
        if not session or "obj" not in session:
            return {"status": "error", "error": "No active session object provided."}
        try:
            resp = self._call_api(session["obj"].tradeBook)
            return {"status": "success", "raw": resp}
        except Exception as e:
            return {"status": "error", "error": str(e)}