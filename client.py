import redis
import os
import json
import pytz
from urllib.parse import quote
from datetime import timedelta, datetime
import re
import requests
import pandas as pd
from typing import List, Union, Optional
import traceback

# token manager
from utils.token_manager import get_access_token


# ==========================================================
# PREDICTION CLIENT (ONLY prediction)
# ==========================================================

class PredictionClient:

    SUPPORTED_TICKERS = {
        "ABB", "ACC", "ADANIGREEN", "ADANITOTAL", "APOLLOHOSP",
    "BAJAJHLDNG", "BANDHANBNK", "BERGEPAINT", "BOSCHLTD", "CANBK",
    "CIPLA", "DABUR", "DLF", "DRREDDY", "HAVELLS",
    "HDFCAMC", "ICICIGI", "ICICIPRULI", "INDUSTOWER", "INFOEDGE",
    "JINDALSTEL", "JSWENERGY", "LUPIN", "MARICO", "MOTHERSON",
    "MUTHOOTFIN", "NMDC", "OIL", "PAGEIND", "PIIND",
    "PNB", "RECLTD", "SHREECEM", "SIEMENS", "SRF",
    "TATACHEM", "TATACONSUM", "TATAELXSI", "TORNTPHARM", "TRENT",
    "UBL", "ZOMATO", "ALKEM", "ASTRAL", "AUROPHARMA",
    "COLPAL", "CONCOR", "FEDERALBNK", "LICI", "MRF",
    "NAUKRI", "TORNTPOWER","TCS","HDFCBANK","BHARTIARTL","ICICIBANK","SBIN","INFY","BAJFINANCE",
    "HINDUNILVR","ITC","MARUTI","HCLTECH","SUNPHARMA","KOTAKBANK","AXISBANK",
    "ULTRACEMCO","BAJAJFINSV","ADANIPORTS","NTPC","ONGC","ASIANPAINT",
    "JSWSTEEL","ADANIPOWER","WIPRO","ADANIENT","POWERGRID","NESTLEIND",
    "COALINDIA","INDIGO","HINDZINC","TATASTEEL","VEDL","SBILIFE","EICHERMOT",
    "GRASIM","HINDALCO","LTIM","TVSMOTOR","DIVISLAB","HDFCLIFE","PIDILITIND",
    "CHOLAFIN","BRITANNIA","AMBUJACEM","GAIL","BANKBARODA","GODREJCP",
    "HEROMOTOCO","TATAPOWER"
    }

    VALID_KEYS = {"XeyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"}

    _TIMESTAMP_VALUE_RE = re.compile(
        r'^\s*(?P<timestamp>\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{2}(?::\d{2})?)\s+(?P<value>-?\d+(?:\.\d+)?)\s*$'
    )

    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
        print(f"[PRED CLIENT INIT] URL={base_url}, API_KEY={api_key[:30]}...")

    # ---------------- RESPONSE FORMATTER ----------------

    def _format_table(self, response_json: dict, tickers: List[str], parameters: List[str]) -> pd.DataFrame:
        rows = []
        result = response_json.get("result", {})

        if not isinstance(result, dict):
            print(f"[PRED CLIENT] Invalid response format: {type(result)}")
            return pd.DataFrame([{
                "Error": "Invalid response format from prediction API",
                "Raw": str(response_json)
            }])

        for ticker in tickers:
            ticker_block = result.get(ticker, {})
            if not isinstance(ticker_block, dict):
                print(f"[PRED CLIENT] Unexpected response for {ticker}: {ticker_block!r}")
                rows.append({"Ticker": ticker, "Parameter": "N/A", "Error": f"Bad API response: {ticker_block!r}"})
                continue

            for param in parameters:
                param_data = ticker_block.get(param)

                if not param_data:
                    rows.append({
                        "Ticker": ticker,
                        "Parameter": param,
                        "Error": "No prediction data"
                    })
                    continue

                if isinstance(param_data, dict):
                    timestamps     = param_data.get("timestamps", [])
                    prices         = param_data.get("predicted_prices", [])
                    traj_pcts      = param_data.get("trajectory_pcts", [])
                    risk_regimes   = param_data.get("risk_regimes", [])
                    directions     = param_data.get("directions", [])

                    if not timestamps or not prices:
                        rows.append({"Ticker": ticker, "Parameter": param, "Error": "Empty prediction data"})
                        continue

                    for i, ts in enumerate(timestamps):
                        price = prices[i] if i < len(prices) else None
                        if price is None:
                            continue
                        rows.append({
                            "Ticker":          ticker,
                            "Parameter":       param,
                            "Timestamp":       ts,
                            "Predicted Price": float(price),
                            "trajectory_pct":  float(traj_pcts[i])   if i < len(traj_pcts)    else 0.0,
                            "risk_regime":     int(risk_regimes[i])   if i < len(risk_regimes)  else 0,
                            "direction":       directions[i]           if i < len(directions)    else "UP",
                        })

                # ---- OLD: legacy CSV string response (fallback) ----
                elif isinstance(param_data, str):
                    for line in param_data.splitlines():
                        m = self._TIMESTAMP_VALUE_RE.match(line.strip())
                        if not m:
                            continue
                        rows.append({
                            "Ticker":          ticker,
                            "Parameter":       param,
                            "Timestamp":       m.group("timestamp"),
                            "Predicted Price": float(m.group("value"))
                        })

                else:
                    print(f"[PRED CLIENT] Unsupported param_data type for {ticker}.{param}: {type(param_data)}")
                    rows.append({"Ticker": ticker, "Parameter": param, "Error": f"Unsupported type: {type(param_data)}"})

        df_result = pd.DataFrame(rows)
        print(f"[PRED CLIENT] Formatted {len(df_result)} prediction rows")
        return df_result

    # ---------------- PREDICTION CALL ----------------

    def get_prediction_once(
        self,
        tickers: Union[str, List[str]],
        time_frame: str = "3 hours",
        parameters: Union[str, List[str]] = "close",
        candle: Optional[str] = None,
        single_run: bool = False,
        debug: bool = False
    ) -> pd.DataFrame:

        print("ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â°ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¥ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â°ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¥ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â°ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¥ PREDICTION CLIENT ENTERED ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â°ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¥ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â°ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¥ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â°ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¸ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¥", flush=True)
        print(f"[PRED CLIENT] ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ CALLED: tickers={tickers}, time_frame={time_frame}, candle={candle}")

        try:
            if self.api_key not in self.VALID_KEYS:
                raise RuntimeError("Unauthorized API key")

            if isinstance(tickers, str):
                tickers = [tickers]

            tickers = [t.upper().replace(".NS", "").strip() for t in tickers]

            invalid = [t for t in tickers if t not in self.SUPPORTED_TICKERS]
            if invalid:
                raise RuntimeError(f"Unsupported tickers: {invalid}")

            if isinstance(parameters, str):
                parameters = [parameters]

            if candle:
                candle = candle.lower()
                if candle.isdigit():
                    candle += "m"

            payload = {
                "action": {
                    "action_type": "predict",
                    "predict": {
                        "given": {
                            "ticker": tickers,
                            "time_frame": time_frame,
                            "candle": candle
                        },
                        "required": {"parameters": parameters}
                    }
                }
            }

            print(f"[PRED CLIENT] Payload built: {len(tickers)} tickers")

            resp = requests.post(
                self.base_url,
                json=payload,
                headers={"X-API-Key": self.api_key},
                timeout=180
            )

            resp.raise_for_status()

            df = self._format_table(resp.json(), tickers, parameters)

            if df.empty:
                return df

            # CRITICAL FIXES

            if "Timestamp" in df.columns:
                df["Timestamp"] = pd.to_datetime(df["Timestamp"])
                df = df.sort_values("Timestamp")

            df.reset_index(drop=True, inplace=True)
            df["Timestamp"] = df["Timestamp"].astype(str)

            print(f"[PRED CLIENT] ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã¢â‚¬Å“ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œ Returning DataFrame {len(df)} rows")

            return df

        except Exception:
            traceback.print_exc()
            raise



# ==========================================================
# MARKET CLIENT (ONLY redis + upstox)
# ==========================================================

class MarketClient:

    def __init__(self):
        self.redis_client = None
        self.access_token = None
        self.ticker_map = {}

        self._init_redis()
        self._load_access_token()
        self._load_ticker_map()

    # ---------------- INIT ----------------

    def _init_redis(self):
        try:
            self.redis_client = redis.RedisCluster(
                host=os.environ.get("REDIS_HOST", "10.45.41.115"),
                port=int(os.environ.get("REDIS_PORT", "6379")),
                ssl=True,
                ssl_cert_reqs=None,
                decode_responses=True,
                socket_connect_timeout=5,
            )
            self.redis_client.ping()
            print("[MARKET CLIENT] Redis connected")
        except Exception as e:
            print("[MARKET CLIENT] Redis NOT available:", e)
            self.redis_client = None

    def _load_access_token(self):
        self.access_token = get_access_token()

    def _load_ticker_map(self):
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            with open(os.path.join(base_dir, "ticker.json")) as f:
                self.ticker_map = json.load(f)
        except Exception:
            self.ticker_map = {}

    # ---------------- LIVE PRICE ----------------

    def fetch_price(self, ticker, target_datetime, candle):
        ist = pytz.timezone("Asia/Kolkata")

        ticker_key = ticker.replace(".NS", "").upper()
        symbol_code = self.ticker_map.get(ticker_key)
        if not symbol_code:
            print(f"[Upstox] Missing instrument key for {ticker_key}")
            return None

        symbol_code = quote(symbol_code, safe="")

        candle = str(candle or "5m").lower()
        try:
            step = int(candle[:-1])
        except:
            step = 5

        now = datetime.now(ist)
     
        url = f"https://api.upstox.com/v2/historical-candle/intraday/{symbol_code}/1minute"
        to_time = target_datetime.astimezone(ist) if target_datetime else datetime.now(ist)
        from_time = to_time - timedelta(minutes=step * 2)
        headers = {"Authorization": f"Bearer {self.access_token}"}


        params = {
            "from": from_time.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            "to": to_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        }
       
        res = requests.get(url, headers=headers, params=params, timeout=15)

        if res.status_code == 401:
            self._load_access_token()
            headers["Authorization"] = f"Bearer {self.access_token}"
            res = requests.get(url, headers=headers, params=params, timeout=15)

        if res.status_code != 200:
            print(f"[Upstox] API error: {res.status_code} - {res.text}")
            return None

        candles = res.json().get("data", {}).get("candles", [])
        if not candles:
            return None

        c = candles[-1]  # Use the last candle in the list
        return {
            "Open": float(c[1]),
            "High": float(c[2]),
            "Low": float(c[3]),
            "Close": float(c[4]),
        }
