# Implementation Plan — Remaining EOD / Session Review Issues

**Repo:** `mintzy-plugin-kelly`  
**Date:** 2026-09-10  
**Scope:** Fix review issues **#4, #5, #6, #8, #9, #10, #11**  
**Out of scope (for now):** **#7** multi-session same broker account (see §7)

---

## 0. Current state (already done)

| Review # | Issue | Status |
|----------|--------|--------|
| 1 | Same symbol — manual qty exited | ✅ Fixed — `_session_open_qty` + `build_eod_exit_plan()` |
| 2 | Empty allow-list → no exit | ✅ Fixed — fallback chain + no `_eod_exit_done` on ledger-open / plan-empty |
| 3 | `_eod_exit_done` set too early | ✅ Fixed — `try_begin_eod_exit` / `mark_eod_exit_done` in all 3 traders |

**New files already in repo:**
- `utils/session_ledger.py`
- `utils/eod_exit.py`

---

## 1. What we still need to fix

| # | Severity | Issue | Action in this plan |
|---|----------|--------|---------------------|
| 4 | Medium | Early EOD return skips cleanup (`positions`, sync, logs) | Phase 1 |
| 5 | Medium | Redis meta write best-effort | Phase 2 |
| 6 | Medium | Redis meta not updated at runtime | Phase 3 (lightweight) |
| 7 | Medium | Multi-session same Angel account | **Deferred** (§7) |
| 8 | Medium | Breaking ops change (`EXIT_ONLY_SESSION_SYMBOLS`) | Phase 4 (docs + deploy note) |
| 9 | Low | Duplicate Redis key builders | **Phase 5 — critical care** |
| 10 | Low | Alert spam on every skipped symbol | Phase 6 |
| 11 | Low | Swallowed `on_skip` exceptions | Phase 6 |

---

## 2. Implementation phases

### Phase 1 — Issue #4: Unified EOD shutdown cleanup

**Goal:** Har EOD path (success, empty plan, retry-fail) par internal state consistent ho.

**New helper** in `utils/eod_exit.py`:

```python
def finalize_eod_shutdown(trader, *, clear_positions=True, sync_broker=False, persist_logs=True):
    """
    - clear self.positions + _session_open_qty
    - optional _sync_cash_with_broker()
    - optional empty eod_exit_records persist (no-op if none)
    """
```

**Call sites** (all 3 traders — `_run_eod_exit_body`):

| Path | `mark_eod_exit_done` | `finalize_eod_shutdown` |
|------|----------------------|-------------------------|
| Exit plan empty + ledger flat (safe complete) | ✅ Yes | ✅ Yes (clear book) |
| Exit plan empty + ledger open (retry) | ❌ No | ❌ No |
| Full exit success | ✅ Yes | ✅ Yes (after orders + sync) |
| Exception | ❌ No | ⚠️ Optional partial clear (log only) |

**Files:** `utils/eod_exit.py`, `auto_trader.py`, `auto_trader_exposure_expansion.py`, `auto_trader_exposure_expansion_org.py`

**Acceptance:**
- Early “no session positions” path clears `self.positions` and `_session_open_qty`
- `_sync_cash_with_broker()` runs on completed EOD (success or safe-empty)

---

### Phase 2 — Issue #5: Redis meta reliability at live start

**Goal:** Live worker kabhi bina Redis meta ke trade na kare (production).

**Changes:**

1. **`session_manager.py` → `start_session()`**
   - Redis `setex` par **2 retries** (100ms backoff)
   - Fail par: live (`strategy != "B"`) + `EOD_STRICT_REDIS_META=true` → terminate worker + `raise` (already partially done — add retry first)

2. **Worker boot validation** (`session_manager.py` `_trader_worker` after trader init):
   ```python
   if strategy != "B" and not symbol_allocations:
       raise RuntimeError("Live start without symbol allocations")
   ```

3. **Alert** on Redis meta save failure (even in soft mode):
   ```python
   print("[SessionManager] CRITICAL: Redis meta save failed ...")
   ```

**Files:** `session_manager.py`

**Acceptance:**
- Transient Redis blip → retry succeeds
- Hard fail on live when strict flag on
- `symbol_allocations` never empty at live worker start

---

### Phase 3 — Issue #6: Runtime symbol list sync (lightweight)

**Goal:** EOD allow-list runtime changes se out-of-sync na ho.

**Approach (minimal — no full dynamic rotation feature):**

1. **On every engine fill** (`_track_engine_fill`): optional Redis `SADD` to  
   `autotrader:session:{id}:symbols_traded` (set of normalized symbols, TTL 86400)

2. **`get_session_symbols_from_redis()`** merge order:
   ```
   meta.symbols  ∪  symbols_traded set  ∪  symbol_allocations  ∪  ledger keys
   ```

3. **Do NOT remove** symbols from meta at runtime (only additive) — avoids accidental shrink.

**New key:** `autotrader:session:{session_id}:symbols_traded` (Redis SET)

**Files:** `utils/redis_keys.py` (Phase 5), `utils/session_symbols.py`, `utils/session_ledger.py`, `_track_engine_fill` in 3 traders

**Acceptance:**
- Symbol traded mid-session but missing from start payload → still in EOD allow-list
- Start payload symbol never traded → not in ledger → **not exited** (ledger qty = 0)

---

### Phase 4 — Issue #8: Ops / deploy documentation

**Goal:** Team aur clients ko behavior change clear ho.

**Actions (no heavy code):**
- Add **§ Production env** (below) to VM `.env` / systemd environment
- Deploy checklist message:
  > “EOD ab sirf session symbols + engine ledger qty exit karega. Manual HARDWYN safe. Same-symbol manual qty safe (ledger). Full account flatten nahi hoga unless `EXIT_ONLY_SESSION_SYMBOLS=false`.”

**Optional:** One startup log line in worker:
```python
print(f"[EOD-CONFIG] EXIT_ONLY_SESSION_SYMBOLS=... EOD_USE_SESSION_LEDGER=... EOD_STRICT_REDIS_META=...")
```

**Files:** `session_manager.py` worker boot log only

---

### Phase 5 — Issue #9: Single source of truth for Redis keys ⚠️

**This is the highest-risk refactor. Keys must not change in production.**

#### 5.1 Create `utils/redis_keys.py` (canonical)

```python
# CANONICAL — do not duplicate elsewhere
SESSION_PREFIX = "autotrader:session:"

def session_pid_key(session_id: str) -> str:
    return f"{SESSION_PREFIX}{session_id}"           # existing: autotrader:session:{id}

def session_meta_key(session_id: str) -> str:
    return f"{SESSION_PREFIX}{session_id}:meta"      # existing

def session_order_ids_key(session_id: str) -> str:
    return f"{SESSION_PREFIX}{session_id}:order_ids" # existing

def session_symbols_traded_key(session_id: str) -> str:
    return f"{SESSION_PREFIX}{session_id}:symbols_traded"  # new Phase 3

# Other autotrader keys (not under session prefix)
SIMULATION_STOP_PREFIX = "autotrader:simulation_stop:"
PYRAMID_RESULT_PREFIX = "autotrader:pyramid_result:"
STOP_JOB_PREFIX = "autotrader:stop_job:"

def simulation_stop_key(session_id): ...
def pyramid_result_key(session_id): ...
def stop_job_key(session_id): ...
def exit_request_key(session_id): ...      # autotrader:exit_request:{id}
def exit_result_key(session_id, symbol): ...
def exit_status_key(session_id): ...       # autotrader:exit_status:{id}
def rms_exited_key(session_id): ...        # autotrader:rms_exited:{id}
```

#### 5.2 Migration map (must stay byte-identical)

| Current location | Current value | New function |
|------------------|---------------|--------------|
| `session_manager.REDIS_KEY_PREFIX` + `session_id` | `autotrader:session:{id}` | `session_pid_key()` |
| `session_manager._session_meta_key()` | `autotrader:session:{id}:meta` | `session_meta_key()` |
| `session_symbols.session_meta_redis_key()` | same | `session_meta_key()` |
| `session_ledger.order_ids_redis_key()` | `autotrader:session:{id}:order_ids` | `session_order_ids_key()` |
| `session_manager` exit queue | `autotrader:exit_request:{id}` | `exit_request_key()` |
| `api_server` exit status | `autotrader:exit_status:{id}` | `exit_status_key()` |

#### 5.3 Safe migration steps

1. Add `utils/redis_keys.py` with functions returning **exact same strings** as today
2. Add **unit-style assert script** `scripts/verify_redis_keys.py`:
   ```python
   assert session_meta_key("session_test") == "autotrader:session:session_test:meta"
   # ... all keys
   ```
3. Replace imports one file at a time:
   - `session_manager.py`
   - `utils/session_symbols.py`
   - `utils/session_ledger.py`
   - `api_server.py` (exit_status only)
   - `auto_trader_exposure_expansion_org.py` (exit_status, rms_exited)
4. **Delete** duplicate constants from `session_symbols.py` / `session_ledger.py` after migration
5. Update `_delete_session_redis_keys()` to also delete:
   - `session_order_ids_key(session_id)`
   - `session_symbols_traded_key(session_id)` (Phase 3)
   - **Do NOT delete** `exit_status`, `pyramid_result` here unless already intended

#### 5.4 What we must NOT do

- ❌ Change prefix `autotrader:session:` 
- ❌ Change suffix order (`:meta`, `:order_ids`)
- ❌ Rename pid key from `autotrader:session:{id}` to something else
- ❌ Mix session_id formats (always full `session_20260910...` string)

**Acceptance:** `verify_redis_keys.py` passes; grep shows no duplicate `REDIS_SESSION_PREFIX` outside `redis_keys.py`

---

### Phase 6 — Issues #10 & #11: Alerts and error logging

**Issue #10 — alert spam**

In `prepare_eod_exit_plan` / `build_eod_exit_plan`:
- Collect skipped symbols in a list
- Call `on_skip` **once** at end:
  ```python
  on_skip_summary(skipped_count, skipped_symbols[:5])
  ```
- Trader callback:
  ```python
  def _on_eod_skip_summary(count, sample):
      if count:
          self.alerts.notify(f"EOD skipped {count} non-session position(s)")
  ```

**Issue #11 — swallowed exceptions**

In any remaining `on_skip` handler:
```python
except Exception as e:
    print(f"[EOD] on_skip callback failed: {e}")
```

**Files:** `utils/session_ledger.py`, `utils/eod_exit.py`, 3 traders (simplify callbacks)

---

## 3. Issue #7 — Multi-session same broker account (DEFERRED)

**Why deferred:** Proper fix needs **account-level** coordination, not session-level:

| Approach | Complexity |
|----------|------------|
| Ops rule: 1 live worker per Angel account | Simple (no code) |
| Redis lock `eod:lock:{client_code}` at EOD | Medium |
| Broker sub-accounts | Infra change |

**Already improved:** Per-session `_session_open_qty` → each worker exits **its** qty, not full broker net.

**Recommendation for production:**  
→ **One live session per broker login per day.** Document in ops runbook. Revisit #7 only if clients need parallel live sessions on same account.

---

## 4. Suggested implementation order

```
Phase 5a — utils/redis_keys.py + verify_redis_keys.py (no callers changed yet)
Phase 5b — Migrate callers to redis_keys.py
Phase 1  — finalize_eod_shutdown
Phase 2  — Redis meta retry + strict prod env
Phase 3  — symbols_traded set
Phase 6  — alert summary + logging
Phase 4  — deploy notes + startup config log
```

**Estimated touch:** ~8–10 files, ~250–350 LOC

---

## 5. Testing checklist (before prod deploy)

| Test | Expected |
|------|----------|
| Session ITC + manual HARDWYN | Exit ITC only, skip HARDWYN |
| Session 10 ITC + manual 90 ITC | Exit **10** only (`[EOD-PLAN]`) |
| Manual ITC only (session config has ITC, no trade) | Exit 0 |
| Redis meta missing | Fallback to ledger / allocations; alert if ledger open |
| EOD called twice | Orders once; `_eod_exit_done` blocks duplicate |
| `verify_redis_keys.py` | All assertions pass |
| Deploy after mid-session start | Ledger only from post-deploy fills (start fresh session day 1) |

---

## 6. Recommended production `.env`

```bash
# ─── EOD / Session-scoped exit (NEW — keep these ON for prod) ───
EXIT_ONLY_SESSION_SYMBOLS=true
EOD_USE_SESSION_LEDGER=true
EOD_STRICT_REDIS_META=true

# ─── Existing plugin (keep your current values) ───
REDIS_HOST=<your-elasticache-or-redis-host>
REDIS_PORT=6379
MAX_TRADER_WORKERS=6
MONGO_URI=<your-mongo-uri>
MONGO_DB_NAME=mintzy_plugin
MONGO_CONFIG_DB_NAME=test
MINTZY_LOGS_DIR=/home/admin/mintzy-plugin/logs

# ─── Live start (existing) ───
LIVE_START_WORKER_GRACE_SECONDS=60
```

### Env variable reference

| Variable | Prod value | Purpose |
|----------|------------|---------|
| `EXIT_ONLY_SESSION_SYMBOLS` | `true` | EOD par sirf session symbols touch karo; manual HARDWYN skip |
| `EOD_USE_SESSION_LEDGER` | `true` | Exit qty = engine ledger qty, broker net cap; same-symbol manual safe |
| `EOD_STRICT_REDIS_META` | `true` | Live start fail if Redis meta (pid + symbols) save fail — no blind worker |
| `EXIT_ONLY_SESSION_SYMBOLS` | `false` | **Rollback only** — purana “exit all broker positions” behavior |
| `EOD_USE_SESSION_LEDGER` | `false` | **Rollback only** — symbol filter + full broker net per allowed symbol |

---

## 7. Redis keys — complete reference

### 7.1 Session worker keys (TTL usually 86400 = 24h)

| Key pattern | Type | Written by | Read by | Purpose |
|-------------|------|------------|---------|---------|
| `autotrader:session:{session_id}` | STRING (pid) | `SessionManager.start_session` | `prepare_for_live_start`, monitor | Worker PID — live/paper running check |
| `autotrader:session:{session_id}:meta` | STRING (JSON) | `SessionManager.start_session` | `prepare_for_live_start`, EOD allow-list | `{strategy, pid, started_at, symbols[]}` |
| `autotrader:session:{session_id}:order_ids` | SET | `_track_engine_fill` / EOD orders | Ledger rebuild (future) | Engine-placed order IDs for session |
| `autotrader:session:{session_id}:symbols_traded` | SET | Phase 3 `_track_engine_fill` | EOD allow-list merge | Symbols engine actually traded today |

**Deleted on paper stop / worker terminate:** pid + meta (today). **Phase 5 add:** also delete `order_ids` + `symbols_traded`.

### 7.2 Session control / handoff keys

| Key pattern | TTL | Purpose |
|-------------|-----|---------|
| `autotrader:simulation_stop:{session_id}` | 300s | Gateway/API paper stop signal → worker |
| `autotrader:pyramid_result:{session_id}` | 600s | Sim→live pyramid handoff payload |
| `autotrader:stop_job:{session_id}` | 600s | Async stop job status |
| `autotrader:exit_request:{session_id}` | LIST | API single-symbol exit queue → worker `lpop` |
| `autotrader:exit_result:{session_id}:{SYMBOL}` | 60s | Exit API response for one symbol |
| `autotrader:exit_status:{session_id}` | 86400s | EOD initiated flag → `api_server` `/exit-status` |
| `autotrader:rms_exited:{session_id}` | 86400s | RMS portfolio exit marker (strategy C) |

### 7.3 Live PnL / market data keys (not session-prefixed)

| Key pattern | TTL | Purpose |
|-------------|-----|---------|
| `live_pnl:{session_id}` | 5s | Frontend live PnL snapshot |
| `price:live:{ticker}` | varies | LTP cache (prediction/market service) |
| `TREND:{symbol}` | — | Trend signal cache |
| LTP shared keys in org/expansion | 5400s | Per-candle shared LTP fetch |

### 7.4 Key relationship diagram

```
session start
    │
    ├─► autotrader:session:{id}          = PID
    ├─► autotrader:session:{id}:meta     = {symbols, strategy, pid}
    │
engine orders
    ├─► autotrader:session:{id}:order_ids     (SADD per order)
    └─► autotrader:session:{id}:symbols_traded (Phase 3, SADD per fill)

EOD
    ├─ READ meta.symbols + symbols_traded + ledger
    ├─ BUILD exit plan (ledger qty cap by broker net)
    └─ WRITE autotrader:exit_status:{id}

paper stop / terminate
    └─ DELETE pid + meta (+ order_ids, symbols_traded after Phase 5)
```

---

## 8. Deploy file list (after all phases)

```
utils/redis_keys.py          (new)
utils/eod_exit.py            (updated)
utils/session_ledger.py      (updated)
utils/session_symbols.py     (updated)
session_manager.py
api_server.py                (exit_status key only, if migrated)
auto_trader.py
auto_trader_exposure_expansion.py
auto_trader_exposure_expansion_org.py
scripts/verify_redis_keys.py (new)
```

---

## 9. Sign-off criteria

- [ ] All phases implemented
- [ ] `py -m scripts.verify_redis_keys` passes
- [ ] Manual test matrix (§5) on test VM `44.208.177.169`
- [ ] Production `.env` updated (§6)
- [ ] Ops briefed on Issue #8 behavior change
- [ ] Issue #7 documented as “one live session per account” policy
