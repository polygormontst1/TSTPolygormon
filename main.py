import os, re, time, json, sqlite3, asyncio, socket, base64
import requests
from telegram import Bot
from telegram.error import TelegramError

# Google Sheets
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# =========================
# CONFIG
# =========================
def log(msg: str):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)

log("VERSION: GSHEETS_BUILD_001")

BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
SOURCE_CHAT_ID = int(os.environ["SOURCE_CHAT_ID"].strip())
TARGET_CHAT_ID = int(os.environ["TARGET_CHAT_ID"].strip())

PROXY_PRICE_URL = os.getenv("PROXY_PRICE_URL", "https://workerrr.developctsro.workers.dev").strip().rstrip("/")
CHECK_INTERVAL_SEC = int(os.getenv("CHECK_INTERVAL_SEC", "15"))
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "3"))
ENTRY_REF_MODE = os.getenv("ENTRY_REF_MODE", "HIGH").upper()
DB_PATH = os.getenv("DB_PATH", "bot.db")

LEVERAGE = float(os.getenv("LEVERAGE", "20"))

# RULES
ACTIVATION_VALID_DAYS = int(os.getenv("ACTIVATION_VALID_DAYS", "30"))
REPORTING_ACTIVE_DAYS = int(os.getenv("REPORTING_ACTIVE_DAYS", "60"))
ENTRY2_DISABLE_PROFIT_PCT = float(os.getenv("ENTRY2_DISABLE_PROFIT_PCT", "15"))

# INSTANCE LOCK
LOCK_TTL_SEC = int(os.getenv("LOCK_TTL_SEC", "90"))
LOCK_RENEW_EVERY_SEC = int(os.getenv("LOCK_RENEW_EVERY_SEC", "30"))
INSTANCE_ID = os.getenv("INSTANCE_ID", "").strip()
if not INSTANCE_ID:
    INSTANCE_ID = f"{socket.gethostname()}-pid{os.getpid()}"

TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

# Google Sheets ENV
GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
GSHEET_SIGNALS_TAB = os.getenv("GSHEET_SIGNALS_TAB", "Signals").strip()
GSHEET_PROFITS_TAB = os.getenv("GSHEET_PROFITS_TAB", "Profits").strip()
GOOGLE_CREDS_JSON_B64 = os.getenv("GOOGLE_CREDS_JSON_B64", "").strip()

GSHEETS_ENABLED = bool(GSHEET_ID and GOOGLE_CREDS_JSON_B64)

# =========================
# PARSING
# =========================
PAIR_RE = re.compile(
    r"^\s*([A-Z0-9]+)\s*/\s*(USDT)\s*(LONG|SHORT|BUY|SELL)\b(?:\s+(ON)\b)?",
    re.IGNORECASE | re.MULTILINE
)

ENTRY1_RE = re.compile(r"1\.\s*Entry price:\s*([0-9.]+)\s*(?:-\s*([0-9.]+))?", re.IGNORECASE)
ENTRY2_RE = re.compile(r"2\.\s*Entry price:\s*([0-9.]+)\s*(?:-\s*([0-9.]+))?", re.IGNORECASE)

RES_RE = re.compile(r"Rezistenční úrovně:\s*(.+?)(?:\n\n|\nStop Loss:|\Z)", re.IGNORECASE | re.DOTALL)

def parse_range(a, b):
    x = float(a)
    y = float(b) if b else float(a)
    return (x, y) if x <= y else (y, x)

def fmt(x):
    if x is None:
        return "-"
    return f"{x:.8f}".rstrip("0").rstrip(".")

def pct_from_entry(price, entry, side):
    if entry is None or entry == 0:
        return 0.0
    return (price - entry) / entry * 100.0 if side == "LONG" else (entry - price) / entry * 100.0

def parse_signal(text: str):
    m = PAIR_RE.search(text or "")
    if not m:
        return None

    base = m.group(1).upper()
    quote = m.group(2).upper()
    action = (m.group(3) or "").lower()
    has_on = (m.group(4) or "").lower() == "on"
    symbol = f"{base}{quote}"

    if action == "long":
        side = "LONG"
        mode = "MARKET"
    elif action == "short":
        side = "SHORT"
        mode = "WAIT" if has_on else "MARKET"
    elif action == "buy":
        side = "LONG"
        mode = "WAIT"
    elif action == "sell":
        side = "SHORT"
        mode = "WAIT"
    else:
        return None

    e1 = ENTRY1_RE.search(text or "")
    if not e1:
        return None
    entry1_low, entry1_high = parse_range(e1.group(1), e1.group(2))

    e2 = ENTRY2_RE.search(text or "")
    entry2_low = entry2_high = None
    if e2:
        entry2_low, entry2_high = parse_range(e2.group(1), e2.group(2))

    rm = RES_RE.search(text or "")
    if not rm:
        return None

    raw = rm.group(1)
    nums = re.findall(r"([0-9]*\.[0-9]+|[0-9]+(?:\.[0-9]+)?)", raw.replace(",", " "))
    tps = [float(n) for n in nums]
    if not tps:
        return None

    tps = sorted(tps, reverse=(side == "SHORT"))

    return {
        "symbol": symbol,
        "side": side,
        "mode": mode,
        "entry1_low": entry1_low,
        "entry1_high": entry1_high,
        "entry2_low": entry2_low,
        "entry2_high": entry2_high,
        "tps": tps
    }

# =========================
# DB
# =========================
def connect_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_message_id INTEGER UNIQUE,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            mode TEXT NOT NULL DEFAULT 'WAIT',
            entry1_low REAL NOT NULL,
            entry1_high REAL NOT NULL,
            entry2_low REAL,
            entry2_high REAL,
            tps_json TEXT NOT NULL,
            created_ts INTEGER NOT NULL,
            activated INTEGER NOT NULL DEFAULT 0,
            activated_ts INTEGER,
            activated_price REAL,
            tp_hits INTEGER NOT NULL DEFAULT 0,
            entry2_activated INTEGER NOT NULL DEFAULT 0,
            entry2_activated_ts INTEGER,
            entry2_activated_price REAL,
            tp1_rehit_after_entry2_sent INTEGER NOT NULL DEFAULT 0,
            avg_reached_after_entry2_sent INTEGER NOT NULL DEFAULT 0,
            reporting_expired INTEGER NOT NULL DEFAULT 0,
            sheet_row INTEGER
        )
    """)

    # Safe ALTER for existing DBs
    for sql in [
        "ALTER TABLE signals ADD COLUMN entry2_activated INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE signals ADD COLUMN entry2_activated_ts INTEGER",
        "ALTER TABLE signals ADD COLUMN entry2_activated_price REAL",
        "ALTER TABLE signals ADD COLUMN tp1_rehit_after_entry2_sent INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE signals ADD COLUMN avg_reached_after_entry2_sent INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE signals ADD COLUMN reporting_expired INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE signals ADD COLUMN sheet_row INTEGER",
        "ALTER TABLE signals ADD COLUMN max_profit_20x REAL NOT NULL DEFAULT 0",
        "ALTER TABLE signals ADD COLUMN tp_hwm_json TEXT",
    ]:
        try:
            conn.execute(sql)
        except Exception:
            pass

    conn.execute("""
        CREATE TABLE IF NOT EXISTS state (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn

def state_get(conn, key, default):
    row = conn.execute("SELECT v FROM state WHERE k=?", (key,)).fetchone()
    return row[0] if row else default

def state_set(conn, key, value):
    conn.execute(
        "INSERT INTO state(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (key, value)
    )
    conn.commit()

def save_signal(conn, source_message_id: int, s: dict):
    try:
        cur = conn.execute(
            """INSERT INTO signals(
                source_message_id, symbol, side, mode,
                entry1_low, entry1_high, entry2_low, entry2_high,
                tps_json, created_ts
            ) VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                source_message_id,
                s["symbol"], s["side"], s["mode"],
                s["entry1_low"], s["entry1_high"], s["entry2_low"], s["entry2_high"],
                json.dumps(s["tps"]),
                int(time.time())
            )
        )
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError:
        return None


# =========================
# DASH DATA HELPERS (TP GRID + EP2 HWM)
# =========================

def _col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n-1, 26)
        s = chr(65 + r) + s
    return s

def _dash_range(row_num: int):
    # Signals sheet layout (after core columns A..U):
    # V = MAX_20x
    # W = MP_spot
    # X = MP_20x
    # Y..AR = TP1..TP20
    return f"{GSHEET_SIGNALS_TAB}!V{row_num}:AR{row_num}"

def _write_dash_row(service, row_num: int, values: list):
    rng = _dash_range(row_num)
    service.spreadsheets().values().update(
        spreadsheetId=GSHEET_ID,
        range=rng,
        valueInputOption="RAW",
        body={"values": [values]}
    ).execute()

def _format_pct(val: float) -> str:
    return f"{val:.1f}%"

def compute_base_tp_pcts_20x(tps: list, entry_price: float, side: str) -> list:
    out = []
    if not entry_price:
        return [""] * 20
    for i in range(20):
        if i < len(tps):
            pct_spot = pct_from_entry(tps[i], entry_price, side)
            out.append(pct_spot * LEVERAGE)
        else:
            out.append("")
    return out

def combined_profit_20x(price: float, side: str, e1_price: float, e2_active: bool, e2_price: float):
    if not e1_price:
        return None, None, None
    p1 = pct_from_entry(price, e1_price, side)
    p2 = pct_from_entry(price, e2_price, side) if (e2_active and e2_price) else 0.0
    return p1, p2, (p1 + p2) * LEVERAGE

# =========================
# GOOGLE SHEETS
# =========================
SIGNALS_HEADERS = [
    "SignalID", "SourceMessageID",
    "CreatedTS", "Symbol", "Side", "Mode",
    "Entry1Low", "Entry1High", "Entry2Low", "Entry2High",
    "TPCount", "TPsJson",
    "Status",
    "Activated", "ActivatedTS", "ActivatedPrice",
    "Entry2Activated", "Entry2ActivatedTS", "Entry2ActivatedPrice",
    "TPHits",
    "ReportingExpired"
]

PROFITS_HEADERS = [
    "EventTS", "SignalID", "Symbol", "Side",
    "TPIndex", "TPPrice",
    "Entry1Price", "ProfitSpotPct_E1", "ProfitLevPct_E1",
    "Entry2Price", "ProfitSpotPct_E2", "ProfitLevPct_E2",
    "Note"
]

def _gsheets_build():
    creds_json = base64.b64decode(GOOGLE_CREDS_JSON_B64.encode("utf-8")).decode("utf-8")
    info = json.loads(creds_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def _ensure_headers(service):
    # Signals
    for tab_name, headers in [(GSHEET_SIGNALS_TAB, SIGNALS_HEADERS), (GSHEET_PROFITS_TAB, PROFITS_HEADERS)]:
        rng = f"{tab_name}!A1:Z1"
        resp = service.spreadsheets().values().get(spreadsheetId=GSHEET_ID, range=rng).execute()
        values = resp.get("values", [])
        if not values:
            service.spreadsheets().values().update(
                spreadsheetId=GSHEET_ID,
                range=f"{tab_name}!A1",
                valueInputOption="RAW",
                body={"values": [headers]}
            ).execute()

def _append_row(service, tab_name: str, row: list):
    resp = service.spreadsheets().values().append(
        spreadsheetId=GSHEET_ID,
        range=f"{tab_name}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]}
    ).execute()

    # updatedRange like 'Signals!A3:U3'
    updated_range = resp.get("updates", {}).get("updatedRange", "")
    m = re.search(r"!A(\d+):", updated_range)
    row_num = int(m.group(1)) if m else None
    return row_num

def _update_row(service, tab_name: str, row_num: int, row: list):
    rng = f"{tab_name}!A{row_num}"
    service.spreadsheets().values().update(
        spreadsheetId=GSHEET_ID,
        range=rng,
        valueInputOption="RAW",
        body={"values": [row]}
    ).execute()

def build_signals_row(db_row: dict):
    # Status derivation
    if db_row["reporting_expired"]:
        status = "EXPIRED"
    elif db_row["activated"] and db_row["tp_hits"] >= db_row["tp_count"]:
        status = "DONE"
    elif db_row["entry2_activated"]:
        status = "ENTRY2"
    elif db_row["activated"]:
        status = "ACTIVE"
    else:
        status = "NEW"

    return [
        db_row["id"], db_row["source_message_id"],
        db_row["created_ts"], db_row["symbol"], db_row["side"], db_row["mode"],
        db_row["entry1_low"], db_row["entry1_high"], db_row["entry2_low"], db_row["entry2_high"],
        db_row["tp_count"], db_row["tps_json"],
        status,
        db_row["activated"], db_row["activated_ts"], db_row["activated_price"],
        db_row["entry2_activated"], db_row["entry2_activated_ts"], db_row["entry2_activated_price"],
        db_row["tp_hits"],
        db_row["reporting_expired"]
    ]

async def gsheets_init_once(state):
    if not GSHEETS_ENABLED:
        return None
    if state.get("service"):
        return state["service"]
    try:
        service = await asyncio.to_thread(_gsheets_build)
        await asyncio.to_thread(_ensure_headers, service)
        state["service"] = service
        log(f"GSHEETS: enabled spreadsheet={GSHEET_ID} tabs=({GSHEET_SIGNALS_TAB},{GSHEET_PROFITS_TAB})")
        return service
    except Exception as e:
        log(f"GSHEETS init error: {e}")
        return None

async def gsheets_upsert_signal(service, conn, sid: int):
    if not service:
        return

    row = conn.execute(
        """SELECT
            id, source_message_id, created_ts, symbol, side, mode,
            entry1_low, entry1_high, entry2_low, entry2_high,
            tps_json, tp_hits, activated, activated_ts, activated_price,
            entry2_activated, entry2_activated_ts, entry2_activated_price,
            reporting_expired, sheet_row, max_profit_20x, tp_hwm_json
        FROM signals WHERE id=?""",
        (sid,)
    ).fetchone()

    if not row:
        return

    (id_, source_message_id, created_ts, symbol, side, mode,
     entry1_low, entry1_high, entry2_low, entry2_high,
     tps_json, tp_hits, activated, activated_ts, activated_price,
     entry2_activated, entry2_activated_ts, entry2_activated_price,
     reporting_expired, sheet_row, max_profit_20x, tp_hwm_json) = row

    tp_count = len(json.loads(tps_json))
    db_row = {
        "id": id_,
        "source_message_id": source_message_id,
        "created_ts": created_ts,
        "symbol": symbol,
        "side": side,
        "mode": mode,
        "entry1_low": entry1_low,
        "entry1_high": entry1_high,
        "entry2_low": entry2_low,
        "entry2_high": entry2_high,
        "tp_count": tp_count,
        "tps_json": tps_json,
        "tp_hits": tp_hits,
        "activated": activated,
        "activated_ts": activated_ts,
        "activated_price": activated_price,
        "entry2_activated": entry2_activated,
        "entry2_activated_ts": entry2_activated_ts,
        "entry2_activated_price": entry2_activated_price,
        "reporting_expired": reporting_expired,
        "max_profit_20x": max_profit_20x,
        "tp_hwm_json": tp_hwm_json
    }

    out_row = build_signals_row(db_row)

    try:
        if sheet_row and int(sheet_row) > 1:
            await asyncio.to_thread(_update_row, service, GSHEET_SIGNALS_TAB, int(sheet_row), out_row)
        else:
            new_row_num = await asyncio.to_thread(_append_row, service, GSHEET_SIGNALS_TAB, out_row)
            if new_row_num:
                conn.execute("UPDATE signals SET sheet_row=? WHERE id=?", (int(new_row_num), sid))
                conn.commit()
    except Exception as e:
        log(f"GSHEETS upsert signal error sid={sid}: {e}")

async def gsheets_append_profit(service, profit_row: list):
    if not service:
        return
    try:
        await asyncio.to_thread(_append_row, service, GSHEET_PROFITS_TAB, profit_row)
    except Exception as e:
        log(f"GSHEETS append profit error: {e}")

# =========================
# INSTANCE LOCK
# =========================
_LOCK_OWNER_KEY = "instance_lock_owner"
_LOCK_UNTIL_KEY = "instance_lock_until"



async def gsheets_upsert_dash(conn, service, sid: int, sheet_row: int, price_now: float):
    row = conn.execute(
        """SELECT side, tps_json, activated, activated_price,
                  entry2_activated, entry2_activated_price, tp_hits,
                  max_profit_20x, tp_hwm_json
             FROM signals WHERE id=?""",
        (sid,)
    ).fetchone()
    if not row:
        return

    side, tps_json, activated, activated_price, e2_act, e2_price, tp_hits, max_20x, tp_hwm_json = row
    if not activated or not activated_price:
        return

    tps = json.loads(tps_json) if tps_json else []
    base_tp = compute_base_tp_pcts_20x(tps, float(activated_price), side)

    # current combined profit (EP1 + EP2 if active)
    p1, p2, comb_lev = combined_profit_20x(
        price_now,
        side,
        float(activated_price),
        bool(e2_act),
        float(e2_price) if e2_price else 0.0
    )
    if comb_lev is None:
        return

    # MAX_20x high-water mark updates every tick
    try:
        stored_max = float(max_20x or 0)
    except:
        stored_max = 0.0
    if comb_lev > stored_max:
        stored_max = comb_lev
        conn.execute("UPDATE signals SET max_profit_20x=? WHERE id=?", (stored_max, sid))
        conn.commit()

    # TP HWM list (20), not used for overwrite unless you later record improvements on hit
    try:
        hwm = json.loads(tp_hwm_json) if tp_hwm_json else [None] * 20
        if not isinstance(hwm, list):
            hwm = [None] * 20
    except:
        hwm = [None] * 20
    while len(hwm) < 20:
        hwm.append(None)
    if len(hwm) > 20:
        hwm = hwm[:20]

    hits = int(tp_hits or 0)
    tp_vals = []
    for i in range(20):
        base = base_tp[i]
        best = hwm[i]
        val = None
        if base != "" and base is not None:
            val = float(base)
        if isinstance(best, (int, float)):
            if val is None or float(best) > val:
                val = float(best)
        if val is None:
            tp_vals.append("")
        else:
            txt = _format_pct(val)
            if (i + 1) <= hits:
                txt = "✓ " + txt
            tp_vals.append(txt)

    mp_spot = pct_from_entry(price_now, float(activated_price), side) * 100.0
    mp_20x = mp_spot * LEVERAGE

    values = [""] * 23  # V..AR
    values[0] = _format_pct(stored_max)  # V MAX_20x
    values[1] = _format_pct(mp_spot)     # W MP_spot
    values[2] = _format_pct(mp_20x)      # X MP_20x
    values[3:23] = tp_vals               # Y..AR TP1..TP20

    await asyncio.to_thread(_write_dash_row, service, int(sheet_row), values)



def _try_acquire_lock_sync(conn, owner: str, ttl_sec: int) -> bool:
    now = int(time.time())
    until = now + ttl_sec
    try:
        conn.execute("BEGIN IMMEDIATE;")
        cur_owner = conn.execute("SELECT v FROM state WHERE k=?", (_LOCK_OWNER_KEY,)).fetchone()
        cur_until = conn.execute("SELECT v FROM state WHERE k=?", (_LOCK_UNTIL_KEY,)).fetchone()

        cur_owner = cur_owner[0] if cur_owner else ""
        cur_until = int(cur_until[0]) if cur_until and cur_until[0].isdigit() else 0

        if (cur_until <= now) or (cur_owner == owner) or (cur_owner == ""):
            conn.execute(
                "INSERT INTO state(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (_LOCK_OWNER_KEY, owner)
            )
            conn.execute(
                "INSERT INTO state(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (_LOCK_UNTIL_KEY, str(until))
            )
            conn.commit()
            return True

        conn.rollback()
        return False
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        log(f"LOCK acquire error: {e}")
        return False

async def acquire_lock(conn, owner: str) -> bool:
    return await asyncio.to_thread(_try_acquire_lock_sync, conn, owner, LOCK_TTL_SEC)

async def renew_lock(conn, owner: str) -> bool:
    return await asyncio.to_thread(_try_acquire_lock_sync, conn, owner, LOCK_TTL_SEC)

def lock_status_str(conn) -> str:
    try:
        o = state_get(conn, _LOCK_OWNER_KEY, "")
        u = state_get(conn, _LOCK_UNTIL_KEY, "0")
        return f"owner={o} until={u}"
    except Exception:
        return "owner=? until=?"

# =========================
# TELEGRAM SEND
# =========================
async def send_to(bot: Bot, chat_id: int, text: str):
    try:
        await bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=True)
    except TelegramError as e:
        log(f"send_to() TelegramError: {repr(e)}")

async def post_source(bot: Bot, text: str):
    await send_to(bot, SOURCE_CHAT_ID, text)

async def post_target(bot: Bot, text: str):
    await send_to(bot, TARGET_CHAT_ID, text)

# =========================
# PRICE
# =========================
def get_price_sync(symbol: str):
    try:
        r = requests.get(
            PROXY_PRICE_URL,
            params={"symbol": symbol},
            timeout=8,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json,*/*"}
        )
        if r.status_code != 200:
            log(f"get_price({symbol}) worker status={r.status_code} body={r.text[:120]}")
            return None
        data = r.json()
        if isinstance(data, dict) and "price" in data:
            return float(data["price"])
        return None
    except Exception as e:
        log(f"get_price({symbol}) error: {e}")
        return None

async def get_price(symbol: str):
    return await asyncio.to_thread(get_price_sync, symbol)

# =========================
# RAW LONG-POLL
# =========================
def tg_get_updates(offset: int, timeout: int = 20):
    try:
        r = requests.get(
            f"{TG_API}/getUpdates",
            params={
                "offset": offset,
                "timeout": timeout,
                "allowed_updates": json.dumps(["channel_post", "edited_channel_post"])
            },
            timeout=timeout + 5
        )

        if r.status_code == 401:
            log("getUpdates Unauthorized (token mismatch).")
            return "unauthorized", None

        if r.status_code == 409:
            return "conflict", None

        if r.status_code != 200:
            log(f"getUpdates HTTP {r.status_code}: {r.text[:120]}")
            return "error", None

        data = r.json()
        if not data.get("ok"):
            desc = data.get("description", "")
            if "Conflict" in desc:
                return "conflict", None
            return "error", None

        return "ok", data.get("result", [])
    except Exception as e:
        log(f"getUpdates exception: {e}")
        return "error", None

def extract_posts(updates):
    posts = []
    max_update_id = None
    for u in updates:
        uid = u.get("update_id")
        if uid is not None:
            max_update_id = uid if max_update_id is None else max(max_update_id, uid)

        cp = u.get("channel_post") or u.get("edited_channel_post")
        if not cp:
            continue
        chat = cp.get("chat", {})
        if chat.get("id") != SOURCE_CHAT_ID:
            continue
        text = cp.get("text") or cp.get("caption") or ""
        posts.append({"message_id": cp.get("message_id"), "text": text})
    return max_update_id, posts

# =========================
# MONITOR
# =========================
def in_range(price, low, high):
    if price is None or low is None or high is None:
        return False
    return low <= price <= high

def is_reporting_active(now_ts: int, activated_ts):
    if not activated_ts:
        return False
    return now_ts <= activated_ts + REPORTING_ACTIVE_DAYS * 86400

def is_activation_valid(now_ts: int, created_ts: int):
    return now_ts <= created_ts + ACTIVATION_VALID_DAYS * 86400

async def monitor_prices(bot: Bot, conn, stop_event: asyncio.Event, gs_state: dict):
    log("monitor_prices() started")
    service = await gsheets_init_once(gs_state)

    while not stop_event.is_set():
        try:
            rows = conn.execute(
                """SELECT
                    id, symbol, side, mode,
                    entry1_low, entry1_high, entry2_low, entry2_high,
                    tps_json, created_ts,
                    activated, activated_ts, activated_price, tp_hits,
                    entry2_activated, entry2_activated_ts, entry2_activated_price,
                    tp1_rehit_after_entry2_sent,
                    avg_reached_after_entry2_sent,
                    reporting_expired
                   FROM signals"""
            ).fetchall()

            now_ts = int(time.time())

            for (
                sid, symbol, side, mode,
                e1l, e1h, e2l, e2h,
                tps_json, created_ts,
                activated, activated_ts, activated_price, tp_hits,
                e2_activated, e2_activated_ts, e2_activated_price,
                tp1_rehit_sent,
                avg_reached_sent,
                reporting_expired
            ) in rows:

                if stop_event.is_set():
                    break

                if reporting_expired:
                    continue

                price = await get_price(symbol)
                log(f"check sid={sid} {symbol} {side} mode={mode} price={price} activated={activated} tp_hits={tp_hits} e2_activated={e2_activated}")

                if price is None:
                    continue

                tps = json.loads(tps_json)

                # 1) WAIT activation within created_ts window
                if not activated and mode == "WAIT":
                    if not is_activation_valid(now_ts, created_ts):
                        continue

                    in_e1 = in_range(price, e1l, e1h)
                    in_e2 = (not in_e1) and in_range(price, e2l, e2h)
                    if in_e1 or in_e2:
                        conn.execute(
                            "UPDATE signals SET activated=1, activated_ts=?, activated_price=? WHERE id=?",
                            (now_ts, price, sid)
                        )

                        if in_e2 and e2l is not None and e2h is not None:
                            conn.execute(
                                "UPDATE signals SET entry2_activated=1, entry2_activated_ts=?, entry2_activated_price=? WHERE id=?",
                                (now_ts, now_ts, price, sid)
                            )
                            e2_activated = 1
                            e2_activated_price = price
                            e2_activated_ts = now_ts

                        conn.commit()

                        # Sheets update
                        await gsheets_upsert_signal(service, conn, sid)
            # DASH GRID (V..AR): MAX_20x, MP, TP1..TP20
            try:
                sr = conn.execute("SELECT sheet_row FROM signals WHERE id=?", (sid,)).fetchone()
                srn = int(sr[0]) if sr and sr[0] else 0
                if srn > 1:
                    await gsheets_upsert_dash(conn, service, sid, srn, float(price))
            except Exception as e:
                log(f"GSHEETS dash update failed sid={sid} err={e}")

msg = (
    "🆕 Nový signál uložen\n"
    f"{s['symbol']} ({s['side']}) [{s['mode']}]\n"
    f"Entry1: {fmt(s['entry1_low'])} - {fmt(s['entry1_high'])}\n"
    f"{entry2_line}"
    f"TPs (rezistenční úrovně): {len(s['tps'])}"
)
await post_target(bot, msg)


                # After activation: enforce reporting window
                if activated:
                    if not is_reporting_active(now_ts, activated_ts):
                        conn.execute("UPDATE signals SET reporting_expired=1 WHERE id=?", (sid,))
                        conn.commit()
                        await gsheets_upsert_signal(service, conn, sid)
                        continue

                # Entry1 price (existing logic)
                if activated:
                    entry1_price = activated_price if activated_price is not None else price
                    if entry1_price is None or entry1_price == 0:
                        entry1_price = price
                else:
                    entry1_price = None

                # perf from entry1
                perf_from_e1 = 0.0
                if activated and entry1_price:
                    perf_from_e1 = pct_from_entry(price, entry1_price, side)

                # 2) Entry2 activation rules
                if activated and (not e2_activated) and (e2l is not None) and (e2h is not None):
                    entry2_allowed = is_activation_valid(now_ts, created_ts) and (perf_from_e1 < ENTRY2_DISABLE_PROFIT_PCT)
                    if entry2_allowed and in_range(price, e2l, e2h):
                        conn.execute(
                            "UPDATE signals SET entry2_activated=1, entry2_activated_ts=?, entry2_activated_price=? WHERE id=?",
                            (now_ts, now_ts, price, sid)
                        )
                        conn.commit()
                        e2_activated = 1
                        e2_activated_price = price
                        e2_activated_ts = now_ts

                        await gsheets_upsert_signal(service, conn, sid)

                        await post_target(bot,
                            "📌 Entry2 aktivována (čekací zóna)\n"
                            f"{symbol} ({side})\n"
                            f"Entry2 cena: {fmt(price)}\n"
                            f"Entry2 zóna: {fmt(e2l)} - {fmt(e2h)}"
                        )

                # 2.5) AVG reached info
                if activated and e2_activated and (avg_reached_sent == 0):
                    if entry1_price and e2_activated_price and e2_activated_price != 0:
                        avg_price = (float(entry1_price) + float(e2_activated_price)) / 2.0
                        avg_reached_now = (price >= avg_price) if side == "LONG" else (price <= avg_price)
                        if avg_reached_now:
                            await post_target(
                                bot,
                                "ℹ️ Po zprůměrování 1. Entry price a 2. Entry price jsme aktuálně zpátky na zprůměrované ceně těchto pozic.\n"
                                f"{symbol} ({side})\n"
                                f"Entry1: {fmt(entry1_price)}\n"
                                f"Entry2: {fmt(e2_activated_price)}\n"
                                f"Zprůměrovaná cena: {fmt(avg_price)}\n"
                                f"Aktuální cena: {fmt(price)}"
                            )
                            conn.execute("UPDATE signals SET avg_reached_after_entry2_sent=1 WHERE id=?", (sid,))
                            conn.commit()
                            avg_reached_sent = 1

                # 3) TP1 re-hit after Entry2 (profit-only msg, keep)
                if activated and e2_activated and (tp_hits >= 1) and (tp1_rehit_sent == 0) and len(tps) >= 1:
                    tp1 = float(tps[0])
                    tp1_is_hit_now = (price >= tp1) if side == "LONG" else (price <= tp1)
                    if tp1_is_hit_now:
                        entry2_price = e2_activated_price if e2_activated_price else None
                        if entry2_price:
                            g2_spot = pct_from_entry(tp1, entry2_price, side)
                            g2_lev = g2_spot * LEVERAGE
                            await post_target(bot,
                                f"🎯 {symbol} – TP1 HIT (po aktivaci 2. Entry)\n"
                                f"Směr: {side}\n"
                                f"Entry2: {fmt(entry2_price)}\n"
                                f"TP1: {fmt(tp1)}\n"
                                f"Zisk: {g2_spot:.2f}% čistého trhu ({g2_lev:.2f}% s pákou {LEVERAGE:g}x) z 2. Entry"
                            )
                        conn.execute("UPDATE signals SET tp1_rehit_after_entry2_sent=1 WHERE id=?", (sid,))
                        conn.commit()
                        tp1_rehit_sent = 1

                # 4) Normal TP hits + Sheets append to Profits
                if activated:
                    entry2_price = e2_activated_price if e2_activated else None

                    while tp_hits < len(tps):
                        tp = float(tps[tp_hits])
                        is_hit = (price >= tp) if side == "LONG" else (price <= tp)
                        if not is_hit:
                            break

                        tp_hits += 1
                        conn.execute("UPDATE signals SET tp_hits=? WHERE id=?", (tp_hits, sid))
                        conn.commit()

                        g1_spot = pct_from_entry(tp, entry1_price, side)
                        g1_lev = g1_spot * LEVERAGE

                        note = ""
                        if entry2_price is not None and entry2_price != 0:
                            g2_spot = pct_from_entry(tp, entry2_price, side)
                            g2_lev = g2_spot * LEVERAGE
                            profit_line = (
                                f"Zisk: {g1_spot:.2f}% ({g1_lev:.2f}% s pákou {LEVERAGE:g}x) z 1. Entry\n"
                                f"      {g2_spot:.2f}% ({g2_lev:.2f}% s pákou {LEVERAGE:g}x) z 2. Entry"
                            )
                        else:
                            g2_spot = ""
                            g2_lev = ""
                            entry2_price = ""
                            profit_line = f"Zisk: {g1_spot:.2f}% čistého trhu ({g1_lev:.2f}% s pákou {LEVERAGE:g}x)"

                        await post_target(bot,
                            f"🎯 {symbol} – TP{tp_hits} HIT\n"
                            f"Směr: {side}\n"
                            f"Entry1: {fmt(entry1_price)}\n"
                            f"{'Entry2: ' + fmt(e2_activated_price) if e2_activated else 'Entry2: -'}\n"
                            f"TP{tp_hits}: {fmt(tp)}\n"
                            f"{profit_line}"
                        )

                        # Sheets: update signal + append profit event
                        await gsheets_upsert_signal(service, conn, sid)

                        profit_row = [
                            int(time.time()), sid, symbol, side,
                            tp_hits, tp,
                            entry1_price, round(g1_spot, 6), round(g1_lev, 6),
                            (e2_activated_price if e2_activated else ""), (round(pct_from_entry(tp, e2_activated_price, side), 6) if e2_activated else ""),
                            (round(pct_from_entry(tp, e2_activated_price, side) * LEVERAGE, 6) if e2_activated else ""),
                            note
                        ]
                        await gsheets_append_profit(service, profit_row)

        except Exception as e:
            log(f"monitor_prices loop error: {e}")

        await asyncio.sleep(CHECK_INTERVAL_SEC)

async def lock_renew_loop(conn, stop_event: asyncio.Event):
    while not stop_event.is_set():
        ok = await renew_lock(conn, INSTANCE_ID)
        if not ok:
            log(f"LOCK LOST -> stopping loops. Current lock: {lock_status_str(conn)}")
            stop_event.set()
            return
        await asyncio.sleep(max(5, LOCK_RENEW_EVERY_SEC))

# =========================
# MAIN
# =========================
async def main_async():
    log("START: main_async entered")
    log(f"ENV: SOURCE={SOURCE_CHAT_ID} TARGET={TARGET_CHAT_ID} CHECK={CHECK_INTERVAL_SEC} POLL={POLL_INTERVAL_SEC} ENTRY_REF_MODE={ENTRY_REF_MODE} DB={DB_PATH} LEVERAGE={LEVERAGE:g} INSTANCE_ID={INSTANCE_ID}")
    log(f"RULES: activation_valid_days={ACTIVATION_VALID_DAYS} reporting_active_days={REPORTING_ACTIVE_DAYS} entry2_disable_profit_pct={ENTRY2_DISABLE_PROFIT_PCT:g}")
    log(f"LOCK: ttl={LOCK_TTL_SEC}s renew_every={LOCK_RENEW_EVERY_SEC}s")

    if GSHEETS_ENABLED:
        log(f"GSHEETS: env detected (id={GSHEET_ID} tabs={GSHEET_SIGNALS_TAB},{GSHEET_PROFITS_TAB})")
    else:
        log("GSHEETS: disabled (missing env vars)")

    bot = Bot(token=BOT_TOKEN)
    conn = connect_db()

    gs_state = {}  # holds google sheets service

    # Leader election loop
    while True:
        got = await acquire_lock(conn, INSTANCE_ID)
        if not got:
            log(f"Not leader. Waiting... lock={lock_status_str(conn)}")
            await asyncio.sleep(10)
            continue

        log(f"LOCK ACQUIRED -> I am leader. lock={lock_status_str(conn)}")

        stop_event = asyncio.Event()
        renew_task = asyncio.create_task(lock_renew_loop(conn, stop_event))

        # leader ping 1x/24h
        try:
            now = int(time.time())
            last_ping = int(state_get(conn, "startup_ping_ts", "0"))
            if now - last_ping > 24 * 3600:
                await post_target(bot, "🤖 Bot běží.")
                state_set(conn, "startup_ping_ts", str(now))
        except Exception as e:
            log(f"startup ping error: {e}")

        offset = int(state_get(conn, "raw_offset", "0"))
        monitor_task = asyncio.create_task(monitor_prices(bot, conn, stop_event, gs_state))

        try:
            while not stop_event.is_set():
                status, updates = await asyncio.to_thread(tg_get_updates, offset + 1, 20)

                if status == "conflict":
                    log("getUpdates Conflict (unexpected with lock) -> sleeping 10s")
                    await asyncio.sleep(10)
                    continue

                if status == "unauthorized":
                    log("FATAL: Unauthorized token. Fix BOT_TOKEN in env.")
                    await asyncio.sleep(10)
                    continue

                if status != "ok":
                    await asyncio.sleep(2)
                    continue

                if not updates:
                    await asyncio.sleep(POLL_INTERVAL_SEC)
                    continue

                max_uid, posts = extract_posts(updates)
                if max_uid is not None:
                    offset = max_uid
                    state_set(conn, "raw_offset", str(offset))

                for p in posts:
                    if stop_event.is_set():
                        break

                    if not p["message_id"]:
                        continue

                    s = parse_signal(p["text"])
                    if not s:
                        continue

                    sid = save_signal(conn, p["message_id"], s)
                    if not sid:
                        continue

                    entry2_line = ""
                    if s["entry2_low"] is not None and s["entry2_high"] is not None:
                        entry2_line = f"Entry2: {fmt(s['entry2_low'])} - {fmt(s['entry2_high'])}\n"

                    await post_source(bot,
                        "🆕 Nový signál uložen\n"
                        f"{s['symbol']} ({s['side']}) [{s['mode']}]\n"
                        f"Entry1: {fmt(s['entry1_low'])} - {fmt(s['entry1_high'])}\n"
                        f"{entry2_line}"
                        f"TPs (rezistenční úrovně): {len(s['tps'])}"
                    )

                    # Sheets: upsert NEW
                    service = await gsheets_init_once(gs_state)
                    await gsheets_upsert_signal(service, conn, sid)

                    log(f"saved signal msg_id={p['message_id']} sid={sid} {s['symbol']} {s['side']} mode={s['mode']}")

                    # MARKET => activate immediately
                    if s["mode"] == "MARKET":
                        price_now = await get_price(s["symbol"])
                        if price_now is None:
                            log(f"MARKET activate: price None for {s['symbol']} (skip)")
                        else:
                            now_ts = int(time.time())
                            conn.execute(
                                "UPDATE signals SET activated=1, activated_ts=?, activated_price=? WHERE id=?",
                                (now_ts, now_ts, price_now, sid)
                            )
                            conn.commit()

                            await gsheets_upsert_signal(service, conn, sid)

                            await post_target(bot,
                                "✅ Signál aktivován (MARKET)\n"
                                f"{s['symbol']} ({s['side']})\n"
                                f"Vstup (Entry1): {fmt(price_now)}\n"
                                f"{('Entry2: ' + fmt(s['entry2_low']) + ' - ' + fmt(s['entry2_high'])) if (s['entry2_low'] is not None and s['entry2_high'] is not None) else 'Entry2: -'}\n"
                                f"TPs: {len(s['tps'])}"
                            )
                            log(f"MARKET activated sid={sid} {s['symbol']} price={price_now}")

                await asyncio.sleep(POLL_INTERVAL_SEC)

        finally:
            stop_event.set()
            for t in (monitor_task, renew_task):
                try:
                    t.cancel()
                except Exception:
                    pass
            for t in (monitor_task, renew_task):
                try:
                    await t
                except Exception:
                    pass
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(main_async())



