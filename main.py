import os, re, time, json, sqlite3, asyncio, socket
import requests
from telegram import Bot
from telegram.error import TelegramError

# =========================
# CONFIG
# =========================
def log(msg: str):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)

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
ACTIVATION_VALID_DAYS = int(os.getenv("ACTIVATION_VALID_DAYS", "30"))   # valid for 1EP/2EP activation from created_ts
REPORTING_ACTIVE_DAYS = int(os.getenv("REPORTING_ACTIVE_DAYS", "60"))   # report max N days from activated_ts
ENTRY2_DISABLE_PROFIT_PCT = float(os.getenv("ENTRY2_DISABLE_PROFIT_PCT", "18"))  # disable entry2 if perf >= N%

# INSTANCE LOCK (prevents Telegram getUpdates Conflict + duplicate reporting)
LOCK_TTL_SEC = int(os.getenv("LOCK_TTL_SEC", "90"))              # lease time
LOCK_RENEW_EVERY_SEC = int(os.getenv("LOCK_RENEW_EVERY_SEC", "30"))  # renew cadence
INSTANCE_ID = os.getenv("INSTANCE_ID", "").strip()
if not INSTANCE_ID:
    INSTANCE_ID = f"{socket.gethostname()}-pid{os.getpid()}"

# Telegram raw API
TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

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

    # LONG: asc, SHORT: desc
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
            reporting_expired INTEGER NOT NULL DEFAULT 0
        )
    """)

    # Safe ALTER for existing DBs
    for sql in [
        "ALTER TABLE signals ADD COLUMN entry2_activated INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE signals ADD COLUMN entry2_activated_ts INTEGER",
        "ALTER TABLE signals ADD COLUMN entry2_activated_price REAL",
        "ALTER TABLE signals ADD COLUMN tp1_rehit_after_entry2_sent INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE signals ADD COLUMN reporting_expired INTEGER NOT NULL DEFAULT 0",
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
# INSTANCE LOCK (SQLite lease)
# =========================
_LOCK_OWNER_KEY = "instance_lock_owner"
_LOCK_UNTIL_KEY = "instance_lock_until"

def _try_acquire_lock_sync(conn, owner: str, ttl_sec: int) -> bool:
    """
    Acquire or renew a lease-style lock in the `state` table.
    Single writer wins via BEGIN IMMEDIATE.
    """
    now = int(time.time())
    until = now + ttl_sec

    try:
        conn.execute("BEGIN IMMEDIATE;")
        cur_owner = conn.execute("SELECT v FROM state WHERE k=?", (_LOCK_OWNER_KEY,)).fetchone()
        cur_until = conn.execute("SELECT v FROM state WHERE k=?", (_LOCK_UNTIL_KEY,)).fetchone()

        cur_owner = cur_owner[0] if cur_owner else ""
        cur_until = int(cur_until[0]) if cur_until and cur_until[0].isdigit() else 0

        # Lock free/expired OR we already own it
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
    # renewal is the same operation (we keep ownership)
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

def is_reporting_active(now_ts: int, activated_ts: int | None):
    if not activated_ts:
        return False
    return now_ts <= activated_ts + REPORTING_ACTIVE_DAYS * 86400

def is_activation_valid(now_ts: int, created_ts: int):
    return now_ts <= created_ts + ACTIVATION_VALID_DAYS * 86400

async def monitor_prices(bot: Bot, conn, stop_event: asyncio.Event):
    log("monitor_prices() started")
    while not stop_event.is_set():
        try:
            rows = conn.execute(
                """SELECT
                    id, symbol, side, mode,
                    entry1_low, entry1_high, entry2_low, entry2_high,
                    tps_json, created_ts,
                    activated, activated_ts, activated_price, tp_hits,
                    entry2_activated, entry2_activated_price,
                    tp1_rehit_after_entry2_sent, reporting_expired
                   FROM signals"""
            ).fetchall()

            log(f"monitor tick: signals={len(rows)}" if rows else "monitor tick: no signals")

            now_ts = int(time.time())

            for (
                sid, symbol, side, mode,
                e1l, e1h, e2l, e2h,
                tps_json, created_ts,
                activated, activated_ts, activated_price, tp_hits,
                e2_activated, e2_activated_price,
                tp1_rehit_sent, reporting_expired
            ) in rows:

                if stop_event.is_set():
                    break

                # If already marked expired, skip
                if reporting_expired:
                    continue

                price = await get_price(symbol)
                log(f"check sid={sid} {symbol} {side} mode={mode} price={price} entry1={e1l}-{e1h} entry2={e2l}-{e2h} activated={activated} tp_hits={tp_hits} e2_activated={e2_activated}")

                if price is None:
                    continue

                tps = json.loads(tps_json)

                # 1) Activation validity (created_ts + 30d) affects activation for WAIT
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
                        # If activated directly in entry2 zone, mark entry2 as active too (still within 30d)
                        if in_e2 and e2l is not None and e2h is not None:
                            conn.execute(
                                "UPDATE signals SET entry2_activated=1, entry2_activated_ts=?, entry2_activated_price=? WHERE id=?",
                                (now_ts, price, sid)
                            )
                            e2_activated = 1
                            e2_activated_price = price

                        conn.commit()
                        await post_target(bot,
                            "✅ Signál aktivován\n"
                            f"{symbol} ({side})\n"
                            f"Vstup (Entry1): {fmt(price)}\n"
                            f"Entry1: {fmt(e1l)} - {fmt(e1h)}"
                        )
                    continue

                # After activation: enforce reporting window (activated_ts + 60d)
                if activated:
                    if not is_reporting_active(now_ts, activated_ts):
                        conn.execute("UPDATE signals SET reporting_expired=1 WHERE id=?", (sid,))
                        conn.commit()
                        continue

                # Determine entry1_price for performance/TP calculations
                if activated:
                    entry1_price = activated_price if activated_price is not None else price
                    if entry1_price is None or entry1_price == 0:
                        entry1_price = price
                else:
                    entry1_price = None

                # Compute current performance from entry1 (used to disable entry2 after +18%)
                perf_from_e1 = 0.0
                if activated and entry1_price:
                    perf_from_e1 = pct_from_entry(price, entry1_price, side)

                # 2) Entry2 activation rules: only if within 30d from created_ts and perf < 18%
                if activated and (not e2_activated) and (e2l is not None) and (e2h is not None):
                    entry2_allowed = is_activation_valid(now_ts, created_ts) and (perf_from_e1 < ENTRY2_DISABLE_PROFIT_PCT)
                    if entry2_allowed and in_range(price, e2l, e2h):
                        conn.execute(
                            "UPDATE signals SET entry2_activated=1, entry2_activated_ts=?, entry2_activated_price=? WHERE id=?",
                            (now_ts, price, sid)
                        )
                        conn.commit()
                        e2_activated = 1
                        e2_activated_price = price
                        await post_target(bot,
                            "📌 Entry2 aktivována (čekací zóna)\n"
                            f"{symbol} ({side})\n"
                            f"Entry2 cena: {fmt(price)}\n"
                            f"Entry2 zóna: {fmt(e2l)} - {fmt(e2h)}"
                        )

                # 3) TP1 re-hit after Entry2 activation (ONLY ONCE)
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
                        conn.execute(
                            "UPDATE signals SET tp1_rehit_after_entry2_sent=1 WHERE id=?",
                            (sid,)
                        )
                        conn.commit()
                        tp1_rehit_sent = 1

                # 4) Normal TP hits
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

                        if entry2_price is not None and entry2_price != 0:
                            g2_spot = pct_from_entry(tp, entry2_price, side)
                            g2_lev = g2_spot * LEVERAGE
                            profit_line = (
                                f"Zisk: {g1_spot:.2f}% ({g1_lev:.2f}% s pákou {LEVERAGE:g}x) z 1. Entry\n"
                                f"      {g2_spot:.2f}% ({g2_lev:.2f}% s pákou {LEVERAGE:g}x) z 2. Entry"
                            )
                        else:
                            profit_line = f"Zisk: {g1_spot:.2f}% čistého trhu ({g1_lev:.2f}% s pákou {LEVERAGE:g}x)"

                        await post_target(bot,
                            f"🎯 {symbol} – TP{tp_hits} HIT\n"
                            f"Směr: {side}\n"
                            f"Entry1: {fmt(entry1_price)}\n"
                            f"{'Entry2: ' + fmt(entry2_price) if entry2_price is not None else 'Entry2: -'}\n"
                            f"TP{tp_hits}: {fmt(tp)}\n"
                            f"{profit_line}"
                        )

        except Exception as e:
            log(f"monitor_prices loop error: {e}")

        await asyncio.sleep(CHECK_INTERVAL_SEC)

async def lock_renew_loop(conn, stop_event: asyncio.Event):
    """
    Keeps the lease alive. If renewal fails, we stop the bot loops so another instance can take over.
    """
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

    bot = Bot(token=BOT_TOKEN)
    conn = connect_db()

    # Leader election loop
    while True:
        got = await acquire_lock(conn, INSTANCE_ID)
        if not got:
            # Not leader: do nothing (prevents Conflict and duplicates)
            log(f"Not leader. Waiting... lock={lock_status_str(conn)}")
            await asyncio.sleep(10)
            continue

        log(f"LOCK ACQUIRED -> I am leader. lock={lock_status_str(conn)}")

        stop_event = asyncio.Event()
        renew_task = asyncio.create_task(lock_renew_loop(conn, stop_event))

        # Anti-spam ping 1x/24h (only leader should do this)
        try:
            now = int(time.time())
            last_ping = int(state_get(conn, "startup_ping_ts", "0"))
            if now - last_ping > 24 * 3600:
                await post_target(bot, "🤖 Bot běží.")
                state_set(conn, "startup_ping_ts", str(now))
        except Exception as e:
            log(f"startup ping error: {e}")

        offset = int(state_get(conn, "raw_offset", "0"))
        monitor_task = asyncio.create_task(monitor_prices(bot, conn, stop_event))

        try:
            while not stop_event.is_set():
                status, updates = await asyncio.to_thread(tg_get_updates, offset + 1, 20)

                # With lock, Conflict should not happen; if it does, we just wait a bit.
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

                    log(f"saved signal msg_id={p['message_id']} sid={sid} {s['symbol']} {s['side']} mode={s['mode']} entry1={s['entry1_low']}-{s['entry1_high']} entry2={s['entry2_low']}-{s['entry2_high']} tps={len(s['tps'])}")

                    # MARKET => activate immediately (Entry1 = activation price)
                    if s["mode"] == "MARKET":
                        price_now = await get_price(s["symbol"])
                        if price_now is None:
                            log(f"MARKET activate: price None for {s['symbol']} (skip)")
                        else:
                            now_ts = int(time.time())
                            conn.execute(
                                "UPDATE signals SET activated=1, activated_ts=?, activated_price=? WHERE id=?",
                                (now_ts, price_now, sid)
                            )
                            conn.commit()
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

            # If we lost lock, loop will attempt to re-acquire; otherwise keep running.
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(main_async())
