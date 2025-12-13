import os, re, time, json, sqlite3, asyncio
import requests
from telegram import Bot
from telegram.error import TelegramError

def log(msg):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)

BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
SOURCE_CHAT_ID = int(os.environ["SOURCE_CHAT_ID"].strip())
TARGET_CHAT_ID = int(os.environ["TARGET_CHAT_ID"].strip())

PROXY_PRICE_URL = "https://workerrr.developctsro.workers.dev"

CHECK_INTERVAL_SEC = int(os.getenv("CHECK_INTERVAL_SEC", "15"))
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "3"))
ENTRY_REF_MODE = os.getenv("ENTRY_REF_MODE", "HIGH").upper()
DB_PATH = os.getenv("DB_PATH", "bot.db")

# ✅ Parsuje:
# - BTC/USDT Long
# - BTC/USDT Short
# - BTC/USDT Buy on ...
# - BTC/USDT Short on ...
# - (volitelně) Sell on ...
# toleruje i "on spot" apod.
PAIR_RE = re.compile(
    r"^\s*([A-Z0-9]+)\s*/\s*(USDT)\s*(LONG|SHORT|BUY|SELL)\b(?:\s+(ON)\b)?",
    re.IGNORECASE | re.MULTILINE
)

ENTRY1_RE = re.compile(r"1\.\s*Entry price:\s*([0-9.]+)\s*(?:-\s*([0-9.]+))?", re.IGNORECASE)
ENTRY2_RE = re.compile(r"2\.\s*Entry price:\s*([0-9.]+)\s*(?:-\s*([0-9.]+))?", re.IGNORECASE)
RES_RE = re.compile(r"Rezistenční úrovně:\s*(.+?)(?:\n\n|\nStop Loss:|\Z)", re.IGNORECASE | re.DOTALL)

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
            entry1_low REAL NOT NULL,
            entry1_high REAL NOT NULL,
            entry2_low REAL,
            entry2_high REAL,
            tps_json TEXT NOT NULL,
            created_ts INTEGER NOT NULL,
            activated INTEGER NOT NULL DEFAULT 0,
            activated_ts INTEGER,
            activated_price REAL,
            tp_hits INTEGER NOT NULL DEFAULT 0
        )
    """)
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

def parse_range(a, b):
    x = float(a)
    y = float(b) if b else float(a)
    return (x, y) if x <= y else (y, x)

def parse_signal(text: str):
    m = PAIR_RE.search(text or "")
    if not m:
        return None

    base = m.group(1).upper()
    quote = m.group(2).upper()
    action = (m.group(3) or "").lower()       # long/short/buy/sell
    has_on = (m.group(4) or "").lower() == "on"  # True/False

    # Side map
    side = "LONG" if action in ("long", "buy") else "SHORT"
    symbol = f"{base}{quote}"

    # ✅ PRAVIDLO OD TEBE:
    # - Long/Short bez "on" => market NOW (aktivace ihned)
    # - Buy on / Short on (a obecně cokoliv s "on") => čekací (trigger přes Entry)
    auto_activate = (action in ("long", "short")) and (not has_on)

    # Entry1 pořád vyžadujeme (kvůli DB / pozdějším TP výpočtům)
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
        "entry1_low": entry1_low,
        "entry1_high": entry1_high,
        "entry2_low": entry2_low,
        "entry2_high": entry2_high,
        "tps": tps,
        "auto_activate": auto_activate
    }

def entry_ref(entry1_low, entry1_high):
    return entry1_low if ENTRY_REF_MODE == "LOW" else entry1_high

def pct_from_entry(price, entry, side):
    return (price - entry) / entry * 100.0 if side == "LONG" else (entry - price) / entry * 100.0

def fmt(x):
    return f"{x:.8f}".rstrip("0").rstrip(".")

async def post_target(bot: Bot, text: str):
    try:
        await bot.send_message(chat_id=TARGET_CHAT_ID, text=text, disable_web_page_preview=True)
    except TelegramError as e:
        log(f"post_target() TelegramError: {repr(e)}")

async def post_source(bot: Bot, text: str):
    try:
        await bot.send_message(chat_id=SOURCE_CHAT_ID, text=text, disable_web_page_preview=True)
    except TelegramError as e:
        log(f"post_source() TelegramError: {repr(e)}")

def get_price_sync(symbol: str):
    try:
        r = requests.get(
            PROXY_PRICE_URL,
            params={"symbol": symbol},
            timeout=8,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"}
        )

        if r.status_code != 200:
            log(f"get_price({symbol}) worker status={r.status_code} body={r.text[:300]}")
            return None

        data = r.json()
        if isinstance(data, dict) and ("price" in data):
            try:
                return float(data["price"])
            except Exception:
                log(f"get_price({symbol}) invalid price={data.get('price')} raw={data}")
                return None

        log(f"get_price({symbol}) invalid response json={data}")
        return None

    except Exception as e:
        log(f"get_price({symbol}) error: {e}")
        return None

async def get_price(symbol: str):
    return await asyncio.to_thread(get_price_sync, symbol)

def save_signal(conn, source_message_id, s):
    try:
        cur = conn.execute(
            """INSERT INTO signals(
                source_message_id, symbol, side, entry1_low, entry1_high, entry2_low, entry2_high, tps_json, created_ts
            ) VALUES(?,?,?,?,?,?,?,?,?)""",
            (
                source_message_id,
                s["symbol"],
                s["side"],
                s["entry1_low"],
                s["entry1_high"],
                s["entry2_low"],
                s["entry2_high"],
                json.dumps(s["tps"]),
                int(time.time())
            )
        )
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError:
        return None

async def fetch_channel_posts(bot: Bot, last_update_id: int):
    try:
        updates = await bot.get_updates(
            offset=last_update_id + 1,
            timeout=20,
            allowed_updates=["channel_post", "edited_channel_post"]
        )
    except TelegramError as e:
        log(f"get_updates TelegramError: {repr(e)}")
        return last_update_id, []

    if not updates:
        return last_update_id, []

    max_id = last_update_id
    posts = []
    for u in updates:
        max_id = max(max_id, u.update_id)
        cp = u.channel_post or u.edited_channel_post
        if not cp:
            continue
        if not cp.chat or cp.chat.id != SOURCE_CHAT_ID:
            continue
        text = (cp.text or cp.caption or "")
        posts.append({"message_id": cp.message_id, "text": text})
    return max_id, posts

async def monitor_prices(bot: Bot, conn):
    log("monitor_prices() started")
    while True:
        try:
            rows = conn.execute(
                "SELECT id, symbol, side, entry1_low, entry1_high, entry2_low, entry2_high, tps_json, activated, tp_hits FROM signals"
            ).fetchall()

            log(f"monitor tick: signals={len(rows)}" if rows else "monitor tick: no signals")

            for sid, symbol, side, e1l, e1h, e2l, e2h, tps_json, activated, tp_hits in rows:
                price = await get_price(symbol)
                log(f"check sid={sid} {symbol} {side} price={price} entry1={e1l}-{e1h} entry2={e2l}-{e2h} activated={activated} tp_hits={tp_hits}")

                if price is None:
                    continue

                tps = json.loads(tps_json)
                e_ref = entry_ref(e1l, e1h)

                # ✅ ČEKACÍ SIGNÁLY: aktivace jen při vstupu do Entry range
                if not activated:
                    is_activated = False

                    if e1l <= price <= e1h:
                        is_activated = True

                    if (not is_activated) and (e2l is not None) and (e2h is not None) and (e2l <= price <= e2h):
                        is_activated = True

                    if is_activated:
                        log(f"ACTIVATE sid={sid} {symbol} price={price} in range.")
                        conn.execute(
                            "UPDATE signals SET activated=1, activated_ts=?, activated_price=? WHERE id=?",
                            (int(time.time()), price, sid)
                        )
                        conn.commit()
                        await post_target(bot,
                            "✅ Signál aktivován\n"
                            f"{symbol} ({side})\n"
                            f"Aktuální cena: {fmt(price)}\n"
                            f"Entry1: {fmt(e1l)} - {fmt(e1h)}"
                        )
                    continue

                # TP hits (po aktivaci)
                while tp_hits < len(tps):
                    tp = float(tps[tp_hits])
                    is_hit = (price >= tp) if side == "LONG" else (price <= tp)
                    if not is_hit:
                        break
                    tp_hits += 1
                    gain = pct_from_entry(tp, e_ref, side)
                    conn.execute("UPDATE signals SET tp_hits=? WHERE id=?", (tp_hits, sid))
                    conn.commit()
                    await post_target(bot,
                        f"🎯 {symbol} – TP{tp_hits} HIT\n"
                        f"TP cena: {fmt(tp)}\n"
                        f"Zisk: {gain:.2f}% (od Entry1 {ENTRY_REF_MODE})"
                    )

        except Exception as e:
            log(f"monitor_prices loop error: {e}")

        await asyncio.sleep(CHECK_INTERVAL_SEC)

async def main_async():
    log("START: main_async entered")
    log(f"ENV: SOURCE={SOURCE_CHAT_ID} TARGET={TARGET_CHAT_ID} CHECK={CHECK_INTERVAL_SEC} POLL={POLL_INTERVAL_SEC} ENTRY_REF_MODE={ENTRY_REF_MODE} DB={DB_PATH}")

    bot = Bot(token=BOT_TOKEN)
    conn = connect_db()

    # Startup ping max 1x/24h (anti spam)
    now = int(time.time())
    last_ping = int(state_get(conn, "startup_ping_ts", "0"))
    if now - last_ping > 24 * 3600:
        await post_target(bot, "🤖 Bot běží. Cena z Worker Proxy.")
        state_set(conn, "startup_ping_ts", str(now))

    last_update_id = int(state_get(conn, "last_update_id", "0"))

    monitor_task = asyncio.create_task(monitor_prices(bot, conn))

    try:
        while True:
            last_update_id, posts = await fetch_channel_posts(bot, last_update_id)
            state_set(conn, "last_update_id", str(last_update_id))

            for p in posts:
                s = parse_signal(p["text"])
                if not s:
                    continue

                sid = save_signal(conn, p["message_id"], s)
                if not sid:
                    continue

                # ✅ Nový signál -> SOURCE
                await post_source(bot,
                    "🆕 Nový signál uložen\n"
                    f"{s['symbol']} ({s['side']})\n"
                    f"Entry1: {fmt(s['entry1_low'])} - {fmt(s['entry1_high'])}\n"
                    f"Entry2: {fmt(s['entry2_low'])} - {fmt(s['entry2_high'])}\n"
                    f"TPs: {len(s['tps'])}"
                )

                log(f"saved signal msg_id={p['message_id']} {s['symbol']} {s['side']} entry1={s['entry1_low']}-{s['entry1_high']} entry2={s['entry2_low']}-{s['entry2_high']} tps={len(s['tps'])} auto_activate={s['auto_activate']}")

                # ✅ MARKET SIGNÁLY (Long/Short) -> aktivace ihned, NEřeší Entry
                if s.get("auto_activate"):
                    price_now = await get_price(s["symbol"])
                    if price_now is None:
                        log(f"auto_activate: price None for {s['symbol']} -> skipping immediate activation")
                    else:
                        conn.execute(
                            "UPDATE signals SET activated=1, activated_ts=?, activated_price=? WHERE id=?",
                            (int(time.time()), price_now, sid)
                        )
                        conn.commit()
                        await post_target(bot,
                            "✅ Signál aktivován (market)\n"
                            f"{s['symbol']} ({s['side']})\n"
                            f"Aktuální cena: {fmt(price_now)}"
                        )
                        log(f"auto_activate DONE sid={sid} {s['symbol']} price={price_now}")

            await asyncio.sleep(POLL_INTERVAL_SEC)

    finally:
        monitor_task.cancel()
        try:
            await monitor_task
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    asyncio.run(main_async())
