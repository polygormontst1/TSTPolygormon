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

# ===== REGEX =====
PAIR_RE = re.compile(
    r"^\s*([A-Z0-9]+)\s*/\s*(USDT)\s*(LONG|SHORT|BUY|SELL)\b(?:\s+(ON)\b)?",
    re.IGNORECASE | re.MULTILINE
)
ENTRY1_RE = re.compile(r"1\.\s*Entry price:\s*([0-9.]+)\s*(?:-\s*([0-9.]+))?", re.IGNORECASE)
ENTRY2_RE = re.compile(r"2\.\s*Entry price:\s*([0-9.]+)\s*(?:-\s*([0-9.]+))?", re.IGNORECASE)
RES_RE = re.compile(r"Rezistenční úrovně:\s*(.+?)(?:\n\n|\nStop Loss:|\Z)", re.IGNORECASE | re.DOTALL)

# ===== DB =====
def connect_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_message_id INTEGER UNIQUE,
            symbol TEXT,
            side TEXT,
            entry1_low REAL,
            entry1_high REAL,
            entry2_low REAL,
            entry2_high REAL,
            tps_json TEXT,
            created_ts INTEGER,
            activated INTEGER DEFAULT 0,
            activated_ts INTEGER,
            activated_price REAL,
            tp_hits INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS state (
            k TEXT PRIMARY KEY,
            v TEXT
        )
    """)
    conn.commit()
    return conn

def state_get(conn, key, default):
    r = conn.execute("SELECT v FROM state WHERE k=?", (key,)).fetchone()
    return r[0] if r else default

def state_set(conn, key, val):
    conn.execute(
        "INSERT INTO state(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (key, val)
    )
    conn.commit()

# ===== HELPERS =====
def parse_range(a, b):
    x = float(a)
    y = float(b) if b else float(a)
    return (x, y) if x <= y else (y, x)

def fmt(x):
    if x is None:
        return "-"
    return f"{x:.8f}".rstrip("0").rstrip(".")

def entry_ref(l, h):
    return l if ENTRY_REF_MODE == "LOW" else h

def pct_from_entry(price, entry, side):
    return (price - entry) / entry * 100 if side == "LONG" else (entry - price) / entry * 100

# ===== PARSER =====
def parse_signal(text):
    m = PAIR_RE.search(text or "")
    if not m:
        return None

    base, quote, action, on_word = m.groups()
    action = action.lower()
    has_on = bool(on_word)

    side = "LONG" if action in ("long", "buy") else "SHORT"
    symbol = f"{base.upper()}{quote.upper()}"

    auto_activate = action in ("long", "short") and not has_on

    e1 = ENTRY1_RE.search(text)
    if not e1:
        return None
    entry1_low, entry1_high = parse_range(e1.group(1), e1.group(2))

    e2 = ENTRY2_RE.search(text)
    entry2_low = entry2_high = None
    if e2:
        entry2_low, entry2_high = parse_range(e2.group(1), e2.group(2))

    rm = RES_RE.search(text)
    if not rm:
        return None
    nums = re.findall(r"([0-9]*\.?[0-9]+)", rm.group(1))
    tps = sorted([float(n) for n in nums], reverse=(side == "SHORT"))

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

# ===== PRICE =====
def get_price_sync(symbol):
    try:
        r = requests.get(PROXY_PRICE_URL, params={"symbol": symbol}, timeout=8)
        if r.status_code != 200:
            log(f"get_price({symbol}) status={r.status_code}")
            return None
        d = r.json()
        return float(d["price"]) if "price" in d else None
    except Exception as e:
        log(f"get_price({symbol}) error {e}")
        return None

async def get_price(symbol):
    return await asyncio.to_thread(get_price_sync, symbol)

# ===== TELEGRAM =====
async def post(chat_id, bot, text):
    try:
        await bot.send_message(chat_id=chat_id, text=text)
    except TelegramError as e:
        log(f"TelegramError {e}")

# ===== MONITOR =====
async def monitor_prices(bot, conn):
    log("monitor_prices() started")
    while True:
        rows = conn.execute(
            "SELECT id,symbol,side,entry1_low,entry1_high,entry2_low,entry2_high,tps_json,activated,tp_hits FROM signals"
        ).fetchall()

        log(f"monitor tick: signals={len(rows)}" if rows else "monitor tick: no signals")

        for r in rows:
            sid,symbol,side,e1l,e1h,e2l,e2h,tps_json,activated,tp_hits = r
            price = await get_price(symbol)
            log(f"check sid={sid} {symbol} price={price}")

            if price is None:
                continue

            tps = json.loads(tps_json)

            if not activated:
                if (e1l <= price <= e1h) or (e2l and e2l <= price <= e2h):
                    conn.execute(
                        "UPDATE signals SET activated=1,activated_ts=?,activated_price=? WHERE id=?",
                        (int(time.time()), price, sid)
                    )
                    conn.commit()
                    await post(TARGET_CHAT_ID, bot,
                        f"✅ Signál aktivován\n{symbol} ({side})\nCena: {fmt(price)}"
                    )
                continue

            while tp_hits < len(tps):
                tp = tps[tp_hits]
                hit = price >= tp if side == "LONG" else price <= tp
                if not hit:
                    break
                tp_hits += 1
                conn.execute("UPDATE signals SET tp_hits=? WHERE id=?", (tp_hits, sid))
                conn.commit()
                gain = pct_from_entry(tp, entry_ref(e1l,e1h), side)
                await post(TARGET_CHAT_ID, bot,
                    f"🎯 {symbol} TP{tp_hits} HIT\nCena: {fmt(tp)}\nZisk: {gain:.2f}%"
                )

        await asyncio.sleep(CHECK_INTERVAL_SEC)

# ===== MAIN =====
async def main_async():
    log("START: main_async entered")
    bot = Bot(token=BOT_TOKEN)
    conn = connect_db()

    last_update_id = int(state_get(conn, "last_update_id", "0"))
    monitor_task = asyncio.create_task(monitor_prices(bot, conn))

    while True:
        updates = await bot.get_updates(offset=last_update_id + 1, timeout=20)
        for u in updates:
            last_update_id = max(last_update_id, u.update_id)
            msg = u.channel_post
            if not msg or msg.chat.id != SOURCE_CHAT_ID:
                continue

            s = parse_signal(msg.text or "")
            if not s:
                continue

            cur = conn.execute(
                """INSERT OR IGNORE INTO signals
                (source_message_id,symbol,side,entry1_low,entry1_high,entry2_low,entry2_high,tps_json,created_ts)
                VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    msg.message_id,
                    s["symbol"], s["side"],
                    s["entry1_low"], s["entry1_high"],
                    s["entry2_low"], s["entry2_high"],
                    json.dumps(s["tps"]),
                    int(time.time())
                )
            )
            conn.commit()
            if cur.rowcount == 0:
                continue

            entry2_line = ""
            if s["entry2_low"] is not None:
                entry2_line = f"Entry2: {fmt(s['entry2_low'])} - {fmt(s['entry2_high'])}\n"

            await post(SOURCE_CHAT_ID, bot,
                "🆕 Nový signál uložen\n"
                f"{s['symbol']} ({s['side']})\n"
                f"Entry1: {fmt(s['entry1_low'])} - {fmt(s['entry1_high'])}\n"
                f"{entry2_line}"
                f"TPs: {len(s['tps'])}"
            )

            if s["auto_activate"]:
                price = await get_price(s["symbol"])
                if price:
                    conn.execute(
                        "UPDATE signals SET activated=1,activated_ts=?,activated_price=? WHERE source_message_id=?",
                        (int(time.time()), price, msg.message_id)
                    )
                    conn.commit()
                    await post(TARGET_CHAT_ID, bot,
                        f"✅ Signál aktivován (market)\n{s['symbol']} ({s['side']})\nCena: {fmt(price)}"
                    )

        state_set(conn, "last_update_id", str(last_update_id))
        await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    asyncio.run(main_async())
