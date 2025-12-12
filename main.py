import os, re, time, json, sqlite3, asyncio
import requests
from telegram import Bot
from telegram.error import TelegramError

def log(msg):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)

BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
SOURCE_CHAT_ID = int(os.environ["SOURCE_CHAT_ID"].strip())
TARGET_CHAT_ID = int(os.environ["TARGET_CHAT_ID"].strip())

CHECK_INTERVAL_SEC = int(os.getenv("CHECK_INTERVAL_SEC", "15"))
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "3"))
ENTRY_REF_MODE = os.getenv("ENTRY_REF_MODE", "HIGH").upper()  # HIGH | LOW
DB_PATH = os.getenv("DB_PATH", "bot.db")

# POSLEDNÍ POKUS: Návrat k Binance Ticker. Očekáváme Chybu 451, ale zkusíme to.
BINANCE_PRICE_URL = "https://api.binance.com/api/v3/ticker/price"

PAIR_RE = re.compile(r"^\s*([A-Z0-9]+)\s*/\s*(USDT)\s*(Buy|Sell)\s*on", re.IGNORECASE | re.MULTILINE)
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
    x = float(a); y = float(b) if b else float(a)
    return (x, y) if x <= y else (y, x)

def parse_signal(text: str):
    m = PAIR_RE.search(text or "")
    if not m:
        return None
    base = m.group(1).upper()
    quote = m.group(2).upper()
    side_word = m.group(3).lower()
    side = "LONG" if side_word == "buy" else "SHORT"
    symbol = f"{base}{quote}"

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
        "tps": tps
    }

def entry_ref(entry1_low, entry1_high):
    return entry1_low if ENTRY_REF_MODE == "LOW" else entry1_high

def pct_from_entry(price, entry, side):
    return (price - entry) / entry * 100.0 if side == "LONG" else (entry - price) / entry * 100.0

def fmt(x):
    return f"{x:.8f}".rstrip("0").rstrip(".")

async def post(bot: Bot, text: str):
    try:
        await bot.send_message(chat_id=TARGET_CHAT_ID, text=text, disable_web_page_preview=True)
    except TelegramError as e:
        log(f"post() TelegramError type={type(e).__name__} repr={repr(e)} str={str(e)}")

# OPRAVENÁ get_price_sync PRO BINANCE PUBLIC TICKER
def get_price_sync(symbol: str):
    # Voláme Binance API s parametrem symbolu (BTCUSDT)
    r = requests.get(BINANCE_PRICE_URL, params={"symbol": symbol}, timeout=8) 
    r.raise_for_status()
    
    data = r.json()
    
    # Binance Ticker vrací cenu pod klíčem 'price'
    if 'price' in data:
        return float(data['price'])
    
    # Zde se objeví log v případě jiného než očekávaného JSONu
    log(f"Binance API: Price not found in response for {symbol}. Raw response: {data}")
    return None

async def get_price(symbol: str):
    try:
        return await asyncio.to_thread(get_price_sync, symbol)
    except requests.exceptions.HTTPError as e:
        # TADY SE UKÁŽE, JESTLI JE TO ZASE CHYBA 451
        log(f"get_price({symbol}) HTTPError: {e.response.status_code} - {e}")
        return None
    except Exception as e:
        log(f"get_price({symbol}) error: {e}")
        return None

def save_signal(conn, source_message_id, s):
    try:
        conn.execute(
            """INSERT INTO signals(source_message_id, symbol, side, entry1_low, entry1_high, entry2_low, entry2_high, tps_json, created_ts)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (source_message_id, s["symbol"], s["side"], s["entry1_low"], s["entry1_high"],
             s["entry2_low"], s["entry2_high"], json.dumps(s["tps"]), int(time.time()))
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

async def fetch_channel_posts(bot: Bot, last_update_id: int):
    try:
        updates = await bot.get_updates(
            offset=last_update_id + 1,
            timeout=20,
            allowed_updates=["channel_post", "edited_channel_post"]
        )
    except TelegramError as e:
        log(f"get_updates TelegramError type={type(e).__name__} repr={repr(e)} str={str(e)}")
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
            # Načítáme Entry 1 i Entry 2
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

                if not activated:
                    # Kontrola Entry 1 NEBO Entry 2
                    is_activated = False
                    
                    # 1. Kontrola Entry 1
                    if e1l <= price <= e1h:
                        is_activated = True
                    
                    # 2. Kontrola Entry 2 (pouze pokud existuje a není již aktivováno v E1)
                    if not is_activated and e2l is not None and e2h is not None and e2l <= price <= e2h:
                        is_activated = True
                    
                    if is_activated:
                        log(f"ACTIVATE sid={sid} {symbol} price={price} in range.")
                        conn.execute(
                            "UPDATE signals SET activated=1, activated_ts=?, activated_price=? WHERE id=?",
                            (int(time.time()), price, sid)
                        )
                        conn.commit()
                        await post(bot,
                            "✅ Signál aktivován\n"
                            f"{symbol} ({side})\n"
                            f"Aktuální cena: {fmt(price)}\n"
                            f"Entry1: {fmt(e1l)} - {fmt(e1h)}"
                        )
                    continue

                # TP hits (až po aktivaci)
                while tp_hits < len(tps):
                    tp = float(tps[tp_hits])
                    is_hit = (price >= tp) if side == "LONG" else (price <= tp)
                    if not is_hit:
                        break
                    tp_hits += 1
                    gain = pct_from_entry(tp, e_ref, side)
                    conn.execute("UPDATE signals SET tp_hits=? WHERE id=?", (tp_hits, sid))
                    conn.commit()
                    await post(bot,
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

    # Startup message
    await post(bot, "🤖 Bot běží.")

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
                inserted = save_signal(conn, p["message_id"], s)
                if inserted:
                    await post(bot,
                        "🆕 Nový signál uložen\n"
                        f"{s['symbol']} ({s['side']})\n"
                        f"Entry1: {fmt(s['entry1_low'])} - {fmt(s['entry1_high'])}\n"
                        f"Entry2: {fmt(s['entry2_low'])} - {fmt(s['entry2_high'])}\n"
                        f"TPs: {len(s['tps'])}"
                    )
                    log(f"saved signal msg_id={p['message_id']} {s['symbol']} {s['side']} entry1={s['entry1_low']}-{s['entry1_high']} entry2={s['entry2_low']}-{s['entry2_high']} tps={len(s['tps'])}")

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
