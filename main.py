import os, re, time, json, sqlite3, requests
from telegram import Bot
from telegram.error import TelegramError

BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
SOURCE_CHAT_ID = int(os.environ["SOURCE_CHAT_ID"].strip())
TARGET_CHAT_ID = int(os.environ["TARGET_CHAT_ID"].strip())

CHECK_INTERVAL_SEC = int(os.getenv("CHECK_INTERVAL_SEC", "15"))
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "3"))
ENTRY_REF_MODE = os.getenv("ENTRY_REF_MODE", "HIGH").upper()  # HIGH | LOW
DB_PATH = os.getenv("DB_PATH", "bot.db")

BINANCE_FAPI_PRICE = "https://fapi.binance.com/fapi/v1/ticker/price"

PAIR_RE = re.compile(r"^\s*([A-Z0-9]+)\s*/\s*(USDT)\s*(Buy|Sell)\s*on", re.IGNORECASE | re.MULTILINE)
ENTRY1_RE = re.compile(r"1\.\s*Entry price:\s*([0-9.]+)\s*(?:-\s*([0-9.]+))?", re.IGNORECASE)
ENTRY2_RE = re.compile(r"2\.\s*Entry price:\s*([0-9.]+)\s*(?:-\s*([0-9.]+))?", re.IGNORECASE)
RES_RE = re.compile(r"Rezistenční úrovně:\s*(.+?)(?:\n\n|\nStop Loss:|\Z)", re.IGNORECASE | re.DOTALL)

def db():
    conn = sqlite3.connect(DB_PATH)
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
    return conn

def state_get(conn, key, default):
    row = conn.execute("SELECT v FROM state WHERE k=?", (key,)).fetchone()
    return row[0] if row else default

def state_set(conn, key, value):
    conn.execute("INSERT INTO state(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (key, value))
    conn.commit()

def parse_range(a, b):
    x = float(a); y = float(b) if b else float(a)
    return (x, y) if x <= y else (y, x)

def parse_signal(text):
    m = PAIR_RE.search(text)
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
    tps = sorted(tps, reverse=(side == "SHORT"))
    if not tps:
        return None

    return dict(symbol=symbol, side=side, entry1_low=entry1_low, entry1_high=entry1_high,
                entry2_low=entry2_low, entry2_high=entry2_high, tps=tps)

def get_price(symbol):
    try:
        r = requests.get(BINANCE_FAPI_PRICE, params={"symbol": symbol}, timeout=8)
        r.raise_for_status()
        return float(r.json()["price"])
    except Exception:
        return None

def in_zone(price, low, high):
    return low <= price <= high

def entry_ref(entry1_low, entry1_high, side):
    if ENTRY_REF_MODE == "LOW":
        return entry1_low
    return entry1_high  # HIGH default

def pct_from_entry(price, entry, side):
    return (price - entry) / entry * 100.0 if side == "LONG" else (entry - price) / entry * 100.0

def fmt(x):
    return f"{x:.8f}".rstrip("0").rstrip(".")

def post(bot, text):
    try:
        bot.send_message(chat_id=TARGET_CHAT_ID, text=text, disable_web_page_preview=True)
    except TelegramError:
        pass

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

def fetch_channel_updates(bot, last_update_id):
    try:
        updates = bot.get_updates(offset=last_update_id + 1, timeout=20,
                                  allowed_updates=["channel_post", "edited_channel_post"])
    except TelegramError:
        return last_update_id, []

    if not updates:
        return last_update_id, []

    max_id = last_update_id
    channel_posts = []
    for u in updates:
        max_id = max(max_id, u.update_id)
        cp = u.channel_post or u.edited_channel_post
        if not cp or not cp.chat or cp.chat.id != SOURCE_CHAT_ID:
            continue
        text = (cp.text or cp.caption or "")
        channel_posts.append({"message_id": cp.message_id, "text": text})

    return max_id, channel_posts

def main():
    bot = Bot(token=BOT_TOKEN)
    conn = db()
    last_update_id = int(state_get(conn, "last_update_id", "0"))
    last_check_ts = 0

    post(bot, "🤖 Bot běží. Čekám na nové signály ve VIP signály.")

    while True:
        last_update_id, posts = fetch_channel_updates(bot, last_update_id)
        if posts:
            state_set(conn, "last_update_id", str(last_update_id))

        for p in posts:
            parsed = parse_signal(p["text"])
            if not parsed:
                continue
            if save_signal(conn, p["message_id"], parsed):
                z2 = ""
                if parsed["entry2_low"] is not None:
                    z2 = f"\nEntry2: {fmt(parsed['entry2_low'])}–{fmt(parsed['entry2_high'])}"
                post(bot,
                     "📌 Nový signál uložen\n"
                     f"{parsed['symbol']} ({parsed['side']})\n"
                     f"Entry1: {fmt(parsed['entry1_low'])}–{fmt(parsed['entry1_high'])}{z2}\n"
                     f"TP: {len(parsed['tps'])} úrovní"
                )

        now = time.time()
        if now - last_check_ts >= CHECK_INTERVAL_SEC:
            last_check_ts = now
            rows = conn.execute("SELECT id, symbol, side, entry1_low, entry1_high, entry2_low, entry2_high, tps_json, activated, tp_hits FROM signals").fetchall()

            for (sid, symbol, side, e1l, e1h, e2l, e2h, tps_json, activated, tp_hits) in rows:
                price = get_price(symbol)
                if price is None:
                    continue

                tps = json.loads(tps_json)
                e_ref = entry_ref(e1l, e1h, side)

                if not activated:
                    hit = in_zone(price, e1l, e1h) or (e2l is not None and in_zone(price, e2l, e2h))
                    if hit:
                        conn.execute("UPDATE signals SET activated=1, activated_ts=?, activated_price=? WHERE id=?",
                                     (int(time.time()), price, sid))
                        conn.commit()
                        post(bot,
                             "✅ Signál aktivován\n"
                             f"{symbol} ({side})\n"
                             f"Aktuální cena: {fmt(price)}\n"
                             f"Entry1: {fmt(e1l)}–{fmt(e1h)}"
                        )
                    continue

                while tp_hits < len(tps):
                    tp = float(tps[tp_hits])
                    is_hit = (price >= tp) if side == "LONG" else (price <= tp)
                    if not is_hit:
                        break
                    tp_hits += 1
                    gain = pct_from_entry(tp, e_ref, side)
                    conn.execute("UPDATE signals SET tp_hits=? WHERE id=?", (tp_hits, sid))
                    conn.commit()
                    post(bot,
                         f"🎯 {symbol} – TP{tp_hits} HIT\n"
                         f"TP cena: {fmt(tp)}\n"
                         f"Zisk: {gain:.2f}% (od Entry1)"
                    )

        time.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    main()
