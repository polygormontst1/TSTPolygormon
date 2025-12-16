#!/usr/bin/env python3
# dashboard_writer.py
# Reads Signals + Profits from Google Sheets and writes a clean member-facing dataset to _DASH_DATA.
# High-water mark rule: values never decrease (computed from Profits combined profit with leverage).

import os, time, json, base64, re, datetime
import requests

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

def log(msg: str):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)

# ========= ENV =========
GSHEET_ID = os.environ["GSHEET_ID"].strip()
GOOGLE_CREDS_JSON_B64 = os.environ["GOOGLE_CREDS_JSON_B64"].strip()

GSHEET_SIGNALS_TAB = os.getenv("GSHEET_SIGNALS_TAB", "Signals").strip()
GSHEET_PROFITS_TAB = os.getenv("GSHEET_PROFITS_TAB", "Profits").strip()
GSHEET_DASHDATA_TAB = os.getenv("GSHEET_DASHDATA_TAB", "_DASH_DATA").strip()

DASH_ROWS = int(os.getenv("DASH_ROWS", "30"))
LEVERAGE = float(os.getenv("LEVERAGE", "20"))
PROXY_PRICE_URL = os.getenv("PROXY_PRICE_URL", "https://workerrr.developctsro.workers.dev").strip().rstrip("/")

# If you later change this in the bot ENV, keep the same value here too (so Dashboard reflects the same rule).
ENTRY2_DISABLE_PROFIT_PCT = float(os.getenv("ENTRY2_DISABLE_PROFIT_PCT", "15"))

# ========= HELPERS =========
def b64_to_json_dict(b64: str) -> dict:
    raw = base64.b64decode(b64.encode("utf-8"))
    return json.loads(raw.decode("utf-8"))

def pct_from_entry(price: float, entry: float, side: str) -> float:
    if not entry:
        return 0.0
    if side.upper() == "LONG":
        return (price - entry) / entry * 100.0
    return (entry - price) / entry * 100.0

def safe_float(x):
    try:
        if x is None: 
            return None
        s = str(x).strip()
        if s == "": 
            return None
        return float(s)
    except Exception:
        return None

def get_price(symbol: str):
    try:
        r = requests.get(PROXY_PRICE_URL, params={"symbol": symbol}, timeout=8,
                         headers={"User-Agent":"Mozilla/5.0","Accept":"application/json,*/*"})
        if r.status_code != 200:
            return None
        data = r.json()
        if isinstance(data, dict) and "price" in data:
            return float(data["price"])
        return None
    except Exception:
        return None

def fmt_price(x):
    if x is None:
        return ""
    # keep small coins readable
    if x < 1:
        return f"{x:.8f}".rstrip("0").rstrip(".")
    return f"{x:.6f}".rstrip("0").rstrip(".")

def fmt_pct(x):
    if x is None:
        return ""
    return f"{x:.1f}%"

def fmt_tp_cell(x):
    if x is None:
        return ""
    return f"✓ {x:.1f}%"

def dt_from_ts(ts: int):
    try:
        return datetime.datetime.utcfromtimestamp(int(ts)).strftime("%d.%m.%Y")
    except Exception:
        return ""

def symbol_to_coin_quote(symbol: str):
    s = (symbol or "").strip().upper()
    if s.endswith("USDT"):
        return s[:-4], "USDT"
    return s, ""

# ========= SHEETS =========
def make_service():
    creds_info = b64_to_json_dict(GOOGLE_CREDS_JSON_B64)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def get_values(service, rng: str):
    resp = service.spreadsheets().values().get(spreadsheetId=GSHEET_ID, range=rng).execute()
    return resp.get("values", [])

def update_values(service, rng: str, values):
    service.spreadsheets().values().update(
        spreadsheetId=GSHEET_ID,
        range=rng,
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

# ========= CORE =========
def read_signals(service):
    # Read whole Signals sheet (safe for typical volumes; if very large, we can optimize later)
    vals = get_values(service, f"{GSHEET_SIGNALS_TAB}!A:U")
    if not vals or len(vals) < 2:
        return [], []
    headers = vals[0]
    rows = vals[1:]
    return headers, rows

def read_profits(service):
    vals = get_values(service, f"{GSHEET_PROFITS_TAB}!A:M")
    if not vals or len(vals) < 2:
        return [], []
    headers = vals[0]
    rows = vals[1:]
    return headers, rows

def build_profit_maps(profit_headers, profit_rows):
    # Build:
    # tp_max_lev[signal_id][tp_index] = max combined_lev seen at that TPIndex
    # max_lev[signal_id] = max combined_lev seen overall (across TP events)
    idx = {h:i for i,h in enumerate(profit_headers)}
    req = ["SignalID","TPIndex","ProfitLev1","ProfitLev2"]
    if any(r not in idx for r in req):
        log(f"Profits headers missing some of {req}. Found={profit_headers}")
        return {}, {}

    tp_max_lev = {}
    max_lev = {}

    for r in profit_rows:
        sid = r[idx["SignalID"]] if idx["SignalID"] < len(r) else ""
        sid = str(sid).strip()
        if not sid:
            continue

        tp_index = r[idx["TPIndex"]] if idx["TPIndex"] < len(r) else ""
        try:
            tp_index = int(float(str(tp_index).strip()))
        except Exception:
            continue

        p1 = safe_float(r[idx["ProfitLev1"]] if idx["ProfitLev1"] < len(r) else None)
        p2 = safe_float(r[idx["ProfitLev2"]] if idx["ProfitLev2"] < len(r) else None)
        combined = (p1 or 0.0) + (p2 or 0.0)

        tp_max_lev.setdefault(sid, {})
        prev = tp_max_lev[sid].get(tp_index)
        if prev is None or combined > prev:
            tp_max_lev[sid][tp_index] = combined

        prevm = max_lev.get(sid)
        if prevm is None or combined > prevm:
            max_lev[sid] = combined

    return tp_max_lev, max_lev

def pick_last_signals(signal_headers, signal_rows, n: int):
    idx = {h:i for i,h in enumerate(signal_headers)}
    if "CreatedTS" not in idx or "SignalID" not in idx:
        raise RuntimeError("Signals tab must have headers CreatedTS and SignalID.")

    def row_ts(r):
        ts = r[idx["CreatedTS"]] if idx["CreatedTS"] < len(r) else ""
        try:
            return int(float(str(ts).strip()))
        except Exception:
            return 0

    sorted_rows = sorted(signal_rows, key=row_ts, reverse=True)
    return sorted_rows[:n], idx

def build_dash_rows(last_rows, sidx, tp_max_lev, max_lev):
    out = []
    for r in last_rows:
        sid = str(r[sidx["SignalID"]]).strip() if sidx["SignalID"] < len(r) else ""
        created_ts = r[sidx["CreatedTS"]] if sidx["CreatedTS"] < len(r) else ""
        created_ts_i = 0
        try:
            created_ts_i = int(float(str(created_ts).strip()))
        except Exception:
            pass

        symbol = r[sidx["Symbol"]] if sidx["Symbol"] < len(r) else ""
        side = (r[sidx["Side"]] if sidx["Side"] < len(r) else "").upper()
        status = (r[sidx["Status"]] if "Status" in sidx and sidx["Status"] < len(r) else "").upper()
        activated = str(r[sidx["Activated"]] if "Activated" in sidx and sidx["Activated"] < len(r) else "").strip() == "1"
        e2_act = str(r[sidx["Entry2Activated"]] if "Entry2Activated" in sidx and sidx["Entry2Activated"] < len(r) else "").strip() == "1"

        e1l = safe_float(r[sidx["Entry1Low"]] if sidx["Entry1Low"] < len(r) else None)
        e1h = safe_float(r[sidx["Entry1High"]] if sidx["Entry1High"] < len(r) else None)
        e2l = safe_float(r[sidx["Entry2Low"]] if "Entry2Low" in sidx and sidx["Entry2Low"] < len(r) else None)
        e2h = safe_float(r[sidx["Entry2High"]] if "Entry2High" in sidx and sidx["Entry2High"] < len(r) else None)

        act_price = safe_float(r[sidx["ActivatedPrice"]] if "ActivatedPrice" in sidx and sidx["ActivatedPrice"] < len(r) else None)
        e2_price = safe_float(r[sidx["Entry2ActivatedPrice"]] if "Entry2ActivatedPrice" in sidx and sidx["Entry2ActivatedPrice"] < len(r) else None)

        # Display EP1/EP2
        ep1 = act_price if (activated and act_price is not None) else (None if (e1l is None and e1h is None) else ((e1l or 0)+(e1h or 0))/2.0)
        ep2 = e2_price if (e2_act and e2_price is not None) else (None if (e2l is None and e2h is None) else ((e2l or 0)+(e2h or 0))/2.0)

        coin, quote = symbol_to_coin_quote(str(symbol))

        mp = get_price(str(symbol).strip().upper())
        mp_spot = pct_from_entry(mp, ep1, side) if (activated and mp is not None and ep1 is not None) else None

        # Leverage display is already in Profits; MP_20x is optional display
        mp_20x = (mp_spot * LEVERAGE) if (mp_spot is not None) else None

        # High-water from TP events (combined lev)
        maxv = max_lev.get(sid)
        tpmap = tp_max_lev.get(sid, {})

        # Row format matches your green table style:
        # Datum | Prefix | Dir | Coin | Quote | EP1 | EP2 | MP/EP1 | MP | TP1..TP20 | ... | Max | Note
        prefix = ""  # you can set from ENV later if you want
        dir_disp = "Long" if side == "LONG" else ("Short" if side == "SHORT" else side.title())

        # EP2 indicator: keep EP2 price in EP2 column; and put a subtle check into Note (optional)
        note = ""
        if e2_act:
            note = "✓ EP2"

        row = [
            dt_from_ts(created_ts_i),     # Datum
            prefix,                       # Prefix
            dir_disp,                     # Dir.
            coin,                         # Coin
            quote,                        # Quote
            fmt_price(ep1),               # EP1
            fmt_price(ep2),               # EP2
            (f"{mp_spot:+.2f}%" if mp_spot is not None else ""),  # MP/EP1 (spot)
            fmt_price(mp),                # MP (current)
        ]

        # TP1..TP20
        for i in range(1, 21):
            v = tpmap.get(i)
            row.append(fmt_tp_cell(v) if v is not None and v != 0 else "")

        # Add Min/Max columns (your sheet has them; we'll fill only Max with high-water combined lev)
        # If your _DASH_DATA has these columns elsewhere, we can remap later.
        row += [
            "",   # Doba
            "",   # SL1
            "",   # SL2
            "",   # Min
            fmt_pct(maxv),  # Max (20x combined high-water)
            "",   # Link
            note  # Note
        ]

        # Add helper for conditional formatting: status at the very end (hidden column in Dashboard if you want)
        row.append(status if status else ("ACTIVE" if activated else "WAIT"))

        out.append(row)

    return out

def ensure_header(service):
    # We will write headers if _DASH_DATA is empty.
    existing = get_values(service, f"{GSHEET_DASHDATA_TAB}!A1:AZ1")
    if existing and existing[0] and str(existing[0][0]).strip().lower() in ("datum","date"):
        return

    headers = ["Datum","Prefix","Dir.","Coin","Quote","EP1","EP2","MP/EP1","MP"]
    headers += [f"TP{i}" for i in range(1,21)]
    headers += ["Doba","SL1","SL2","Min","Max","Link","Note","Status"]
    update_values(service, f"{GSHEET_DASHDATA_TAB}!A1", [headers])
    log("DASH: headers written to _DASH_DATA")

def main():
    log("dashboard_writer START")
    service = make_service()

    ensure_header(service)

    sh, sr = read_signals(service)
    ph, pr = read_profits(service)

    tp_max_lev, max_lev = build_profit_maps(ph, pr)

    last_rows, sidx = pick_last_signals(sh, sr, DASH_ROWS)
    dash_rows = build_dash_rows(last_rows, sidx, tp_max_lev, max_lev)

    # Write from A2 downward, clear enough area first
    # We write fixed width: 9 + 20 + 7 + 1 = 37 columns
    width = 9 + 20 + 7 + 1
    end_col = col_letter(width)
    rng = f"{GSHEET_DASHDATA_TAB}!A2:{end_col}{2 + max(0,len(dash_rows)-1)}"

    # Clear old rows area (keep header)
    clear_rng = f"{GSHEET_DASHDATA_TAB}!A2:{end_col}2000"
    service.spreadsheets().values().clear(spreadsheetId=GSHEET_ID, range=clear_rng, body={}).execute()

    if dash_rows:
        update_values(service, f"{GSHEET_DASHDATA_TAB}!A2", dash_rows)

    log(f"dashboard_writer DONE rows={len(dash_rows)} (DASH_ROWS={DASH_ROWS})")

def col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

if __name__ == "__main__":
    main()
