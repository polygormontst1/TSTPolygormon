#!/usr/bin/env python3
# dashboard_writer.py
# Reads Signals + Profits from Google Sheets and writes a clean member-facing dataset to _DASH_DATA.
# High-water mark rule: values never decrease (computed from Profits combined leverage profit).
#
# Runs in a loop (Render Background Worker requires a long-running process).

import os
import time
import json
import base64
import datetime

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

# optional – if you want MP/EP1 and MP; if missing/unreachable, MP fields will just be blank
PROXY_PRICE_URL = os.getenv("PROXY_PRICE_URL", "").strip().rstrip("/")

# Keep consistent with bot rule (future-proof)
ENTRY2_DISABLE_PROFIT_PCT = float(os.getenv("ENTRY2_DISABLE_PROFIT_PCT", "15"))

# loop interval for Render worker
WRITER_INTERVAL_SEC = int(os.getenv("WRITER_INTERVAL_SEC", "120"))


# ========= HELPERS =========
def b64_to_json_dict(b64: str) -> dict:
    raw = base64.b64decode(b64.encode("utf-8"))
    return json.loads(raw.decode("utf-8"))


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


def pct_from_entry(price: float, entry: float, side: str) -> float:
    if not entry:
        return 0.0
    if side.upper() == "LONG":
        return (price - entry) / entry * 100.0
    return (entry - price) / entry * 100.0


def get_price(symbol: str):
    if not PROXY_PRICE_URL:
        return None
    try:
        r = requests.get(
            PROXY_PRICE_URL,
            params={"symbol": symbol},
            timeout=8,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json,*/*"},
        )
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
    # Safe across Python versions (no datetime.UTC dependency)
    try:
        return datetime.datetime.fromtimestamp(int(ts), tz=datetime.timezone.utc).strftime("%d.%m.%Y")
    except Exception:
        return ""


def symbol_to_coin_quote(symbol: str):
    s = (symbol or "").strip().upper()
    if s.endswith("USDT"):
        return s[:-4], "USDT"
    return s, ""


def col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


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
        body={"values": values},
    ).execute()


# ========= CORE =========
def read_signals(service):
    vals = get_values(service, f"{GSHEET_SIGNALS_TAB}!A:U")
    if not vals or len(vals) < 2:
        return [], []
    return vals[0], vals[1:]


def read_profits(service):
    vals = get_values(service, f"{GSHEET_PROFITS_TAB}!A:Z")
    if not vals or len(vals) < 2:
        return [], []
    return vals[0], vals[1:]


def ensure_header(service):
    existing = get_values(service, f"{GSHEET_DASHDATA_TAB}!A1:AZ1")
    if existing and existing[0] and str(existing[0][0]).strip().lower() in ("datum", "date"):
        return

    headers = ["Datum", "Prefix", "Dir.", "Coin", "Quote", "EP1", "EP2", "MP/EP1", "MP"]
    headers += [f"TP{i}" for i in range(1, 21)]
    headers += ["Doba", "SL1", "SL2", "Min", "Max", "Link", "Note", "Status"]
    update_values(service, f"{GSHEET_DASHDATA_TAB}!A1", [headers])
    log("DASH: headers written to _DASH_DATA")


def build_profit_maps(profit_headers, profit_rows):
    # Uses your Profits columns: ProfitLevPct_E1 + ProfitLevPct_E2 as combined leverage profit.
    idx = {h: i for i, h in enumerate(profit_headers)}

    req = ["SignalID", "TPIndex", "ProfitLevPct_E1", "ProfitLevPct_E2"]
    if any(r not in idx for r in req):
        log(f"Profits headers missing some of {req}. Found={profit_headers}")
        return {}, {}

    tp_max_lev = {}  # {sid: {tp_index: max_combined}}
    max_lev = {}     # {sid: max_combined}

    for row in profit_rows:
        sid = str(row[idx["SignalID"]] if idx["SignalID"] < len(row) else "").strip()
        if not sid:
            continue

        tp_raw = row[idx["TPIndex"]] if idx["TPIndex"] < len(row) else ""
        try:
            tp_index = int(float(str(tp_raw).strip()))
        except Exception:
            continue

        p1 = safe_float(row[idx["ProfitLevPct_E1"]] if idx["ProfitLevPct_E1"] < len(row) else None) or 0.0
        p2 = safe_float(row[idx["ProfitLevPct_E2"]] if idx["ProfitLevPct_E2"] < len(row) else None) or 0.0
        combined = p1 + p2

        tp_max_lev.setdefault(sid, {})
        prev_tp = tp_max_lev[sid].get(tp_index)
        if prev_tp is None or combined > prev_tp:
            tp_max_lev[sid][tp_index] = combined

        prev_m = max_lev.get(sid)
        if prev_m is None or combined > prev_m:
            max_lev[sid] = combined

    return tp_max_lev, max_lev


def pick_last_signals(signal_headers, signal_rows, n: int):
    idx = {h: i for i, h in enumerate(signal_headers)}
    if "CreatedTS" not in idx or "SignalID" not in idx:
        raise RuntimeError("Signals tab must have headers CreatedTS and SignalID.")

    def row_ts(r):
        ts = r[idx["CreatedTS"]] if idx["CreatedTS"] < len(r) else ""
        try:
            return int(float(str(ts).strip()))
        except Exception:
            return 0

    return sorted(signal_rows, key=row_ts, reverse=True)[:n], idx


def build_dash_rows(last_rows, sidx, tp_max_lev, max_lev):
    out = []
    for r in last_rows:
        sid = str(r[sidx["SignalID"]] if sidx["SignalID"] < len(r) else "").strip()

        created_ts = r[sidx["CreatedTS"]] if sidx["CreatedTS"] < len(r) else ""
        try:
            created_ts_i = int(float(str(created_ts).strip()))
        except Exception:
            created_ts_i = 0

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

        ep1 = act_price if (activated and act_price is not None) else (
            None if (e1l is None and e1h is None) else ((e1l or 0) + (e1h or 0)) / 2.0
        )
        ep2 = e2_price if (e2_act and e2_price is not None) else (
            None if (e2l is None and e2h is None) else ((e2l or 0) + (e2h or 0)) / 2.0
        )

        coin, quote = symbol_to_coin_quote(str(symbol))

        mp = get_price(str(symbol).strip().upper())
        mp_spot = pct_from_entry(mp, ep1, side) if (activated and mp is not None and ep1 is not None) else None

        prefix = ""
        dir_disp = "Long" if side == "LONG" else ("Short" if side == "SHORT" else side.title())

        note = "✓ EP2" if e2_act else ""

        row = [
            dt_from_ts(created_ts_i),
            prefix,
            dir_disp,
            coin,
            quote,
            fmt_price(ep1),
            fmt_price(ep2),
            (f"{mp_spot:+.2f}%" if mp_spot is not None else ""),
            fmt_price(mp),
        ]

        tpmap = tp_max_lev.get(sid, {})
        for i in range(1, 21):
            v = tpmap.get(i)
            row.append(fmt_tp_cell(v) if v is not None else "")

        maxv = max_lev.get(sid)
        row += ["", "", "", "", fmt_pct(maxv), "", note, status if status else ("ACTIVE" if activated else "WAIT")]
        out.append(row)

    return out


def main_once():
    service = make_service()
    ensure_header(service)

    sh, sr = read_signals(service)
    ph, pr = read_profits(service)

    tp_max_lev, max_lev = build_profit_maps(ph, pr)

    last_rows, sidx = pick_last_signals(sh, sr, DASH_ROWS)
    dash_rows = build_dash_rows(last_rows, sidx, tp_max_lev, max_lev)

    # Clear old rows (keep header). Total columns = 9 base + 20 TP + 8 tail = 37 columns.
    width = 9 + 20 + 8
    end_col = col_letter(width)
    clear_rng = f"{GSHEET_DASHDATA_TAB}!A2:{end_col}2000"
    service.spreadsheets().values().clear(spreadsheetId=GSHEET_ID, range=clear_rng, body={}).execute()

    if dash_rows:
        update_values(service, f"{GSHEET_DASHDATA_TAB}!A2", dash_rows)

    log(f"dashboard_writer DONE rows={len(dash_rows)} (DASH_ROWS={DASH_ROWS})")


if __name__ == "__main__":
    log("dashboard_writer START (loop)")
    while True:
        try:
            main_once()
        except Exception as e:
            log(f"WRITER ERROR: {e}")
        time.sleep(WRITER_INTERVAL_SEC)
