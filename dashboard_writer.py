#!/usr/bin/env python3
# dashboard_writer.py
# FIX: TP cells can be EP1-only (dark) or EP1+EP2 combined (light) marked with "(SUM)"
# FIX: ignore Profits rows with Note containing "RETURN" (e.g., RETURN_TO_EP1)
# NOTE: decimal comma supported

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
PROXY_PRICE_URL = os.getenv("PROXY_PRICE_URL", "").strip().rstrip("/")
WRITER_INTERVAL_SEC = int(os.getenv("WRITER_INTERVAL_SEC", "120"))


# ========= HELPERS =========
def b64_to_json_dict(b64: str) -> dict:
    raw = base64.b64decode(b64.encode("utf-8"))
    return json.loads(raw.decode("utf-8"))


def safe_float(x):
    """
    Robust numeric parser:
    - supports Czech decimal comma: '12,34'
    - strips '✓' and '%' from cells like '✓ 12,3%'
    """
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "":
            return None
        s = s.replace("✓", "").replace("%", "").strip()
        s = s.replace(" ", "")
        s = s.replace(",", ".")
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def pct_from_entry(price: float, entry: float, side: str) -> float:
    if entry is None or entry == 0:
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


def fmt_tp_cell(x, tag: str | None = None):
    """
    If tag == "(SUM)", we mark combined EP1+EP2 TP so Sheets can color it safely.
    """
    if x is None:
        return ""
    if tag:
        return f"✓ {x:.1f}% {tag}"
    return f"✓ {x:.1f}%"


def dt_from_ts(ts: int):
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
    """
    Build per-signal per-TP maxima for E1 and E2 profits separately.
    Ignore rows where Note contains "RETURN" (these are not TP hits).
    """
    idx = {h: i for i, h in enumerate(profit_headers)}

    req = ["SignalID", "TPIndex", "ProfitLevPct_E1", "ProfitLevPct_E2"]
    if any(r not in idx for r in req):
        log(f"Profits headers missing some of {req}. Found={profit_headers}")
        return {}, {}, {}, {}

    note_i = idx.get("Note", None)

    tp_max_e1 = {}  # {sid: {tp: max_e1}}
    tp_max_e2 = {}  # {sid: {tp: max_e2}}
    max_e1 = {}     # {sid: max_e1_any_tp}
    max_e2 = {}     # {sid: max_e2_any_tp}

    for row in profit_rows:
        # filter non-TP events like RETURN_TO_EP1
        if note_i is not None and note_i < len(row):
            note = str(row[note_i]).strip().upper()
            if "RETURN" in note:
                continue

        sid = str(row[idx["SignalID"]] if idx["SignalID"] < len(row) else "").strip()
        if not sid:
            continue

        tp_raw = row[idx["TPIndex"]] if idx["TPIndex"] < len(row) else ""
        try:
            tp = int(float(str(tp_raw).strip()))
        except Exception:
            continue

        p1 = safe_float(row[idx["ProfitLevPct_E1"]] if idx["ProfitLevPct_E1"] < len(row) else None)
        p2 = safe_float(row[idx["ProfitLevPct_E2"]] if idx["ProfitLevPct_E2"] < len(row) else None)

        if p1 is not None:
            tp_max_e1.setdefault(sid, {})
            prev = tp_max_e1[sid].get(tp)
            if prev is None or p1 > prev:
                tp_max_e1[sid][tp] = p1
            prevm = max_e1.get(sid)
            if prevm is None or p1 > prevm:
                max_e1[sid] = p1

        if p2 is not None:
            tp_max_e2.setdefault(sid, {})
            prev = tp_max_e2[sid].get(tp)
            if prev is None or p2 > prev:
                tp_max_e2[sid][tp] = p2
            prevm = max_e2.get(sid)
            if prevm is None or p2 > prevm:
                max_e2[sid] = p2

    return tp_max_e1, tp_max_e2, max_e1, max_e2


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


def build_dash_rows(last_rows, sidx, tp_max_e1, tp_max_e2, max_e1, max_e2):
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

        tpmap1 = tp_max_e1.get(sid, {})
        tpmap2 = tp_max_e2.get(sid, {})

        # YOUR RULE:
        # - default TP is EP1-only (dark)
        # - if EP2 active AND (EP1 + EP2) beats EP1-only => write combined and mark "(SUM)" (light)
        for i in range(1, 21):
            v1 = tpmap1.get(i)  # EP1-only profit
            v2 = tpmap2.get(i)  # EP2 profit (for same TP)

            if v1 is None and v2 is None:
                row.append("")
                continue

            # baseline: EP1-only if present, else fallback to EP2-only
            base = v1 if v1 is not None else v2
            tag = None
            outv = base

            if e2_act and (v1 is not None) and (v2 is not None):
                sumv = v1 + v2
                if sumv > base:
                    outv = sumv
                    tag = "(s 2EP)"

            TH = 0.05  # minimum TP profit to display (%)

            if outv is None or outv < TH:
                row.append("")
            else:
                row.append(fmt_tp_cell(outv, tag))



        # "Max" column: keep simple (EP1-only max, or combined if it beats it)
        max_base = max_e1.get(sid)
        max_tag = None
        max_out = max_base
        if e2_act and (max_e1.get(sid) is not None) and (max_e2.get(sid) is not None):
            s = max_e1[sid] + max_e2[sid]
            if (max_base is None) or (s > max_base):
                max_out = s
                max_tag = "(s 2EP)"

        max_cell = (f"{max_out:.1f}% {max_tag}" if (max_out is not None and max_tag) else fmt_pct(max_out))

        row += ["", "", "", "", max_cell, "", note, status if status else ("ACTIVE" if activated else "WAIT")]
        out.append(row)

    return out


def main_once():
    service = make_service()
    ensure_header(service)

    sh, sr = read_signals(service)
    ph, pr = read_profits(service)

    tp_max_e1, tp_max_e2, max_e1, max_e2 = build_profit_maps(ph, pr)

    last_rows, sidx = pick_last_signals(sh, sr, DASH_ROWS)
    dash_rows = build_dash_rows(last_rows, sidx, tp_max_e1, tp_max_e2, max_e1, max_e2)

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
