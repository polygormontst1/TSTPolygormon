#!/usr/bin/env python3
# dashboard_writer.py
# FIXED VERSION – correct TP profit handling with EP2 gating

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
    return json.loads(base64.b64decode(b64).decode("utf-8"))


def safe_float(x):
    try:
        if x is None:
            return None
        s = str(x).replace("%", "").replace("✓", "").strip()
        return float(s)
    except Exception:
        return None


def fmt_price(x):
    if x is None:
        return ""
    return f"{x:.6f}".rstrip("0").rstrip(".")


def fmt_tp_cell(x):
    if x is None:
        return ""
    return f"✓ {x:.1f}%"


def dt_from_ts(ts: int):
    try:
        return datetime.datetime.fromtimestamp(int(ts), tz=datetime.timezone.utc).strftime("%d.%m.%Y")
    except Exception:
        return ""


def symbol_to_coin_quote(symbol: str):
    s = (symbol or "").upper()
    if s.endswith("USDT"):
        return s[:-4], "USDT"
    return s, ""


def col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# ========= SHEETS =========
def make_service():
    creds = Credentials.from_service_account_info(
        b64_to_json_dict(GOOGLE_CREDS_JSON_B64),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def get_values(service, rng):
    return service.spreadsheets().values().get(
        spreadsheetId=GSHEET_ID, range=rng
    ).execute().get("values", [])


def update_values(service, rng, values):
    service.spreadsheets().values().update(
        spreadsheetId=GSHEET_ID,
        range=rng,
        valueInputOption="RAW",
        body={"values": values},
    ).execute()


# ========= CORE =========
def read_signals(service):
    v = get_values(service, f"{GSHEET_SIGNALS_TAB}!A:Z")
    return (v[0], v[1:]) if len(v) > 1 else ([], [])


def read_profits(service):
    v = get_values(service, f"{GSHEET_PROFITS_TAB}!A:Z")
    return (v[0], v[1:]) if len(v) > 1 else ([], [])


def ensure_header(service):
    h = get_values(service, f"{GSHEET_DASHDATA_TAB}!A1:AZ1")
    if h and h[0] and str(h[0][0]).lower() in ("datum", "date"):
        return

    headers = ["Datum", "Prefix", "Dir.", "Coin", "Quote", "EP1", "EP2", "MP/EP1", "MP"]
    headers += [f"TP{i}" for i in range(1, 21)]
    headers += ["Doba", "SL1", "SL2", "Min", "Max", "Link", "Note", "Status"]

    update_values(service, f"{GSHEET_DASHDATA_TAB}!A1", [headers])
    log("Headers created in _DASH_DATA")


def build_profit_maps(ph, pr):
    idx = {h: i for i, h in enumerate(ph)}
    req = ["SignalID", "TPIndex", "ProfitLevPct_E1", "ProfitLevPct_E2"]

    if any(r not in idx for r in req):
        log("Missing profit headers")
        return {}, {}

    tp_map = {}
    max_map = {}

    for row in pr:
        sid = str(row[idx["SignalID"]]).strip()
        if not sid:
            continue

        try:
            tp = int(float(row[idx["TPIndex"]]))
        except Exception:
            continue

        p1 = safe_float(row[idx["ProfitLevPct_E1"]])
        p2 = safe_float(row[idx["ProfitLevPct_E2"]])

        tp_map.setdefault(sid, {}).setdefault(tp, {"e1": None, "e2": None})

        if p1 is not None:
            cur = tp_map[sid][tp]["e1"]
            tp_map[sid][tp]["e1"] = p1 if cur is None or p1 > cur else cur

        if p2 is not None:
            cur = tp_map[sid][tp]["e2"]
            tp_map[sid][tp]["e2"] = p2 if cur is None or p2 > cur else cur

    return tp_map


def pick_last_signals(h, r, n):
    idx = {h[i]: i for i in range(len(h))}
    rows = sorted(
        r,
        key=lambda x: int(float(x[idx["CreatedTS"]])) if x[idx["CreatedTS"]] else 0,
        reverse=True,
    )
    return rows[:n], idx


def build_dash_rows(rows, idx, tp_map):
    out = []

    for r in rows:
        sid = str(r[idx["SignalID"]]).strip()
        side = str(r[idx["Side"]]).upper()
        activated = str(r[idx["Activated"]]).strip() == "1"
        e2_act = str(r[idx["Entry2Activated"]]).strip() == "1"

        ts = int(float(r[idx["CreatedTS"]])) if r[idx["CreatedTS"]] else 0
        symbol = r[idx["Symbol"]]

        ep1 = safe_float(r[idx["ActivatedPrice"]]) if activated else None
        ep2 = safe_float(r[idx["Entry2ActivatedPrice"]]) if e2_act else None

        coin, quote = symbol_to_coin_quote(symbol)

        row = [
            dt_from_ts(ts),
            "",
            "Long" if side == "LONG" else "Short",
            coin,
            quote,
            fmt_price(ep1),
            fmt_price(ep2),
            "",
            "",
        ]

        tpdata = tp_map.get(sid, {})
        for i in range(1, 21):
            d = tpdata.get(i)
            if not d:
                row.append("")
                continue

            if e2_act:
                v = max(x for x in [d["e1"], d["e2"]] if x is not None)
            else:
                v = d["e1"]

            row.append(fmt_tp_cell(v) if v is not None else "")

        row += ["", "", "", "", "", "", "✓ EP2" if e2_act else "", "ACTIVE" if activated else "WAIT"]
        out.append(row)

    return out


def main_once():
    service = make_service()
    ensure_header(service)

    sh, sr = read_signals(service)
    ph, pr = read_profits(service)

    tp_map = build_profit_maps(ph, pr)
    rows, idx = pick_last_signals(sh, sr, DASH_ROWS)
    dash_rows = build_dash_rows(rows, idx, tp_map)

    width = 9 + 20 + 8
    end = col_letter(width)

    service.spreadsheets().values().clear(
        spreadsheetId=GSHEET_ID,
        range=f"{GSHEET_DASHDATA_TAB}!A2:{end}2000",
        body={},
    ).execute()

    if dash_rows:
        update_values(service, f"{GSHEET_DASHDATA_TAB}!A2", dash_rows)

    log(f"dashboard_writer DONE rows={len(dash_rows)}")


if __name__ == "__main__":
    log("dashboard_writer START")
    while True:
        try:
            main_once()
        except Exception as e:
            log(f"WRITER ERROR: {e}")
        time.sleep(WRITER_INTERVAL_SEC)
