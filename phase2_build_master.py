#!/usr/bin/env python3
"""
phase2_build_master.py  ─  PHASE 3 : INTERMEDIATE  →  MASTER SHEET
====================================================================
Reads the ranked rows for the CURRENT BATCH from the intermediate
sheet and writes/updates the master sheet with a new date-stamped
column group.

Silo type is detected from the keyword itself (no silo_type column):
  keyword ends with " Admissions"  → Admissions
  keyword ends with " Fees"        → Fees
  keyword ends with " Placements"  → Placements
  keyword ends with " Scholarships"→ Scholarships
  keyword ends with ")"            → Single_Course
  otherwise                        → Main

✦ DAILY RUN LIMIT — once per calendar day, resets at 12:00 AM.
✦ NO ARGUMENTS NEEDED — all sheet IDs read from pipeline_state.json.

Master sheet column layout
──────────────────────────
Fixed (always present):
  A college_id  B course_id  C college_name  D course_name  E base_keyword

Per-run group (13 cols, date-stamped):
  Admissions [DATE]   Admissions URL [DATE]
  Fees [DATE]         Fees URL [DATE]
  Placements [DATE]   Placements URL [DATE]
  Scholarships [DATE] Scholarships URL [DATE]
  Main [DATE]         Main URL [DATE]
  Single_Course [DATE] Single_Course URL [DATE]
  Updated At [DATE]

USAGE
─────
    python phase2_build_master.py
"""

import os, sys, json
from datetime import date, datetime, timedelta

import gspread
from google.oauth2.credentials       import Credentials
from google_auth_oauthlib.flow       import InstalledAppFlow
from google.auth.transport.requests  import Request


# ══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════

CREDENTIALS_FILE   = "credentials.json"
TOKEN_FILE         = "token.json"
STATE_FILE         = "pipeline_state.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

MASTER_FIXED_HEADERS = [
    "college_id", "course_id", "college_name", "course_name", "base_keyword",
]
MASTER_FIXED_COUNT = 5

SILO_ORDER = [
    "Admissions", "Fees", "Placements", "Scholarships", "Main", "Single_Course",
]

# Intermediate column indices (1-based) — 8 columns, no silo_type
INTER_COLLEGE_ID_COL   = 1   # A
INTER_COURSE_ID_COL    = 2   # B
INTER_COLLEGE_NAME_COL = 3   # C
INTER_COURSE_NAME_COL  = 4   # D
INTER_KEYWORD_COL      = 5   # E
INTER_RANK_COL         = 6   # F  ← cleared for next batch
INTER_URL_COL          = 7   # G  ← cleared for next batch
INTER_TIME_COL         = 8   # H  ← cleared for next batch


# ══════════════════════════════════════════════════════════════
#  SILO DETECTION FROM KEYWORD
# ══════════════════════════════════════════════════════════════

_SILO_SUFFIXES = [
    (" Admissions",   "Admissions"),
    (" Fees",         "Fees"),
    (" Placements",   "Placements"),
    (" Scholarships", "Scholarships"),
]

def detect_silo(keyword: str) -> str:
    kw = keyword.strip()
    for suffix, silo in _SILO_SUFFIXES:
        if kw.endswith(suffix):
            return silo
    if kw.endswith(")"):
        return "Single_Course"
    return "Main"


# ══════════════════════════════════════════════════════════════
#  DAILY RUN GUARD
# ══════════════════════════════════════════════════════════════

def check_daily_limit(state: dict) -> None:
    today    = date.today().isoformat()
    last_run = state.get("phase2_last_run")
    if last_run == today:
        now           = datetime.now()
        next_midnight = datetime(now.year, now.month, now.day) + timedelta(days=1)
        delta         = next_midnight - now
        hours         = int(delta.total_seconds() // 3600)
        minutes       = int((delta.total_seconds() % 3600) // 60)
        print("=" * 60)
        print("  Already run today!")
        print("=" * 60)
        print(f"\n  phase2_build_master.py already executed on {today}.")
        print(f"  Next run available after 12:00 AM  ({hours}h {minutes}m from now).")
        print('\n  To force a re-run: set "phase2_last_run" to null in pipeline_state.json.')
        sys.exit(0)


def record_run(state: dict) -> None:
    state["phase2_last_run"] = date.today().isoformat()


# ══════════════════════════════════════════════════════════════
#  UTILITIES
# ══════════════════════════════════════════════════════════════

def col_letter(n: int) -> str:
    letters = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


# ══════════════════════════════════════════════════════════════
#  AUTH & STATE
# ══════════════════════════════════════════════════════════════

def get_credentials() -> Credentials:
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(CREDENTIALS_FILE):
                print(f"ERROR: '{CREDENTIALS_FILE}' not found.")
                sys.exit(1)
            flow  = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    return creds


def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        print(f"ERROR: '{STATE_FILE}' not found. Run python setup.py first.")
        sys.exit(1)
    with open(STATE_FILE) as f:
        return json.load(f)


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def validate_sheet_ids(state: dict) -> None:
    if not state.get("spreadsheet_id"):
        print("ERROR: 'spreadsheet_id' missing from pipeline_state.json.")
        print("  Run  python setup.py  first.")
        sys.exit(1)


# ══════════════════════════════════════════════════════════════
#  READ INTERMEDIATE — filter by batch college_ids
# ══════════════════════════════════════════════════════════════

def read_intermediate_for_batch(
    inter_ws: gspread.Worksheet,
    college_ids: set[str],
) -> list[dict]:
    all_values = inter_ws.get_all_values()
    results = []

    for i, row in enumerate(all_values):
        if i == 0:
            continue
        while len(row) < INTER_TIME_COL:
            row.append("")

        cid      = str(row[INTER_COLLEGE_ID_COL   - 1]).strip()
        csid     = str(row[INTER_COURSE_ID_COL    - 1]).strip()
        cname    = str(row[INTER_COLLEGE_NAME_COL - 1]).strip()
        crname   = str(row[INTER_COURSE_NAME_COL  - 1]).strip()
        keyword  = str(row[INTER_KEYWORD_COL      - 1]).strip()
        rank     = str(row[INTER_RANK_COL         - 1]).strip()
        url      = str(row[INTER_URL_COL          - 1]).strip()

        if cid not in college_ids:
            continue
        if not rank:
            continue

        results.append({
            "college_id":   cid,
            "course_id":    csid,
            "college_name": cname,
            "course_name":  crname,
            "keyword":      keyword,
            "rank":         rank,
            "url":          url,
            "silo_type":    detect_silo(keyword),   # derived from keyword
        })

    return results


# ══════════════════════════════════════════════════════════════
#  GROUP BY COLLEGE-COURSE PAIR
# ══════════════════════════════════════════════════════════════

def group_by_pair(rows: list[dict]) -> dict[tuple, dict]:
    grouped: dict[tuple, dict] = {}
    for r in rows:
        key = (r["college_id"], r["course_id"])
        if key not in grouped:
            grouped[key] = {
                "college_name": r["college_name"],
                "course_name":  r["course_name"],
                "base_keyword": "",
                "silos":        {},
            }
        silo = r["silo_type"]
        grouped[key]["silos"][silo] = {"rank": r["rank"], "url": r["url"]}
        if silo == "Main":
            grouped[key]["base_keyword"] = r["keyword"]
    return grouped


# ══════════════════════════════════════════════════════════════
#  MASTER SHEET OPERATIONS
# ══════════════════════════════════════════════════════════════

def get_master_index(master_ws: gspread.Worksheet):
    all_values = master_ws.get_all_values()
    if not all_values:
        return {}, MASTER_FIXED_HEADERS[:], MASTER_FIXED_COUNT + 1

    header_row  = all_values[0]
    pair_to_row = {}
    for i, row in enumerate(all_values):
        if i == 0:
            continue
        while len(row) < 2:
            row.append("")
        cid, csid = str(row[0]).strip(), str(row[1]).strip()
        if cid and csid:
            pair_to_row[(cid, csid)] = i + 1

    last_col = len(header_row)
    while last_col > 0 and not str(header_row[last_col - 1]).strip():
        last_col -= 1
    return pair_to_row, header_row, last_col + 1


def build_column_headers(date_str: str) -> list[str]:
    headers = []
    for silo in SILO_ORDER:
        headers.append(f"{silo} [{date_str}]")
        headers.append(f"{silo} URL [{date_str}]")
    headers.append(f"Updated At [{date_str}]")
    return headers   # 13 total


def build_silo_values(silos: dict) -> list:
    values = []
    for silo in SILO_ORDER:
        info = silos.get(silo, {})
        values.append(info.get("rank", ""))
        values.append(info.get("url",  ""))
    values.append(datetime.now().strftime("%Y-%m-%d %H:%M"))
    return values


# ══════════════════════════════════════════════════════════════
#  CLEAR NEXT BATCH IN INTERMEDIATE (columns F, G, H)
# ══════════════════════════════════════════════════════════════

def clear_next_batch_in_intermediate(
    inter_ws: gspread.Worksheet,
    next_college_ids: set[str],
) -> int:
    all_values = inter_ws.get_all_values()
    ranges_to_clear = []

    for i, row in enumerate(all_values):
        if i == 0:
            continue
        while len(row) < 1:
            row.append("")
        cid = str(row[INTER_COLLEGE_ID_COL - 1]).strip()
        if cid not in next_college_ids:
            continue
        sheet_row = i + 1
        ranges_to_clear.extend([f"F{sheet_row}", f"G{sheet_row}", f"H{sheet_row}"])

    if ranges_to_clear:
        inter_ws.batch_clear(ranges_to_clear)

    return len(ranges_to_clear) // 3


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def main() -> None:
    # ── Step 0: Load state & daily limit ──────────────────────
    print("\n[0/5] Checking run eligibility …")
    state = load_state()
    validate_sheet_ids(state)
    check_daily_limit(state)
    print(f"  First run today ({date.today().isoformat()}) — proceeding.")

    # ── Step 1: Load run info from state (written by phase1) ──
    print("\n[1/5] Loading pipeline state …")
    run_number = state.get("run_number", 1)
    print(f"  Run #{run_number}")

    # ── Step 2: Auth ──────────────────────────────────────────
    print("\n[2/5] Authenticating …")
    creds = get_credentials()
    gc    = gspread.authorize(creds)
    print("  Authenticated")

    # ── Step 3: Open spreadsheet and tabs ────────────────────
    print("\n[3/5] Opening spreadsheet and tabs …")
    try:
        sh = gc.open_by_key(state["spreadsheet_id"])
        print(f"  Spreadsheet  → '{sh.title}'")
    except gspread.exceptions.SpreadsheetNotFound:
        print("  ERROR: Spreadsheet not found — re-run setup.py.")
        sys.exit(1)

    try:
        inter_ws = sh.worksheet("Intermediate")
        print("  Intermediate tab → found")
    except gspread.exceptions.WorksheetNotFound:
        print("  ERROR: 'Intermediate' tab not found.")
        sys.exit(1)

    try:
        master_ws = sh.worksheet("Content")
        print("  Content tab  → found")
    except gspread.exceptions.WorksheetNotFound:
        print("  ERROR: 'Content' tab not found.")
        sys.exit(1)

    # ── Step 4: Read ALL ranked college_ids from Intermediate ─
    # No batch concept — checkpoint system: phase1 picks colleges,
    # Apps Script ranks them, phase2 reads whoever has rank filled in.
    print("\n[4/5] Reading ranked rows from Intermediate …")
    all_inter = inter_ws.get_all_values()
    college_ids: set[str] = set()
    for i, row in enumerate(all_inter):
        if i == 0:
            continue
        cid  = str(row[INTER_COLLEGE_ID_COL - 1]).strip() if len(row) >= INTER_COLLEGE_ID_COL else ""
        rank = str(row[INTER_RANK_COL - 1]).strip()       if len(row) >= INTER_RANK_COL       else ""
        if cid and rank:
            college_ids.add(cid)

    if not college_ids:
        print("  No ranked rows found in Intermediate.")
        print("  Apps Script must run before phase2. Exiting.")
        sys.exit(0)

    print(f"  Ranked colleges found : {len(college_ids)}")

    ranked_rows = read_intermediate_for_batch(inter_ws, college_ids)
    if not ranked_rows:
        print("  No ranked rows returned. Exiting.")
        sys.exit(0)
    print(f"  Total ranked keyword rows : {len(ranked_rows)}")

    # ── Step 5: Group and write to Content (master) sheet ─────
    print("\n[5/5] Writing to Content sheet …")
    grouped     = group_by_pair(ranked_rows)
    date_str    = date.today().isoformat()
    new_headers = build_column_headers(date_str)

    pair_to_row, _, next_col = get_master_index(master_ws)

    end_col   = next_col + len(new_headers) - 1
    hdr_range = f"{col_letter(next_col)}1:{col_letter(end_col)}1"
    master_ws.update(hdr_range, [new_headers], value_input_option="USER_ENTERED")

    print(f"  Date stamp  : {date_str}")
    print(f"  New columns : {col_letter(next_col)} – {col_letter(end_col)}")

    rows_to_append = []
    cell_updates   = []

    for (cid, csid), info in grouped.items():
        silo_values = build_silo_values(info["silos"])
        if (cid, csid) in pair_to_row:
            master_row = pair_to_row[(cid, csid)]
            c_range    = (f"{col_letter(next_col)}{master_row}:"
                          f"{col_letter(end_col)}{master_row}")
            cell_updates.append({"range": c_range, "values": [silo_values]})
        else:
            base_kw    = info["base_keyword"] or f"{info['college_name']} {info['course_name']}"
            fixed_part = [cid, csid, info["college_name"], info["course_name"], base_kw]
            padding    = [""] * (next_col - MASTER_FIXED_COUNT - 1)
            rows_to_append.append(fixed_part + padding + silo_values)

    if cell_updates:
        master_ws.batch_update(cell_updates, value_input_option="USER_ENTERED")
        print(f"  Updated  {len(cell_updates)} existing rows")
    if rows_to_append:
        master_ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        print(f"  Appended {len(rows_to_append)} new rows")
    print(f"  Total pairs : {len(grouped)}")

    record_run(state)
    save_state(state)

    print(f"\n{'─'*60}")
    print(f"  Run #         : {run_number}")
    print(f"  Pairs written : {len(grouped)}")
    print(f"  Date stamp    : {date_str}")
    print(f"  Spreadsheet   : {sh.url}")
    print(f"{'─'*60}")
    print("\n  Done! Next run available after 12:00 AM tomorrow.")


if __name__ == "__main__":
    main()
