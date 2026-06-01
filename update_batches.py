#!/usr/bin/env python3
"""
update_batches.py  —  Add / refresh the 'Batch' column in the Source sheet
===========================================================================
Run once manually to populate batch numbers right now:
    python3 update_batches.py

Also called automatically at the start of every full pipeline run and
every Quick Run — so new colleges always get a batch number immediately.

Batch logic (matches quick_run.py batch selection exactly):
  - Sort ALL unique colleges by Add value (desc) from Content sheet.
    Colleges with no Add value get Add = 0 (go to the last batch).
  - Batch 1 = colleges ranked #1–50 by Add
  - Batch 2 = colleges ranked #51–100
  - … and so on.
  - Every row in Source for the same college gets the same batch number.
"""

import os, sys, json
import gspread
from google.oauth2.credentials       import Credentials
from google_auth_oauthlib.flow       import InstalledAppFlow
from google.auth.transport.requests  import Request

CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
STATE_FILE       = "pipeline_state.json"
SUBGROUP_SIZE    = 50

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]


# ── Auth helpers ──────────────────────────────────────────────────────────────

def get_credentials():
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


def load_state():
    if not os.path.exists(STATE_FILE):
        print(f"ERROR: '{STATE_FILE}' not found.")
        sys.exit(1)
    with open(STATE_FILE) as f:
        return json.load(f)


# ── Core function (also imported by phase1_populate and quick_run) ────────────

def update_source_batches(sh, subgroup_size: int = SUBGROUP_SIZE):
    """
    Read Source + Content, compute batch numbers, write/update the
    'Batch' column in the Source sheet.

    Safe to call on every pipeline run:
      - Creates the Batch column if it doesn't exist.
      - Overwrites existing values if it does.
      - New colleges added since the last run get a batch number immediately.

    Returns: dict {college_id: batch_number}
    """
    print("\n  Updating Batch column in Source sheet …")

    src_ws  = sh.worksheet("Source")
    src_all = src_ws.get_all_values()
    if not src_all:
        print("  WARNING: Source sheet is empty — skipping batch update.")
        return {}

    header = src_all[0]

    # ── Find college_id column ────────────────────────────────────────────────
    cid_col = None
    for i, h in enumerate(header):
        hl = h.strip().lower().replace(" ", "_").replace("-", "_")
        if hl in ("college_id", "collegeid", "college_i_d"):
            cid_col = i
            break
    if cid_col is None:
        cid_col = 0   # fallback: column A

    # ── Batch column is always column E (index 4) ────────────────────────────
    batch_col  = 4          # 0-based → column E
    col_letter = "E"
    if str(header[batch_col]).strip().lower() != "batch":
        src_ws.update([["Batch"]], "E1", value_input_option="USER_ENTERED")
        print(f"  'Batch' header written to column E.")
    else:
        print(f"  'Batch' column found at column E.")

    # ── Load Add values from Content sheet ────────────────────────────────────
    add_values = _load_add_values(sh)

    # ── Get unique college IDs (preserve first-seen order as tiebreak) ────────
    unique_cids = []
    seen        = set()
    for row in src_all[1:]:
        if not any(str(v).strip() for v in row):
            continue
        cid = str(row[cid_col]).strip() if cid_col < len(row) else ""
        if cid and cid not in seen and cid != "---CHECKPOINT---":
            unique_cids.append(cid)
            seen.add(cid)

    # ── Assign batch numbers by Source row order (first 50 = batch 1, etc.) ─
    # unique_cids is already in Source first-seen order — no sorting needed.
    batch_map: dict[str, int] = {}
    for idx, cid in enumerate(unique_cids):
        batch_map[cid] = (idx // subgroup_size) + 1

    total_batches = max(batch_map.values()) if batch_map else 0
    print(f"  {len(unique_cids)} colleges → {total_batches} batches of {subgroup_size}")

    # ── Build column values for every data row ────────────────────────────────
    # We send one list-of-lists for the entire column (row 2 downward).
    batch_values = []
    for row in src_all[1:]:
        if not any(str(v).strip() for v in row):
            batch_values.append([""])   # blank row stays blank
            continue
        cid = str(row[cid_col]).strip() if cid_col < len(row) else ""
        batch_values.append([batch_map.get(cid, "")])

    if not batch_values:
        print("  No data rows to update.")
        return batch_map

    # Write entire column in one API call
    start_cell = f"{col_letter}2"
    end_cell   = f"{col_letter}{1 + len(batch_values)}"
    src_ws.update(batch_values, f"{start_cell}:{end_cell}",
                  value_input_option="USER_ENTERED")

    print(f"  ✓ Batch column written ({len(batch_values)} rows, "
          f"{total_batches} batches).")
    return batch_map


# ── Internal helpers ──────────────────────────────────────────────────────────

def _col_letter(n: int) -> str:
    """1-based column index → Excel letter (1→A, 27→AA …)."""
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def _load_add_values(sh) -> dict:
    """Read Content sheet → {college_id: float}.  Returns {} on any error."""
    try:
        content_ws  = sh.worksheet("Content")
        content_all = content_ws.get_all_values()
        if len(content_all) < 2:
            return {}
        header = [str(h).strip() for h in content_all[0]]

        key_col = 0
        for i, h in enumerate(header):
            hl = h.lower().replace(" ", "_").replace("-", "_")
            if hl in ("college_id", "collegeid"):
                key_col = i
                break

        add_col = None
        for i, h in enumerate(header):
            hl = h.lower()
            if "add" in hl and ("search" in hl or "traffic" in hl or "volume" in hl):
                add_col = i
                break
        if add_col is None:
            for i in range(len(header) - 1, -1, -1):
                if header[i].strip():
                    add_col = i
                    break
        if add_col is None:
            return {}

        result = {}
        for row in content_all[1:]:
            if not row or not any(str(v).strip() for v in row):
                continue
            cid = str(row[key_col]).strip() if key_col < len(row) else ""
            if not cid:
                continue
            raw = str(row[add_col]).strip().replace(",", "") if add_col < len(row) else "0"
            try:
                result[cid] = float(raw) if raw else 0.0
            except ValueError:
                result[cid] = 0.0
        return result
    except Exception:
        return {}


# ── Standalone entry point ────────────────────────────────────────────────────

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(base_dir)

    print("=" * 60)
    print("  update_batches.py — Batch column updater")
    print("=" * 60)

    state = load_state()
    if not state.get("spreadsheet_id"):
        print("ERROR: spreadsheet_id missing from pipeline_state.json.")
        sys.exit(1)

    print("\nAuthenticating …")
    creds = get_credentials()
    gc    = gspread.authorize(creds)
    print("  ✓ Authenticated")

    sh = gc.open_by_key(state["spreadsheet_id"])
    print(f"  ✓ Opened: '{sh.title}'")

    update_source_batches(sh)

    print("\n✓ Done. Open your Source sheet to see the Batch column.")
