#!/usr/bin/env python3
"""
setup.py  ─  ONE-TIME SETUP  (run this FIRST, once only)
=========================================================
Registers ONE Google Spreadsheet that contains all 4 tabs:

  Source       — college source data (must already exist with data)
  Intermediate — keyword + ranking working sheet (Apps Script runs here)
  Final        — live aggregated view per college-course (auto-synced by Apps Script)
  Content      — historical columnar ranking data (written by phase2 Python)

Tabs are CREATED automatically if they don't exist yet.
The Source tab must already exist and have college data in it.
No existing data is ever deleted or replaced.

USAGE
─────
    python setup.py

You will be prompted for ONE Google Spreadsheet URL that will hold all tabs.

SETUP (one time)
────────────────
    pip install gspread google-auth google-auth-oauthlib
    • Put credentials.json (OAuth2 Desktop) in the same folder.
    • First run opens a browser for Google sign-in → token.json saved.
"""

import os, sys, re, json

import gspread
from google.oauth2.credentials       import Credentials
from google_auth_oauthlib.flow       import InstalledAppFlow
from google.auth.transport.requests  import Request


# ══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════

CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
STATE_FILE       = "pipeline_state.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

# Tab names (must match Apps Script constants)
TAB_SOURCE       = "Source"
TAB_INTERMEDIATE = "Intermediate"
TAB_FINAL        = "Final"
TAB_CONTENT      = "Content"

# Headers for each tab
SOURCE_REQUIRED_COLS = ["college_id", "course_id", "college_name", "course_name"]

INTER_HEADER = [
    "college_id", "course_id", "college_name", "course_name",
    "keyword", "Rank", "Found URL", "updated_at",
]

FINAL_HEADER = [
    "College_Id", "Course_Id", "College_Name", "Course_Name", "Keywords",
    "Admissions", "Admissions_URL",
    "Fees", "Fees_URL",
    "Placements", "Placements_URL",
    "Scholarships", "Scholarships_URL",
    "Main", "Main_URL",
    "Single_Course", "Single_Course_URL",
    "Updated_at",
]

CONTENT_HEADER = [
    "college_id", "course_id", "college_name", "course_name", "base_keyword",
]


# ══════════════════════════════════════════════════════════════
#  UTILITIES
# ══════════════════════════════════════════════════════════════

def extract_sheet_id(url: str) -> str | None:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", url)
    return m.group(1) if m else None


def get_credentials() -> Credentials:
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("  Refreshing token …")
            creds.refresh(Request())
        else:
            if not os.path.exists(CREDENTIALS_FILE):
                print(f"\nERROR: '{CREDENTIALS_FILE}' not found.")
                print("  Download OAuth2 Desktop credentials from GCP Console and save as credentials.json")
                sys.exit(1)
            print("  Opening browser for Google sign-in …")
            flow  = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
        print(f"  Token cached → {TOKEN_FILE}")
    return creds


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {
        "spreadsheet_id":     None,
        "script_id":          None,
        "batches":            [],
        "next_batch_to_rank": 0,
        "phase1_last_run":    None,
        "phase2_last_run":    None,
    }


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def prompt_url(label: str) -> str:
    while True:
        url = input(f"\n  Paste the {label} URL:\n  > ").strip()
        if not url:
            print("  ✗ URL cannot be empty. Try again.")
            continue
        sheet_id = extract_sheet_id(url)
        if not sheet_id:
            print("  ✗ Could not find a spreadsheet ID in that URL. Try again.")
            continue
        return url


# ══════════════════════════════════════════════════════════════
#  TAB HELPERS
# ══════════════════════════════════════════════════════════════

def get_or_create_tab(sh: gspread.Spreadsheet, name: str) -> gspread.Worksheet:
    """Return an existing tab by name, or create it if it doesn't exist."""
    try:
        ws = sh.worksheet(name)
        print(f"    Found existing tab: '{name}'")
        return ws
    except gspread.exceptions.WorksheetNotFound:
        print(f"    Creating new tab: '{name}' …")
        return sh.add_worksheet(title=name, rows=1000, cols=30)


def write_header_if_empty(ws: gspread.Worksheet, header: list, tab_name: str) -> None:
    """Write header row only if the tab is completely empty."""
    first_row = ws.row_values(1)
    if not first_row or all(v.strip() == "" for v in first_row):
        ws.append_row(header, value_input_option="USER_ENTERED")
        print(f"    ✓ Header written to '{tab_name}' tab")
    else:
        print(f"    ✓ '{tab_name}' already has a header row (not overwritten)")


def validate_source_tab(ws: gspread.Worksheet) -> None:
    """Verify Source tab has the required columns."""
    headers       = ws.row_values(1)
    headers_lower = [h.lower().replace(" ", "_") for h in headers]
    missing = [c for c in SOURCE_REQUIRED_COLS if c not in headers_lower]
    if missing:
        print(f"  ✗ Source tab is missing required columns: {missing}")
        print(f"    Found headers: {headers}")
        sys.exit(1)
    row_count = len(ws.get_all_values()) - 1
    print(f"    ✓ Source tab OK  ({row_count} data rows)")


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def main() -> None:
    print("=" * 60)
    print("  Collegedunia Rank Pipeline — ONE-TIME SETUP")
    print("=" * 60)
    print("\nThis configures ONE Google Spreadsheet with 4 tabs:")
    print(f"  {TAB_SOURCE} | {TAB_INTERMEDIATE} | {TAB_FINAL} | {TAB_CONTENT}")
    print("\nNo existing data will be deleted or replaced.\n")

    # ── Load existing state ───────────────────────────────────
    state = load_state()

    # Migrate old 3-sheet config to new single-spreadsheet config
    old_ids = ["source_sheet_id", "intermediate_sheet_id", "master_sheet_id"]
    if any(state.get(k) for k in old_ids) and not state.get("spreadsheet_id"):
        print("⚠  Detected old 3-spreadsheet config.")
        print("   Migrating to single-spreadsheet mode.")
        print("   You will need to provide the new unified spreadsheet URL.\n")
        for k in old_ids:
            state.pop(k, None)

    if state.get("spreadsheet_id"):
        print(f"⚠  pipeline_state.json already configured:")
        print(f"   Spreadsheet ID: {state['spreadsheet_id']}")
        ans = input("\n  Re-register with a different spreadsheet? (y/n): ").strip().lower()
        if ans != "y":
            print("\nSetup skipped — existing config kept.")
            sys.exit(0)

    # ── Prompt for ONE spreadsheet URL ────────────────────────
    print("[Step 1/1]  UNIFIED SPREADSHEET")
    print("  This spreadsheet must already have a 'Source' tab with college data.")
    print("  All other tabs (Intermediate, Final, Content) will be created if missing.")
    ss_url = prompt_url("unified spreadsheet")
    ss_id  = extract_sheet_id(ss_url)

    # ── Auth ──────────────────────────────────────────────────
    print("\n\nAuthenticating with Google …")
    creds = get_credentials()
    gc    = gspread.authorize(creds)
    print("✓ Authenticated\n")

    # ── Open spreadsheet ──────────────────────────────────────
    try:
        sh = gc.open_by_key(ss_id)
        print(f"✓ Opened: '{sh.title}'\n")
    except gspread.exceptions.SpreadsheetNotFound:
        print("✗ Spreadsheet not found.")
        print("  Make sure the spreadsheet is shared with your Google account as Editor.")
        sys.exit(1)

    # ── Prompt for Apps Script Deployment ID (optional) ──────
    print("\n[Step 2/2]  APPS SCRIPT DEPLOYMENT ID  (for run_pipeline.py)")
    print("  This allows Python to trigger the ranking script automatically.")
    print("  If you haven't deployed yet, press Enter to skip and add it later.\n")
    print("  To get this ID:")
    print("    1. In your spreadsheet → Extensions → Apps Script")
    print("    2. Click Deploy → New deployment")
    print("    3. Type: API executable  |  Execute as: Me")
    print("    4. Click Deploy → copy the Deployment ID shown")
    existing_sid = state.get("script_id", "")
    if existing_sid:
        print(f"  (current: {existing_sid})")
    raw_sid = input("\n  Paste Deployment ID (or press Enter to skip):\n  > ").strip()
    script_id = raw_sid if raw_sid else existing_sid

    # ── Check / create each tab ───────────────────────────────
    print("Checking / creating tabs …\n")

    # Source tab — must already exist with data
    print(f"  [{TAB_SOURCE}] (must already exist)")
    try:
        source_ws = sh.worksheet(TAB_SOURCE)
        validate_source_tab(source_ws)
    except gspread.exceptions.WorksheetNotFound:
        print(f"  ✗ '{TAB_SOURCE}' tab not found in the spreadsheet.")
        print(f"    Please create it manually and add college data, then re-run setup.py.")
        sys.exit(1)

    # Intermediate tab — create if missing, write header if empty
    print(f"\n  [{TAB_INTERMEDIATE}] (Apps Script runs ranking here)")
    inter_ws = get_or_create_tab(sh, TAB_INTERMEDIATE)
    write_header_if_empty(inter_ws, INTER_HEADER, TAB_INTERMEDIATE)

    # Final tab — create if missing, write header if empty
    print(f"\n  [{TAB_FINAL}] (live aggregated view — Apps Script maintains this)")
    final_ws = get_or_create_tab(sh, TAB_FINAL)
    write_header_if_empty(final_ws, FINAL_HEADER, TAB_FINAL)

    # Content tab — create if missing, write header if empty
    print(f"\n  [{TAB_CONTENT}] (historical columnar data — phase2 Python maintains this)")
    content_ws = get_or_create_tab(sh, TAB_CONTENT)
    write_header_if_empty(content_ws, CONTENT_HEADER, TAB_CONTENT)

    # ── Save state ────────────────────────────────────────────
    state["spreadsheet_id"]    = ss_id
    state["script_id"]         = script_id or state.get("script_id")
    state.setdefault("batches",            [])
    state.setdefault("next_batch_to_rank", 0)
    state.setdefault("phase1_last_run",    None)
    state.setdefault("phase2_last_run",    None)
    save_state(state)

    # ── Summary ───────────────────────────────────────────────
    print(f"\n{'─'*60}")
    print("  ✓ Setup complete!  pipeline_state.json updated.")
    print(f"{'─'*60}")
    print(f"\n  Spreadsheet ID : {ss_id}")
    if state.get("script_id"):
        print(f"  Script ID      : {state['script_id']}")
    else:
        print(f"  Script ID      : ⚠ not set yet (needed for run_pipeline.py)")
    print(f"\n  Tabs ready:")
    print(f"    ✓ {TAB_SOURCE:<14} — college source data (read-only for pipeline)")
    print(f"    ✓ {TAB_INTERMEDIATE:<14} — keyword ranking working sheet")
    print(f"    ✓ {TAB_FINAL:<14} — live aggregated view (auto-synced by Apps Script)")
    print(f"    ✓ {TAB_CONTENT:<14} — historical run data (phase2 Python)")
    print("\nNEXT STEPS")
    print("  1. python phase1_populate.py")
    print("       → Reads Source tab, generates keywords in Intermediate tab")
    print("  2. Open spreadsheet → Extensions → Apps Script")
    print("       → Paste intermediate_apps_script.js")
    print("       → Fill BRIGHTDATA_API_TOKEN → Run Batch")
    print("       → Final tab is updated automatically when batch finishes")
    print("  3. python phase2_build_master.py")
    print("       → Aggregates rankings into Content tab (historical tracking)")
    print("\n  TIP: In Apps Script → Rank Checker → Enable Auto-Sync on Delete")
    print("       so that deleting rows from Intermediate auto-updates Final.")
    if not state.get("script_id"):
        print("\n  ⚠  To enable the single-command pipeline (run_pipeline.py):")
        print("     Follow the ONE-TIME SETUP steps at the top of run_pipeline.py")


if __name__ == "__main__":
    main()
