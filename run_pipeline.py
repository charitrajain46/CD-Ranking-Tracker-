#!/usr/bin/env python3
"""
run_pipeline.py — SINGLE-COMMAND FULL PIPELINE
================================================
Runs the ENTIRE ranking pipeline end-to-end with one command:

    python run_pipeline.py

Pipeline stages:
  Stage 1 → phase1_populate.py  — Source tab → Intermediate tab (keywords)
  Stage 2 → Apps Script API     — Intermediate tab → rankings via Bright Data
             (auto-repeats every 5.5 min until all rows are ranked)
  Stage 3 → phase2_build_master.py — Intermediate → Content tab (history)

══════════════════════════════════════════════════════════════
ONE-TIME SETUP (do this ONCE before first use)
══════════════════════════════════════════════════════════════

Step 1 — Enable Apps Script API in GCP
  → https://console.cloud.google.com/apis/library/script.googleapis.com
    (same GCP project as your credentials.json — just click "Enable")

Step 2 — Link Apps Script to your GCP project
  → Open your Google Spreadsheet
  → Extensions → Apps Script
  → Click the gear icon (⚙) → Project Settings
  → Scroll to "Google Cloud Platform (GCP) Project"
  → Click "Change project"
  → Enter your GCP Project Number
    (found at https://console.cloud.google.com → Dashboard → Project info)
  → Click "Set project"

Step 3 — Deploy Apps Script as API executable
  → In the Apps Script editor, click "Deploy" → "New deployment"
  → Click the gear next to "Select type" → choose "API executable"
  → Description: Pipeline API
  → Execute as: Me
  → Click "Deploy"
  → Copy the Script ID shown (looks like: AKfy...xyz)
    NOTE: This is the DEPLOYMENT ID, different from the Script ID in settings

Step 4 — Save the Script ID
  → Run:  python setup.py
  → When prompted for the Deployment ID, paste it

Step 5 — Re-authenticate (adds Apps Script scope to your token)
  → Delete token.json if it exists:  rm token.json
  → Run any script (e.g. python setup.py) — browser will open for sign-in

After these 5 steps, run:  python run_pipeline.py
══════════════════════════════════════════════════════════════
"""

import os, sys, json, time, subprocess, socket, atexit
from datetime import datetime, date, timedelta

import gspread
from google.oauth2.credentials       import Credentials
from google_auth_oauthlib.flow       import InstalledAppFlow
from google.auth.transport.requests  import Request
from googleapiclient.discovery       import build
from googleapiclient.errors          import HttpError

# Apps Script runs for up to 6 minutes — set global socket timeout to 8 minutes
socket.setdefaulttimeout(480)


# ══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════

CREDENTIALS_FILE  = "credentials.json"
TOKEN_FILE        = "token.json"
STATE_FILE        = "pipeline_state.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/script.projects",          # Execution API
    "https://www.googleapis.com/auth/script.external_request",  # UrlFetchApp
    "https://www.googleapis.com/auth/script.scriptapp",         # Script triggers
]

# How many times to re-invoke Apps Script if rows remain unranked
# Each invocation runs for ~5.5 minutes
#   227 rows  →  ~2 rounds   (~12 min)
#   6,000 rows → ~36 rounds  (~3.5 hrs)  — sequential, 1 call/row
#   6,000 rows → ~6 rounds   (~35 min)   — after parallel fetchAll upgrade
#  42,000 rows → ~42 rounds  (~4 hrs)    — parallel fetchAll only
MAX_ROUNDS = 70

# Seconds to wait after each Apps Script call before checking progress
POLL_PAUSE_SEC = 15


# ══════════════════════════════════════════════════════════════
#  AUTH & STATE
# ══════════════════════════════════════════════════════════════

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
                sys.exit(1)
            print("  Opening browser for Google sign-in …")
            flow  = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
        print(f"  Token saved → {TOKEN_FILE}")
    return creds


# ══════════════════════════════════════════════════════════════
#  PIPELINE LOCK  (blocks Quick Run while main pipeline runs)
# ══════════════════════════════════════════════════════════════

def set_pipeline_lock(state: dict) -> None:
    """Write pipeline_lock timestamp to state file — blocks quick_run.py."""
    state["pipeline_lock"] = datetime.now().isoformat()
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def clear_pipeline_lock() -> None:
    """
    Remove pipeline_lock from state file.
    Registered with atexit so it runs even on crash/KeyboardInterrupt.
    """
    try:
        if not os.path.exists(STATE_FILE):
            return
        with open(STATE_FILE) as f:
            state = json.load(f)
        if "pipeline_lock" in state:
            state.pop("pipeline_lock")
            with open(STATE_FILE, "w") as f:
                json.dump(state, f, indent=2)
    except Exception:
        pass   # best-effort; don't crash during cleanup


def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        print(f"ERROR: '{STATE_FILE}' not found. Run python setup.py first.")
        sys.exit(1)
    with open(STATE_FILE) as f:
        return json.load(f)


def validate_state(state: dict) -> None:
    errors = []
    if not state.get("spreadsheet_id"):
        errors.append("spreadsheet_id is missing — run python setup.py")
    if not state.get("script_id"):
        errors.append(
            "script_id is missing — complete the ONE-TIME SETUP at the top of this file, "
            "then run python setup.py to save the Deployment ID"
        )
    if errors:
        print("\nERROR — pipeline_state.json is not ready:")
        for e in errors:
            print(f"  • {e}")
        sys.exit(1)


# ══════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════

def banner(title: str) -> None:
    print(f"\n{'═'*60}")
    print(f"  {title}")
    print(f"{'═'*60}")


def count_unranked_rows(inter_ws: gspread.Worksheet) -> int:
    """
    Count Intermediate rows that have a keyword in column E
    but no value in column F (Rank) yet.
    """
    all_vals = inter_ws.get_all_values()
    count = 0
    for i, row in enumerate(all_vals):
        if i == 0:
            continue   # skip header
        keyword = str(row[4]).strip() if len(row) > 4 else ""
        rank    = str(row[5]).strip() if len(row) > 5 else ""
        if keyword and not rank:
            count += 1
    return count


# ══════════════════════════════════════════════════════════════
#  STAGE 1 — PHASE 1 (Source → Intermediate)
# ══════════════════════════════════════════════════════════════

def run_stage1():
    """
    Returns:
      True  — phase1 ran successfully, rows queued for ranking
      None  — nothing to rank today (exit code 2 = all colleges within 15-day cycle)
      False — phase1 failed with an error
    """
    banner("STAGE 1 — Populating Intermediate tab from Source")
    result = subprocess.run(
        [sys.executable, "phase1_populate.py"]
        # stdout/stderr stream directly to terminal (no capture_output)
    )
    if result.returncode == 2:
        print("\n  ✓ Nothing to rank today — all colleges are within the 15-day cycle.")
        print("  Pipeline will run again automatically at midnight.")
        return None
    if result.returncode != 0:
        print("\n  ✗ Stage 1 failed — see errors above.")
        return False
    return True


# ══════════════════════════════════════════════════════════════
#  STAGE 2 — APPS SCRIPT (Intermediate → Rankings)
# ══════════════════════════════════════════════════════════════

def invoke_apps_script(script_service, script_id: str) -> bool:
    """
    Call checkRanksBatched() via the Apps Script Execution API.
    Returns True if the call succeeded (even if more rows remain),
    False if the API returned an error.
    """
    try:
        response = script_service.scripts().run(
            scriptId=script_id,
            body={
                "function": "checkRanksBatched",
                "devMode":  False,
            }
        ).execute()

        if response.get("error"):
            err = response["error"]
            print(f"\n  ✗ Apps Script error (code {err.get('code', '?')}): "
                  f"{err.get('message', 'unknown error')}")
            details = err.get("details", [])
            for d in details:
                msg = d.get("scriptStackTraceElements") or d.get("errorMessage", "")
                if msg:
                    print(f"      {msg}")
            return False

        return True

    except HttpError as e:
        status = e.resp.status if hasattr(e, "resp") else "?"
        print(f"\n  ✗ HTTP {status} from Apps Script API: {e}")
        if status == 403:
            print("\n  Possible causes:")
            print("  • Apps Script API not enabled in GCP Console")
            print("  • GCP project not linked to this Apps Script")
            print("  • Script not deployed as 'API executable'")
            print("  • token.json missing script.projects scope → delete it and re-run")
        return False

    except Exception as e:
        print(f"\n  ✗ Unexpected error calling Apps Script: {e}")
        return False


def run_stage2(script_service, script_id: str, inter_ws: gspread.Worksheet) -> bool:
    banner("STAGE 2 — Running Apps Script ranking (via Execution API)")

    for round_num in range(1, MAX_ROUNDS + 1):
        unranked = count_unranked_rows(inter_ws)

        if unranked == 0:
            print(f"\n  ✓ All rows ranked — Apps Script stage complete.")
            return True

        print(f"\n  Round {round_num}/{MAX_ROUNDS} — {unranked} rows still need ranking")
        print(f"  Calling checkRanksBatched() … (runs for ~5.5 min inside Apps Script)")

        ok = invoke_apps_script(script_service, script_id)
        if not ok:
            print("\n  Aborting ranking stage due to error.")
            return False

        print(f"  Apps Script call returned.  Waiting {POLL_PAUSE_SEC}s …")
        time.sleep(POLL_PAUSE_SEC)

        unranked_after = count_unranked_rows(inter_ws)
        print(f"  Progress: {unranked} → {unranked_after} unranked rows")

        if unranked_after == 0:
            print(f"\n  ✓ All rows ranked after round {round_num}!")
            return True

        if unranked_after >= unranked:
            print(f"\n  ⚠ No progress in round {round_num}.")
            print("  Check Apps Script logs: Extensions → Apps Script → Executions")
            return False

        # More rows remain — loop will call Apps Script again
        print(f"  {unranked_after} rows still unranked — starting round {round_num + 1} …")
        time.sleep(3)

    print(f"\n  ⚠ Reached maximum rounds ({MAX_ROUNDS}). "
          f"{count_unranked_rows(inter_ws)} rows may still be unranked.")
    print("  Run 'python run_pipeline.py' again to continue, or run Apps Script manually.")
    return False


# ══════════════════════════════════════════════════════════════
#  STAGE 3 — PHASE 2 (Intermediate → Content tab)
# ══════════════════════════════════════════════════════════════

def run_stage3() -> bool:
    banner("STAGE 3 — Building Content tab (historical data)")
    result = subprocess.run([sys.executable, "phase2_build_master.py"])
    if result.returncode != 0:
        print("\n  ✗ Stage 3 failed — see errors above.")
        return False
    return True


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def main():
    start_time = datetime.now()
    banner(f"Collegedunia Rank Pipeline — FULL RUN  ({start_time.strftime('%Y-%m-%d %H:%M')})")

    # ── Load & validate ───────────────────────────────────────
    state = load_state()
    validate_state(state)

    script_id      = state["script_id"]
    spreadsheet_id = state["spreadsheet_id"]

    # ── Set pipeline lock (blocks quick_run.py) ───────────────
    set_pipeline_lock(state)
    atexit.register(clear_pipeline_lock)   # clears lock even on crash

    # ── Authenticate ──────────────────────────────────────────
    # (Daily run guard is handled inside phase1_populate.py by
    #  checking the Final sheet for today's date — not state.)
    print("\nAuthenticating …")
    creds = get_credentials()
    gc    = gspread.authorize(creds)
    print("  ✓ Authenticated")

    # ── Open Intermediate tab (used for progress polling) ─────
    try:
        sh       = gc.open_by_key(spreadsheet_id)
        inter_ws = sh.worksheet("Intermediate")
        print(f"  ✓ Opened spreadsheet: '{sh.title}'")
    except gspread.exceptions.SpreadsheetNotFound:
        print("ERROR: Spreadsheet not found — re-run setup.py.")
        sys.exit(1)
    except gspread.exceptions.WorksheetNotFound:
        print("ERROR: 'Intermediate' tab not found — re-run setup.py.")
        sys.exit(1)

    # ── Build Apps Script API service (socket timeout = 8 min) ──
    script_service = build("script", "v1", credentials=creds)

    # ── Stage 1 — Populate Intermediate ──────────────────────
    ok = run_stage1()
    if ok is None:
        # Nothing to rank today — clean exit, not an error
        elapsed = datetime.now() - start_time
        banner(f"✓ PIPELINE SKIPPED — Nothing to rank today  ({int(elapsed.total_seconds())}s)")
        print(f"  Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print()
        sys.exit(0)
    if not ok:
        print("\nPipeline aborted at Stage 1.")
        sys.exit(1)

    # ── Stage 2 — Run Apps Script ranking ────────────────────
    ok = run_stage2(script_service, script_id, inter_ws)
    if not ok:
        print("\nRanking stage had issues. Final tab may be partially populated.")
        print("You can:")
        print("  • Fix the issue and run  python run_pipeline.py  again")
        print("  • Or run Apps Script manually from the spreadsheet")
        print("  • Then run  python phase2_build_master.py  to finish Stage 3")
        sys.exit(1)

    # ── Stage 3 — Build Content tab ──────────────────────────
    ok = run_stage3()
    if not ok:
        print("\nPipeline aborted at Stage 3.")
        sys.exit(1)

    # ── Done ──────────────────────────────────────────────────
    elapsed = datetime.now() - start_time
    mins    = int(elapsed.total_seconds() // 60)
    secs    = int(elapsed.total_seconds() % 60)

    banner(f"✓ PIPELINE COMPLETE  (took {mins}m {secs}s)")
    print(f"  Spreadsheet: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
    print(f"  Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print()


if __name__ == "__main__":
    main()
