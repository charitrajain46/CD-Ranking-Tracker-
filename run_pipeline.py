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

import os, sys, json, time, subprocess, socket, atexit, smtplib, threading
from email.mime.text import MIMEText
from datetime import datetime, date, timedelta, timezone

import gspread
from google.oauth2.credentials       import Credentials
from google_auth_oauthlib.flow       import InstalledAppFlow
from google.auth.transport.requests  import Request
from googleapiclient.discovery       import build
from googleapiclient.errors          import HttpError

# Apps Script runs for up to 6 minutes — set global socket timeout to 8 minutes
socket.setdefaulttimeout(480)

# IST = UTC+5:30
_IST = timezone(timedelta(hours=5, minutes=30))
def now_ist() -> datetime:
    """Return current time in IST (works correctly on UTC servers like PythonAnywhere)."""
    return datetime.now(timezone.utc).astimezone(_IST)


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


def col_letter(n: int) -> str:
    """1-based column number → Excel letter (1→A, 27→AA …)."""
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def detect_silo(keyword: str) -> str:
    """Identify which ranking silo a keyword belongs to."""
    kw = keyword.strip()
    if kw.endswith(" Admissions"):   return "Admissions"
    if kw.endswith(" Fees"):         return "Fees"
    if kw.endswith(" Placements"):   return "Placements"
    if kw.endswith(" Scholarships"): return "Scholarships"
    if kw.endswith(")"):             return "Single_Course"
    return "Main"


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


# Offset from run_start_col (1-based) to each silo's rank column
SILO_OFFSETS = {
    "Admissions":    0,
    "Fees":          2,
    "Placements":    4,
    "Scholarships":  6,
    "Main":          8,
    "Single_Course": 10,
}


def mark_stuck_rows(inter_ws: gspread.Worksheet,
                    final_ws,
                    run_start_col: int) -> None:
    """
    Write NOT_FOUND to every Intermediate row that has a keyword but no rank,
    and to the corresponding silo column in the Final sheet for this run.
    Called only after Apps Script has made no progress for MAX_NO_PROGRESS rounds.
    """
    all_vals = inter_ws.get_all_values()
    stuck = []   # (inter_sheet_row_1based, cid, crs_id, keyword)

    for i, row in enumerate(all_vals):
        if i == 0:
            continue
        keyword = str(row[4]).strip() if len(row) > 4 else ""
        rank    = str(row[5]).strip() if len(row) > 5 else ""
        if keyword and not rank:
            cid    = str(row[0]).strip()
            crs_id = str(row[1]).strip()
            stuck.append((i + 1, cid, crs_id, keyword))

    if not stuck:
        return

    print(f"\n  Marking {len(stuck)} stuck rows as NOT_FOUND …")

    # ── Intermediate: write NOT_FOUND to column F ─────────────────
    inter_updates = [
        {"range": f"F{sr}", "values": [["NOT_FOUND"]]}
        for sr, _, _, _ in stuck
    ]
    try:
        inter_ws.batch_update(inter_updates)
        print(f"  ✓ {len(inter_updates)} Intermediate rows marked NOT_FOUND.")
    except Exception as e:
        print(f"  ⚠ Could not update Intermediate: {e}")

    kws    = [kw for _, _, _, kw in stuck]
    sample = kws[:10]
    print(f"  Stuck keywords: {sample}" + (" …" if len(kws) > 10 else ""))

    # ── Final: write NOT_FOUND to current run's silo columns ──────
    if final_ws is None or run_start_col is None:
        return

    try:
        final_all = final_ws.get_all_values()
    except Exception as e:
        print(f"  ⚠ Could not read Final sheet: {e}")
        return

    # (cid, crs_id) → set of silo names that are stuck
    stuck_silos: dict = {}
    for _, cid, crs_id, kw in stuck:
        silo = detect_silo(kw)
        stuck_silos.setdefault((cid, crs_id), set()).add(silo)

    final_updates = []
    for i, row in enumerate(final_all):
        if i == 0:
            continue
        cid    = str(row[0]).strip()
        crs_id = str(row[1]).strip()
        pair   = (cid, crs_id)
        if pair not in stuck_silos:
            continue
        for silo in stuck_silos[pair]:
            offset = SILO_OFFSETS.get(silo)
            if offset is None:
                continue
            col_0 = (run_start_col - 1) + offset   # 0-based column index
            val   = str(row[col_0]).strip() if col_0 < len(row) else ""
            if not val:   # only fill empty cells
                final_updates.append(
                    {"range": f"{col_letter(col_0 + 1)}{i + 1}",
                     "values": [["NOT_FOUND"]]}
                )

    if final_updates:
        try:
            final_ws.batch_update(final_updates)
            print(f"  ✓ {len(final_updates)} Final sheet cells marked NOT_FOUND.")
        except Exception as e:
            print(f"  ⚠ Could not update Final sheet: {e}")
    else:
        print("  No empty Final cells to mark.")


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

    A keepalive thread prints a progress line every 25 s while the blocking
    HTTP call is in-flight.  This prevents SSE connections (via the web UI)
    from going silent for >5 min and being closed by PythonAnywhere / proxies.
    """
    _stop = threading.Event()

    def _keepalive():
        elapsed = 0
        while not _stop.wait(25):
            elapsed += 25
            print(f"  … Apps Script still running ({elapsed}s elapsed) …", flush=True)

    ka_thread = threading.Thread(target=_keepalive, daemon=True)
    ka_thread.start()

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

    finally:
        _stop.set()
        ka_thread.join(timeout=1)


def run_stage2(script_service, script_id: str, inter_ws: gspread.Worksheet,
               final_ws=None, run_start_col: int = None) -> bool:
    banner("STAGE 2 — Running Apps Script ranking (via Execution API)")

    # Allow up to 3 consecutive rounds with no progress before giving up.
    # This tolerates Apps Script calls that get Canceled early (connection drops,
    # transient errors) without aborting the whole pipeline prematurely.
    MAX_NO_PROGRESS = 3
    no_progress_streak = 0

    for round_num in range(1, MAX_ROUNDS + 1):
        unranked = count_unranked_rows(inter_ws)

        if unranked == 0:
            print(f"\n  ✓ All rows ranked — Apps Script stage complete.")
            return True

        print(f"\n  Round {round_num}/{MAX_ROUNDS} — {unranked} rows still need ranking")
        print(f"  Calling checkRanksBatched() … (runs for ~5.5 min inside Apps Script)")

        ok = invoke_apps_script(script_service, script_id)
        if not ok:
            print("\n  Aborting ranking stage due to Apps Script API error.")
            return False

        print(f"  Apps Script call returned.  Waiting {POLL_PAUSE_SEC}s …")
        time.sleep(POLL_PAUSE_SEC)

        unranked_after = count_unranked_rows(inter_ws)
        print(f"  Progress: {unranked} → {unranked_after} unranked rows")

        if unranked_after == 0:
            print(f"\n  ✓ All rows ranked after round {round_num}!")
            return True

        if unranked_after >= unranked:
            # No progress this round — Apps Script may have been Canceled early
            # or is still running async. Wait 6 min and recheck.
            no_progress_streak += 1
            print(f"\n  ⚠ No progress in round {round_num} "
                  f"(streak: {no_progress_streak}/{MAX_NO_PROGRESS}). "
                  f"Waiting 6 min for async completion …")
            time.sleep(360)

            unranked_retry = count_unranked_rows(inter_ws)
            print(f"  Retry check: {unranked} → {unranked_retry} unranked rows")

            if unranked_retry == 0:
                print(f"\n  ✓ All rows ranked (Apps Script completed asynchronously)!")
                return True

            if unranked_retry < unranked:
                print(f"\n  Progress detected after wait — continuing …")
                no_progress_streak = 0   # reset streak on any progress
                continue

            # Still no progress after wait
            if no_progress_streak >= MAX_NO_PROGRESS:
                remaining = count_unranked_rows(inter_ws)
                print(f"\n  ⚠ No progress for {MAX_NO_PROGRESS} consecutive rounds.")
                print(f"  {remaining} rows appear permanently stuck — Apps Script cannot")
                print(f"  rank them (likely empty keywords or no search results).")
                mark_stuck_rows(inter_ws, final_ws, run_start_col)
                print(f"  Proceeding to Stage 3 with partial results.")
                return True   # proceed to Stage 3 — don't throw away the good results

            print(f"  Retrying round {round_num + 1} …")
            continue

        # Progress made — reset streak and loop for next round
        no_progress_streak = 0
        print(f"  {unranked_after} rows still unranked — starting round {round_num + 1} …")
        time.sleep(3)

    remaining = count_unranked_rows(inter_ws)
    print(f"\n  ⚠ Reached maximum rounds ({MAX_ROUNDS}). "
          f"{remaining} rows still unranked.")
    print("  Proceeding to Stage 3 with partial results.")
    return True   # proceed instead of aborting


# ══════════════════════════════════════════════════════════════
#  FINAL SHEET QUALITY CHECK
# ══════════════════════════════════════════════════════════════

def check_final_sheet(gc, spreadsheet_id: str, run_number: int = 1) -> tuple:
    """
    Read the Final sheet and check if all rank columns for the current run are populated.
    NOT_FOUND is treated as a valid filled value (Apps Script tried, no result found).

    Returns:
        ("passed",  summary_string)  — all rows have ranks (or NOT_FOUND)
        ("partial", summary_string)  — some rows still missing ranks
        ("failed",  summary_string)  — sheet empty or all ranks missing
    """
    # Compute 0-based rank column indices for this run.
    # Only check the 5 mandatory silos — Single_Course (offset 10) is optional:
    # not every college has a specialization keyword, so that column is
    # legitimately empty and should not count against the pass/fail result.
    # run 1 → [5, 7, 9, 11, 13]   (Admissions, Fees, Placements, Scholarships, Main)
    # run 2 → [18, 20, 22, 24, 26]  etc.
    run_start_0idx   = 5 + (run_number - 1) * 13
    rank_col_indices = [run_start_0idx + i * 2 for i in range(5)]

    try:
        sh       = gc.open_by_key(spreadsheet_id)
        final_ws = sh.worksheet("Final")
        rows     = final_ws.get_all_values()
    except Exception as e:
        return ("failed", f"Could not read Final sheet: {e}")

    data_rows = [r for r in rows[1:] if any(str(v).strip() for v in r)]  # skip header + blanks

    if not data_rows:
        return ("failed", "Final sheet is empty — no rows written.")

    total     = len(data_rows)
    complete  = 0
    missing   = 0

    for row in data_rows:
        row_missing = False
        for ci in rank_col_indices:
            val = str(row[ci]).strip() if ci < len(row) else ""
            if not val:           # empty → rank not yet fetched
                row_missing = True
                break
        if row_missing:
            missing += 1
        else:
            complete += 1

    pct = round(complete / total * 100) if total else 0
    summary = f"{complete}/{total} rows fully ranked ({pct}%)"

    if missing == 0:
        return ("passed", summary)
    elif complete > 0:
        return ("partial", f"{summary} — {missing} rows missing ranks")
    else:
        return ("failed", f"{summary} — no ranks populated")


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
#  EMAIL NOTIFICATION
# ══════════════════════════════════════════════════════════════

UI_CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ui_config.json")

def _load_email_config(state: dict) -> dict:
    """
    Build a single email config dict by merging ui_config.json (Email & Distribution tab)
    with pipeline_state.json fields.  ui_config.json takes priority for SMTP settings
    since the user configured it via the UI.
    """
    # Hard-coded recipients (same as pipeline_cron.yml)
    FIXED_RECIPIENTS = [
        "charitra.jain@collegedunia.com",   # testing — add others once confirmed working
        # "shivang.p@collegedunia.com",
        # "shivang.singh@collegedunia.com",
        # "anurag.priyadarshan@collegedunia.com",
    ]

    cfg = {
        "smtp_host":        "smtp.gmail.com",
        "smtp_port":        465,
        "smtp_user":        "",
        "smtp_password":    "",
        "smtp_from":        "",
        "to_list":          FIXED_RECIPIENTS,   # always send to these
        "sendgrid_api_key": "",                 # fallback when SMTP is blocked
    }

    # 1. Try ui_config.json (set via the Email & Distribution tab in the UI)
    if os.path.exists(UI_CONFIG_FILE):
        try:
            with open(UI_CONFIG_FILE) as f:
                ui = json.load(f)
            if ui.get("smtp_host"):   cfg["smtp_host"]     = ui["smtp_host"]
            if ui.get("smtp_port"):   cfg["smtp_port"]      = int(ui["smtp_port"])
            if ui.get("smtp_user"):   cfg["smtp_user"]      = ui["smtp_user"]
            if ui.get("smtp_password"): cfg["smtp_password"]= ui["smtp_password"]
            if ui.get("smtp_from"):   cfg["smtp_from"]      = ui["smtp_from"]
            if ui.get("sendgrid_api_key"): cfg["sendgrid_api_key"] = ui["sendgrid_api_key"]
            recs = ui.get("recipients", [])
            if recs:
                extra = [r["email"] for r in recs if r.get("email")]
                # Merge with fixed recipients (no duplicates)
                cfg["to_list"] = list(dict.fromkeys(FIXED_RECIPIENTS + extra))
        except Exception:
            pass

    # 2. Fall back to pipeline_state.json fields if UI config is incomplete
    if not cfg["smtp_user"] and state.get("notify_email"):
        cfg["smtp_user"]     = state["notify_email"]
        cfg["smtp_password"] = state.get("notify_email_password", "")
        cfg["smtp_from"]     = state["notify_email"]

    # Always ensure fixed recipients are in the list
    cfg["to_list"] = list(dict.fromkeys(FIXED_RECIPIENTS + cfg["to_list"]))

    return cfg


def _send_via_sendgrid(to_list: list, subject: str, body: str, from_email: str, api_key: str) -> None:
    """Send email via SendGrid HTTP API — works on PythonAnywhere free tier (no SMTP needed)."""
    import urllib.request, urllib.error
    payload = json.dumps({
        "personalizations": [{"to": [{"email": addr} for addr in to_list]}],
        "from":             {"email": from_email},
        "subject":          subject,
        "content":          [{"type": "text/plain", "value": body}],
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data    = payload,
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type":  "application/json",
        },
        method  = "POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        if resp.status not in (200, 202):
            raise RuntimeError(f"SendGrid returned HTTP {resp.status}")


def send_pipeline_email(state: dict, status: str, elapsed_str: str, spreadsheet_id: str) -> None:
    """Send email notification after pipeline run — always called regardless of outcome."""
    # Reload state from disk (phase1 may have updated it during the run)
    try:
        with open(STATE_FILE) as f:
            state = json.load(f)
    except Exception:
        pass

    cfg = _load_email_config(state)

    if not cfg["smtp_user"] or not cfg["smtp_password"]:
        print("  ⚠ Email not configured — skipping notification.")
        print("    Configure SMTP in the Email & Distribution tab of the UI, or run setup.py.")
        return
    if not cfg["to_list"]:
        print("  ⚠ No email recipients configured — skipping notification.")
        return

    icon      = {"passed": "✅", "skipped": "⏭", "partial": "⚠️"}.get(status, "❌")
    sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit?usp=sharing"
    label     = {"passed": "Passed", "skipped": "Skipped", "partial": "Partial", "failed": "Failed"}.get(status, status.capitalize())
    subject   = f"Pipeline Run {icon} {label} — {now_ist().strftime('%d-%m-%Y %H:%M IST')}"
    body      = f"""Pipeline run completed.

Status   : {status}
Run date : {now_ist().strftime('%d-%m-%Y %H:%M IST')}
Duration : {elapsed_str}

📊 View ranking results in Google Sheet:
{sheet_url}
"""
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"]    = cfg["smtp_from"] or cfg["smtp_user"]
        msg["To"]      = ", ".join(cfg["to_list"])

        port = int(cfg["smtp_port"])
        if port == 465:
            with smtplib.SMTP_SSL(cfg["smtp_host"], 465) as srv:
                srv.login(cfg["smtp_user"], cfg["smtp_password"])
                srv.sendmail(msg["From"], cfg["to_list"], msg.as_string())
        else:
            with smtplib.SMTP(cfg["smtp_host"], port) as srv:
                srv.ehlo(); srv.starttls()
                srv.login(cfg["smtp_user"], cfg["smtp_password"])
                srv.sendmail(msg["From"], cfg["to_list"], msg.as_string())

        print(f"  ✓ Email sent → {', '.join(cfg['to_list'])}")
    except Exception as e:
        err_str = str(e)
        # PythonAnywhere free tier blocks SMTP (Errno 101 / Network unreachable)
        # → fall back to SendGrid HTTP API which is always allowed
        if "101" in err_str or "unreachable" in err_str.lower() or "connect" in err_str.lower():
            sg_key = cfg.get("sendgrid_api_key", "")
            if sg_key:
                try:
                    from_addr = cfg["smtp_from"] or cfg["smtp_user"]
                    _send_via_sendgrid(cfg["to_list"], subject, body, from_addr, sg_key)
                    print(f"  ✓ Email sent via SendGrid → {', '.join(cfg['to_list'])}")
                    return
                except Exception as sg_err:
                    print(f"  ⚠ SendGrid fallback also failed: {sg_err}")
            else:
                print("  ⚠ SMTP blocked (Errno 101). Add 'sendgrid_api_key' to ui_config.json to enable SendGrid fallback.")
        print(f"  ⚠ Email failed: {e}")


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def main():
    start_time     = datetime.now()
    final_status   = "failed"    # updated to "success" or "skipped" on clean exit
    spreadsheet_id = ""
    is_auto        = "--auto" in sys.argv   # midnight scheduler passes this flag

    banner(f"Collegedunia Rank Pipeline — FULL RUN  ({now_ist().strftime('%d-%m-%Y %H:%M IST')})")

    # ── Load & validate ───────────────────────────────────────
    state = load_state()
    validate_state(state)

    script_id      = state["script_id"]
    spreadsheet_id = state["spreadsheet_id"]

    # ── Set pipeline lock (blocks quick_run.py) ───────────────
    set_pipeline_lock(state)
    atexit.register(clear_pipeline_lock)   # clears lock even on crash

    try:
        # ── Authenticate ──────────────────────────────────────────
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

        # ── Block manual runs when Intermediate sheet already has data ──
        if not is_auto:
            all_rows = inter_ws.get_all_values()
            data_rows = [r for r in all_rows[1:] if any(c.strip() for c in r)]
            if data_rows:
                banner("⏭  MANUAL RUN BLOCKED — Sheets already have data")
                print("  The Intermediate sheet still has data from the last run.")
                print("  Clear the Intermediate and Final sheets in Google Sheets first,")
                print("  then run the pipeline again.")
                print()
                sys.exit(0)

        # ── Build Apps Script API service (socket timeout = 8 min) ──
        script_service = build("script", "v1", credentials=creds)

        # ── Stage 1 — Populate Intermediate ──────────────────────
        ok = run_stage1()
        if ok is None:
            # Nothing to rank today — clean exit, not an error
            elapsed = datetime.now() - start_time
            elapsed_str = f"{int(elapsed.total_seconds() // 60)}m {int(elapsed.total_seconds() % 60)}s"
            banner(f"✓ PIPELINE SKIPPED — Nothing to rank today  ({elapsed_str})")
            print(f"  Finished at: {now_ist().strftime('%d-%m-%Y %H:%M IST')}")
            print()
            final_status = "skipped"
            send_pipeline_email(state, "skipped", elapsed_str, spreadsheet_id)
            sys.exit(0)
        if not ok:
            print("\nPipeline aborted at Stage 1.")
            sys.exit(1)

        # ── Reload state — phase1 writes run_number during its run ──
        try:
            with open(STATE_FILE) as _sf:
                state = json.load(_sf)
        except Exception:
            pass
        run_number    = state.get("run_number", 1)
        run_start_col = 5 + (run_number - 1) * 13 + 1
        print(f"\n  Run #{run_number}  |  Final rank columns start at col {run_start_col}")

        # Open Final sheet so mark_stuck_rows can write NOT_FOUND there
        try:
            final_ws_stage2 = sh.worksheet("Final")
        except gspread.exceptions.WorksheetNotFound:
            final_ws_stage2 = None

        # ── Stage 2 — Run Apps Script ranking ────────────────────
        ok = run_stage2(script_service, script_id, inter_ws,
                        final_ws=final_ws_stage2, run_start_col=run_start_col)
        if not ok:
            # Only hits here on a real Apps Script API error (HTTP 403, 500, etc.)
            # Stuck/unranked rows cause run_stage2 to return True (partial), not False.
            print("\nRanking stage failed due to an Apps Script API error.")
            print("Check the error above, then run  python run_pipeline.py  again.")
            sys.exit(1)

        # ── Stage 3 — Build Content tab ──────────────────────────
        ok = run_stage3()
        if not ok:
            print("\nPipeline aborted at Stage 3.")
            sys.exit(1)

        # ── Done — check Final sheet quality ─────────────────────
        elapsed = datetime.now() - start_time
        mins    = int(elapsed.total_seconds() // 60)
        secs    = int(elapsed.total_seconds() % 60)
        elapsed_str = f"{mins}m {secs}s"

        print("\nChecking Final sheet quality …")
        sheet_status, sheet_summary = check_final_sheet(gc, spreadsheet_id, run_number)
        print(f"  Final sheet: {sheet_summary}")

        if sheet_status == "passed":
            banner(f"✓ PIPELINE PASSED  (took {elapsed_str})")
            final_status = "passed"
        elif sheet_status == "partial":
            banner(f"⚠ PIPELINE PARTIAL  (took {elapsed_str})")
            final_status = "partial"
        else:
            banner(f"✗ PIPELINE FAILED — Final sheet incomplete  (took {elapsed_str})")
            final_status = "failed"

        print(f"  Final sheet : {sheet_summary}")
        print(f"  Spreadsheet : https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
        print(f"  Finished at : {now_ist().strftime('%d-%m-%Y %H:%M IST')}")
        print()

    finally:
        # ── Always send email (success / skipped / failed) ────────
        elapsed = datetime.now() - start_time
        elapsed_str = f"{int(elapsed.total_seconds() // 60)}m {int(elapsed.total_seconds() % 60)}s"
        # Only send if not already sent for "skipped" path above
        if final_status != "skipped":
            send_pipeline_email(state, final_status, elapsed_str, spreadsheet_id)



if __name__ == "__main__":
    main()
