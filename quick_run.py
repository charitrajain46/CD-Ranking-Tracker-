#!/usr/bin/env python3
"""
quick_run.py  —  On-demand ranking for any batch or college
============================================================
Usage:  python3 quick_run.py

Two modes:
  [1] BATCH   — rank top 40% from a specific group-of-50
                (e.g. batch 2 = colleges ranked #51–#100 by Add value)
  [2] COLLEGE — rank ALL courses for a specific College ID

Writes results to two dedicated sheets (cleared + rebuilt each run):
  "Quick Run Intermediate"  — keyword rows (same format as Intermediate)
  "Quick Run Final"         — ranked results  (same format as Final)

LOCK: Cannot run while the main automatic pipeline is active.
If the main pipeline is running, Quick Run exits immediately.
"""

import os, re, sys, json, csv, time, socket, random
from datetime import datetime

import gspread
from google.oauth2.credentials       import Credentials
from google_auth_oauthlib.flow       import InstalledAppFlow
from google.auth.transport.requests  import Request
from googleapiclient.discovery       import build
from googleapiclient.errors          import HttpError

socket.setdefaulttimeout(480)


# ══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════

CREDENTIALS_FILE      = "credentials.json"
TOKEN_FILE            = "token.json"
STATE_FILE            = "pipeline_state.json"
QR_BATCH_STATE_FILE   = "qr_batch_state.json"   # persists batch selections
CSV_FILE              = "Colleges_Short_Form.csv"

SUBGROUP_SIZE    = 50      # must match phase1_populate.py
SAMPLE_RATIO     = 0.40    # must match phase1_populate.py

QR_INTER_SHEET   = "Quick Run Intermediate"
QR_FINAL_SHEET   = "Quick Run Final"
QR_RUN_START_COL = 6      # Quick Run Final always uses Run-1 columns (F–R)

POLL_PAUSE_SEC   = 15
MAX_ROUNDS       = 30      # Quick runs are small; 30 rounds is plenty

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/script.projects",
    "https://www.googleapis.com/auth/script.external_request",
    "https://www.googleapis.com/auth/script.scriptapp",
]

INTER_HEADER = [
    "college_id", "course_id", "college_name", "course_name",
    "keyword", "Rank", "Found URL", "updated_at",
]

SILO_RUN_COLS = [
    "Admissions", "Admissions_URL",
    "Fees",       "Fees_URL",
    "Placements", "Placements_URL",
    "Scholarships","Scholarships_URL",
    "Main",       "Main_URL",
    "Single_Course","Single_Course_URL",
    "Updated_at",
]

FINAL_HEADER = [
    "College_Id", "Course_Id", "College_Name", "Course_Name", "Keywords",
] + SILO_RUN_COLS


# ══════════════════════════════════════════════════════════════
#  BANNER / UI HELPERS
# ══════════════════════════════════════════════════════════════

def banner(title):
    print(f"\n{'═'*60}")
    print(f"  {title}")
    print(f"{'═'*60}")

def sep():
    print(f"  {'─'*56}")


# ══════════════════════════════════════════════════════════════
#  STATE + AUTH
# ══════════════════════════════════════════════════════════════

def load_state():
    if not os.path.exists(STATE_FILE):
        print(f"ERROR: '{STATE_FILE}' not found. Run python setup.py first.")
        sys.exit(1)
    with open(STATE_FILE) as f:
        return json.load(f)


def get_credentials():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("  Refreshing token …")
            creds.refresh(Request())
        else:
            if not os.path.exists(CREDENTIALS_FILE):
                print(f"ERROR: '{CREDENTIALS_FILE}' not found.")
                sys.exit(1)
            print("  Opening browser for Google sign-in …")
            flow  = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    return creds


# ══════════════════════════════════════════════════════════════
#  PIPELINE LOCK CHECK
# ══════════════════════════════════════════════════════════════

def check_pipeline_lock(state):
    """
    Exit immediately if the main pipeline is running.
    The main pipeline sets 'pipeline_lock' in pipeline_state.json
    when it starts and clears it when it finishes.
    """
    lock = state.get("pipeline_lock")
    if lock:
        banner("⚠  BLOCKED — Main pipeline is currently running")
        print(f"  Locked since : {lock}")
        print()
        print("  Quick Run cannot run while the automatic pipeline")
        print("  is active. Wait for it to finish, then retry.")
        print()
        print("  If the pipeline crashed and left a stale lock, run:")
        print("    python3 clear_lock.py")
        sys.exit(1)


# ══════════════════════════════════════════════════════════════
#  HELPERS  (shared logic identical to phase1_populate.py)
# ══════════════════════════════════════════════════════════════

def _flex(row_dict, *keys):
    for k in keys:
        for variant in (k, k.lower(), k.upper(), k.replace("_", " ")):
            val = row_dict.get(variant)
            if val is not None and str(val).strip():
                return str(val).strip()
    return ""


def extract_college_short_fallback(name):
    m = re.search(r'\[([^\]]+)\]', name)
    if m:
        return m.group(1).strip()
    clean = name.split(',')[0]
    clean = re.sub(r'\s*[-–]+\s*$', '', clean)
    clean = re.sub(r'[\[\]{}\(\)]', '', clean).strip()
    return clean


def extract_course_short(name):
    matches = re.findall(r'\[([^\]]+)\]', name)
    if matches:
        return ' + '.join(m.strip() for m in matches)
    clean = name.split(',')[0]
    clean = re.sub(r'\{[^}]*\}', '', clean)
    clean = re.sub(r'\([^)]*\)', '', clean)
    clean = re.sub(r'[\[\]]', '', clean).strip()
    return clean


def extract_spec(name):
    groups = re.findall(r'\(([^)]+)\)', name)
    return groups[-1].strip() if groups else None


def build_keyword_rows(college_id, course_id, college_name_full, course_name_full,
                       c_short, crs_short, spec):
    base  = f"{c_short} {crs_short}"
    blank = ["", "", ""]
    rows  = [
        [college_id, course_id, college_name_full, course_name_full, base]                   + blank,
        [college_id, course_id, college_name_full, course_name_full, f"{base} Admissions"]   + blank,
        [college_id, course_id, college_name_full, course_name_full, f"{base} Fees"]         + blank,
        [college_id, course_id, college_name_full, course_name_full, f"{base} Placements"]   + blank,
        [college_id, course_id, college_name_full, course_name_full, f"{base} Scholarships"] + blank,
    ]
    if spec:
        rows.append(
            [college_id, course_id, college_name_full, course_name_full,
             f"{base} ({spec})"] + blank
        )
    return rows


def load_college_short_forms():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), CSV_FILE)
    if not os.path.exists(path):
        return {}
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            mapping = {}
            with open(path, newline="", encoding=encoding) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cid   = str(row.get("College Id", "") or row.get("college_id", "")).strip()
                    short = str(row.get("Short_form",  "") or row.get("short_form",  "")).strip()
                    if cid and short:
                        mapping[cid] = short
            return mapping
        except UnicodeDecodeError:
            continue
        except Exception:
            return {}
    return {}


# ══════════════════════════════════════════════════════════════
#  SOURCE + CONTENT READERS
# ══════════════════════════════════════════════════════════════

def parse_source(sh):
    """Returns {college_id: [record_dict, ...]} — one entry per course row."""
    src_ws  = sh.worksheet("Source")
    src_raw = src_ws.get_all_values()
    if not src_raw:
        print("ERROR: Source sheet is empty.")
        sys.exit(1)
    src_header = src_raw[0]
    all_records = {}
    for row in src_raw[1:]:
        if not any(str(v).strip() for v in row):
            continue
        record = {src_header[i]: (row[i] if i < len(row) else "")
                  for i in range(len(src_header))}
        cid = _flex(record, "college_id", "College_Id", "College_ID")
        if not cid:
            continue
        all_records.setdefault(cid, []).append(record)
    return all_records


def load_add_values(sh):
    """Returns {college_id: float} from Content sheet."""
    try:
        content_ws  = sh.worksheet("Content")
        content_all = content_ws.get_all_values()
        if len(content_all) < 2:
            return {}
        header = [str(h).strip() for h in content_all[0]]

        # Find key column
        key_col = 0
        for i, h in enumerate(header):
            hl = h.lower().replace(" ", "_").replace("-", "_")
            if hl in ("college_id", "collegeid"):
                key_col = i
                break

        # Find Add column
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

        add_values = {}
        for row in content_all[1:]:
            if not row or not any(str(v).strip() for v in row):
                continue
            key = str(row[key_col]).strip() if key_col < len(row) else ""
            if not key:
                continue
            raw = str(row[add_col]).strip().replace(",", "") if add_col < len(row) else "0"
            try:
                val = float(raw) if raw else 0.0
            except ValueError:
                val = 0.0
            add_values[key] = val
        return add_values
    except Exception:
        return {}


# ══════════════════════════════════════════════════════════════
#  BATCH SELECTION PERSISTENCE
# ══════════════════════════════════════════════════════════════

def _qr_batch_state_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), QR_BATCH_STATE_FILE)


def load_qr_batch_state() -> dict:
    """Load saved batch → [(college_id, course_id), ...] mappings."""
    path = _qr_batch_state_path()
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_qr_batch_state(state: dict):
    """Persist batch selections so repeat runs reuse the same colleges."""
    path = _qr_batch_state_path()
    with open(path, "w") as f:
        json.dump(state, f, indent=2)


# ══════════════════════════════════════════════════════════════
#  SELECTION — BATCH MODE
# ══════════════════════════════════════════════════════════════

def select_batch(all_source_records, add_values, batch_num):
    """
    Sort ALL colleges by Add desc → split into groups of 50.
    For batch N: take slice [(N-1)*50 : N*50].

    FIRST RUN of a batch:
        Pick 40% at random from the group → save selection to qr_batch_state.json.
    REPEAT RUN of the same batch:
        Reload the saved (college_id, course_id) pairs so the same colleges
        are ranked again — enabling proper before/after comparison.

    Returns list of (college_id, record_dict).
    """
    all_cids      = list(all_source_records.keys())
    sorted_cids   = sorted(all_cids,
                           key=lambda c: add_values.get(c, 0.0),
                           reverse=True)
    total_batches = (len(sorted_cids) + SUBGROUP_SIZE - 1) // SUBGROUP_SIZE

    if batch_num < 1 or batch_num > total_batches:
        print(f"\n  ⚠  Batch {batch_num} is out of range.")
        print(f"     Valid batches : 1 – {total_batches}")
        print(f"     ({len(sorted_cids)} colleges ÷ {SUBGROUP_SIZE} per group)")
        sys.exit(1)

    start  = (batch_num - 1) * SUBGROUP_SIZE
    end    = min(start + SUBGROUP_SIZE, len(sorted_cids))
    group  = sorted_cids[start:end]
    n_pick = max(1, round(len(group) * SAMPLE_RATIO))

    batch_state = load_qr_batch_state()
    batch_key   = str(batch_num)

    if batch_key in batch_state:
        # ── REPEAT RUN: restore saved selection ───────────────
        saved_pairs = batch_state[batch_key]   # [[college_id, course_id], ...]
        result, missing = [], []

        for college_id, course_id in saved_pairs:
            if college_id not in all_source_records:
                missing.append(college_id)
                continue
            records = all_source_records[college_id]
            # Find the exact saved course; fall back to first course if gone
            rec = next(
                (r for r in records
                 if _flex(r, "course_id", "Course_Id", "Course_ID") == course_id),
                records[0]
            )
            result.append((college_id, rec))

        if missing:
            print(f"\n  ⚠  {len(missing)} saved college(s) no longer in Source (skipped):")
            for m in missing[:5]:
                print(f"     - {m}")
            if len(missing) > 5:
                print(f"     … and {len(missing)-5} more")

        print(f"\n  Batch {batch_num} of {total_batches}  [REPEAT RUN — saved selection reused]:")
        print(f"    Rank range (by Add) : #{start+1} – #{end}")
        print(f"    Saved colleges      : {len(saved_pairs)}")
        print(f"    Available now       : {len(result)}")

        return result

    else:
        # ── FIRST RUN: pick randomly and save ────────────────
        picked_cids = random.sample(group, min(n_pick, len(group)))

        pairs_to_save, result = [], []
        for cid in picked_cids:
            rec       = all_source_records[cid][0]
            course_id = _flex(rec, "course_id", "Course_Id", "Course_ID")
            pairs_to_save.append([cid, course_id])
            result.append((cid, rec))

        batch_state[batch_key] = pairs_to_save
        save_qr_batch_state(batch_state)

        print(f"\n  Batch {batch_num} of {total_batches}  [FIRST RUN — random selection saved]:")
        print(f"    Rank range (by Add)  : #{start+1} – #{end}")
        print(f"    Colleges in group    : {len(group)}")
        print(f"    Randomly selected    : {len(result)}")
        print(f"    Saved to             : {QR_BATCH_STATE_FILE}")
        print(f"    Next run of batch {batch_num} will reuse these same colleges.")

        return result


# ══════════════════════════════════════════════════════════════
#  SELECTION — COLLEGE MODE
# ══════════════════════════════════════════════════════════════

def select_college(all_source_records, college_id):
    """
    Return the FIRST course row for this college_id.
    One college → one course → one set of keyword rows (same as auto pipeline).
    Returns list of (college_id, record_dict) with a single entry.
    """
    cid     = str(college_id).strip()
    records = all_source_records.get(cid)
    if not records:
        print(f"\n  ⚠  College ID '{cid}' not found in Source sheet.")
        print(f"     Double-check the college_id column and try again.")
        sys.exit(1)
    rec   = records[0]   # always pick first course — consistent with auto pipeline
    cname = _flex(rec, "course_name", "Course_Name")
    print(f"\n  College {cid}: {len(records)} course(s) in Source → picking first course.")
    print(f"    Course: {cname or '(no name)'}")
    return [(cid, rec)]


# ══════════════════════════════════════════════════════════════
#  BUILD QUICK RUN SHEETS
# ══════════════════════════════════════════════════════════════

def build_quick_run_sheets(sh, selected, college_short_forms):
    """
    1. Get or create Quick Run Intermediate and Quick Run Final sheets.
    2. CLEAR both sheets completely (wipes previous quick run data).
    3. Build keyword rows → write to Quick Run Intermediate.
    4. Build identity rows → write to Quick Run Final (Run-1 format).
    Returns (qr_inter_ws, keyword_row_count).
    """
    # ── Get or create sheets ──────────────────────────────────
    try:
        qr_inter_ws = sh.worksheet(QR_INTER_SHEET)
        print(f"  '{QR_INTER_SHEET}' → found")
    except gspread.exceptions.WorksheetNotFound:
        qr_inter_ws = sh.add_worksheet(QR_INTER_SHEET, rows=5000, cols=10)
        print(f"  '{QR_INTER_SHEET}' → created")

    try:
        qr_final_ws = sh.worksheet(QR_FINAL_SHEET)
        print(f"  '{QR_FINAL_SHEET}' → found")
    except gspread.exceptions.WorksheetNotFound:
        qr_final_ws = sh.add_worksheet(QR_FINAL_SHEET, rows=5000, cols=20)
        print(f"  '{QR_FINAL_SHEET}' → created")

    # ── Clear both sheets ────────────────────────────────────
    print("\n  Clearing previous Quick Run data …")
    qr_inter_ws.clear()
    qr_final_ws.clear()

    # ── Build rows ───────────────────────────────────────────
    inter_rows = [INTER_HEADER]
    final_rows = [FINAL_HEADER]
    seen_pairs = set()

    for cid, rec in selected:
        college_id   = cid
        course_id    = _flex(rec, "course_id",    "Course_Id",   "Course_ID")
        college_name = _flex(rec, "college_name", "College_Name")
        course_name  = _flex(rec, "course_name",  "Course_Name")
        pair         = (college_id, course_id)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)

        c_short   = (college_short_forms.get(college_id)
                     or extract_college_short_fallback(college_name))
        crs_short = extract_course_short(course_name)
        spec      = extract_spec(course_name)
        keyword   = f"{c_short} {crs_short}"

        inter_rows.extend(build_keyword_rows(
            college_id, course_id, college_name, course_name,
            c_short, crs_short, spec
        ))
        final_rows.append(
            [college_id, course_id, college_name, course_name, keyword]
            + [""] * 13   # 13 blank silo columns (Run-1 block)
        )

    # ── Write in one API call each ────────────────────────────
    qr_inter_ws.update(inter_rows, "A1", value_input_option="USER_ENTERED")
    qr_final_ws.update(final_rows, "A1", value_input_option="USER_ENTERED")

    kw_count = len(inter_rows) - 1
    print(f"  Quick Run Intermediate : {kw_count} keyword rows")
    print(f"  Quick Run Final        : {len(final_rows)-1} identity rows")

    return qr_inter_ws, kw_count


# ══════════════════════════════════════════════════════════════
#  APPS SCRIPT INVOCATION
# ══════════════════════════════════════════════════════════════

def count_unranked(qr_inter_ws):
    """Count keyword rows in Quick Run Intermediate that still have no rank."""
    all_vals = qr_inter_ws.get_all_values()
    count = 0
    for i, row in enumerate(all_vals):
        if i == 0:
            continue
        keyword = str(row[4]).strip() if len(row) > 4 else ""
        rank    = str(row[5]).strip() if len(row) > 5 else ""
        if keyword and not rank:
            count += 1
    return count


def invoke_quick_run_script(script_service, script_id):
    """Call checkQuickRunRanks() in the Apps Script project."""
    try:
        response = script_service.scripts().run(
            scriptId=script_id,
            body={"function": "checkQuickRunRanks", "devMode": False}
        ).execute()
        if response.get("error"):
            err = response["error"]
            print(f"\n  ✗ Apps Script error: {err.get('message', 'unknown')}")
            details = err.get("details", [])
            for d in details:
                msg = d.get("scriptStackTraceElements") or d.get("errorMessage", "")
                if msg:
                    print(f"      {msg}")
            return False
        return True
    except HttpError as e:
        status = e.resp.status if hasattr(e, "resp") else "?"
        print(f"\n  ✗ HTTP {status}: {e}")
        if status == 403:
            print("  → Ensure Apps Script API is enabled and script is deployed.")
        return False
    except Exception as e:
        print(f"\n  ✗ Unexpected error: {e}")
        return False


def run_quick_ranking(script_service, script_id, qr_inter_ws):
    """
    Loop until all Quick Run Intermediate rows are ranked.
    Each Apps Script call runs for up to 5.5 minutes (parallel batches of 10).
    """
    for round_num in range(1, MAX_ROUNDS + 1):
        unranked = count_unranked(qr_inter_ws)
        if unranked == 0:
            print(f"\n  ✓ All rows ranked!")
            return True

        print(f"\n  Round {round_num}/{MAX_ROUNDS} — {unranked} rows still unranked")
        print(f"  Calling checkQuickRunRanks() … (runs ~5.5 min in Apps Script)")
        ok = invoke_quick_run_script(script_service, script_id)
        if not ok:
            return False

        print(f"  Waiting {POLL_PAUSE_SEC}s …")
        time.sleep(POLL_PAUSE_SEC)

        unranked_after = count_unranked(qr_inter_ws)
        print(f"  Progress: {unranked} → {unranked_after} unranked rows")

        if unranked_after == 0:
            print(f"\n  ✓ All rows ranked after round {round_num}!")
            return True
        if unranked_after >= unranked:
            print(f"\n  ⚠ No progress in round {round_num}.")
            print("  Check Apps Script logs: Extensions → Apps Script → Executions")
            return False

        time.sleep(3)

    print(f"\n  ⚠ Reached maximum rounds ({MAX_ROUNDS}).")
    return False


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def main():
    start_time = datetime.now()
    banner(f"QUICK RUN — On-demand Ranking  ({start_time.strftime('%Y-%m-%d %H:%M')})")

    # ── Load state + lock check ───────────────────────────────
    state = load_state()
    check_pipeline_lock(state)

    script_id      = state.get("script_id", "")
    spreadsheet_id = state.get("spreadsheet_id", "")
    if not script_id or not spreadsheet_id:
        print("ERROR: pipeline_state.json missing script_id or spreadsheet_id.")
        print("Run python setup.py first.")
        sys.exit(1)

    # ── Choose mode ───────────────────────────────────────────
    print()
    print("  What do you want to rank?")
    print()
    print("    [1] BATCH   — top 40% from a specific group of 50 colleges")
    print("                  (e.g. batch 2 = colleges ranked #51–#100 by Add)")
    print("    [2] COLLEGE — all courses for a specific College ID")
    print()
    while True:
        choice = input("  Enter 1 or 2: ").strip()
        if choice in ("1", "2"):
            break
        print("  Please enter 1 or 2.")

    mode = "batch" if choice == "1" else "college"

    # ── Authenticate ──────────────────────────────────────────
    print("\nAuthenticating …")
    creds          = get_credentials()
    gc             = gspread.authorize(creds)
    script_service = build("script", "v1", credentials=creds)
    print("  ✓ Authenticated")

    # ── Open spreadsheet ─────────────────────────────────────
    try:
        sh = gc.open_by_key(spreadsheet_id)
        print(f"  ✓ Opened: '{sh.title}'")
    except gspread.exceptions.SpreadsheetNotFound:
        print("ERROR: Spreadsheet not found.")
        sys.exit(1)

    # ── Auto-update Batch column in Source ────────────────────
    # Runs every Quick Run so new colleges are assigned a batch
    # number immediately when added to Source.
    try:
        from update_batches import update_source_batches
        update_source_batches(sh)
    except Exception as _ub_err:
        print(f"  WARNING: Batch column update skipped ({_ub_err})")

    # ── Load Source + Content ─────────────────────────────────
    print("\nReading Source sheet …")
    all_source_records = parse_source(sh)
    print(f"  {len(all_source_records)} colleges in Source")

    print("Reading Add values from Content sheet …")
    add_values = load_add_values(sh)
    print(f"  {len(add_values)} Add values loaded")

    college_short_forms = load_college_short_forms()

    # ── Selection ─────────────────────────────────────────────
    if mode == "batch":
        print()
        total_batches = (len(all_source_records) + SUBGROUP_SIZE - 1) // SUBGROUP_SIZE
        while True:
            try:
                raw = input(f"  Enter batch number (1–{total_batches}): ").strip()
                batch_num = int(raw)
                break
            except ValueError:
                print("  Please enter a number.")
        selected = select_batch(all_source_records, add_values, batch_num)
        run_label = f"Batch {batch_num}"
    else:
        print()
        college_id = input("  Enter College ID: ").strip()
        selected   = select_college(all_source_records, college_id)
        run_label  = f"College {college_id}"

    # ── Confirm ───────────────────────────────────────────────
    kw_est = sum(
        6 if extract_spec(_flex(rec, "course_name", "Course_Name")) else 5
        for _, rec in selected
    )
    print()
    sep()
    print(f"  Run type  : {run_label}")
    print(f"  Colleges  : {len(set(c for c, _ in selected))}")
    print(f"  Keywords  : ~{kw_est} rows")
    print(f"  Sheets    : '{QR_INTER_SHEET}'  +  '{QR_FINAL_SHEET}'")
    print(f"  NOTE      : Any previous Quick Run data will be cleared.")
    sep()
    print()
    confirm = input("  Proceed? [y/N]: ").strip().lower()
    if confirm not in ("y", "yes"):
        print("\n  Cancelled.")
        sys.exit(0)

    # ── Build sheets ──────────────────────────────────────────
    banner("Building Quick Run sheets")
    qr_inter_ws, kw_count = build_quick_run_sheets(sh, selected, college_short_forms)

    # ── Rank ──────────────────────────────────────────────────
    banner("Running Quick Ranking via Apps Script")
    ok = run_quick_ranking(script_service, script_id, qr_inter_ws)

    # ── Summary ───────────────────────────────────────────────
    elapsed = datetime.now() - start_time
    mins    = int(elapsed.total_seconds() // 60)
    secs    = int(elapsed.total_seconds() % 60)

    if ok:
        banner(f"✓ QUICK RUN COMPLETE  ({mins}m {secs}s)")
    else:
        banner(f"⚠ QUICK RUN FINISHED WITH ISSUES  ({mins}m {secs}s)")

    print(f"  Results → '{QR_INTER_SHEET}'  and  '{QR_FINAL_SHEET}' tabs")
    print(f"  Spreadsheet: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
    print()


if __name__ == "__main__":
    main()
