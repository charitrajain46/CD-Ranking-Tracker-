#!/usr/bin/env python3
"""
phase1_populate.py  ─  PHASE 1 : SOURCE  →  INTERMEDIATE + FINAL
=================================================================
15-DAY CYCLE:
  Every college is re-ranked every CYCLE_DAYS (15) days.
  "Due" = never ranked OR last ranked >= CYCLE_DAYS days ago.
  Tracked via updated_at column (col H) in Intermediate.

SMART SELECTION (replaces random 40%):
  Due colleges sorted descending by "Add (Search Volume and Traffic)"
  from the Content sheet.
  Split into groups of SUBGROUP_SIZE (50).
  Pick top SAMPLE_RATIO (40%) from each group.
  Stop when MAX_DAILY_COLLEGES (550) reached.

NEW COLLEGE HANDLING:
  New colleges are eligible ONLY on the very first run (empty Intermediate).
  Once colleges exist in Intermediate the tracked set is LOCKED — subsequent
  runs re-rank those colleges only.  New Source additions are ignored unless
  you add them manually via Quick Run (college mode).

DELETION SYNC:
  Colleges removed from Source are auto-removed from Intermediate + Final.

AUTO-SCHEDULING:
  Adds a daily midnight cron job (0 0 * * *) on first run.
  The pipeline then runs automatically every night at 12:00 AM.
"""

import os, re, sys, json, subprocess, csv
from datetime import date, datetime, timedelta

import gspread
from google.oauth2.credentials      import Credentials
from google_auth_oauthlib.flow      import InstalledAppFlow
from google.auth.transport.requests import Request


# ══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════

CREDENTIALS_FILE   = "credentials.json"
TOKEN_FILE         = "token.json"
STATE_FILE         = "pipeline_state.json"
CSV_FILE           = "Colleges_Short_Form.csv"

MAX_DAILY_COLLEGES = 550    # max college-course pairs ranked per day
SUBGROUP_SIZE      = 50     # sub-batch size for smart selection
SAMPLE_RATIO       = 0.40   # pick top 40% from each sub-batch
CYCLE_DAYS         = 15     # re-rank every N days

# Exit codes
EXIT_OK            = 0
EXIT_NOTHING_TODAY = 2   # graceful: nothing to rank (all within cycle)
EXIT_ERROR         = 1

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

INTER_HEADER = [
    "college_id", "course_id", "college_name", "course_name",
    "keyword", "Rank", "Found URL", "updated_at",
]

SILO_RUN_COLS = [
    "Admissions", "Admissions_URL",
    "Fees", "Fees_URL",
    "Placements", "Placements_URL",
    "Scholarships", "Scholarships_URL",
    "Main", "Main_URL",
    "Single_Course", "Single_Course_URL",
    "Updated_at",
]

FINAL_BASE_HEADER = [
    "College_Id", "Course_Id", "College_Name", "Course_Name", "Keywords",
] + SILO_RUN_COLS


# ══════════════════════════════════════════════════════════════
#  UTILITY HELPERS
# ══════════════════════════════════════════════════════════════

def col_letter(n):
    """1-based column number → Excel letter (1→A, 27→AA …)."""
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def detect_silo_py(keyword):
    kw = keyword.strip()
    if kw.endswith(" Admissions"):   return "Admissions"
    if kw.endswith(" Fees"):         return "Fees"
    if kw.endswith(" Placements"):   return "Placements"
    if kw.endswith(" Scholarships"): return "Scholarships"
    if kw.endswith(")"):             return "Single_Course"
    return "Main"


def _flex(row_dict, *keys):
    """Return first non-empty value from dict matching any of the given keys."""
    for k in keys:
        for variant in (k, k.lower(), k.upper(), k.replace("_", " ")):
            val = row_dict.get(variant)
            if val is not None and str(val).strip():
                return str(val).strip()
    return ""


def make_contiguous_ranges(row_numbers, col_start="F", col_end="H"):
    """
    Convert a list of sheet row numbers into minimal range strings.
    e.g. [2,3,4,7,8] → ["F2:H4", "F7:H8"]
    """
    if not row_numbers:
        return []
    rows   = sorted(set(row_numbers))
    ranges = []
    start  = end = rows[0]
    for r in rows[1:]:
        if r == end + 1:
            end = r
        else:
            ranges.append(f"{col_start}{start}:{col_end}{end}")
            start = end = r
    ranges.append(f"{col_start}{start}:{col_end}{end}")
    return ranges


# ══════════════════════════════════════════════════════════════
#  AUTH & STATE
# ══════════════════════════════════════════════════════════════

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
                sys.exit(EXIT_ERROR)
            print("  Opening browser for Google sign-in …")
            flow  = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
        print(f"  Token cached → {TOKEN_FILE}")
    return creds


def load_state():
    if not os.path.exists(STATE_FILE):
        print(f"ERROR: '{STATE_FILE}' not found. Run python setup.py first.")
        sys.exit(EXIT_ERROR)
    with open(STATE_FILE) as f:
        return json.load(f)


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def validate_sheet_ids(state):
    if not state.get("spreadsheet_id"):
        print("ERROR: 'spreadsheet_id' missing from pipeline_state.json.")
        sys.exit(EXIT_ERROR)


# ══════════════════════════════════════════════════════════════
#  COLLEGE SHORT FORMS  (CSV)
# ══════════════════════════════════════════════════════════════

def load_college_short_forms():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), CSV_FILE)
    if not os.path.exists(path):
        print(f"  WARNING: '{CSV_FILE}' not found — using name fallback.")
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
            print(f"  Loaded {len(mapping)} college short forms.")
            return mapping
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"  WARNING: Could not read CSV ({e}) — using name fallback.")
            return {}
    print(f"  WARNING: Could not decode '{CSV_FILE}' — using name fallback.")
    return {}


# ══════════════════════════════════════════════════════════════
#  CONTENT SHEET — ADD VALUES
# ══════════════════════════════════════════════════════════════

def load_add_values(sh):
    """
    Read Content sheet and return {college_id: add_value (float)}.

    Scans the header row to find:
      • Key column  : looks for 'college_id' first; falls back to column A.
      • Add column  : looks for a header containing 'add' AND
                      ('search' OR 'traffic' OR 'volume').

    Returns empty dict on any failure — safe fallback (all colleges get Add=0,
    so selection degrades gracefully to top-of-Source order).
    """
    try:
        content_ws  = sh.worksheet("Content")
        content_all = content_ws.get_all_values()
        if len(content_all) < 2:
            print("  WARNING: Content sheet has no data — Add=0 for all colleges.")
            return {}

        header = [str(h).strip() for h in content_all[0]]

        # ── Find College_Id column ────────────────────────────
        key_col = None
        for i, h in enumerate(header):
            hl = h.lower().replace(" ", "_").replace("-", "_")
            if hl in ("college_id", "collegeid", "college_i_d"):
                key_col = i
                break
        if key_col is None:
            key_col = 0   # default: column A
            print("  Content sheet: no 'College_Id' header found — using column A as key.")
        else:
            print(f"  Content sheet: key column = '{header[key_col]}' (col {key_col + 1}).")

        # ── Find Add column ───────────────────────────────────
        add_col = None
        for i, h in enumerate(header):
            hl = h.lower()
            if "add" in hl and ("search" in hl or "traffic" in hl or "volume" in hl):
                add_col = i
                break
        if add_col is None:
            # Fallback: last non-empty header column
            for i in range(len(header) - 1, -1, -1):
                if header[i].strip():
                    add_col = i
                    break
        if add_col is None:
            print("  WARNING: Could not find Add column in Content sheet — Add=0 for all.")
            return {}

        print(f"  Content sheet: Add column = '{header[add_col]}' (col {add_col + 1}).")

        # ── Build lookup dict ─────────────────────────────────
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

        print(f"  Content sheet: {len(add_values)} Add values loaded.")
        return add_values

    except gspread.exceptions.WorksheetNotFound:
        print("  WARNING: 'Content' tab not found — Add=0 for all colleges.")
        return {}
    except Exception as e:
        print(f"  WARNING: Content sheet error ({e}) — Add=0 for all colleges.")
        return {}


# ══════════════════════════════════════════════════════════════
#  15-DAY CYCLE — LAST RANKED DATE
# ══════════════════════════════════════════════════════════════

def get_last_ranked_dates(data_rows):
    """
    Scan Intermediate data rows and return {college_id: last_ranked_date}.
    Uses updated_at column (index 7 = col H).
    Only counts rows that have been actually ranked (updated_at not empty).
    """
    last_ranked = {}   # {college_id: date}
    for row in data_rows:
        cid     = str(row[0]).strip() if len(row) > 0 else ""
        updated = str(row[7]).strip() if len(row) > 7 else ""
        if not cid or not updated:
            continue
        try:
            d = datetime.strptime(updated[:10], "%Y-%m-%d").date()
            if cid not in last_ranked or d > last_ranked[cid]:
                last_ranked[cid] = d
        except ValueError:
            continue
    return last_ranked


# ══════════════════════════════════════════════════════════════
#  SMART SELECTION
# ══════════════════════════════════════════════════════════════

def smart_select(due_list, add_values, max_count):
    """
    Select up to max_count items from due_list using the tiered approach:
      1. Sort all by Add value descending (highest traffic first).
      2. Split into groups of SUBGROUP_SIZE (50).
      3. Pick top SAMPLE_RATIO (40%) from each group → ensures coverage
         across ALL traffic tiers, not just the absolute top colleges.
      4. Stop when max_count reached.

    due_list  : list of (college_id, source_record_dict)
    add_values: {college_id: float}  — from Content sheet
    Returns   : list of (college_id, source_record_dict)
    """
    # Sort descending by Add value; unknown colleges get 0 (go to end)
    ranked = sorted(
        due_list,
        key=lambda x: add_values.get(x[0], 0.0),
        reverse=True,
    )

    selected = []
    for i in range(0, len(ranked), SUBGROUP_SIZE):
        group  = ranked[i : i + SUBGROUP_SIZE]
        n_pick = max(1, round(len(group) * SAMPLE_RATIO))
        selected.extend(group[:n_pick])
        if len(selected) >= max_count:
            selected = selected[:max_count]
            break

    return selected


# ══════════════════════════════════════════════════════════════
#  NAME HELPERS
# ══════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════
#  KEYWORD ROW BUILDER
# ══════════════════════════════════════════════════════════════

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
            [college_id, course_id, college_name_full, course_name_full, f"{base} ({spec})"] + blank
        )
    return rows


# ══════════════════════════════════════════════════════════════
#  DELETION SYNC
# ══════════════════════════════════════════════════════════════

def sync_deletions(inter_ws, data_rows, final_ws, final_all, all_source_cids):
    """
    Delete rows from Intermediate and Final whose college_id is no longer in Source.
    Deletes bottom-to-top to keep row indices stable.
    Returns (updated_data_rows, n_inter_deleted, n_final_deleted).
    """
    # ── Intermediate ──────────────────────────────────────────
    inter_del = [
        i + 2   # +1 for header, +1 for 0-based → 1-based sheet row
        for i, row in enumerate(data_rows)
        if row and str(row[0]).strip() and str(row[0]).strip() not in all_source_cids
    ]
    for sheet_row in sorted(inter_del, reverse=True):
        inter_ws.delete_rows(sheet_row)
    updated_data_rows = [
        r for r in data_rows
        if not (r and str(r[0]).strip() and str(r[0]).strip() not in all_source_cids)
    ]

    # ── Final ──────────────────────────────────────────────────
    final_del = []
    if final_ws is not None and final_all:
        final_del = [
            i + 1   # row 0 of final_all = sheet row 1
            for i, row in enumerate(final_all)
            if i > 0 and row and str(row[0]).strip()
               and str(row[0]).strip() not in all_source_cids
        ]
        for sheet_row in sorted(final_del, reverse=True):
            final_ws.delete_rows(sheet_row)

    return updated_data_rows, len(inter_del), len(final_del)


# ══════════════════════════════════════════════════════════════
#  CHECKPOINT CLEANUP  (one-time legacy removal)
# ══════════════════════════════════════════════════════════════

def remove_checkpoint_rows(src_ws, src_raw):
    """
    Delete any '---CHECKPOINT---' rows from Source.
    The 15-day cycle replaces the checkpoint system entirely.
    """
    marker   = "---CHECKPOINT---"
    to_delete = []
    for i, row in enumerate(src_raw):
        if i == 0:
            continue
        if row and str(row[0]).strip() == marker:
            to_delete.append(i + 1)   # 1-based sheet row

    if to_delete:
        for sheet_row in sorted(to_delete, reverse=True):
            src_ws.delete_rows(sheet_row)
        print(f"  Removed {len(to_delete)} legacy checkpoint row(s) from Source.")

    return len(to_delete)


# ══════════════════════════════════════════════════════════════
#  DAILY RUN GUARD
# ══════════════════════════════════════════════════════════════

def show_already_ran_message(today_str, last_run_str=None):
    now           = datetime.now()
    next_midnight = datetime(now.year, now.month, now.day) + timedelta(days=1)
    delta         = next_midnight - now
    hours         = int(delta.total_seconds() // 3600)
    minutes       = int((delta.total_seconds() % 3600) // 60)

    print("=" * 60)
    print(f"  Already ran today ({today_str}) — skipping.")
    print("=" * 60)

    # Compute next batch due date from last run + cycle
    if last_run_str:
        try:
            last_run_date   = datetime.strptime(last_run_str, "%Y-%m-%d").date()
            next_batch_date = last_run_date + timedelta(days=CYCLE_DAYS)
            days_until      = (next_batch_date - date.today()).days
            print(f"\n  Next batch re-run  : in {days_until} day(s)"
                  f"  ->  {next_batch_date.strftime('%a, %Y-%m-%d')}  at 12:00 AM")
        except ValueError:
            pass

    print(f"  Tonight's auto-run : 12:00 AM tomorrow"
          f"  ({hours}h {minutes}m from now)")
    print(f"\n  To force re-run today: edit '{STATE_FILE}'")
    print(f"  and change 'phase1_last_run' to yesterday's date.")


# ══════════════════════════════════════════════════════════════
#  MIDNIGHT CRON SETUP
# ══════════════════════════════════════════════════════════════

def ensure_midnight_cron():
    """
    Add a daily midnight cron entry for this pipeline if not already present.
    Uses a unique tag comment to detect duplicates.
    Safe to call on every run — only writes crontab once.
    """
    tag = "# article-ranking-pipeline-auto"
    try:
        script_dir  = os.path.dirname(os.path.abspath(__file__))
        python_path = sys.executable
        log_path    = os.path.join(script_dir, "pipeline.log")

        # Check existing crontab
        result   = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        existing = result.stdout if result.returncode == 0 else ""

        if tag in existing:
            print("  ✓ Midnight cron already set up — no change needed.")
            return

        cron_line = (
            f"0 0 * * *  cd {script_dir} && "
            f"{python_path} run_pipeline.py >> {log_path} 2>&1  {tag}"
        )
        new_crontab = existing.rstrip("\n") + "\n" + cron_line + "\n"
        subprocess.run(["crontab", "-"], input=new_crontab, text=True, check=True)

        print("  ✓ Daily midnight cron job created.")
        print(f"    Schedule  : 0 0 * * *  (every day at 12:00 AM)")
        print(f"    Log file  : {log_path}")

    except Exception as e:
        script_dir  = os.path.dirname(os.path.abspath(__file__))
        python_path = sys.executable
        print(f"  WARNING: Could not write crontab automatically ({e}).")
        print(f"  Set it up manually — run:  crontab -e")
        print(f"  Then add this line:")
        print(f"    0 0 * * *  cd {script_dir} && "
              f"{python_path} run_pipeline.py >> pipeline.log 2>&1  {tag}")


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def main():
    today = date.today()

    # ── [0] Load & clean state ────────────────────────────────
    print("\n[0/6] Loading pipeline state …")
    state = load_state()
    validate_sheet_ids(state)
    # Remove all legacy fields from old batch system
    for old_key in ("batches", "next_batch_to_rank", "locked_source_ids"):
        state.pop(old_key, None)

    # ── [1] College short forms ───────────────────────────────
    print("\n[1/6] Loading college short forms …")
    college_short_forms = load_college_short_forms()

    # ── [2] Authenticate + open sheets ───────────────────────
    print("\n[2/6] Authenticating …")
    creds = get_credentials()
    gc    = gspread.authorize(creds)
    print("  ✓ Authenticated")

    print("\n[3/6] Opening spreadsheet and tabs …")
    try:
        sh = gc.open_by_key(state["spreadsheet_id"])
        print(f"  Spreadsheet → '{sh.title}'")
    except gspread.exceptions.SpreadsheetNotFound:
        print("ERROR: Spreadsheet not found — re-run setup.py.")
        sys.exit(EXIT_ERROR)

    src_ws   = sh.worksheet("Source")
    inter_ws = sh.worksheet("Intermediate")
    print("  Source + Intermediate → found")

    try:
        final_ws = sh.worksheet("Final")
        print("  Final → found")
    except gspread.exceptions.WorksheetNotFound:
        final_ws = None
        print("  Final → not found (will be created)")

    # ── Fresh-start check ─────────────────────────────────────
    # If BOTH Intermediate and Final are empty (no data rows),
    # the pipeline has never run — bypass the daily run guard
    # and start from scratch regardless of phase1_last_run date.
    def _sheet_is_empty(ws):
        if ws is None:
            return True
        vals = ws.get_all_values()
        # Empty = no rows at all, OR only a header row with no data below it
        data_rows = [r for r in vals[1:] if any(str(v).strip() for v in r)]
        return len(data_rows) == 0

    inter_empty = _sheet_is_empty(inter_ws)
    final_empty = _sheet_is_empty(final_ws)

    if inter_empty and final_empty:
        print("\n  ⚡ Both Intermediate and Final sheets are empty.")
        print("  Starting from scratch — daily run guard bypassed.")
        state.pop("phase1_last_run", None)   # clear stale date in memory only
    else:
        # Daily run guard — phase1 runs at most once per calendar day
        if state.get("phase1_last_run") == today.isoformat():
            show_already_ran_message(today.isoformat(), state.get("phase1_last_run"))
            sys.exit(EXIT_NOTHING_TODAY)

    # ── [3] Read all sheets ───────────────────────────────────
    print("\n[4/6] Reading all sheets …")

    # Source
    src_raw = src_ws.get_all_values()
    if not src_raw:
        print("ERROR: Source sheet is empty.")
        sys.exit(EXIT_ERROR)
    src_header = src_raw[0]

    # Remove legacy checkpoint rows (one-time cleanup)
    n_removed = remove_checkpoint_rows(src_ws, src_raw)
    if n_removed:
        src_raw    = src_ws.get_all_values()   # re-read after cleanup
        src_header = src_raw[0]

    # Parse all Source records → {college_id: [records]}
    all_source_records = {}   # {cid: [record_dict, ...]}
    all_source_cids    = set()
    for row in src_raw[1:]:
        if not any(str(v).strip() for v in row):
            continue
        record = {src_header[i]: (row[i] if i < len(row) else "")
                  for i in range(len(src_header))}
        cid = _flex(record, "college_id", "College_Id", "College_ID")
        if not cid:
            continue
        all_source_cids.add(cid)
        all_source_records.setdefault(cid, []).append(record)

    print(f"  Source       : {len(all_source_cids)} unique colleges")

    # Intermediate
    inter_all  = inter_ws.get_all_values()
    has_header = len(inter_all) > 0 and any(v.strip() for v in inter_all[0])
    data_start = 1 if has_header else 0
    data_rows  = [r for r in inter_all[data_start:] if any(v.strip() for v in r)]
    inter_cids = set(str(r[0]).strip() for r in data_rows if r and str(r[0]).strip())
    print(f"  Intermediate : {len(data_rows)} rows  ({len(inter_cids)} colleges)")

    # Final
    final_all = final_ws.get_all_values() if final_ws else []

    # Content — Add values
    print("\n[5/6] Loading Add values from Content sheet …")
    add_values = load_add_values(sh)

    # ── [4] Deletion sync ─────────────────────────────────────
    print("\n[6/6] Syncing deletions, selecting, and updating …")

    deleted_cids = inter_cids - all_source_cids
    if deleted_cids:
        print(f"  {len(deleted_cids)} college(s) removed from Source → syncing …")
        data_rows, n_idel, n_fdel = sync_deletions(
            inter_ws, data_rows, final_ws, final_all, all_source_cids
        )
        inter_cids -= deleted_cids
        print(f"  ✓ Removed {n_idel} rows from Intermediate, {n_fdel} from Final.")
    else:
        print("  No deletions needed.")

    # ── [5] 15-day cycle: determine due colleges ──────────────
    last_ranked = get_last_ranked_dates(data_rows)

    new_cids  = all_source_cids - inter_cids   # never been in Intermediate
    due_new   = []   # (cid, record) — brand new colleges
    due_re    = []   # (cid, record) — existing colleges due for re-rank

    for cid in sorted(all_source_cids):   # sorted for determinism
        records = all_source_records.get(cid, [])
        if not records:
            continue
        record = records[0]   # pick first listed course for this college

        if cid in new_cids:
            due_new.append((cid, record))
        elif cid in inter_cids:
            lr = last_ranked.get(cid)
            if lr is None or (today - lr).days >= CYCLE_DAYS:
                due_re.append((cid, record))

    print(f"\n  New (never ranked)     : {len(due_new)}")
    print(f"  Due for re-rank (≥{CYCLE_DAYS}d) : {len(due_re)}")
    print(f"  Not yet due            : "
          f"{len(inter_cids) - len(due_re)} colleges (ranked < {CYCLE_DAYS} days ago)")

    # ── [6] Smart selection ───────────────────────────────────
    # LOCK RULE: Once any colleges exist in Intermediate (pipeline has run
    # before), do NOT auto-add new Source colleges. The tracked set is fixed
    # to whatever was selected on the first run.
    # New colleges only enter the pool on the very first run (empty Intermediate).
    if inter_cids:
        all_due     = due_re   # subsequent run — locked to existing colleges only
        new_in_pool = []
        if due_new:
            print(f"  {len(due_new)} new Source college(s) exist but are SKIPPED"
                  f" — pipeline is locked to the initial selection.")
    else:
        all_due     = due_new + due_re   # first run — all Source colleges eligible
        new_in_pool = due_new

    selected = smart_select(all_due, add_values, MAX_DAILY_COLLEGES)
    selected_cids = set(cid for cid, _ in selected)
    selected_new  = [cid for cid in selected_cids if cid in new_cids]
    selected_re   = [cid for cid in selected_cids if cid in inter_cids]

    # Unselected new colleges — only meaningful on the first run
    unselected_new = [(cid, rec) for cid, rec in new_in_pool if cid not in selected_cids]

    print(f"\n  Selected today : {len(selected)} colleges"
          f"  ({len(selected_new)} new + {len(selected_re)} re-rank)"
          f"  [max {MAX_DAILY_COLLEGES}]")
    if unselected_new:
        print(f"  Not selected   : {len(unselected_new)} colleges"
              f"  (will be picked in next available run)")

    # Nothing to do at all?
    if not selected:
        now           = datetime.now()
        next_midnight = datetime(now.year, now.month, now.day) + timedelta(days=1)
        delta         = next_midnight - now
        hours         = int(delta.total_seconds() // 3600)
        minutes       = int((delta.total_seconds() % 3600) // 60)

        print("=" * 60)
        print(f"  No batch generated today — all colleges within the {CYCLE_DAYS}-day cycle.")
        print("=" * 60)

        # Find the earliest date a college will become due
        if last_ranked:
            next_due_date = min(d + timedelta(days=CYCLE_DAYS) for d in last_ranked.values())
            days_until    = (next_due_date - today).days
            if days_until <= 0:
                print(f"\n  Next batch due     : today  ({next_due_date.strftime('%a, %Y-%m-%d')})")
            else:
                print(f"\n  Next batch due     : in {days_until} day(s)"
                      f"  ->  {next_due_date.strftime('%a, %Y-%m-%d')}  at 12:00 AM")
        else:
            print(f"\n  Next batch due     : in {CYCLE_DAYS} days (no ranking dates found)")

        print(f"  Tonight's auto-run : 12:00 AM tomorrow  ({hours}h {minutes}m from now)")

        state["phase1_last_run"] = today.isoformat()
        save_state(state)
        print("\n  Setting up midnight auto-run …")
        ensure_midnight_cron()
        sys.exit(EXIT_NOTHING_TODAY)

    # ── Determine run number + Final column group ─────────────
    current_run = 1
    if final_ws is not None and final_all:
        fh        = final_all[0] if final_all else []
        non_empty = [h for h in fh if str(h).strip()]
        n_groups  = max(0, (len(non_empty) - 5) // 13)
        if n_groups == 0:
            current_run = 1
        else:
            last_upd_idx    = 5 + n_groups * 13 - 1
            last_group_used = any(
                last_upd_idx < len(r) and str(r[last_upd_idx]).strip()
                for r in final_all[1:]
            )
            current_run = n_groups + 1 if last_group_used else n_groups

    run_start_col = 5 + (current_run - 1) * 13 + 1
    print(f"\n  Run #{current_run}  |  Final column group: "
          f"{col_letter(run_start_col)}–{col_letter(run_start_col + 12)}")

    # ── Clear rank cells for re-ranking existing colleges ─────
    if selected_re:
        rows_to_clear = [
            i + 2   # +1 header, +1 0-based
            for i, row in enumerate(data_rows)
            if row and str(row[0]).strip() in selected_re
        ]
        ranges = make_contiguous_ranges(rows_to_clear)
        if ranges:
            CHUNK = 500
            for chunk_start in range(0, len(ranges), CHUNK):
                inter_ws.batch_clear(ranges[chunk_start : chunk_start + CHUNK])
            print(f"  Cleared rank data for {len(selected_re)} re-ranking colleges "
                  f"({len(rows_to_clear)} rows, {len(ranges)} range(s)).")

    # ── Append keyword rows for newly selected colleges ───────
    new_inter_rows = []
    no_csv_miss    = 0

    for cid, rec in selected:
        if cid not in new_cids:
            continue   # existing college — ranks cleared above
        college_id   = cid
        course_id    = _flex(rec, "course_id",    "Course_Id",   "Course_ID")
        college_name = _flex(rec, "college_name", "College_Name")
        course_name  = _flex(rec, "course_name",  "Course_Name")

        if college_id in college_short_forms:
            c_short = college_short_forms[college_id]
        else:
            c_short     = extract_college_short_fallback(college_name)
            no_csv_miss += 1

        new_inter_rows.extend(build_keyword_rows(
            college_id, course_id, college_name, course_name,
            c_short,
            extract_course_short(course_name),
            extract_spec(course_name),
        ))

    if new_inter_rows:
        if not has_header:
            inter_ws.append_row(INTER_HEADER, value_input_option="USER_ENTERED")
            print("  Header written to Intermediate.")
        inter_ws.append_rows(new_inter_rows, value_input_option="USER_ENTERED")
        print(f"  {len(new_inter_rows)} keyword rows appended for "
              f"{len(selected_new)} newly selected colleges.")

    # ── Update Final tab ──────────────────────────────────────
    print("\n  Updating Final tab …")
    try:
        if final_ws is None:
            final_ws = sh.add_worksheet("Final", rows=2000, cols=100)
            print("  Created 'Final' tab.")
    except Exception:
        try:
            final_ws = sh.worksheet("Final")
        except gspread.exceptions.WorksheetNotFound:
            print("  WARNING: Could not create/find Final tab — skipping.")
            final_ws = None

    if final_ws is not None:
        # Write header for Run 1 if sheet has no header yet
        if current_run == 1:
            existing_row1 = final_ws.row_values(1)
            if not any(str(v).strip() for v in existing_row1):
                end_col = col_letter(len(FINAL_BASE_HEADER))
                final_ws.update([FINAL_BASE_HEADER], f"A1:{end_col}1")
                print("  Run 1 header written to Final.")

        # Extend header for this run (run 2+)
        elif current_run > 1:
            final_header_row = final_ws.row_values(1)
            expected_cols    = 5 + (current_run - 1) * 13
            while len(final_header_row) < expected_cols:
                final_header_row.append("")
            final_header_row = final_header_row[:expected_cols] + SILO_RUN_COLS
            end_col = col_letter(len(final_header_row))
            final_ws.update([final_header_row], f"A1:{end_col}1")
            print(f"  Run {current_run} header added: "
                  f"{col_letter(run_start_col)}–{end_col}")

        # Add Final identity rows ONLY for colleges selected for ranking today.
        # Unselected colleges are NOT added — Final only grows when a college is ranked.
        fresh_final  = final_ws.get_all_values()
        final_pairs  = set(
            (str(r[0]).strip(), str(r[1]).strip())
            for i, r in enumerate(fresh_final)
            if i > 0 and len(r) >= 2
        )
        seen_pairs   = set()
        final_new    = []

        for cid, rec in selected:
            if cid not in new_cids:
                continue   # existing college already has a Final row
            college_id   = cid
            course_id    = _flex(rec, "course_id",    "Course_Id",   "Course_ID")
            college_name = _flex(rec, "college_name", "College_Name")
            course_name  = _flex(rec, "course_name",  "Course_Name")
            pair         = (college_id, course_id)

            if pair in final_pairs or pair in seen_pairs:
                continue
            seen_pairs.add(pair)

            if college_id in college_short_forms:
                c_short = college_short_forms[college_id]
            else:
                c_short = extract_college_short_fallback(college_name)
            keyword = f"{c_short} {extract_course_short(course_name)}"

            final_new.append(
                [college_id, course_id, college_name, course_name, keyword]
                + [""] * (current_run * 13)
            )

        if final_new:
            final_ws.append_rows(final_new, value_input_option="USER_ENTERED")
            print(f"  {len(final_new)} new identity rows added to Final.")
        else:
            print("  Final already up to date — no new rows needed.")

        # Write run_start_col to Source!Z1 for Apps Script
        if src_ws.col_count < 26:
            src_ws.resize(cols=26)
        src_ws.update([[run_start_col]], "Z1")
        print(f"  run_start_col = {run_start_col} written to Source!Z1.")

    # ── Save state ────────────────────────────────────────────
    state["run_number"]      = current_run
    state["phase1_last_run"] = today.isoformat()
    save_state(state)

    # ── Set up midnight cron ──────────────────────────────────
    print("\n  Setting up midnight auto-run …")
    ensure_midnight_cron()

    # ── Summary ───────────────────────────────────────────────
    total_inter = len(data_rows) + len(new_inter_rows)
    print(f"\n{'─'*60}")
    print(f"  Run #{current_run}  |  Final cols: "
          f"{col_letter(run_start_col)}–{col_letter(run_start_col + 12)}")
    print(f"  Due pool               : {len(all_due)} colleges")
    print(f"  Selected for ranking   : {len(selected)}"
          f"  ({len(selected_re)} re-rank + {len(selected_new)} new)")
    print(f"  Not selected today     : {len(unselected_new)} (queued for next run)")
    if no_csv_miss:
        print(f"  CSV short-form misses  : {no_csv_miss}")
    print(f"  Intermediate total rows: {total_inter}")
    print(f"  Next auto-run          : tomorrow 12:00 AM (cron)")
    print(f"{'─'*60}")
    print(f"\n  Done! Apps Script will rank {total_inter} Intermediate rows.")


if __name__ == "__main__":
    main()
