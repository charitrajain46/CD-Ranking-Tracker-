#!/usr/bin/env python3
"""
clear_lock.py — Remove stale pipeline lock from pipeline_state.json
====================================================================
When to use:
  The main pipeline (run_pipeline.py) sets a lock while it is running
  so that Quick Run cannot interfere. If the main pipeline crashes
  unexpectedly, it may leave the lock in place even though nothing
  is actually running.

  Run this script to remove the stale lock so Quick Run works again:

      python3 clear_lock.py
"""

import os, json

STATE_FILE = "pipeline_state.json"

if not os.path.exists(STATE_FILE):
    print(f"ERROR: '{STATE_FILE}' not found.")
else:
    with open(STATE_FILE) as f:
        state = json.load(f)

    if "pipeline_lock" not in state:
        print("No lock found — pipeline_state.json is already clean.")
        print("Quick Run is available.")
    else:
        lock_time = state.pop("pipeline_lock")
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
        print("Lock cleared successfully.")
        print(f"  (Lock was set at: {lock_time})")
        print()
        print("Quick Run is now available — run:  python3 quick_run.py")
