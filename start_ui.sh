#!/bin/bash
# ───────────────────────────────────────────────────────────────────────────
#  start_ui.sh — Start the Collegedunia Ranking Pipeline Web UI
#
#  First-time setup (run ONCE):
#      bash start_ui.sh --install
#
#  Normal start:
#      bash start_ui.sh
#
#  Then open:  http://localhost:5050
# ───────────────────────────────────────────────────────────────────────────

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

if [[ "$1" == "--install" ]]; then
  echo "Installing dependencies…"
  pip3 install flask reportlab --break-system-packages
  echo "✓ Done. Run:  bash start_ui.sh"
  exit 0
fi

# Check flask is available
python3 -c "import flask" 2>/dev/null || {
  echo ""
  echo "  ✗ Flask not installed."
  echo "  Run this once:  bash start_ui.sh --install"
  echo ""
  exit 1
}

echo ""
echo "  ╔══════════════════════════════════════════════════════╗"
echo "  ║  Collegedunia Ranking Pipeline — Web UI             ║"
echo "  ║  http://localhost:5050                               ║"
echo "  ║  Press Ctrl+C to stop                               ║"
echo "  ╚══════════════════════════════════════════════════════╝"
echo ""

python3 app.py
