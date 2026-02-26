#!/usr/bin/env bash
# auto_redeploy.sh — pull the latest code and restart the service if it changed.
#
# Usage (run manually after git commands):
#   sudo bash /opt/brain-acad-lab-2/arbitrage_dashboard/scripts/auto_redeploy.sh
#
# Or add to a cron job to poll every minute:
#   * * * * * /opt/brain-acad-lab-2/arbitrage_dashboard/scripts/auto_redeploy.sh >> /var/log/arb_redeploy.log 2>&1
#
# Environment variables (optional, override defaults):
#   ARB_REPO_DIR   — path to the git repo        (default: /opt/brain-acad-lab-2)
#   ARB_BRANCH     — branch to track             (default: copilot/update-coin-refresh-process)
#   ARB_SERVICE    — systemd service name         (default: arbitrage)
#   ARB_VENV       — path to Python venv          (default: $ARB_REPO_DIR/venv)
#   ARB_APP_DIR    — path to app.py directory     (default: $ARB_REPO_DIR/arbitrage_dashboard)

set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
REPO_DIR="${ARB_REPO_DIR:-/opt/brain-acad-lab-2}"
BRANCH="${ARB_BRANCH:-copilot/update-coin-refresh-process}"
SERVICE="${ARB_SERVICE:-arbitrage}"
VENV="${ARB_VENV:-$REPO_DIR/arbitrage_dashboard/.venv}"
APP_DIR="${ARB_APP_DIR:-$REPO_DIR/arbitrage_dashboard}"
TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"

# ── Helper ───────────────────────────────────────────────────────────────────
log() { echo "[$TIMESTAMP] $*"; }

# ── Main ─────────────────────────────────────────────────────────────────────
cd "$REPO_DIR"

# Record commit before pull
BEFORE="$(git rev-parse HEAD 2>/dev/null || echo 'none')"

# Fetch all branches and prune stale remote refs
git fetch --all --prune --quiet

# Check out the target branch (no-op if already on it)
git checkout "$BRANCH" --quiet 2>/dev/null || true

# Fast-forward only — never create a merge commit
if ! git pull --ff-only --quiet origin "$BRANCH" 2>/dev/null; then
    log "WARNING: git pull --ff-only failed (diverged?). Skipping redeploy."
    exit 1
fi

# Record commit after pull
AFTER="$(git rev-parse HEAD 2>/dev/null || echo 'none')"

if [ "$BEFORE" = "$AFTER" ]; then
    log "No changes (HEAD=$AFTER). Service not restarted."
    exit 0
fi

log "New commit detected: $BEFORE -> $AFTER"
log "$(git log -1 --oneline)"

# Install/update Python dependencies
log "Installing Python dependencies..."
"$VENV/bin/pip" install --quiet -r "$APP_DIR/requirements.txt"

# Restart the systemd service
log "Restarting service '$SERVICE'..."
systemctl restart "$SERVICE"

# Wait up to 10 seconds for the service to become active
for i in $(seq 1 10); do
    sleep 1
    STATUS="$(systemctl is-active "$SERVICE" 2>/dev/null || echo 'unknown')"
    if [ "$STATUS" = "active" ]; then
        log "Service '$SERVICE' is running. Redeploy complete."
        exit 0
    fi
done

log "WARNING: Service '$SERVICE' did not become active after restart. Check: journalctl -u $SERVICE -n 50"
exit 1
