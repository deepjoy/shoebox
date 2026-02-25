#!/usr/bin/env bash
# ============================================================================
# Shoebox Demo — Multi-Level Scanner
# ============================================================================
#
# Demonstrates Phase 6 features:
#   - L1 scan on startup (discovers pre-existing files)
#   - L1 scan detects deleted files (re-scan after external removal)
#   - Objects are listable immediately after startup (no upload needed)
#   - L2 background scan populates metadata (size via Content-Length)
#   - L3 background scan computes hashes (ETag populated)
#   - Per-file progress logging during L2 and L3 scans
#   - Filesystem watcher detects new files added externally
#   - Filesystem watcher detects modified files
#   - Filesystem watcher detects deleted files
#   - S3 API operations coexist with scanner-managed objects
#
# Environment variables:
#   SHOEBOX_ENDPOINT — override the endpoint URL (default: http://127.0.0.1:$PORT)
#
# Record with asciinema:
#   asciinema rec --cols 100 --rows 30 -c './demos/pre-release/06-scanner.sh' demo.cast
# ============================================================================

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../util/lib.sh"

# --- Setup -------------------------------------------------------------------

DEMO_ROOT="$(mktemp -d)"
trap 'kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; rm -rf "$DEMO_ROOT"' EXIT

BUCKET="$DEMO_ROOT/photos"
mkdir -p "$BUCKET/vacation"

require_shoebox

PORT=9879
ENDPOINT="${SHOEBOX_ENDPOINT:-http://127.0.0.1:$PORT}"

# Pre-populate the bucket with files BEFORE starting the server.
# Use larger files so L3 (content hashing) takes visible time.
echo "Hello from a pre-existing file" > "$BUCKET/readme.txt"
dd if=/dev/urandom of="$BUCKET/vacation/photo-beach.dat" bs=1M count=50 2>/dev/null
dd if=/dev/urandom of="$BUCKET/vacation/photo-mountain.dat" bs=1M count=50 2>/dev/null

# --- Parts ------------------------------------------------------------------

p01_server_startup() {
  step "Start server (L1 scan runs on startup, discovering pre-existing files)"
  SHOEBOX_LOG=info $SHOEBOX --port "$PORT" "$BUCKET" &>"$DEMO_ROOT/server.log" &
  SERVER_PID=$!

  wait_for_server "$ENDPOINT"

  # Extract credentials
  ACCESS_KEY=$(grep -oP 'AKIA[A-Z0-9]{16}' "$DEMO_ROOT/server.log" | head -1)
  SECRET_KEY=$(grep -oP 'Secret: \K[A-Za-z0-9/+=]+' "$DEMO_ROOT/server.log" | head -1)

  setup_aws_env "$ACCESS_KEY" "$SECRET_KEY" "$ENDPOINT"

  ok "Server started — L1 scan discovered pre-existing files"
}
part p01_server_startup "Phase 6 — Multi-Level Scanner"

p02_l1_verification() {
  step "HEAD object immediately — L1 data only (no size, no ETag)"
  note "The background scan hasn't started yet (worker polls every 500ms)."
  run "aws s3api head-object --bucket photos --key readme.txt"

  step "List objects — pre-existing files are visible immediately"
  run "aws s3 ls s3://photos/ --recursive"
  ok "L1 scan discovers all files: 3 pre-existing files found"
}
part p02_l1_verification "L1 Scan — Immediate Discovery"

p03_background_scans() {
  step "Wait for background L2+L3 scans (per-file progress in server log)"
  note "L2 runs stat() for metadata, then L3 streams each file through MD5+SHA-256."

  # Wait for L3 to finish (polling server log)
  for i in $(seq 1 60); do
    if grep -q 'L3 content-hash scan complete' "$DEMO_ROOT/server.log" 2>/dev/null; then break; fi
    sleep 0.5
  done

  step "Scanner progress (from server log):"
  grep -E 'L[23].*(scan starting|complete)' "$DEMO_ROOT/server.log" | head -20 || true

  step "HEAD object after L2+L3 — size and ETag now populated"
  note "L2 added ContentLength (file size); L3 added ETag (MD5 content hash)."
  run "aws s3api head-object --bucket photos --key readme.txt"

  ETAG=$(aws s3api head-object --bucket photos --key readme.txt --query ETag --output text 2>/dev/null || echo "")
  if [[ -n "$ETAG" && "$ETAG" != "None" && "$ETAG" != "\"\"" ]]; then
    ok "L3 scan computed content hash: ETag=$ETAG"
  else
    note "ETag not yet populated (L3 scan may still be in progress)"
  fi

  note "API operations continue to work during background scans"
}
part p03_background_scans "L2+L3 Background Scans"

p04_watcher_new_files() {
  step "Add a new file directly to the filesystem (bypassing S3 API)"
  echo "New file added while server is running" > "$BUCKET/new-file.txt"
  note "Waiting for filesystem watcher to detect the change..."
  sleep 2

  step "List objects — watcher should have detected the new file"
  run "aws s3 ls s3://photos/ --recursive"
  ok "Filesystem watcher detected the new file"
}
part p04_watcher_new_files "Filesystem Watcher — New Files"

p05_watcher_modifications() {
  step "Modify an existing file directly on the filesystem"
  echo "Modified content — this file has been changed" > "$BUCKET/readme.txt"
  note "Waiting for filesystem watcher to detect the modification..."
  sleep 2

  step "Verify modification detected — HEAD shows updated Content-Length"
  run "aws s3api head-object --bucket photos --key readme.txt"
  ok "Filesystem watcher detected the modification"
}
part p05_watcher_modifications "Filesystem Watcher — Modifications"

p06_watcher_deletions() {
  step "Delete a file directly from the filesystem"
  rm "$BUCKET/vacation/photo-beach.dat"
  note "Waiting for filesystem watcher to detect the deletion..."
  sleep 2

  step "List objects — deleted file should be removed"
  run "aws s3 ls s3://photos/ --recursive"
  ok "Filesystem watcher detected the deletion"
}
part p06_watcher_deletions "Filesystem Watcher — Deletions"

p07_l1_deletion_restart() {
  step "Test L1 deletion detection: stop server, remove a file, restart"
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  note "Server stopped"

  rm "$BUCKET/vacation/photo-mountain.dat"
  note "Removed vacation/photo-mountain.dat while server was down"

  SHOEBOX_LOG=info $SHOEBOX --port "$PORT" --show-secrets "$BUCKET" &>"$DEMO_ROOT/server.log" &
  SERVER_PID=$!

  wait_for_server "$ENDPOINT"

  # Re-extract credentials (secrets only shown with --show-secrets on restart)
  ACCESS_KEY=$(grep -oP 'AKIA[A-Z0-9]{16}' "$DEMO_ROOT/server.log" | head -1)
  SECRET_KEY=$(grep -oP 'Secret: \K[A-Za-z0-9/+=]+' "$DEMO_ROOT/server.log" | head -1)
  setup_aws_env "$ACCESS_KEY" "$SECRET_KEY" "$ENDPOINT"

  step "List objects after restart — L1 scan should detect the missing file"
  run "aws s3 ls s3://photos/ --recursive"
  ok "L1 scan detects deleted files: photo-mountain.dat is gone"
}
part p07_l1_deletion_restart "L1 Deletion Detection — Restart"

p08_api_coexistence() {
  step "Upload via S3 API still works alongside scanner"
  echo "Uploaded via API" | aws s3 cp - s3://photos/api-upload.txt
  run "aws s3 ls s3://photos/ --recursive"
  ok "API uploads coexist with scanner-discovered files"
}
part p08_api_coexistence "S3 API Coexistence"

p99_done() {
  note "Scanner checklist items demonstrated:"
  note "  [x] L1 scan discovers all files"
  note "  [x] L1 scan detects deleted files"
  note "  [x] L2 scan collects metadata (Content-Length)"
  note "  [x] L3 scan computes correct hashes (ETag)"
  note "  [x] Startup L1 scan completes before API serves"
  note "  [x] Background scans don't block API"
  note "  [x] Filesystem watcher detects new files"
  note "  [x] Filesystem watcher detects modifications"
  note "  [x] Filesystem watcher detects deletions"
  note "  [x] S3 API operations coexist with scanner-managed objects"
  note ""
  note "Items tested via unit/integration tests (not in demo):"
  note "  [x] L3 detects file modification during scan"
  note "  [x] P0 scan preempts P1/P2"
  note "  [x] Debouncing prevents duplicate events"
}
part p99_done "Phase 6 Demo Complete"

# --- Run --------------------------------------------------------------------

run_demo
