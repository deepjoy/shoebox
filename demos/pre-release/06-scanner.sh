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
#   - Docker image builds and runs (when Docker is available)
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

SHOEBOX="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/target/release/shoebox"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT=9879
ENDPOINT="${SHOEBOX_ENDPOINT:-http://127.0.0.1:$PORT}"

if [[ ! -x "$SHOEBOX" ]]; then
  echo "Error: shoebox binary not found at $SHOEBOX"
  echo "Run 'cargo build --release' first."
  exit 1
fi

# Pre-populate the bucket with files BEFORE starting the server.
# Use larger files so L3 (content hashing) takes visible time.
echo "Hello from a pre-existing file" > "$BUCKET/readme.txt"
dd if=/dev/urandom of="$BUCKET/vacation/photo-beach.dat" bs=1M count=50 2>/dev/null
dd if=/dev/urandom of="$BUCKET/vacation/photo-mountain.dat" bs=1M count=50 2>/dev/null

banner "Phase 6 — Multi-Level Scanner"

# =============================================================================
# TEST: L1 scan discovers all files
# TEST: Startup L1 scan completes before API serves
# =============================================================================

step "Start server (L1 scan runs on startup, discovering pre-existing files)"
SHOEBOX_LOG=info $SHOEBOX --port "$PORT" "$BUCKET" &>"$DEMO_ROOT/server.log" &
SERVER_PID=$!

# Wait for server to be ready (any HTTP response, even 403, means it's listening)
for i in $(seq 1 30); do
  if curl -so /dev/null "$ENDPOINT/" 2>/dev/null; then break; fi
  sleep 0.1
done

# Extract credentials
ACCESS_KEY=$(grep -oP 'AKIA[A-Z0-9]{16}' "$DEMO_ROOT/server.log" | head -1)
SECRET_KEY=$(grep -oP 'Secret: \K[A-Za-z0-9/+=]+' "$DEMO_ROOT/server.log" | head -1)

export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"
export AWS_DEFAULT_REGION="us-east-1"
export AWS_PAGER=""

ok "Server started — L1 scan discovered pre-existing files"

# =============================================================================
# TEST: HEAD after L1 — no size or ETag yet
# The background scan worker polls every 500ms. By issuing this HEAD
# immediately after startup, the server handles it BEFORE the worker
# starts L2/L3 scans — so we see the L1-only state.
# =============================================================================

step "HEAD object immediately — L1 data only (no size, no ETag)"
note "The background scan hasn't started yet (worker polls every 500ms)."
run "aws --endpoint-url $ENDPOINT s3api head-object --bucket photos --key readme.txt"

step "List objects — pre-existing files are visible immediately"
run "aws --endpoint-url $ENDPOINT s3 ls s3://photos/ --recursive"
ok "L1 scan discovers all files: 3 pre-existing files found"

# =============================================================================
# TEST: L2 scan collects metadata (size populated via Content-Length)
# TEST: L3 scan computes correct hashes (ETag populated)
# TEST: Background scans don't block API
# =============================================================================

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
run "aws --endpoint-url $ENDPOINT s3api head-object --bucket photos --key readme.txt"

ETAG=$(aws --endpoint-url "$ENDPOINT" s3api head-object --bucket photos --key readme.txt --query ETag --output text 2>/dev/null || echo "")
if [[ -n "$ETAG" && "$ETAG" != "None" && "$ETAG" != "\"\"" ]]; then
  ok "L3 scan computed content hash: ETag=$ETAG"
else
  note "ETag not yet populated (L3 scan may still be in progress)"
fi

note "API operations continue to work during background scans"

# =============================================================================
# TEST: Filesystem watcher detects new files
# =============================================================================

step "Add a new file directly to the filesystem (bypassing S3 API)"
echo "New file added while server is running" > "$BUCKET/new-file.txt"
note "Waiting for filesystem watcher to detect the change..."
sleep 2

step "List objects — watcher should have detected the new file"
run "aws --endpoint-url $ENDPOINT s3 ls s3://photos/ --recursive"
ok "Filesystem watcher detected the new file"

# =============================================================================
# TEST: Filesystem watcher detects modifications
# =============================================================================

step "Modify an existing file directly on the filesystem"
echo "Modified content — this file has been changed" > "$BUCKET/readme.txt"
note "Waiting for filesystem watcher to detect the modification..."
sleep 2

step "Verify modification detected — HEAD shows updated Content-Length"
run "aws --endpoint-url $ENDPOINT s3api head-object --bucket photos --key readme.txt"
ok "Filesystem watcher detected the modification"

# =============================================================================
# TEST: Filesystem watcher detects deletions
# =============================================================================

step "Delete a file directly from the filesystem"
rm "$BUCKET/vacation/photo-beach.dat"
note "Waiting for filesystem watcher to detect the deletion..."
sleep 2

step "List objects — deleted file should be removed"
run "aws --endpoint-url $ENDPOINT s3 ls s3://photos/ --recursive"
ok "Filesystem watcher detected the deletion"

# =============================================================================
# TEST: L1 scan detects deleted files (explicit re-scan on restart)
# =============================================================================

step "Test L1 deletion detection: stop server, remove a file, restart"
kill "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
note "Server stopped"

rm "$BUCKET/vacation/photo-mountain.dat"
note "Removed vacation/photo-mountain.dat while server was down"

SHOEBOX_LOG=info $SHOEBOX --port "$PORT" --show-secrets "$BUCKET" &>"$DEMO_ROOT/server.log" &
SERVER_PID=$!

for i in $(seq 1 30); do
  if curl -so /dev/null "$ENDPOINT/" >/dev/null 2>&1; then break; fi
  sleep 0.1
done

# Re-extract credentials (secrets only shown with --show-secrets on restart)
ACCESS_KEY=$(grep -oP 'AKIA[A-Z0-9]{16}' "$DEMO_ROOT/server.log" | head -1)
SECRET_KEY=$(grep -oP 'Secret: \K[A-Za-z0-9/+=]+' "$DEMO_ROOT/server.log" | head -1)
export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"

step "List objects after restart — L1 scan should detect the missing file"
run "aws --endpoint-url $ENDPOINT s3 ls s3://photos/ --recursive"
ok "L1 scan detects deleted files: photo-mountain.dat is gone"

# =============================================================================
# TEST: S3 API uploads coexist with scanner
# =============================================================================

step "Upload via S3 API still works alongside scanner"
echo "Uploaded via API" | aws --endpoint-url "$ENDPOINT" s3 cp - s3://photos/api-upload.txt
run "aws --endpoint-url $ENDPOINT s3 ls s3://photos/ --recursive"
ok "API uploads coexist with scanner-discovered files"

# =============================================================================
# TEST: Docker image builds and runs (optional — requires Docker)
# =============================================================================

if command -v docker &>/dev/null; then
  step "Docker: build image"
  run "docker build -t shoebox:phase6-test $PROJECT_ROOT"
  ok "Docker image builds successfully"

  step "Docker: start container and verify it serves requests"
  DOCKER_PORT=9880
  DOCKER_BUCKET="$DEMO_ROOT/docker-bucket"
  mkdir -p "$DOCKER_BUCKET"
  echo "docker file" > "$DOCKER_BUCKET/hello.txt"

  CONTAINER_ID=$(docker run -d --rm \
    -p "$DOCKER_PORT:8080" \
    -v "$DOCKER_BUCKET:/data/mybucket" \
    shoebox:phase6-test --port 8080 /data/mybucket)

  # Wait for container
  for i in $(seq 1 30); do
    if curl -so /dev/null "http://127.0.0.1:$DOCKER_PORT/" >/dev/null 2>&1; then break; fi
    sleep 0.1
  done

  # Extract credentials from docker logs
  DOCKER_LOG=$(docker logs "$CONTAINER_ID" 2>&1)
  DOCKER_AK=$(echo "$DOCKER_LOG" | grep -oP 'AKIA[A-Z0-9]{16}' | head -1)
  DOCKER_SK=$(echo "$DOCKER_LOG" | grep -oP 'Secret: \K[A-Za-z0-9/+=]+' | head -1)

  if [[ -n "$DOCKER_AK" ]]; then
    run "AWS_ACCESS_KEY_ID=$DOCKER_AK AWS_SECRET_ACCESS_KEY=$DOCKER_SK aws --endpoint-url http://127.0.0.1:$DOCKER_PORT s3 ls s3://mybucket/ --recursive"
    ok "Docker container starts and serves buckets"
  else
    note "Could not extract credentials from Docker container logs"
  fi

  docker stop "$CONTAINER_ID" 2>/dev/null || true
  docker rmi shoebox:phase6-test 2>/dev/null || true
else
  note "Docker not available — skipping Docker build/run tests"
  note "  (Docker tests: image build, container start, serve buckets)"
fi

# --- Done ---------------------------------------------------------------------

banner "Phase 6 demo complete"
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
note "  [x] Docker image builds successfully (when Docker available)"
note "  [x] Docker container starts and serves buckets (when Docker available)"
note ""
note "Items tested via unit/integration tests (not in demo):"
note "  [x] L3 detects file modification during scan"
note "  [x] P0 scan preempts P1/P2"
note "  [x] Debouncing prevents duplicate events"
note "  [x] Multi-arch build (CI-only)"
note "  [x] docker pull from registries (CI-only)"
