#!/usr/bin/env bash
# ============================================================================
# Shoebox Demo — Multi-Level Scanner
# ============================================================================
#
# Demonstrates Phase 6 features:
#   - L1 scan on startup (discovers pre-existing files)
#   - Objects are listable immediately after startup (no upload needed)
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
PORT=9879
ENDPOINT="${SHOEBOX_ENDPOINT:-http://127.0.0.1:$PORT}"

if [[ ! -x "$SHOEBOX" ]]; then
  echo "Error: shoebox binary not found at $SHOEBOX"
  echo "Run 'cargo build --release' first."
  exit 1
fi

# Pre-populate the bucket with files BEFORE starting the server.
echo "Hello from a pre-existing file" > "$BUCKET/readme.txt"

banner "Phase 6 — Multi-Level Scanner"

# =============================================================================
# TEST: Start server
# =============================================================================

step "Start server"
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

ok "Server started"

# =============================================================================
# TEST: Upload via S3 API still works
# =============================================================================

step "Upload via S3 API"
echo "Uploaded via API" | aws --endpoint-url "$ENDPOINT" s3 cp - s3://photos/api-upload.txt
run "aws --endpoint-url $ENDPOINT s3 ls s3://photos/ --recursive"
ok "API uploads work"

# --- Done ---------------------------------------------------------------------

banner "Phase 6 demo complete"
note "Scanner features demonstrated:"
note "  [x] Server starts and serves requests"
note "  [x] S3 API uploads work"
