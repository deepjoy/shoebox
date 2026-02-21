#!/usr/bin/env bash
# ============================================================================
# Shoebox Demo — Common Object Operations
# ============================================================================
#
# Demonstrates Phase 4 features:
#   - CopyObject (same bucket, cross bucket, conditional)
#   - RenameObject (atomic rename via x-shoebox-rename header)
#   - Range requests (bytes=0-N, bytes=-N, bytes=N-, 206 status, 416 on invalid)
#   - Conditional requests (If-Match, If-None-Match, If-Modified-Since,
#     If-Unmodified-Since)
#   - Object tagging (put, get, delete, limit enforcement)
#   - AWS CLI tagging commands
#
# Environment variables:
#   SHOEBOX_ENDPOINT — override the endpoint URL (default: http://127.0.0.1:$PORT)
#
# Record with asciinema:
#   asciinema rec --cols 100 --rows 30 -c './demos/pre-release/04-common-operations.sh' demo.cast
# ============================================================================

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../util/lib.sh"

# --- Setup -------------------------------------------------------------------

DEMO_ROOT="$(mktemp -d)"
trap 'kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; rm -rf "$DEMO_ROOT"' EXIT

BUCKET_A="$DEMO_ROOT/photos"
BUCKET_B="$DEMO_ROOT/archive"
mkdir -p "$BUCKET_A" "$BUCKET_B"

SHOEBOX="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/target/release/shoebox"
PORT=9877
ENDPOINT="${SHOEBOX_ENDPOINT:-http://127.0.0.1:$PORT}"

if [[ ! -x "$SHOEBOX" ]]; then
  echo "Error: shoebox binary not found at $SHOEBOX"
  echo "Run 'cargo build --release' first."
  exit 1
fi

# Helper: signed curl request using curl's built-in AWS SigV4 signing
# Only used for Shoebox-specific extensions (x-shoebox-rename) that have
# no AWS CLI equivalent.
signed_curl() {
  local method="$1"; shift
  local path="$1"; shift

  curl -s "$@" \
    -X "$method" \
    --aws-sigv4 "aws:amz:us-east-1:s3" \
    --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    "${ENDPOINT}${path}"
}

# ── 1. Server startup ──────────────────────────────────────────────────────

banner "Phase 4 — Common Object Operations"

step "Start server with two buckets (photos, archive)"
SHOEBOX_LOG=off "$SHOEBOX" --host 127.0.0.1 --port "$PORT" --show-secrets "$BUCKET_A" "$BUCKET_B" > "$DEMO_ROOT/startup.txt" 2>&1 &
SERVER_PID=$!

for i in $(seq 1 30); do
  if curl -s -o /dev/null "$ENDPOINT/" 2>/dev/null; then
    break
  fi
  sleep 0.1
done

# Extract credentials from photos bucket
ACCESS_KEY=$(grep access_key_id "$BUCKET_A/.shoebox/config.toml" | head -1 | cut -d'"' -f2)
SECRET_KEY=$(grep secret_access_key "$BUCKET_A/.shoebox/config.toml" | head -1 | cut -d'"' -f2)

export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL="$ENDPOINT"

ok "Server started on $ENDPOINT with buckets: photos, archive"
sleep "$DELAY"

# ── 2. Seed test data ─────────────────────────────────────────────────────

banner "Setup — Upload Test Objects"

step "Create test files"
echo "The quick brown fox jumps over the lazy dog" > "$DEMO_ROOT/fox.txt"
dd if=/dev/urandom bs=1024 count=10 of="$DEMO_ROOT/data.bin" 2>/dev/null
note "fox.txt — 45 bytes of text"
note "data.bin — 10 KB of random data"

step "Upload to photos bucket"
run "aws s3 cp $DEMO_ROOT/fox.txt s3://photos/animals/fox.txt"
run "aws s3 cp $DEMO_ROOT/data.bin s3://photos/data.bin"

step "Verify uploads"
run "aws s3 ls s3://photos/ --recursive"
sleep "$DELAY"

# ── Done ─────────────────────────────────────────────────────────────────────

banner "Phase 4 Demo Complete"
note "Phase 4 features will be demonstrated as they are implemented."

sleep "${END_DELAY}"
