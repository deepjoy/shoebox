#!/usr/bin/env bash
# ============================================================================
# Shoebox Demo — Multipart Uploads
# ============================================================================
#
# Demonstrates Phase 5 features:
#   - InitiateMultipartUpload
#   - UploadPart
#   - CompleteMultipartUpload
#   - AbortMultipartUpload
#   - ListParts
#   - ListMultipartUploads
#   - Part number validation
#   - ETag verification on complete
#   - Automatic multipart upload with AWS CLI
#
# Environment variables:
#   SHOEBOX_ENDPOINT — override the endpoint URL (default: http://127.0.0.1:$PORT)
#
# Record with asciinema:
#   asciinema rec --cols 100 --rows 30 -c './demos/pre-release/05-multipart.sh' demo.cast
# ============================================================================

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../util/lib.sh"

# --- Setup -------------------------------------------------------------------

DEMO_ROOT="$(mktemp -d)"
trap 'kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; rm -rf "$DEMO_ROOT"' EXIT

BUCKET="$DEMO_ROOT/uploads"
mkdir -p "$BUCKET"

SHOEBOX="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/target/release/shoebox"
PORT=9878
ENDPOINT="${SHOEBOX_ENDPOINT:-http://127.0.0.1:$PORT}"

if [[ ! -x "$SHOEBOX" ]]; then
  echo "Error: shoebox binary not found at $SHOEBOX"
  echo "Run 'cargo build --release' first."
  exit 1
fi

# Helper: signed curl request using curl's built-in AWS SigV4 signing
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

banner "Phase 5 — Multipart Uploads"

step "Start server with uploads bucket"
SHOEBOX_LOG=off "$SHOEBOX" --host 127.0.0.1 --port "$PORT" --show-secrets "$BUCKET" > "$DEMO_ROOT/startup.txt" 2>&1 &
SERVER_PID=$!

for i in $(seq 1 30); do
  if curl -s -o /dev/null "$ENDPOINT/" 2>/dev/null; then
    break
  fi
  sleep 0.1
done

# Extract credentials
ACCESS_KEY=$(grep access_key_id "$BUCKET/.shoebox/config.toml" | head -1 | cut -d'"' -f2)
SECRET_KEY=$(grep secret_access_key "$BUCKET/.shoebox/config.toml" | head -1 | cut -d'"' -f2)

export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL="$ENDPOINT"

ok "Server started on $ENDPOINT with bucket: uploads"
sleep "$DELAY"

# ── 2. Initiate + Upload Parts ────────────────────────────────────────────

banner "Initiate + Upload Parts"

step "Create test file (split into 3 parts)"
echo "Part 1: The quick brown fox" > "$DEMO_ROOT/part1.txt"
echo "Part 2: jumps over the lazy" > "$DEMO_ROOT/part2.txt"
echo "Part 3: dog." > "$DEMO_ROOT/part3.txt"

step "Initiate multipart upload"
INITIATE_RESPONSE=$(signed_curl POST "/uploads/test-file.txt?uploads" -H "content-type: text/plain")
UPLOAD_ID=$(echo "$INITIATE_RESPONSE" | grep -o '<UploadId>[^<]*</UploadId>' | sed 's/<[^>]*>//g')
echo "  Upload ID: $UPLOAD_ID"
ok "Multipart upload initiated"

step "Upload Part 1"
ETAG1=$(signed_curl PUT "/uploads/test-file.txt?partNumber=1&uploadId=$UPLOAD_ID" \
  -d @"$DEMO_ROOT/part1.txt" -D- | grep -i '^etag:' | cut -d' ' -f2 | tr -d '\r')
echo "  ETag: $ETAG1"
ok "Part 1 uploaded"

step "Upload Part 2"
ETAG2=$(signed_curl PUT "/uploads/test-file.txt?partNumber=2&uploadId=$UPLOAD_ID" \
  -d @"$DEMO_ROOT/part2.txt" -D- | grep -i '^etag:' | cut -d' ' -f2 | tr -d '\r')
echo "  ETag: $ETAG2"
ok "Part 2 uploaded"

step "Upload Part 3"
ETAG3=$(signed_curl PUT "/uploads/test-file.txt?partNumber=3&uploadId=$UPLOAD_ID" \
  -d @"$DEMO_ROOT/part3.txt" -D- | grep -i '^etag:' | cut -d' ' -f2 | tr -d '\r')
echo "  ETag: $ETAG3"
ok "Part 3 uploaded"

# Clean up — abort the upload since Complete isn't wired yet
signed_curl DELETE "/uploads/test-file.txt?uploadId=$UPLOAD_ID" > /dev/null 2>&1 || true

sleep "$DELAY"

# ── Done ───────────────────────────────────────────────────────────────────

banner "Phase 5 Demo Complete"
note "All Phase 5 features demonstrated so far:"
note "  - InitiateMultipartUpload creates upload ID"
note "  - UploadPart uploads individual parts with ETags"

sleep "${END_DELAY}"
