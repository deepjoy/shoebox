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

# ── 3. CopyObject — same bucket ───────────────────────────────────────────

banner "CopyObject — Same Bucket"

step "Copy fox.txt to backup/fox-copy.txt within photos"
run "aws s3 cp s3://photos/animals/fox.txt s3://photos/backup/fox-copy.txt"

step "Verify both files exist"
run "aws s3 ls s3://photos/ --recursive"
ok "CopyObject within same bucket works"
sleep "$DELAY"

# ── 4. CopyObject — cross bucket ─────────────────────────────────────────

banner "CopyObject — Cross Bucket"

step "Copy fox.txt from photos to archive bucket"
note "Using archive bucket credentials to authorize destination write"
ARCHIVE_KEY=$(grep access_key_id "$BUCKET_B/.shoebox/config.toml" | head -1 | cut -d'"' -f2)
ARCHIVE_SECRET=$(grep secret_access_key "$BUCKET_B/.shoebox/config.toml" | head -1 | cut -d'"' -f2)
export AWS_ACCESS_KEY_ID="$ARCHIVE_KEY"
export AWS_SECRET_ACCESS_KEY="$ARCHIVE_SECRET"
run "aws s3api copy-object --bucket archive --key fox-archived.txt --copy-source photos/animals/fox.txt"
ok "Cross-bucket copy succeeded"

step "Verify file in archive bucket"
run "aws s3 ls s3://archive/ --recursive"
ok "CopyObject across buckets works"

# Switch back to photos credentials
export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"
sleep "$DELAY"

# ── 5. CopyObject — conditional headers ──────────────────────────────────

banner "CopyObject — Conditional Headers"

step "Get ETag of source file"
ETAG=$(aws s3api head-object --bucket photos --key animals/fox.txt --query ETag --output text 2>/dev/null || echo '""')
note "ETag: $ETAG"

step "Copy with matching --copy-source-if-match (should succeed)"
run "aws s3api copy-object --bucket photos --key conditional-copy.txt --copy-source photos/animals/fox.txt --copy-source-if-match '$ETAG'"
ok "Conditional copy with matching ETag succeeded"

step "Copy with wrong --copy-source-if-match (should fail 412)"
RESULT=$(aws s3api copy-object --bucket photos --key should-not-exist.txt \
  --copy-source "photos/animals/fox.txt" \
  --copy-source-if-match '"wrong-etag"' 2>&1 || true)
note "$RESULT"
if echo "$RESULT" | grep -qi "PreconditionFailed\|Precondition\|412"; then
  ok "Conditional copy with wrong ETag correctly returned 412"
fi
sleep "$DELAY"

# ── Done ─────────────────────────────────────────────────────────────────────

banner "Phase 4 Demo Complete"
note "All Phase 4 features demonstrated so far:"
note "  - CopyObject within same bucket"
note "  - CopyObject across buckets"
note "  - CopyObject conditional headers (if-match)"

sleep "${END_DELAY}"
