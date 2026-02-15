#!/usr/bin/env bash
# ============================================================================
# Shoebox Demo — Core S3 operations via AWS CLI
# ============================================================================
#
# Exercises every Current endpoint using the standard AWS CLI, proving
# shoebox is a drop-in S3-compatible backend:
#   PutObject, GetObject, HeadObject, DeleteObject,
#   ListBuckets, HeadBucket, ListObjectsV2 (prefix, delimiter, pagination),
#   DeleteObjects (bulk delete)
#
# Record with asciinema:
#   asciinema rec --cols 100 --rows 30 -c './demos/pre-release/02-core-operations.sh' demo.cast
#
# Replay:
#   asciinema play demo.cast
# ============================================================================

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../util/lib.sh"

# --- Setup -------------------------------------------------------------------

DEMO_ROOT="$(mktemp -d)"
trap 'kill "$SERVER_PID" 2>/dev/null; rm -rf "$DEMO_ROOT"' EXIT

BUCKET_DIR="$DEMO_ROOT/photos"
mkdir -p "$BUCKET_DIR"

SHOEBOX="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/target/release/shoebox"
PORT=9876
ENDPOINT="http://127.0.0.1:$PORT"

if [[ ! -x "$SHOEBOX" ]]; then
  echo "Error: shoebox binary not found at $SHOEBOX"
  echo "Run 'cargo build --release' first."
  exit 1
fi

# AWS CLI configuration — auth is not enforced yet(TODO),
# but the CLI requires credentials to be set.
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_PAGER=""
AWS="aws --endpoint-url $ENDPOINT"

# Start shoebox in the background
SHOEBOX_LOG=off "$SHOEBOX" --host 127.0.0.1 --port "$PORT" "$BUCKET_DIR" &
SERVER_PID=$!

# Wait for server to be ready
for i in $(seq 1 30); do
  if curl -s -o /dev/null "$ENDPOINT/" 2>/dev/null; then
    break
  fi
  sleep 0.1
done

# ============================================================================
# Part 1 — Upload objects (aws s3 cp)
# ============================================================================

banner "Core S3 Operations — Upload"

note "aws s3 cp uploads files via PutObject, just like real S3."
sleep "$DELAY"

step "Upload a text file"
echo "Hello from shoebox!" > "$DEMO_ROOT/hello.txt"
run "$AWS s3 cp '$DEMO_ROOT/hello.txt' s3://photos/hello.txt"

step "Upload a JSON file with custom content type"
echo '{"name":"shoebox","version":"0.1"}' > "$DEMO_ROOT/info.json"
run "$AWS s3 cp '$DEMO_ROOT/info.json' s3://photos/info.json --content-type application/json"

step "Upload several files into a sub-prefix (vacation/)"
mkdir -p "$DEMO_ROOT/vacation"
for f in beach.jpg sunset.jpg poolside.jpg; do
  echo "binary-data-placeholder-for-$f" > "$DEMO_ROOT/vacation/$f"
done
run "$AWS s3 cp '$DEMO_ROOT/vacation/' s3://photos/vacation/ --recursive"

ok "5 objects uploaded."

# ============================================================================
# Part 2 — List buckets
# ============================================================================

banner "Bucket Operations"

note "aws s3 ls (no path) calls ListBuckets — just like real AWS."
sleep "$DELAY"

step "List all buckets"
run "$AWS s3 ls"

step "Check bucket exists (s3api head-bucket)"
run "$AWS s3api head-bucket --bucket photos"

step "Non-existent bucket returns an error"
run "$AWS s3api head-bucket --bucket no-such-bucket 2>&1 || true"

# ============================================================================
# Part 3 — List objects (ListObjectsV2)
# ============================================================================

banner "ListObjectsV2 — Prefix, Delimiter, Pagination"

note "aws s3 ls uses delimiter=/ by default (shows 'directories')."
sleep "$DELAY"

step "List top-level (delimiter=/)"
run "$AWS s3 ls s3://photos/"

step "List under vacation/ prefix"
run "$AWS s3 ls s3://photos/vacation/"

step "Recursive listing (no delimiter — flat list)"
run "$AWS s3 ls s3://photos/ --recursive"

step "Pagination with s3api: page-size=2"
run "$AWS s3api list-objects-v2 --bucket photos --page-size 2 --max-items 2"

note "NextToken in the output can be passed with --starting-token to fetch the next page."
NEXT_TOKEN=$($AWS s3api list-objects-v2 --bucket photos --page-size 2 --max-items 2 \
  | grep -o '"NextToken": "[^"]*"' | cut -d'"' -f4 || true)
if [[ -n "$NEXT_TOKEN" ]]; then
  step "Fetch page 2 with --starting-token"
  run "$AWS s3api list-objects-v2 --bucket photos --page-size 2 --max-items 2 --starting-token '$NEXT_TOKEN'"
fi

# ============================================================================
# Part 4 — Download objects (aws s3 cp)
# ============================================================================

banner "Download Objects"

note "aws s3 cp from s3:// downloads via GetObject."
sleep "$DELAY"

step "Download hello.txt"
run "$AWS s3 cp s3://photos/hello.txt '$DEMO_ROOT/downloaded.txt'"
step "Verify contents"
run "cat '$DEMO_ROOT/downloaded.txt'"

step "Download the vacation/ prefix recursively"
run "$AWS s3 cp s3://photos/vacation/ '$DEMO_ROOT/downloaded-vacation/' --recursive"
run "ls -l '$DEMO_ROOT/downloaded-vacation/'"

# ============================================================================
# Part 5 — Inspect objects (HeadObject)
# ============================================================================

banner "Inspect Object Metadata"

note "s3api head-object returns metadata as JSON — no body downloaded."
sleep "$DELAY"

step "Head hello.txt"
run "$AWS s3api head-object --bucket photos --key hello.txt"

step "Head info.json"
run "$AWS s3api head-object --bucket photos --key info.json"

step "Head a non-existent key (returns 404)"
run "$AWS s3api head-object --bucket photos --key does-not-exist.txt 2>&1 || true"

# ============================================================================
# Part 6 — Delete operations
# ============================================================================

banner "Delete Operations"

note "aws s3 rm deletes a single object via DeleteObject."
sleep "$DELAY"

step "Delete hello.txt"
run "$AWS s3 rm s3://photos/hello.txt"

step "Verify it's gone"
run "$AWS s3 ls s3://photos/ --recursive"

step "Bulk-delete vacation/ with --recursive (uses DeleteObjects API)"
run "$AWS s3 rm s3://photos/vacation/ --recursive"

step "List remaining objects (should be 1: info.json)"
run "$AWS s3 ls s3://photos/ --recursive"

# ============================================================================
# Done
# ============================================================================

banner "Done!"

echo ""
ok "All Current S3 operations verified with the standard AWS CLI."
ok "shoebox is a drop-in local replacement for S3."
echo ""
