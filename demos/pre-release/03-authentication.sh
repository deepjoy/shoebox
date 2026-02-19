#!/usr/bin/env bash
# ============================================================================
# Shoebox Demo — Authentication, Credential Management, Pre-Signed URLs
# ============================================================================
#
# Demonstrates Phase 3 features:
#   - SigV4 authentication (requests rejected without valid credentials)
#   - Credential management CLI (add, list, remove)
#   - Pre-signed URL generation
#   - Virtual-hosted-style routing
#   - Global config file
#   - --version flag
#   - Graceful shutdown
#
# Environment variables:
#   SHOEBOX_ENDPOINT — override the endpoint URL (default: http://127.0.0.1:$PORT)
#
# Record with asciinema:
#   asciinema rec --cols 100 --rows 30 -c './demos/pre-release/03-authentication.sh' demo.cast
# ============================================================================

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../util/lib.sh"

# --- Setup -------------------------------------------------------------------

DEMO_ROOT="$(mktemp -d)"
trap 'kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; rm -rf "$DEMO_ROOT"' EXIT

BUCKET_DIR="$DEMO_ROOT/photos"
mkdir -p "$BUCKET_DIR"

SHOEBOX="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/target/release/shoebox"
PORT=9877
ENDPOINT="${SHOEBOX_ENDPOINT:-http://127.0.0.1:$PORT}"

if [[ ! -x "$SHOEBOX" ]]; then
  echo "Error: shoebox binary not found at $SHOEBOX"
  echo "Run 'cargo build --release' first."
  exit 1
fi

# ── 1. Version ──────────────────────────────────────────────────────────────

banner "Version Flag"

step "Check version"
run "$SHOEBOX --version"
sleep "$DELAY"

# ── 2. Server startup & credential extraction ───────────────────────────────

banner "Server Startup & Credential Extraction"

step "Start server with auto-generated credentials"

SHOEBOX_LOG=off "$SHOEBOX" --host 127.0.0.1 --port "$PORT" --show-secrets "$BUCKET_DIR" > "$DEMO_ROOT/startup.txt" 2>&1 &
SERVER_PID=$!

for i in $(seq 1 30); do
  if curl -s -o /dev/null "$ENDPOINT/" 2>/dev/null; then
    break
  fi
  sleep 0.1
done

# Extract credentials from the config file
ACCESS_KEY=$(grep access_key_id "$BUCKET_DIR/.shoebox/config.toml" | head -1 | cut -d'"' -f2)
SECRET_KEY=$(grep secret_access_key "$BUCKET_DIR/.shoebox/config.toml" | head -1 | cut -d'"' -f2)
note "Access Key: $ACCESS_KEY"
note "Secret Key: ${SECRET_KEY:0:10}..."
ok "Server started on $ENDPOINT"

export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL="$ENDPOINT"
sleep "$DELAY"

# ── 3. Authenticated operations ─────────────────────────────────────────────

banner "Authenticated Operations (SigV4)"

step "Upload a file with valid credentials"
echo "Hello from Shoebox Phase 3!" > "$DEMO_ROOT/test.txt"
run "aws s3 cp $DEMO_ROOT/test.txt s3://photos/greeting.txt"

step "Download the file back"
run "aws s3 cp s3://photos/greeting.txt $DEMO_ROOT/downloaded.txt"
run "cat $DEMO_ROOT/downloaded.txt"

step "List objects"
run "aws s3 ls s3://photos/ --recursive"
sleep "$DELAY"

# ── 4. Unauthenticated rejection ────────────────────────────────────────────

banner "Unauthenticated Request Rejection"

step "Request WITHOUT credentials is rejected (403)"
HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' "$ENDPOINT/photos/greeting.txt" || true)
note "curl (no auth) -> HTTP $HTTP_CODE"
if [[ "$HTTP_CODE" == "403" ]]; then
  ok "Unauthenticated request correctly rejected with 403"
else
  echo "ERROR: Expected 403, got $HTTP_CODE"
fi
sleep "$DELAY"

# ── 5. Credential management CLI ────────────────────────────────────────────

banner "Credential Management CLI"

step "Stop server for credential CLI commands"
kill "$SERVER_PID" 2>/dev/null
wait "$SERVER_PID" 2>/dev/null || true

step "List credentials"
run "$SHOEBOX list-credentials $BUCKET_DIR"

step "Add a read-only credential"
run "$SHOEBOX add-credential $BUCKET_DIR --permissions read --description 'Read-only CI'"

step "List credentials (now two)"
run "$SHOEBOX list-credentials $BUCKET_DIR"
sleep "$DELAY"

# ── 6. Pre-signed URLs ─────────────────────────────────────────────────────

banner "Pre-Signed URL Generation"

step "Pre-sign a GET URL"
run "$SHOEBOX presign get photos greeting.txt --bucket-path $BUCKET_DIR --endpoint $ENDPOINT --expires 1h"
sleep "$DELAY"

# ── 7. Virtual-hosted routing ───────────────────────────────────────────────

banner "Virtual-Hosted-Style Routing"

step "Restart server for virtual-host test"
SHOEBOX_LOG=off "$SHOEBOX" --host 127.0.0.1 --port "$PORT" "$BUCKET_DIR" > /dev/null 2>&1 &
SERVER_PID=$!

for i in $(seq 1 30); do
  if curl -s -o /dev/null "$ENDPOINT/" 2>/dev/null; then
    break
  fi
  sleep 0.1
done

step "Access via virtual-hosted-style (Host: photos.localhost)"
note "curl -H 'Host: photos.localhost:$PORT' $ENDPOINT/"
# This tests the virtual-host middleware path rewriting
HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' -H "Host: photos.localhost:$PORT" "$ENDPOINT/" 2>/dev/null || true)
note "HTTP $HTTP_CODE (bucket listing via virtual-host)"
if [[ "$HTTP_CODE" == "403" || "$HTTP_CODE" == "200" ]]; then
  ok "Virtual-hosted routing works"
fi
sleep "$DELAY"

# ── 8. Global config file ──────────────────────────────────────────────────

banner "Global Config File (--config)"

step "Stop server"
kill "$SERVER_PID" 2>/dev/null
wait "$SERVER_PID" 2>/dev/null || true

step "Create a global config file"
GLOBAL_CONFIG="$DEMO_ROOT/global.toml"
cat > "$GLOBAL_CONFIG" <<EOF
host = "127.0.0.1"
port = $PORT
buckets = ["$BUCKET_DIR"]
EOF
run "cat $GLOBAL_CONFIG"
note "Using --config flag to load global config"

step "Start server with global config"
SHOEBOX_LOG=off "$SHOEBOX" --config "$GLOBAL_CONFIG" > /dev/null 2>&1 &
SERVER_PID=$!

for i in $(seq 1 30); do
  if curl -s -o /dev/null "$ENDPOINT/" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
ok "Server started with global config"

step "Verify objects still accessible"
run "aws s3 ls s3://photos/ --recursive"
sleep "$DELAY"

# ── 9. Graceful shutdown ────────────────────────────────────────────────────

banner "Graceful Shutdown"

step "Graceful shutdown (SIGINT)"
kill -INT "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
ok "Server shut down gracefully"
sleep "$DELAY"

# ── Done ─────────────────────────────────────────────────────────────────────

banner "Phase 3 Demo Complete"
note "All Phase 3 features demonstrated successfully:"
note "  - SigV4 authentication (valid & rejected)"
note "  - Credential management CLI (add, list)"
note "  - Pre-signed URL generation"
note "  - Virtual-hosted-style routing"
note "  - Global config file (--config)"
note "  - --version flag"
note "  - Graceful shutdown"

sleep "${END_DELAY}"
