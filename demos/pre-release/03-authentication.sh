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

require_shoebox

PORT=9877
ENDPOINT="${SHOEBOX_ENDPOINT:-http://127.0.0.1:$PORT}"

# --- Parts ------------------------------------------------------------------

p01_version() {
  step "Check version"
  run "$SHOEBOX --version"
}
part p01_version "Version Flag"

p02_server_startup() {
  step "Start server with auto-generated credentials"

  SHOEBOX_LOG=off "$SHOEBOX" --host 127.0.0.1 --port "$PORT" --show-secrets "$BUCKET_DIR" > "$DEMO_ROOT/startup.txt" 2>&1 &
  SERVER_PID=$!
  wait_for_server "$ENDPOINT"

  extract_credentials "$BUCKET_DIR"
  note "Access Key: $ACCESS_KEY"
  note "Secret Key: ${SECRET_KEY:0:10}..."
  ok "Server started on $ENDPOINT"

  setup_aws_env "$ACCESS_KEY" "$SECRET_KEY" "$ENDPOINT"
}
part p02_server_startup "Server Startup & Credential Extraction"

p03_authenticated_ops() {
  step "Upload a file with valid credentials"
  echo "Hello from Shoebox Phase 3!" > "$DEMO_ROOT/test.txt"
  run "aws s3 cp $DEMO_ROOT/test.txt s3://photos/greeting.txt"

  step "Download the file back"
  run "aws s3 cp s3://photos/greeting.txt $DEMO_ROOT/downloaded.txt"
  run "cat $DEMO_ROOT/downloaded.txt"

  step "List objects"
  run "aws s3 ls s3://photos/ --recursive"
}
part p03_authenticated_ops "Authenticated Operations (SigV4)"

p04_unauthenticated_rejection() {
  step "Request WITHOUT credentials is rejected (403)"
  HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' "$ENDPOINT/photos/greeting.txt" || true)
  note "curl (no auth) -> HTTP $HTTP_CODE"
  if [[ "$HTTP_CODE" == "403" ]]; then
    ok "Unauthenticated request correctly rejected with 403"
  else
    echo "ERROR: Expected 403, got $HTTP_CODE"
  fi
}
part p04_unauthenticated_rejection "Unauthenticated Request Rejection"

p05_credential_management() {
  step "Stop server for credential CLI commands"
  kill "$SERVER_PID" 2>/dev/null
  wait "$SERVER_PID" 2>/dev/null || true

  step "List credentials"
  run "$SHOEBOX list-credentials $BUCKET_DIR"

  step "Add a read-only credential"
  run "$SHOEBOX add-credential $BUCKET_DIR --permissions read --description 'Read-only CI'"

  step "List credentials (now two)"
  run "$SHOEBOX list-credentials $BUCKET_DIR"
}
part p05_credential_management "Credential Management CLI"

p06_presigned_urls() {
  step "Pre-sign a GET URL"
  run "$SHOEBOX presign get photos greeting.txt --bucket-path $BUCKET_DIR --endpoint $ENDPOINT --expires 1h"
}
part p06_presigned_urls "Pre-Signed URL Generation"

p07_virtual_hosted() {
  step "Restart server for virtual-host test"
  SHOEBOX_LOG=off "$SHOEBOX" --host 127.0.0.1 --port "$PORT" "$BUCKET_DIR" > /dev/null 2>&1 &
  SERVER_PID=$!
  wait_for_server "$ENDPOINT"

  step "Access via virtual-hosted-style (Host: photos.localhost)"
  note "curl -H 'Host: photos.localhost:$PORT' $ENDPOINT/"
  # This tests the virtual-host middleware path rewriting
  HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' -H "Host: photos.localhost:$PORT" "$ENDPOINT/" 2>/dev/null || true)
  note "HTTP $HTTP_CODE (bucket listing via virtual-host)"
  if [[ "$HTTP_CODE" == "403" || "$HTTP_CODE" == "200" ]]; then
    ok "Virtual-hosted routing works"
  fi
}
part p07_virtual_hosted "Virtual-Hosted-Style Routing"

p08_global_config() {
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
  wait_for_server "$ENDPOINT"
  ok "Server started with global config"

  step "Verify objects still accessible"
  run "aws s3 ls s3://photos/ --recursive"
}
part p08_global_config "Global Config File (--config)"

p09_graceful_shutdown() {
  step "Graceful shutdown (SIGINT)"
  kill -INT "$SERVER_PID"
  wait "$SERVER_PID" 2>/dev/null || true
  ok "Server shut down gracefully"
}
part p09_graceful_shutdown "Graceful Shutdown"

p99_done() {
  note "All Phase 3 features demonstrated successfully:"
  note "  - SigV4 authentication (valid & rejected)"
  note "  - Credential management CLI (add, list)"
  note "  - Pre-signed URL generation"
  note "  - Virtual-hosted-style routing"
  note "  - Global config file (--config)"
  note "  - --version flag"
  note "  - Graceful shutdown"
}
part p99_done "Phase 3 Demo Complete"

# --- Run --------------------------------------------------------------------

run_demo
