#!/usr/bin/env bash
# ============================================================================
# Shoebox Demo — Bucket Configuration (CORS + Notifications)
# ============================================================================
#
# Demonstrates Phase 9 features:
#   - PutBucketCors / GetBucketCors / DeleteBucketCors
#   - CORS preflight (OPTIONS) handling
#   - PutBucketNotification / GetBucketNotification
#   - EventBus emission on object operations
#
# Record with asciinema:
#   asciinema rec --cols 100 --rows 30 -c './demos/pre-release/09-bucket-config.sh' demo.cast
# ============================================================================

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../util/lib.sh"

# --- Setup -------------------------------------------------------------------

DEMO_ROOT="$(mktemp -d)"
trap 'kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; rm -rf "$DEMO_ROOT"' EXIT

BUCKET_DIR="$DEMO_ROOT/photos"
mkdir -p "$BUCKET_DIR"

require_shoebox

PORT=9891
ENDPOINT="http://127.0.0.1:$PORT"

# Start shoebox in the background
SHOEBOX_LOG=off "$SHOEBOX" --host 127.0.0.1 --port "$PORT" --show-secrets "$BUCKET_DIR" > "$DEMO_ROOT/startup.txt" 2>&1 &
SERVER_PID=$!
wait_for_server "$ENDPOINT"

extract_credentials "$BUCKET_DIR"
setup_aws_env "$ACCESS_KEY" "$SECRET_KEY" "$ENDPOINT"

AWS="aws --endpoint-url $ENDPOINT"

# --- Parts -------------------------------------------------------------------

p01_cors_configuration() {
  step "Configure CORS rules for the bucket"

  note "PUT bucket CORS configuration"
  CORS_BODY='[{"allowed_origins":["https://example.com","https://*.example.com"],"allowed_methods":["GET","PUT","POST","DELETE"],"allowed_headers":["*"],"expose_headers":["ETag","x-amz-meta-custom"],"max_age_seconds":3600}]'
  run "signed_curl PUT '/photos?cors' -H 'Content-Type: application/json' -d '$CORS_BODY'"
  echo ""

  note "GET bucket CORS configuration"
  run "signed_curl GET '/photos?cors' | python3 -m json.tool"
  echo ""
}
part p01_cors_configuration "CORS Configuration"

p02_cors_preflight() {
  step "Test CORS preflight (OPTIONS) request"

  note "OPTIONS request from https://example.com (should return 200 with CORS headers)"
  run "curl -s -D - -o /dev/null -X OPTIONS '$ENDPOINT/photos/test.jpg' -H 'Origin: https://example.com' -H 'Access-Control-Request-Method: PUT' -H 'Access-Control-Request-Headers: content-type' 2>&1 | head -15"
  echo ""

  note "OPTIONS request from unauthorized origin (should return 403)"
  run "curl -s -o /dev/null -w 'HTTP Status: %{http_code}\n' -X OPTIONS '$ENDPOINT/photos/test.jpg' -H 'Origin: https://evil.com' -H 'Access-Control-Request-Method: GET'"
  echo ""

  note "OPTIONS from subdomain wildcard match (https://app.example.com)"
  run "curl -s -D - -o /dev/null -X OPTIONS '$ENDPOINT/photos/test.jpg' -H 'Origin: https://app.example.com' -H 'Access-Control-Request-Method: GET' 2>&1 | head -15"
  echo ""
}
part p02_cors_preflight "CORS Preflight"

p03_cors_on_regular_request() {
  step "Verify CORS headers on regular (authenticated) request"

  note "Upload a test file"
  echo "cors-test" > "$DEMO_ROOT/cors-test.txt"
  run "$AWS s3 cp '$DEMO_ROOT/cors-test.txt' s3://photos/cors-test.txt"
  echo ""

  note "GET with Origin header — response should include CORS headers"
  run "signed_curl GET '/photos/cors-test.txt' -H 'Origin: https://example.com' -D - -o /dev/null 2>&1 | head -15"
  echo ""

  note "GET with wildcard subdomain Origin header"
  run "signed_curl GET '/photos/cors-test.txt' -H 'Origin: https://cdn.example.com' -D - -o /dev/null 2>&1 | head -15"
  echo ""
}
part p03_cors_on_regular_request "CORS on Requests"

p04_notification_configuration() {
  step "Configure webhook notifications"

  note "PUT bucket notification configuration"
  NOTIF_BODY='[{"id":"image-processor","url":"http://localhost:19999/process","events":["s3:ObjectCreated:*"],"filter":{"prefix":"uploads/","suffix":".jpg"}},{"id":"cleanup-notifier","url":"http://localhost:19999/cleanup","events":["s3:ObjectRemoved:*"],"filter":null}]'
  run "signed_curl PUT '/photos?notification' -H 'Content-Type: application/json' -d '$NOTIF_BODY'"
  echo ""

  note "GET bucket notification configuration"
  run "signed_curl GET '/photos?notification' | python3 -m json.tool"
  echo ""
}
part p04_notification_configuration "Notification Configuration"

p05_events_on_operations() {
  step "Upload & delete objects (events emitted to EventBus)"

  note "Upload a file — triggers s3:ObjectCreated:Put event"
  echo "Hello from Phase 9!" > "$DEMO_ROOT/test.txt"
  run "$AWS s3 cp '$DEMO_ROOT/test.txt' s3://photos/uploads/photo.jpg"
  echo ""

  note "Delete a file — triggers s3:ObjectRemoved:Delete event"
  run "$AWS s3 rm s3://photos/uploads/photo.jpg"
  echo ""

  note "Events are delivered to configured webhooks (best-effort with retry)."
  note "Since no webhook server is running, delivery will be attempted and logged."
  echo ""
}
part p05_events_on_operations "Event Emission"

p06_delete_cors() {
  step "Delete CORS configuration"

  run "signed_curl DELETE '/photos?cors' -o /dev/null -w 'HTTP Status: %{http_code}\n'"
  echo ""

  note "Verify CORS rules are empty"
  run "signed_curl GET '/photos?cors'"
  echo ""

  note "Preflight should now return 403 (no CORS rules configured)"
  run "curl -s -o /dev/null -w 'HTTP Status: %{http_code}\n' -X OPTIONS '$ENDPOINT/photos/test.jpg' -H 'Origin: https://example.com' -H 'Access-Control-Request-Method: GET'"
  echo ""
}
part p06_delete_cors "Delete CORS"

p99_done() {
  echo ""
  ok "CORS configuration: PUT, GET, DELETE all working."
  ok "CORS preflight: exact and subdomain wildcard matching verified."
  ok "Notification configuration: PUT and GET working."
  ok "Events emitted on object create and delete."
  echo ""
}
part p99_done "Done!"

# --- Main --------------------------------------------------------------------

banner "Shoebox Demo — Bucket Configuration (CORS + Notifications)"

run_demo
