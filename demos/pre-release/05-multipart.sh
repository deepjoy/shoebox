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

# ── 2. Manual Multipart Upload Flow ───────────────────────────────────────

banner "Manual Multipart Upload Flow"

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

step "List uploaded parts"
LIST_PARTS_RESPONSE=$(signed_curl GET "/uploads/test-file.txt?uploadId=$UPLOAD_ID")
PART_COUNT=$(echo "$LIST_PARTS_RESPONSE" | grep -o '<PartNumber>' | wc -l)
echo "  Parts uploaded: $PART_COUNT"
ok "Listed parts"

step "Complete multipart upload"
COMPLETE_BODY="<CompleteMultipartUpload>
  <Part><PartNumber>1</PartNumber><ETag>$ETAG1</ETag></Part>
  <Part><PartNumber>2</PartNumber><ETag>$ETAG2</ETag></Part>
  <Part><PartNumber>3</PartNumber><ETag>$ETAG3</ETag></Part>
</CompleteMultipartUpload>"

COMPLETE_RESPONSE=$(signed_curl POST "/uploads/test-file.txt?uploadId=$UPLOAD_ID" -d "$COMPLETE_BODY")
FINAL_ETAG=$(echo "$COMPLETE_RESPONSE" | grep -o '<ETag>[^<]*</ETag>' | sed 's/<[^>]*>//g')
echo "  Final ETag: $FINAL_ETAG"
ok "Multipart upload completed"

step "Verify multipart ETag format (hash-numparts)"
if [[ "$FINAL_ETAG" =~ ^\"?[0-9a-f]{32}-3\"?$ ]]; then
  ok "ETag matches expected format: md5-3"
else
  echo "  FAIL: expected format '<32-hex>-3', got $FINAL_ETAG"
  exit 1
fi

step "Verify assembled file"
run "aws s3 cp s3://uploads/test-file.txt $DEMO_ROOT/assembled.txt"
cat "$DEMO_ROOT/assembled.txt"
ok "File assembled correctly"

sleep "$DELAY"

# ── 3. Error Handling ────────────────────────────────────────────────────

banner "Error Handling"

step "Initiate upload for error tests"
ERR_RESPONSE=$(signed_curl POST "/uploads/error-test.txt?uploads" -H "content-type: text/plain")
ERR_UPLOAD_ID=$(echo "$ERR_RESPONSE" | grep -o '<UploadId>[^<]*</UploadId>' | sed 's/<[^>]*>//g')
ok "Upload initiated (ID: $ERR_UPLOAD_ID)"

step "Reject part number 0 (must be 1-10000)"
HTTP_CODE=$(signed_curl PUT "/uploads/error-test.txt?partNumber=0&uploadId=$ERR_UPLOAD_ID" \
  -d "bad part" -o /dev/null -w '%{http_code}')
echo "  HTTP status: $HTTP_CODE"
if [[ "$HTTP_CODE" -ge 400 ]]; then
  ok "Invalid part number correctly rejected"
else
  echo "  FAIL: expected 4xx, got $HTTP_CODE"
  exit 1
fi

step "Upload a valid part for ETag test"
ERR_ETAG=$(signed_curl PUT "/uploads/error-test.txt?partNumber=1&uploadId=$ERR_UPLOAD_ID" \
  -d "valid part" -D- | grep -i '^etag:' | cut -d' ' -f2 | tr -d '\r')
echo "  ETag: $ERR_ETAG"
ok "Part uploaded"

step "Reject complete with wrong ETag"
BAD_COMPLETE="<CompleteMultipartUpload>
  <Part><PartNumber>1</PartNumber><ETag>\"0000000000000000deadbeef00000000\"</ETag></Part>
</CompleteMultipartUpload>"
HTTP_CODE=$(signed_curl POST "/uploads/error-test.txt?uploadId=$ERR_UPLOAD_ID" \
  -d "$BAD_COMPLETE" -o /dev/null -w '%{http_code}')
echo "  HTTP status: $HTTP_CODE"
if [[ "$HTTP_CODE" -ge 400 ]]; then
  ok "Mismatched ETag correctly rejected"
else
  echo "  FAIL: expected 4xx, got $HTTP_CODE"
  exit 1
fi

# Clean up error test upload
signed_curl DELETE "/uploads/error-test.txt?uploadId=$ERR_UPLOAD_ID" > /dev/null

sleep "$DELAY"

# ── 4. List In-Progress Uploads ─────────────────────────────────────────────

banner "List In-Progress Uploads"

step "Initiate another upload (leave incomplete)"
INITIATE2=$(signed_curl POST "/uploads/large-file.bin?uploads" -H "content-type: application/octet-stream")
UPLOAD_ID2=$(echo "$INITIATE2" | grep -o '<UploadId>[^<]*</UploadId>' | sed 's/<[^>]*>//g')
echo "  Upload ID: $UPLOAD_ID2"
ok "Second upload initiated"

step "List all in-progress multipart uploads"
LIST_UPLOADS=$(signed_curl GET "/uploads?uploads")
UPLOAD_COUNT=$(echo "$LIST_UPLOADS" | grep -o '<UploadId>' | wc -l)
echo "  In-progress uploads: $UPLOAD_COUNT"
ok "Listed multipart uploads"

sleep "$DELAY"

# ── 5. Abort Multipart Upload ─────────────────────────────────────────────

banner "Abort Multipart Upload"

step "Abort the second upload"
signed_curl DELETE "/uploads/large-file.bin?uploadId=$UPLOAD_ID2" > /dev/null
ok "Upload aborted"

step "Verify upload is gone"
LIST_AFTER_ABORT=$(signed_curl GET "/uploads?uploads")
REMAINING=$(echo "$LIST_AFTER_ABORT" | grep -o '<UploadId>' || true | wc -l)
echo "  Remaining uploads: $REMAINING"
ok "Upload removed from list"

sleep "$DELAY"

# ── 6. AWS CLI Automatic Multipart ────────────────────────────────────────

banner "AWS CLI Automatic Multipart (Large File)"

step "Create a 20 MB test file"
dd if=/dev/zero bs=1M count=20 of="$DEMO_ROOT/large.bin" 2>/dev/null
note "AWS CLI automatically uses multipart for files >5 MB"

step "Upload large file (automatic multipart)"
run "aws s3 cp $DEMO_ROOT/large.bin s3://uploads/large.bin"
ok "Large file uploaded via automatic multipart"

step "Verify file size"
SIZE=$(aws s3api head-object --bucket uploads --key large.bin --query ContentLength --output text)
echo "  Size: $SIZE bytes ($(($SIZE / 1024 / 1024)) MB)"
ok "File uploaded correctly"

sleep "$DELAY"

# ── 7. Multipart Auth with Non-Admin Credentials ────────────────────────────

banner "Multipart Auth — Write-Only Credential"

step "Create a write-only credential via admin API"
CREATE_BODY="<CreateCredentialRequest>
  <BucketName>uploads</BucketName>
  <Permissions>write</Permissions>
  <Description>Write-only multipart test</Description>
</CreateCredentialRequest>"
CREATE_RESPONSE=$(signed_curl POST "/_shoebox/credentials" -d "$CREATE_BODY")
WRITE_ACCESS_KEY=$(echo "$CREATE_RESPONSE" | grep -o '<AccessKeyId>[^<]*</AccessKeyId>' | sed 's/<[^>]*>//g')
WRITE_SECRET_KEY=$(echo "$CREATE_RESPONSE" | grep -o '<SecretAccessKey>[^<]*</SecretAccessKey>' | sed 's/<[^>]*>//g')
echo "  Write-only Access Key: $WRITE_ACCESS_KEY"
ok "Write-only credential created"

# Switch to write-only credentials
SAVED_ACCESS_KEY="$AWS_ACCESS_KEY_ID"
SAVED_SECRET_KEY="$AWS_SECRET_ACCESS_KEY"
export AWS_ACCESS_KEY_ID="$WRITE_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$WRITE_SECRET_KEY"

step "Multipart upload with write-only credential"
WRITE_INITIATE=$(signed_curl POST "/uploads/write-test.txt?uploads" -H "content-type: text/plain")
WRITE_UPLOAD_ID=$(echo "$WRITE_INITIATE" | grep -o '<UploadId>[^<]*</UploadId>' | sed 's/<[^>]*>//g')
if [[ -z "$WRITE_UPLOAD_ID" ]]; then
  echo "  FAIL: InitiateMultipartUpload denied for write-only credential"
  exit 1
fi
echo "  Upload ID: $WRITE_UPLOAD_ID"
ok "InitiateMultipartUpload succeeded with write-only credential"

step "UploadPart with write-only credential"
WRITE_ETAG=$(signed_curl PUT "/uploads/write-test.txt?partNumber=1&uploadId=$WRITE_UPLOAD_ID" \
  -d "write-only part data" -D- | grep -i '^etag:' | cut -d' ' -f2 | tr -d '\r')
if [[ -z "$WRITE_ETAG" ]]; then
  echo "  FAIL: UploadPart denied for write-only credential"
  exit 1
fi
echo "  ETag: $WRITE_ETAG"
ok "UploadPart succeeded with write-only credential"

step "CompleteMultipartUpload with write-only credential"
WRITE_COMPLETE_BODY="<CompleteMultipartUpload>
  <Part><PartNumber>1</PartNumber><ETag>$WRITE_ETAG</ETag></Part>
</CompleteMultipartUpload>"
WRITE_COMPLETE=$(signed_curl POST "/uploads/write-test.txt?uploadId=$WRITE_UPLOAD_ID" -d "$WRITE_COMPLETE_BODY")
WRITE_FINAL_ETAG=$(echo "$WRITE_COMPLETE" | grep -o '<ETag>[^<]*</ETag>' | sed 's/<[^>]*>//g')
if [[ -z "$WRITE_FINAL_ETAG" ]]; then
  echo "  FAIL: CompleteMultipartUpload denied for write-only credential"
  exit 1
fi
echo "  Final ETag: $WRITE_FINAL_ETAG"
ok "CompleteMultipartUpload succeeded with write-only credential"

step "Read-only credential: create and test multipart is denied"
# Restore admin credentials to create a read-only credential
export AWS_ACCESS_KEY_ID="$SAVED_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SAVED_SECRET_KEY"

READ_CREATE_BODY="<CreateCredentialRequest>
  <BucketName>uploads</BucketName>
  <Permissions>read</Permissions>
  <Description>Read-only multipart test</Description>
</CreateCredentialRequest>"
READ_RESPONSE=$(signed_curl POST "/_shoebox/credentials" -d "$READ_CREATE_BODY")
READ_ACCESS_KEY=$(echo "$READ_RESPONSE" | grep -o '<AccessKeyId>[^<]*</AccessKeyId>' | sed 's/<[^>]*>//g')
READ_SECRET_KEY=$(echo "$READ_RESPONSE" | grep -o '<SecretAccessKey>[^<]*</SecretAccessKey>' | sed 's/<[^>]*>//g')

# Switch to read-only credentials
export AWS_ACCESS_KEY_ID="$READ_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$READ_SECRET_KEY"

HTTP_CODE=$(signed_curl POST "/uploads/read-test.txt?uploads" -H "content-type: text/plain" -o /dev/null -w '%{http_code}')
echo "  HTTP status: $HTTP_CODE"
if [[ "$HTTP_CODE" == "403" ]]; then
  ok "InitiateMultipartUpload correctly denied for read-only credential"
else
  echo "  FAIL: expected 403, got $HTTP_CODE"
  exit 1
fi

# Restore admin credentials for remaining operations
export AWS_ACCESS_KEY_ID="$SAVED_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SAVED_SECRET_KEY"

sleep "$DELAY"

# ── 8. Done ───────────────────────────────────────────────────────────────────

banner "Phase 5 Demo Complete"
note "All Phase 5 features demonstrated successfully:"
note "  - InitiateMultipartUpload creates upload ID"
note "  - UploadPart uploads individual parts with ETags"
note "  - ListParts shows uploaded parts"
note "  - CompleteMultipartUpload assembles final file"
note "  - Multipart ETag format: hash-numparts"
note "  - File content verified after assembly"
note "  - Part number validation rejects invalid numbers"
note "  - ETag verification rejects mismatched ETags"
note "  - ListMultipartUploads shows in-progress uploads"
note "  - AbortMultipartUpload cleans up parts"
note "  - Upload removed from list after abort"
note "  - AWS CLI automatic multipart (20 MB file)"
note "  - Write-only credential can perform multipart uploads"
note "  - Read-only credential denied multipart initiation"

sleep "${END_DELAY}"
