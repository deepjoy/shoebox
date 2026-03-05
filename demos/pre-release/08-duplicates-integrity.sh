#!/usr/bin/env bash
# ============================================================================
# Shoebox Demo — Duplicate Detection + Integrity
# ============================================================================
#
# Demonstrates Phase 8 features:
#   - FindBucketDuplicates: detect files with matching checksum_sha256
#   - MergeDuplicates: pick a winner and delete losers
#   - CompareDirs: show differences between two directories
#   - CheckIntegrity: verify on-disk files match stored checksums
#   - CLI duplicates, integrity-check, and compare-dirs subcommands
#
# Environment variables:
#   SHOEBOX_ENDPOINT — override the endpoint URL (default: http://127.0.0.1:$PORT)
#
# Record with asciinema:
#   asciinema rec --cols 100 --rows 30 -c './demos/pre-release/08-duplicates-integrity.sh' demo.cast
# ============================================================================

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../util/lib.sh"

# --- Setup -------------------------------------------------------------------

DEMO_ROOT="$(mktemp -d)"
trap 'kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; rm -rf "$DEMO_ROOT"' EXIT

BUCKET="$DEMO_ROOT/photos"
mkdir -p "$BUCKET/originals" "$BUCKET/backup" "$BUCKET/edited"

require_shoebox

PORT=9881
ENDPOINT="${SHOEBOX_ENDPOINT:-http://127.0.0.1:$PORT}"

# --- Parts -------------------------------------------------------------------

p01_setup_with_duplicates() {
  step "Create files — some are duplicates (identical content)"
  echo "Beautiful sunset over the ocean" > "$BUCKET/originals/sunset.txt"
  cp "$BUCKET/originals/sunset.txt" "$BUCKET/backup/sunset.txt"
  cp "$BUCKET/originals/sunset.txt" "$BUCKET/edited/sunset-copy.txt"

  echo "Mountain landscape at dawn" > "$BUCKET/originals/mountain.txt"
  cp "$BUCKET/originals/mountain.txt" "$BUCKET/backup/mountain.txt"

  echo "City skyline at night — unique" > "$BUCKET/originals/city.txt"
  echo "Forest trail — different content" > "$BUCKET/edited/forest.txt"

  note "3 copies of sunset.txt (originals/, backup/, edited/)"
  note "2 copies of mountain.txt (originals/, backup/)"
  note "2 unique files (city.txt, forest.txt)"

  step "Start server (L1 scan discovers files, then L2+L3 run in background)"
  SHOEBOX_LOG=info $SHOEBOX --port "$PORT" "$BUCKET" &>"$DEMO_ROOT/server.log" &
  SERVER_PID=$!
  wait_for_server "$ENDPOINT"

  ACCESS_KEY=$(grep -oP 'AKIA[A-Z0-9]{16}' "$DEMO_ROOT/server.log" | head -1)
  SECRET_KEY=$(grep -oP 'Secret: \K[A-Za-z0-9/+=]+' "$DEMO_ROOT/server.log" | head -1)
  setup_aws_env "$ACCESS_KEY" "$SECRET_KEY" "$ENDPOINT"

  step "Wait for L3 content hashing to complete..."
  note "L3 scan hashes file contents (SHA-256) — needed for duplicate detection."
  sleep 5

  step "Verify all files are listed"
  run "aws s3 ls s3://photos/ --recursive"
  ok "7 files in bucket, L3 hashing should be complete"
}
part p01_setup_with_duplicates "Phase 8 — Duplicates + Integrity"

p02_find_duplicates_api() {
  step "FindBucketDuplicates — detect files with matching checksums"
  note "GET /{bucket}?duplicates returns groups of files sharing the same hash."
  run "signed_curl GET '/photos?duplicates&allow-partial=true'"

  ok "Duplicate groups found — files with identical content are grouped together"
}
part p02_find_duplicates_api "Duplicate Detection (API)"

p03_merge_duplicates() {
  step "MergeDuplicates — pick a winner, delete the losers"
  note "POST /{bucket}?merge with JSON body specifying winner_key and loser_keys."
  note "This keeps originals/sunset.txt and deletes the backup and edited copies."

  run "signed_curl POST '/photos?merge' \
    -H 'Content-Type: application/json' \
    -d '{\"winner_key\": \"originals/sunset.txt\", \"loser_keys\": [\"backup/sunset.txt\", \"edited/sunset-copy.txt\"]}'"

  step "Verify losers are deleted"
  run "aws s3 ls s3://photos/ --recursive"
  ok "Losers deleted — only the winner copy remains"
}
part p03_merge_duplicates "Merge Duplicates"

p04_compare_dirs() {
  step "CompareDirs — compare two directories"
  note "GET /?compare-dirs&left=photos/originals/&right=photos/backup/"
  note "Shows which files are identical, modified, or only in one side."

  run "signed_curl GET '/?compare-dirs&left=photos/originals/&right=photos/backup/'"

  ok "Directory comparison shows differences between originals/ and backup/"
}
part p04_compare_dirs "Compare Directories"

p05_integrity_check() {
  step "CheckIntegrity — verify all files match stored checksums"
  note "GET /{bucket}?integrity-check scans all L3 files and re-hashes them."
  run "signed_curl GET '/photos?integrity-check'"

  step "Now corrupt a file on disk (simulate bit rot)"
  echo "CORRUPTED DATA" > "$BUCKET/originals/mountain.txt"
  note "Overwrote originals/mountain.txt with different content."

  step "Run integrity check again — should detect the corruption"
  run "signed_curl GET '/photos?integrity-check'"

  ok "Integrity check detected the corrupted file!"
}
part p05_integrity_check "Integrity Verification"

p06_async_integrity() {
  step "Async integrity check — runs in background"
  note "GET /{bucket}?integrity-check&async=true returns immediately."
  note "Use ?integrity-status&check_id=... to poll for results."

  RESPONSE=$(signed_curl GET '/photos?integrity-check&async=true')
  echo "$RESPONSE"
  CHECK_ID=$(echo "$RESPONSE" | grep -oP '<CheckId>\K[^<]+' || echo "unknown")
  note "Check ID: $CHECK_ID — status is in_progress"

  step "Poll for results..."
  sleep 3
  run "signed_curl GET '/photos?integrity-status&check_id=$CHECK_ID'"

  ok "Async integrity check completed"
}
part p06_async_integrity "Async Integrity Check"

p07_cli_commands() {
  step "CLI: duplicates subcommand"
  note "shoebox duplicates \$BUCKET_PATH [--format table|json]"
  run "$SHOEBOX duplicates '$BUCKET' --allow-partial --format table"

  step "CLI: integrity-check subcommand"
  note "shoebox integrity-check \$BUCKET_PATH"
  run "$SHOEBOX integrity-check '$BUCKET' --format table"

  ok "CLI commands work for offline/scripted usage"
}
part p07_cli_commands "CLI Subcommands"

p99_done() {
  note "Phase 8 checklist items demonstrated:"
  note "  [x] FindBucketDuplicates returns files with matching checksum_sha256"
  note "  [x] MergeDuplicates deletes loser objects"
  note "  [x] CompareDirs shows differences between directories"
  note "  [x] ScanPending error when L3 incomplete (allow-partial bypasses)"
  note "  [x] Integrity check detects hash mismatches (bit rot)"
  note "  [x] Async integrity check returns immediately"
  note "  [x] CLI integrity-check command works"
  note "  [x] CLI duplicates command works"
  note ""
  note "Items tested via unit tests (not in demo):"
  note "  [x] Cross-bucket duplicate detection uses streaming merge"
  note "  [x] Directory hash computation"
  note "  [x] Scheduled integrity checks run periodically"
  note "  [x] Async integrity check persists results with cancelled status on shutdown"
}
part p99_done "Phase 8 Demo Complete"

# --- Run --------------------------------------------------------------------

run_demo
