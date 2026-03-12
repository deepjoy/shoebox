#!/usr/bin/env bash
# ============================================================================
# Shoebox Social Demo — Week 1 Progress Post (Feb 18)
# ============================================================================
#
# A tight, punchy demo for X/Bluesky. Target: ~15 seconds as a GIF.
# Shows: start server → upload photos → list them → download one.
#
# Record:
#   asciinema rec --cols 72 --rows 24 -c './demos/social/week-01-progress.sh' \
#     demos/social/week-01-progress.cast
#
# Convert to GIF (install: cargo install agg):
#   agg --cols 72 --rows 24 --speed 1.2 --theme monokai \
#     demos/social/week-01-progress.cast demos/social/week-01-progress.gif
#
# Convert to MP4 (if agg not available, use gifski + ffmpeg):
#   ffmpeg -i demos/social/week-01-progress.gif -movflags faststart \
#     -pix_fmt yuv420p demos/social/week-01-progress.mp4
# ============================================================================

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../util/lib.sh"

# Override delay for social — snappy but readable
DELAY="${DEMO_DELAY:-0.8}"

# --- Setup (invisible to viewer) -------------------------------------------

DEMO_ROOT="$(mktemp -d)"
trap 'kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; rm -rf "$DEMO_ROOT"' EXIT

BUCKET_DIR="$DEMO_ROOT/photos"
mkdir -p "$BUCKET_DIR"

# Create sample photo files to upload
mkdir -p "$DEMO_ROOT/uploads"
echo "JFIF-binary-data-kashmir-1985"   > "$DEMO_ROOT/uploads/kashmir-1985.jpg"
echo "JFIF-binary-data-beach-2019"     > "$DEMO_ROOT/uploads/beach-2019.jpg"
echo "JFIF-binary-data-birthday-party" > "$DEMO_ROOT/uploads/birthday-party.jpg"
echo "JFIF-binary-data-graduation"     > "$DEMO_ROOT/uploads/graduation.jpg"
echo "JFIF-binary-data-first-snow"     > "$DEMO_ROOT/uploads/first-snow.jpg"

require_shoebox

PORT=9876
ENDPOINT="http://127.0.0.1:$PORT"

# Start shoebox silently in the background
SHOEBOX_LOG=off "$SHOEBOX" --host 127.0.0.1 --port "$PORT" "$BUCKET_DIR" \
  > /dev/null 2>&1 &
SERVER_PID=$!
wait_for_server "$ENDPOINT"

extract_credentials "$BUCKET_DIR"
setup_aws_env "$ACCESS_KEY" "$SECRET_KEY" "$ENDPOINT"

# --- Parts ------------------------------------------------------------------

p01_upload() {
  note "Upload photos via standard AWS CLI"
  sleep "$DELAY"

  step "Upload a batch of photos"
  run "aws s3 cp '$DEMO_ROOT/uploads/' s3://photos/ --recursive"
}
part p01_upload "shoebox — S3 for your local files"

p02_list() {
  step "List what's in the bucket"
  run "aws s3 ls s3://photos/"
}
part p02_list "Browse the bucket"

p03_download() {
  step "Download a photo"
  run "aws s3 cp s3://photos/kashmir-1985.jpg '$DEMO_ROOT/downloaded.jpg'"

  step "Verify"
  run "ls -lh '$DEMO_ROOT/downloaded.jpg'"

  echo ""
  ok "Standard S3 tools. Local files. No cloud required."
  echo ""
}
part p03_download "Download & verify"

# --- Run --------------------------------------------------------------------

run_demo
