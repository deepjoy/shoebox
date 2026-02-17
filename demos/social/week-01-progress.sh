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
trap 'kill "$SERVER_PID" 2>/dev/null; wait "$SERVER_PID" 2>/dev/null; rm -rf "$DEMO_ROOT"' EXIT

BUCKET_DIR="$DEMO_ROOT/photos"
mkdir -p "$BUCKET_DIR"

# Create sample photo files to upload
mkdir -p "$DEMO_ROOT/uploads"
echo "JFIF-binary-data-kashmir-1985"   > "$DEMO_ROOT/uploads/kashmir-1985.jpg"
echo "JFIF-binary-data-beach-2019"     > "$DEMO_ROOT/uploads/beach-2019.jpg"
echo "JFIF-binary-data-birthday-party" > "$DEMO_ROOT/uploads/birthday-party.jpg"
echo "JFIF-binary-data-graduation"     > "$DEMO_ROOT/uploads/graduation.jpg"
echo "JFIF-binary-data-first-snow"     > "$DEMO_ROOT/uploads/first-snow.jpg"

SHOEBOX="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/target/release/shoebox"
PORT=9876
ENDPOINT="http://127.0.0.1:$PORT"

if [[ ! -x "$SHOEBOX" ]]; then
  echo "Error: shoebox binary not found. Run 'cargo build --release' first."
  exit 1
fi

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL="$ENDPOINT"

# Start shoebox silently in the background
SHOEBOX_LOG=off "$SHOEBOX" --host 127.0.0.1 --port "$PORT" "$BUCKET_DIR" \
  > /dev/null 2>&1 &
SERVER_PID=$!

# Wait for ready
for i in $(seq 1 30); do
  curl -s -o /dev/null "$ENDPOINT/" 2>/dev/null && break
  sleep 0.1
done

# ============================================================================
# The demo — what the viewer sees
# ============================================================================

banner "shoebox — S3 for your local files"

# --- Beat 1: Upload photos -------------------------------------------------

note "Upload photos via standard AWS CLI"
sleep "$DELAY"

step "Upload a batch of photos"
run "aws s3 cp '$DEMO_ROOT/uploads/' s3://photos/ --recursive"

# --- Beat 2: List them via S3 ----------------------------------------------

step "List what's in the bucket"
run "aws s3 ls s3://photos/"

# --- Beat 3: Download one back ---------------------------------------------

step "Download a photo"
run "aws s3 cp s3://photos/kashmir-1985.jpg '$DEMO_ROOT/downloaded.jpg'"

step "Verify"
run "ls -lh '$DEMO_ROOT/downloaded.jpg'"

# --- Done -------------------------------------------------------------------

echo ""
ok "Standard S3 tools. Local files. No cloud required."
echo ""
sleep "$END_DELAY"
