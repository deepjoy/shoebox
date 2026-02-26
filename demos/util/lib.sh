#!/usr/bin/env bash
# ============================================================================
# Shoebox Demo Library — shared helpers for all demo scripts
# ============================================================================
#
# Source this file from any demo script:
#   source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
#
# Environment variables:
#   DEMO_DELAY      — pause between steps (default 1.5s, set 0 for CI)
#   DEMO_END_DELAY  — hold on final frame before exit (default 3s, set 0 for CI)
# ============================================================================

# --- Config -----------------------------------------------------------------

DELAY="${DEMO_DELAY:-1.5}"
END_DELAY="${DEMO_END_DELAY:-4}"

# Disable the AWS CLI pager so demos don't hang waiting for user input.
export AWS_PAGER=""

# --- Palette — Akaroa / Coral Reef / Mongoose / Avocado / Como -------------

AKAROA='\033[38;2;216;198;176m'     # #d8c6b0 — headings
CORAL='\033[38;2;197;185;155m'      # #c5b99b — notes, dim text
MONGOOSE='\033[38;2;183;167;123m'   # #b7a77b — commands
AVOCADO='\033[38;2;142;155;105m'    # #8e9b69 — steps, success
COMO='\033[38;2;73;110;93m'         # #496e5d — banners, dividers
BOLD='\033[1m'
RESET='\033[0m'

# --- Helpers ----------------------------------------------------------------

banner() {
  clear
  echo ""
  echo -e "${COMO}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
  echo -e "${BOLD}${AKAROA}  $1${RESET}"
  echo -e "${COMO}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
  echo ""
  sleep "$DELAY"
}

step() {
  echo -e "\n${AVOCADO}▸ $1${RESET}"
  sleep "$DELAY"
}

run() {
  echo -e "${MONGOOSE}\$ $1${RESET}"
  local start_ns end_ns elapsed_ms output line_count
  start_ns=$(date +%s%N)
  output=$(eval "$1" 2>&1)
  end_ns=$(date +%s%N)
  elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
  line_count=$(printf '%s\n' "$output" | wc -l)
  if (( line_count > 80 )); then
    echo -e "  ${CORAL}# … showing last 80 of $line_count lines${RESET}"
    printf '%s\n' "$output" | tail -80
  else
    printf '%s\n' "$output"
  fi
  echo -e "  ${CORAL}(${elapsed_ms}ms)${RESET}"
  sleep "$DELAY"
}

note() {
  echo -e "  ${CORAL}# $1${RESET}"
}

ok() {
  echo -e "  ${AVOCADO}✓ $1${RESET}"
}

# --- Part registry ------------------------------------------------------------

PARTS=()

part() {
  PARTS+=("$1|$2")
}

run_demo() {
  for entry in "${PARTS[@]}"; do
    local fn="${entry%%|*}"
    local title="${entry#*|}"
    banner "$title"
    "$fn"
    sleep "$DELAY"
  done
  sleep "$END_DELAY"
}

# --- Setup helpers ---------------------------------------------------------

# Resolve the project root and shoebox binary path.
# Uses BASH_SOURCE[1] to find the calling script's location.
require_shoebox() {
  PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[1]}")/../.." && pwd)"
  SHOEBOX="$PROJECT_ROOT/target/release/shoebox"
  if [[ ! -x "$SHOEBOX" ]]; then
    echo "Error: shoebox binary not found at $SHOEBOX"
    echo "Run 'cargo build --release' first."
    exit 1
  fi
}

# Poll until the server responds to HTTP requests (up to 3 seconds).
# Usage: wait_for_server http://127.0.0.1:9876
wait_for_server() {
  local endpoint="$1"
  for _i in $(seq 1 30); do
    if curl -s -o /dev/null "$endpoint/" 2>/dev/null; then return 0; fi
    sleep 0.1
  done
  echo "Error: server did not become ready at $endpoint"
  exit 1
}

# Extract access_key_id and secret_access_key from a bucket's config.toml.
# Sets ACCESS_KEY and SECRET_KEY in the caller's scope.
# Usage: extract_credentials "$BUCKET_DIR"
extract_credentials() {
  local config="$1/.shoebox/config.toml"
  ACCESS_KEY=$(grep access_key_id "$config" | head -1 | cut -d'"' -f2)
  SECRET_KEY=$(grep secret_access_key "$config" | head -1 | cut -d'"' -f2)
}

# Export standard AWS environment variables for the AWS CLI.
# Usage: setup_aws_env "$ACCESS_KEY" "$SECRET_KEY" "$ENDPOINT"
setup_aws_env() {
  export AWS_ACCESS_KEY_ID="$1"
  export AWS_SECRET_ACCESS_KEY="$2"
  export AWS_DEFAULT_REGION=us-east-1
  export AWS_ENDPOINT_URL="$3"
}

# Signed curl request using curl's built-in AWS SigV4 signing.
# Reads AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL from env.
# Usage: signed_curl GET "/bucket/key?query" [-o /dev/null -w '%{http_code}']
signed_curl() {
  local method="$1"; shift
  local path="$1"; shift

  curl -s "$@" \
    -X "$method" \
    --aws-sigv4 "aws:amz:us-east-1:s3" \
    --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    "${AWS_ENDPOINT_URL}${path}"
}
