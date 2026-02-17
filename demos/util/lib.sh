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
