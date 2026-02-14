#!/usr/bin/env bash
# ============================================================================
# Shoebox Demo Library — shared helpers for all demo scripts
# ============================================================================
#
# Source this file from any demo script:
#   source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
#
# Environment variables:
#   DEMO_DELAY  — pause between steps (default 1.5s, set 0 for CI)
# ============================================================================

# --- Config -----------------------------------------------------------------

DELAY="${DEMO_DELAY:-2.5}"

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
  eval "$1"
  sleep "$DELAY"
}

note() {
  echo -e "  ${CORAL}# $1${RESET}"
}

ok() {
  echo -e "  ${AVOCADO}✓ $1${RESET}"
}
