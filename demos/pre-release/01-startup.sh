#!/usr/bin/env bash
# ============================================================================
# Shoebox Demo — CLI startup with single and multiple directories
# ============================================================================
#
# Record with asciinema:
#   asciinema rec --cols 90 --rows 30 -c './docs/demo/startup.sh' demo.cast
#
# Replay:
#   asciinema play demo.cast
# ============================================================================

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../util/lib.sh"

# --- Setup: create disposable demo directories -----------------------------

DEMO_ROOT="$(mktemp -d)"
trap 'rm -rf "$DEMO_ROOT"' EXIT

mkdir -p "$DEMO_ROOT/Photos"
mkdir -p "$DEMO_ROOT/Videos"
mkdir -p "$DEMO_ROOT/Documents"

# Populate with sample files so the directories feel real
touch "$DEMO_ROOT/Photos/vacation.jpg" "$DEMO_ROOT/Photos/portrait.png"
touch "$DEMO_ROOT/Videos/clip-2024.mp4"
touch "$DEMO_ROOT/Documents/notes.md" "$DEMO_ROOT/Documents/report.pdf"

SHOEBOX="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/target/release/shoebox"

# Verify binary exists
if [[ ! -x "$SHOEBOX" ]]; then
  echo "Error: shoebox binary not found at $SHOEBOX"
  echo "Run 'cargo build --release' first."
  exit 1
fi

# ============================================================================
# Part 1 — Single bucket
# ============================================================================

banner "Shoebox — lightweight S3-compatible storage"

note "Point shoebox at any directory to serve it as an S3 bucket."
note "Credentials are generated automatically on first run."
sleep "$DELAY"

step "Serving a single directory (first run)"
note "On first run, credentials are generated and displayed automatically."
run "SHOEBOX_LOG=off $SHOEBOX '$DEMO_ROOT/Photos'"

# ============================================================================
# Part 2 — Subsequent run hides secrets
# ============================================================================

banner "Subsequent runs"

note "On subsequent runs, secrets are hidden by default."
note "Pass --show-secrets to reveal them again."
sleep "$DELAY"

step "Re-running the same directory"
run "SHOEBOX_LOG=off $SHOEBOX '$DEMO_ROOT/Photos'"

step "With --show-secrets"
run "SHOEBOX_LOG=off $SHOEBOX --show-secrets '$DEMO_ROOT/Photos'"

# ============================================================================
# Part 3 — Multiple buckets
# ============================================================================

banner "Multiple directories at once"

note "Pass several paths to serve them all from one instance."
sleep "$DELAY"

step "Serving three directories"
run "SHOEBOX_LOG=off $SHOEBOX '$DEMO_ROOT/Photos' '$DEMO_ROOT/Videos' '$DEMO_ROOT/Documents'"

# ============================================================================
# Part 4 — Custom host and port
# ============================================================================

step "Custom host and port"
note "Use --host and --port (or env vars) to change the listen address."
sleep "$DELAY"

run "SHOEBOX_LOG=off $SHOEBOX --host 127.0.0.1 --port 8080 '$DEMO_ROOT/Photos'"

# ============================================================================
# Done
# ============================================================================

banner "Done!"
note "Each directory now has a .shoebox/config.toml with its credentials."
step "Peeking at generated config"
run "cat '$DEMO_ROOT/Photos/.shoebox/config.toml'"

echo ""
ok "That's shoebox — zero-config, no-copy, S3-compatible local storage."
echo ""
