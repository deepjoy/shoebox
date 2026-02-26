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

require_shoebox

# Track the current port so each run_shoebox instance uses a unique port.
NEXT_PORT=9870

# Helper: run shoebox, capture its output, then stop the server.
# Shoebox is a long-running server, so we background it and collect output.
# Automatically injects --host 127.0.0.1 --port <unique> unless the command
# already contains --port.
run_shoebox() {
  local outfile="$DEMO_ROOT/output.txt"
  local cmd="$1"

  # Auto-assign a unique port to avoid "Address already in use"
  local port="$NEXT_PORT"
  if [[ "$cmd" != *"--port"* ]]; then
    # Inject --host and --port before the first path argument
    cmd="${cmd/$SHOEBOX/$SHOEBOX --host 127.0.0.1 --port $port}"
  else
    # Extract the port from the command
    port=$(echo "$cmd" | grep -oP '(?<=--port )\d+')
  fi
  NEXT_PORT=$((NEXT_PORT + 1))

  local listen_url="http://127.0.0.1:$port"

  echo -e "${MONGOOSE}\$ $cmd${RESET}"
  # Use setsid so the server gets its own process group we can kill cleanly.
  setsid bash -c "$cmd" > "$outfile" 2>&1 &
  local pid=$!

  # Wait for server to be ready (or exit quickly for non-server commands)
  for _i in $(seq 1 30); do
    if ! kill -0 "$pid" 2>/dev/null; then break; fi
    if curl -s -o /dev/null "$listen_url/" 2>/dev/null; then break; fi
    sleep 0.1
  done

  # Give the startup banner a moment to flush
  sleep 0.2

  # Show captured output
  cat "$outfile"

  # Stop the server — kill the entire process group
  kill -- -"$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  sleep "$DELAY"
}

# --- Parts ------------------------------------------------------------------

p01_single_bucket() {
  note "Point shoebox at any directory to serve it as an S3 bucket."
  note "Credentials are generated automatically on first run."

  step "Serving a single directory (first run)"
  note "On first run, credentials are generated and displayed automatically."
  run_shoebox "SHOEBOX_LOG=off $SHOEBOX '$DEMO_ROOT/Photos'"
}
part p01_single_bucket "Shoebox — lightweight S3-compatible storage"

p02_subsequent_runs() {
  note "On subsequent runs, secrets are hidden by default."
  note "Pass --show-secrets to reveal them again."

  step "Re-running the same directory"
  run_shoebox "SHOEBOX_LOG=off $SHOEBOX '$DEMO_ROOT/Photos'"

  step "With --show-secrets"
  run_shoebox "SHOEBOX_LOG=off $SHOEBOX --show-secrets '$DEMO_ROOT/Photos'"
}
part p02_subsequent_runs "Subsequent runs"

p03_multiple_buckets() {
  note "Pass several paths to serve them all from one instance."

  step "Serving three directories"
  run_shoebox "SHOEBOX_LOG=off $SHOEBOX '$DEMO_ROOT/Photos' '$DEMO_ROOT/Videos' '$DEMO_ROOT/Documents'"
}
part p03_multiple_buckets "Multiple directories at once"

p04_custom_host_port() {
  note "Use --host and --port (or env vars) to change the listen address."

  step "Custom host and port"
  run_shoebox "SHOEBOX_LOG=off $SHOEBOX --host 127.0.0.1 --port 9999 '$DEMO_ROOT/Photos'"
}
part p04_custom_host_port "Custom Host & Port"

p99_done() {
  note "Each directory now has a .shoebox/config.toml with its credentials."
  step "Peeking at generated config"
  run "cat '$DEMO_ROOT/Photos/.shoebox/config.toml'"

  echo ""
  ok "That's shoebox — zero-config, no-copy, S3-compatible local storage."
  echo ""
}
part p99_done "Done!"

# --- Run --------------------------------------------------------------------

run_demo
