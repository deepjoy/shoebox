#!/bin/sh
set -e

# Find the first path argument (skip flags like --host, --port, etc.)
DATA_DIR=""
for arg in "$@"; do
  case "$arg" in
    -*) ;; # skip flags
    *)  DATA_DIR="$arg"; break ;;
  esac
done

# Default to /data if no path argument found
DATA_DIR="${DATA_DIR:-/data}"

# If running as root AND the data dir exists, match its owner
if [ "$(id -u)" = "0" ] && [ -d "$DATA_DIR" ]; then
  TARGET_UID=$(stat -c '%u' "$DATA_DIR")
  TARGET_GID=$(stat -c '%g' "$DATA_DIR")

  # If the dir is owned by root, just run directly
  if [ "$TARGET_UID" = "0" ]; then
    exec shoebox "$@"
  fi

  # Create a shoebox user/group with the matching UID/GID
  getent group "$TARGET_GID" >/dev/null 2>&1 || addgroup --gid "$TARGET_GID" shoebox
  GROUP_NAME=$(getent group "$TARGET_GID" | cut -d: -f1)
  id -u shoebox >/dev/null 2>&1 || adduser --disabled-password --gecos "" --uid "$TARGET_UID" --ingroup "$GROUP_NAME" --no-create-home shoebox

  exec gosu shoebox shoebox "$@"
fi

exec shoebox "$@"
