# Shoebox Documentation

## Getting Started

- [Installation](installation.md) — Docker, cargo install, from source
- [Quickstart](quickstart.md) — Running in 5 minutes

## Guides

### Configuration & Setup
- [Configuration](guides/configuration.md) — Global config, per-bucket config, data directory, environment variables
- [Credentials](guides/credentials.md) — Authentication, permissions, managing access keys

### Using with S3 Tools
- [S3 Compatibility](guides/s3-compatibility.md) — Supported operations, AWS CLI, rclone, SDK setup
- [Pre-signed URLs](guides/presigned-urls.md) — Temporary download/upload links without credentials
- [CORS](guides/cors.md) — Browser access for web applications

### File Intelligence
- [Duplicate Detection](guides/duplicates.md) — Finding and merging duplicate files
- [Integrity Checking](guides/integrity.md) — Detecting bit rot and verifying file integrity

### Events & Automation
- [Webhooks](guides/webhooks.md) — Event notifications for file changes

### Reference
- [CLI Reference](guides/cli-reference.md) — All commands, flags, and options
- [Troubleshooting](guides/troubleshooting.md) — Common issues and solutions

## Understanding Shoebox

- [Why Shoebox?](why-shoebox.md) — The problem, the approach, who it's for
- [When Not to Use Shoebox](when-not-to-use-shoebox.md) — Honest limitations

## Architecture (for contributors)

- [Library API](architecture/library-api.md) — Embedding Shoebox as a Rust library
- [Library Consumers](architecture/library-consumers.md) — Integration guide for Rust developers
- [No Versioning](architecture/no-versioning.md) — Why Shoebox doesn't implement S3 versioning
