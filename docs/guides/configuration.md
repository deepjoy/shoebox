# Configuration

Shoebox is zero-config by default — `shoebox ~/Photos` just works. Configuration becomes useful when you want custom credentials, multiple buckets from a config file, or state stored separately from your data.

## Per-Bucket Config

Each bucket stores its configuration in `.shoebox/config.toml` inside the bucket directory. This file is auto-generated on first run with an admin credential.

```
~/Photos/
├── your-files/
└── .shoebox/
    ├── config.toml      # Credentials + settings (0600 permissions)
    ├── metadata.db      # SQLite database
    └── parts/           # Multipart upload temp files
```

### Format

```toml
# Optional: override the bucket name (defaults to directory name)
bucket_name = "my-photos"

[[credentials]]
access_key_id = "AKIAFQA4RDZ3OQYV5VZF"
secret_access_key = "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"
description = "Full access (admin)"

[[credentials]]
access_key_id = "AKIAW7NEXAMPLE12345"
secret_access_key = "anotherSecretKey1234567890123456789012"
description = "Gallery viewer"
permissions = ["read"]
```

### Fields

| Field | Required | Description |
|-------|----------|-------------|
| `bucket_name` | No | Custom bucket name. Defaults to the directory name, lowercased and sanitized. |
| `credentials` | Auto-generated | Array of access credentials. See [Credentials](credentials.md). |

### File Permissions

On Unix systems, `config.toml` is created with `0600` permissions (owner read/write only). This prevents other users on the system from reading your credentials.

## Global Config

A global config file lets you define buckets, credentials, and server settings in one place. Pass it with `--config` or the `SHOEBOX_CONFIG` environment variable.

```bash
shoebox --config /etc/shoebox.toml
# or
SHOEBOX_CONFIG=/etc/shoebox.toml shoebox
```

### Format

```toml
host = "127.0.0.1"
port = 8080
buckets = ["/home/user/Photos", "/home/user/Documents"]

# Increase for high-churn environments (rsync, archive extraction)
watch_channel_capacity = 5000

# Global credentials apply to ALL buckets
[[credentials]]
access_key_id = "AKIAGLOBAL1234567890"
secret_access_key = "globalSecretKey1234567890123456789012345"
description = "Cross-bucket admin"
```

### Fields

| Field | Default | Description |
|-------|---------|-------------|
| `host` | `0.0.0.0` | Listen address |
| `port` | `9000` | Listen port |
| `buckets` | `[]` | Bucket directory paths (alternative to CLI positional args) |
| `credentials` | `[]` | Global credentials that work across all buckets |
| `watch_channel_capacity` | `1000` | Filesystem event channel size. Increase if you see "channel overflow" warnings during bulk file operations. |

When both a global config and CLI args are provided, CLI bucket paths are added to any buckets defined in the config file. CLI `--host` and `--port` flags override the config file values.

### Minimal Global Config

```toml
buckets = ["/home/user/Photos"]
```

Everything else is optional. Each bucket still auto-generates its own credential in `.shoebox/config.toml` on first run.

## Data Directory Mode

By default, Shoebox stores its state (config, SQLite database, multipart parts) in a `.shoebox/` directory inside each bucket. If the bucket directory is read-only (e.g., a mounted NAS share), use `--data-dir` to store state elsewhere:

```bash
shoebox --data-dir /var/lib/shoebox ~/Photos /mnt/nas/documents
```

State layout with `--data-dir`:

```
/var/lib/shoebox/           # --data-dir
├── photos/
│   ├── config.toml
│   ├── metadata.db
│   └── parts/
└── documents/
    ├── config.toml
    ├── metadata.db
    └── parts/

~/Photos/                   # Bucket root (can be read-only)
└── your-files/

/mnt/nas/documents/         # Bucket root (can be read-only)
└── your-files/
```

## Bucket Name Derivation

Shoebox derives bucket names from directory names:

| Directory | Bucket Name | Notes |
|-----------|------------|-------|
| `/home/user/Photos` | `photos` | Lowercased |
| `/home/user/My Photos` | `my-photos` | Spaces become hyphens |
| `/home/user/My_Cool_Photos!` | `my-cool-photos` | Invalid chars become hyphens, trailing punctuation trimmed |
| `/home/user/ab` | `ab-bucket` | Padded to minimum 3 characters |
| `/home/user/My---Photos` | `my-photos` | Consecutive hyphens collapsed |

To override the derived name, set `bucket_name` in the per-bucket config:

```toml
# .shoebox/config.toml
bucket_name = "vacation-2024"
```

Bucket names must be valid S3 bucket names: 3-63 characters, lowercase letters, numbers, hyphens, and periods.

## Environment Variables

All serve-mode options can be set via environment variables:

| Variable | Flag Equivalent | Default |
|----------|----------------|---------|
| `SHOEBOX_HOST` | `--host` | `0.0.0.0` |
| `SHOEBOX_PORT` | `--port` | `9000` |
| `SHOEBOX_DATA_DIR` | `--data-dir` | _(none)_ |
| `SHOEBOX_CONFIG` | `--config` | _(none)_ |
| `SHOEBOX_LOG` | — | `info` |
| `RUST_LOG` | — | _(fallback for SHOEBOX_LOG)_ |

**Precedence:** CLI flags > environment variables > global config file > defaults.

## See Also

- [Credentials](credentials.md) — Managing access keys and permissions
- [CLI Reference](cli-reference.md) — All commands and flags
- [Troubleshooting](troubleshooting.md) — Common configuration issues
