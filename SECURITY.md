# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in Shoebox, please report it responsibly.

**Email:** [code@deepjoy.com](mailto:code@deepjoy.com)

Please include:
- A description of the vulnerability
- Steps to reproduce
- Potential impact

I'll acknowledge your report within 48 hours and aim to release a fix within 7 days for critical issues. Please don't open a public issue for security vulnerabilities.

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.3.x   | Yes       |
| < 0.3   | No        |

## Security Model

### Authentication

Shoebox uses **AWS Signature V4** for request authentication — the same protocol used by AWS S3. Every request must be signed with a valid access key and secret. See [credentials documentation](docs/guides/credentials.md) for details.

### Credential Storage

- Credentials are stored in plain text in `.shoebox/config.toml` inside each bucket directory.
- On Unix systems, credential files are created with `0600` permissions (owner read/write only).
- Secrets are shown once on first run and hidden on subsequent starts unless `--show-secrets` is passed.

### Network Exposure

- Shoebox binds to `0.0.0.0:9000` by default. If your machine is network-accessible, Shoebox is too.
- There is **no TLS** — Shoebox serves plain HTTP. Use a reverse proxy (Traefik, nginx, Caddy) for HTTPS.
- CORS rules are configurable per bucket. No origins are allowed by default.

### Integrity Verification

- Every file is hashed (SHA-256) during background scanning.
- Scheduled integrity checks run every 24 hours to detect bit rot or silent corruption.
- See [integrity documentation](docs/guides/integrity.md) for details.

## Known Limitations

- **No encryption at rest.** Files are stored as-is on the filesystem. Use full-disk encryption (LUKS, FileVault, BitLocker) if you need encryption at rest.
- **No TLS termination.** Shoebox serves HTTP only. Deploy behind a reverse proxy for HTTPS.
- **Single-machine scope.** Shoebox is not designed for distributed or multi-node deployments. There is no replication or consensus protocol.
- **Plain-text credentials.** Credential files are protected by filesystem permissions, not encryption.
- **No rate limiting.** Shoebox does not throttle requests. Use a reverse proxy if you need rate limiting.
