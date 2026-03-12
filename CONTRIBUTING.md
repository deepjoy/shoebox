# Contributing to Shoebox

Thanks for your interest in contributing! This document covers the basics to get you started.

## Getting Started

### Prerequisites

- [Rust](https://rustup.rs/) (stable toolchain)
- [lefthook](https://github.com/evilmartians/lefthook) (git hooks)

### Setup

```bash
git clone https://github.com/deepjoy/shoebox.git
cd shoebox
lefthook install
cargo build
```

### Running Tests

```bash
cargo test --all-features
```

### Formatting and Linting

The project uses `cargo fmt` and `clippy`. Lefthook runs these automatically on pre-commit, but you can run them manually:

```bash
cargo fmt --check
cargo clippy --all-features -- -D warnings
```

## Making Changes

1. Fork the repository and create a branch from `main`.
2. Make your changes.
3. Add tests for new functionality.
4. Ensure `cargo test --all-features` passes.
5. Ensure `cargo clippy --all-features -- -D warnings` is clean.
6. Open a pull request against `main`.

## Commit Messages

This project uses [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add new feature
fix: correct a bug
docs: update documentation
refactor: restructure code without behavior change
chore: maintenance tasks
```

These are used by [release-plz](https://release-plz.ino.rs/) to auto-generate changelogs and determine version bumps.

## Project Structure

```
src/
├── main.rs          # CLI entrypoint
├── lib.rs           # Library root (Shoebox struct)
├── server/          # Axum HTTP server, S3 API routes
├── scanner/         # Filesystem scanning (L1/L2/L3)
├── auth/            # AWS SigV4 authentication
├── storage/         # Object storage operations
└── ...
crates/
└── taskmill/        # Internal task scheduler
docs/                # User-facing documentation
```

## Questions?

Open an issue or start a discussion — happy to help.
