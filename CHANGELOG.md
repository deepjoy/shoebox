# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/deepjoy/shoebox/releases/tag/v0.1.0) - 2026-02-21

### Added

- *(multipart)* implement S3 multipart upload lifecycle ([#17](https://github.com/deepjoy/shoebox/pull/17))
- *(object)* add common S3 object operations — copy, rename, range, conditionals, tagging ([#16](https://github.com/deepjoy/shoebox/pull/16))
- *(auth)* add AWS Signature Version 4 authentication and credential management ([#14](https://github.com/deepjoy/shoebox/pull/14))
- *(demos)* add week 1 social demo and end-delay support
- *(config)* add --data-dir option and decouple state storage from bucket root
- *(cli)* hide secrets by default and add --show-secrets flag
- *(demos)* add CLI startup demo with asciinema recording
- *(cli)* add binary entry point with tracing and bucket startup
- *(metadata)* add SQLite-backed object metadata store with migration
- *(storage)* add filesystem-backed storage layer with symlink safety
- *(config)* add bucket config loading, resolution, and tests
- *(config)* add bucket configuration, credential generation, and CLI scaffolding
- *(error)* add S3 bucket name validation with comprehensive tests
- *(error)* add XML response rendering and error conversions for S3Error
- *(error)* add S3-compatible error type with HTTP status mapping
- *(shoebox)* scaffold Rust library crate with project metadata

### Fixed

- *(auth)* register global credentials in provider before server startup ([#15](https://github.com/deepjoy/shoebox/pull/15))
- move AWS_PAGER to shared lib and rename initial migration
- *(demos)* add timing, clear screen, and disable AWS pager
- address code review findings for S3 API layer
- harden config file creation, error responses, and storage usage

### Other

- *(release)* add release-plz workflow for automated releases ([#18](https://github.com/deepjoy/shoebox/pull/18))
- link TODO comments to GitHub issues #9, #10
- Merge branch 'main' into core-operations
- link TODO comments to GitHub issues #4, #5, #6, #7
- reformat map_err closure in config serialization
- add GitHub Actions workflow and apply rustfmt/clippy fixes
- add IDE and dev environment directories to .gitignore
- *(shoebox)* add initial README with project vision and early-stage status
- Initial commit
