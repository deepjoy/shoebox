# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.3](https://github.com/deepjoy/shoebox/compare/shoebox-v0.3.2...shoebox-v0.3.3) - 2026-03-11

### Fixed

- spawn abandoned multipart upload cleanup loop in Shoebox::run() ([#66](https://github.com/deepjoy/shoebox/pull/66))

## [0.3.2](https://github.com/deepjoy/shoebox/compare/shoebox-v0.3.1...shoebox-v0.3.2) - 2026-03-11

### Fixed

- use cargo-chef for Docker layer caching instead of ephemeral BuildKit mounts ([#64](https://github.com/deepjoy/shoebox/pull/64))

## [0.3.1](https://github.com/deepjoy/shoebox/compare/shoebox-v0.3.0...shoebox-v0.3.1) - 2026-03-11

### Added

- *(api)* add bucket stats endpoint ([#60](https://github.com/deepjoy/shoebox/pull/60))

### Other

- fix inconsistencies across project documentation  ([#63](https://github.com/deepjoy/shoebox/pull/63))
- update README for v0.3.0 and add project documentation ([#62](https://github.com/deepjoy/shoebox/pull/62))

## [0.3.0](https://github.com/deepjoy/shoebox/compare/shoebox-v0.2.1...shoebox-v0.3.0) - 2026-03-05

### Added

- add duplicate detection, integrity checking, and directory comparison ([#49](https://github.com/deepjoy/shoebox/pull/49))
- add sync endpoint with move detection and inode tracking ([#48](https://github.com/deepjoy/shoebox/pull/48))
- *(taskmill)* type-keyed state map with post-build injection ([#46](https://github.com/deepjoy/shoebox/pull/46))
- add S3 additional checksums (SHA-256, SHA-1, CRC32, CRC32C) ([#42](https://github.com/deepjoy/shoebox/pull/42))
- *(taskmill)* add adaptive priority task scheduler with IO-aware concurrency ([#38](https://github.com/deepjoy/shoebox/pull/38))

### Other

- *(docker)* enable PR trigger and copy crates dir in Dockerfile ([#47](https://github.com/deepjoy/shoebox/pull/47))
- *(taskmill)* separate priority from task payload, upgrade on dedup ([#44](https://github.com/deepjoy/shoebox/pull/44))
- remove versioning_enabled field from BucketConfig ([#43](https://github.com/deepjoy/shoebox/pull/43))
- *(scanner)* migrate from custom scheduler to taskmill ([#41](https://github.com/deepjoy/shoebox/pull/41))

## [0.2.1](https://github.com/deepjoy/shoebox/compare/v0.2.0...v0.2.1) - 2026-02-26

### Fixed

- *(docker)* use explicit Docker Hub repo name for image publishing ([#36](https://github.com/deepjoy/shoebox/pull/36))

## [0.2.0](https://github.com/deepjoy/shoebox/compare/v0.1.0...v0.2.0) - 2026-02-26

### Added

- add Dockerfile and multi-arch Docker publishing workflow ([#35](https://github.com/deepjoy/shoebox/pull/35))
- *(scanner)* improve reliability, performance, and observability ([#33](https://github.com/deepjoy/shoebox/pull/33))
- *(scanner)* add batch limits and continuation to scan worker loop ([#32](https://github.com/deepjoy/shoebox/pull/32))
- *(config)* add GlobalConfig support to ShoeboxBuilder ([#31](https://github.com/deepjoy/shoebox/pull/31))
- *(metadata)* add streaming list_objects_stream API ([#29](https://github.com/deepjoy/shoebox/pull/29))

### Fixed

- *(bucket)* add non_exhaustive attribute to LoadedBucket ([#28](https://github.com/deepjoy/shoebox/pull/28))

### Other

- extract typed ShoeboxError and migrate main.rs to builder API ([#34](https://github.com/deepjoy/shoebox/pull/34))
- *(bucket)* unify BucketRuntime into LoadedBucket and fix watcher lifetime ([#26](https://github.com/deepjoy/shoebox/pull/26))

## [0.1.0](https://github.com/deepjoy/shoebox/compare/v0.1.0-alpha.2...v0.1.0) - 2026-02-24

### Other

- *(readme)* update project status for v0.1.0 release ([#25](https://github.com/deepjoy/shoebox/pull/25))
