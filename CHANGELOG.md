# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
