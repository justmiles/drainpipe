# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Cloudflare provider default rate limiters
- Cloudflare logpush job missing quals resolution

### Changed

- Cloudflare zone setting composite key now includes id and zone_id

### Fixed

- Cloudflare provider key resolution and reserved keyword column quoting
- Cloudflare logpush bad request handling and reduced rate limit defaults
- AWS organizations account rows no longer incorrectly filtered as cross-account

## [0.1.1] - 2026-06-01

### Added

- Command to validate configs
- Support for custom key columns
- Azure tenant-level subscription auto-discovery
- HCL configuration support with environment variable interpolation
- Docker build integration to GitHub workflow

### Changed

- Switched configuration format from YAML to HCL

### Removed

- ARM architecture from release packages

### Fixed

- JSON null literals now correctly treated as SQL NULL instead of string "null"
- Free disk space and sequential builds for large dependency trees
- GoReleaser configuration with shell hook and Docker v2 support
