# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Cloudflare provider default rate limiters
- Missing quals resolution for cloudflare_logpush_job

### Changed

- Cloudflare_zone_setting now uses composite key [id, zone_id]

### Fixed

- Cloudflare provider key resolution and reserved keyword column quoting
- Cloudflare logpush bad request handling and rate limit defaults
- AWS organizations account cross-account filtering

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

## [0.1.0] - 2026-04-29

### Added

- HCL configuration support for environment variable interpolation
- Azure tenant-level subscription auto-discovery

### Fixed

- JSON null literals now correctly treated as SQL NULL instead of string "null"


## [0.0.3] - 2026-04-28

### Changed

- Configuration now uses HCL instead of YAML


## [0.0.2] - 2026-03-27

### Added

- Docker build integration to GitHub workflow

### Changed

- Improved build process

### Removed

- ARM architecture from release packages


## [0.0.1] - 2026-03-21

### Added

- Initial release of Drainpipe tool
- GitHub Actions workflow for CI/CD
- GoReleaser configuration for automated releases

### Fixed

- Disk space management and sequential builds for large dependency trees
- GoReleaser shell hook and Docker v2 configuration

