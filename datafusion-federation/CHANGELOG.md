# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.4](https://github.com/datafusion-contrib/datafusion-federation/compare/datafusion-federation-v0.3.3...datafusion-federation-v0.3.4) - 2025-01-12

### Other

- upgrade datafusion to 44 (#103)

## [0.3.3](https://github.com/datafusion-contrib/datafusion-federation/compare/datafusion-federation-v0.3.2...datafusion-federation-v0.3.3) - 2025-01-04

### Fixed

- handle `LogicalPlan::Limit` separately to preserve skip and offset in `rewrite_table_scans` (#101)

## [0.3.2](https://github.com/datafusion-contrib/datafusion-federation/compare/datafusion-federation-v0.3.1...datafusion-federation-v0.3.2) - 2024-12-05

### Other

- Release plz action: install required dependencies ([#85](https://github.com/datafusion-contrib/datafusion-federation/pull/85))
