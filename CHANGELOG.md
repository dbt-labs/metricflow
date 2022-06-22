# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.100.0] - 2022-06-22

### Breaking Changes
- Updated MetricFlow config parameters for BigQuery users. See description on https://github.com/transform-data/metricflow/pull/62 for usage instructions.

### Added
- Metric / dimension SQL validations via DW engine (@QMalcolm)
- Support for PostgreSQL and setup for local testing (@rexledesma)
- Support for DuckDB (@plypaul)
- Support for individual user logins to BigQuery (@WilliamDee)
- Ability to create the MetricFlow client from a model in a specified directory (@WilliamDee)
- Description and owner fields to model objects (@WilliamDee)

### Updated
- CLI tests without mocks (@WilliamDee)
- Package dependencies to address security issues (@alliehowe)
- Query cancellation behavior on CLI exit (@plypaul)

## [0.93.0] - 2022-04-27

### Added
- Simple Developer API for interacting with MetricFlow engine based on a local config file for Warehouse credentials (@williamdee)
- Config validation for Materialization structs (@lebca)
- Config templates for useful metrics sourced from Salesforce data, for use by end users of MetricFlow (@JStein77)


## [0.92.1] - 2022-04-13

### Added

- Ability to visualize DataFlow Plan from the command line via the `--display-plans` flag (@plypaul)

### Fixed

- Resolved BigQuery read credential failures on multi-line service account keys (@zzsza)
- Resolved ciso8601 installation failures on Ubuntu (@plypaul)
- Merge-blocking CI test failures and configuration errors (@marcodamore, @tlento)
- Typing errors in MeasureReference (@lebca)

### Removed
- Remove legacy interfaces (@andykram)
- Remove legacy time_format parameters from test models (@belindabennett)

### Updated

- Tutorial updates and extensions (@nhandel)
- README improvements (@allegraholland, @belindabennett, @lebca, @tlento)
- Contributor guide (@tlento)
- CLI versioning (@williamdee)
- CLI dependency restructuring in preparation for API extensions (@williamdee)

### Special thanks

Special thanks to @zzsza for the quick fix for our BigQuery token parsing bug!