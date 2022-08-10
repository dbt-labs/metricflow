# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.111.0] - 2022-08-10

### Added
- Early beta for data source inference - Snowflake users can now run a command to bootstrap data source definitions based on warehouse metadata for tables in a given schema. Check out `mf infer --help` for more details. Feedback welcome! (@serramatutu)
- Support for semi-additive measures - semi-additive measures are measures which can be aggregated across some dimensions, but not others. The most common use case for this is an end-of-period measure, such as a statement balance or end of day inventory hold. As of this release we support semi-additive restrictions bound by any time dimension. See the description on [PR #183](https://github.com/transform-data/metricflow/pull/183) for more details on usage. (@WilliamDee)
- Expanded data warehouse validations - data warehoue validation runs will now check to ensure all measures, identifiers, and dimensions defined in data sources are queryable. (@QMalcolm)

### Fixed
- Resolved serialization error on custom Pydantic objects in the model parser (@tlento)
- Cleaned up error message handling and missing test depencies on model validation (@tlento)
- Eliminated a class of circular import errors by forking reference classes from the specs module (@WilliamDee)
- Fixed error state when specifying a where constraint with metric_time (@plypaul)
- Removed unused jsonschema element (@QMalcolm)

### Updated
- Updated the tutorial to use metric_time instead of arbitrary time dimensions (@plypaul)
- Increased strictness of typechecking enforcement (@tlento)
- Removed unnecessary "all time" WHERE clause rendering for cases where time constraints were not provided (@WilliamDee)
- Updated Click version from `^8.1.3` to `>=7.1.2` to temporarily resolve dependency issue for downstream Flask `1.1.4` usage (@jack-transform)
- Updated Jinja2 version from `2.11.0` to `>=2.11.0` to allow downstream Flask `1.1.4` users to update to `2.x.x` (@jpreillymb, @tlento)

## [0.110.1] - 2022-07-25

### Fixed
- Cleaned up package metadata and broken links in external README viewers (@tlento)
- Fixed cases where output columns were rendering in the wrong order (@WilliamDee)

### Updated
- Improved model config schema validation to return all jsonschema errors instead of one at a time (@QMalcolm)
- Streamlined rendered SQL to use BETWEEN expressions for date range checks (@WilliamDee)

## [0.110.0] - 2022-07-21

### Breaking Changes
- Updated query inputs for time series dimensions to use `metric_time` instead of dimension names, since it is now possible for measures to have different time dimensions for time series aggregation. This also removes the restriction that all data sources have the same primary time dimension name. However, users issuing queries might experience exceptions if they are not using `metric_time` as their requested primary time dimension. (@plypaul)
- Added enforcement for new reserved keyword `metric_time` (@tlento)
- Reordered column output to `time dimension, identifiers, dimensions, metrics`, which could break automation relying on order-dependent parsing of CLI output. We encourage affected users to switch to using the API, and to access the resulting data frame with order-independent (i.e., by name) access to column values. (@WilliamDee)
- Removed support for SQLite - expected impact is minimal as the repo has been cut to DuckDB for in memory testing (@plypaul)

### Added
- Support for specifying measure-specific time dimensions for time series aggregations (@plypaul)
- Validation against use of SQL reserved keywords as element names, which previously would have resulted in SQL errors at query time (@QMalcolm)

### Updated
- Improved code structure around the model validation process (@QMalcolm)
- Improved readability of model validation errors in CLI output (@QMalcolm)
- Cleaned up DuckDB documentation (@yanghua)
- Fixed edge case with model parsing where a constraint like `ds = CURRENT_DATE()` would throw an error (@tlento)
- Restructured config parser to rely more on Pydantic's built in object parsing, allowing for more - and more standardized - customization on input types and parsing mechanics (@tlento)
- Removed framework operation comments from SQL explain plans by default, added a flag to override (@WilliamDee, @nhandel)
- Updated Click to 8.1.3 to resolve dependency conflicts and facilitate later addition of command completion (@jack-transform)
- Improved developer documentation and workflows for PostgreSQL

## [0.100.2] 2022-06-24

## Updated
- Updated numpy version to 1.23.0 to fix issues with operations on m1 macbooks (@serramatutu)
- Downgraded DuckDB version to 0.3.4 to fix segfaults observed with multithreaded operations (@plypaul)
- Added YAML linting to model validation process to catch syntax errors more explicitly (@QMalcolm)


## [0.100.1] - 2022-06-23

### Updated
- Fixed CLI support for PostgreSQL (@plypaul)
- Improved validation error messaging (@QMalcolm)

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