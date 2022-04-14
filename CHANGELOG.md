# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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