# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MetricFlow is a semantic layer that simplifies defining and managing metrics by compiling metric definitions into clear, reusable SQL. It supports complex metric types, multi-hop joins, and aggregation to different time granularities. The project uses a dataflow-based query planning approach where metric requests are compiled into optimized, engine-specific SQL.

## Architecture

The repository is structured as a multi-package Python project:

- **`metricflow/`** - Main MetricFlow package containing the query execution engine
  - `dataflow/` - Dataflow plan optimization and execution nodes
  - `plan_conversion/` - Converts dataflow plans to SQL plans
  - `sql/` - SQL generation and rendering
  - `engine/` - Database adapter implementations
  - `execution/` - Query execution framework

- **`metricflow-semantics/`** - Core semantic layer definitions and interfaces
  - `model/` - Semantic model definitions (metrics, dimensions, entities)
  - `specs/` - Specification classes for query components
  - `semantic_graph/` - Graph representation of semantic relationships
  - `query/` - Query parsing and validation

- **`dbt-metricflow/`** - dbt integration package
- **`metricflow-semantic-interfaces/`** - Public API interfaces

## Development Commands

### Environment Setup
```bash
# Install Hatch package manager
make install-hatch

# All commands run through Hatch environments defined in pyproject.toml
# Default environment is 'dev-env' which uses DuckDB
```

### Testing
```bash
# Run full test suite (excludes slow tests)
make test

# Run all tests including slow ones
make test-include-slow

# Run tests for specific database engines
make test-postgresql
make test-bigquery
make test-databricks
make test-redshift
make test-snowflake
make test-trino
make test-athena

# Run single test file
hatch run dev-env:pytest tests_metricflow/path/to/test.py

# Run tests by name pattern
hatch run dev-env:pytest -k "test_pattern" tests_metricflow/

# Regenerate test snapshots
make regenerate-test-snapshots

# Run performance tests
make perf
```

### Code Quality
```bash
# Run all linters and formatters
make lint

# The project uses:
# - Black (formatting, line length 120)
# - Ruff (linting with Google docstring convention)
# - MyPy (type checking)
# - Pre-commit hooks for validation
```

### Database-Specific Development
The project supports multiple SQL engines through Hatch environments. Set these environment variables for non-DuckDB testing:
```bash
export MF_SQL_ENGINE_URL="<connection_url>"
export MF_SQL_ENGINE_PASSWORD="<password>"
export MF_TEST_ADAPTER_TYPE="<engine_type>"
```

### Interactive CLI
```bash
# Use local MetricFlow CLI (must run from dbt project directory)
hatch run dev-env:mf --help
hatch run dev-env:mf tutorial
```

## Key Design Principles

From TENETS.md, the project follows these core principles:

1. **DRY (Don't Repeat Yourself)** - Avoid duplication through thoughtful abstractions
2. **SQL-centric compilation** - All metric logic remains inspectable as SQL
3. **Maximal Flexibility** - Support any metric on any data model aggregated to any dimension

SQL generation prioritizes: Correctness → Performance → Legibility → Ease of Manipulation

## Testing Strategy

- **Integration tests** - Located in `tests_metricflow/integration/test_cases/` with YAML config files
- **Unit tests** - Component and module tests mirror the main package structure
- **Snapshot testing** - Expected outputs stored in `tests_metricflow/snapshots/`
- **Multi-engine testing** - All tests can run against different SQL engines

## Code Style

- Line length: 120 characters
- Required import: `from __future__ import annotations`
- Google-style docstrings
- Type hints required
- First-party imports: `dbt_metricflow`, `metricflow`