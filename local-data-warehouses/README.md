# Local Data Warehouses

This folder includes utilities to run data warehouses for local development. See the [Contributing guide](../CONTRIBUTING.md)
to ensure your environment is setup properly.

## DuckDB

By default, tests will run with DuckDB.

## PostgreSQL

We assume that you have PostgreSQL and Docker installed in your environment.

In a separate terminal window, run PostgreSQL in the background. Note - you MUST have Docker running on localhost in order for the PostgreSQL container to spin up.

```sh
make postgres
```

Then, when running `pytest`, ensure that `MF_SQL_ENGINE_URL` and `MF_SQL_ENGINE_PASSWORD` are setup
to access the PostgreSQL instance.

```sh
export MF_SQL_ENGINE_URL="postgresql://metricflow@localhost:5432/metricflow"
export MF_SQL_ENGINE_PASSWORD="metricflowing"

poetry run pytest tests/
```

## ClickHouse

ClickHouse support is **experimental**. MetricFlow compiles `SETTINGS join_use_nulls = 1`
onto generated SQL so unmatched LEFT/FULL OUTER JOIN cells are SQL NULL (ClickHouse
otherwise fills numeric/string cells with `0` / `''`). That contract lives on the
statement, not a session `SET`. This engine path does not emit `FINAL` for
ReplacingMergeTree / CDC tables, and it is not wired into hosted Semantic Layer /
Fusion.

We assume that you have Docker installed in your environment.

In a separate terminal window, run ClickHouse in the background. Note - you MUST have Docker running on localhost in order for the ClickHouse container to spin up.

```sh
make clickhouse
```

Then run the ClickHouse suite:

```sh
export MF_SQL_ENGINE_URL="clickhouse://metricflow@localhost:8123/metricflow"
export MF_SQL_ENGINE_PASSWORD="metricflowing"

make test-clickhouse
```

`hatch run clickhouse-env:pytest` (and `make test-clickhouse`) use the URL in
`pyproject.toml` (`localhost:8123`). If your container is published on a different
host port, invoke pytest with that environment's interpreter after exporting
`MF_SQL_ENGINE_URL` / `MF_SQL_ENGINE_PASSWORD`.

The ClickHouse container exposes:

- Port 8123: HTTP interface
- Port 9000: Native protocol interface

Default credentials:

- Database: `metricflow`
- User: `metricflow`
- Password: `metricflowing`
