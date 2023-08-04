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

poetry run pytest metricflow/test/
```
