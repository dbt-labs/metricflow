# Local Data Warehouses

This folder includes utilities to run data warehouses for local development. See the [Contributing guide](../CONTRIBUTING.md)
to ensure your environment is setup properly.

## SQLite

We assume that you have SQLite installed in your environment. By default, tests will run with SQLite.

## PostgreSQL

We assume that you have PostgreSQL and Docker installed in your environment.

In a separate terminal window, run PostgreSQL in the background.

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
