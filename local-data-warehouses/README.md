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

Then, ensure that `MF_SQL_ENGINE_URL` and `MF_SQL_ENGINE_PASSWORD` are setup to access the PostgreSQL instance. Our test runs are configured to do this automatically via environment configurations. To run all tests try:

```sh
make test-postgresql
```

To run some subset of tests in the postgres environment, you can use the postgres-env in a `hatch run` command, for example:

```sh
hatch -v run postgres-env:pytest -vv -n auto -k "itest_simple" metricflow/test
```
