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

Then, ensure that `MF_SQL_ENGINE_URL` and `MF_SQL_ENGINE_PASSWORD` are setup to access the PostgreSQL instance. Our test runs are configured to do this automatically via environment configurations. To run all tests from repo root use the following command:

```sh
make test-postgresql
```

To run some subset of tests in the postgres environment, you can use the postgres-env in a `hatch run` command, for example:

```sh
hatch -v run postgres-env:pytest -vv -n auto -k "itest_simple" metricflow/test
```

## Trino

First, configure a local Trino container. Our current test configuration assumes the local container is running on the default port 8080, and was set up through docker hub via the following command:

```sh
docker run --name trino -d -p 8080:8080 trinodb/trino
```

Advanced users may choose to follow instructions in [Trino's documentation] (https://trino.io/docs/current/installation.html) for other custom local configurations. If you should take that approach you may need to modify our test environment configuration to work with your local setup.

Once you have a local Trino cluster setup, you may run all tests from repo root via the following command:

```sh
make test-trino
```

As with postgresql, you may use the trino-env in a `hatch run` command to execute tests in a trino environment, for example:

```sh
hatch -v run trino-env:pytest -vv -n auto -k "itest_simple" metricflow/test
```