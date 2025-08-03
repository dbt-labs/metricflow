# Contributing to `MetricFlow`

Welcome to the MetricFlow developer community, we're thrilled to have you aboard!

## Before you get started, please...

1. Familiarize yourself with our [Code of Conduct](https://www.getdbt.com/community/code-of-conduct/#:~:text=We%20want%20everyone%20to%20have,don't%20be%20a%20jerk.). In summary - be kind to each other. We're all here trying to make the data world a better place to work.
2. Make sure you can sign our [Contributor License Agreement](https://docs.getdbt.com/community/resources/contributor-license-agreements). Unfortunately, we cannot accept PRs unless you have signed. If you are not able to sign the agreement you may still participate in our Slack community or interact with Issues. To sign the agreement simply put up a PR, and you will receive instructions there.

## Environment setup

1. Ensure you have a relevant version of Python.
2. You may need to install the following required system dependencies:
    - MySqlClient:
        - Follow the [instructions from MySQL](https://dev.mysql.com/doc/mysql-getting-started/en/)
        - Mac users might prefer to use Homebrew: `brew install mysql`
    - Postgres:
        - Postgres provides [pre-built packages for download and installation](https://www.postgresql.org/download/)
        - Mac users might prefer to use Homebrew: `brew install postgresql`
    - Docker:
        - This is only required if you are developing with Postgres.
        - Follow the [instructions from Docker](https://docs.docker.com/get-docker/)
3. [Create a fork](https://docs.github.com/en/get-started/quickstart/fork-a-repo) of the [MetricFlow repo](https://github.com/dbt-labs/metricflow) and [clone it locally](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository).
4. Install [Hatch](https://github.com/pypa/hatch) via `make install-hatch` - this is the tool we use to manage our build dependencies.

## Start testing and development

You're ready to start! Note all `make` and `hatch` commands should be run from your repository root unless otherwise indicated.

`pyproject.yaml` includes a definition for a Hatch environment named `dev-env` that is similar to a virtual environment
and allows packages to be installed in isolation. The `Makefile` includes a number of other useful commands as well, such as `make test`, which handle the environment switching. For engine-specific testing refer to the `<engine>-env` environments defined in `pyproject.yaml` and the `test-<engine>` commands in the `Makefile` - for example, postgres tests are most easily run through the `postgres-env` instead of `dev-env`, or via `make test-postgresql`.

When running any one of the hatch commands, the environment is automatically set up for you.

1. Run some tests to make sure things happen:
    - Run the full test suite: `make test`
    - Run a subset of tests based on path: `hatch run dev-env:pytest tests/plan_conversion`
    - Run a subset of tests based on test name substring: `hatch run dev-env:pytest -k "query" tests`
2. Now you may wish to break some tests. Make some local changes and run the relevant tests again and see if you broke them!
    - Working with integration tests
        - These tests are driven by a set of test configs in [tests_metricflow/integration/test_cases](tests_metricflow/integration/test_cases/). They compare the output of a MetricFlow query against the output of a similar SQL query.
        - These tests all run on consistent input data, which is [created in the target warehouse via setup fixtures](tests_metricflow/fixtures/table_fixtures.py).
            - Modify the [test inputs](tests_metricflow/fixtures/source_table_snapshots) for one of the test models if you are looking to test boundary cases involving things like repeated rows of data.
        - Let's break a test!
            - Change a SQL query inside of [tests_metricflow/integration/test_cases/itest_simple.yaml](tests_metricflow/integration/test_cases/itest_simple.yaml)
            - Run the test case: `hatch run dev-env:pytest -k "itest_simple" tests/integration`. Did it fail?
    - Working with module and component tests
        - These are generally laid out in a similar hierarchy to the main package.
        - Let's try them out:
            - Run the [dataflow plan to sql plan conversion tests](tests_metricflow/plan_conversion/test_dataflow_to_sql_plan.py): `hatch run dev-env:pytest tests_metricflow/plan_conversion/test_dataflow_to_sql_plan.py`.
            - Modify something in the [dataflow to sql plan converter logic](metricflow/plan_conversion/to_sql_plan/dataflow_to_sql.py). I like to throw exceptions just to make sure things blow up.
            - Run the test again. Did anything break?
    - Remember to clean up when you're done playing with the tests!
3. Make changes to the codebase and verify them through further testing, including test runs against other warehouse engines.
    - To run tests against other engines you MUST have read and write access to an instance of the execution engine and database.
    - Run the following commands in your shell, replacing the tags with the appropriate values:
        - `export MF_SQL_ENGINE_URL=<YOUR_WAREHOUSE_CONNECTION_URL>`
        - `export MF_SQL_ENGINE_PASSWORD=<YOUR_WAREHOUSE_PASSWORD>`
    - Run `make test-<engine>` to execute the entire test suite against the target engine. This will also set the `MF_TEST_ADAPTER_TYPE` to the proper engine identifier and pull in and configure the necessary dbt adapter dependencies for query execution. For example, to run tests against BigQuery, run `make test-bigquery`
    - By default, without `MF_SQL_ENGINE_URL` and `MF_SQL_ENGINE_PASSWORD` set, your tests will run against DuckDB.
4. Run the linters with `make lint` at any time, but especially before submitting a PR. We use:
    - `Black` for formatting
    - `Ruff` for general Python linting
    - `MyPy` for typechecking
5. To see how your changes work with more interactive queries, use your repo-local CLI.
    - Run `hatch run dev-env:mf --help`
    - Follow the CLI help from there, just remember your local CLI is always `hatch run dev-env:run mf <COMMAND>`!
    - Note this will only work if you invoke the command from within a properly configured dbt project, so it may be simpler to clone the [jaffle-sl-template repo](https://github.com/dbt-labs/jaffle-sl-template) and do an editable install (via `pip install -e /path/to/metricflow/repo`) in a separate Python virtual environment.
6. Some tests generate snapshots in the test directory. Separate snapshots may be generated for each SQL engine. You can regenerate these snapshots by running `make regenerate-test-snapshots`.

## Adding or modifying a CHANGELOG Entry!

We use [changie](https://changie.dev) to generate `CHANGELOG` entries. **Note:** Do not edit the `CHANGELOG.md` directly. Your modifications will be lost.

In order to use it, you can:

1. Follow the steps to [install `changie`](https://changie.dev/guide/installation/) for your system.
2. Once changie is installed and your PR is created for a new feature, run the following command and changie will walk you through the process of creating a changelog entry. `changie new`.
3. Commit the file that's created and your changelog entry is complete!
4. (Optional if contributing to a feature in progress) Modify the changie yaml file in `metricflow/.changes/unreleased/` related to your change. If you need help finding this file, please ask within the discussion for the pull request!

You don't need to worry about which `metricflow` version your change will go into. Just create the changelog entry with `changie`, and open your PR against the `main` branch. All merged changes will be included in the next minor version of `metricflow`. The maintainers _may_ choose to "backport" specific changes in order to patch older minor versions. In that case, a maintainer will take care of that backport after merging your PR, before releasing the new version of `metricflow`.

## Submit your contribution!

1. Merge your changes into your fork of the MetricFlow repository
2. Make a well-formed Pull Request (PR) from your fork into the main MetricFlow repository. If you're not clear on what a well-formed PR looks like, fear not! We will help you here and throughout the review process.
    - Well-formed PRs are composed of one or more well-formed commits, and include clear indications of how they were tested and verified prior to submission.
    - Well-formed commits are focused (loosely speaking they do one conceptual thing) and well-described.
    - A good commit message - like a good PR message - will have three components:
        1. A succinct title explaining what the commit does
        2. A separate body describing WHY the change is being made
        3. Additional detail on what the commit does, if needed
    - We want this because we believe the hardest part of a collaborative software project is not getting the computer to do what it's supposed to do. It's communicating to a human reader what you meant for the computer to do (and why!), and also getting the computer to do that thing.
    - This helps you too - well-formed PRs get reviewed a lot faster and a lot more productively. We want your contribution experience to be as smooth as possible and this helps immensely!
3. One of our core contributors will review your PR and either approve it or send it back with requests for updates
4. Once the PR has been approved, our core contributors will merge it into the main project.
5. You will get a shoutout in our changelog/release notes. Thank you for your contribution!
