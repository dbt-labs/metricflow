# Contributing to `MetricFlow`

Welcome to the MetricFlow developer community, we're thrilled to have you aboard!

## Before you get started, please...

1. Familiarize yourself with our [Code of Conduct](http://community.transform.co/metricflow-signup). In summary - be kind to each other. We're all here trying to make the data world a better place to work.
2. Sign our [Contributor License Agreement](TransformCLA.md). Unfortunately, we cannot accept PRs unless you have signed. If you are not able to sign the agreement you may still participate in our Slack community or interact with Issues

## Environment setup

1. Ensure you have Python `3.8` or `3.9`.
2. Install the following required system dependencies:
    - MySqlClient:
        - Follow the [instructions from MySQL](https://dev.mysql.com/doc/mysql-getting-started/en/)
        - Mac users might prefer to use Homebrew: `brew install mysql`
    - Postgres:
        - Postgres provides [pre-built packages for download and installation](https://www.postgresql.org/download/)
        - Mac users might prefer to use Homebrew: `brew install postgresql`
    - SQLite:
        - You likely have this installed already, but if it is missing [SQLite provides pre-built packages for download and installation](https://www.sqlite.org/download.html)
3. [Create a fork](https://docs.github.com/en/get-started/quickstart/fork-a-repo) of the [MetricFlow repo](https://github.com/transform-data/metricflow) and [clone it locally](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository). 
4. Activate a [Python virtual environment](https://docs.python.org/3/library/venv.html). While this is not required, it is *strongly* encouraged.
    - We provide `make venv` and `make remove_venv` helpers for creating/deleting standard Python virtual envs. You may pass `VENV_NAME=your_custom_name` to override the default `venv` location.
    - [conda](https://docs.conda.io/en/latest/) users may prefer [conda's environment management](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html) instead.
5. Run `make install` to get all of your dependencies loaded and ready for development
    - This includes useful dev tools, including pre-commit for linting. 
    - You may run `pre-commit install` if you would like the linters to run prior to all local git commits

## Start testing and development

You're ready to start! Note all `make` and `poetry` commands should be run from your repository root unless otherwise indicated.

1. Run some tests to make sure things happen:
    - Run the full test suite: `make test`
    - Run a subset of tests based on path: `poetry run pytest metricflow/test/plan_conversion`
    - Run a subset of tests based on test name substring: `poetry run pytest -k "query" metricflow/test`
2. Now you may wish to break some tests. Make some local changes and run the relevant tests again and see if you broke them!
    - Working with integration tests
        - These tests are driven by a set of test configs in [metricflow/test/integration/test_cases](metricflow/test/integration/test_cases/). They compare the output of a MetricFlow query against the output of a similar SQL query.
        - These tests all run on consistent input data, which is [created in the target warehouse via setup fixtures](metricflow/test/fixtures/table_fixtures.py). 
            - Modify this file if you are looking to test boundary cases involving things like repeated rows of data.
        - Let's break a test!
            - Change a SQL query inside of [metricflow/test/integration/test_cases/itest_simple.yaml](metricflow/test/integration/test_cases/itest_simple.yaml)
            - Run the test case: `poetry run pytest -k "itest_simple.yaml" metricflow/test/integration`. Did it fail?
    - Working with module and component tests
        - These are generally laid out in a similar hierarchy to the main package.
        - Let's try them out:
            - Run the [dataflow plan to sql plan conversion tests](metricflow/test/plan_conversion/test_dataflow_to_sql_plan.py): `poetry run pytest metricflow/test/plan_conversion/test_dataflow_to_sql_plan.py`.
            - Modify something in the [dataflow to sql plan converter logic](metricflow/plan_conversion/dataflow_to_sql.py). I like to throw exceptions just to make sure things blow up.
            - Run the test again. Did anything break?
    - Remember to clean up when you're done playing with the tests!
3. Make changes to the codebase and verify them through further testing, including test runs against other warehouse engines.
    - To run tests against other engines you MUST have read and write access to an instance of the execution engine and database.
    - Run the following commands in your shell, replacing the tags with the appropriate values:
        - `export MF_SQL_ENGINE_URL=<YOUR_WAREHOUSE_CONNECTION_URL>`
        - `export MF_SQL_ENGINE_PASSWORD=<YOUR_WAREHOUSE_PASSWORD>`
    - Run `make test` to execute the entire test suite against the target engine.
4. Run the linters with `make lint` at any time, but especially before submitting a PR. We use:
    - `Black` for formatting
    - `Flake8` for general Python linting
    - `MyPy` for typechecking
5. To see how your changes work with mnore interactive queries, use your repo-local CLI.
    - Run `poetry run mf --help`
    - Follow the CLI help from there, just remember your local CLI is always `poetry run mf <COMMAND>`!

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
