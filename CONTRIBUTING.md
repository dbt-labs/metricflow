# Contributing to `MetricFlow`

Welcome to the MetricFlow developer community, we're thrilled to have you aboard!

## Before you get started...

.... please do the following.

1. Familiarize yourself with our [Code of Conduct](http://community.transform.co/metricflow-signup). In summary - be kind to each other. We're all here trying to make the data world a better place to work.
2. Sign our [Contributor License Agreement](TransformCLA.md). Unfortunately, we cannot accept PRs unless you have signed. If you are not able to sign the agreement you may still participate in our Slack community or interact with Issues

## Environment setup

1. Install Python if you have not already done so. MetricFlow is built on Python 3.9, although 3.8 will also work.
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
4. Activate a Python virtual environment. While this is not required, it is *strongly* encouraged. The following instructions are for stock Python venv usage. If you use conda feel free to use conda env instead.
    - From repo root, run `make venv` to create a virtual env named `venv` (you may pass `VENV_NAME=your_custom_name` if you wish to customize). Follow the instructions to activate your venv.
5. Run `make install` to get all of your dependencies loaded and ready for development
    - This includes useful dev tools, including pre-commit for linting. 
    - You may run `pre-commit install` if you would like the linters to run prior to all local git commits

## Start testing and development

1. At this point you will probably wish to run some tests. By default, we run all tests against SQLite. You can run the test suite with any of the following:
    - Run the full test suite from repo root: `make test`
    - Run a subset of tests, e.g., just for dataflow plan conversion, from repo root: `poetry run pytest metricflow/test/plan_conversion`
    - You may also run tests with any pytest selection convention: `poetry run pytest -k "query" metricflow/test`
2. Now you may wish to break some tests. Make some local changes and run the relevant tests again and see if you broke them!
    - Integration tests run end to end, comparing the output of a query against a test model with the output of a similar SQL query. To break these by modifying the tests themselves, you can change the test configs for a given test so that the queries no longer return equivalent results.
        - The test configs are in [metricflow/test/integration/test_cases](metricflow/test/integration/test_cases/)
        - Change a SQL query inside of [metricflow/test/integration/test_cases/itest_simple.yaml](metricflow/test/integration/test_cases/itest_simple.yaml)
        - Run the test case from repo root with `poetry run pytest -k "itest_simple.yaml" metricflow/test/integration` - if you broke it, it should fail!
        - These tests all run on consistent input data, which is [created in the target warehouse via setup fixtures](metricflow/test/fixtures/table_fixtures.py). Modify this file if you are looking to test boundary cases involving things like repeated rows of data.
    - Module and component tests are for specific components and we generally try to mirror the hierarchy between main and test packages. Try running the [dataflow plan to sql plan conversion tests](metricflow/test/plan_conversion/test_dataflow_to_sql_plan.py) bu calling `poetry run pytest metricflow/test/plan_conversion/test_dataflow_to_sql_plan.py`.
        - You might consider breaking this test by modifying something in the [dataflow to sql plan converter logic](metricflow/plan_conversion/dataflow_to_sql.py)
    - Remember to clean up when you're done playing with the tests!
3. Make changes to the codebase and verify them through further testing! You may wish to test your changes against other warehouse engines.
    - To run tests against other engines you MUST have read and write access to an instance of the execution engine and database.
    - The test runner picks up credentials for the supported engine via two environment variables. Run the following commands in your shell, replacing the tags with the appropriate values:
        - export MF_SQL_ENGINE_URL=<YOUR_WAREHOUSE_CONNECTION_URL>
        - export MF_SQL_ENGINE_PASSWORD=<YOUR_WAREHOUSE_PASSWORD>
    - At this point, if those are valid for an engine supported by MetricFlow, a call to `make test` will run the entire test suite against the target engine.
4. If you have not configured automatic pre-commit linting in your local repo, run the linters via `make lint` - this will run all of the following:
    - `Black` for formatting
    - `Flake8` for general Python linting
    - `MyPy` for typechecking
5. To see how your changes work in the wild, so to speak, you can use your repo-local CLI to execute queries against whatever model you have available to you. This is optional but highly recommended for new features. To run the CLI locally:
    - From repository root
    - `poetry run mf --help`
    - Follow the CLI help from there!

## Submit your contribution!

1. Merge your changes into your fork of the MetricFlow repository
2. Make a well-formed Pull Request (PR) from your fork into the main MetricFlow repository. If you're not clear on what a well-formed PR looks like, fear not! We will help you here and throughout the review process.
    - Well-formed PRs are composed of one or more well-formed commits, and include clear indications of how they were tested and verified prior to submission.
    - Well-formed commits are focused (loosely speaking they do one conceptual thing) and well-described.
    - A good commit message - like a good PR message - will have three parts:
        1. A succinct title explaining what the commit does
        2. A separate body describing WHY the change is being made
        3. Additional detail on what the commit does, if needed
    - We want this because we believe the hardest part of a collaborative software project is not getting the computer to do what it's supposed to do. It's communicating to a human reader what you meant for the computer to do, and also getting the computer to do that thing.
    - This helps you too - well-formed PRs get reviewed a lot faster and a lot more productively. We want your contribution experience to be as smooth as possible and this helps immensely!
3. One of our core contributors will review your PR and either approve it or send it back with requests for updates
4. Once the PR has been approved, our core contributors will merge it into the main project.
5. You will get a shoutout in our changelog/release notes. Thank you for your contribution!
