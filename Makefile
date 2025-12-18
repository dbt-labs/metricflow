#
# Local Env/Dev Setup
#

# Controls the number of parallel workers running tests. Try "make -e PARALLELISM=4 test".
PARALLELISM = auto
# Additional command line options to pass to pytest.
ADDITIONAL_PYTEST_OPTIONS =
# Additional command line options to pass to `pre-commit`
ADDITIONAL_PRECOMMIT_OPTIONS =

PERFORMANCE_OUTPUT_FILE = performance-report.json
PERFORMANCE_COMPARISON_OUTPUT_FILE = performance-comparison.md
TESTS_PERFORMANCE = tests_metricflow/performance

# Pytest that can populate the persistent source schema
USE_PERSISTENT_SOURCE_SCHEMA = --use-persistent-source-schema
TESTS_DBT_METRICFLOW = tests_dbt_metricflow
TESTS_METRICFLOW = tests_metricflow
TESTS_METRICFLOW_SEMANTICS = tests_metricflow_semantics
POPULATE_PERSISTENT_SOURCE_SCHEMA = $(TESTS_METRICFLOW)/source_schema_tools.py::populate_source_schema

# Install Hatch package / project manager
.PHONY: install-hatch
install-hatch:
	pip3 install hatch

.PHONY: perf
perf:
	hatch -v run dev-env:pytest -vv -n 1 $(ADDITIONAL_PYTEST_OPTIONS) --output-json $(PERFORMANCE_OUTPUT_FILE) $(TESTS_PERFORMANCE)/

.PHONY: perf-compare
perf-compare:
	hatch -v run dev-env:python $(TESTS_PERFORMANCE)/compare_reports.py $A $B $(PERFORMANCE_COMPARISON_OUTPUT_FILE)

# Testing and linting
.PHONY: test
test:
	cd metricflow-semantics && hatch -v run dev-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) $(TESTS_METRICFLOW_SEMANTICS)/
	hatch -v run dev-env:pytest -vv -n $(PARALLELISM) -m "not slow" $(ADDITIONAL_PYTEST_OPTIONS) $(TESTS_METRICFLOW)/

.PHONY: test-include-slow
test-include-slow:
	cd dbt-metricflow && hatch -v run dev-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) $(TESTS_DBT_METRICFLOW)/
	cd metricflow-semantics && hatch -v run dev-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) $(TESTS_METRICFLOW_SEMANTICS)/
	hatch -v run dev-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) $(TESTS_METRICFLOW)/

.PHONY: test-postgresql
test-postgresql:
	hatch -v run postgres-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) $(TESTS_METRICFLOW)/

# Engine-specific test environments.
.PHONY: test-bigquery
test-bigquery:
	hatch -v run bigquery-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) $(TESTS_METRICFLOW)/

.PHONY: populate-persistent-source-schema-bigquery
populate-persistent-source-schema-bigquery:
	hatch -v run bigquery-env:pytest -vv $(ADDITIONAL_PYTEST_OPTIONS) $(USE_PERSISTENT_SOURCE_SCHEMA) $(POPULATE_PERSISTENT_SOURCE_SCHEMA)

.PHONY: test-databricks
test-databricks:
	hatch -v run databricks-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) $(TESTS_METRICFLOW)/

.PHONY: populate-persistent-source-schema-databricks
populate-persistent-source-schema-databricks:
	hatch -v run databricks-env:pytest -vv $(ADDITIONAL_PYTEST_OPTIONS) $(USE_PERSISTENT_SOURCE_SCHEMA) $(POPULATE_PERSISTENT_SOURCE_SCHEMA)

.PHONY: test-redshift
test-redshift:
	hatch -v run redshift-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) $(TESTS_METRICFLOW)/

.PHONY: populate-persistent-source-schema-redshift
populate-persistent-source-schema-redshift:
	hatch -v run redshift-env:pytest -vv $(ADDITIONAL_PYTEST_OPTIONS) $(USE_PERSISTENT_SOURCE_SCHEMA) $(POPULATE_PERSISTENT_SOURCE_SCHEMA)


.PHONY: test-snowflake
test-snowflake:
	hatch -v run snowflake-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) $(TESTS_METRICFLOW)/

.PHONY: populate-persistent-source-schema-snowflake
populate-persistent-source-schema-snowflake:
	hatch -v run snowflake-env:pytest -vv $(ADDITIONAL_PYTEST_OPTIONS) $(USE_PERSISTENT_SOURCE_SCHEMA) $(POPULATE_PERSISTENT_SOURCE_SCHEMA)

.PHONY: test-trino
test-trino:
	hatch -v run trino-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) $(TESTS_METRICFLOW)/

.PHONY: lint
lint:
	hatch -v run dev-env:pre-commit run --verbose --all-files $(ADDITIONAL_PRECOMMIT_OPTIONS)

# Running data warehouses locally
.PHONY: postgresql postgres
postgresql postgres:
	make -C local-data-warehouses postgresql

.PHONY: trino
trino:
	make -C local-data-warehouses trino

# Re-generate test snapshots using all supported SQL engines.
.PHONY: regenerate-test-snapshots
regenerate-test-snapshots:
	hatch -v run dev-env:python tests_metricflow/generate_snapshots.py

# Populate persistent source schemas for all relevant SQL engines.
.PHONY: populate-persistent-source-schemas
populate-persistent-source-schemas:
	hatch -v run dev-env:python $(TESTS_METRICFLOW)/populate_persistent_source_schemas.py

# Sync dbt-semantic-interfaces files to metricflow-semantic-interfaces folder
.PHONY: sync-dsi
sync-dsi:
	python scripts/sync_dsi.py

# Re-generate snapshots for the default SQL engine.
.PHONY: test-snap
test-snap:
	make test ADDITIONAL_PYTEST_OPTIONS=--overwrite-snapshots

.PHONY: testx
testx:
	make test ADDITIONAL_PYTEST_OPTIONS=-x

.PHONY: testx-snap
testx-snap:
	make test ADDITIONAL_PYTEST_OPTIONS='-x --overwrite-snapshots'

.PHONY: test-snap-slow
test-snap-slow:
	cd dbt-metricflow && hatch -v run dev-env:pytest -vv -n $(PARALLELISM) --overwrite-snapshots $(TESTS_DBT_METRICFLOW)/
	cd metricflow-semantics && hatch -v run dev-env:pytest -vv -n $(PARALLELISM) --overwrite-snapshots $(TESTS_METRICFLOW_SEMANTICS)/
	hatch -v run dev-env:pytest -vv -n $(PARALLELISM) --overwrite-snapshots $(TESTS_METRICFLOW)/

.PHONY: test-build-packages
test-build-packages:
	PYTHONPATH=. python scripts/ci_tests/run_package_build_tests.py --metricflow-repo-directory=.
