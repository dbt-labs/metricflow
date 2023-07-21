#
# Local Env/Dev Setup
#

# Controls the number of parallel workers running tests. Try "make -e PARALLELISM=4 test".
PARALLELISM = "auto"
# Additional command line options to pass to pytest.
ADDITIONAL_PYTEST_OPTIONS = ""

# Install Hatch package / project manager
.PHONY: install-hatch
install-hatch:
	pip3 install hatch

# Testing and linting
.PHONY: test
test:
	hatch -v run dev-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) metricflow/test/

.PHONY: test-postgresql
test-postgresql:
	hatch -v run postgres-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) metricflow/test/

# Engine-specific test environments. In most cases you should run these with
# `make -e ADDITIONAL_PYTEST_OPTIONS="--use-persistent-source-schema" test-<engine_type>`
.PHONY: test-bigquery
test-bigquery:
	hatch -v run bigquery-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) metricflow/test/

.PHONY: test-databricks
test-databricks:
	hatch -v run databricks-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) metricflow/test/

.PHONY: test-redshift
test-redshift:
	hatch -v run redshift-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) metricflow/test/

.PHONY: test-snowflake
test-snowflake:
	hatch -v run snowflake-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) metricflow/test/

.PHONY: lint
lint:
	hatch -v run dev-env:pre-commit run --all-files

# Running data warehouses locally
.PHONY: postgresql postgres
postgresql postgres:
	make -C local-data-warehouses postgresql

# Re-generate test snapshots using all supported SQL engines.
.PHONY: regenerate-test-snapshots
regenerate-test-snapshots:
	hatch -v run dev-env:python metricflow/test/generate_snapshots.py
