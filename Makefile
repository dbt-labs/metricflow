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
.PHONY: test-core
test-core:
	hatch -v run dev-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) metricflow/test --ignore metricflow/test/model/dbt_cloud_parsing/

# Test that depend on dbt-related packages.
.PHONY: test-dbt-associated
test-dbt-associated:
	hatch -v run dev-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) metricflow/test/model/dbt_cloud_parsing/

.PHONY: test
test:
	hatch -v run dev-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) metricflow/test/

.PHONY: test-postgresql
test-postgresql:
	hatch -v run postgres-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) metricflow/test/

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
