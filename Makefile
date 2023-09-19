#
# Local Env/Dev Setup
#

# Controls the number of parallel workers running tests. Try "make -e PARALLELISM=4 test".
PARALLELISM = "auto"
# Additional command line options to pass to pytest.
ADDITIONAL_PYTEST_OPTIONS = ""

# Pytest that can populate the persistent source schema
USE_PERSISTENT_SOURCE_SCHEMA = "--use-persistent-source-schema"
POPULATE_PERSISTENT_SOURCE_SCHEMA = "metricflow/test/source_schema_tools.py::populate_source_schema"

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

# Engine-specific test environments.
.PHONY: test-bigquery
test-bigquery:
	hatch -v run bigquery-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) metricflow/test/

.PHONY: populate-persistent-source-schema-bigquery
populate-persistent-source-schema-bigquery:
	hatch -v run bigquery-env:pytest -vv $(ADDITIONAL_PYTEST_OPTIONS) $(USE_PERSISTENT_SOURCE_SCHEMA) $(POPULATE_PERSISTENT_SOURCE_SCHEMA)

.PHONY: test-databricks
test-databricks:
	hatch -v run databricks-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) metricflow/test/

.PHONY: populate-persistent-source-schema-databricks
populate-persistent-source-schema-databricks:
	hatch -v run databricks-env:pytest -vv $(ADDITIONAL_PYTEST_OPTIONS) $(USE_PERSISTENT_SOURCE_SCHEMA) $(POPULATE_PERSISTENT_SOURCE_SCHEMA)

.PHONY: test-redshift
test-redshift:
	hatch -v run redshift-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) metricflow/test/

.PHONY: populate-persistent-source-schema-redshift
populate-persistent-source-schema-redshift:
	hatch -v run redshift-env:pytest -vv $(ADDITIONAL_PYTEST_OPTIONS) $(USE_PERSISTENT_SOURCE_SCHEMA) $(POPULATE_PERSISTENT_SOURCE_SCHEMA)


.PHONY: test-snowflake
test-snowflake:
	hatch -v run snowflake-env:pytest -vv -n $(PARALLELISM) $(ADDITIONAL_PYTEST_OPTIONS) metricflow/test/

.PHONY: populate-persistent-source-schema-snowflake
populate-persistent-source-schema-snowflake:
	hatch -v run snowflake-env:pytest -vv $(ADDITIONAL_PYTEST_OPTIONS) $(USE_PERSISTENT_SOURCE_SCHEMA) $(POPULATE_PERSISTENT_SOURCE_SCHEMA)


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
