# Controls the number of parallel workers running tests. Try "make -e PARALLELISM=4 test".
PARALLELISM = auto
# Testing and linting
.PHONY: test
test:
	hatch -v run dev-env:pytest -vv -n $(PARALLELISM) tests_dbt_metricflow
