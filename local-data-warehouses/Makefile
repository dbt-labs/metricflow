#
# Local Data Warehouse Setup
#

.PHONY: postgresql
postgresql:
	docker-compose -f postgresql/docker-compose.yaml up

.PHONY: trino
trino:
	docker-compose -f trino/docker-compose.yaml up

.PHONY: clickhouse
clickhouse:
	docker-compose -f clickhouse/docker-compose.yaml up
