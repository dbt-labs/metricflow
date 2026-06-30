#
# Local Data Warehouse Setup
#

.PHONY: postgresql
postgresql:
	docker-compose -f postgresql/docker-compose.yaml up

.PHONY: trino
trino:
	docker-compose -f trino/docker-compose.yaml up

.PHONY: starrocks
starrocks:
	docker-compose -f starrocks/docker-compose.yaml up
