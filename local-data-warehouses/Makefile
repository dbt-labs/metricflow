#
# Local Data Warehouse Setup
#

.PHONY: postgresql
postgresql:
	docker-compose -f postgresql/docker-compose.yaml up

.PHONY: trino
trino:
	docker-compose -f trino/docker-compose.yaml up

.PHONY: vertica
vertica:
	docker-compose -f vertica/docker-compose.yaml up
