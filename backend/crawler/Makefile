.PHONY: generate-sqlc

include .env

generate-sqlc:
	docker run --rm -v $(shell pwd)/internals/storage:/src -w /src sqlc/sqlc generate

migrate-up:
	docker run --rm -v $(shell pwd)/internals/storage/migrations:/migrations --network host migrate/migrate -path=/migrations/ -database $(POSTGRES_URL) up 1

migrate-down:
	docker run --rm -v $(shell pwd)/internals/storage/migrations:/migrations --network host migrate/migrate -path=/migrations/ -database $(POSTGRES_URL) down 1
