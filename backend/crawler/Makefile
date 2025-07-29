.PHONY: generate-sqlc

generate-sqlc:
	docker run --rm -v $(shell pwd)/pkg/storage:/src -w /src sqlc/sqlc generate

