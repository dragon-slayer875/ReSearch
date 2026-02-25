.PHONY: generate-sqlc

generate-sqlc:
	docker run --rm -v $(shell pwd)/internals/storage:/src -w /src sqlc/sqlc generate

