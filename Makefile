.PHONY: generate-sqlc generate-sqlc-crawler generate-sqlc-indexer generate-sqlc-page-rank generate-sqlc-server generate-sqlc-tf-idf

include .env

generate-sqlc: generate-sqlc-crawler generate-sqlc-indexer generate-sqlc-page-rank generate-sqlc-server generate-sqlc-tf-idf

generate-sqlc-crawler:
	docker run --name sqlc-generator --rm -v $(shell pwd)/services/crawler/internals/storage:/src -v ./migrations:/migrations -w /src sqlc/sqlc generate

generate-sqlc-indexer:
	docker run --name sqlc-generator --rm -v $(shell pwd)/services/indexer/internals/storage:/src -v ./migrations:/migrations -w /src sqlc/sqlc generate

generate-sqlc-tf-idf:
	docker run --name sqlc-generator --rm -v $(shell pwd)/services/tf-idf/internals/storage:/src -v ./migrations:/migrations -w /src sqlc/sqlc generate

generate-sqlc-page-rank:
	docker run --name sqlc-generator --rm -v $(shell pwd)/services/page-rank/internals/storage:/src -v ./migrations:/migrations -w /src sqlc/sqlc generate

generate-sqlc-server:
	docker run --name sqlc-generator --rm -v $(shell pwd)/services/server/internals/storage:/src -v ./migrations:/migrations -w /src sqlc/sqlc generate

migrate-up:
	docker run --rm -v $(shell pwd)/internals/storage/migrations:/migrations --network host migrate/migrate -path=/migrations/ -database $(POSTGRES_URL) up 1

migrate-down:
	docker run --rm -v $(shell pwd)/internals/storage/migrations:/migrations --network host migrate/migrate -path=/migrations/ -database $(POSTGRES_URL) down 1
