FROM golang:1.25.6-alpine AS builder

WORKDIR /app

COPY . .

RUN go mod download

COPY . .

RUN go build -o research-indexer ./cmd/main.go

# Run tests
# RUN go test -v ./...

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/research-indexer .

CMD ["./research-indexer"]
