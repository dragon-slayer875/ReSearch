FROM golang:1.25.6-alpine AS builder

WORKDIR /app

COPY . .

# Download dependencies
RUN go mod download

# Build the application
RUN go build -o research-page-rank ./cmd/main.go

# Run tests
# RUN go test -v ./...

# Use a smaller base image
FROM alpine:latest

# Set the working directory
WORKDIR /app

# Copy the built binary from the builder stage
COPY --from=builder /app/research-page-rank .

# Run the application
CMD ["./research-page-rank"]
