FROM golang:1.25.6-alpine AS builder

WORKDIR /app

COPY . .

# Download dependencies
RUN go mod download

# Copy the application
COPY . .

# Build the application
RUN go build -o research-crawler ./cmd/main.go

# Run tests
# RUN go test -v ./...

# Use a smaller base image
FROM alpine:latest

# Set the working directory
WORKDIR /app

# Copy thhe built binary from the builder stage
COPY --from=builder /app/research-crawler .

# Run the application
CMD ["./research-crawler"]
