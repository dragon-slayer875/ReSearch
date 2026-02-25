FROM golang:1.25.6-alpine AS builder

WORKDIR /app

COPY . .

# Download dependencies
RUN go mod download

# Build the application
RUN go build -o research-server ./cmd/main.go

# Run tests
# RUN go test -v ./...

# Use a smaller base image
FROM alpine:latest

# Set the working directory
WORKDIR /app

# Copy thhe built binary from the builder stage
COPY --from=builder /app/research-server .
COPY --from=builder /app/public ./public

# Run the application
CMD ["./research-server"]
