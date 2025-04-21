FROM golang:alpine AS builder

# Set working directory
WORKDIR /app

# Install necessary build tools
RUN apk add --no-cache git

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN go build -o goquery .

# Use a smaller image for the final container
FROM alpine:latest

# Install necessary runtime dependencies
RUN apk --no-cache add ca-certificates tzdata

# Set working directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/goquery .

# Copy the .env file (if you want to use the .env file inside the container)
# Comment this out if you're using environment variables from docker-compose
COPY .env .

# Expose the application port
EXPOSE 9000

# Run the application
CMD ["./goquery"]
