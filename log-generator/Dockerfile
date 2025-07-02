FROM golang:1.24.3-alpine

WORKDIR /app

# Copy go.mod and go.sum to download dependencies first, leveraging Docker cache
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY ./log-generator .

# Build the log-generator binary
RUN go build -o main .

# Command to run the executable
CMD ["./main"]
