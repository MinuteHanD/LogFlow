FROM golang:1.24.3-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY ./log-generator .

RUN go build -o main .

CMD ["./main"]
