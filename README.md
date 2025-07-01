# LogFlow: A Resilient Log Ingestion Pipeline

LogFlow is a high-performance, distributed log ingestion pipeline designed to reliably process and store log data from various sources. It is built on a microservice architecture, leveraging modern technologies like Go, Docker, Kafka, and Elasticsearch to create a scalable and fault-tolerant system.

This project serves as a practical example of building a real-world data engineering pipeline, complete with data validation, message queuing, and structured storage.

## Architecture Overview

The pipeline consists of three core microservices that communicate asynchronously via Kafka topics. This decoupled design ensures that each service can be scaled and maintained independently, and it provides a buffer to handle bursts of traffic without losing data.

1.  **Ingestor**: A lightweight HTTP server that acts as the entry point for all logs. It validates incoming data against a set of rules, and upon success, publishes the raw log to the `raw_logs` Kafka topic.
2.  **Parser**: A consumer service that reads from the `raw_logs` topic. It parses the raw log, normalizes its fields (like timestamps and log levels), enriches it with metadata, and then publishes the structured result to the `parsed_logs` topic. Invalid logs that cannot be parsed are sent to a Dead-Letter Queue (DLQ) for later inspection.
3.  **Storage Writer**: The final service in the pipeline. It consumes structured logs from the `parsed_logs` topic and writes them to a time-series index in Elasticsearch for long-term storage, analysis, and visualization.

## Features

- **HTTP Log Ingestion**: Simple and fast log submission over HTTP.
- **Data Validation**: Ensures data quality at the edge before it enters the system.
- **Asynchronous Processing**: Uses Kafka as a message broker to decouple services and buffer data.
- **Log Parsing & Normalization**: Standardizes log formats for consistent storage.
- **Dead-Letter Queue (DLQ)**: Isolates and saves malformed messages for later analysis without halting the pipeline.
- **Structured Storage**: Stores logs in Elasticsearch, enabling powerful search and analytics.
- **Containerized**: Fully containerized with Docker and Docker Compose for easy setup and deployment.
- **Comprehensive Testing**: Includes both unit and end-to-end integration tests to ensure reliability.

## Technology Stack

- **Backend**: Go (Golang) 1.24.3
- **API Framework**: Gin
- **Message Queue**: Apache Kafka
- **Database**: Elasticsearch
- **Containerization**: Docker & Docker Compose
- **Go Libraries**:
    - `sarama`: Kafka client for Go
    - `go-elasticsearch`: Official Elasticsearch client
    - `gin-gonic`: HTTP web framework

## Prerequisites

- Docker and Docker Compose
- Go 1.24.3 or later (for local development and testing)

## Getting Started

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/MinuteHanD/LogFlow.git
    cd LogFlow
    ```

2.  **Start all services using Docker Compose:**
    This command will build the Docker images for the Go services and start all containers in the background.
    ```bash
    docker compose up -d --build
    ```

3.  **Send a test log:**
    Use the provided script to send a variety of test logs to the pipeline.
    ```bash
    ./send_all_logs.sh
    ```
    Or send a custom log using `curl`:
    ```bash
    curl -X POST http://localhost:8081/log \
      -H "Content-Type: application/json" \
      -d '{ \
        "level": "info", \
        "service": "Solid Snake", \
        "message": "Dragon of Dojima", \
        "timestamp": "2025-07-01T12:00:00Z" \
      }'
    ```

4.  **View logs in Elasticsearch:**
    Allow a few moments for the pipeline to process the log, then query Elasticsearch directly:
    ```bash
    curl -X GET "http://localhost:9200/logs-*/_search?pretty" -H 'Content-Type: application/json' -d'\
    {\
      "query": { "match_all": {} },\
      "size": 10,\
      "sort": [ { "timestamp": "desc" } ]\
    }'
    ```
    You can also view the logs and create dashboards in **Kibana** by navigating to `http://localhost:5601` in your browser.

## Testing

The project includes a suite of tests to ensure code quality and system reliability.

### Unit Tests

Unit tests check individual functions and components in isolation. They are fast and do not require any external dependencies like Docker.

To run all unit tests for all services:
```bash
go test -v ./...
```

### Integration Test

The integration test verifies the entire pipeline, from HTTP ingestion to storage in Elasticsearch. It uses Docker Compose to manage the required services.

**Note:** This test is slower and will start and stop Docker containers on your machine.

To run the integration test:
```bash
go test -v -tags=integration
```


