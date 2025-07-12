# LogFlow: A Resilient Log Ingestion Pipeline

[![Go CI](https://github.com/MinuteHanD/log-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/MinuteHanD/log-pipeline/actions/workflows/ci.yml)

LogFlow is a high-performance, fault-tolerant log ingestion pipeline built in Go. It provides a complete, containerized solution for receiving, processing, and storing log data at scale, using Kafka for message queuing and Elasticsearch for durable, searchable storage.

This project is designed with a microservices architecture, ensuring that each component is decoupled, independently scalable, and resilient to failure.

---

## Architecture

The pipeline follows an asynchronous, multi-stage process. Each service is a distinct Go microservice that communicates via Kafka topics, ensuring high throughput and system stability.

```
                  ┌──────────┐      ┌──────────┐      ┌───────────┐      ┌───────────────┐
[HTTP Client]───> │ Ingestor │ ───> │  Kafka   │ ───> │  Parser   │ ───> │     Kafka     │
                  └──────────┘      │ raw_logs │      └───────────┘      │  parsed_logs  │
                                    └──────────┘            │            └───────────────┘
                                                          ▼                     │
                                                   ┌──────────────┐             ▼
                                                   │    Kafka     │      ┌────────────────┐
                                                   │ raw_logs_dlq │      │ Storage Writer │
                                                   └──────────────┘      └────────────────┘
                                                                                  │
                                                                                  ▼
                                                                        ┌─────────────────┐
                                                                        │  Elasticsearch  │
                                                                        └─────────────────┘
```

### Core Components

*   **Ingestor**: A Go service using the Gin framework that exposes an HTTP endpoint (`/log`) to receive log entries. It performs initial validation (size, format, required fields) and publishes valid raw logs to the `raw_logs` Kafka topic.
*   **Parser**: A Kafka consumer that reads from `raw_logs`. It normalizes data (e.g., standardizing log levels, parsing timestamps), enriches logs with metadata, and generates a unique ID. Processed logs are published to `parsed_logs`. Logs that fail parsing are routed to a Dead-Letter Queue (`raw_logs_dlq`) for later analysis.
*   **Storage Writer**: Consumes structured logs from the `parsed_logs` topic. It creates daily time-based indices in Elasticsearch and writes the final log document. If Elasticsearch is unavailable or rejects a document, the message is routed to its own DLQ (`parsed_logs_dlq`).

---

## Key Features

*   **Fault Tolerance**: Utilizes Dead-Letter Queues (DLQs) at both the parsing and storage stages to prevent data loss from malformed messages or downstream service outages.
*   **Structured, Centralized Logging**: All services use Go's native `slog` library to output JSON-formatted logs, enabling easier debugging and analysis of the pipeline itself.
*   **Asynchronous & Decoupled**: Kafka acts as a buffer, allowing the ingestor to handle traffic spikes without overwhelming the processing and storage layers. Services can be scaled, updated, or restarted independently.
*   **Comprehensive Testing**: The project is validated by both unit tests and a full end-to-end integration test that spins up the entire stack.
*   **Containerized**: Fully defined in `docker-compose.yml` for a reproducible, one-command setup.
*   **Centralized Configuration**: Uses a `config.yaml` file with environment variable overrides for flexible deployment.

---

## Technology Stack

*   **Language**: Go
*   **Services**: Gin (HTTP), Sarama (Kafka Client), go-elasticsearch (Elasticsearch Client)
*   **Infrastructure**: Docker, Docker Compose, Kafka, Elasticsearch

---

## Prerequisites

*   Go (latest version recommended)
*   Docker and Docker Compose

---

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/MinuteHanD/log-pipeline.git
cd log-pipeline
```

### 2. Run the Entire Pipeline

This command builds the Go services, starts all containers, and runs them in the background.

```bash
docker compose up -d --build
```

### 3. Send Test Logs

A helper script is provided to send a mix of valid and invalid logs to the ingestor, which helps test the main pipeline and the Dead-Letter Queue functionality.

```bash
./send_all_logs.sh
```

### 4. Verify in Elasticsearch

Wait a few moments for the logs to be processed. You can then query Elasticsearch to see the stored logs. This command fetches the 10 most recent logs.

```bash
curl -X GET "http://localhost:9200/logs-*/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "query": { "match_all": {} },
  "size": 10,
  "sort": [ { "timestamp": { "order": "desc" } } ]
}'
```

You can also view the data in Kibana by visiting `http://localhost:5601`.

---

## Development and Testing

For local development, you can run the Go services directly on your machine while the backing services (Kafka, Elasticsearch) run in Docker.

### 1. Start Infrastructure Services

```bash
docker compose up -d kafka elasticsearch kibana
```

### 2. Set Environment Variables

The services are configured to connect to the infrastructure running in Docker. The following environment variables allow the Go services to find them. You can set these in your shell's configuration file (e.g., `.bashrc`, `.zshrc`) or export them in each terminal session.

```bash
export KAFKA_BROKERS=localhost:9092
export ELASTICSEARCH_URL=http://localhost:9200
```

### 3. Run Each Go Service

Open a separate terminal for each service and run the following commands:

```bash
# Terminal 1: Ingestor
go run ./ingestor

# Terminal 2: Parser
go run ./parser

# Terminal 3: Storage Writer
go run ./storage-writer
```

### Running Tests

The project includes both unit tests and integration tests, which are separated by Go build tags.

*   **Run Unit Tests**:
    These tests are fast and do not require any external dependencies.

    ```bash
    go test -v ./...
    ```

*   **Run the End-to-End Integration Test**:
    This test will automatically start and stop the required Docker containers. Make sure Docker is running before executing this command.

    ```bash
    go test -v -tags=integration
    ```

---

## Future Work

*   **Metrics & Observability**: Expose Prometheus metrics from each service for monitoring and alerting.
*   **Correlation IDs**: Implement a correlation ID at the `ingestor` and pass it through Kafka headers to trace a single request across all services.
*   **DLQ Re-processing**: Build a utility or service to consume from the DLQ topics, attempt to re-process messages, and archive unrecoverable ones.
