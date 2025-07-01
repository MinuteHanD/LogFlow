Sir, like, okay. So the problem is that the whole README you've got is wrapped in a markdown code block. If you paste that directly into GitHub, it'll just show the raw text instead of, you know, actually formatting it. You just need the text *inside* that block.

Here's the code. Just copy and paste this directly into your `readme.md` file on GitHub.

-----

# LogFlow: A Resilient Log Ingestion Pipeline

[](https://github.com/MinuteHanD/log-pipeline/actions/workflows/ci.yml)

LogFlow is a multi-service log ingestion pipeline built in Go. It demonstrates a decoupled, fault-tolerant architecture for processing and storing high-volume log data. The system uses Kafka as a durable message bus and Elasticsearch for indexed storage and searchability.

This project is fully containerized and includes a comprehensive testing suite.

-----

## Architecture

The pipeline follows an asynchronous, three-stage process. Each service is a distinct microservice that communicates via Kafka topics, ensuring resilience and scalability.

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

  * **Ingestor**: A Go service using the Gin framework. It exposes an HTTP endpoint (`/log`) to receive log entries. It performs initial validation (size, format, required fields) and publishes valid raw logs to the `raw_logs` Kafka topic.
  * **Parser**: A Kafka consumer group that reads from `raw_logs`. It normalizes data (e.g., standardizing log levels, parsing timestamps), enriches logs with metadata, and generates a unique ID. Processed logs are published to `parsed_logs`. Logs that fail parsing are routed to a Dead-Letter Queue (`raw_logs_dlq`) for later analysis.
  * **Storage Writer**: Consumes structured logs from the `parsed_logs` topic. It creates daily time-based indices in Elasticsearch and writes the final log document. If Elasticsearch is unavailable or rejects a document, the message is routed to its own DLQ (`parsed_logs_dlq`).

-----

## Key Features

  * **Fault Tolerance**: Utilizes Dead-Letter Queues (DLQs) at both the parsing and storage stages to prevent data loss from malformed messages or downstream service outages.
  * **Structured, Centralized Logging**: All services use Go's native `slog` library to output JSON-formatted logs, enabling easier debugging and analysis of the pipeline itself.
  * **Asynchronous & Decoupled**: Kafka acts as a buffer, allowing the ingestor to handle traffic spikes without overwhelming the processing and storage layers. Services can be scaled, updated, or restarted independently.
  * **Comprehensive Testing**: The project is validated by:
      * **Unit Tests**: Focused tests for business logic within each service (e.g., validation, parsing logic).
      * **End-to-End Integration Test**: A Go-based test that uses `docker-compose` to spin up the entire stack, sends a log via HTTP, and verifies its existence in Elasticsearch.
  * **Containerized**: Fully defined in `docker-compose.yml` for reproducible one-command setup and deployment.

-----

## Technology Stack

  * **Language**: Go 1.24.3
  * **Services**: Gin (HTTP), Sarama (Kafka Client), go-elasticsearch (Elasticsearch Client)
  * **Infrastructure**: Docker, Docker Compose, Kafka, Elasticsearch

-----

## Prerequisites

  * Go 1.24.3+
  * Docker and Docker Compose

-----

## Getting Started

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/MinuteHanD/log-pipeline.git
    cd log-pipeline
    ```

2.  **Start all services:**
    The `--build` flag ensures the Go services are compiled into fresh images.

    ```bash
    docker compose up -d --build
    ```

3.  **Send test logs:**
    A script is provided to send a variety of valid and invalid logs to test the pipeline and DLQ functionality.

    ```bash
    ./send_all_logs.sh
    ```

4.  **Verify in Elasticsearch:**
    Allow a few seconds for processing, then query Elasticsearch. This command fetches the 10 most recent logs.

    ```bash
    curl -X GET "http://localhost:9200/logs-*/_search?pretty" -H 'Content-Type: application/json' -d'
    {
      "query": { "match_all": {} },
      "size": 10,
      "sort": [ { "timestamp": { "order": "desc" } } ]
    }'
    ```

    You can also use Kibana at `http://localhost:5601`.

-----

## Development and Testing

For local development, you can run the infrastructure in Docker and the Go services directly on your host.

1.  **Start infrastructure services:**

    ```bash
    docker compose up -d kafka elasticsearch kibana
    ```

2.  **Run each Go service in a separate terminal:**

    ```bash
    # Terminal 1: Ingestor
    export KAFKA_BROKERS=localhost:9092
    go run ./ingestor

    # Terminal 2: Parser
    export KAFKA_BROKERS=localhost:9092
    go run ./parser

    # Terminal 3: Storage Writer
    export KAFKA_BROKERS=localhost:9092
    export ELASTICSEARCH_URL=http://localhost:9200
    go run ./storage-writer
    ```

### Running Tests

The project separates fast unit tests from slower, dependency-heavy integration tests using Go build tags.

  * **Run Unit Tests:**
    This executes all `*_test.go` files that do *not* have the `integration` build tag.

    ```bash
    go test -v ./...
    ```

  * **Run Integration Test:**
    This command specifically runs tests tagged with `integration`. It will manage Docker containers automatically. Ensure Docker is running.

    ```bash
    go test -v -tags=integration
    ```

-----

## Future Work & Improvements

  * **Configuration Management**: Centralize configuration (e.g., Kafka topics, ports) using a library like Viper to read from YAML files with environment variable overrides.
  * **Metrics & Observability**: Expose Prometheus metrics from each service (e.g., logs processed, error rates, processing latency) for monitoring and alerting.
  * **Correlation IDs**: Implement a correlation ID at the `ingestor` and pass it through Kafka headers to trace a single request across all services in the structured logs.
  * **DLQ Re-processing**: Build a utility or service to consume from the DLQ topics, attempt to re-process messages, and archive unrecoverable ones.