# LogFlow

A microservice-based log ingestion pipeline written in Go that processes logs through Kafka and stores them in Elasticsearch.

## Components

- `ingestor/`: Accepts incoming logs via HTTP and publishes them to Kafka
- `parser/`: Consumes logs from Kafka, parses them, and forwards them to the storage writer
- `storage-writer/`: Writes parsed logs to Elasticsearch for persistence

## Prerequisites

- Docker and Docker Compose
- Go 1.24.3 or later (for local development)

## Quick Start

1. Clone the repository:
   ```bash
   git clone https://github.com/MinuteHanD/LogFlow.git
   cd LogFlow
   ```

2. Start all services using Docker Compose:
   ```bash
   docker-compose up -d --build
   ```


3. Send some test logs using the provided script:
   ```bash
   ./send-logs.sh
   ```

   Or send a custom log using curl:
   ```bash
   curl -X POST http://localhost:8081/log \
     -H "Content-Type: application/json" \
     -d '{
       "level": "info",
       "service": "test-service",
       "message": "Hello, World!",
       "timestamp": "2024-03-14T12:00:00Z"
     }'
   ```

4. View logs in Kibana:
   - Open http://localhost:5601 in your browser
   - Navigate to "Discover" to view your logs

## Development

To run services locally for development:

1. Start the infrastructure services:
   ```bash
   docker-compose up -d kafka elasticsearch kibana
   ```

2. Run each service in a separate terminal:
   ```bash
   # Terminal 1
   cd ingestor && go run .

   # Terminal 2
   cd parser && go run .

   # Terminal 3
   cd storage-writer && go run .
   ```

## Work In Progress


