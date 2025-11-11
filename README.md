# Flink Agentic Ops

An intelligent agentic application built with Apache Flink to monitor Flink metrics and logs, perform stateful aggregation, and generate operational recommendations using a vector database.

## Overview

Flink Agentic Ops is a real-time monitoring and recommendation system that:
- Consumes Flink metrics and logs from Kafka topics
- Aggregates data using Flink stateful operations
- Uses a vector database to match scenarios with predefined operational actions
- Generates intelligent recommendations based on detected patterns
- Outputs recommendations to a Kafka topic for downstream consumption

## Architecture

```
Kafka (Metrics Topic) ──┐
                        ├──> Metric Aggregator ──┐
Kafka (Logs Topic) ─────┤                        ├──> Data Combiner ──> Recommendation Agent ──> Kafka (Recommendations Topic)
                        └──> Log Aggregator ─────┘                            │
                                                                               ▼
                                                                        Vector Database
```

### Components

1. **Data Models**: 
   - `FlinkMetric`: Represents a metric event (CPU, memory, throughput, etc.)
   - `FlinkLog`: Represents a log event (errors, warnings, exceptions)
   - `AggregatedData`: Combined metrics and logs for a time window
   - `Recommendation`: Generated operational recommendation

2. **Aggregators**:
   - `MetricAggregator`: Stateful aggregation of metrics per job using Flink's KeyedProcessFunction
   - `LogAggregator`: Stateful aggregation of logs per job with error message collection
   - `DataCombiner`: Combines metric and log aggregations into unified data

3. **Agent Framework**:
   - `RecommendationAgent`: Analyzes aggregated data and generates recommendations using vector database
   - `VectorDatabase`: Interface for storing and querying operational scenarios
   - `InMemoryVectorDatabase`: Simple implementation using keyword matching (can be replaced with real vector DB)

4. **Kafka Integration**:
   - Deserialization schemas for metrics and logs
   - Serialization schema for recommendations
   - Source and sink connectors for Kafka

## Features

- **Real-time Monitoring**: Continuously processes metrics and logs from Flink applications
- **Stateful Aggregation**: Uses Flink's state management for windowed aggregations
- **Intelligent Recommendations**: Matches detected scenarios with best practices from vector database
- **Scalable Architecture**: Built on Flink for horizontal scalability
- **Fault Tolerant**: Leverages Flink's checkpointing for exactly-once processing
- **Extensible**: Easy to add new scenarios and recommendation patterns

## Predefined Scenarios

The system includes the following built-in scenarios:
- High CPU Usage
- High Memory Usage
- Checkpoint Failures
- High Backpressure
- Task Failures
- Kafka Lag Increasing
- Network Bottleneck
- Exception Spike
- Low Throughput
- State Growth

## Prerequisites

- Java 11 or higher
- Apache Maven 3.6+
- Apache Kafka 3.x
- Apache Flink 1.18.0

## Building the Application

```bash
mvn clean package
```

This will create a JAR file in the `target/` directory: `flink-agentic-ops-1.0-SNAPSHOT.jar`

## Configuration

Configuration can be set via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | `localhost:9092` |
| `METRICS_TOPIC` | Kafka topic for metrics | `flink-metrics` |
| `LOGS_TOPIC` | Kafka topic for logs | `flink-logs` |
| `RECOMMENDATIONS_TOPIC` | Kafka topic for recommendations | `flink-recommendations` |
| `CONSUMER_GROUP` | Kafka consumer group ID | `flink-agentic-ops` |
| `WINDOW_SIZE_MS` | Aggregation window size in milliseconds | `60000` (1 minute) |
| `MAX_ERROR_MESSAGES` | Maximum error messages to collect per window | `10` |

## Running the Application

### Local Execution

```bash
# Set environment variables
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export METRICS_TOPIC=flink-metrics
export LOGS_TOPIC=flink-logs
export RECOMMENDATIONS_TOPIC=flink-recommendations

# Run the application
./bin/flink run target/flink-agentic-ops-1.0-SNAPSHOT.jar
```

### Flink Cluster Execution

```bash
# Submit to Flink cluster
./bin/flink run -c com.gimral.flink.agentic.FlinkAgenticOpsApp \
  target/flink-agentic-ops-1.0-SNAPSHOT.jar
```

## Input Data Format

### Metrics Topic
```json
{
  "timestamp": 1699876543000,
  "job_id": "job-12345",
  "metric_name": "cpu_usage",
  "metric_value": 85.5,
  "host": "taskmanager-1",
  "task_name": "Map"
}
```

### Logs Topic
```json
{
  "timestamp": 1699876543000,
  "job_id": "job-12345",
  "log_level": "ERROR",
  "message": "Connection timeout to external service",
  "host": "taskmanager-1",
  "task_name": "Sink",
  "exception": "java.net.SocketTimeoutException: Connection timeout"
}
```

## Output Format

### Recommendations Topic
```json
{
  "job_id": "job-12345",
  "timestamp": 1699876600000,
  "severity": "HIGH",
  "scenario": "High CPU Usage: CPU utilization at 85.5%",
  "recommended_actions": [
    "Check for inefficient operators or UDFs",
    "Review parallelism settings and consider increasing parallelism",
    "Analyze task manager resource allocation",
    "Look for hot partitions in keyed operations"
  ],
  "rationale": "Analysis: High CPU Usage: CPU utilization at 85.5%. CPU utilization is above 80% for extended period",
  "confidence_score": 0.85
}
```

## Testing

Run the unit tests:

```bash
mvn test
```

## Extending the System

### Adding New Scenarios

1. Edit `InMemoryVectorDatabase.java` and add new `ActionDocument` entries in the `initialize()` method
2. Update `RecommendationAgent.java` to detect the new scenario in the `analyzeData()` method

### Using a Real Vector Database

Replace `InMemoryVectorDatabase` with a production vector database:
1. Implement the `VectorDatabase` interface for your chosen database (Qdrant, Pinecone, Milvus, etc.)
2. Add appropriate dependencies to `pom.xml`
3. Update `FlinkAgenticOpsApp.java` to use the new implementation

## Project Structure

```
src/
├── main/
│   ├── java/com/gimral/flink/agentic/
│   │   ├── model/              # Data models
│   │   ├── aggregator/         # Stateful aggregators
│   │   ├── agent/              # Recommendation agent
│   │   ├── vectordb/           # Vector database interface and implementation
│   │   ├── source/             # Kafka deserialization schemas
│   │   ├── sink/               # Kafka serialization schemas
│   │   └── FlinkAgenticOpsApp.java  # Main application
│   └── resources/
│       ├── log4j2.xml          # Logging configuration
│       └── application.properties  # Application properties
└── test/
    └── java/com/gimral/flink/agentic/  # Unit tests
```

## License

See LICENSE file for details.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.
