# Flink Agentic Ops - Architecture

## System Overview

Flink Agentic Ops is an intelligent real-time monitoring system that consumes Flink metrics and logs from Kafka, performs stateful aggregation, and generates actionable recommendations using a vector database.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Flink Applications                             │
│                                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                 │
│  │   Flink Job  │  │   Flink Job  │  │   Flink Job  │                 │
│  │   (Job-1)    │  │   (Job-2)    │  │   (Job-N)    │                 │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                 │
│         │                  │                  │                          │
│         │ Metrics & Logs   │ Metrics & Logs   │ Metrics & Logs          │
│         └──────────────────┴──────────────────┘                          │
└─────────────────────────┬───────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Kafka Topics                                     │
│                                                                          │
│  ┌─────────────────────┐        ┌─────────────────────┐                │
│  │  flink-metrics      │        │  flink-logs         │                │
│  │  (Partitioned: 3)   │        │  (Partitioned: 3)   │                │
│  └──────────┬──────────┘        └──────────┬──────────┘                │
└─────────────┼──────────────────────────────┼────────────────────────────┘
              │                               │
              └───────────┬───────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Flink Agentic Ops Application                        │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │                       Kafka Sources                             │    │
│  │  ┌─────────────────────┐      ┌─────────────────────┐          │    │
│  │  │ FlinkMetric Source  │      │  FlinkLog Source    │          │    │
│  │  │ + Deserialization   │      │  + Deserialization  │          │    │
│  │  └──────────┬──────────┘      └──────────┬──────────┘          │    │
│  └─────────────┼───────────────────────────┼─────────────────────┘    │
│                │                            │                           │
│                ▼                            ▼                           │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │              Stateful Aggregation (Keyed by Job ID)             │    │
│  │  ┌─────────────────────┐      ┌─────────────────────┐          │    │
│  │  │  MetricAggregator   │      │   LogAggregator     │          │    │
│  │  │  - Running averages │      │   - Log level counts│          │    │
│  │  │  - Metric counts    │      │   - Error messages  │          │    │
│  │  │  - Windowed (60s)   │      │   - Exception types │          │    │
│  │  └──────────┬──────────┘      └──────────┬──────────┘          │    │
│  └─────────────┼───────────────────────────┼─────────────────────┘    │
│                │                            │                           │
│                └────────────┬───────────────┘                           │
│                             ▼                                           │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │                      DataCombiner                               │    │
│  │           (Merges metrics + logs per window)                    │    │
│  └────────────────────────┬────────────────────────────────────────┘    │
│                           │                                             │
│                           ▼                                             │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │                  RecommendationAgent                            │    │
│  │                                                                 │    │
│  │  1. Analyzes aggregated data                                   │    │
│  │  2. Detects anomalies/scenarios                                │    │
│  │  3. Queries vector database                                    │    │
│  │  4. Generates recommendations                                  │    │
│  │                                                                 │    │
│  │  ┌──────────────────────────────────────────────────────┐     │    │
│  │  │         Vector Database (InMemory/Pluggable)          │     │    │
│  │  │                                                        │     │    │
│  │  │  • High CPU Usage → Actions                           │     │    │
│  │  │  • High Memory Usage → Actions                        │     │    │
│  │  │  • Checkpoint Failures → Actions                      │     │    │
│  │  │  • High Backpressure → Actions                        │     │    │
│  │  │  • Task Failures → Actions                            │     │    │
│  │  │  • Kafka Lag → Actions                                │     │    │
│  │  │  • Network Bottleneck → Actions                       │     │    │
│  │  │  • Exception Spike → Actions                          │     │    │
│  │  │  • Low Throughput → Actions                           │     │    │
│  │  │  • State Growth → Actions                             │     │    │
│  │  └──────────────────────────────────────────────────────┘     │    │
│  └────────────────────────┬────────────────────────────────────────┘    │
│                           │                                             │
│                           ▼                                             │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │                    Kafka Sink                                   │    │
│  │            (Recommendations Serialization)                      │    │
│  └────────────────────────┬────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Kafka Topics                                     │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │              flink-recommendations (Partitioned: 3)             │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Downstream Consumers                                  │
│  • Alerting Systems                                                     │
│  • Dashboards                                                           │
│  • Automated Remediation Systems                                        │
│  • Operations Teams                                                     │
└─────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

1. **Input**: Flink applications emit metrics and logs to Kafka topics
2. **Ingestion**: Flink Agentic Ops consumes from both topics using Kafka sources
3. **Aggregation**: Data is aggregated per job ID in 60-second windows using Flink state
4. **Analysis**: Recommendation agent analyzes aggregated data for anomalies
5. **Recommendation**: Agent queries vector DB for similar scenarios and actions
6. **Output**: Recommendations with confidence scores are written to output topic
7. **Consumption**: Downstream systems consume and act on recommendations

## Component Details

### Data Models

- **FlinkMetric**: CPU, memory, throughput, backpressure, checkpoint status
- **FlinkLog**: Error/warning messages, exceptions, log levels
- **AggregatedData**: Combined metrics and logs for a time window
- **Recommendation**: Severity, scenario, actions, rationale, confidence score

### Stateful Processing

- **MetricAggregator**: Maintains running averages and counts per metric per job
- **LogAggregator**: Counts log levels, collects error messages and exceptions
- **DataCombiner**: Merges metric and log aggregations for the same window

### Agent Framework

- **RecommendationAgent**: Core intelligence layer
  - Threshold-based detection (CPU > 80%, Memory > 85%, etc.)
  - Pattern matching for failures and anomalies
  - Vector DB query for best practices
  - Confidence scoring based on relevance

### Vector Database

- **Interface**: Pluggable design for any vector DB
- **Implementation**: In-memory keyword matching (production: replace with real vector DB)
- **Content**: 10 operational scenarios with curated actions

## Scalability

- **Horizontal**: Increase Kafka partitions and Flink parallelism
- **Vertical**: Increase task manager resources for larger state
- **State Backend**: Supports RocksDB for large state management

## Fault Tolerance

- **Checkpointing**: Enabled every 30 seconds
- **State Recovery**: Automatic recovery from checkpoint on failure
- **Kafka Delivery**: At-least-once guarantee for recommendations

## Configuration

Environment variables control:
- Kafka connection details
- Topic names
- Window size (default: 60 seconds)
- Max error messages per window
- Consumer group ID

## Extension Points

1. **Vector Database**: Replace InMemoryVectorDatabase with production DB
2. **Scenarios**: Add new patterns in vector DB or RecommendationAgent
3. **Thresholds**: Adjust detection thresholds in RecommendationAgent
4. **Actions**: Extend ActionDocument with custom remediation steps
5. **Metrics**: Add new metric types in FlinkMetric model
