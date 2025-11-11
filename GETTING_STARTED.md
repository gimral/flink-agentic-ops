# Getting Started with Flink Agentic Ops

This guide will help you get the Flink Agentic Ops application up and running.

## Prerequisites

- Java 11 or higher
- Apache Maven 3.6+
- Docker and Docker Compose (for local development)

## Quick Start with Docker Compose

The easiest way to test the application locally is using Docker Compose, which will set up Kafka and Flink for you.

### 1. Start the Infrastructure

```bash
# Start Kafka and Flink cluster
docker-compose up -d

# Wait for services to be ready (about 30 seconds)
docker-compose logs -f kafka-setup
```

### 2. Build the Application

```bash
# Build the application
mvn clean package

# The JAR will be created at: target/flink-agentic-ops-1.0-SNAPSHOT.jar
```

### 3. Submit the Job to Flink

```bash
# Copy the JAR to the jobmanager container
docker cp target/flink-agentic-ops-1.0-SNAPSHOT.jar jobmanager:/tmp/

# Submit the job
docker exec -it jobmanager flink run \
  -c com.gimral.flink.agentic.FlinkAgenticOpsApp \
  /tmp/flink-agentic-ops-1.0-SNAPSHOT.jar
```

### 4. Monitor the Job

Open your browser and navigate to: http://localhost:8081

You should see the Flink Web UI with your job running.

### 5. Generate Sample Data

```bash
# Generate sample metrics and logs
./scripts/generate-sample-data.sh
```

### 6. Consume Recommendations

```bash
# Watch the recommendations being generated
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic flink-recommendations \
  --from-beginning \
  --property print.key=false \
  --property print.value=true
```

## Manual Setup (Without Docker)

If you prefer to set up Kafka and Flink manually:

### 1. Start Kafka

```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka
bin/kafka-server-start.sh config/server.properties

# Create topics
bin/kafka-topics.sh --create --topic flink-metrics --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
bin/kafka-topics.sh --create --topic flink-logs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
bin/kafka-topics.sh --create --topic flink-recommendations --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 2. Start Flink

```bash
# Start Flink cluster
./bin/start-cluster.sh

# Verify Flink is running
./bin/flink list
```

### 3. Build and Submit the Job

```bash
# Build the application
mvn clean package

# Submit to Flink
./bin/flink run -c com.gimral.flink.agentic.FlinkAgenticOpsApp target/flink-agentic-ops-1.0-SNAPSHOT.jar
```

## Sending Test Data

### Using Kafka Console Producer

```bash
# Send a metric event
echo '{"timestamp":1699876543000,"job_id":"job-12345","metric_name":"cpu_usage","metric_value":85.5,"host":"taskmanager-1","task_name":"Map"}' | \
  kafka-console-producer --bootstrap-server localhost:9092 --topic flink-metrics

# Send a log event
echo '{"timestamp":1699876543000,"job_id":"job-12345","log_level":"ERROR","message":"Connection timeout","host":"taskmanager-1","task_name":"Sink","exception":"java.net.SocketTimeoutException"}' | \
  kafka-console-producer --bootstrap-server localhost:9092 --topic flink-logs
```

### Using Sample Files

```bash
# Send sample metric
cat examples/sample-metric.json | kafka-console-producer --bootstrap-server localhost:9092 --topic flink-metrics

# Send sample log
cat examples/sample-log.json | kafka-console-producer --bootstrap-server localhost:9092 --topic flink-logs
```

## Configuration

The application can be configured via environment variables:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export METRICS_TOPIC=flink-metrics
export LOGS_TOPIC=flink-logs
export RECOMMENDATIONS_TOPIC=flink-recommendations
export CONSUMER_GROUP=flink-agentic-ops
export WINDOW_SIZE_MS=60000
export MAX_ERROR_MESSAGES=10
```

## Troubleshooting

### Job Not Receiving Data

1. Check if Kafka topics exist:
   ```bash
   kafka-topics.sh --list --bootstrap-server localhost:9092
   ```

2. Check if data is in topics:
   ```bash
   kafka-console-consumer --bootstrap-server localhost:9092 --topic flink-metrics --from-beginning --max-messages 1
   ```

3. Check Flink job logs:
   ```bash
   # For Docker
   docker exec -it taskmanager tail -f /opt/flink/log/flink-*-taskexecutor-*.out
   
   # For manual setup
   tail -f log/flink-*-taskexecutor-*.out
   ```

### Job Failed to Start

1. Check if all dependencies are satisfied
2. Verify Java version (must be 11 or higher)
3. Check Flink Web UI for error messages
4. Review the job logs for stack traces

### No Recommendations Generated

1. Ensure both metrics and logs are being sent
2. Check that metric/log values exceed the thresholds (e.g., CPU > 80%)
3. Verify the aggregation window has completed (default: 1 minute)
4. Check the logs for any processing errors

## Next Steps

- Explore the [README.md](README.md) for detailed architecture information
- Review the predefined scenarios in `InMemoryVectorDatabase.java`
- Customize thresholds in `RecommendationAgent.java`
- Integrate with a real vector database for production use
- Add custom scenarios and recommendations

## Clean Up

```bash
# Stop Docker Compose
docker-compose down

# Or stop manual setup
./bin/stop-cluster.sh  # Flink
bin/kafka-server-stop.sh  # Kafka
bin/zookeeper-server-stop.sh  # Zookeeper
```
