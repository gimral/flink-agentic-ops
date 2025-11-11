#!/bin/bash
# Example script to generate sample data for testing the Flink Agentic Ops application
# This script requires kafka-console-producer to be available

KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}
METRICS_TOPIC=${METRICS_TOPIC:-flink-metrics}
LOGS_TOPIC=${LOGS_TOPIC:-flink-logs}

echo "Generating sample Flink metrics to topic: $METRICS_TOPIC"
echo "Generating sample Flink logs to topic: $LOGS_TOPIC"

# Generate sample metrics
for i in {1..10}; do
  TIMESTAMP=$(date +%s)000
  CPU=$(awk -v min=40 -v max=95 'BEGIN{srand(); print min+rand()*(max-min)}')
  MEMORY=$(awk -v min=50 -v max=90 'BEGIN{srand(); print min+rand()*(max-min)}')
  
  echo "{\"timestamp\":$TIMESTAMP,\"job_id\":\"job-12345\",\"metric_name\":\"cpu_usage\",\"metric_value\":$CPU,\"host\":\"taskmanager-1\",\"task_name\":\"Map\"}" | \
    kafka-console-producer --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS --topic $METRICS_TOPIC
  
  echo "{\"timestamp\":$TIMESTAMP,\"job_id\":\"job-12345\",\"metric_name\":\"memory_usage\",\"metric_value\":$MEMORY,\"host\":\"taskmanager-1\",\"task_name\":\"Map\"}" | \
    kafka-console-producer --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS --topic $METRICS_TOPIC
  
  sleep 2
done

# Generate sample logs
for i in {1..5}; do
  TIMESTAMP=$(date +%s)000
  
  echo "{\"timestamp\":$TIMESTAMP,\"job_id\":\"job-12345\",\"log_level\":\"ERROR\",\"message\":\"Connection timeout to external service\",\"host\":\"taskmanager-1\",\"task_name\":\"Sink\",\"exception\":\"java.net.SocketTimeoutException: Connection timeout\"}" | \
    kafka-console-producer --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS --topic $LOGS_TOPIC
  
  sleep 3
done

echo "Sample data generation complete"
