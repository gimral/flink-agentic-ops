package com.gimral.flink.agentic;

import com.gimral.flink.agentic.agent.RecommendationAgent;
import com.gimral.flink.agentic.aggregator.DataCombiner;
import com.gimral.flink.agentic.aggregator.LogAggregator;
import com.gimral.flink.agentic.aggregator.MetricAggregator;
import com.gimral.flink.agentic.model.AggregatedData;
import com.gimral.flink.agentic.model.FlinkLog;
import com.gimral.flink.agentic.model.FlinkMetric;
import com.gimral.flink.agentic.model.Recommendation;
import com.gimral.flink.agentic.sink.RecommendationSerializationSchema;
import com.gimral.flink.agentic.source.FlinkLogDeserializationSchema;
import com.gimral.flink.agentic.source.FlinkMetricDeserializationSchema;
import com.gimral.flink.agentic.vectordb.InMemoryVectorDatabase;
import com.gimral.flink.agentic.vectordb.VectorDatabase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Main application for Flink Agentic Operations
 * Monitors Flink metrics and logs from Kafka, aggregates data, and generates recommendations
 */
public class FlinkAgenticOpsApp {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkAgenticOpsApp.class);

    // Configuration parameters
    private static final String KAFKA_BOOTSTRAP_SERVERS = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String METRICS_TOPIC = getEnvOrDefault("METRICS_TOPIC", "flink-metrics");
    private static final String LOGS_TOPIC = getEnvOrDefault("LOGS_TOPIC", "flink-logs");
    private static final String RECOMMENDATIONS_TOPIC = getEnvOrDefault("RECOMMENDATIONS_TOPIC", "flink-recommendations");
    private static final String CONSUMER_GROUP = getEnvOrDefault("CONSUMER_GROUP", "flink-agentic-ops");
    private static final long WINDOW_SIZE_MS = Long.parseLong(getEnvOrDefault("WINDOW_SIZE_MS", "60000")); // 1 minute
    private static final int MAX_ERROR_MESSAGES = Integer.parseInt(getEnvOrDefault("MAX_ERROR_MESSAGES", "10"));

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink Agentic Ops Application");
        LOG.info("Kafka Bootstrap Servers: {}", KAFKA_BOOTSTRAP_SERVERS);
        LOG.info("Metrics Topic: {}", METRICS_TOPIC);
        LOG.info("Logs Topic: {}", LOGS_TOPIC);
        LOG.info("Recommendations Topic: {}", RECOMMENDATIONS_TOPIC);
        LOG.info("Window Size: {} ms", WINDOW_SIZE_MS);

        // Create execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(30000); // Checkpoint every 30 seconds

        // Create Kafka sources
        KafkaSource<FlinkMetric> metricsSource = KafkaSource.<FlinkMetric>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(METRICS_TOPIC)
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new FlinkMetricDeserializationSchema())
                .build();

        KafkaSource<FlinkLog> logsSource = KafkaSource.<FlinkLog>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(LOGS_TOPIC)
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new FlinkLogDeserializationSchema())
                .build();

        // Read from Kafka sources
        DataStream<FlinkMetric> metricsStream = env
                .fromSource(metricsSource, WatermarkStrategy
                        .<FlinkMetric>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((metric, timestamp) -> metric.getTimestamp()), 
                        "Metrics Source");

        DataStream<FlinkLog> logsStream = env
                .fromSource(logsSource, WatermarkStrategy
                        .<FlinkLog>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((log, timestamp) -> log.getTimestamp()), 
                        "Logs Source");

        // Aggregate metrics by job ID
        DataStream<AggregatedData> aggregatedMetrics = metricsStream
                .keyBy(FlinkMetric::getJobId)
                .process(new MetricAggregator(WINDOW_SIZE_MS))
                .name("Metric Aggregator");

        // Aggregate logs by job ID
        DataStream<AggregatedData> aggregatedLogs = logsStream
                .keyBy(FlinkLog::getJobId)
                .process(new LogAggregator(WINDOW_SIZE_MS, MAX_ERROR_MESSAGES))
                .name("Log Aggregator");

        // Combine metrics and logs
        DataStream<AggregatedData> combinedData = aggregatedMetrics
                .connect(aggregatedLogs)
                .keyBy(AggregatedData::getJobId, AggregatedData::getJobId)
                .process(new DataCombiner())
                .name("Data Combiner");

        // Initialize vector database
        VectorDatabase vectorDb = new InMemoryVectorDatabase();

        // Generate recommendations using the agent
        DataStream<Recommendation> recommendations = combinedData
                .process(new RecommendationAgent(vectorDb))
                .name("Recommendation Agent");

        // Create Kafka sink for recommendations
        KafkaSink<Recommendation> recommendationsSink = KafkaSink.<Recommendation>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(RECOMMENDATIONS_TOPIC)
                        .setValueSerializationSchema(new RecommendationSerializationSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // Write recommendations to Kafka
        recommendations.sinkTo(recommendationsSink).name("Recommendations Sink");

        // Log recommendations for debugging
        recommendations.print();

        // Execute the job
        env.execute("Flink Agentic Ops - Monitoring and Recommendations");
        LOG.info("Flink Agentic Ops Application finished");
    }

    private static String getEnvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }
}
