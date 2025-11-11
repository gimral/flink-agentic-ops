package com.gimral.flink.agentic.agent;

import com.gimral.flink.agentic.model.AggregatedData;
import com.gimral.flink.agentic.model.Recommendation;
import com.gimral.flink.agentic.vectordb.ActionDocument;
import com.gimral.flink.agentic.vectordb.VectorDatabase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Agent that analyzes aggregated data and generates recommendations using vector database
 */
public class RecommendationAgent extends ProcessFunction<AggregatedData, Recommendation> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RecommendationAgent.class);

    private transient VectorDatabase vectorDb;
    private final VectorDatabase vectorDbTemplate;

    // Thresholds for triggering recommendations
    private final double cpuThreshold = 80.0;
    private final double memoryThreshold = 85.0;
    private final long errorThreshold = 10;
    private final long exceptionThreshold = 5;

    public RecommendationAgent(VectorDatabase vectorDb) {
        this.vectorDbTemplate = vectorDb;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Initialize vector database
        vectorDb = vectorDbTemplate;
        vectorDb.initialize();
        LOG.info("RecommendationAgent initialized with vector database");
    }

    @Override
    public void processElement(AggregatedData data, Context ctx, Collector<Recommendation> out) throws Exception {
        LOG.debug("Processing aggregated data for job: {}", data.getJobId());

        // Analyze metrics and logs to identify scenarios
        List<String> detectedScenarios = analyzeData(data);

        // For each detected scenario, query vector database and generate recommendations
        for (String scenario : detectedScenarios) {
            List<ActionDocument> relevantActions = vectorDb.querySimilarScenarios(scenario, 3);
            
            if (!relevantActions.isEmpty()) {
                Recommendation recommendation = createRecommendation(
                    data.getJobId(),
                    System.currentTimeMillis(),
                    scenario,
                    relevantActions
                );
                out.collect(recommendation);
                LOG.info("Generated recommendation for job {} - Scenario: {}", 
                    data.getJobId(), scenario);
            }
        }
    }

    private List<String> analyzeData(AggregatedData data) {
        List<String> scenarios = new ArrayList<>();

        // Check CPU metrics
        Double avgCpu = data.getAvgMetrics().get("cpu_usage");
        if (avgCpu != null && avgCpu > cpuThreshold) {
            scenarios.add("High CPU Usage: CPU utilization at " + String.format("%.1f", avgCpu) + "%");
        }

        // Check memory metrics
        Double avgMemory = data.getAvgMetrics().get("memory_usage");
        if (avgMemory != null && avgMemory > memoryThreshold) {
            scenarios.add("High Memory Usage: Memory utilization at " + String.format("%.1f", avgMemory) + "%");
        }

        // Check for checkpoint failures
        Double checkpointFailures = data.getAvgMetrics().get("checkpoint_failures");
        if (checkpointFailures != null && checkpointFailures > 0) {
            scenarios.add("Checkpoint Failures: " + checkpointFailures.intValue() + " failures detected");
        }

        // Check backpressure
        Double backpressure = data.getAvgMetrics().get("backpressure");
        if (backpressure != null && backpressure > 0.5) {
            scenarios.add("High Backpressure: Backpressure level at " + String.format("%.2f", backpressure));
        }

        // Check log levels
        Long errorCount = data.getLogLevelCounts().getOrDefault("ERROR", 0L);
        if (errorCount > errorThreshold) {
            scenarios.add("Exception Spike: " + errorCount + " errors detected");
        }

        // Check for exceptions
        if (!data.getExceptionTypes().isEmpty() && data.getExceptionTypes().size() >= exceptionThreshold) {
            scenarios.add("Task Failures: Multiple exception types detected - " + 
                String.join(", ", data.getExceptionTypes()));
        }

        // Check throughput
        Double throughput = data.getAvgMetrics().get("throughput");
        if (throughput != null && throughput < 1000) {
            scenarios.add("Low Throughput: Only " + throughput.intValue() + " records/sec");
        }

        // Check state size
        Double stateSize = data.getAvgMetrics().get("state_size_mb");
        if (stateSize != null && stateSize > 10000) {
            scenarios.add("State Growth: State size at " + String.format("%.1f", stateSize) + " MB");
        }

        return scenarios;
    }

    private Recommendation createRecommendation(
            String jobId, 
            long timestamp, 
            String scenario, 
            List<ActionDocument> actionDocs) {
        
        // Use the top action document
        ActionDocument topAction = actionDocs.get(0);
        
        // Build rationale from all relevant documents
        StringBuilder rationale = new StringBuilder();
        rationale.append("Analysis: ").append(scenario).append(". ");
        rationale.append(topAction.getDescription());
        
        if (actionDocs.size() > 1) {
            rationale.append(" Similar scenarios found: ");
            for (int i = 1; i < actionDocs.size(); i++) {
                rationale.append(actionDocs.get(i).getScenario());
                if (i < actionDocs.size() - 1) {
                    rationale.append(", ");
                }
            }
        }

        // Calculate confidence score based on relevance scores
        double avgRelevance = actionDocs.stream()
            .mapToDouble(ActionDocument::getRelevanceScore)
            .average()
            .orElse(0.0);
        double confidenceScore = Math.min(avgRelevance / 10.0, 1.0); // Normalize to 0-1

        return new Recommendation(
            jobId,
            timestamp,
            topAction.getSeverity(),
            scenario,
            topAction.getActions(),
            rationale.toString(),
            confidenceScore
        );
    }
}
