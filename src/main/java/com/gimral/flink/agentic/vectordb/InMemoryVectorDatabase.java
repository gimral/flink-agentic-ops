package com.gimral.flink.agentic.vectordb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Simple in-memory implementation of VectorDatabase using keyword matching
 * In production, this would be replaced with a real vector database like Qdrant, Pinecone, or Milvus
 */
public class InMemoryVectorDatabase implements VectorDatabase {
    private static final long serialVersionUID = 1L;
    
    private List<ActionDocument> documents;

    public InMemoryVectorDatabase() {
        this.documents = new ArrayList<>();
    }

    @Override
    public void initialize() {
        // Predefined scenarios and actions for Flink operations
        documents.add(new ActionDocument(
            "High CPU Usage",
            "CPU utilization is above 80% for extended period",
            Arrays.asList(
                "Check for inefficient operators or UDFs",
                "Review parallelism settings and consider increasing parallelism",
                "Analyze task manager resource allocation",
                "Look for hot partitions in keyed operations"
            ),
            "HIGH"
        ));

        documents.add(new ActionDocument(
            "High Memory Usage",
            "Memory usage is above 85% threshold",
            Arrays.asList(
                "Review state size and enable state backend TTL",
                "Consider increasing task manager memory",
                "Check for memory leaks in user code",
                "Enable RocksDB state backend for large states"
            ),
            "HIGH"
        ));

        documents.add(new ActionDocument(
            "Checkpoint Failures",
            "Checkpoints are failing or timing out frequently",
            Arrays.asList(
                "Increase checkpoint timeout configuration",
                "Check for backpressure in the pipeline",
                "Review state backend performance",
                "Ensure sufficient storage for checkpoints"
            ),
            "CRITICAL"
        ));

        documents.add(new ActionDocument(
            "High Backpressure",
            "Backpressure detected in the job graph",
            Arrays.asList(
                "Identify bottleneck operators",
                "Increase parallelism of slow operators",
                "Optimize sink performance",
                "Review window operations and reduce window size if possible"
            ),
            "HIGH"
        ));

        documents.add(new ActionDocument(
            "Task Failures",
            "Tasks are failing and restarting frequently",
            Arrays.asList(
                "Review exception stack traces in logs",
                "Check external system availability (databases, Kafka, etc.)",
                "Verify input data quality and handle edge cases",
                "Increase task failure tolerance or restart strategy delay"
            ),
            "CRITICAL"
        ));

        documents.add(new ActionDocument(
            "Kafka Lag Increasing",
            "Consumer lag is increasing over time",
            Arrays.asList(
                "Increase Kafka consumer parallelism",
                "Check for slow processing operators downstream",
                "Review Kafka broker performance",
                "Consider scaling out the Flink job"
            ),
            "MEDIUM"
        ));

        documents.add(new ActionDocument(
            "Network Bottleneck",
            "Network I/O is saturated or high latency observed",
            Arrays.asList(
                "Review data shuffling operations",
                "Optimize serialization format",
                "Consider data locality optimization",
                "Check network infrastructure and bandwidth"
            ),
            "MEDIUM"
        ));

        documents.add(new ActionDocument(
            "Exception Spike",
            "Sudden increase in exception count",
            Arrays.asList(
                "Review recent exception types and messages",
                "Check for data quality issues in input streams",
                "Verify external service dependencies",
                "Add defensive error handling in operators"
            ),
            "HIGH"
        ));

        documents.add(new ActionDocument(
            "Low Throughput",
            "Job throughput is below expected levels",
            Arrays.asList(
                "Identify slow operators using metrics",
                "Increase parallelism where needed",
                "Review for data skew issues",
                "Optimize expensive operations and UDFs"
            ),
            "MEDIUM"
        ));

        documents.add(new ActionDocument(
            "State Growth",
            "State size is growing unbounded",
            Arrays.asList(
                "Enable state TTL to expire old entries",
                "Review state retention policies",
                "Consider state cleanup logic",
                "Monitor for memory leaks in state"
            ),
            "HIGH"
        ));
    }

    @Override
    public List<ActionDocument> querySimilarScenarios(String query, int topK) {
        // Simple keyword-based matching (in production, use proper vector similarity)
        String queryLower = query.toLowerCase();
        String[] keywords = queryLower.split("\\s+");

        return documents.stream()
            .map(doc -> {
                ActionDocument scoredDoc = new ActionDocument(
                    doc.getScenario(),
                    doc.getDescription(),
                    doc.getActions(),
                    doc.getSeverity()
                );
                
                // Calculate simple relevance score based on keyword matches
                double score = calculateRelevanceScore(doc, keywords);
                scoredDoc.setRelevanceScore(score);
                
                return scoredDoc;
            })
            .filter(doc -> doc.getRelevanceScore() > 0)
            .sorted((d1, d2) -> Double.compare(d2.getRelevanceScore(), d1.getRelevanceScore()))
            .limit(topK)
            .collect(Collectors.toList());
    }

    private double calculateRelevanceScore(ActionDocument doc, String[] keywords) {
        double score = 0.0;
        String scenarioLower = doc.getScenario().toLowerCase();
        String descriptionLower = doc.getDescription().toLowerCase();

        for (String keyword : keywords) {
            if (scenarioLower.contains(keyword)) {
                score += 2.0; // Higher weight for scenario matches
            }
            if (descriptionLower.contains(keyword)) {
                score += 1.0;
            }
        }

        return score;
    }
}
