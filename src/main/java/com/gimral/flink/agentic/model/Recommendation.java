package com.gimral.flink.agentic.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Data model representing an operational recommendation
 */
public class Recommendation implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("job_id")
    private String jobId;

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("severity")
    private String severity;

    @JsonProperty("scenario")
    private String scenario;

    @JsonProperty("recommended_actions")
    private List<String> recommendedActions;

    @JsonProperty("rationale")
    private String rationale;

    @JsonProperty("confidence_score")
    private double confidenceScore;

    public Recommendation() {
        this.recommendedActions = new ArrayList<>();
    }

    public Recommendation(String jobId, long timestamp, String severity, String scenario, 
                         List<String> recommendedActions, String rationale, double confidenceScore) {
        this.jobId = jobId;
        this.timestamp = timestamp;
        this.severity = severity;
        this.scenario = scenario;
        this.recommendedActions = recommendedActions != null ? recommendedActions : new ArrayList<>();
        this.rationale = rationale;
        this.confidenceScore = confidenceScore;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public String getScenario() {
        return scenario;
    }

    public void setScenario(String scenario) {
        this.scenario = scenario;
    }

    public List<String> getRecommendedActions() {
        return recommendedActions;
    }

    public void setRecommendedActions(List<String> recommendedActions) {
        this.recommendedActions = recommendedActions;
    }

    public String getRationale() {
        return rationale;
    }

    public void setRationale(String rationale) {
        this.rationale = rationale;
    }

    public double getConfidenceScore() {
        return confidenceScore;
    }

    public void setConfidenceScore(double confidenceScore) {
        this.confidenceScore = confidenceScore;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Recommendation that = (Recommendation) o;
        return timestamp == that.timestamp &&
                Double.compare(that.confidenceScore, confidenceScore) == 0 &&
                Objects.equals(jobId, that.jobId) &&
                Objects.equals(severity, that.severity) &&
                Objects.equals(scenario, that.scenario);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, timestamp, severity, scenario, confidenceScore);
    }

    @Override
    public String toString() {
        return "Recommendation{" +
                "jobId='" + jobId + '\'' +
                ", timestamp=" + timestamp +
                ", severity='" + severity + '\'' +
                ", scenario='" + scenario + '\'' +
                ", recommendedActions=" + recommendedActions +
                ", rationale='" + rationale + '\'' +
                ", confidenceScore=" + confidenceScore +
                '}';
    }
}
