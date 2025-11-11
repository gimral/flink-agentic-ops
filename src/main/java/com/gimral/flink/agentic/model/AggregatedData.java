package com.gimral.flink.agentic.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Data model representing aggregated metrics and logs for a job
 */
public class AggregatedData implements Serializable {
    private static final long serialVersionUID = 1L;

    private String jobId;
    private long windowStart;
    private long windowEnd;
    private Map<String, Double> avgMetrics;
    private Map<String, Long> metricCounts;
    private Map<String, Long> logLevelCounts;
    private List<String> errorMessages;
    private List<String> exceptionTypes;

    public AggregatedData() {
        this.avgMetrics = new HashMap<>();
        this.metricCounts = new HashMap<>();
        this.logLevelCounts = new HashMap<>();
        this.errorMessages = new ArrayList<>();
        this.exceptionTypes = new ArrayList<>();
    }

    public AggregatedData(String jobId, long windowStart, long windowEnd) {
        this();
        this.jobId = jobId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Map<String, Double> getAvgMetrics() {
        return avgMetrics;
    }

    public void setAvgMetrics(Map<String, Double> avgMetrics) {
        this.avgMetrics = avgMetrics;
    }

    public Map<String, Long> getMetricCounts() {
        return metricCounts;
    }

    public void setMetricCounts(Map<String, Long> metricCounts) {
        this.metricCounts = metricCounts;
    }

    public Map<String, Long> getLogLevelCounts() {
        return logLevelCounts;
    }

    public void setLogLevelCounts(Map<String, Long> logLevelCounts) {
        this.logLevelCounts = logLevelCounts;
    }

    public List<String> getErrorMessages() {
        return errorMessages;
    }

    public void setErrorMessages(List<String> errorMessages) {
        this.errorMessages = errorMessages;
    }

    public List<String> getExceptionTypes() {
        return exceptionTypes;
    }

    public void setExceptionTypes(List<String> exceptionTypes) {
        this.exceptionTypes = exceptionTypes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregatedData that = (AggregatedData) o;
        return windowStart == that.windowStart &&
                windowEnd == that.windowEnd &&
                Objects.equals(jobId, that.jobId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, windowStart, windowEnd);
    }

    @Override
    public String toString() {
        return "AggregatedData{" +
                "jobId='" + jobId + '\'' +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                ", avgMetrics=" + avgMetrics +
                ", metricCounts=" + metricCounts +
                ", logLevelCounts=" + logLevelCounts +
                ", errorMessages=" + errorMessages +
                ", exceptionTypes=" + exceptionTypes +
                '}';
    }
}
