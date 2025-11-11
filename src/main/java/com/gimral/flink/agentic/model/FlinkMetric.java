package com.gimral.flink.agentic.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Objects;

/**
 * Data model representing a Flink metric event
 */
public class FlinkMetric implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("job_id")
    private String jobId;

    @JsonProperty("metric_name")
    private String metricName;

    @JsonProperty("metric_value")
    private double metricValue;

    @JsonProperty("host")
    private String host;

    @JsonProperty("task_name")
    private String taskName;

    public FlinkMetric() {
    }

    public FlinkMetric(long timestamp, String jobId, String metricName, double metricValue, String host, String taskName) {
        this.timestamp = timestamp;
        this.jobId = jobId;
        this.metricName = metricName;
        this.metricValue = metricValue;
        this.host = host;
        this.taskName = taskName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public double getMetricValue() {
        return metricValue;
    }

    public void setMetricValue(double metricValue) {
        this.metricValue = metricValue;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlinkMetric that = (FlinkMetric) o;
        return timestamp == that.timestamp &&
                Double.compare(that.metricValue, metricValue) == 0 &&
                Objects.equals(jobId, that.jobId) &&
                Objects.equals(metricName, that.metricName) &&
                Objects.equals(host, that.host) &&
                Objects.equals(taskName, that.taskName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, jobId, metricName, metricValue, host, taskName);
    }

    @Override
    public String toString() {
        return "FlinkMetric{" +
                "timestamp=" + timestamp +
                ", jobId='" + jobId + '\'' +
                ", metricName='" + metricName + '\'' +
                ", metricValue=" + metricValue +
                ", host='" + host + '\'' +
                ", taskName='" + taskName + '\'' +
                '}';
    }
}
