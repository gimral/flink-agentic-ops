package com.gimral.flink.agentic.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Objects;

/**
 * Data model representing a Flink log event
 */
public class FlinkLog implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("job_id")
    private String jobId;

    @JsonProperty("log_level")
    private String logLevel;

    @JsonProperty("message")
    private String message;

    @JsonProperty("host")
    private String host;

    @JsonProperty("task_name")
    private String taskName;

    @JsonProperty("exception")
    private String exception;

    public FlinkLog() {
    }

    public FlinkLog(long timestamp, String jobId, String logLevel, String message, String host, String taskName, String exception) {
        this.timestamp = timestamp;
        this.jobId = jobId;
        this.logLevel = logLevel;
        this.message = message;
        this.host = host;
        this.taskName = taskName;
        this.exception = exception;
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

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
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

    public String getException() {
        return exception;
    }

    public void setException(String exception) {
        this.exception = exception;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlinkLog flinkLog = (FlinkLog) o;
        return timestamp == flinkLog.timestamp &&
                Objects.equals(jobId, flinkLog.jobId) &&
                Objects.equals(logLevel, flinkLog.logLevel) &&
                Objects.equals(message, flinkLog.message) &&
                Objects.equals(host, flinkLog.host) &&
                Objects.equals(taskName, flinkLog.taskName) &&
                Objects.equals(exception, flinkLog.exception);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, jobId, logLevel, message, host, taskName, exception);
    }

    @Override
    public String toString() {
        return "FlinkLog{" +
                "timestamp=" + timestamp +
                ", jobId='" + jobId + '\'' +
                ", logLevel='" + logLevel + '\'' +
                ", message='" + message + '\'' +
                ", host='" + host + '\'' +
                ", taskName='" + taskName + '\'' +
                ", exception='" + exception + '\'' +
                '}';
    }
}
