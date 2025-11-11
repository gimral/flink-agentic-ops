package com.gimral.flink.agentic.model;

import org.junit.Test;
import static org.junit.Assert.*;

public class FlinkMetricTest {

    @Test
    public void testFlinkMetricCreation() {
        FlinkMetric metric = new FlinkMetric(
            System.currentTimeMillis(),
            "job-123",
            "cpu_usage",
            75.5,
            "host-1",
            "task-1"
        );

        assertEquals("job-123", metric.getJobId());
        assertEquals("cpu_usage", metric.getMetricName());
        assertEquals(75.5, metric.getMetricValue(), 0.001);
        assertEquals("host-1", metric.getHost());
        assertEquals("task-1", metric.getTaskName());
    }

    @Test
    public void testFlinkMetricEquality() {
        long timestamp = System.currentTimeMillis();
        FlinkMetric metric1 = new FlinkMetric(timestamp, "job-123", "cpu_usage", 75.5, "host-1", "task-1");
        FlinkMetric metric2 = new FlinkMetric(timestamp, "job-123", "cpu_usage", 75.5, "host-1", "task-1");

        assertEquals(metric1, metric2);
        assertEquals(metric1.hashCode(), metric2.hashCode());
    }
}
