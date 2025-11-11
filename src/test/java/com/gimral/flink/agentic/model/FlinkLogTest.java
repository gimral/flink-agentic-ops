package com.gimral.flink.agentic.model;

import org.junit.Test;
import static org.junit.Assert.*;

public class FlinkLogTest {

    @Test
    public void testFlinkLogCreation() {
        FlinkLog log = new FlinkLog(
            System.currentTimeMillis(),
            "job-123",
            "ERROR",
            "Connection timeout",
            "host-1",
            "task-1",
            "java.net.SocketTimeoutException"
        );

        assertEquals("job-123", log.getJobId());
        assertEquals("ERROR", log.getLogLevel());
        assertEquals("Connection timeout", log.getMessage());
        assertEquals("host-1", log.getHost());
        assertEquals("task-1", log.getTaskName());
        assertEquals("java.net.SocketTimeoutException", log.getException());
    }

    @Test
    public void testFlinkLogEquality() {
        long timestamp = System.currentTimeMillis();
        FlinkLog log1 = new FlinkLog(timestamp, "job-123", "ERROR", "Connection timeout", "host-1", "task-1", "java.net.SocketTimeoutException");
        FlinkLog log2 = new FlinkLog(timestamp, "job-123", "ERROR", "Connection timeout", "host-1", "task-1", "java.net.SocketTimeoutException");

        assertEquals(log1, log2);
        assertEquals(log1.hashCode(), log2.hashCode());
    }
}
