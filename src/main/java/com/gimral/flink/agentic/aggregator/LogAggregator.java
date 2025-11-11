package com.gimral.flink.agentic.aggregator;

import com.gimral.flink.agentic.model.AggregatedData;
import com.gimral.flink.agentic.model.FlinkLog;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Stateful aggregation of Flink logs per job
 */
public class LogAggregator extends KeyedProcessFunction<String, FlinkLog, AggregatedData> {
    private static final long serialVersionUID = 1L;

    private transient ValueState<AggregatedData> aggregatedState;
    private final long windowSizeMs;
    private final int maxErrorMessages;

    public LogAggregator(long windowSizeMs, int maxErrorMessages) {
        this.windowSizeMs = windowSizeMs;
        this.maxErrorMessages = maxErrorMessages;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<AggregatedData> descriptor = 
            new ValueStateDescriptor<>("aggregated-logs", AggregatedData.class);
        aggregatedState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(FlinkLog log, Context ctx, Collector<AggregatedData> out) throws Exception {
        AggregatedData aggregated = aggregatedState.value();
        
        if (aggregated == null || shouldStartNewWindow(aggregated, log.getTimestamp())) {
            // Output previous window if exists
            if (aggregated != null) {
                out.collect(aggregated);
            }
            
            // Start new window
            aggregated = new AggregatedData(
                log.getJobId(),
                log.getTimestamp(),
                log.getTimestamp() + windowSizeMs
            );
        }

        // Update log level counts
        String logLevel = log.getLogLevel();
        Long currentCount = aggregated.getLogLevelCounts().getOrDefault(logLevel, 0L);
        aggregated.getLogLevelCounts().put(logLevel, currentCount + 1);

        // Collect error messages (limited to maxErrorMessages)
        if (("ERROR".equals(logLevel) || "WARN".equals(logLevel)) && 
            aggregated.getErrorMessages().size() < maxErrorMessages) {
            aggregated.getErrorMessages().add(log.getMessage());
        }

        // Collect exception types
        if (log.getException() != null && !log.getException().isEmpty()) {
            String exceptionType = extractExceptionType(log.getException());
            if (!aggregated.getExceptionTypes().contains(exceptionType)) {
                aggregated.getExceptionTypes().add(exceptionType);
            }
        }

        aggregatedState.update(aggregated);
        
        // Register timer for window end
        ctx.timerService().registerProcessingTimeTimer(aggregated.getWindowEnd());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AggregatedData> out) throws Exception {
        AggregatedData aggregated = aggregatedState.value();
        if (aggregated != null && timestamp >= aggregated.getWindowEnd()) {
            out.collect(aggregated);
            aggregatedState.clear();
        }
    }

    private boolean shouldStartNewWindow(AggregatedData aggregated, long timestamp) {
        return timestamp >= aggregated.getWindowEnd();
    }

    private String extractExceptionType(String exception) {
        // Extract the exception class name from the stack trace
        String[] lines = exception.split("\n");
        if (lines.length > 0) {
            String firstLine = lines[0].trim();
            int colonIndex = firstLine.indexOf(':');
            if (colonIndex > 0) {
                return firstLine.substring(0, colonIndex).trim();
            }
            return firstLine;
        }
        return "Unknown";
    }
}
