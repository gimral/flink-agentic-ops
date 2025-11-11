package com.gimral.flink.agentic.aggregator;

import com.gimral.flink.agentic.model.AggregatedData;
import com.gimral.flink.agentic.model.FlinkMetric;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Stateful aggregation of Flink metrics per job
 */
public class MetricAggregator extends KeyedProcessFunction<String, FlinkMetric, AggregatedData> {
    private static final long serialVersionUID = 1L;

    private transient ValueState<AggregatedData> aggregatedState;
    private final long windowSizeMs;

    public MetricAggregator(long windowSizeMs) {
        this.windowSizeMs = windowSizeMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<AggregatedData> descriptor = 
            new ValueStateDescriptor<>("aggregated-metrics", AggregatedData.class);
        aggregatedState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(FlinkMetric metric, Context ctx, Collector<AggregatedData> out) throws Exception {
        AggregatedData aggregated = aggregatedState.value();
        
        if (aggregated == null || shouldStartNewWindow(aggregated, metric.getTimestamp())) {
            // Output previous window if exists
            if (aggregated != null) {
                out.collect(aggregated);
            }
            
            // Start new window
            aggregated = new AggregatedData(
                metric.getJobId(),
                metric.getTimestamp(),
                metric.getTimestamp() + windowSizeMs
            );
        }

        // Update aggregated metrics
        String metricName = metric.getMetricName();
        Double currentAvg = aggregated.getAvgMetrics().getOrDefault(metricName, 0.0);
        Long currentCount = aggregated.getMetricCounts().getOrDefault(metricName, 0L);
        
        // Calculate running average
        double newAvg = ((currentAvg * currentCount) + metric.getMetricValue()) / (currentCount + 1);
        aggregated.getAvgMetrics().put(metricName, newAvg);
        aggregated.getMetricCounts().put(metricName, currentCount + 1);

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
}
