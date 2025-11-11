package com.gimral.flink.agentic.aggregator;

import com.gimral.flink.agentic.model.AggregatedData;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Combines aggregated metrics and logs into a single aggregated data stream
 */
public class DataCombiner extends CoProcessFunction<AggregatedData, AggregatedData, AggregatedData> {
    private static final long serialVersionUID = 1L;

    private transient ValueState<AggregatedData> metricsState;
    private transient ValueState<AggregatedData> logsState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<AggregatedData> metricsDescriptor = 
            new ValueStateDescriptor<>("metrics-data", AggregatedData.class);
        ValueStateDescriptor<AggregatedData> logsDescriptor = 
            new ValueStateDescriptor<>("logs-data", AggregatedData.class);
        
        metricsState = getRuntimeContext().getState(metricsDescriptor);
        logsState = getRuntimeContext().getState(logsDescriptor);
    }

    @Override
    public void processElement1(AggregatedData metrics, Context ctx, Collector<AggregatedData> out) throws Exception {
        metricsState.update(metrics);
        tryEmitCombined(out);
    }

    @Override
    public void processElement2(AggregatedData logs, Context ctx, Collector<AggregatedData> out) throws Exception {
        logsState.update(logs);
        tryEmitCombined(out);
    }

    private void tryEmitCombined(Collector<AggregatedData> out) throws Exception {
        AggregatedData metrics = metricsState.value();
        AggregatedData logs = logsState.value();

        if (metrics != null && logs != null) {
            // Combine both metrics and logs into one object
            AggregatedData combined = new AggregatedData(
                metrics.getJobId(),
                Math.min(metrics.getWindowStart(), logs.getWindowStart()),
                Math.max(metrics.getWindowEnd(), logs.getWindowEnd())
            );
            
            combined.setAvgMetrics(metrics.getAvgMetrics());
            combined.setMetricCounts(metrics.getMetricCounts());
            combined.setLogLevelCounts(logs.getLogLevelCounts());
            combined.setErrorMessages(logs.getErrorMessages());
            combined.setExceptionTypes(logs.getExceptionTypes());

            out.collect(combined);
            
            // Clear states after emission
            metricsState.clear();
            logsState.clear();
        }
    }
}
