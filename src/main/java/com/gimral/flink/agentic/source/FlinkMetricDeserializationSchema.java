package com.gimral.flink.agentic.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gimral.flink.agentic.model.FlinkMetric;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Deserialization schema for FlinkMetric from JSON
 */
public class FlinkMetricDeserializationSchema implements DeserializationSchema<FlinkMetric> {
    private static final long serialVersionUID = 1L;
    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        objectMapper = new ObjectMapper();
    }

    @Override
    public FlinkMetric deserialize(byte[] message) throws IOException {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper.readValue(message, FlinkMetric.class);
    }

    @Override
    public boolean isEndOfStream(FlinkMetric nextElement) {
        return false;
    }

    @Override
    public TypeInformation<FlinkMetric> getProducedType() {
        return TypeInformation.of(FlinkMetric.class);
    }
}
