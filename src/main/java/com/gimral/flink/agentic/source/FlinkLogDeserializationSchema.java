package com.gimral.flink.agentic.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gimral.flink.agentic.model.FlinkLog;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Deserialization schema for FlinkLog from JSON
 */
public class FlinkLogDeserializationSchema implements DeserializationSchema<FlinkLog> {
    private static final long serialVersionUID = 1L;
    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        objectMapper = new ObjectMapper();
    }

    @Override
    public FlinkLog deserialize(byte[] message) throws IOException {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper.readValue(message, FlinkLog.class);
    }

    @Override
    public boolean isEndOfStream(FlinkLog nextElement) {
        return false;
    }

    @Override
    public TypeInformation<FlinkLog> getProducedType() {
        return TypeInformation.of(FlinkLog.class);
    }
}
