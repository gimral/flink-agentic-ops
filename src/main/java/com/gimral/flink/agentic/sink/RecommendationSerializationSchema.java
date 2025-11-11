package com.gimral.flink.agentic.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gimral.flink.agentic.model.Recommendation;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Serialization schema for Recommendation to JSON
 */
public class RecommendationSerializationSchema implements SerializationSchema<Recommendation> {
    private static final long serialVersionUID = 1L;
    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(Recommendation element) {
        try {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }
            return objectMapper.writeValueAsBytes(element);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize recommendation", e);
        }
    }
}
