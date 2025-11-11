package com.gimral.flink.agentic.vectordb;

import java.io.Serializable;
import java.util.List;

/**
 * Interface for vector database operations to store and retrieve operational scenarios and actions
 */
public interface VectorDatabase extends Serializable {
    
    /**
     * Query the vector database for similar scenarios based on the input query
     * @param query The query text describing the current situation
     * @param topK Number of top similar results to return
     * @return List of action documents matching the scenario
     */
    List<ActionDocument> querySimilarScenarios(String query, int topK);
    
    /**
     * Initialize the vector database with predefined scenarios and actions
     */
    void initialize();
}
