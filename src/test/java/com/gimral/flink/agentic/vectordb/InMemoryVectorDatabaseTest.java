package com.gimral.flink.agentic.vectordb;

import org.junit.Before;
import org.junit.Test;
import java.util.List;
import static org.junit.Assert.*;

public class InMemoryVectorDatabaseTest {

    private InMemoryVectorDatabase vectorDb;

    @Before
    public void setUp() {
        vectorDb = new InMemoryVectorDatabase();
        vectorDb.initialize();
    }

    @Test
    public void testQuerySimilarScenarios_HighCPU() {
        List<ActionDocument> results = vectorDb.querySimilarScenarios("High CPU usage detected", 3);
        
        assertFalse("Should return results", results.isEmpty());
        assertTrue("Should find CPU related scenario", 
            results.get(0).getScenario().toLowerCase().contains("cpu"));
    }

    @Test
    public void testQuerySimilarScenarios_Memory() {
        List<ActionDocument> results = vectorDb.querySimilarScenarios("Memory usage is very high", 3);
        
        assertFalse("Should return results", results.isEmpty());
        assertTrue("Should find memory related scenario", 
            results.get(0).getScenario().toLowerCase().contains("memory"));
    }

    @Test
    public void testQuerySimilarScenarios_Checkpoint() {
        List<ActionDocument> results = vectorDb.querySimilarScenarios("Checkpoint failures occurring", 3);
        
        assertFalse("Should return results", results.isEmpty());
        assertTrue("Should find checkpoint related scenario", 
            results.get(0).getScenario().toLowerCase().contains("checkpoint"));
    }

    @Test
    public void testQuerySimilarScenarios_NoMatch() {
        List<ActionDocument> results = vectorDb.querySimilarScenarios("xyz random text abc", 3);
        
        assertTrue("Should return empty list for no match", results.isEmpty());
    }

    @Test
    public void testRelevanceScoring() {
        List<ActionDocument> results = vectorDb.querySimilarScenarios("CPU memory usage high", 5);
        
        assertFalse("Should return results", results.isEmpty());
        
        // Verify results are sorted by relevance score
        for (int i = 0; i < results.size() - 1; i++) {
            assertTrue("Results should be sorted by relevance", 
                results.get(i).getRelevanceScore() >= results.get(i + 1).getRelevanceScore());
        }
    }
}
