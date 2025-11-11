package com.gimral.flink.agentic.vectordb;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Document representing an action that can be taken for a given scenario
 */
public class ActionDocument implements Serializable {
    private static final long serialVersionUID = 1L;

    private String scenario;
    private String description;
    private List<String> actions;
    private String severity;
    private double relevanceScore;

    public ActionDocument() {
    }

    public ActionDocument(String scenario, String description, List<String> actions, String severity) {
        this.scenario = scenario;
        this.description = description;
        this.actions = actions;
        this.severity = severity;
        this.relevanceScore = 0.0;
    }

    public String getScenario() {
        return scenario;
    }

    public void setScenario(String scenario) {
        this.scenario = scenario;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<String> getActions() {
        return actions;
    }

    public void setActions(List<String> actions) {
        this.actions = actions;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public double getRelevanceScore() {
        return relevanceScore;
    }

    public void setRelevanceScore(double relevanceScore) {
        this.relevanceScore = relevanceScore;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ActionDocument that = (ActionDocument) o;
        return Objects.equals(scenario, that.scenario) &&
                Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scenario, description);
    }

    @Override
    public String toString() {
        return "ActionDocument{" +
                "scenario='" + scenario + '\'' +
                ", description='" + description + '\'' +
                ", actions=" + actions +
                ", severity='" + severity + '\'' +
                ", relevanceScore=" + relevanceScore +
                '}';
    }
}
