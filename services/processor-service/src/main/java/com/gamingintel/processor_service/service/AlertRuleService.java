package com.gamingintel.processor_service.service;

import com.gamingintel.processor_service.dto.AiAnalysisMessage;
import com.gamingintel.processor_service.dto.AlertMessage;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class AlertRuleService {

    public AlertMessage evaluate(AiAnalysisMessage message) {
        List<String> triggeredRules = new ArrayList<>();

        Integer importanceScore = message.getImportanceScore();
        String updateType = normalize(message.getUpdateType());
        String sentiment = normalize(message.getSentiment());
        Double confidence = message.getConfidence();

        if (importanceScore != null && importanceScore >= 8) {
            triggeredRules.add("HIGH_IMPORTANCE_SCORE");
        }

        if ("security".equals(updateType)) {
            triggeredRules.add("SECURITY_UPDATE");
        }

        if ("negative".equals(sentiment)
                && confidence != null
                && confidence >= 0.75) {
            triggeredRules.add("NEGATIVE_SENTIMENT_HIGH_CONFIDENCE");
        }

        if ("balance_change".equals(updateType)
                && importanceScore != null
                && importanceScore >= 7) {
            triggeredRules.add("IMPORTANT_BALANCE_CHANGE");
        }

        if (triggeredRules.isEmpty()) {
            return null;
        }

        String severity = calculateSeverity(
                importanceScore,
                updateType,
                sentiment,
                confidence,
                triggeredRules);

        return AlertMessage.builder()
                .alertId(UUID.randomUUID().toString())
                .gid(message.getGid())
                .severity(severity)
                .importanceScore(importanceScore)
                .sentiment(message.getSentiment())
                .confidence(confidence)
                .updateType(message.getUpdateType())
                .summary(message.getSummary())
                .triggeredRules(triggeredRules)
                .createdAt(Instant.now().toString())
                .source("alert-rule-service")
                .build();
    }

    private String calculateSeverity(
            Integer importanceScore,
            String updateType,
            String sentiment,
            Double confidence,
            List<String> triggeredRules) {
        if ("security".equals(updateType)) {
            return "critical";
        }

        if (importanceScore != null && importanceScore >= 9) {
            return "critical";
        }

        if ("negative".equals(sentiment)
                && confidence != null
                && confidence >= 0.85) {
            return "high";
        }

        if (triggeredRules.contains("IMPORTANT_BALANCE_CHANGE")) {
            return "medium";
        }

        return "high";
    }

    private String normalize(String value) {
        return value == null ? "" : value.trim().toLowerCase();
    }
}