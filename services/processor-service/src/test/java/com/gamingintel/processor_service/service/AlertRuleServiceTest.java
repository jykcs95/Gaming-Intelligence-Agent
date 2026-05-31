package com.gamingintel.processor_service.service;

import com.gamingintel.processor_service.dto.AiAnalysisMessage;
import com.gamingintel.processor_service.dto.AlertMessage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AlertRuleServiceTest {

    private final AlertRuleService alertRuleService = new AlertRuleService();

    @Test
    void evaluateShouldReturnNullWhenNoRulesMatch() {
        AiAnalysisMessage message = baseMessage()
                .importanceScore(3)
                .updateType("content_update")
                .sentiment("positive")
                .confidence(0.90)
                .build();

        AlertMessage result = alertRuleService.evaluate(message);

        assertThat(result).isNull();
    }

    @Test
    void evaluateShouldTriggerHighImportanceScoreRule() {
        AiAnalysisMessage message = baseMessage()
                .importanceScore(8)
                .updateType("content_update")
                .sentiment("neutral")
                .confidence(0.60)
                .build();

        AlertMessage result = alertRuleService.evaluate(message);

        assertThat(result).isNotNull();
        assertThat(result.getTriggeredRules()).containsExactly("HIGH_IMPORTANCE_SCORE");
        assertThat(result.getSeverity()).isEqualTo("high");
        assertThat(result.getGid()).isEqualTo("test-gid");
        assertThat(result.getSource()).isEqualTo("alert-rule-service");
    }

    @Test
    void evaluateShouldTriggerSecurityUpdateRuleAndSetCriticalSeverity() {
        AiAnalysisMessage message = baseMessage()
                .importanceScore(5)
                .updateType("security")
                .sentiment("neutral")
                .confidence(0.50)
                .build();

        AlertMessage result = alertRuleService.evaluate(message);

        assertThat(result).isNotNull();
        assertThat(result.getTriggeredRules()).contains("SECURITY_UPDATE");
        assertThat(result.getSeverity()).isEqualTo("critical");
    }

    @Test
    void evaluateShouldTriggerNegativeSentimentHighConfidenceRule() {
        AiAnalysisMessage message = baseMessage()
                .importanceScore(4)
                .updateType("content_update")
                .sentiment("negative")
                .confidence(0.75)
                .build();

        AlertMessage result = alertRuleService.evaluate(message);

        assertThat(result).isNotNull();
        assertThat(result.getTriggeredRules()).containsExactly("NEGATIVE_SENTIMENT_HIGH_CONFIDENCE");
        assertThat(result.getSeverity()).isEqualTo("high");
    }

    @Test
    void evaluateShouldTriggerImportantBalanceChangeRuleAndSetMediumSeverity() {
        AiAnalysisMessage message = baseMessage()
                .importanceScore(7)
                .updateType("balance_change")
                .sentiment("neutral")
                .confidence(0.70)
                .build();

        AlertMessage result = alertRuleService.evaluate(message);

        assertThat(result).isNotNull();
        assertThat(result.getTriggeredRules()).containsExactly("IMPORTANT_BALANCE_CHANGE");
        assertThat(result.getSeverity()).isEqualTo("medium");
    }

    @Test
    void evaluateShouldSetCriticalSeverityWhenImportanceScoreIsAtLeastNine() {
        AiAnalysisMessage message = baseMessage()
                .importanceScore(9)
                .updateType("content_update")
                .sentiment("neutral")
                .confidence(0.60)
                .build();

        AlertMessage result = alertRuleService.evaluate(message);

        assertThat(result).isNotNull();
        assertThat(result.getTriggeredRules()).containsExactly("HIGH_IMPORTANCE_SCORE");
        assertThat(result.getSeverity()).isEqualTo("critical");
    }

    @Test
    void evaluateShouldNormalizeCaseAndWhitespace() {
        AiAnalysisMessage message = baseMessage()
                .importanceScore(6)
                .updateType("  SECURITY  ")
                .sentiment("  NEGATIVE  ")
                .confidence(0.90)
                .build();

        AlertMessage result = alertRuleService.evaluate(message);

        assertThat(result).isNotNull();
        assertThat(result.getTriggeredRules())
                .containsExactly("SECURITY_UPDATE", "NEGATIVE_SENTIMENT_HIGH_CONFIDENCE");
        assertThat(result.getSeverity()).isEqualTo("critical");
    }

    private AiAnalysisMessage.AiAnalysisMessageBuilder baseMessage() {
        return AiAnalysisMessage.builder()
                .gid("test-gid")
                .summary("Test summary")
                .keyPoints("Point 1; Point 2")
                .createdAt("2026-05-31T18:00:00Z")
                .source("ai-analysis-service");
    }
}