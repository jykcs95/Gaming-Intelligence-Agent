package com.gamingintel.processor_service.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gamingintel.processor_service.dto.AiAnalysisMessage;
import com.gamingintel.processor_service.dto.ProcessedSteamUpdateMessage;
import com.gamingintel.processor_service.entity.AiAnalysisEntity;
import com.gamingintel.processor_service.repository.AiAnalysisRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Service
public class AiAnalysisService {

    private static final String SOURCE = "ollama";

    private final OllamaClientService ollamaClientService;
    private final AiAnalysisRepository aiAnalysisRepository;
    private final ObjectMapper objectMapper;

    public AiAnalysisService(
            OllamaClientService ollamaClientService,
            AiAnalysisRepository aiAnalysisRepository,
            ObjectMapper objectMapper) {
        this.ollamaClientService = ollamaClientService;
        this.aiAnalysisRepository = aiAnalysisRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public AiAnalysisMessage analyzeAndSave(ProcessedSteamUpdateMessage message) {
        log.info("Starting AI analysis. gid={}", message.getGid());

        String analysisJson = ollamaClientService.analyzeSteamUpdate(message);

        log.info("Ollama analysis response for gid={}: {}", message.getGid(), analysisJson);

        JsonNode analysis = parseAnalysis(analysisJson);

        Instant createdAt = Instant.now();

        AiAnalysisEntity entity = AiAnalysisEntity.builder()
                .gid(message.getGid())
                .summary(getText(analysis, "summary", "No summary generated"))
                .sentiment(getText(analysis, "sentiment", "neutral"))
                .confidence(getDouble(analysis, "confidence", 0.5))
                .importanceScore(getInteger(analysis, "importance_score", 1))
                .updateType(getText(analysis, "update_type", "unknown"))
                .keyPoints(toJsonString(analysis.get("key_points")))
                .createdAt(createdAt)
                .build();

        AiAnalysisEntity saved = aiAnalysisRepository.save(entity);

        log.info("Saved AI analysis. gid={}, id={}", saved.getGid(), saved.getId());

        return AiAnalysisMessage.builder()
                .gid(saved.getGid())
                .summary(saved.getSummary())
                .sentiment(saved.getSentiment())
                .confidence(saved.getConfidence())
                .importanceScore(saved.getImportanceScore())
                .updateType(saved.getUpdateType())
                .keyPoints(saved.getKeyPoints())
                .createdAt(saved.getCreatedAt().toString())
                .source(SOURCE)
                .build();
    }

    private JsonNode parseAnalysis(String analysisJson) {
        try {
            return objectMapper.readTree(analysisJson);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to parse AI analysis JSON: " + analysisJson, ex);
        }
    }

    private String getText(JsonNode node, String fieldName, String defaultValue) {
        JsonNode value = node.get(fieldName);

        if (value == null || value.isNull()) {
            return defaultValue;
        }

        return value.asText(defaultValue);
    }

    private Double getDouble(JsonNode node, String fieldName, Double defaultValue) {
        JsonNode value = node.get(fieldName);

        if (value == null || value.isNull()) {
            return defaultValue;
        }

        double result = value.asDouble(defaultValue);

        if ("confidence".equals(fieldName)) {
            return Math.clamp(result, 0, 1);
        }

        return result;
    }

    private Integer getInteger(JsonNode node, String fieldName, Integer defaultValue) {
        JsonNode value = node.get(fieldName);

        if (value == null || value.isNull()) {
            return defaultValue;
        }

        int result = value.asInt(defaultValue);

        if ("importance_score".equals(fieldName)) {
            return Math.clamp(result, 0, 10);
        }

        return result;
    }

    private String toJsonString(JsonNode node) {
        try {
            if (node == null || node.isNull()) {
                return "[]";
            }

            return objectMapper.writeValueAsString(node);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to serialize key_points", ex);
        }
    }
}