package com.gamingintel.processor_service.service;

import com.gamingintel.processor_service.dto.search.SemanticAlertSearchResponse;
import com.gamingintel.processor_service.entity.AlertEntity;
import com.gamingintel.processor_service.repository.AlertRepository;
import com.gamingintel.processor_service.repository.search.SemanticSearchRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class SemanticSearchService {

    private final AlertRepository alertRepository;
    private final OllamaEmbeddingClient ollamaEmbeddingClient;
    private final SemanticSearchRepository semanticSearchRepository;

    public List<SemanticAlertSearchResponse> searchAlerts(String query, int limit) {
        int safeLimit = Math.clamp(limit, 1, 25);
        List<Double> queryEmbedding = ollamaEmbeddingClient.embed(query);

        return semanticSearchRepository.searchAlerts(queryEmbedding, safeLimit);
    }

    public int rebuildAlertEmbeddings() {
        List<AlertEntity> alerts = alertRepository.findAll();

        semanticSearchRepository.deleteAlertEmbeddings();

        int count = 0;

        for (AlertEntity alert : alerts) {
            String sourceText = toSearchText(alert);
            List<Double> embedding = ollamaEmbeddingClient.embed(sourceText);

            semanticSearchRepository.insertAlertEmbedding(
                    alert.getGid(),
                    sourceText,
                    embedding);

            count++;
        }

        return count;
    }

    private String toSearchText(AlertEntity alert) {
        return String.join(
                "\n",
                nullToEmpty(alert.getGameName()),
                nullToEmpty(alert.getSeverity()),
                nullToEmpty(alert.getUpdateType()),
                nullToEmpty(alert.getSummary()),
                nullToEmpty(alert.getSource()));
    }

    private String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}