package com.gamingintel.processor_service.service;

import com.gamingintel.processor_service.dto.AlertResponse;
import com.gamingintel.processor_service.entity.AlertEntity;
import com.gamingintel.processor_service.exception.AlertNotFoundException;
import com.gamingintel.processor_service.repository.AlertRepository;
import org.springframework.stereotype.Service;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.util.List;

@Service
public class AlertQueryService {

    private final AlertRepository alertRepository;

    public AlertQueryService(AlertRepository alertRepository) {
        this.alertRepository = alertRepository;
    }

    public List<AlertResponse> getRecentAlerts() {
        return alertRepository.findTop20ByOrderByCreatedAtDesc()
                .stream()
                .map(this::toResponse)
                .toList();
    }

    public AlertResponse getAlertByGid(String gid) {
        return alertRepository.findByGid(gid)
                .map(this::toResponse)
                .orElseThrow(() -> new AlertNotFoundException(gid));
    }

    public List<AlertResponse> getAlertsBySeverity(String severity) {
        return alertRepository.findBySeverityIgnoreCaseOrderByCreatedAtDesc(severity)
                .stream()
                .map(this::toResponse)
                .toList();
    }

    private AlertResponse toResponse(AlertEntity entity) {
        return AlertResponse.builder()
                .alertId(entity.getAlertId())
                .gid(entity.getGid())
                .severity(entity.getSeverity())
                .importanceScore(entity.getImportanceScore())
                .sentiment(entity.getSentiment())
                .confidence(entity.getConfidence())
                .updateType(entity.getUpdateType())
                .summary(entity.getSummary())
                .triggeredRules(entity.getTriggeredRules())
                .source(entity.getSource())
                .createdAt(entity.getCreatedAt())
                .build();
    }

    public List<AlertResponse> getAlerts(String severity, int limit) {
        int safeLimit = Math.clamp(limit, 1, 100);
        Pageable pageable = PageRequest.of(0, safeLimit);

        if (severity != null && !severity.isBlank()) {
            return alertRepository.findBySeverityIgnoreCaseOrderByCreatedAtDesc(severity, pageable)
                    .stream()
                    .map(this::toResponse)
                    .toList();
        }

        return alertRepository.findByOrderByCreatedAtDesc(pageable)
                .stream()
                .map(this::toResponse)
                .toList();
    }
}