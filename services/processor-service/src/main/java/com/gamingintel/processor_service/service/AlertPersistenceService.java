package com.gamingintel.processor_service.service;

import com.gamingintel.processor_service.dto.AlertMessage;
import com.gamingintel.processor_service.entity.AlertEntity;
import com.gamingintel.processor_service.repository.AlertRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class AlertPersistenceService {

    private static final Logger log = LoggerFactory.getLogger(AlertPersistenceService.class);

    private final AlertRepository alertRepository;

    public AlertPersistenceService(AlertRepository alertRepository) {
        this.alertRepository = alertRepository;
    }

    public AlertEntity save(AlertMessage alertMessage) {
        log.info(
                "Attempting to save alert to database gid={} alertId={} severity={}",
                alertMessage.getGid(),
                alertMessage.getAlertId(),
                alertMessage.getSeverity());

        if (alertRepository.existsByGid(alertMessage.getGid())) {
            log.info("Alert already exists in database for gid={}", alertMessage.getGid());

            return alertRepository.findByGid(alertMessage.getGid())
                    .orElseThrow(() -> new IllegalStateException(
                            "Alert exists but could not be found for gid: " + alertMessage.getGid()));
        }

        AlertEntity entity = AlertEntity.builder()
                .alertId(alertMessage.getAlertId())
                .gid(alertMessage.getGid())
                .severity(alertMessage.getSeverity())
                .importanceScore(alertMessage.getImportanceScore())
                .sentiment(alertMessage.getSentiment())
                .confidence(alertMessage.getConfidence())
                .updateType(alertMessage.getUpdateType())
                .summary(alertMessage.getSummary())
                .triggeredRules(alertMessage.getTriggeredRules())
                .source(alertMessage.getSource())
                .createdAt(parseCreatedAt(alertMessage.getCreatedAt()))
                .build();

        AlertEntity saved = alertRepository.save(entity);

        log.info(
                "Saved alert to database id={} gid={} alertId={}",
                saved.getId(),
                saved.getGid(),
                saved.getAlertId());

        return saved;
    }

    private Instant parseCreatedAt(String createdAt) {
        if (createdAt == null || createdAt.isBlank()) {
            return Instant.now();
        }

        return Instant.parse(createdAt);
    }
}