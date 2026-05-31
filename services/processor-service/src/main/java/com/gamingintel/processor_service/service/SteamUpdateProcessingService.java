package com.gamingintel.processor_service.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gamingintel.processor_service.dto.ProcessedSteamUpdateMessage;
import com.gamingintel.processor_service.dto.SteamUpdateMessage;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;

@Service
public class SteamUpdateProcessingService {

    private static final String SOURCE = "steam";

    private final IdempotentConsumerService idempotentConsumerService;
    private final SteamEventService steamEventService;
    private final ObjectMapper objectMapper;

    public SteamUpdateProcessingService(
            IdempotentConsumerService idempotentConsumerService,
            SteamEventService steamEventService,
            ObjectMapper objectMapper) {
        this.idempotentConsumerService = idempotentConsumerService;
        this.steamEventService = steamEventService;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Optional<ProcessedSteamUpdateMessage> process(
            SteamUpdateMessage message) {
        validate(message);

        if (idempotentConsumerService.alreadyProcessed(message.getGid())) {
            return Optional.empty();
        }

        Instant eventTime = toEventTime(message.getDate());
        String rawPayload = toJson(message);

        steamEventService.saveEvent(
                message.getGid(),
                message.getAppId(),
                message.getTitle(),
                eventTime,
                rawPayload);

        idempotentConsumerService.markProcessed(message.getGid());

        ProcessedSteamUpdateMessage processedMessage = ProcessedSteamUpdateMessage.builder()
                .gid(message.getGid())
                .appId(message.getAppId())
                .title(message.getTitle())
                .url(message.getUrl())
                .author(message.getAuthor())
                .contents(message.getContents())
                .eventTime(eventTime.toString())
                .processedAt(Instant.now().toString())
                .source(SOURCE)
                .build();

        return Optional.of(processedMessage);
    }

    private void validate(SteamUpdateMessage message) {
        if (message == null) {
            throw new IllegalArgumentException("Steam update message cannot be null");
        }

        if (message.getGid() == null || message.getGid().isBlank()) {
            throw new IllegalArgumentException("Steam update message is missing gid");
        }

        if (message.getAppId() == null) {
            throw new IllegalArgumentException("Steam update message is missing app_id");
        }
    }

    private Instant toEventTime(Long steamDate) {
        if (steamDate == null) {
            return Instant.now();
        }

        return Instant.ofEpochSecond(steamDate);
    }

    private String toJson(SteamUpdateMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException(
                    "Failed to serialize raw Steam update payload",
                    ex);
        }
    }
}