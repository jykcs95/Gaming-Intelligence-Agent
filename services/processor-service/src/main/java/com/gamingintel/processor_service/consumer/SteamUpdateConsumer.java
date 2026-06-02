package com.gamingintel.processor_service.consumer;

import com.gamingintel.processor_service.config.KafkaTopics;
import com.gamingintel.processor_service.dto.ProcessedSteamUpdateMessage;
import com.gamingintel.processor_service.dto.SteamUpdateMessage;
import com.gamingintel.processor_service.producer.ProcessedUpdateProducer;
import com.gamingintel.processor_service.service.SteamUpdateProcessingService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Slf4j
@Component
public class SteamUpdateConsumer {

    private final SteamUpdateProcessingService steamUpdateProcessingService;
    private final ProcessedUpdateProducer processedUpdateProducer;
    private final Counter rawUpdatesConsumedCounter;
    private final Counter rawUpdatesProcessedSuccessCounter;
    private final Counter rawUpdatesDuplicateSkippedCounter;
    private final Counter rawUpdatesProcessedFailureCounter;

    public SteamUpdateConsumer(
            SteamUpdateProcessingService steamUpdateProcessingService,
            ProcessedUpdateProducer processedUpdateProducer,
            MeterRegistry meterRegistry) {
        this.steamUpdateProcessingService = steamUpdateProcessingService;
        this.processedUpdateProducer = processedUpdateProducer;

        this.rawUpdatesConsumedCounter = Counter.builder("raw_updates_consumed")
                .description("Total raw update messages consumed")
                .register(meterRegistry);

        this.rawUpdatesProcessedSuccessCounter = Counter.builder("raw_updates_processed_success")
                .description("Total raw update messages processed successfully and published")
                .register(meterRegistry);

        this.rawUpdatesDuplicateSkippedCounter = Counter.builder("raw_updates_duplicate_skipped")
                .description("Total duplicate raw update messages skipped")
                .register(meterRegistry);

        this.rawUpdatesProcessedFailureCounter = Counter.builder("raw_updates_processed_failure")
                .description("Total raw update messages that failed processing")
                .register(meterRegistry);
    }

    @KafkaListener(topics = KafkaTopics.RAW_UPDATES, groupId = "processor-service", containerFactory = "kafkaListenerContainerFactory", properties = {
            "spring.json.value.default.type=com.gamingintel.processor_service.dto.SteamUpdateMessage"
    })
    public void consume(SteamUpdateMessage message) {
        rawUpdatesConsumedCounter.increment();

        log.info("Received Steam update message: gid={}, appId={}, title={}",
                message.getGid(),
                message.getAppId(),
                message.getTitle());

        try {
            Optional<ProcessedSteamUpdateMessage> processedMessage = steamUpdateProcessingService.process(message);

            if (processedMessage.isEmpty()) {
                rawUpdatesDuplicateSkippedCounter.increment();

                log.info("Skipping duplicate Steam update message: gid={}", message.getGid());
                return;
            }

            processedUpdateProducer.send(processedMessage.get());

            rawUpdatesProcessedSuccessCounter.increment();

            log.info("Published processed Steam update message: gid={}", message.getGid());
        } catch (Exception ex) {
            rawUpdatesProcessedFailureCounter.increment();

            throw new IllegalStateException(
                    "Failed to process raw Steam update gid=" + message.getGid(),
                    ex);
        }
    }
}