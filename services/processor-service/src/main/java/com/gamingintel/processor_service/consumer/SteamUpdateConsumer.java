package com.gamingintel.processor_service.consumer;

import com.gamingintel.processor_service.config.KafkaTopics;
import com.gamingintel.processor_service.dto.ProcessedSteamUpdateMessage;
import com.gamingintel.processor_service.dto.SteamUpdateMessage;
import com.gamingintel.processor_service.producer.ProcessedUpdateProducer;
import com.gamingintel.processor_service.service.SteamUpdateProcessingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Slf4j
@Component
public class SteamUpdateConsumer {

    private final SteamUpdateProcessingService steamUpdateProcessingService;
    private final ProcessedUpdateProducer processedUpdateProducer;

    public SteamUpdateConsumer(
            SteamUpdateProcessingService steamUpdateProcessingService,
            ProcessedUpdateProducer processedUpdateProducer) {
        this.steamUpdateProcessingService = steamUpdateProcessingService;
        this.processedUpdateProducer = processedUpdateProducer;
    }

    @KafkaListener(topics = KafkaTopics.RAW_UPDATES, groupId = "processor-service", containerFactory = "kafkaListenerContainerFactory", properties = {
            "spring.json.value.default.type=com.gamingintel.processor_service.dto.SteamUpdateMessage"
    })
    public void consume(SteamUpdateMessage message) {
        log.info("Received Steam update message: gid={}, appId={}, title={}",
                message.getGid(),
                message.getAppId(),
                message.getTitle());

        Optional<ProcessedSteamUpdateMessage> processedMessage = steamUpdateProcessingService.process(message);

        if (processedMessage.isEmpty()) {
            log.info("Skipping duplicate Steam update message: gid={}", message.getGid());
            return;
        }

        processedUpdateProducer.send(processedMessage.get());

        log.info("Published processed Steam update message: gid={}", message.getGid());
    }
}