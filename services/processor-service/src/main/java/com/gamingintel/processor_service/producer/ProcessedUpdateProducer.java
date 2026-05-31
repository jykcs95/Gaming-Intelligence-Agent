package com.gamingintel.processor_service.producer;

import com.gamingintel.processor_service.config.KafkaTopics;
import com.gamingintel.processor_service.dto.ProcessedSteamUpdateMessage;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class ProcessedUpdateProducer {

    private final KafkaTemplate<String, ProcessedSteamUpdateMessage> kafkaTemplate;

    public ProcessedUpdateProducer(
            KafkaTemplate<String, ProcessedSteamUpdateMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(ProcessedSteamUpdateMessage message) {
        kafkaTemplate.send(
                KafkaTopics.PROCESSED_UPDATES,
                message.getGid(),
                message);
    }
}