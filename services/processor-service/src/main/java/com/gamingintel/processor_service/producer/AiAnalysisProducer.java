package com.gamingintel.processor_service.producer;

import com.gamingintel.processor_service.config.KafkaTopics;
import com.gamingintel.processor_service.dto.AiAnalysisMessage;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class AiAnalysisProducer {

    private final KafkaTemplate<String, AiAnalysisMessage> kafkaTemplate;

    public AiAnalysisProducer(KafkaTemplate<String, AiAnalysisMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(AiAnalysisMessage message) {
        kafkaTemplate.send(
                KafkaTopics.AI_ANALYSIS,
                message.getGid(),
                message);
    }
}