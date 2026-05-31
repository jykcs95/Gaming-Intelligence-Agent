package com.gamingintel.processor_service.producer;

import com.gamingintel.processor_service.config.KafkaTopics;
import com.gamingintel.processor_service.dto.AlertMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class AlertProducer {

    private static final Logger log = LoggerFactory.getLogger(AlertProducer.class);

    private final KafkaTemplate<String, AlertMessage> kafkaTemplate;

    public AlertProducer(KafkaTemplate<String, AlertMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(AlertMessage alertMessage) {
        kafkaTemplate.send(
                KafkaTopics.ALERTS,
                alertMessage.getGid(),
                alertMessage);

        log.info(
                "Published alert gid={} severity={} rules={}",
                alertMessage.getGid(),
                alertMessage.getSeverity(),
                alertMessage.getTriggeredRules());
    }
}