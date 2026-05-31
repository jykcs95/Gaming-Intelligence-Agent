package com.gamingintel.processor_service.consumer;

import com.gamingintel.processor_service.config.KafkaTopics;
import com.gamingintel.processor_service.dto.AiAnalysisMessage;
import com.gamingintel.processor_service.dto.AlertMessage;
import com.gamingintel.processor_service.producer.AlertProducer;
import com.gamingintel.processor_service.service.AlertRuleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class AiAnalysisConsumer {

    private static final Logger log = LoggerFactory.getLogger(AiAnalysisConsumer.class);

    private final AlertRuleService alertRuleService;
    private final AlertProducer alertProducer;

    public AiAnalysisConsumer(
            AlertRuleService alertRuleService,
            AlertProducer alertProducer) {
        this.alertRuleService = alertRuleService;
        this.alertProducer = alertProducer;
    }

    @KafkaListener(topics = KafkaTopics.AI_ANALYSIS, groupId = "alert-generation-consumer", properties = {
            "spring.json.value.default.type=com.gamingintel.processor_service.dto.AiAnalysisMessage"
    })
    public void consume(AiAnalysisMessage message) {
        log.info(
                "Received AI analysis for alert evaluation gid={} updateType={} importanceScore={}",
                message.getGid(),
                message.getUpdateType(),
                message.getImportanceScore());

        AlertMessage alertMessage = alertRuleService.evaluate(message);

        if (alertMessage == null) {
            log.info("No alert generated for gid={}", message.getGid());
            return;
        }

        alertProducer.publish(alertMessage);
    }
}