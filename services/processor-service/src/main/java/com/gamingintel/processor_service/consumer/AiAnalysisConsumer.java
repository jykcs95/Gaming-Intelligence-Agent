package com.gamingintel.processor_service.consumer;

import com.gamingintel.processor_service.config.KafkaTopics;
import com.gamingintel.processor_service.dto.AiAnalysisMessage;
import com.gamingintel.processor_service.dto.AlertMessage;
import com.gamingintel.processor_service.producer.AlertProducer;
import com.gamingintel.processor_service.service.AlertPersistenceService;
import com.gamingintel.processor_service.service.AlertRuleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class AiAnalysisConsumer {

        private static final Logger log = LoggerFactory.getLogger(AiAnalysisConsumer.class);

        private final AlertRuleService alertRuleService;
        private final AlertPersistenceService alertPersistenceService;
        private final AlertProducer alertProducer;

        public AiAnalysisConsumer(
                        AlertRuleService alertRuleService,
                        AlertPersistenceService alertPersistenceService,
                        AlertProducer alertProducer) {
                this.alertRuleService = alertRuleService;
                this.alertPersistenceService = alertPersistenceService;
                this.alertProducer = alertProducer;
        }

        @KafkaListener(topics = KafkaTopics.AI_ANALYSIS, groupId = "alert-generation-consumer", properties = {
                        "spring.json.value.default.type=com.gamingintel.processor_service.dto.AiAnalysisMessage"
        })
        public void consume(AiAnalysisMessage message) {
                log.info(
                                "AI_ANALYSIS_CONSUMER_RECEIVED gid={} updateType={} importanceScore={} sentiment={} confidence={}",
                                message.getGid(),
                                message.getUpdateType(),
                                message.getImportanceScore(),
                                message.getSentiment(),
                                message.getConfidence());

                AlertMessage alertMessage = alertRuleService.evaluate(message);

                if (alertMessage == null) {
                        log.info(
                                        "ALERT_RULE_RESULT_NO_ALERT gid={} updateType={} importanceScore={} sentiment={} confidence={}",
                                        message.getGid(),
                                        message.getUpdateType(),
                                        message.getImportanceScore(),
                                        message.getSentiment(),
                                        message.getConfidence());
                        return;
                }

                log.info(
                                "ALERT_RULE_RESULT_CREATED gid={} alertId={} severity={} rules={}",
                                alertMessage.getGid(),
                                alertMessage.getAlertId(),
                                alertMessage.getSeverity(),
                                alertMessage.getTriggeredRules());

                alertPersistenceService.save(alertMessage);

                log.info(
                                "ALERT_PERSISTENCE_CALL_COMPLETED gid={} alertId={}",
                                alertMessage.getGid(),
                                alertMessage.getAlertId());

                alertProducer.publish(alertMessage);
        }
}