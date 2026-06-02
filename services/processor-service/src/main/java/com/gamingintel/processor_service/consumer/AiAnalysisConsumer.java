package com.gamingintel.processor_service.consumer;

import com.gamingintel.processor_service.config.KafkaTopics;
import com.gamingintel.processor_service.dto.AiAnalysisMessage;
import com.gamingintel.processor_service.dto.AlertMessage;
import com.gamingintel.processor_service.producer.AlertProducer;
import com.gamingintel.processor_service.service.AlertPersistenceService;
import com.gamingintel.processor_service.service.AlertRuleService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
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

        private final Counter aiAnalysisMessagesConsumedCounter;
        private final Counter alertsCreatedCounter;
        private final Counter alertsSkippedCounter;
        private final Counter alertsPublishedCounter;
        private final Counter alertGenerationFailureCounter;

        public AiAnalysisConsumer(
                        AlertRuleService alertRuleService,
                        AlertPersistenceService alertPersistenceService,
                        AlertProducer alertProducer,
                        MeterRegistry meterRegistry) {
                this.aiAnalysisMessagesConsumedCounter = Counter.builder("ai_analysis_messages_consumed")
                                .description("Total AI analysis messages consumed for alert generation")
                                .register(meterRegistry);

                this.alertsCreatedCounter = Counter.builder("alerts_created")
                                .description("Total alerts created by alert rules")
                                .register(meterRegistry);

                this.alertsSkippedCounter = Counter.builder("alerts_skipped")
                                .description("Total AI analysis messages that did not trigger alerts")
                                .register(meterRegistry);

                this.alertsPublishedCounter = Counter.builder("alerts_published")
                                .description("Total alerts published to Kafka")
                                .register(meterRegistry);

                this.alertGenerationFailureCounter = Counter.builder("alert_generation_failure")
                                .description("Total alert generation failures")
                                .register(meterRegistry);
                this.alertRuleService = alertRuleService;
                this.alertPersistenceService = alertPersistenceService;
                this.alertProducer = alertProducer;
        }

        @KafkaListener(topics = KafkaTopics.AI_ANALYSIS, groupId = "alert-generation-consumer", properties = {
                        "spring.json.value.default.type=com.gamingintel.processor_service.dto.AiAnalysisMessage"
        })
        public void consume(AiAnalysisMessage message) {
                aiAnalysisMessagesConsumedCounter.increment();

                log.info("Received AI analysis for alert evaluation. gid={}", message.getGid());

                try {
                        AlertMessage alertMessage = alertRuleService.evaluate(message);

                        if (alertMessage == null) {
                                alertsSkippedCounter.increment();
                                log.info("No alert generated for gid={}", message.getGid());
                                return;
                        }

                        alertsCreatedCounter.increment();

                        alertPersistenceService.save(alertMessage);

                        alertProducer.publish(alertMessage);

                        alertsPublishedCounter.increment();

                        log.info(
                                        "Alert generated, persisted, and published. gid={}, severity={}",
                                        alertMessage.getGid(),
                                        alertMessage.getSeverity());
                } catch (Exception ex) {
                        alertGenerationFailureCounter.increment();

                        throw new IllegalStateException(
                                        "Failed alert generation for gid=" + message.getGid(),
                                        ex);
                }
        }
}