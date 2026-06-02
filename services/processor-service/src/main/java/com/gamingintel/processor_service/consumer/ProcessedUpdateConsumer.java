package com.gamingintel.processor_service.consumer;

import com.gamingintel.processor_service.config.KafkaTopics;
import com.gamingintel.processor_service.dto.AiAnalysisMessage;
import com.gamingintel.processor_service.dto.ProcessedSteamUpdateMessage;
import com.gamingintel.processor_service.producer.AiAnalysisProducer;
import com.gamingintel.processor_service.service.AiAnalysisService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ProcessedUpdateConsumer {

        private final AiAnalysisService aiAnalysisService;
        private final AiAnalysisProducer aiAnalysisProducer;
        private final Counter processedUpdatesConsumedCounter;
        private final Counter aiAnalysisSuccessCounter;
        private final Counter aiAnalysisFailureCounter;

        public ProcessedUpdateConsumer(
                        AiAnalysisService aiAnalysisService,
                        AiAnalysisProducer aiAnalysisProducer,
                        MeterRegistry meterRegistry) {
                this.aiAnalysisService = aiAnalysisService;
                this.aiAnalysisProducer = aiAnalysisProducer;

                this.processedUpdatesConsumedCounter = Counter.builder("processed_updates_consumed")
                                .description("Total processed update messages consumed for AI analysis")
                                .register(meterRegistry);

                this.aiAnalysisSuccessCounter = Counter.builder("ai_analysis_success")
                                .description("Total successful AI analyses")
                                .register(meterRegistry);

                this.aiAnalysisFailureCounter = Counter.builder("ai_analysis_failure")
                                .description("Total failed AI analyses")
                                .register(meterRegistry);
        }

        @KafkaListener(topics = KafkaTopics.PROCESSED_UPDATES, groupId = "ai-analysis-service", properties = {
                        "spring.json.value.default.type=com.gamingintel.processor_service.dto.ProcessedSteamUpdateMessage"
        })
        public void consume(ProcessedSteamUpdateMessage message) {
                processedUpdatesConsumedCounter.increment();

                log.info(
                                "Received processed Steam update for AI analysis. gid={}, appId={}, title={}",
                                message.getGid(),
                                message.getAppId(),
                                message.getTitle());

                try {
                        AiAnalysisMessage analysisMessage = aiAnalysisService.analyzeAndSave(message);

                        aiAnalysisProducer.send(analysisMessage);

                        aiAnalysisSuccessCounter.increment();

                        log.info("AI analysis completed and published. gid={}", message.getGid());
                } catch (Exception ex) {
                        aiAnalysisFailureCounter.increment();

                        log.error("Failed AI analysis for gid={}", message.getGid(), ex);
                        throw ex;
                }
        }
}