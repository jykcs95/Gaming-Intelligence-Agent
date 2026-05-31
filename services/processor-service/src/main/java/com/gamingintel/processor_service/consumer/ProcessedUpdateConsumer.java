package com.gamingintel.processor_service.consumer;

import com.gamingintel.processor_service.config.KafkaTopics;
import com.gamingintel.processor_service.dto.AiAnalysisMessage;
import com.gamingintel.processor_service.dto.ProcessedSteamUpdateMessage;
import com.gamingintel.processor_service.producer.AiAnalysisProducer;
import com.gamingintel.processor_service.service.AiAnalysisService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ProcessedUpdateConsumer {

    private final AiAnalysisService aiAnalysisService;
    private final AiAnalysisProducer aiAnalysisProducer;

    public ProcessedUpdateConsumer(
            AiAnalysisService aiAnalysisService,
            AiAnalysisProducer aiAnalysisProducer) {
        this.aiAnalysisService = aiAnalysisService;
        this.aiAnalysisProducer = aiAnalysisProducer;
    }

    @KafkaListener(topics = KafkaTopics.PROCESSED_UPDATES, groupId = "ai-analysis-service", properties = {
            "spring.json.value.default.type=com.gamingintel.processor_service.dto.ProcessedSteamUpdateMessage"
    })
    public void consume(ProcessedSteamUpdateMessage message) {
        log.info(
                "Received processed Steam update for AI analysis. gid={}, appId={}, title={}",
                message.getGid(),
                message.getAppId(),
                message.getTitle());

        try {
            AiAnalysisMessage analysisMessage = aiAnalysisService.analyzeAndSave(message);

            aiAnalysisProducer.send(analysisMessage);

            log.info("AI analysis completed and published. gid={}", message.getGid());
        } catch (Exception ex) {
            log.error("Failed AI analysis for gid={}", message.getGid(), ex);
            throw ex;
        }
    }
}