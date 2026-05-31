package com.gamingintel.processor_service.config;

public final class KafkaTopics {

    private KafkaTopics() {
    }

    public static final String RAW_UPDATES = "raw_updates";
    public static final String RAW_UPDATES_DLQ = "raw_updates_dlq";
    public static final String PROCESSED_UPDATES = "processed_updates";
    public static final String AI_ANALYSIS = "ai_analysis";
    public static final String ALERTS = "alerts";
}