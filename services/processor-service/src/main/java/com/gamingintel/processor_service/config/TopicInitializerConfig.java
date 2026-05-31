package com.gamingintel.processor_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicInitializerConfig {

    private static final int PARTITIONS = 3;
    private static final int REPLICAS = 1;

    @Bean
    public NewTopic rawUpdatesTopic() {
        return TopicBuilder.name(KafkaTopics.RAW_UPDATES)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

    @Bean
    public NewTopic rawUpdatesDlqTopic() {
        return TopicBuilder.name(KafkaTopics.RAW_UPDATES_DLQ)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

    @Bean
    public NewTopic processedUpdatesTopic() {
        return TopicBuilder.name(KafkaTopics.PROCESSED_UPDATES)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

    @Bean
    public NewTopic aiAnalysisTopic() {
        return TopicBuilder.name(KafkaTopics.AI_ANALYSIS)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

    @Bean
    public NewTopic alertsTopic() {
        return TopicBuilder.name(KafkaTopics.ALERTS)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }
}