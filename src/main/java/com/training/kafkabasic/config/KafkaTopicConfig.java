package com.training.kafkabasic.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Dichiara i topic usati dal training. Spring Kafka li crea automaticamente
 * all'avvio tramite il KafkaAdmin, se non esistono già sul broker.
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${training.kafka.topic-1}")
    private String topic1;

    @Value("${training.kafka.topic-2}")
    private String topic2;

    @Value("${training.kafka.partitions}")
    private int partitions;

    @Bean
    public NewTopic trainingTopic1() {
        return TopicBuilder.name(topic1)
                .partitions(partitions)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic trainingTopic2() {
        return TopicBuilder.name(topic2)
                .partitions(partitions)
                .replicas(1)
                .build();
    }
}
