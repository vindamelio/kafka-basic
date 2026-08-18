package com.training.kafkabasic.config;

import com.training.kafkabasic.common.TrainingMessage;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Un'unica ProducerFactory condivisa (chiave = id del messaggio come stringa,
 * valore = TrainingMessage serializzato in JSON) e due KafkaTemplate, uno per
 * ciascun topic di training, usati dai rispettivi KafkaItemWriter dei job producer.
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${training.kafka.topic-1}")
    private String topic1;

    @Value("${training.kafka.topic-2}")
    private String topic2;

    @Bean
    public ProducerFactory<String, TrainingMessage> trainingProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        // niente header __TypeId__: messaggi JSON puliti, comodi da leggere nella Kafka UI
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, TrainingMessage> producer1KafkaTemplate(
            ProducerFactory<String, TrainingMessage> trainingProducerFactory) {
        KafkaTemplate<String, TrainingMessage> template = new KafkaTemplate<>(trainingProducerFactory);
        template.setDefaultTopic(topic1);
        return template;
    }

    @Bean
    public KafkaTemplate<String, TrainingMessage> producer2KafkaTemplate(
            ProducerFactory<String, TrainingMessage> trainingProducerFactory) {
        KafkaTemplate<String, TrainingMessage> template = new KafkaTemplate<>(trainingProducerFactory);
        template.setDefaultTopic(topic2);
        return template;
    }
}
