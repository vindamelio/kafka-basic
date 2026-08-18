package com.training.kafkabasic.consumer;

import com.training.kafkabasic.common.TrainingMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.kafka.KafkaItemReader;
import org.springframework.batch.item.kafka.builder.KafkaItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Properties;
import java.util.stream.IntStream;

/**
 * Job "consumer": uno step per topic, ciascuno con un KafkaItemReader che ad
 * ogni esecuzione legge tutti i messaggi correntemente disponibili sulle
 * partizioni assegnate e poi si ferma (comportamento "limitato" adatto a un
 * job batch schedulato, a differenza di un @KafkaListener sempre attivo).
 */
@Configuration
public class ConsumerJobConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${training.kafka.topic-1}")
    private String topic1;

    @Value("${training.kafka.topic-2}")
    private String topic2;

    @Value("${training.kafka.partitions}")
    private int partitions;

    @Bean
    public Job consumerJob(JobRepository jobRepository, Step consumeTopic1Step, Step consumeTopic2Step) {
        return new JobBuilder("consumerJob", jobRepository)
                .start(consumeTopic1Step)
                .next(consumeTopic2Step)
                .build();
    }

    @Bean
    public Step consumeTopic1Step(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                   ConsumedMessageRepository repository) {
        KafkaItemReader<String, TrainingMessage> reader = kafkaItemReader(topic1);
        ItemProcessor<TrainingMessage, ConsumedMessage> processor = toConsumedMessage(topic1);
        ItemWriter<ConsumedMessage> writer = chunk -> {
            for (ConsumedMessage message : chunk) {
                repository.save(message);
            }
        };

        return new StepBuilder("consumeTopic1Step", jobRepository)
                .<TrainingMessage, ConsumedMessage>chunk(10, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public Step consumeTopic2Step(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                   ConsumedMessageRepository repository) {
        KafkaItemReader<String, TrainingMessage> reader = kafkaItemReader(topic2);
        ItemProcessor<TrainingMessage, ConsumedMessage> processor = toConsumedMessage(topic2);
        ItemWriter<ConsumedMessage> writer = chunk -> {
            for (ConsumedMessage message : chunk) {
                repository.save(message);
            }
        };

        return new StepBuilder("consumeTopic2Step", jobRepository)
                .<TrainingMessage, ConsumedMessage>chunk(10, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    private KafkaItemReader<String, TrainingMessage> kafkaItemReader(String topic) {
        Integer[] partitionIds = IntStream.range(0, partitions).boxed().toArray(Integer[]::new);
        return new KafkaItemReaderBuilder<String, TrainingMessage>()
                .name(topic + "-reader")
                .topic(topic)
                .partitions(partitionIds)
                .consumerProperties(consumerProperties())
                .saveState(true)
                .build();
    }

    private Properties consumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, TrainingMessage.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.training.kafkabasic.common");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        return props;
    }

    private ItemProcessor<TrainingMessage, ConsumedMessage> toConsumedMessage(String topic) {
        return message -> new ConsumedMessage(topic, message.source(), message.id(), message.content());
    }
}
