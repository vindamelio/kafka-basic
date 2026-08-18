package com.training.kafkabasic.producer2;

import com.training.kafkabasic.common.RandomContentGenerator;
import com.training.kafkabasic.common.TrainingMessage;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.batch.item.kafka.builder.KafkaItemWriterBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Job "producer2": stessa logica di {@link com.training.kafkabasic.producer1.Producer1JobConfig},
 * ma su una tabella di staging e un topic Kafka distinti — utile per vedere
 * nella Kafka UI due producer indipendenti che alimentano lo stesso consumer.
 */
@Configuration
public class Producer2JobConfig {

    private static final String GENERATED_IDS_KEY = "producer2.generatedIds";

    @Value("${training.producer.messages-per-run}")
    private int messagesPerRun;

    @Bean
    public Job producer2Job(JobRepository jobRepository, Step producer2GenerateStep, Step producer2PublishStep) {
        return new JobBuilder("producer2Job", jobRepository)
                .start(producer2GenerateStep)
                .next(producer2PublishStep)
                .build();
    }

    @Bean
    public Step producer2GenerateStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                       StagingMessage2Repository repository) {
        return new StepBuilder("producer2GenerateStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    List<StagingMessage2> generated = new ArrayList<>();
                    for (int i = 0; i < messagesPerRun; i++) {
                        generated.add(new StagingMessage2(RandomContentGenerator.randomSentence("p2")));
                    }
                    repository.saveAll(generated);

                    List<Long> ids = generated.stream().map(StagingMessage2::getId).toList();
                    chunkContext.getStepContext().getStepExecution().getJobExecution()
                            .getExecutionContext().put(GENERATED_IDS_KEY, new ArrayList<>(ids));

                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Step producer2PublishStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                      ItemReader<StagingMessage2> producer2Reader,
                                      ItemProcessor<StagingMessage2, TrainingMessage> producer2Processor,
                                      CompositeItemWriter<TrainingMessage> producer2CompositeWriter) {
        return new StepBuilder("producer2PublishStep", jobRepository)
                .<StagingMessage2, TrainingMessage>chunk(10, transactionManager)
                .reader(producer2Reader)
                .processor(producer2Processor)
                .writer(producer2CompositeWriter)
                .build();
    }

    @Bean
    @org.springframework.batch.core.configuration.annotation.StepScope
    public ItemReader<StagingMessage2> producer2Reader(
            StagingMessage2Repository repository,
            @Value("#{jobExecutionContext['" + GENERATED_IDS_KEY + "']}") List<Long> generatedIds) {
        List<StagingMessage2> items = generatedIds == null || generatedIds.isEmpty()
                ? List.of()
                : repository.findAllById(generatedIds);
        return new ListItemReader<>(items);
    }

    @Bean
    public ItemProcessor<StagingMessage2, TrainingMessage> producer2Processor() {
        return staging -> new TrainingMessage(staging.getId(), "producer2", staging.getContent(), Instant.now());
    }

    @Bean
    public KafkaItemWriter<String, TrainingMessage> producer2KafkaItemWriter(
            @Qualifier("producer2KafkaTemplate") KafkaTemplate<String, TrainingMessage> producer2KafkaTemplate) {
        return new KafkaItemWriterBuilder<String, TrainingMessage>()
                .kafkaTemplate(producer2KafkaTemplate)
                .itemKeyMapper(message -> String.valueOf(message.id()))
                .build();
    }

    @Bean
    public CompositeItemWriter<TrainingMessage> producer2CompositeWriter(
            KafkaItemWriter<String, TrainingMessage> producer2KafkaItemWriter,
            StagingMessage2Repository repository) {
        ItemWriter<TrainingMessage> markSentWriter = chunk ->
                repository.markAsSent(chunk.getItems().stream().map(TrainingMessage::id).toList());

        List<ItemWriter<? super TrainingMessage>> delegates = new ArrayList<>();
        delegates.add(producer2KafkaItemWriter);
        delegates.add(markSentWriter);

        CompositeItemWriter<TrainingMessage> writer = new CompositeItemWriter<>();
        writer.setDelegates(delegates);
        return writer;
    }
}
