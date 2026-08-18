package com.training.kafkabasic.producer1;

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
 * Job "producer1": genera N righe random nella tabella di staging
 * (staging_message_1), poi le pubblica sul topic training-topic-1 e le
 * marca come SENT. Le due fasi sono step separati dello stesso job; gli id
 * generati nel primo step vengono passati al secondo tramite la
 * ExecutionContext del job, cosi' il reader del secondo step legge
 * esattamente (e solo) le righe appena create, senza il rischio di "saltare"
 * righe che una lettura per stato PENDING/SENT avrebbe invece introdotto.
 */
@Configuration
public class Producer1JobConfig {

    private static final String GENERATED_IDS_KEY = "producer1.generatedIds";

    @Value("${training.producer.messages-per-run}")
    private int messagesPerRun;

    @Bean
    public Job producer1Job(JobRepository jobRepository, Step producer1GenerateStep, Step producer1PublishStep) {
        return new JobBuilder("producer1Job", jobRepository)
                .start(producer1GenerateStep)
                .next(producer1PublishStep)
                .build();
    }

    @Bean
    public Step producer1GenerateStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                       StagingMessage1Repository repository) {
        return new StepBuilder("producer1GenerateStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    List<StagingMessage1> generated = new ArrayList<>();
                    for (int i = 0; i < messagesPerRun; i++) {
                        generated.add(new StagingMessage1(RandomContentGenerator.randomSentence("p1")));
                    }
                    repository.saveAll(generated);

                    List<Long> ids = generated.stream().map(StagingMessage1::getId).toList();
                    chunkContext.getStepContext().getStepExecution().getJobExecution()
                            .getExecutionContext().put(GENERATED_IDS_KEY, new ArrayList<>(ids));

                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Step producer1PublishStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                      ItemReader<StagingMessage1> producer1Reader,
                                      ItemProcessor<StagingMessage1, TrainingMessage> producer1Processor,
                                      CompositeItemWriter<TrainingMessage> producer1CompositeWriter) {
        return new StepBuilder("producer1PublishStep", jobRepository)
                .<StagingMessage1, TrainingMessage>chunk(10, transactionManager)
                .reader(producer1Reader)
                .processor(producer1Processor)
                .writer(producer1CompositeWriter)
                .build();
    }

    @Bean
    @org.springframework.batch.core.configuration.annotation.StepScope
    public ItemReader<StagingMessage1> producer1Reader(
            StagingMessage1Repository repository,
            @Value("#{jobExecutionContext['" + GENERATED_IDS_KEY + "']}") List<Long> generatedIds) {
        List<StagingMessage1> items = generatedIds == null || generatedIds.isEmpty()
                ? List.of()
                : repository.findAllById(generatedIds);
        return new ListItemReader<>(items);
    }

    @Bean
    public ItemProcessor<StagingMessage1, TrainingMessage> producer1Processor() {
        return staging -> new TrainingMessage(staging.getId(), "producer1", staging.getContent(), Instant.now());
    }

    @Bean
    public KafkaItemWriter<String, TrainingMessage> producer1KafkaItemWriter(
            @Qualifier("producer1KafkaTemplate") KafkaTemplate<String, TrainingMessage> producer1KafkaTemplate) {
        return new KafkaItemWriterBuilder<String, TrainingMessage>()
                .kafkaTemplate(producer1KafkaTemplate)
                .itemKeyMapper(message -> String.valueOf(message.id()))
                .build();
    }

    @Bean
    public CompositeItemWriter<TrainingMessage> producer1CompositeWriter(
            KafkaItemWriter<String, TrainingMessage> producer1KafkaItemWriter,
            StagingMessage1Repository repository) {
        ItemWriter<TrainingMessage> markSentWriter = chunk ->
                repository.markAsSent(chunk.getItems().stream().map(TrainingMessage::id).toList());

        List<ItemWriter<? super TrainingMessage>> delegates = new ArrayList<>();
        delegates.add(producer1KafkaItemWriter);
        delegates.add(markSentWriter);

        CompositeItemWriter<TrainingMessage> writer = new CompositeItemWriter<>();
        writer.setDelegates(delegates);
        return writer;
    }
}
