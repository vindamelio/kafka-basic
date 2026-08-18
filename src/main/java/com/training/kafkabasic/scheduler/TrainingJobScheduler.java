package com.training.kafkabasic.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Lancia periodicamente i tre job di training. Ogni esecuzione riceve un
 * JobParameters con un timestamp, obbligatorio per far accettare a Spring
 * Batch la rieesecuzione dello stesso job più volte.
 */
@Component
public class TrainingJobScheduler {

    private static final Logger log = LoggerFactory.getLogger(TrainingJobScheduler.class);

    private final JobLauncher jobLauncher;
    private final Job producer1Job;
    private final Job producer2Job;
    private final Job consumerJob;

    public TrainingJobScheduler(JobLauncher jobLauncher,
                                 @Qualifier("producer1Job") Job producer1Job,
                                 @Qualifier("producer2Job") Job producer2Job,
                                 @Qualifier("consumerJob") Job consumerJob) {
        this.jobLauncher = jobLauncher;
        this.producer1Job = producer1Job;
        this.producer2Job = producer2Job;
        this.consumerJob = consumerJob;
    }

    @Scheduled(cron = "${training.schedule.cron}")
    public void runProducer1() {
        launch(producer1Job, "producer1Job");
    }

    @Scheduled(cron = "${training.schedule.cron}")
    public void runProducer2() {
        launch(producer2Job, "producer2Job");
    }

    @Scheduled(cron = "${training.schedule.cron}")
    public void runConsumer() {
        launch(consumerJob, "consumerJob");
    }

    private void launch(Job job, String jobName) {
        try {
            jobLauncher.run(job, new JobParametersBuilder()
                    .addLong("run.timestamp", System.currentTimeMillis())
                    .toJobParameters());
        } catch (Exception e) {
            log.error("Esecuzione del job {} fallita", jobName, e);
        }
    }
}
