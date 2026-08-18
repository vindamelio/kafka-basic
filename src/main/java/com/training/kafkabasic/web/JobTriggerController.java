package com.training.kafkabasic.web;

import com.training.kafkabasic.common.AbstractStagingMessage.MessageStatus;
import com.training.kafkabasic.consumer.ConsumedMessage;
import com.training.kafkabasic.consumer.ConsumedMessageRepository;
import com.training.kafkabasic.producer1.StagingMessage1Repository;
import com.training.kafkabasic.producer2.StagingMessage2Repository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Endpoint pensati per il training: permettono di lanciare i job a comando
 * (senza aspettare i 5 minuti dello scheduler) e di ispezionare rapidamente
 * lo stato delle tabelle di staging e dei messaggi consumati.
 */
@RestController
@RequestMapping("/api")
public class JobTriggerController {

    private final JobLauncher jobLauncher;
    private final Job producer1Job;
    private final Job producer2Job;
    private final Job consumerJob;
    private final StagingMessage1Repository stagingMessage1Repository;
    private final StagingMessage2Repository stagingMessage2Repository;
    private final ConsumedMessageRepository consumedMessageRepository;

    public JobTriggerController(JobLauncher jobLauncher,
                                 @Qualifier("producer1Job") Job producer1Job,
                                 @Qualifier("producer2Job") Job producer2Job,
                                 @Qualifier("consumerJob") Job consumerJob,
                                 StagingMessage1Repository stagingMessage1Repository,
                                 StagingMessage2Repository stagingMessage2Repository,
                                 ConsumedMessageRepository consumedMessageRepository) {
        this.jobLauncher = jobLauncher;
        this.producer1Job = producer1Job;
        this.producer2Job = producer2Job;
        this.consumerJob = consumerJob;
        this.stagingMessage1Repository = stagingMessage1Repository;
        this.stagingMessage2Repository = stagingMessage2Repository;
        this.consumedMessageRepository = consumedMessageRepository;
    }

    @PostMapping("/jobs/{name}")
    public ResponseEntity<Map<String, String>> runJob(@PathVariable String name) throws Exception {
        Job job = switch (name) {
            case "producer1" -> producer1Job;
            case "producer2" -> producer2Job;
            case "consumer" -> consumerJob;
            default -> null;
        };
        if (job == null) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "job sconosciuto: usa producer1, producer2 o consumer"));
        }

        JobExecution execution = jobLauncher.run(job, new JobParametersBuilder()
                .addLong("run.timestamp", System.currentTimeMillis())
                .toJobParameters());

        return ResponseEntity.ok(Map.of(
                "job", job.getName(),
                "status", execution.getStatus().toString()
        ));
    }

    @GetMapping("/messages/consumed")
    public List<ConsumedMessage> recentConsumedMessages() {
        return consumedMessageRepository.findTop20ByOrderByConsumedAtDesc();
    }

    @GetMapping("/messages/staging")
    public Map<String, Object> stagingStatus() {
        return Map.of(
                "staging1Pending", stagingMessage1Repository.findByStatus(MessageStatus.PENDING, Pageable.unpaged()).size(),
                "staging1Sent", stagingMessage1Repository.findByStatus(MessageStatus.SENT, Pageable.unpaged()).size(),
                "staging2Pending", stagingMessage2Repository.findByStatus(MessageStatus.PENDING, Pageable.unpaged()).size(),
                "staging2Sent", stagingMessage2Repository.findByStatus(MessageStatus.SENT, Pageable.unpaged()).size()
        );
    }
}
