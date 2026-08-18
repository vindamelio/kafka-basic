package com.training.kafkabasic.consumer;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * Riga persistita dal job consumer per ogni messaggio Kafka letto, a
 * prescindere da quale dei due producer lo abbia originato.
 */
@Getter
@Setter
@Entity
@Table(name = "consumed_message")
@NoArgsConstructor
public class ConsumedMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 100)
    private String sourceTopic;

    @Column(nullable = false, length = 50)
    private String producerSource;

    @Column(nullable = false)
    private Long originalMessageId;

    @Column(nullable = false, length = 255)
    private String content;

    @Column(nullable = false)
    private Instant consumedAt = Instant.now();

    public ConsumedMessage(String sourceTopic, String producerSource, Long originalMessageId, String content) {
        this.sourceTopic = sourceTopic;
        this.producerSource = producerSource;
        this.originalMessageId = originalMessageId;
        this.content = content;
    }
}
