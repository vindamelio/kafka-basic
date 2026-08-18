package com.training.kafkabasic.common;

import jakarta.persistence.Column;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.MappedSuperclass;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * Campi comuni alle tabelle di staging dei job producer: una riga per messaggio
 * generato, marcata come SENT solo dopo la pubblicazione riuscita su Kafka.
 */
@Getter
@Setter
@NoArgsConstructor
@MappedSuperclass
public abstract class AbstractStagingMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 255)
    private String content;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private MessageStatus status = MessageStatus.PENDING;

    @Column(nullable = false)
    private Instant createdAt = Instant.now();

    protected AbstractStagingMessage(String content) {
        this.content = content;
    }

    public enum MessageStatus {
        PENDING,
        SENT
    }
}
