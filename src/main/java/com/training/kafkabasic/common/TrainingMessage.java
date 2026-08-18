package com.training.kafkabasic.common;

import java.time.Instant;

/**
 * Payload JSON scambiato su Kafka tra i job producer e il job consumer.
 * "source" identifica quale job producer ha generato il messaggio.
 */
public record TrainingMessage(Long id, String source, String content, Instant producedAt) {
}
