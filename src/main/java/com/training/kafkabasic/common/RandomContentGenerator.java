package com.training.kafkabasic.common;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Genera contenuti testuali casuali per i messaggi di training: nessuna logica
 * di dominio, solo stringhe leggibili utili a distinguere i messaggi nei log
 * e nella Kafka UI.
 */
public final class RandomContentGenerator {

    private static final List<String> WORDS = List.of(
            "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
            "kafka", "batch", "stream", "topic", "partition", "offset", "broker", "consumer"
    );

    private RandomContentGenerator() {
    }

    public static String randomSentence(String prefix) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int words = random.nextInt(3, 6);
        StringBuilder sb = new StringBuilder(prefix).append('-');
        for (int i = 0; i < words; i++) {
            sb.append(WORDS.get(random.nextInt(WORDS.size())));
            if (i < words - 1) {
                sb.append('-');
            }
        }
        sb.append('-').append(random.nextInt(1000, 9999));
        return sb.toString();
    }
}
