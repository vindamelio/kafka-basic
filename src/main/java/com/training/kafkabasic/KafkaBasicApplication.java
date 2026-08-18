package com.training.kafkabasic;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class KafkaBasicApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaBasicApplication.class, args);
    }
}
