package com.training.kafkabasic.consumer;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ConsumedMessageRepository extends JpaRepository<ConsumedMessage, Long> {

    List<ConsumedMessage> findTop20ByOrderByConsumedAtDesc();
}
