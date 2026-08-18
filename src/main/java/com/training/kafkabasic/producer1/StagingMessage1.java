package com.training.kafkabasic.producer1;

import com.training.kafkabasic.common.AbstractStagingMessage;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "staging_message_1")
@NoArgsConstructor
public class StagingMessage1 extends AbstractStagingMessage {

    public StagingMessage1(String content) {
        super(content);
    }
}
