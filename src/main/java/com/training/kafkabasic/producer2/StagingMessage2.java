package com.training.kafkabasic.producer2;

import com.training.kafkabasic.common.AbstractStagingMessage;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "staging_message_2")
@NoArgsConstructor
public class StagingMessage2 extends AbstractStagingMessage {

    public StagingMessage2(String content) {
        super(content);
    }
}
