package com.training.kafkabasic.producer1;

import com.training.kafkabasic.common.AbstractStagingMessage.MessageStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface StagingMessage1Repository extends JpaRepository<StagingMessage1, Long> {

    List<StagingMessage1> findByStatus(MessageStatus status, Pageable pageable);

    @Modifying(clearAutomatically = true)
    @Query("update StagingMessage1 s set s.status = com.training.kafkabasic.common.AbstractStagingMessage.MessageStatus.SENT where s.id in :ids")
    void markAsSent(@Param("ids") List<Long> ids);
}
