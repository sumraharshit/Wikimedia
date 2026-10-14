package com.kafka.kafkaConsumerDatabase.repository;

import com.kafka.kafkaConsumerDatabase.entity.EventData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ConsumerRepository extends JpaRepository<EventData, Long> {
}
