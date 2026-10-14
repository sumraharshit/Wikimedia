package com.kafka.kafkaConsumerDatabase;

import com.kafka.kafkaConsumerDatabase.entity.EventData;
import com.kafka.kafkaConsumerDatabase.repository.ConsumerRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumer {

    private final ConsumerRepository consumerRepository;

    public KafkaConsumer(ConsumerRepository consumerRepository) {
        this.consumerRepository = consumerRepository;
    }

    @KafkaListener(topics = "wikimedia_recentChange", groupId = "consumerGroup")
    public void consume(String eventMessage){
        log.info("The Event has been received by the consumer: " + eventMessage);

        EventData eventData = new EventData();
        eventData.setData(eventMessage);

        consumerRepository.save(eventData);

    }
}
