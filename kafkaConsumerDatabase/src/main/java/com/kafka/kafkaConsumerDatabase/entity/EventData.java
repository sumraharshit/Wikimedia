package com.kafka.kafkaConsumerDatabase.entity;

import jakarta.persistence.*;
import lombok.Setter;

@Entity
@Table(name = "event_data")
@Setter
public class EventData {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Lob //when data is huge we can use the @Lob annotation
    private String data;
}
