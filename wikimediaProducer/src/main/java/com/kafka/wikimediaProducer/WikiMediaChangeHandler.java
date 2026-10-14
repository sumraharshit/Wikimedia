package com.kafka.wikimediaProducer;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
public class WikiMediaChangeHandler implements BackgroundEventHandler {

    //EventHandler is deprecated, instead used the BackgroundEventHandler

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public WikiMediaChangeHandler(KafkaTemplate<String, String> kafkaTemplate, String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        log.info("Opened");
    }

    @Override
    public void onClosed() throws Exception {
        log.info("Closed");
    }

    //This medthod is triggered whenever a new event is published in our source i.e the wikimedia
    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {

        log.info(messageEvent.getData());
        kafkaTemplate.send(topic, messageEvent.getData());
    }



    @Override
    public void onComment(String comment) throws Exception {
        log.info("Comment Arrived: " + comment);
    }

    @Override
    public void onError(Throwable t) {
        log.error("The error is received: ", t);
    }
}
