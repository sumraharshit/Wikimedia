package com.kafka.wikimediaProducer;


import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.internal.http2.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.URL;
import java.util.concurrent.TimeUnit;

@Service
public class WikimediaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public WikimediaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage() throws InterruptedException {

        Headers headers = new Headers.Builder()
                .add("User-Agent", "WikiMediaProducer")
                .build();

        String topic = "wikimedia_recentChange";
        BackgroundEventHandler eventHandler = new WikiMediaChangeHandler(kafkaTemplate, topic);

       // String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        HttpUrl url = new HttpUrl.Builder()
                 .scheme("https")
                .host("stream.wikimedia.org")
                .addPathSegment("v2")
                .addPathSegment("stream")
                .addPathSegment("recentchange")
                .build();

        HttpUrl url2 = HttpUrl.get("https://stream.wikimedia.org/v2/stream/recentchange");

        EventSource.Builder eventBuilder = new EventSource.Builder(
                ConnectStrategy.http(url)
                        .headers(headers)
        );

        BackgroundEventSource.Builder backgroundEventSourceBuilder = new BackgroundEventSource.Builder(eventHandler, eventBuilder);

        BackgroundEventSource backgroundEventSource = backgroundEventSourceBuilder.build();

        backgroundEventSource.start(); //starting the evensourcce in a seperate thread

        TimeUnit.MINUTES.sleep(10); //The event soure will stop after the 10 minutes upon closing of the appliccation
        //we did this bz if we close the application, the main thread will be killed and jvm blocks all the background proccesses and kill them
        //with the sleep(), we are keeping the main thread alive so the background task can be completed
    }

}
