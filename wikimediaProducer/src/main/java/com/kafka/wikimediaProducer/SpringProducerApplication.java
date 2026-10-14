package com.kafka.wikimediaProducer;

import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringProducerApplication implements CommandLineRunner {

    public static void main(String[] args){
        SpringApplication.run(SpringProducerApplication.class);
    }

    @Autowired
    private WikimediaProducer wikimediaProducer;


    //whenever the application starts it calls the run method, and we call the sendMessages method().
    @Override
    public void run(String @NonNull ... args) throws Exception {
        wikimediaProducer.sendMessage();
    }
}
