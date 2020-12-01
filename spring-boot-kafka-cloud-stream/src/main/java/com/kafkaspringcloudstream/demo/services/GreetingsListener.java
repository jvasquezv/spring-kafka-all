package com.kafkaspringcloudstream.demo.services;

import com.kafkaspringcloudstream.demo.models.Greetings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;

@Slf4j
@EnableBinding(Sink.class)
public class GreetingsListener {
    @StreamListener("input")
    public void consumeMessage(Greetings greetings) {
        log.info("Received greetings: {}", greetings);
    }
}