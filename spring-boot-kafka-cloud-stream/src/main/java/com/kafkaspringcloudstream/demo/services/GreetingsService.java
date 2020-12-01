package com.kafkaspringcloudstream.demo.services;

import com.kafkaspringcloudstream.demo.models.Greetings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.MimeTypeUtils;

@Service
@Slf4j
@EnableBinding(Source.class)
public class GreetingsService {
    @Autowired
    private MessageChannel output;

    public void sendGreeting(final Greetings greetings) {
        log.info("Sending greetings {}", greetings);

        output.send(MessageBuilder
                .withPayload(greetings)
                .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON)
                .build());

    }
}