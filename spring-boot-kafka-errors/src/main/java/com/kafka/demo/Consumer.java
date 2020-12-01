package com.kafka.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;


@Service
class Consumer {

    private final Logger logger = LoggerFactory.getLogger(Consumer.class);

    /*@KafkaListener(id = "fooGroup", topics = "example-topic")
    public void listen(String in) {
        logger.info("Received: " + in);
        if (in.startsWith("foo")) {
            throw new RuntimeException("failed");
        }
    }*/

    @KafkaListener(id = "fooGroup", topics = "example-topic")
    public void listen(String in, @Header(KafkaHeaders.GROUP_ID) String groupId, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        logger.info("Received: " + in);
        logger.info("groupId: " + groupId);
        logger.info("topic: " + topic);
        if (in.startsWith("foo")) {
            throw new FooException("failed");
        }
    }

    @KafkaListener(id = "dltGroup", topics = "example-topic.DLT")
    public void dltListen(String in) {
        logger.info("Received from DLT: " + in);
    }
}