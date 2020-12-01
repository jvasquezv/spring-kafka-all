package com.demo.engine;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

@Service
public class Consumer {

    private final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private final CountDownLatch latch = new CountDownLatch(3);

    @KafkaListener(topics = "example-topic", groupId = "group_id")
    public void consume(String message) throws IOException {
        logger.info(String.format("#### -> Consumed message -> %s", message));
    }

    /*@KafkaListener(topics = "example-topic")
    public void listen(ConsumerRecord<?, ?> cr) throws Exception {
        logger.info(cr.toString());
        latch.countDown();
    }*/
}
