package com.kschool.kafka;


import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class ConsumerStream implements Runnable {
    LinkedBlockingQueue<Map<String, Object>> inQueue;
    KafkaStream kafkaStream;
    ObjectMapper objectMapper = new ObjectMapper();

    public ConsumerStream(KafkaStream kafkaStream, LinkedBlockingQueue<Map<String, Object>> inQueue) {
        this.inQueue = inQueue;
        this.kafkaStream = kafkaStream;
    }

    public void run() {
        //TODO: Ejercicio 5
    }
}
