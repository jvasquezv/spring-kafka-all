package com.kafkamulticonsumer.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(id = "multiGroup", topics = { "foos", "bars" })
public class Consumer {
    private final Logger logger = LoggerFactory.getLogger(Consumer.class);

    @KafkaHandler
    public void foo(Foo2 foo) {
        System.out.println("Received: " + foo);
    }

    @KafkaHandler
    public void bar(Bar2 bar) {
        System.out.println("Received: " + bar);
    }

    @KafkaHandler(isDefault = true)
    public void unknown(String str, ConsumerRecordMetadata meta) {
        System.out.println("Received unknown: " + str);
        System.out.println("Received unknown: " + meta.topic());
    }

}