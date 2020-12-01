package com.kschool.kafka;


import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Consumidor {
    LinkedBlockingQueue<Map<String, Object>> inQueue;
    Map<String, Integer> topicCountMap;

    private ConsumerConnector consumer;
    private ExecutorService executor;

    public Consumidor(String topic, Integer partitions, LinkedBlockingQueue<Map<String, Object>> inQueue) {
        this.inQueue = inQueue;
        this.topicCountMap = new HashMap<>();
        this.topicCountMap.put(topic, partitions);
        this.executor = Executors.newFixedThreadPool(partitions);
        this.consumer = //TODO: Ejercicio 2
    }

    private ConsumerConfig getKafkaConsumer() {
        //TODO: Ejercicio 1
    }

    public void start() {
        //TODO: Ejercicio 3

        //TODO: Ejercicio 4
    }

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Tiempo de espera agotado.");

            }
        } catch (InterruptedException e) {
            System.out.println("Interrupcion durante el apagado!!");
        }
    }
}
