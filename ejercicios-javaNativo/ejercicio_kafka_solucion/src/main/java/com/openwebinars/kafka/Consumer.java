package com.openwebinars.kafka;


import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Consumer {
    LinkedBlockingQueue<Map<String, Object>> inQueue;
    Map<String, Integer> topicCountMap;
    String topic;
    Integer threads;

    private ExecutorService executor;
    KafkaConsumer<String, String> consumer;


    public Consumer(String topic, Integer threads, LinkedBlockingQueue<Map<String, Object>> inQueue) {
        this.inQueue = inQueue;
        this.topic = topic;
        this.threads = threads;
        this.executor = Executors.newFixedThreadPool(threads);
        this.consumer = new KafkaConsumer<>(getKafkaConsumer());
    }

    private Properties getKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "openwebinars-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        return props;
    }

    public void start() {
        for (Integer i = 0; i < threads; i++) {
            executor.submit(new ConsumerStream(consumer, inQueue));
        }
    }

    public void shutdown() {
        if (consumer != null) consumer.close();
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
