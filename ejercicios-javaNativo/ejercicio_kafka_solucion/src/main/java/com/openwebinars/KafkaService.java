package com.openwebinars;

import com.openwebinars.kafka.Consumer;
import com.openwebinars.kafka.Producers;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class KafkaService {
    public static void main(String[] args) {
        LinkedBlockingQueue<Map<String, Object>> inQueue = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Map<String, Object>> outQueue = new LinkedBlockingQueue<>();

        String inTopic = "inTopic";
        String controlTopic = "controlTopic";
        String metricTopic = "metricTopic";
        String alertTopic = "alertTopic";

        Integer partitions = 1;
        String processNanme = args.length == 0 ? "defaultName" : args[0];

        final Consumer consumer = new Consumer(inTopic, partitions, inQueue);
        final Processor processor = new Processor(inQueue, outQueue);
        final Producers producer = new Producers(processNanme, controlTopic, metricTopic, alertTopic, outQueue);

        producer.start();
        processor.start();
        consumer.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                consumer.shutdown();
                processor.shutdown();
                producer.shutdown();
                System.out.println("Apagado!");
            }
        });
    }
}
