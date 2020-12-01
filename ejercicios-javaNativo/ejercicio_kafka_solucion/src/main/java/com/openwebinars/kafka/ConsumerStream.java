package com.openwebinars.kafka;


import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Esta clase es la encargada de consumir de cada stream de kafka (kafkaStream).
 * Cada mensaje es parseado desde un json string a un mapa (event).
 * El mapa es introducido en la cola (queue) que es utilizada para comunicar el consumidor (Consumer) con el procesador
 * (Processor).
 */
public class ConsumerStream implements Runnable {
    LinkedBlockingQueue<Map<String, Object>> inQueue;
    KafkaStream kafkaStream;
    ObjectMapper objectMapper = new ObjectMapper();

    public ConsumerStream(KafkaStream kafkaStream, LinkedBlockingQueue<Map<String, Object>> inQueue) {
        this.inQueue = inQueue;
        this.kafkaStream = kafkaStream;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> consumerIterator = kafkaStream.iterator();

        while (consumerIterator.hasNext()) {
            String kafkaMessage = new String(consumerIterator.next().message());

            try {
                Map<String, Object> event = objectMapper.readValue(kafkaMessage, Map.class);
                inQueue.put(event);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
