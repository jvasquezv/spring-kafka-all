package com.openwebinars.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openwebinars.Processor;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public class Producers extends Thread{
    LinkedBlockingQueue<Map<String, Object>> outQueue;
    Producer<String, String> kafkaProducer;
    ObjectMapper objectMapper;
    String metricTopic;
    String controlTopic;
    String alertTopic;
    String systemName;


    public Producers(String systemName, String controlTopic, String metricTopic, String alertTopic,
                     LinkedBlockingQueue<Map<String, Object>> outQueue){
        this.outQueue = outQueue;
        this.kafkaProducer = new Producer(getProducerConfig());
        this.objectMapper = new ObjectMapper();
        this.metricTopic = metricTopic;
        this.alertTopic = alertTopic;
        this.controlTopic = controlTopic;
        this.systemName = systemName;
    }

    private ProducerConfig getProducerConfig(){
        Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        return new ProducerConfig(properties);
    }

    @Override
    public void run() {
        while (!isInterrupted()){
            try {
                Map<String, Object> event = outQueue.take();
                event.put("systemName", systemName);
                String eventType = (String) event.get("type");
                String topic = null;

                if(eventType.equals(Processor.EventType.CONTROL.type)){
                    topic = controlTopic;
                } else if(eventType.equals(Processor.EventType.ALERT.type)){
                    topic = alertTopic;
                } else if(eventType.equals(Processor.EventType.METRIC.type)){
                    topic = metricTopic;
                }

                if(topic != null) {
                    String json = objectMapper.writeValueAsString(event);
                    kafkaProducer.send(new KeyedMessage<String, String>(topic, json));
                } else {
                    System.err.println("The event type is null");
                }

            } catch (InterruptedException e){
                System.out.println("Apagando el productor ... ");

            } catch(JsonProcessingException e) {
                System.out.println("No se puede convertir a JSON");
            }
        }
    }

    public void shutdown(){
        interrupt();
    }
}
