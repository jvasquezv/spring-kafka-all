package com.kschool.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kschool.Processor;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public class Productor extends Thread{
    LinkedBlockingQueue<Map<String, Object>> outQueue;
    Producer<String, String> kafkaProducer;
    ObjectMapper objectMapper;
    String metricTopic;
    String controlTopic;
    String alertTopic;
    String systemName;


    public Productor(String systemName, String controlTopic, String metricTopic, String alertTopic,
                     LinkedBlockingQueue<Map<String, Object>> outQueue){
        this.outQueue = outQueue;
        this.objectMapper = new ObjectMapper();
        this.metricTopic = metricTopic;
        this.alertTopic = alertTopic;
        this.controlTopic = controlTopic;
        this.systemName = systemName;
        this.kafkaProducer = //TODO: Ejercicio 8
    }

    private ProducerConfig getProducerConfig(){
        //TODO: Ejercicio 7
    }

    @Override
    public void run() {
        while (!isInterrupted()){
            try {
                Map<String, Object> event = outQueue.take();
                event.put("systemName", systemName);
                String eventType = (String) event.get("type");
                String topic = null;

                if(eventType.equals(Processor.EventType.CONTROL.get)){
                    topic = controlTopic;
                } else if(eventType.equals(Processor.EventType.ALERT.get)){
                    topic = alertTopic;
                } else if(eventType.equals(Processor.EventType.METRIC.get)){
                    topic = metricTopic;
                }

                if(topic != null) {
                   // TODO: Ejercicio 9
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
