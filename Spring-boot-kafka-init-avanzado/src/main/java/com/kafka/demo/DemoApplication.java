package com.kafka.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootApplication
public class DemoApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Autowired
    private KafkaTemplate<String, String> template;
    private final CountDownLatch latch = new CountDownLatch(3);

    @Override
    public void run(String... args) throws Exception {
        this.sendMessage("foo0");
        this.sendMessage("foo1");
        this.sendMessage("foo2");
        this.sendMessage("foo3");
        log.info("All received");
    }

    @KafkaListener(topics = "myTopic",groupId = "foo")
    public void listen(ConsumerRecord<?, ?> cr) throws InterruptedException {
        latch.await(60, TimeUnit.SECONDS);
        log.info("Consumido: "+ cr.toString());
    }

    public void sendMessage(String message){

        ListenableFuture<SendResult<String, String>> future = template.send("myTopic", message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.info("Sent message=[{}] with offset=[{}]" , result ,result.getRecordMetadata().offset());
            }
            @Override
            public void onFailure(Throwable ex) {
                log.info("Unable to send message=[{}] due to : {}",message,ex.getMessage());
            }
        });
    }

}
