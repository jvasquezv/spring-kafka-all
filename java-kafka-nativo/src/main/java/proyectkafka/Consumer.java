package proyectkafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Properties properties=new Properties();
        properties.setProperty("log4j.rootLogger","INFO,stdout");
        properties.setProperty("log4j.rootCategory","INFO");
        properties.setProperty("log4j.appender.stdout",     "org.apache.log4j.ConsoleAppender");
        properties.setProperty("log4j.appender.stdout.layout",  "org.apache.log4j.PatternLayout");
        properties.setProperty("log4j.appender.stdout.layout.ConversionPattern","%d{yyyy/MM/dd HH:mm:ss.SSS} [%5p] %t (%F) :: %m%n");


        PropertyConfigurator.configure(properties);

        Logger logger = LoggerFactory.getLogger(Consumer.class);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC,"earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
        try {
            consumer.subscribe(Arrays.asList("odd", "even", "ufo_sightings"));
            int counter = 0;
            while (counter <= 1000) {
                ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(1000));
                if (recs.count() == 0) {
                } else {
                    for (ConsumerRecord<String, String> rec : recs) {
                        //Thread.sleep(1000);
                        logger.info("Recieved  {} - {}", rec.key(), rec.value());


                        //System.out.printf("Recieved %s: %s", rec.key(), rec.value());
                    }
                }
                counter++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}


