package proyectkafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Producer.class);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        try {
            for (int i = 0; i < 1000; i++) {
                ProducerRecord<String, String> data;
                if (i % 2 == 0) {
                    data = new ProducerRecord<String, String>("even", 0, Integer.toString(i), String.format("%d is even", i));
                } else {
                    data = new ProducerRecord<String, String>("odd", 0, Integer.toString(i), String.format("%d is odd", i));
                }
                logger.info("ok");
                producer.send(data);
            }
            logger.info("Erro ok");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}