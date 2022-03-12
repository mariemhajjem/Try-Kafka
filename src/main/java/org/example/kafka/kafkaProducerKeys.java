package org.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class kafkaProducerKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";
        Logger logger = LoggerFactory.getLogger(kafkaProducerKeys.class);
        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        for(int i=0;i<10;i++) {
            // create a producer record
            String topic = "kafka-topic";
            String value = "hello world"+ Integer.toString(i);
            String key = "id_"+ Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key, value);
            logger.info("Key: "+ key);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Metadata: \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "offset: " + recordMetadata.offset() + "\n" +
                                "timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error" + e);
                    }
                }
            }).get();
        }
        producer.flush();
        producer.close();
    }
}
