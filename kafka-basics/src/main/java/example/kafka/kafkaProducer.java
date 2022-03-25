package example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class kafkaProducer {
    public static void main(String[] args) {
        //String bootstrapServers = "127.0.0.1:9092";
        Logger logger = LoggerFactory.getLogger(kafkaProducer.class);
        // Create Producer Properties
        Properties properties = new Properties();
        setupBootstrapAndSerializers(properties);
        setupBatchingAndCompression(properties);
        setupRetriesInFlightTimeout(properties);


        // create the Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        for(int i=0;i<10;i++) {
            // create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("kafka-topic", "hello world"+ Integer.toString(i));

            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("Metadata: \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "offset: " + recordMetadata.offset() + "\n" +
                            "timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error" + e);
                }
            });
        }
        producer.flush();
        producer.close();
    }
    private static void setupRetriesInFlightTimeout(Properties props) {
        // Only two in-flight messages per Kafka broker connection
        // - max.in.flight.requests.per.connection (default 5)
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        // Set the number of retries - retries
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Request timeout - request.timeout.ms
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);

        // Only retry after one second.
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1_000);

    }

    private static void setupBatchingAndCompression(Properties props) {
        // If 0, it turns the batching off.
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 10_240);
        // turns linger on and allows us to batch for 10 ms if size is not met
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1000);

        // Use Snappy compression for batch compression
        // props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

    }

    private static void setupBootstrapAndSerializers(Properties props) {
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "TwitterKafkaProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Custom Serializer - config "value.serializer"
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


    }
}
