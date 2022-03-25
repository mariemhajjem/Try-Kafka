package example.kafkaTwitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    public TwitterProducer() {}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client client = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();
        //create kafka Producer
        KafkaProducer<String,String> producer = createKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            new MetricsProducerReporter(producer).run();
            logger.info("stopping app...");
            logger.info("shut down twitter client...");
            client.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done!...");
        }));
        // loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);

            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg != null){
                logger.info(msg);

                producer.send(new ProducerRecord<>(AppConstants.TOPIC, null, msg), (recordMetadata, e) -> {
                    new MetricsProducerReporter(producer).run();
                    if(e != null)
                        logger.error(("Erooor"+ e));
                });
            };
        }
        logger.info("End of application!!");
    }
    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("bigdata","kafka");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1("CMR6WgmCfvJATL8DGGlrpCBC9", "a4puMvtFCq7irWzPjuLvZD4pT0G3tXNrR0vXaNk2nu4kaxvF8T", "1077161826117476352-0GODQHRRBO1eC8cPPJXctNeVNUf09v", "VKqJM4Y2AKQ1sJmYddf3Rj1nKw1EY2IBt5D81ezLjtBKq");
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }
    public KafkaProducer<String,String> createKafkaProducer() {
        // Create Producer Properties
        Properties properties = new Properties();
        setupBootstrapAndSerializers(properties);
        setupBatchingAndCompression(properties);
        setupRetriesInFlightTimeout(properties);

        //high throughput producer (but the expense is latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy"); //default none, else Gzip , lz4..
        // Also the consumer knows how to decompress auto
        // lag in ms, 9adeh you93ed yesstana 9bal ma yab3eth req to kafka batch mais better throughput
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024)); // 32 KB batch size
        // create the Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    private static void setupRetriesInFlightTimeout(Properties props) {
        //create safe Producer // idempotent producer to guarantee a stable and safe pipeline
        /*props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true"); //idempotence producer won't introduce duplicates on network error
        props.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE)); */
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
