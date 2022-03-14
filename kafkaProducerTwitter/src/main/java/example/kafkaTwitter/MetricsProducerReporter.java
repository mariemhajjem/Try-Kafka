package example.kafkaTwitter;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class MetricsProducerReporter implements Runnable{
    private final Producer<String,String> producer;
    private final Logger logger = LoggerFactory.getLogger(MetricsProducerReporter.class);

    public MetricsProducerReporter(Producer<String, String> producer) {
        this.producer = producer;
    }

    @Override
    public void run() {
        while (true) {
            final Map<MetricName, ? extends Metric> metrics = producer.metrics();

            displayMetrics(metrics);
            try {
                Thread.sleep(3_000);
            } catch (InterruptedException e) {
                logger.warn("metrics interrupted");
                Thread.interrupted();
                break;
            }
        }
    }


    static class MetricPair {
        private final MetricName metricName;
        private final Metric metric;

        MetricPair(MetricName metricName, Metric metric) {
            this.metricName = metricName;
            this.metric = metric;
        }

        public String toString() {
            return metricName.group() + "." + metricName.name();
        }
    }

    /**
     * Only prints out the names that are in metricsNameFilter. Java8 stream is used to filter and
     * sort metrics. We get rid of metric values that are NaN,infinite numbers and 0s. THen we sort
     * map by converting it to TreeMap. The metric
     *
     * @param metrics
     */
    private void displayMetrics(Map<MetricName, ? extends Metric> metrics) {
        final Map<String, MetricPair> metricsDisplayMap = metrics.entrySet().stream()
//                // Filter out metrics not in metricsNameFilter
//                .filter(metricNameEntry -> metricsNameFilter.contains(metricNameEntry.getKey().name()))
//                // Filter out metrics not in metricsNameFilter
//                .filter(metricNameEntry -> !Double.isInfinite(metricNameEntry.getValue().value())
//                        && !Double.isNaN(metricNameEntry.getValue().value())
//                        && metricNameEntry.getValue().metricValue().toString() != "0")
                // Turn Map<MetricName,Metric> into TreeMap<String, MetricPair>
                .map(entry -> new MetricPair(entry.getKey(), entry.getValue()))
                .collect(Collectors.toMap(MetricPair::toString, it -> it, (a, b) -> a, TreeMap::new));


        // Output metrics
        final StringBuilder builder = new StringBuilder(255);
        builder.append("\n---------------------------------------\n");
        metricsDisplayMap.entrySet().forEach(entry -> {
            MetricPair metricPair = entry.getValue();
            String name = entry.getKey();
            builder.append(String.format(Locale.US, "%50s%25s\t\t%s\t\t%s\n", name,
                    metricPair.metricName.name(), metricPair.metric.metricValue(),
                    metricPair.metricName.description()));
        });
        builder.append("\n---------------------------------------\n");
        logger.info(builder.toString());
    }

}
