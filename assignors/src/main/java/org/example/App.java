package org.example;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class App {
    private static Logger logger = Logger.getLogger(App.class.getName());

    // Track assigned partition lag count to be passed along to the consumer group leader during rebalances
    public static final Map<TopicPartition,Number> partitionLagCounts = new HashMap<>();
    // Track assigned partition lag time to be passed along to the consumer group leader during rebalances
    public static final Map<TopicPartition,Double> partitionLagTimes = new HashMap<>();

    public static void main(String[] args) {
        Map<String,String> env = System.getenv();

        // Ref: https://kafka.apache.org/28/documentation.html#consumerconfigs
        HashMap<String,Object> config = new HashMap<>(Map.of(
            "key.deserializer",              "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer",            "org.example.JsonDeserializer",
            "bootstrap.servers",             env.getOrDefault("bootstrap_servers",
                                               "broker0:9092,broker1:9092,broker2:9092"),
            "group.id",                      env.getOrDefault("group_id", "group0"),
            "enable.auto.commit",            "false",
            "partition.assignment.strategy", env.getOrDefault("partition_assignment_strategy",
                                               "org.apache.kafka.clients.consumer.RangeAssignor")
        ));
        String hostname = getHostname();
        if (runningInDocker() && hostname != null)
            config.put("group.instance.id", hostname);

        Thread[] workers = new Thread[
            Integer.parseInt(env.getOrDefault(
                "num_threads",
                Integer.toString(Runtime.getRuntime().availableProcessors())
            ))
        ];

        logger.info(String.format("Starting consumer with %d threads and partition.assignment.strategy %s",
            workers.length, config.get("partition.assignment.strategy")));

        AtomicBoolean shutdown = new AtomicBoolean(false);
        BlockingQueue<ConsumerRecord<String,Map<String,Object>>> queue =
            new ArrayBlockingQueue<>(Integer.parseInt(env.getOrDefault("queue_size", "1000")));

        try (KafkaConsumer<String,Map<String,Object>> consumer = new KafkaConsumer<>(config)) {
            //
            // Consumer threads
            //

            for (int i = 0; i < workers.length; i++) {
                workers[i] = new Thread(() -> {
                    String name = Thread.currentThread().getName();
                    while (!shutdown.get()) {
                        try {
                            ConsumerRecord<String,Map<String,Object>> record;
                            if ((record = queue.poll(100, TimeUnit.MILLISECONDS)) == null)
                                continue;

                            // Simulate processing the record
                            Thread.sleep((Integer)record.value().getOrDefault("cpu_ms", 1));
                        } catch (InterruptedException e) {
                            logger.severe(String.format("%s Interrupted %s", name, e.getMessage()));
                            break;
                        }
                    }
                }, "worker-" + i);

                workers[i].start();
            }

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                shutdown.set(true);
                for (Thread worker : workers) {
                    try { worker.join(); } catch (InterruptedException e) { }
                }
            }));

            //
            // Producer "thread"
            //

            int rebalancePeriodSecs = 60; // TODO In practice, 900 is more appropriate
            float rebalanceTriggerPct = 0.25f; // TODO Should be based on the number of consumers
            Instant[] lastRebalance = new Instant[] { Instant.now() };

            consumer.subscribe(
                Pattern.compile(env.getOrDefault("topic_pattern", "topic.*")),
                new ConsumerRebalanceListener() {
                    @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        logger.info("Assigned partitions: " + partitions.toString());
                        lastRebalance[0] = Instant.now();
                    }

                    @Override public void onPartitionsLost(Collection<TopicPartition> partitions) {
                        logger.warning("Lost partitions: " + partitions.toString());
                    }

                    @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        logger.info("Revoked partitions: " + partitions.toString());
                        lastRebalance[0] = Instant.now();
                    }
                }
            );

            int metricsPeriodLength = 60;
            Instant metricsPeriod = Instant.now().plusSeconds(metricsPeriodLength);

            while (!shutdown.get()) {
                ConsumerRecords<String,Map<String,Object>> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String,Map<String,Object>> record : records) {
                    try {
                        queue.put(record);
                    } catch (InterruptedException e) {
                        logger.severe("producer Interrupted " + e.getMessage());
                        break;
                    }
                }

                // TODO Ideally commit offsets in the worker threads after fully processing the record,
                //      however KafkaConsumer is not only not thread-safe but all operations must be done in the same thread
                if (!records.isEmpty())
                    consumer.commitAsync();

                if (Duration.between(lastRebalance[0], Instant.now()).toSeconds() > rebalancePeriodSecs
                        && ThreadLocalRandom.current().nextDouble() < rebalanceTriggerPct) {
                    lastRebalance[0] = Instant.now();
                    consumer.enforceRebalance();
                    logger.info("Triggering artificial rebalance");
                }

                // Ref: https://kafka.apache.org/28/generated/consumer_metrics.html
                if (Instant.now().isAfter(metricsPeriod)) {
                    metricsPeriod = Instant.now().plusSeconds(metricsPeriodLength);

                    synchronized (partitionLagCounts) {
                        partitionLagCounts.clear();

                        for (Map.Entry<MetricName,? extends Metric> metricEntry : consumer.metrics().entrySet()) {
                            MetricName metricName = metricEntry.getKey();
                            Map<String,String> metricNameTags = metricName.tags();
                            if (!metricName.name().equals("records-lag") || !metricNameTags.containsKey("partition"))
                                continue;

                            partitionLagCounts.put(
                                new TopicPartition(metricNameTags.get("topic"), Integer.parseInt(metricNameTags.get("partition"))),
                                (Number) metricEntry.getValue().metricValue()
                            );
                        }
                    }

                    synchronized (partitionLagTimes) {
                        partitionLagTimes.clear();

                        Map<TopicPartition,Number> partitionLags = new HashMap<>();
                        Map<String,Double> topicConsumeRates = new HashMap<>();

                        for (Map.Entry<MetricName,? extends Metric> metricEntry : consumer.metrics().entrySet()) {
                            MetricName metricName = metricEntry.getKey();
                            Map<String,String> metricNameTags = metricName.tags();

                            if (metricName.name().equals("records-lag") && metricNameTags.containsKey("partition")) {
                                partitionLags.put(
                                    new TopicPartition(metricNameTags.get("topic"), Integer.parseInt(metricNameTags.get("partition"))),
                                    (Number) metricEntry.getValue().metricValue()
                                );
                            } else if (metricName.name().equals("records-consumed-rate")) {
                                topicConsumeRates.put(
                                    metricNameTags.get("topic"),
                                    (Double) metricEntry.getValue().metricValue()
                                );
                            }
                        }

                        Map<String,Integer> partitionCounts = partitionLags.keySet().stream()
                            .map(TopicPartition::topic)
                            .collect(Collectors.toMap(
                                topic -> topic,
                                topic -> consumer.partitionsFor(topic).size(),
                                (existing, current) -> existing
                            ));

                        for (Map.Entry<TopicPartition,Number> entry : partitionLags.entrySet()) {
                            TopicPartition partition = entry.getKey();
                            Number partitionLag = entry.getValue();

                            Double topicConsumeRate;
                            if ((topicConsumeRate = topicConsumeRates.get(partition.topic())) == null) {
                                logger.warning("Missing consume rate for " + partition.toString());
                                continue;
                            }
                            Integer partitionCount = partitionCounts.get(partition.topic());
                            Double partitionConsumeRate = topicConsumeRate / partitionCount;

                            Double partitionLagTime;
                            if (partitionConsumeRate.compareTo(0.0) == 0) {
                                if (((Double) partitionLag.doubleValue()).compareTo(0.0) == 0)
                                    // Though zero divided by zero is undefined, effectively it is zero lag time;
                                    // avoiding NaN because it interferes with proper sorting in the partition assignor
                                    partitionLagTime = 0.0;
                                else
                                    partitionLagTime = Double.POSITIVE_INFINITY;
                            } else {
                                partitionLagTime = partitionLag.doubleValue() / partitionConsumeRate;
                            }

                            partitionLagTimes.put(partition, partitionLagTime);
                        }
                    }
                }
            }
        }
    }

    private static boolean runningInDocker() {
        return (new File("/.dockerenv")).exists();
    }

    private static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return null;
        }
    }
}
