package org.example;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Logger;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Balance partitions to consumer assignments based on lag time
 *
 * Lag time is defined as lag count divided by partition consume rate (records per second).
 *
 * Balancing partitions by lag is intended to spread the load evenly across consumers.
 * When some partitions have high lag, this ensures that their consumers have fewer partitions,
 * in order to allow those consumers to process the lag and catch up as quickly as possible.
 *
 * Taking consume rate into account accommodates topics that have different processing times.
 *
 * Algorithm outline:
 * 0. Prerequisite: Consumers periodically trigger rebalances (if not occurring otherwise)
 * 1. Sort the consumers by lag time (high to low)
 *    a. If the difference between the highest and median value is less than q%, abort
 *       i. Do not use the lowest value since anticipating inactive partitions
 * 2. Segment the consumers into the top n% and the remaining consumers
 * 3. For the consumers in the first segment, assign their m% least lagged partitions
 *    randomly into consumers of the second segment (bottom (100-n)%)
 *    a. Not assigning to the least-lagged consumers to avoid ping-ponging partitions
 *    b. Do not assign more than (total partitions / consumer count) * x to any consumer
 *       i. To avoid overloading consumers with inactive / low-lagged partitions
 */
public class CooperativeLagTimePartitionAssignor implements ConsumerPartitionAssignor {
    private static final float CONSUMER_MOST_LAGGED_PCT = 0.15f;
    private static final float PARTITION_LEAST_LAGGED_PCT = 0.15f;
    private static final float OVERLOAD_FACTOR = 1.67f;
    
    private static final int REASSIGN_PERIOD_SECS = 60; // TODO 900 is more appropriate
    
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    
    private static Logger logger = Logger.getLogger(CooperativeLagTimePartitionAssignor.class.getName());
    
    private Instant lastRebalance = null;
    
    @Override public String name() {
        return "coop-lag-time";
    }
    
    @Override public short version() {
        return 1;
    }
    
    @Override public List<RebalanceProtocol> supportedProtocols() {
        return List.of(RebalanceProtocol.COOPERATIVE, RebalanceProtocol.EAGER);
    }
    
    /** Assign partitions (by the consumer group leader and triggered by a rebalance) */
    @Override public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
        // TODO Rather than use a number of disparate data structures, consider defining:
        //      Map<String,List<TopicPartitionInfo>> in which TopicPartitionInfo contains the lag time
        //      (possibly also with a Map<TopicPartition,String> for fast consumer lookups)
        
        final Map<String,Subscription> subscriptionMap = groupSubscription.groupSubscription();
        
        final Set<String> allConsumers = subscriptionMap.keySet().stream()
            //.filter(consumer -> !consumer.startsWith("special"))
            .collect(Collectors.toSet());
        
        Set<String> idleConsumers = allConsumers.stream()
            .filter(consumer -> subscriptionMap.get(consumer).ownedPartitions().isEmpty())
            .collect(Collectors.toSet());
        
        final Set<TopicPartition> allPartitions = subscriptionMap.values().stream()
            .flatMap(subscription -> subscription.topics().stream())
            .distinct()
            .flatMap(topic -> metadata.availablePartitionsForTopic(topic).stream())
            .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
            .collect(Collectors.toSet());
        
        //
        // Initialize the assignment mapping
        // (based on the subscription mapping)
        // 
        
        Map<TopicPartition,String> assignmentMap = subscriptionMap.entrySet().stream()
            .flatMap(entry -> entry.getValue().ownedPartitions().stream()
                                  .map(partition -> new Pair<TopicPartition,String>(partition, entry.getKey()))
            )
            .collect(Collectors.toMap(
                Pair::getFirst,
                Pair::getSecond,
                // Not possible for two consumers to own the same partition
                (existing, replacement) -> existing,
                HashMap::new
            ));
        
        final Set<TopicPartition> ownedPartitions = new HashSet<>(assignmentMap.keySet());
        final Set<TopicPartition> unownedPartitions = allPartitions.stream()
            .filter(partition -> !ownedPartitions.contains(partition))
            .collect(Collectors.toSet());
        
        //
        // Sort the partitions by lag time
        // 

        PriorityQueue<PartitionLagTime> partitionLagTimes = new PriorityQueue<>(Collections.reverseOrder());
        Set<String> consumersWithoutUserData = new HashSet<>(allConsumers);
        
        for (Map.Entry<String,Subscription> entry : subscriptionMap.entrySet()) {
            String consumer = entry.getKey();
            ByteBuffer userData = entry.getValue().userData();
            if (userData == null) continue;
            
            List<PartitionLagTime> deserializedUserData = deserializeUserData(userData, consumer);
            if (deserializedUserData == null) continue;
            
            partitionLagTimes.addAll(deserializedUserData);
            consumersWithoutUserData.remove(consumer);
        }
        
        if (!consumersWithoutUserData.isEmpty())
            logger.warning("Consumers without valid user data: " + consumersWithoutUserData.toString());
        
        logger.info(String.format(
            "Consumers: %d, Idle consumers: %d, Owned/unowned partitions: %d/%d, Partitions with infinite/zero lagtime: %d/%d, Partition lagtime stddev: %.2f",
            allConsumers.size(), idleConsumers.size(), ownedPartitions.size(), unownedPartitions.size(),
            partitionLagTimes.stream().filter(e -> e.getSecond() == Double.POSITIVE_INFINITY).count(),
            partitionLagTimes.stream().filter(e -> e.getSecond().compareTo(0.0) == 0).count(),
            calculateStdDev(partitionLagTimes)
        ));
        
        //
        // Identify lagged consumers
        // 

        final Map<String,List<PartitionLagTime>> consumerLagTimes = partitionLagTimes.stream()
            .filter(partLagTime -> assignmentMap.containsKey(partLagTime.getFirst()))
            .map(partLagTime -> new Pair<String,PartitionLagTime>(
                assignmentMap.get(partLagTime.getFirst()),
                partLagTime
            ))
            .collect(Collectors.groupingBy(
                Pair::getFirst,
                Collectors.mapping(Pair::getSecond, Collectors.toList())
            ));
        
        // TODO Consider consumers with too many partitions lagged as well?
        Set<String> laggedConsumers;
        {
            FixedSizeMaxHeap<ConsumerLagTime> laggedConsumerLagTimes = new FixedSizeMaxHeap<>(
                (int)Math.max(1.0, allConsumers.size() * CONSUMER_MOST_LAGGED_PCT)
            );
            consumerLagTimes.entrySet().stream()
                .map(conLagTime -> new ConsumerLagTime(
                    conLagTime.getKey(),
                    conLagTime.getValue().stream().mapToDouble(PartitionLagTime::getSecond).sum()
                ))
                .forEach(laggedConsumerLagTimes::add);
            
            laggedConsumers = StreamSupport.stream(
                    Spliterators.spliterator(
                        laggedConsumerLagTimes.iterator(),
                        laggedConsumerLagTimes.size(),
                        Spliterator.ORDERED | Spliterator.SORTED | Spliterator.DISTINCT
                    ),
                    false
                )
                // Stream<ConsumerLagTime>
                .map(ConsumerLagTime::getFirst)
                .collect(Collectors.toSet());
        }
        
        final int maxPartitionsPerConsumer = (int)(allPartitions.size() / allConsumers.size() * OVERLOAD_FACTOR);
        
        Set<String> nonLaggedConsumers = allConsumers.stream()
            .filter(consumer -> !laggedConsumers.contains(consumer) &&
                                subscriptionMap.get(consumer).ownedPartitions().size() < maxPartitionsPerConsumer)
            .collect(Collectors.toSet());
        
        //
        // Assign unowned partitions
        // (first to idle consumers then non-lagged consumers)
        // 
        
        // TODO The state variables should be updated as partition assignment are made.
        //      Eg an unfortunate series of random values could lead to a consumer being overloaded.

        for (TopicPartition partition : unownedPartitions) {
            if (!idleConsumers.isEmpty()) {
                String consumer = randomSetEntry(idleConsumers);
                idleConsumers.remove(consumer);
                
                assignmentMap.put(partition, consumer);
                logger.info(String.format("Assigned partition %s to idle consumer %s", partition.toString(), consumer));
            }
            
            else if (!nonLaggedConsumers.isEmpty()) {
                String consumer = randomSetEntry(nonLaggedConsumers);
                
                assignmentMap.put(partition, consumer);
                logger.info(String.format("Assigned partition %s to non-lagged consumer %s", partition.toString(), consumer));
                
                // TODO If the consumer is handling >=maxPartitionsPerConsumer, remove it from nonLaggedConsumers
            }
            
            else {
                String consumer = randomSetEntry(allConsumers);
                
                assignmentMap.put(partition, consumer);
                logger.info(String.format("Assigned partition %s to consumer %s", partition.toString(), consumer));
            }
        }
        
        //
        // Remove (least lagged) partitions from lagged consumers
        //
        
        if (lastRebalance != null && Duration.between(lastRebalance, Instant.now()).toSeconds() > REASSIGN_PERIOD_SECS) {
            for (String laggedConsumer : laggedConsumers) {
                PriorityQueue<PartitionLagTime> laggedConsumerLagTimes = new PriorityQueue<>(consumerLagTimes.get(laggedConsumer));
                int partitionsToRemove = (int)(laggedConsumerLagTimes.size() * PARTITION_LEAST_LAGGED_PCT);
                
                for (int i = 0; i < partitionsToRemove; i++) {
                    TopicPartition partition = laggedConsumerLagTimes.poll().getFirst();
                    assignmentMap.remove(partition);
                    logger.info(String.format("Removed partition %s from lagged consumer %s", partition.toString(), laggedConsumer));
                }
            }
        }

        //
        // Convert the assignment mapping into a GroupAssignment object
        //

        Map<String,Assignment> invertedAssignmentMap = assignmentMap.entrySet().stream()
            .collect(Collectors.groupingBy(
                Map.Entry::getValue,
                Collectors.mapping(Map.Entry::getKey, Collectors.toList())
            ))
            // Map<String,List<TopicPartition>>
            .entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> new Assignment(entry.getValue())
            ));
        
        // Ensure consumers without partitions are represented
        for (String consumer : setDifference(allConsumers, invertedAssignmentMap.keySet()))
            invertedAssignmentMap.put(consumer, new Assignment(Collections.emptyList()));

        lastRebalance = Instant.now();
        return new GroupAssignment(invertedAssignmentMap);
    }
    
    /** Generate data (by consumer group members to be used by the group leader for partition assignments) */
    @Override public ByteBuffer subscriptionUserData(Set<String> topics) {
        synchronized (App.partitionLagTimes) {
            if (App.partitionLagTimes.isEmpty())
                return null;

            Set<String> seenTopics = App.partitionLagTimes.keySet().stream()
                .map(TopicPartition::topic)
                .collect(Collectors.toSet());
                
            if (!seenTopics.equals(topics)) {
                Set<String> unseenTopics = setDifference(topics, seenTopics);
                if (!unseenTopics.isEmpty())
                    logger.warning("Missing topics in App.partitionLagTime: " + unseenTopics.toString());
                    
                Set<String> unexpectedTopics = setDifference(seenTopics, topics);
                if (!unexpectedTopics.isEmpty())
                    logger.warning("Unexpected topics in App.partitionLagTime: " + unexpectedTopics.toString());
            }

            final int bufferLength = Short.BYTES +                     // Version
                Integer.BYTES +                                        // Entry count
                App.partitionLagTimes.keySet().stream()
                    .map(key -> Integer.BYTES +                        // Topic string length
                                key.topic().getBytes(CHARSET).length + // Topic string
                                Integer.BYTES +                        // Partition
                                Double.BYTES)                          // Lag time
                    .reduce(0, Integer::sum);
            ByteBuffer buffer = ByteBuffer.allocate(bufferLength);

            buffer.putShort(version());
            buffer.putInt(App.partitionLagTimes.size());

            for (Map.Entry<TopicPartition,Double> entry : App.partitionLagTimes.entrySet()) {
                byte[] topicBytes = entry.getKey().topic().getBytes(CHARSET);
                buffer.putInt(topicBytes.length);
                buffer.put(topicBytes);
                buffer.putInt(entry.getKey().partition());
                buffer.putDouble(entry.getValue());
            }

            buffer.flip();
            return buffer;
        }
    }
    
    private List<PartitionLagTime> deserializeUserData(ByteBuffer userData, String consumer) {
        List<PartitionLagTime> partitionLagTimes = new ArrayList<>();
        
        try {
            if (userData.getShort() != version()) {
                logger.warning(String.format("%s: User data version mismatch", consumer));
                return null;
            }

            int entryCount;
            if ((entryCount = userData.getInt()) < 0) {
                logger.warning(String.format("%s: Invalid entry count", consumer));
                return null;
            }

            for (int i = 0; i < entryCount; i++) {
                int topicLength;
                if ((topicLength = userData.getInt()) < 1) {
                    logger.warning(String.format("%s: Invalid topic length", consumer));
                    return null;
                }
                byte[] topicBytes = new byte[topicLength];
                userData.get(topicBytes);
                String topic = new String(topicBytes);

                int partition;
                if ((partition = userData.getInt()) < 0) {
                    logger.warning(String.format("%s: Invalid partition", consumer));
                    return null;
                }

                double lagTime;
                if ((lagTime = userData.getDouble()) < 0) {
                    logger.warning(String.format("%s: Invalid lag time", consumer));
                    return null;
                }

                partitionLagTimes.add(new PartitionLagTime(new TopicPartition(topic, partition), lagTime));
            }

            return partitionLagTimes;
        }
        
        catch (BufferUnderflowException e) {
            logger.severe(String.format("%s: Unable to deserialize user data: %s", consumer, e.getMessage()));
            return null;
        }
    }
    
    /** Callback (on a consumer group member) when its assignment is received */
    @Override public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        // No-op, similar behavior with KafkaConsumer and ConsumerRebalanceListener
    }
    
    private static class PartitionLagTime extends Pair<TopicPartition,Double>
                                          implements Comparable<PartitionLagTime>
    {
        public PartitionLagTime(TopicPartition partition, double lagTime) {
            super(partition, lagTime);
        }

        public int compareTo(PartitionLagTime other) {
            return Double.compare(this.getSecond(), other.getSecond());
        }
    }
    
    private static class ConsumerLagTime extends Pair<String,Double>
                                         implements Comparable<ConsumerLagTime>
    {
        public ConsumerLagTime(String consumer, double lagTime) {
            super(consumer, lagTime);
        }

        public int compareTo(ConsumerLagTime other) {
            return Double.compare(this.getSecond(), other.getSecond());
        }
    }
    
    private static double calculateStdDev(Iterable<PartitionLagTime> partitionLagTimes) {
        double sum = 0;
        int count = 0;
        for (PartitionLagTime partitionLagTime : partitionLagTimes) {
            sum += partitionLagTime.getSecond();
            count++;
        }
        
        if (count == 0 || count == 1) return 0;
        final double mean = sum / count;
        
        double variance = 0;
        for (PartitionLagTime partitionLagTime : partitionLagTimes)
            variance += Math.pow(partitionLagTime.getSecond() - mean, 2);
        variance /= count;

        return Math.sqrt(variance);
    }
    
    private static <T> T randomSetEntry(Set<T> set) {
        if (set.isEmpty()) return null;
        return set.stream()
            .skip(ThreadLocalRandom.current().nextInt(set.size()))
            .findFirst().get();
    }
    
    private static <T> Set<T> setDifference(Set<T> set1, Set<T> set2) {
        HashSet<T> diff = new HashSet<>(set1);
        diff.removeAll(set2);
        return diff;
    }
}
