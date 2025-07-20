package org.example;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Logger;
import java.util.Map;
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
public class LagTimePartitionAssignor implements ConsumerPartitionAssignor {
    private static final int REBALANCE_PERIOD_SECS = 60; // TODO 900 is more appropriate
    private static final int CONSUMER_TOP_LAGGED_PCT = 15;
    private static final int PARTITION_NON_LAGGED_PCT = 15;
    private static final float PARTITION_OVERLOAD_FACTOR = 1.67f;

    private static Logger logger = Logger.getLogger(LagTimePartitionAssignor.class.getName());

    // Track the last assignment to maintain assignment stickiness
    private Map<TopicPartition,String> lastAssignmentMap = new HashMap<>();
    private Instant lastRebalance = null;

    @Override public String name() {
        return "lag-time";
    }

    @Override public short version() {
        return 1;
    }

    @Override public List<RebalanceProtocol> supportedProtocols() {
        // TODO What are the COOPERATIVE requirements?
        //      Ref: consumer.internals.ConsumerCoordinator.validateCooperativeAssignment
        return List.of(RebalanceProtocol.EAGER);
    }

    /** Assign partitions (by the consumer group leader and triggered by a rebalance) */
    @Override public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
        // TODO Ensure assignment cpu time and memory usage is acceptable for at least 50k partitions
        //      Should test for as high as 200k partitions as a safety factor for future growth

        Map<String,Subscription> subscriptionMap = groupSubscription.groupSubscription();

        Set<String> consumerIds = subscriptionMap.keySet();

        Set<TopicPartition> partitions = subscriptionMap.values().stream()
            .flatMap(subscription -> subscription.topics().stream())
            .flatMap(topic -> metadata.availablePartitionsForTopic(topic).stream())
            .map(partInfo -> new TopicPartition(partInfo.topic(), partInfo.partition()))
            .collect(Collectors.toSet());

        //
        // Initialize the assignment mapping based on the last execution's assignment mapping
        // (or randomly if this is the initial assignment by this group leader)
        //

        Map<TopicPartition,String> assignmentMap = new HashMap<>();
        for (TopicPartition partition : partitions) {
            String lastConsumerId = lastAssignmentMap.get(partition);
            if (lastConsumerId != null && consumerIds.contains(lastConsumerId)) {
                assignmentMap.put(partition, lastConsumerId);
            } else {
                // Newly created partition or last consumer for this partition was removed,
                // handled by randomly assigning to one of the currently available consumers
                String consumerId = consumerIds.stream()
                    .skip(ThreadLocalRandom.current().nextInt(consumerIds.size()))
                    .findFirst().get();
                assignmentMap.put(partition, consumerId);

                logger.info(String.format(
                    "Initialization: Assigned partition %s to consumer %s",
                    partition.toString(), consumerId
                ));
            }
        }

        //
        // Ensure every consumer is assigned at least one partition
        //

        Set<String> usedConsumerIds = assignmentMap.values().stream().collect(Collectors.toSet());
        Set<String> unusedConsumerIds = consumerIds.stream()
            .filter(consumerId -> !usedConsumerIds.contains(consumerId)).collect(Collectors.toSet());

        for (String unusedConsumerId : unusedConsumerIds) {
            TopicPartition partition = partitions.stream()
                .skip(ThreadLocalRandom.current().nextInt(consumerIds.size()))
                .findFirst().get();
            assignmentMap.put(partition, unusedConsumerId);

            logger.info(String.format(
                "Unused consumer: Assigned partition %s to consumer %s",
                partition.toString(), unusedConsumerId
            ));
        }

        if (lastRebalance != null &&
            Duration.between(lastRebalance, Instant.now()).toSeconds() > REBALANCE_PERIOD_SECS) {

            //
            // Sort the partitions by lag time
            //

            // TODO This approach is inaccurate because it assumes the most lagged partitions determine the most lagged consumers,
            //      whereas it should account for all partition lag to properly determine the most lagged consumers.
            //      The algorithm is fixed in CooperativeLagTimePartitionAssignor but retained here for reference.

            FixedSizeMaxHeap<PartitionLagTime> partitionLagTimes = new FixedSizeMaxHeap<>(
                (int)Math.max(1, consumerIds.size() * CONSUMER_TOP_LAGGED_PCT / 100)
            );
            Set<String> consumerIdsWithoutUserData = new HashSet<>(consumerIds);

            for (Map.Entry<String,Subscription> entry : subscriptionMap.entrySet()) {
                String consumerId = entry.getKey();
                ByteBuffer userData = entry.getValue().userData();
                if (userData == null) continue;

                try {
                    if (userData.getShort() != version()) {
                        logger.warning(String.format("%s: User data version mismatch", consumerId));
                        continue;
                    }

                    int entryCount;
                    if ((entryCount = userData.getInt()) < 0) {
                        logger.warning(String.format("%s: Invalid entry count", consumerId));
                        continue;
                    }

                    for (int i = 0; i < entryCount; i++) {
                        int topicLength;
                        if ((topicLength = userData.getInt()) < 1) {
                            logger.warning(String.format("%s: Invalid topic length", consumerId));
                            break;
                        }
                        byte[] topicBytes = new byte[topicLength];
                        userData.get(topicBytes);
                        String topic = new String(topicBytes);

                        int partition;
                        if ((partition = userData.getInt()) < 0) {
                            logger.warning(String.format("%s: Invalid partition", consumerId));
                            break;
                        }

                        double lagTime;
                        if ((lagTime = userData.getDouble()) < 0) {
                            logger.warning(String.format("%s: Invalid lag time", consumerId));
                            break;
                        }

                        partitionLagTimes.add(new PartitionLagTime(
                            new TopicPartition(topic, partition), lagTime
                        ));
                    }

                    consumerIdsWithoutUserData.remove(consumerId);
                }

                catch (BufferUnderflowException e) {
                    logger.severe(String.format(
                        "%s: Unable to deserialize user data: %s",
                        consumerId, e.getMessage()
                    ));
                }
            }

            if (!consumerIdsWithoutUserData.isEmpty())
                logger.warning(
                    "Consumers without valid user data: " +
                    consumerIdsWithoutUserData.toString()
                );

            // TODO If the difference between the highest and median value is less than q%, do nothing
            //      Do not use the lowest value since anticipating inactive partitions

            final int maxPartitionsPerConsumer =
                (int)(partitionLagTimes.entryCount() / consumerIds.size() * PARTITION_OVERLOAD_FACTOR);

            Map<String,Long> consumerPartitionCounts = assignmentMap.entrySet().stream()
                .collect(Collectors.groupingBy(
                    Map.Entry::getValue,
                    Collectors.mapping(partition -> partition, Collectors.counting())
                ));

            Set<String> laggedConsumerIds = StreamSupport.stream(
                    Spliterators.spliterator(
                        partitionLagTimes.iterator(),
                        partitionLagTimes.size(),
                        Spliterator.ORDERED | Spliterator.SORTED | Spliterator.DISTINCT
                    ),
                    false
                ) // Stream<PartitionLagTime>
                .map(pair -> assignmentMap.get(pair.getFirst()))
                .collect(Collectors.toSet());

            Set<String> nonLaggedConsumerIds = consumerIds.stream()
                .filter(consumerId -> !laggedConsumerIds.contains(consumerId) &&
                                      consumerPartitionCounts.get(consumerId) < maxPartitionsPerConsumer)
                .collect(Collectors.toSet());

            Map<String,Set<TopicPartition>> laggedConsumerAllPartitions =
                assignmentMap.entrySet().stream()
                    .filter(entry -> laggedConsumerIds.contains(entry.getValue()))
                    .collect(Collectors.groupingBy(
                        entry -> entry.getValue(),
                        Collectors.mapping(entry -> entry.getKey(), Collectors.toSet())
                    ));

            Map<String,Set<TopicPartition>> laggedConsumerLaggedPartitions =
                StreamSupport.stream(
                    Spliterators.spliterator(
                        partitionLagTimes.iterator(),
                        partitionLagTimes.size(),
                        Spliterator.ORDERED | Spliterator.SORTED | Spliterator.DISTINCT
                    ),
                    false
                ) // Stream<PartitionLagTime>
                .map(Pair::getFirst)
                .collect(Collectors.groupingBy(
                    partition -> assignmentMap.get(partition),
                    Collectors.mapping(partition -> partition, Collectors.toSet())
                ));

            for (String laggedConsumerId : laggedConsumerIds) {
                // TODO Originally intended to transfer the m% least lagged partitions,
                //      but this would require another per-partition data structure.
                //      For now simply transfer m% non-lagged partitions instead.

                Set<TopicPartition> allPartitions = laggedConsumerAllPartitions.get(laggedConsumerId);
                Set<TopicPartition> transferablePartitions = new HashSet<>(allPartitions);
                transferablePartitions.removeAll(laggedConsumerLaggedPartitions.get(laggedConsumerId));

                int transferCount = Math.min(
                    transferablePartitions.size(),
                    allPartitions.size() * PARTITION_NON_LAGGED_PCT / 100
                );
                for (int i = 0; i < transferCount && !nonLaggedConsumerIds.isEmpty(); i++) {
                    TopicPartition partition = transferablePartitions.stream()
                        .skip(ThreadLocalRandom.current().nextInt(transferablePartitions.size()))
                        .findFirst().get();
                    transferablePartitions.remove(partition);

                    String nonLaggedConsumerId = nonLaggedConsumerIds.stream()
                        .skip(ThreadLocalRandom.current().nextInt(nonLaggedConsumerIds.size()))
                        .findFirst().get();

                    assignmentMap.put(partition, nonLaggedConsumerId);

                    //consumerPartitionCounts.merge(laggedConsumerId, -1L, Long::sum);
                    if (consumerPartitionCounts.merge(nonLaggedConsumerId, 1L, Long::sum)
                            >= maxPartitionsPerConsumer)
                        nonLaggedConsumerIds.remove(nonLaggedConsumerId);

                    //laggedConsumerAllPartitions.get(laggedConsumerId).remove(partition);

                    logger.info(String.format(
                        "Lag time: Assigned partition %s to consumer %s (from %s)",
                        partition.toString(), nonLaggedConsumerId, laggedConsumerId
                    ));
                }
            }
        }

        lastAssignmentMap = assignmentMap;
        lastRebalance = Instant.now();

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

        return new GroupAssignment(invertedAssignmentMap);
    }

    /** Generate data (by group members) to be used by the group leader for partition assignments */
    @Override public ByteBuffer subscriptionUserData(Set<String> topics) {
        synchronized (App.partitionLagTimes) {
            if (App.partitionLagTimes.isEmpty())
                return null;

            Set<String> seenTopics = App.partitionLagTimes.keySet().stream()
                .map(topicPartition -> topicPartition.topic())
                .collect(Collectors.toSet());
            if (!seenTopics.equals(topics)) {
                HashSet<String> unseenTopics = new HashSet<>(topics);
                unseenTopics.removeAll(seenTopics);
                logger.warning("No partition lag time for topics " + unseenTopics.toString());
            }

            final int bufferLength = Short.BYTES +              // Version
                Integer.BYTES +                                 // Entry count
                App.partitionLagTimes.keySet().stream()
                    .map(key -> Integer.BYTES +                 // Topic string length
                                key.topic().getBytes().length + // Topic string
                                Integer.BYTES +                 // Partition
                                Double.BYTES)                   // Lag time
                    .reduce(0, Integer::sum);
            ByteBuffer buffer = ByteBuffer.allocate(bufferLength);

            buffer.putShort(version());
            buffer.putInt(App.partitionLagTimes.size());

            for (Map.Entry<TopicPartition,Double> entry : App.partitionLagTimes.entrySet()) {
                byte[] topicBytes = entry.getKey().topic().getBytes();
                buffer.putInt(topicBytes.length);
                buffer.put(topicBytes);
                buffer.putInt(entry.getKey().partition());
                buffer.putDouble(entry.getValue());
            }

            buffer.flip();
            return buffer;
        }
    }

    /** Callback (on a group member) when its assignment is received */
    @Override public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        // No-op, similar behavior possible with KafkaConsumer and ConsumerRebalanceListener
    }

    private class PartitionLagTime extends Pair<TopicPartition,Double>
                                   implements Comparable<PartitionLagTime>
    {
        public PartitionLagTime(TopicPartition partition, double lagTime) {
            super(partition, lagTime);
        }

        public int compareTo(PartitionLagTime other) {
            return Double.compare(this.getSecond(), other.getSecond());
        }
    }
}
