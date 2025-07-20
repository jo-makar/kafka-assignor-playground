#!/usr/bin/env python3
# Simulate partition assignor algorithms

import random
import statistics

num_intervals = 100
num_consumers, consume_rate = 6, (2000, 3000)
num_partitions, produce_rate, initial_lag = 40, (100, 500), (0, 5000)

partitions = {
    idx: { 'lag': random.randint(*initial_lag), 'produce_rate': random.randint(*produce_rate) }
    for idx in range(num_partitions)
}

consumers = {
    idx: { 'consume_rate': random.randint(*consume_rate), 'partitions': []  }
    for idx in range(num_consumers)
}

for part_num in partitions.keys():
    consumers[random.randint(0, num_consumers-1)]['partitions'].append(part_num)

for step in range(num_intervals):
    #
    # Update the partition lags
    # (with consume rate evenly distributed)
    #

    for partition in partitions.values():
        partition['lag'] += partition['produce_rate']

    for consumer in consumers.values():
        consume_count = consumer['consume_rate']
        for idx, part_num in enumerate(consumer['partitions']):
            part_consume_count = min(
                partitions[part_num]['lag'],
                consume_count // (len(consumer['partitions']) - idx)
            )
            partitions[part_num]['lag'] -= part_consume_count
            consume_count -= part_consume_count

    #
    # Display the current cluster state
    #

    display_buffer = ''
    consumer_lags = []
    for idx, consumer in consumers.items():
        lag = ' '.join([ f'p{part_num}:{partitions[part_num]["lag"]}'
                         for part_num in consumer['partitions'] ])
        total_lag = sum([partitions[part_num]['lag'] for part_num in consumer['partitions']])
        consumer_lags.append(total_lag)
        display_buffer += f'c{idx}: {lag} => {total_lag}\n'

    print(f'== step {step}, stddev {statistics.stdev(consumer_lags):.1f} ==')
    print(display_buffer[:-1])

    if False:
        '''
        Lag count based balancing
        (with periodically triggered rebalancing)

        Algorithm outline:
        1. Sort the consumers by lag count (high to low)
           a. Not implemented: If the difference between the highest and median value is less than q%, abort
              i. Do not use the lowest value since anticipating inactive partitions
        2. Segment the consumers into the top n% and the remaining consumers
        3. For the consumers in the first segment, assign their m% least lagged partitions
           randomly into consumers of the second segment (bottom (100-n)%)
           a. Not assigning to the least-lagged consumers to avoid ping-ponging partitions
           b. Not implemented: Do not assign more than (total partitions / consumer count) * x to any consumer
              i. To avoid overloading consumers with inactive / low-lagged partitions
        '''

        consumer_top_lagged_pct = 0.15
        partition_least_lagged_pct = 0.33

        consumers_sorted = sorted(list(consumers.items()),
            key=lambda c: sum([partitions[n]['lag'] for n in c[1]['partitions']]),
            reverse=True
        )
        split_idx = max(1, int(len(consumers_sorted) * consumer_top_lagged_pct))
        consumers_lagged = consumers_sorted[:split_idx]
        consumers_remaining = consumers_sorted[split_idx:]

        for idx, consumer in consumers_lagged:
            if len(consumer['partitions']) < 2: continue

            partitions_sorted = sorted(
                consumer['partitions'],
                key=lambda p: partitions[p]['lag'],
                reverse=True
            )
            split_idx = max(1, int(len(partitions_sorted) * partition_least_lagged_pct))
            partitions_lagged = partitions_sorted[:split_idx]
            partitions_remaining = partitions_sorted[split_idx:]

            for part_num in partitions_lagged:
                consumer_dest = random.choice(consumers_remaining)
                print(f'moving c{idx} p{part_num} to c{consumer_dest[0]}')
                consumer['partitions'].remove(part_num)
                consumer_dest[1]['partitions'].append(part_num)

    else:
        '''
        Lag time based balancing
        (with periodically triggered rebalancing)

        Algorithm outline:
        1. Sort the consumers by lag time (high to low)
           a. Let lag time = lag count / consume rate (records/sec)
           b. Since is no partition consume rate metric, use the topic consume rate / partition count
              i. Ref: https://kafka.apache.org/28/generated/consumer_metrics.html
        (The following is identical to lag count based balancing algorithm above)
           c. Not implemented: If the difference between the highest and median value is less than q%, abort
              i. Do not use the lowest value since anticipating inactive partitions
        2. Segment the consumers into the top n% and the remaining consumers
        3. For the consumers in the first segment, assign their m% least lagged partitions
           randomly into consumers of the second segment (bottom (100-n)%)
           a. Not assigning to the least-lagged consumers to avoid ping-ponging partitions
           b. Not implemented: Do not assign more than (total partitions / consumer count) * x to any consumer
              i. To avoid overloading consumers with inactive / low-lagged partitions
        '''

        consumer_top_lagged_pct = 0.15
        partition_least_lagged_pct = 0.33

        # In practice the lag time calculation won't be so accurate
        def lag_time(consumer, part_num):
            consume_rate_per_partition = consumer['consume_rate'] / len(consumer['partitions'])
            return partitions[part_num]['lag'] / consume_rate_per_partition

        consumers_sorted = sorted(list(consumers.items()),
            key=lambda c: sum([lag_time(c[1], n) for n in c[1]['partitions']]),
            reverse=True
        )
        # The remainder is identical to the lag count based balancing algorithm above
        split_idx = max(1, int(len(consumers_sorted) * consumer_top_lagged_pct))
        consumers_lagged = consumers_sorted[:split_idx]
        consumers_remaining = consumers_sorted[split_idx:]

        for idx, consumer in consumers_lagged:
            if len(consumer['partitions']) < 2: continue

            partitions_sorted = sorted(
                consumer['partitions'],
                key=lambda p: partitions[p]['lag'],
                reverse=True
            )
            split_idx = max(1, int(len(partitions_sorted) * partition_least_lagged_pct))
            partitions_lagged = partitions_sorted[:split_idx]
            partitions_remaining = partitions_sorted[split_idx:]

            for part_num in partitions_lagged:
                consumer_dest = random.choice(consumers_remaining)
                print(f'moving c{idx} p{part_num} to c{consumer_dest[0]}')
                consumer['partitions'].remove(part_num)
                consumer_dest[1]['partitions'].append(part_num)

    print()
