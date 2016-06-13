package org.apache.kafka.clients.producer.internals;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

public class Partitioner {
    private final AtomicInteger counter = new AtomicInteger(new Random().nextInt());
    
    public int partition(ProducerRecord<byte[], Byte[]> record, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitiInfosForTopic(record.topic());
        int numPartitions = partitions.size();
        if (record.partition() != null) {
            if (record.partition() < 0 || record.partition() >= numPartitions)
                throw new IllegalArgumentException("Invalid partition given with record: " + record.partition()
                        + " is not in the range [0..." + numPartitions + "].");
            return record.partition();
        } else if (record.key() == null) {
            int nextValue = counter.getAndIncrement();
            List<PartitionInfo> availablePartitions = cluster.availablePartitionForTopic(record.topic());
            if (availablePartitions.size() > 0) {
                int part = Utils.abs(nextValue) % availablePartitions.size();
                return availablePartitions.get(part).partition();
            } else {
                return Utils.abs(nextValue) % numPartitions;
            }
        } else {
            return Utils.abs(Utils.murmur2(record.key())) % numPartitions;
        }
    }
}
