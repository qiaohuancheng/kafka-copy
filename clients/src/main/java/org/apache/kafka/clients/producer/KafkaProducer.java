package org.apache.kafka.clients.producer;

import java.sql.Time;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.callback.Callback;

import org.apache.kafka.clients.producer.internals.Partitioner;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

public class KafkaProducer<K,V> implements Producer<K,V> {
    
    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);
    
    private final Partitioner partitioner;
    private final int maxRequestSize;
    private final long metadataFetchTimeoutMs;
    private long totalMemorySize;
    private final Metadata metadata;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final Metrics metrics;
    private final Thread ioThread;
    private final CompressionType compressionType;
    private final Sensor errors;
    private final Time time;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final ProducerConfig producerConfig;
    private static final AtomicInteger producerAutoId = new AtomicInteger(1);
    
    

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record,
            Callback callback) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<PartitionInfo> partitionFor(String topic) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }

}
