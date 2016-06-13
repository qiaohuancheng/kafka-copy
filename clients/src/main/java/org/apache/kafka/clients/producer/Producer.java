package org.apache.kafka.clients.producer;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import javax.security.auth.callback.Callback;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;

public interface Producer<K,V> extends Closeable{
    public Future<RecordMetadata> send(ProducerRecord<K, V> record);
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback);
    public List<PartitionInfo> partitionFor(String topic);
    public Map<MetricName, ? extends Metric> metrics ();
    public void close();

}
