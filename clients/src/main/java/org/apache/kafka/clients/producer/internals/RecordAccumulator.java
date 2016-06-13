package org.apache.kafka.clients.producer.internals;

import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.JavaNioAccess.BufferPool;

public class RecordAccumulator {
    private static final Logger log = LoggerFactory.getLogger(RecordAccumulator.class);
    
    private volatile boolean closed;
    private int drainIndex;
    private final int batchSize;
    private final long lingerMs;
    private final long retryBackoffMs;
    private final BufferPool free;
    private final Time time;
    private final ConcurrentMap<TopicPartition, Deque<RecordBatch>> batches;
    
    public RecordAccumulator(int batchSize, long totalSize, long lingerMs,
            long retryBackoffMs, boolean blockOnBufferFull, Metrics metrics,
            Time time, Map<String, String> metricsTags) {
        this.drainIndex = 0;
        this.closed = false;
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.retryBackoffMs = retryBackoffMs;
        this.batches = new CopyOnWriteMap<TopicPartition, Deque<RecordBatch>> ();
        String metricGrpName = "producer-metrics";
        this.free = new BufferBool(totalSize, batchSize, blockOnBufferFull, metrics,
                time, metricGrpName, metricsTags);
        this.time = time;
        registerMetrics(metrics, metricGrpName, metricsTags);
    }
    
    private void registerMetrics(Metrics metrics, String metricsGrpName,
            Map<String, String> metricTags) {
        MetricName metricName = new MetricName("waiting-threads", metricsGrpName,
                "The number user threads blocked waiting for buffer memory to enqueue their records",
                metricTags);
        metrics.addMetrics(metricName, new Measurable() {
            @Override
            public double measure(MetricConfig config, long now) {
                public double measure(MetricConfig config, long now) {
                    return free.queued();
                }
            }
        });
    }

}
