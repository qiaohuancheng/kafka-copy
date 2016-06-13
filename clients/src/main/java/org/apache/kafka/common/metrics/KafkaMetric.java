package org.apache.kafka.common.metrics;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.utils.Time;

public class KafkaMetric implements Metric {
    private MetricName metricName;
    private final Object lock;
    private final Time time;
    private final Measurable measurable;
    private MetricConfig config;
    
    KafkaMetric(Object lock, MetricName name, Measurable measurable , MetricConfig config, 
            Time time) {
        super();
        this.metricName = metricName;
        this.lock = lock;
        this.measurable = measurable;
        this.config = config;
        this.time = time;
    }
    
    MetricConfig config(){
        return this.config;
    }
    
    @Override
    public MetricName metricName() {
        return this.metricName;
    }

    @Override
    public double value() {
        synchronized (this.lock) {
            return value(time.millisenconds());
        }
    }
    
    double value(long timeMs) {
        return this.measurable.measure(config, timeMs);
    }
    
    public void config(MetricConfig config) {
        synchronized (this.lock) {
            this.config = config;
        }
    }
}
