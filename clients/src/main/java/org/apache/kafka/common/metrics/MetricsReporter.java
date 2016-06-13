package org.apache.kafka.common.metrics;

import java.util.List;

import org.apache.kafka.common.Configurable;

public interface MetricsReporter extends Configurable{
    public void init(List<KafkaMetric> metrics);
    
    public void metricChange(KafkaMetric metric);
    
    public void close();
}
