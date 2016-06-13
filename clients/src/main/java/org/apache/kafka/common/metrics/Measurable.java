package org.apache.kafka.common.metrics;

public interface Measurable {
    public double measure(MetricConfig config, long now);
}
