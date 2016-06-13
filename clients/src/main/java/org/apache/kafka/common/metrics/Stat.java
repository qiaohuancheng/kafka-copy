package org.apache.kafka.common.metrics;

public interface Stat {
    public void record(MetricConfig config, double value, long timeMs);
}
