package org.apache.kafka.common;

public interface Metric {
    public MetricName metricName();
    public double value();

}
