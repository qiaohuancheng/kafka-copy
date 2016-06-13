package org.apache.kafka.common.metrics;

import java.util.concurrent.TimeUnit;

public class MetricConfig {
    private Quota quota;
    private int samples;
    private long eventWindow;
    private long timeWindowMs;
    private TimeUnit unit;
    
    public MetricConfig() {
        super();
        this.quota = null;
        this.samples = 2;
        this.eventWindow = Long.MAX_VALUE;
        this.timeWindowMs = TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);
        this.unit = TimeUnit.SECONDS;
    }
    
    public Quota quota() {
        return this.quota;
    }
}
