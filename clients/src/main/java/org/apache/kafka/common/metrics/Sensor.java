package org.apache.kafka.common.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.CompoundStat.NamedMeasurable;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

public class Sensor {
    private final Metrics registry;
    private final String name;
    private final Sensor[] parents;
    private final List<Stat> stats;
    private final List<KafkaMetric> metrics;
    private final MetricConfig config;
    private final Time time;
    
    public Sensor(Metrics registry, String name, Sensor[] parents, MetricConfig config, Time time) {
        super();
        this.registry = registry;
        this.name = Utils.notNull(name);
        this.parents = parents == null ? new Sensor[0] : parents;
        this.metrics = new ArrayList<KafkaMetric>();
        this.stats = new ArrayList<Stat>();
        this.config = config;
        this.time = time;
        checkForest(new HashSet<Sensor>());
    }
    
    private void checkForest(Set<Sensor> sensors) {
        if (!sensors.add(this))
            throw new IllegalArgumentException("Circular dependency in sensors: " + name() 
                    + " is its own parent.");
        for (int i = 0; i < parents.length; i++) 
            parents[i].checkForest(sensors);
    }
    
    public String name() {
        return this.name;
    }
    
    public void record() {
        record(1.0);
    }
    
    public void record(double value) {
        record(value, time.millisenconds());
    }
    
    public void record(double value, long timeMs) {
        synchronized (this) {
            for (int i = 0; i < this.stats.size(); i++)
                this.stats.get(i).record(config, value, timeMs);
            checkQuotas(timeMs);
        }
        
        for (int i = 0; i < parents.length; i++)
            parents[i].record(value, timeMs);
    }
    
    private void checkQuotas(long timeMs) {
        for (int i = 0; i < this.metrics.size(); i++) {
            KafkaMetric metric = metrics.get(i);
            MetricConfig config = metric.config();
            if (config != null) {
                Quota quota = config.quota();
                if (quota != null) {
                    if (!quota.acceptable(metric.value(timeMs)))
                        throw new QuotaViolationException(metric.metricName() + " is in violation of "
                                + "its quota of  " + quota.bound());
                }
            }
        }
    }
    
    public void add(CompoundStat stat) {
        add(stat, null);
    }
    
    public synchronized void add(CompoundStat stat, MetricConfig config) {
        this.stats.add(Utils.notNull(stat));
        for (NamedMeasurable m: stat.stats()) {
            KafkaMetric metric = new KafkaMetric(this, m.name(), m.stat(), 
                    config == null ? this.config : config, time);
            this.registry.registerMetric(metric);
            this.metrics.add(metric);
        }
    }
    
    public void add(MetricName metricName, MeasurableStat stat) {
        add(metricName, stat, null);
    }
    
    public synchronized void add(MetricName metricName, MeasurableStat stat, MetricConfig config) {
        KafkaMetric metric = new KafkaMetric(new Object(), Utils.notNull(metricName), 
                Utils.notNull(stat), config == null ? this.config : config, time);
        this.registry.registerMetric(metric);
        this.metrics.add(metric);
        this.stats.add(stat);
    }
    
    synchronized List<KafkaMetric> metrics() {
        return Collections.unmodifiableList(this.metrics);
    }
}
