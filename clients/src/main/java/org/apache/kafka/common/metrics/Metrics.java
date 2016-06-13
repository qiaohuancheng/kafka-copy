package org.apache.kafka.common.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

public class Metrics {
    private final MetricConfig config;
    private final ConcurrentMap<MetricName, KafkaMetric> metrics;
    private final ConcurrentMap<String, Sensor> sensors;
    private final List<MetricsReporter> reporters;
    private final Time time;
    
    public Metrics() {
        this(new MetricConfig());
    }
    
    public Metrics(Time time) {
        this(new MetricConfig(), new ArrayList<MetricsReporter>(0), time);
    }
    
    public Metrics(MetricConfig defaultConfig) {
        this(defaultConfig, new ArrayList<MetricsReporter>(0), new SystemTime());
    }
    
    public Metrics(MetricConfig defaultConfig, List<MetricsReporter> reporters, Time time) {
        this.config = defaultConfig;
        this.sensors = new CopyOnWriteMap<String, Sensor>();
        this.metrics = new CopyOnWriteMap<MetricName, KafkaMetric>();
        this.reporters = Utils.notNull(reporters);
        this.time = time;
        for (MetricsReporter reporter: reporters)
            reporter.init(new ArrayList<KafkaMetric>());
    }
    
    synchronized void registerMetric(KafkaMetric metric) {
        MetricName metricName = metric.metricName();
        if (this.metrics.containsKey(metricName))
            throw new IllegalArgumentException("A metric named '" + metricName + "' already "
                    + "exists, can't register another one.");
        this.metrics.put(metricName, metric);
        for (MetricsReporter reporter : reporters)
            reporter.metricChange(metric);
    }
    
    public Sensor getSensor(String name) {
        return this.sensors.get(Utils.notNull(name));
    }
    
    public Sensor sensor(String name) {
        return sensor(name, null, (Sensor[]) null);
    }
    
    public Sensor sensor(String name, Sensor... parents) {
        return sensor(name, null, parents);
    }
    
    public synchronized Sensor sensor(String name, MetricConfig config, Sensor... parents) {
        Sensor s = getSensor(name);
        if (s == null) {
            s = new Sensor(this, name, parents, config == null ? this.config : config, time);
            this.sensors.put(name, s);
        }
        return s;
    }
    
    public void addMetrics(MetricName metricName, Measurable measurable) {
        addMetrics(metricName, null, measurable);
    }
    
    public synchronized void addMetrics(MetricName metricName, MetricConfig config,
            Measurable measurable) {
        KafkaMetric m = new KafkaMetric(new Object(), Utils.notNull(metricName),
                Utils.notNull(measurable), config == null ? this.config : config, time);
        registerMetric(m);
    }
    
    public synchronized void addReporter(MetricsReporter reporter) {
        Utils.notNull(reporter).init(new ArrayList<KafkaMetric>(metrics.values()));
        this.reporters.add(reporter);
    }
    
    public Map<MetricName, KafkaMetric> metrics() {
        return this.metrics;
    }
    
    public void close() {
        for (MetricsReporter reporter : this.reporters)
            reporter.close();
    }
}
