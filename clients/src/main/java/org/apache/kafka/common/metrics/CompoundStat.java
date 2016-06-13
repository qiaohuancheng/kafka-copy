package org.apache.kafka.common.metrics;

import java.util.List;

import org.apache.kafka.common.MetricName;

public interface CompoundStat extends Stat {
    public List<NamedMeasurable> stats();
    public class NamedMeasurable {
        private final MetricName name;
        private final Measurable stat;
        
        public NamedMeasurable(MetricName name, Measurable stat) {
            super();
            this.name = name;
            this.stat = stat;
        }
        
        public MetricName name() {
            return name;
        }
        
        public Measurable stat() {
            return stat;
        }
    }
}
