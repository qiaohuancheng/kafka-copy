package org.apache.kafka.common;

import com.sun.javafx.collections.MappingChange.Map;

public interface Configurable {
    public void configure(Map<String, ?> configs);
}
