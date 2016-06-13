package org.apache.kafka.common;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.utils.Utils;


public class MetricName {
    private final String name;
    private final String group;
    private final String description;
    private Map<String, String> tags;
    private int hash = 0;
    
    public MetricName(String name, String group, String description, Map<String, String> tags) {
        this.name = Utils.notNull(name);
        this.group = Utils.notNull(group);
        this.description = Utils.notNull(description);
        this.tags = Utils.notNull(tags);
    }
    
    public MetricName(String name, String group, String description, String... keyValue) {
        this(name, group, description, getTags(keyValue));
    }
    
    private static Map<String, String> getTags(String... keyValue) {
        if((keyValue.length % 2) != 0)
            throw new IllegalArgumentException("keyValue needs to be specified in paris");
        Map<String, String> tags = new HashMap<String, String>();
        
        for (int i=0; i<(keyValue.length / 2); i++)
            tags.put(keyValue[i], keyValue[i+1]);
        return tags;
    }
    

}
