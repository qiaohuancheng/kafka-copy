package org.apache.kafka.clients.producer.internals;

import java.util.Set;

import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Metadata {
    private static final Logger log = LoggerFactory.getLogger(Metadata.class);
    private final long refreshBackoffMs;
    private final long metadataExpireMs;
    private int version;
    private long lastRefreshMs;
    private Cluster cluster;
    private boolean needUpdate;
    private final Set<String> topics;
    
    public Metadata() {
        this(100L, 60 * 60 * 1000L);
    }
    
    public Metadata(long refreshBackoffMs, long metadataExpireMs) {
        
        
    }
    

}
