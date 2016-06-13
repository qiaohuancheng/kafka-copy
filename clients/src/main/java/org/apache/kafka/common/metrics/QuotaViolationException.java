package org.apache.kafka.common.metrics;

import org.apache.kafka.common.KafkaException;


public class QuotaViolationException  extends KafkaException{
    private static final long serialVersionUID = 1L;
    
    public QuotaViolationException(String m) {
        super(m);
    }
}
