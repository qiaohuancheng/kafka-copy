package org.apache.kafka.clients.producer;

import org.apache.kafka.common.TopicPartition;

public final class RecordMetadata {
    private final long offset;
    private final TopicPartition topicPartition;
    
    private RecordMetadata(TopicPartition topicPartition, long offset) {
        super();
        this.offset = offset;
        this.topicPartition = topicPartition;
    }
    
    public RecordMetadata(TopicPartition topicPartition, long baseOffset, long relativeOffset) {
        this(topicPartition, baseOffset == -1 ? baseOffset : baseOffset + relativeOffset);
    }
    
    public long offset() {
        return this.offset;
    }
    
    public String topic() {
        return this.topicPartition.topic();
    }
    
    public int partition() {
        return this.topicPartition.partition();
    }

}
