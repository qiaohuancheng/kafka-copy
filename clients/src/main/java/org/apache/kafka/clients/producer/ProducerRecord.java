package org.apache.kafka.clients.producer;

public class ProducerRecord<K, V> {
    private final String topic;
    private final Integer partition;
    private final K key;
    private final V value;
    
    public ProducerRecord(String topic, Integer partition, K key, V value) {
        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be null");
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
    }
    
    public ProducerRecord(String topic, K key, V value) {
        this(topic, null, key, value);
    }
    
    public ProducerRecord(String topic, V value) {
        this(topic, null, value);
    }
    
    public String topic() {
        return topic;
    }
    
    public K key() {
        return key;
    }
    
    public V value() {
        return value;
    }
    
    public Integer partition() {
        return partition;
    }
    
    @Override
    public String toString() {
        String key = this.key == null ? "null" : this.key.toString();
        String value = this.value == null ? "null" : this.value.toString();
        return "ProducerRecord(topic=)" + topic + ", partition=" + partition + 
                ",key=" + key + ", value=" + value;
    }
}
