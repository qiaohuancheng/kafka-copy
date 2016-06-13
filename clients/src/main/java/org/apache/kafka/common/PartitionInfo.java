package org.apache.kafka.common;


public class PartitionInfo {
    private final String topic;
    private final int partition;
    private final Node leader;
    private final Node[] replicas;
    private final Node[] inSyncReplicas;
    
    public PartitionInfo(String topic, int partition, Node leader, Node[] replicas, Node[] inSyncReplicas) {
        this.topic = topic;
        this.partition = partition;
        this.leader = leader;
        this.replicas = replicas;
        this.inSyncReplicas = inSyncReplicas;
    }
    
    public String topic() {
        return topic;
    }
    
    public int partition() {
        return partition;
    }
    
    public Node leader() {
        return leader;
    }
    
    public Node[] replicas() {
        return replicas;
    }
    
    public Node[] inSyncReplicas() {
        return inSyncReplicas;
    }
    
    @Override
    public String toString() {
        return String.format("Partition(topic = %s, partition = %d, leader = %d, replicas = %s, isr = %s)",
                topic,
                partition,
                leader.id(),
                fmtNodeIds(replicas),
                fmtNodeIds(inSyncReplicas));
    }
    
    private String fmtNodeIds(Node[] nodes) {
        StringBuilder b = new StringBuilder("[");
        for (int i = 0; i < nodes.length - 1; i++) {
            b.append(Integer.toString(nodes[i].id()));
            b.append(',');
        }
        if (nodes.length > 0) {
            b.append(Integer.toString(nodes[nodes.length - 1].id()));
            b.append(',');
        }
        b.append("]");
        return b.toString();
    }

}
