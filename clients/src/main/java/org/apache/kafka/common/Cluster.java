package org.apache.kafka.common;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.utils.Utils;

public class Cluster {
    private final List<Node> nodes;
    private final Map<TopicPartition, PartitionInfo> partitionsByTopicPartition;
    private final Map<String, List<PartitionInfo>> partitionsByTopic;
    private final Map<String, List<PartitionInfo>> avaliablePartitionsByTopic;
    private final Map<Integer, List<PartitionInfo>> partitionsByNode;
    
    public Cluster(Collection<Node> nodes, Collection<PartitionInfo> partitions) {
        List<Node> copy = new ArrayList<Node>(nodes);
        Collections.shuffle(copy);
        this.nodes = Collections.unmodifiableList(copy);
        
        this.partitionsByTopicPartition = new HashMap<TopicPartition, PartitionInfo>(partitions.size());
        for (PartitionInfo p : partitions)
            this.partitionsByTopicPartition.put(new TopicPartition(p.topic(), p.partition()), p);
        
        HashMap<String, List<PartitionInfo>> partsForTopic = new HashMap<String, List<PartitionInfo>>();
        HashMap<Integer, List<PartitionInfo>> partsForNode = new HashMap<Integer, List<PartitionInfo>>();
        for(Node n: this.nodes) {
            partsForNode.put(n.id(), new ArrayList<PartitionInfo>());
        }
        for (PartitionInfo p: partitions) {
            if (!partsForTopic.containsKey(p.topic()))
                partsForTopic.put(p.topic(), new ArrayList<PartitionInfo>());
            List<PartitionInfo> psTopic = partsForTopic.get(p.topic());
            psTopic.add(p);
            
            if (p.leader() != null) {
                List<PartitionInfo> psNode = Utils.notNull(partsForNode.get(p.leader().id()));
                psNode.add(p);
            }
        }
        
        this.partitionsByTopic = new HashMap<String, List<PartitionInfo>>(partsForTopic.size());
        this.avaliablePartitionsByTopic = new HashMap<String, List<PartitionInfo>>(partsForTopic.size());
        for(Map.Entry<String, List<PartitionInfo>> entry : partsForTopic.entrySet()) {
            String topic = entry.getKey();
            List<PartitionInfo> partitionList = entry.getValue();
            this.partitionsByTopic.put(topic, Collections.unmodifiableList(partitionList));
            List<PartitionInfo> availablePartitions = new ArrayList<PartitionInfo>();
            for (PartitionInfo part : partitionList) {
                if (part.leader() != null)
                    availablePartitions.add(part);
            }
            this.avaliablePartitionsByTopic.put(topic, Collections.unmodifiableList(availablePartitions));
        }
        this.partitionsByNode = new HashMap<Integer, List<PartitionInfo>>(partitionsByNode.size());
        for (Map.Entry<Integer, List<PartitionInfo>> entry : partsForNode.entrySet())
            this.partitionsByNode.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
    }
    
    public static Cluster empty() {
        return new Cluster(new ArrayList<Node>(0), new ArrayList<PartitionInfo>(0));
    }
    
    public static Cluster bootstrap(List<InetSocketAddress> addresses) {
        List<Node> nodes = new ArrayList<Node>();
        int nodeId = -1;
        for (InetSocketAddress address : addresses) 
            nodes.add(new Node(nodeId--, address.getHostName(), address.getPort()));
        return new Cluster(nodes, new ArrayList<PartitionInfo>(0));
    }
    
    public List<Node> nodes() {
        return this.nodes;
    }
    
    public Node leaderFor(TopicPartition topicPartition) {
        PartitionInfo info = partitionsByTopicPartition.get(topicPartition);
        if (info == null) 
            return null;
        else 
            return info.leader();
    }
    
    public PartitionInfo partition(TopicPartition topicPartition) {
        return partitionsByTopicPartition.get(topicPartition);
    }
    
    public List<PartitionInfo> partitiInfosForTopic(String topic) {
        return this.partitionsByTopic.get(topic);
    }
    
    public List<PartitionInfo> availablePartitionForTopic(String topic) {
        return this.avaliablePartitionsByTopic.get(topic);
    }
    
    public List<PartitionInfo> partitionsForNode(int nodeId) {
        return this.partitionsByNode.get(nodeId);
    }
    
    public Set<String> topics() {
        return this.partitionsByTopic.keySet();
    }
    
    @Override
    public String toString() {
        return "Cluster(nodes = " + this.nodes + ", partitions = " + this.partitionsByTopicPartition.values() + ")";
    }
}
