package cl.duoc.vidasalud.kafka.admin.dto;

import java.util.Map;

public class TopicInfo {

    private String name;
    private int partitions;
    private int replicationFactor;
    private Map<String, String> configs;

    public TopicInfo() {}

    public TopicInfo(String name, int partitions, int replicationFactor, Map<String, String> configs) {
        this.name = name;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
        this.configs = configs;
    }

    public String getName() { return name; }
    public int getPartitions() { return partitions; }
    public int getReplicationFactor() { return replicationFactor; }
    public Map<String, String> getConfigs() { return configs; }
}