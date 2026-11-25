package cl.duoc.vidasalud.kafka.admin.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.util.Map;

public class TopicRequest {

    @NotBlank
    private String name;

    @Min(1)
    private int partitions = 3;

    @Min(1)
    private short replicationFactor = 3;

    private Map<String, String> configs;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public int getPartitions() { return partitions; }
    public void setPartitions(int partitions) { this.partitions = partitions; }
    public short getReplicationFactor() { return replicationFactor; }
    public void setReplicationFactor(short replicationFactor) { this.replicationFactor = replicationFactor; }
    public Map<String, String> getConfigs() { return configs; }
    public void setConfigs(Map<String, String> configs) { this.configs = configs; }
}