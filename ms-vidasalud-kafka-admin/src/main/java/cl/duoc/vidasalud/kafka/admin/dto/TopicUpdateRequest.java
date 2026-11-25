package cl.duoc.vidasalud.kafka.admin.dto;

import jakarta.validation.constraints.Min;

import java.util.Map;

public class TopicUpdateRequest {

    @Min(1)
    private Integer partitions;

    private Map<String, String> configs;

    public Integer getPartitions() { return partitions; }
    public void setPartitions(Integer partitions) { this.partitions = partitions; }
    public Map<String, String> getConfigs() { return configs; }
    public void setConfigs(Map<String, String> configs) { this.configs = configs; }
}