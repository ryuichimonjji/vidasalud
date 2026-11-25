package cl.duoc.vidasalud.kafka.admin.service;

import cl.duoc.vidasalud.kafka.admin.dto.GroupInfo;
import cl.duoc.vidasalud.kafka.admin.dto.TopicInfo;
import cl.duoc.vidasalud.kafka.admin.dto.TopicRequest;
import cl.duoc.vidasalud.kafka.admin.dto.TopicUpdateRequest;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);

    private final KafkaAdmin kafkaAdmin;

    public KafkaAdminService(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    private AdminClient client() {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    // ---------- Tópicos ----------

    public void createTopic(TopicRequest req) {
        try (AdminClient admin = client()) {
            NewTopic topic = new NewTopic(req.getName(), req.getPartitions(), req.getReplicationFactor());
            if (req.getConfigs() != null) {
                topic.configs(req.getConfigs());
            }
            admin.createTopics(List.of(topic)).all().get();
            log.info("Topico creado: {} (particiones={}, replicas={})",
                    req.getName(), req.getPartitions(), req.getReplicationFactor());
        } catch (ExecutionException e) {
            throw new IllegalStateException("No se pudo crear el topico: " + e.getCause().getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Operacion interrumpida", e);
        }
    }

    public List<String> listTopics() {
        try (AdminClient admin = client()) {
            return admin.listTopics().names().get().stream()
                    .filter(n -> !n.startsWith("__"))
                    .sorted()
                    .toList();
        } catch (ExecutionException e) {
            throw new IllegalStateException("No se pudo listar topicos: " + e.getCause().getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Operacion interrumpida", e);
        }
    }

    public TopicInfo describeTopic(String name) {
        try (AdminClient admin = client()) {
            TopicDescription desc = admin.describeTopics(List.of(name)).allTopicNames().get().get(name);

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
            Config config = admin.describeConfigs(List.of(resource)).all().get().get(resource);

            Map<String, String> configs = new TreeMap<>();
            for (String key : List.of("retention.ms", "cleanup.policy", "min.insync.replicas",
                    "segment.bytes", "max.message.bytes")) {
                ConfigEntry entry = config.get(key);
                if (entry != null) {
                    configs.put(key, entry.value());
                }
            }

            int replicas = desc.partitions().isEmpty() ? 0 : desc.partitions().get(0).replicas().size();
            return new TopicInfo(name, desc.partitions().size(), replicas, configs);
        } catch (ExecutionException e) {
            throw new IllegalStateException("No se pudo describir el topico: " + e.getCause().getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Operacion interrumpida", e);
        }
    }

    public void updateTopic(String name, TopicUpdateRequest req) {
        try (AdminClient admin = client()) {
            if (req.getPartitions() != null) {
                admin.createPartitions(Map.of(name, NewPartitions.increaseTo(req.getPartitions()))).all().get();
                log.info("Particiones de {} aumentadas a {}", name, req.getPartitions());
            }
            if (req.getConfigs() != null && !req.getConfigs().isEmpty()) {
                ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
                List<AlterConfigOp> ops = req.getConfigs().entrySet().stream()
                        .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()),
                                AlterConfigOp.OpType.SET))
                        .toList();
                admin.incrementalAlterConfigs(Map.of(resource, ops)).all().get();
                log.info("Configuracion de {} actualizada: {}", name, req.getConfigs());
            }
        } catch (ExecutionException e) {
            throw new IllegalStateException("No se pudo modificar el topico: " + e.getCause().getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Operacion interrumpida", e);
        }
    }

    public void deleteTopic(String name) {
        try (AdminClient admin = client()) {
            admin.deleteTopics(List.of(name)).all().get();
            log.info("Topico eliminado: {}", name);
        } catch (ExecutionException e) {
            throw new IllegalStateException("No se pudo eliminar el topico: " + e.getCause().getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Operacion interrumpida", e);
        }
    }

    // ---------- Consumer groups ----------

    public List<String> listGroups() {
        try (AdminClient admin = client()) {
            return admin.listConsumerGroups().all().get().stream()
                    .map(ConsumerGroupListing::groupId)
                    .sorted()
                    .toList();
        } catch (ExecutionException e) {
            throw new IllegalStateException("No se pudo listar groups: " + e.getCause().getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Operacion interrumpida", e);
        }
    }

    public GroupInfo describeGroup(String groupId) {
        try (AdminClient admin = client()) {
            ConsumerGroupDescription desc = admin.describeConsumerGroups(List.of(groupId))
                    .describedGroups().get(groupId).get();

            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get();

            // Offset final de cada particion, para calcular el lag
            Map<TopicPartition, OffsetSpec> latestRequest = new HashMap<>();
            committed.keySet().forEach(tp -> latestRequest.put(tp, OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest = latestRequest.isEmpty()
                    ? Map.of()
                    : admin.listOffsets(latestRequest).all().get();

            Map<String, Long> offsets = new TreeMap<>();
            Map<String, Long> endOffsets = new TreeMap<>();
            Map<String, Long> lag = new TreeMap<>();
            long totalLag = 0;

            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed.entrySet()) {
                TopicPartition tp = entry.getKey();
                String key = tp.topic() + "-" + tp.partition();
                long current = entry.getValue().offset();
                long end = latest.get(tp) == null ? current : latest.get(tp).offset();
                long partitionLag = Math.max(0, end - current);

                offsets.put(key, current);
                endOffsets.put(key, end);
                lag.put(key, partitionLag);
                totalLag += partitionLag;
            }

            return new GroupInfo(groupId, String.valueOf(desc.state()), desc.members().size(),
                    offsets, endOffsets, lag, totalLag);
        } catch (ExecutionException e) {
            throw new IllegalStateException("No se pudo describir el group: " + e.getCause().getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Operacion interrumpida", e);
        }
    }

    // ---------- Cluster ----------

    public Map<String, Object> describeCluster() {
        try (AdminClient admin = client()) {
            DescribeClusterResult cluster = admin.describeCluster();
            List<String> brokers = cluster.nodes().get().stream()
                    .sorted(Comparator.comparingInt(org.apache.kafka.common.Node::id))
                    .map(n -> n.id() + "@" + n.host() + ":" + n.port())
                    .toList();

            Map<String, Object> info = new LinkedHashMap<>();
            info.put("clusterId", cluster.clusterId().get());
            info.put("controller", cluster.controller().get().id());
            info.put("brokers", brokers);
            return info;
        } catch (ExecutionException e) {
            throw new IllegalStateException("No se pudo describir el cluster: " + e.getCause().getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Operacion interrumpida", e);
        }
    }
}
