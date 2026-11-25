package cl.duoc.vidasalud.kafka.admin.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsInitializer {

    public static final String EVENTS_TOPIC = "vidasalud.appointments.events";
    public static final String ANALYTICS_TOPIC = "vidasalud.appointments.analytics";

    private static final String DAYS_7_MS = String.valueOf(7L * 24 * 60 * 60 * 1000);
    private static final String DAYS_30_MS = String.valueOf(30L * 24 * 60 * 60 * 1000);

    @Bean
    public NewTopic appointmentsEventsTopic() {
        return TopicBuilder.name(EVENTS_TOPIC)
                .partitions(3)
                .replicas(3)
                .config(TopicConfig.RETENTION_MS_CONFIG, DAYS_7_MS)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .build();
    }

    @Bean
    public NewTopic appointmentsAnalyticsTopic() {
        return TopicBuilder.name(ANALYTICS_TOPIC)
                .partitions(3)
                .replicas(3)
                .config(TopicConfig.RETENTION_MS_CONFIG, DAYS_30_MS)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .build();
    }
}