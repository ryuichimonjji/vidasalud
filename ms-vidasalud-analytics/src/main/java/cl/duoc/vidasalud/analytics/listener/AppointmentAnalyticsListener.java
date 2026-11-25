package cl.duoc.vidasalud.analytics.listener;

import cl.duoc.vidasalud.analytics.dto.AppointmentAnalyticsEvent;
import cl.duoc.vidasalud.analytics.service.AnalyticsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumer group "vidasalud-analytics-group": actualiza las metricas por servicio.
 * Si se levantan varias instancias de este microservicio, Kafka reparte las 3 particiones
 * entre ellas (balanceo de carga dentro del mismo group).
 */
@Component
public class AppointmentAnalyticsListener {

    private static final Logger log = LoggerFactory.getLogger(AppointmentAnalyticsListener.class);

    private final AnalyticsService analyticsService;

    public AppointmentAnalyticsListener(AnalyticsService analyticsService) {
        this.analyticsService = analyticsService;
    }

    @KafkaListener(
            topics = "${kafka.topics.analytics}",
            groupId = "${kafka.groups.analytics}",
            properties = "spring.json.value.default.type=cl.duoc.vidasalud.analytics.dto.AppointmentAnalyticsEvent")
    public void onEvent(@Payload AppointmentAnalyticsEvent event,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset) {
        analyticsService.process(event);
        log.info("[analytics] servicio={} estado={} minutos={} (particion={} offset={})",
                event.getServiceId(), event.getStatus(), event.getMinutesSinceCreation(), partition, offset);
    }
}
