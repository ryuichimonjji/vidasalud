package cl.duoc.vidasalud.analytics.listener;

import cl.duoc.vidasalud.analytics.dto.AppointmentKafkaEvent;
import cl.duoc.vidasalud.analytics.service.AuditService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumer group "vidasalud-audit-group": guarda el historial de cada atencion.
 * La logica de negocio vive en AuditService; este listener solo adapta Kafka.
 */
@Component
public class AppointmentEventsListener {

    private static final Logger log = LoggerFactory.getLogger(AppointmentEventsListener.class);

    private final AuditService auditService;

    public AppointmentEventsListener(AuditService auditService) {
        this.auditService = auditService;
    }

    @KafkaListener(
            topics = "${kafka.topics.events}",
            groupId = "${kafka.groups.audit}",
            properties = "spring.json.value.default.type=cl.duoc.vidasalud.analytics.dto.AppointmentKafkaEvent")
    public void onEvent(@Payload AppointmentKafkaEvent event,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset) {
        auditService.record(event);
        log.info("[audit] {} cita={} estado={} (particion={} offset={})",
                event.getEventType(), event.getAppointmentId(), event.getStatus(), partition, offset);
    }
}
