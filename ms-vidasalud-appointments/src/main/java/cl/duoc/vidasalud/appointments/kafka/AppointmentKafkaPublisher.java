package cl.duoc.vidasalud.appointments.kafka;

import cl.duoc.vidasalud.appointments.model.Appointment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;

@Component
public class AppointmentKafkaPublisher {

    private static final Logger log = LoggerFactory.getLogger(AppointmentKafkaPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String eventsTopic;
    private final String analyticsTopic;

    public AppointmentKafkaPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                                     @Value("${kafka.topics.events}") String eventsTopic,
                                     @Value("${kafka.topics.analytics}") String analyticsTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventsTopic = eventsTopic;
        this.analyticsTopic = analyticsTopic;
    }

    public void publishCreated(Appointment a) {
        publish("CREATED", a);
    }

    public void publishStatusChanged(Appointment a) {
        publish("STATUS_CHANGED", a);
    }

    private void publish(String eventType, Appointment a) {
        try {
            AppointmentKafkaEvent event = new AppointmentKafkaEvent(
                    eventType, a.getId(), a.getPatientId(), a.getServiceId(), a.getStatus());

            // Key = appointmentId: todos los eventos de una cita van a la misma particion (orden)
            send(eventsTopic, String.valueOf(a.getId()), event);

            long minutes = a.getCreatedAt() == null ? 0
                    : Duration.between(a.getCreatedAt(), LocalDateTime.now()).toMinutes();
            AppointmentAnalyticsEvent analytics = new AppointmentAnalyticsEvent(
                    a.getId(), a.getServiceId(), a.getStatus(), minutes);

            // Key = serviceId: agrupa la analitica por servicio
            send(analyticsTopic, a.getServiceId(), analytics);

        } catch (Exception e) {
            // Kafka nunca debe romper la creacion o actualizacion de la cita
            log.error("Error publicando en Kafka (cita {}): {}", a.getId(), e.getMessage());
        }
    }

    private void send(String topic, String key, Object payload) {
        kafkaTemplate.send(topic, key, payload).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Fallo envio a {} key={}: {}", topic, key, ex.getMessage());
            } else {
                log.info("Kafka OK topico={} particion={} offset={} key={}",
                        topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        key);
            }
        });
    }
}