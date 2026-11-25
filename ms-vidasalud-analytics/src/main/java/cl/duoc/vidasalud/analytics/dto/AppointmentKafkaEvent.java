package cl.duoc.vidasalud.analytics.dto;

import cl.duoc.vidasalud.analytics.model.AppointmentStatus;

import java.time.LocalDateTime;

/**
 * Evento del topico vidasalud.appointments.events (key = appointmentId).
 * Describe el ciclo de vida de una atencion: CREATED o STATUS_CHANGED.
 */
public class AppointmentKafkaEvent {

    private String eventType;
    private Long appointmentId;
    private String patientId;
    private String serviceId;
    private AppointmentStatus status;
    private LocalDateTime occurredAt;

    public AppointmentKafkaEvent() {}

    public AppointmentKafkaEvent(String eventType, Long appointmentId, String patientId,
                                 String serviceId, AppointmentStatus status, LocalDateTime occurredAt) {
        this.eventType = eventType;
        this.appointmentId = appointmentId;
        this.patientId = patientId;
        this.serviceId = serviceId;
        this.status = status;
        this.occurredAt = occurredAt;
    }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public Long getAppointmentId() { return appointmentId; }
    public void setAppointmentId(Long appointmentId) { this.appointmentId = appointmentId; }
    public String getPatientId() { return patientId; }
    public void setPatientId(String patientId) { this.patientId = patientId; }
    public String getServiceId() { return serviceId; }
    public void setServiceId(String serviceId) { this.serviceId = serviceId; }
    public AppointmentStatus getStatus() { return status; }
    public void setStatus(AppointmentStatus status) { this.status = status; }
    public LocalDateTime getOccurredAt() { return occurredAt; }
    public void setOccurredAt(LocalDateTime occurredAt) { this.occurredAt = occurredAt; }
}
