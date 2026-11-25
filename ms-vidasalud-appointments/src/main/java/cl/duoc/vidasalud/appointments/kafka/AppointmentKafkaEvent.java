package cl.duoc.vidasalud.appointments.kafka;

import cl.duoc.vidasalud.appointments.model.AppointmentStatus;

import java.time.LocalDateTime;

public class AppointmentKafkaEvent {

    private String eventType; // CREATED o STATUS_CHANGED
    private Long appointmentId;
    private String patientId;
    private String serviceId;
    private AppointmentStatus status;
    private LocalDateTime occurredAt;

    public AppointmentKafkaEvent() {}

    public AppointmentKafkaEvent(String eventType, Long appointmentId, String patientId,
                                 String serviceId, AppointmentStatus status) {
        this.eventType = eventType;
        this.appointmentId = appointmentId;
        this.patientId = patientId;
        this.serviceId = serviceId;
        this.status = status;
        this.occurredAt = LocalDateTime.now();
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