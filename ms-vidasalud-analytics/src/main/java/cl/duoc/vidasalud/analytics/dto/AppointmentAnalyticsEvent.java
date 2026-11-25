package cl.duoc.vidasalud.analytics.dto;

import cl.duoc.vidasalud.analytics.model.AppointmentStatus;

import java.time.LocalDateTime;

/**
 * Evento del topico vidasalud.appointments.analytics (key = serviceId).
 * Trae solo lo necesario para metricas, sin datos del paciente.
 */
public class AppointmentAnalyticsEvent {

    private Long appointmentId;
    private String serviceId;
    private AppointmentStatus status;
    private long minutesSinceCreation;
    private LocalDateTime occurredAt;

    public AppointmentAnalyticsEvent() {}

    public AppointmentAnalyticsEvent(Long appointmentId, String serviceId, AppointmentStatus status,
                                     long minutesSinceCreation, LocalDateTime occurredAt) {
        this.appointmentId = appointmentId;
        this.serviceId = serviceId;
        this.status = status;
        this.minutesSinceCreation = minutesSinceCreation;
        this.occurredAt = occurredAt;
    }

    public Long getAppointmentId() { return appointmentId; }
    public void setAppointmentId(Long appointmentId) { this.appointmentId = appointmentId; }
    public String getServiceId() { return serviceId; }
    public void setServiceId(String serviceId) { this.serviceId = serviceId; }
    public AppointmentStatus getStatus() { return status; }
    public void setStatus(AppointmentStatus status) { this.status = status; }
    public long getMinutesSinceCreation() { return minutesSinceCreation; }
    public void setMinutesSinceCreation(long minutesSinceCreation) { this.minutesSinceCreation = minutesSinceCreation; }
    public LocalDateTime getOccurredAt() { return occurredAt; }
    public void setOccurredAt(LocalDateTime occurredAt) { this.occurredAt = occurredAt; }
}
