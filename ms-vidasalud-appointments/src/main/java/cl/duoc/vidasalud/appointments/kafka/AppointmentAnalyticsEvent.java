package cl.duoc.vidasalud.appointments.kafka;

import cl.duoc.vidasalud.appointments.model.AppointmentStatus;

import java.time.LocalDateTime;

public class AppointmentAnalyticsEvent {

    private Long appointmentId;
    private String serviceId;
    private AppointmentStatus status;
    private long minutesSinceCreation;
    private LocalDateTime occurredAt;

    public AppointmentAnalyticsEvent() {}

    public AppointmentAnalyticsEvent(Long appointmentId, String serviceId, AppointmentStatus status,
                                     long minutesSinceCreation) {
        this.appointmentId = appointmentId;
        this.serviceId = serviceId;
        this.status = status;
        this.minutesSinceCreation = minutesSinceCreation;
        this.occurredAt = LocalDateTime.now();
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