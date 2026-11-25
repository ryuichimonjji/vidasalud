package cl.duoc.vidasalud.analytics.dto;

import java.util.Map;

public class ServiceSummary {

    private final String serviceId;
    private final long totalEvents;
    private final Map<String, Long> byStatus;
    private final double avgMinutesSinceCreation;

    public ServiceSummary(String serviceId, long totalEvents, Map<String, Long> byStatus,
                          double avgMinutesSinceCreation) {
        this.serviceId = serviceId;
        this.totalEvents = totalEvents;
        this.byStatus = byStatus;
        this.avgMinutesSinceCreation = avgMinutesSinceCreation;
    }

    public String getServiceId() { return serviceId; }
    public long getTotalEvents() { return totalEvents; }
    public Map<String, Long> getByStatus() { return byStatus; }
    public double getAvgMinutesSinceCreation() { return avgMinutesSinceCreation; }
}
