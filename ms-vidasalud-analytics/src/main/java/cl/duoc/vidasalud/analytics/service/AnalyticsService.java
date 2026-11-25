package cl.duoc.vidasalud.analytics.service;

import cl.duoc.vidasalud.analytics.dto.AppointmentAnalyticsEvent;
import cl.duoc.vidasalud.analytics.dto.ServiceSummary;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metricas en tiempo casi real por servicio medico, actualizadas con cada evento
 * del topico vidasalud.appointments.analytics.
 */
@Service
public class AnalyticsService {

    private final Map<String, Stats> stats = new ConcurrentHashMap<>();

    public void process(AppointmentAnalyticsEvent event) {
        if (event == null || event.getServiceId() == null || event.getServiceId().isBlank()
                || event.getStatus() == null) {
            throw new IllegalArgumentException("Evento de analitica invalido: faltan datos obligatorios");
        }
        stats.computeIfAbsent(event.getServiceId(), k -> new Stats()).add(event);
    }

    public Map<String, ServiceSummary> summary() {
        Map<String, ServiceSummary> result = new TreeMap<>();
        stats.forEach((serviceId, s) -> result.put(serviceId, s.toSummary(serviceId)));
        return result;
    }

    private static class Stats {
        private long total;
        private long minutesSum;
        private final Map<String, Long> byStatus = new TreeMap<>();

        synchronized void add(AppointmentAnalyticsEvent e) {
            total++;
            minutesSum += Math.max(0, e.getMinutesSinceCreation());
            byStatus.merge(e.getStatus().name(), 1L, Long::sum);
        }

        synchronized ServiceSummary toSummary(String serviceId) {
            double avg = total == 0 ? 0 : (double) minutesSum / total;
            return new ServiceSummary(serviceId, total, new TreeMap<>(byStatus), avg);
        }
    }
}
