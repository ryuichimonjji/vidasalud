package cl.duoc.vidasalud.analytics.controller;

import cl.duoc.vidasalud.analytics.dto.AppointmentKafkaEvent;
import cl.duoc.vidasalud.analytics.dto.ServiceSummary;
import cl.duoc.vidasalud.analytics.service.AnalyticsService;
import cl.duoc.vidasalud.analytics.service.AuditService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/analytics")
public class AnalyticsController {

    private final AnalyticsService analyticsService;
    private final AuditService auditService;

    public AnalyticsController(AnalyticsService analyticsService, AuditService auditService) {
        this.analyticsService = analyticsService;
        this.auditService = auditService;
    }

    /** Metricas por servicio medico, calculadas a partir del topico de analitica. */
    @GetMapping("/summary")
    public Map<String, ServiceSummary> summary() {
        return analyticsService.summary();
    }

    /** Historial ordenado de estados de una atencion, construido desde el topico de eventos. */
    @GetMapping("/appointments/{id}/history")
    public List<AppointmentKafkaEvent> history(@PathVariable Long id) {
        return auditService.historyOf(id);
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return Map.of("trackedAppointments", auditService.trackedAppointments(),
                "services", analyticsService.summary().size());
    }
}
