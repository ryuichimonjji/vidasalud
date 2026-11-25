package cl.duoc.vidasalud.analytics.service;

import cl.duoc.vidasalud.analytics.dto.AppointmentAnalyticsEvent;
import cl.duoc.vidasalud.analytics.dto.ServiceSummary;
import cl.duoc.vidasalud.analytics.model.AppointmentStatus;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AnalyticsServiceTest {

    private final AnalyticsService service = new AnalyticsService();

    private AppointmentAnalyticsEvent event(String serviceId, AppointmentStatus status, long minutes) {
        return new AppointmentAnalyticsEvent(1L, serviceId, status, minutes, LocalDateTime.now());
    }

    @Test
    void agrupaMetricasPorServicio() {
        service.process(event("MEDICINA_GENERAL", AppointmentStatus.SOLICITADA, 0));
        service.process(event("MEDICINA_GENERAL", AppointmentStatus.CONFIRMADA, 10));
        service.process(event("PEDIATRIA", AppointmentStatus.SOLICITADA, 0));

        ServiceSummary general = service.summary().get("MEDICINA_GENERAL");
        assertEquals(2, general.getTotalEvents());
        assertEquals(5.0, general.getAvgMinutesSinceCreation());
        assertEquals(1L, general.getByStatus().get("CONFIRMADA"));
        assertEquals(1, service.summary().get("PEDIATRIA").getTotalEvents());
    }

    @Test
    void rechazaEventoSinServicio() {
        assertThrows(IllegalArgumentException.class,
                () -> service.process(event(null, AppointmentStatus.SOLICITADA, 0)));
    }

    @Test
    void rechazaEventoSinEstado() {
        assertThrows(IllegalArgumentException.class,
                () -> service.process(event("PEDIATRIA", null, 0)));
    }
}
