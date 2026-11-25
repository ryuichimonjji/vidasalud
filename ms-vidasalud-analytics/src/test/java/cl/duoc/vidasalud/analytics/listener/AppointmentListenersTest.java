package cl.duoc.vidasalud.analytics.listener;

import cl.duoc.vidasalud.analytics.dto.AppointmentAnalyticsEvent;
import cl.duoc.vidasalud.analytics.dto.AppointmentKafkaEvent;
import cl.duoc.vidasalud.analytics.model.AppointmentStatus;
import cl.duoc.vidasalud.analytics.service.AnalyticsService;
import cl.duoc.vidasalud.analytics.service.AuditService;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AppointmentListenersTest {

    @Test
    void listenerDeEventosGuardaEnAuditoria() {
        AuditService audit = new AuditService();
        AppointmentEventsListener listener = new AppointmentEventsListener(audit);

        listener.onEvent(new AppointmentKafkaEvent("CREATED", 1L, "11111111-1", "PEDIATRIA",
                AppointmentStatus.SOLICITADA, LocalDateTime.now()), 0, 0L);

        assertEquals(1, audit.historyOf(1L).size());
    }

    @Test
    void listenerDeAnaliticaActualizaMetricas() {
        AnalyticsService analytics = new AnalyticsService();
        AppointmentAnalyticsListener listener = new AppointmentAnalyticsListener(analytics);

        listener.onEvent(new AppointmentAnalyticsEvent(1L, "PEDIATRIA", AppointmentStatus.CONFIRMADA, 4,
                LocalDateTime.now()), 1, 3L);

        assertEquals(1, analytics.summary().get("PEDIATRIA").getTotalEvents());
    }

    @Test
    void eventoInvalidoLanzaExcepcionNoReintentable() {
        AppointmentAnalyticsListener listener = new AppointmentAnalyticsListener(new AnalyticsService());

        // El DefaultErrorHandler no reintenta IllegalArgumentException: se registra y se continua
        assertThrows(IllegalArgumentException.class, () -> listener.onEvent(
                new AppointmentAnalyticsEvent(1L, null, AppointmentStatus.CONFIRMADA, 4, LocalDateTime.now()), 0, 0L));
    }
}
