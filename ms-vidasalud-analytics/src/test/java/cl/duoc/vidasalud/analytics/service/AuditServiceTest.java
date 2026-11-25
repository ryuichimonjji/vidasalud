package cl.duoc.vidasalud.analytics.service;

import cl.duoc.vidasalud.analytics.dto.AppointmentKafkaEvent;
import cl.duoc.vidasalud.analytics.model.AppointmentStatus;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuditServiceTest {

    private final AuditService service = new AuditService();

    private AppointmentKafkaEvent event(Long id, String type, AppointmentStatus status) {
        return new AppointmentKafkaEvent(type, id, "11111111-1", "PEDIATRIA", status, LocalDateTime.now());
    }

    @Test
    void mantieneElOrdenDeLosEventosDeUnaCita() {
        service.record(event(7L, "CREATED", AppointmentStatus.SOLICITADA));
        service.record(event(7L, "STATUS_CHANGED", AppointmentStatus.CONFIRMADA));

        List<AppointmentKafkaEvent> history = service.historyOf(7L);
        assertEquals(2, history.size());
        assertEquals(AppointmentStatus.SOLICITADA, history.get(0).getStatus());
        assertEquals(AppointmentStatus.CONFIRMADA, history.get(1).getStatus());
    }

    @Test
    void citaSinEventosDevuelveListaVacia() {
        assertTrue(service.historyOf(999L).isEmpty());
    }

    @Test
    void rechazaEventoSinId() {
        assertThrows(IllegalArgumentException.class,
                () -> service.record(event(null, "CREATED", AppointmentStatus.SOLICITADA)));
    }
}
