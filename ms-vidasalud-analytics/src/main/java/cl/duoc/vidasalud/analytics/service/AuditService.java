package cl.duoc.vidasalud.analytics.service;

import cl.duoc.vidasalud.analytics.dto.AppointmentKafkaEvent;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Historial de cambios de estado por atencion. Como el topico usa appointmentId como key,
 * todos los eventos de una atencion llegan en orden desde la misma particion.
 */
@Service
public class AuditService {

    private final Map<Long, List<AppointmentKafkaEvent>> history = new ConcurrentHashMap<>();

    public void record(AppointmentKafkaEvent event) {
        if (event == null || event.getAppointmentId() == null || event.getStatus() == null
                || event.getEventType() == null) {
            throw new IllegalArgumentException("Evento de auditoria invalido: faltan datos obligatorios");
        }
        history.computeIfAbsent(event.getAppointmentId(), id -> new CopyOnWriteArrayList<>()).add(event);
    }

    public List<AppointmentKafkaEvent> historyOf(Long appointmentId) {
        return new ArrayList<>(history.getOrDefault(appointmentId, List.of()));
    }

    public int trackedAppointments() {
        return history.size();
    }
}
