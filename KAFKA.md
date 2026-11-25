# VidaSalud - Kafka (EP5 / EP6)

## Arquitectura

```
appointments (productor) --RabbitMQ--> notify            (flujo existente, no se toca)
        |
        +--Kafka--> vidasalud.appointments.events    (key=appointmentId, retention 7d)  --> vidasalud-audit-group
        +--Kafka--> vidasalud.appointments.analytics (key=serviceId,     retention 30d) --> vidasalud-analytics-group
                                                                                         (ms-vidasalud-analytics, :8084)
```

- 3 nodos Zookeeper (quorum 2/3) + 3 brokers. Topicos con 3 particiones, 3 replicas, `min.insync.replicas=2`.
- Productor: `acks=all` + idempotencia. Consumidores: `ErrorHandlingDeserializer`, reintentos con backoff
  exponencial, errores de datos (`IllegalArgumentException`) no se reintentan y quedan en el log.
- Kafka no reemplaza a RabbitMQ: RabbitMQ entrega la notificacion puntual; Kafka guarda el historial y alimenta la analitica.

## Levantar (en la nube exportar primero `KAFKA_PUBLIC_HOST=<IP publica>`)

```bash
cd ms-vidasalud-kafka-admin/docker
docker compose -f docker-compose-kafka.yml up -d   # si cambio la IP: agregar --force-recreate
docker compose -f docker-compose-kafka-ui.yml up -d      # Kafka UI en :8090
```

Orden de arranque: kafka-admin (:8083, crea los topicos) -> analytics (:8084) -> appointments (:8081).
Variable para los microservicios: `KAFKA_BOOTSTRAP_SERVERS=<ip>:9092,<ip>:9093,<ip>:9094`.

## Demo rapida

```bash
# crear una cita (dispara RabbitMQ + 2 eventos Kafka)
curl -X POST localhost:8081/api/appointments -H 'Content-Type: application/json' \
     -d '{"patientId":"11111111-1","serviceId":"PEDIATRIA"}'

curl localhost:8084/api/analytics/summary                 # metricas por servicio (se actualizan solas)
curl localhost:8084/api/analytics/appointments/1/history  # historial de estados de la cita
curl localhost:8083/api/kafka/cluster                     # brokers y controller
curl localhost:8083/api/kafka/topics/vidasalud.appointments.events
curl localhost:8083/api/kafka/groups/vidasalud-analytics-group   # offsets, endOffsets y lag
```

Para mostrar balanceo: levantar 2 instancias de analytics con `--server.port` distinto y ver en Kafka UI como se
reparten las 3 particiones dentro del mismo group.

## IPs elasticas

Reemplazar en el proyecto `ELASTIC_IP_RABBITMQ` (appointments, notify, rabbitmq-admin) y `ELASTIC_IP_BACKEND` (frontend `auth-config.ts`).
Alternativa sin tocar codigo: variable `RABBITMQ_HOST`. Para CORS del BFF con frontend desplegado: variable `CORS_ALLOWED_ORIGINS` (separadas por coma).
