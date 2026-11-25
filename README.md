VidaSalud

Sistema de gestión de citas médicas basado en microservicios. Cuando se crea o actualiza una cita, el sistema:

Envía una notificación por RabbitMQ.
Guarda el historial y las métricas en Apache Kafka (cluster de 3 brokers) para auditoría y analítica.
Arquitectura
Frontend (Angular) --> BFF (:8080) --> appointments (:8081) --RabbitMQ--> notify (:8082)
                                              |
                                              +--Kafka--> vidasalud.appointments.events    --> grupo vidasalud-audit-group
                                              +--Kafka--> vidasalud.appointments.analytics --> grupo vidasalud-analytics-group
                                                                                                (analytics :8084)
RabbitMQ entrega la notificación puntual de cada cita. Kafka no lo reemplaza: guarda el historial y alimenta la analítica.
Kafka: 3 nodos Zookeeper + 3 brokers. Los tópicos tienen 3 particiones, 3 réplicas y min.insync.replicas=2.
Productor: acks=all con idempotencia.
Consumidores: ErrorHandlingDeserializer y reintentos con backoff exponencial. Los errores de datos no se reintentan.
Autenticación: Microsoft Entra ID (Azure). El BFF valida el JWT y exige uno de los roles Admin, Operador o Cliente.
Estructura del repositorio
Carpeta	Puerto	Descripción
frontend-vidasalud	4200	Aplicación Angular con login MSAL y gestión de citas.
ms-vidasalud-bff	8080	Backend for Frontend: valida el token y reenvía a appointments.
ms-vidasalud-appointments	8081	CRUD de citas (H2 en memoria). Publica en RabbitMQ y Kafka.
ms-vidasalud-notify	8082	Consume RabbitMQ y procesa notificaciones y dead letters.
ms-vidasalud-kafka-admin	8083	Crea los tópicos al arrancar y expone la administración del cluster Kafka.
ms-vidasalud-analytics	8084	Consume Kafka y expone métricas e historial de citas.
ms-vidasalud-rabbitmq-admin	8083	Administración de colas, exchanges y bindings. Incluye el docker-compose de RabbitMQ.

kafka-admin y rabbitmq-admin usan el mismo puerto (8083). No los levantes a la vez en la misma máquina, o cambia el puerto de uno.

Requisitos
Docker Desktop (con WSL2 en Windows)
JDK 17
Node.js y npm (solo para el frontend)
No necesitas instalar Maven: cada servicio incluye mvnw / mvnw.cmd.
Puesta en marcha en local
1. Kafka (3 Zookeeper + 3 brokers)
bash
cd ms-vidasalud-kafka-admin/docker
docker compose -f docker-compose-kafka.yml up -d
docker compose -f docker-compose-kafka.yml ps     # deben verse 6 contenedores

Espera 30 a 60 segundos a que los brokers terminen de arrancar.

2. Kafka UI (opcional)

Se levanta después del paso 1, porque usa la red kafka-net.

bash
docker compose -f docker-compose-kafka-ui.yml up -d

Disponible en http://localhost:8090.

3. RabbitMQ (cluster de 2 nodos)
bash
cd ms-vidasalud-rabbitmq-admin/docker
docker compose up -d

Panel web: http://localhost:15672 (usuario guest, clave guest).

4. Microservicios

Una terminal por servicio, en este orden: kafka-admin, analytics, appointments y notify.

Windows (cmd):

cmd
cd ms-vidasalud-kafka-admin
mvnw.cmd spring-boot:run
cmd
cd ms-vidasalud-appointments
set RABBITMQ_HOST=localhost
mvnw.cmd spring-boot:run

Linux / macOS:

bash
cd ms-vidasalud-appointments
RABBITMQ_HOST=localhost ./mvnw spring-boot:run

appointments y notify necesitan RABBITMQ_HOST. kafka-admin crea los tópicos vidasalud.appointments.events y vidasalud.appointments.analytics al arrancar.

5. BFF y frontend (opcional)
bash
cd ms-vidasalud-bff
./mvnw spring-boot:run

cd ../frontend-vidasalud
npm install
npm start

El frontend queda en http://localhost:4200. Requiere configurar frontend-vidasalud/src/app/auth-config.ts (ver sección de configuración).

Variables de entorno
Variable	Usada por	Valor por defecto	Descripción
RABBITMQ_HOST	appointments, notify, rabbitmq-admin	ELASTIC_IP_RABBITMQ	Host de RabbitMQ. En local usar localhost.
KAFKA_BOOTSTRAP_SERVERS	appointments, analytics, kafka-admin	localhost:9092,localhost:9093,localhost:9094	Brokers de Kafka.
KAFKA_PUBLIC_HOST	docker-compose-kafka.yml	localhost	IP o host que los brokers anuncian a los clientes. En la nube, la IP pública.
CORS_ALLOWED_ORIGINS	BFF	http://localhost:4200	Orígenes permitidos por CORS, separados por coma.
Probar el flujo

Crear una cita (dispara 1 mensaje a RabbitMQ y 2 eventos a Kafka):

bash
curl -X POST http://localhost:8081/api/appointments \
  -H "Content-Type: application/json" \
  -d '{"patientId":"11111111-1","serviceId":"PEDIATRIA"}'

En cmd de Windows, en una sola línea:

cmd
curl -X POST http://localhost:8081/api/appointments -H "Content-Type: application/json" -d "{\"patientId\":\"11111111-1\",\"serviceId\":\"PEDIATRIA\"}"

Consultar el resultado (también funcionan en el navegador):

http://localhost:8084/api/analytics/summary
http://localhost:8084/api/analytics/appointments/1/history
http://localhost:8083/api/kafka/cluster
http://localhost:8083/api/kafka/groups/vidasalud-analytics-group

En Kafka UI (http://localhost:8090): Topics → vidasalud.appointments.events → Messages para ver los mensajes, y Consumers para ver los grupos y su lag.

Para mostrar el balanceo de consumidores, levanta una segunda instancia de analytics con otro puerto (--server.port) y observa en Kafka UI cómo se reparten las 3 particiones dentro del mismo grupo.

API
Servicio	Endpoints
appointments	POST /api/appointments, GET /api/appointments, GET /api/appointments/{id}, PUT /api/appointments/{id}/status
BFF	Mismos endpoints que appointments, protegidos con JWT.
analytics	GET /api/analytics/summary, GET /api/analytics/appointments/{id}/history, GET /api/analytics/stats
kafka-admin	POST/GET /api/kafka/topics, GET/PUT/DELETE /api/kafka/topics/{name}, GET /api/kafka/cluster, GET /api/kafka/groups, GET /api/kafka/groups/{groupId}
rabbitmq-admin	POST /api/queues, DELETE /api/queues/{name}, POST /api/exchanges, DELETE /api/exchanges/{name}, POST/DELETE /api/bindings
Despliegue en la nube (AWS EC2)
Asigna una Elastic IP a cada instancia (una IP pública fija), para que la dirección no cambie al detener e iniciar la instancia.
Reemplaza los marcadores en el código, o usa variables de entorno:
ELASTIC_IP_RABBITMQ en los application.yml de appointments, notify y rabbitmq-admin (o define RABBITMQ_HOST).
ELASTIC_IP_BACKEND en frontend-vidasalud/src/app/auth-config.ts.
En la instancia de Kafka, antes de levantar el compose:
bash
export KAFKA_PUBLIC_HOST=<Elastic IP>
docker compose -f docker-compose-kafka.yml up -d --force-recreate
Arranca los microservicios con KAFKA_BOOTSTRAP_SERVERS=<ip>:9092,<ip>:9093,<ip>:9094.
En el Security Group, abre solo lo necesario: 22, 5672, 15672, 9092 a 9094, 8090 y 8080. Limita el origen a tu IP.
Si publicas el frontend, agrega su URL en CORS_ALLOWED_ORIGINS y como Redirect URI en Microsoft Entra ID.
Configuración y seguridad
RabbitMQ usa guest/guest por defecto. Cámbialo antes de exponer el servicio fuera de tu equipo.
Abrir los puertos de Kafka y RabbitMQ a 0.0.0.0/0 permite que cualquiera en internet se conecte.
Los IDs de Microsoft Entra ID (clientId, tenantId) están en auth-config.ts y en el application.yml del BFF. No son secretos, pero identifican la aplicación.
La base de datos de appointments es H2 en memoria: los datos se pierden al reiniciar el servicio.
Si algo falla
Un broker no levanta: docker compose -f docker-compose-kafka.yml logs kafka-1. Si cambiaste la configuración, usa down -v y vuelve a levantar.
Kafka UI no abre: levantaste Kafka UI antes del cluster. Levanta primero Kafka.
Puerto ocupado: revisa que nada use 8080 a 8084, 9092 a 9094, 5672, 15672 y 8090.
appointments o notify no conectan a RabbitMQ: falta definir RABBITMQ_HOST en esa terminal, o sigue el marcador ELASTIC_IP_RABBITMQ.
mvnw no descarga Maven: revisa tu conexión o el proxy.
Tecnologías

Java 17, Spring Boot, Spring AMQP, Spring for Apache Kafka, Spring Security (OAuth2 Resource Server), Spring Data JPA (H2), Angular 21, MSAL, Docker Compose, RabbitMQ y Apache Kafka.
