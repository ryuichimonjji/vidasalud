package cl.duoc.vidasalud.analytics.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Manejo de errores de los consumidores Kafka.
 * Spring Boot aplica este CommonErrorHandler a todos los @KafkaListener.
 *
 * - Errores transitorios: se reintenta con backoff exponencial.
 * - Errores de datos (IllegalArgumentException, mensajes que no deserializan):
 *   no se reintentan, se registran en el log y el consumo continua con el siguiente
 *   mensaje, asi un mensaje defectuoso no bloquea la particion.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            @Value("${kafka.retry.max-attempts}") int maxAttempts,
            @Value("${kafka.retry.initial-interval-ms}") long initialInterval,
            @Value("${kafka.retry.multiplier}") double multiplier,
            @Value("${kafka.retry.max-interval-ms}") long maxInterval) {

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxAttempts);
        backOff.setInitialInterval(initialInterval);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxInterval);

        DefaultErrorHandler handler = new DefaultErrorHandler((record, ex) ->
                log.error("Mensaje descartado tras agotar reintentos. topico={} particion={} offset={} key={} causa={}",
                        record.topic(), record.partition(), record.offset(), record.key(), ex.getMessage()),
                backOff);

        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        handler.setRetryListeners((record, ex, attempt) ->
                log.warn("Reintento {} topico={} offset={} causa={}",
                        attempt, record.topic(), record.offset(), ex.getMessage()));
        return handler;
    }
}
