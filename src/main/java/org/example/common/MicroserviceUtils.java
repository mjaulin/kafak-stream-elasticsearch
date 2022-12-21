package org.example.common;

import io.dropwizard.lifecycle.Managed;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MicroserviceUtils {

    private static final Long CALL_TIMEOUT = 10000L;

    public static KafkaStreams startStreams(StreamsBuilder builder, Properties config) {
        final var streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp(); // don't do this in prod as it clears your state stores
        final CountDownLatch startLatch = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
                startLatch.countDown();
            }

        });
        streams.start();

        try {
            if (!startLatch.await(60, TimeUnit.SECONDS)) {
                throw new RuntimeException("Streams never finished rebalancing on startup");
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("Streams started");
        return streams;
    }

    public static void setTimeout(final AsyncResponse asyncResponse) {
        asyncResponse.setTimeout(CALL_TIMEOUT, TimeUnit.MILLISECONDS);
        asyncResponse.setTimeoutHandler(resp -> resp.resume(
                Response.status(Response.Status.GATEWAY_TIMEOUT)
                        .entity("HTTP GET timed out after " + CALL_TIMEOUT + " ms\n")
                        .build()));
    }

    public static <K, T> KafkaProducer<K, T> createProducer(final Schemas.Topic<K, T> topic, final String appId) {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 1);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, appId);

        return new KafkaProducer<>(producerConfig, topic.keySerde().serializer(), topic.valueSerde().serializer());
    }

    public static Properties baseStreamsConfig(final String appId) {
        final var config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        config.put(StreamsConfig.STATE_DIR_CONFIG, "./state");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1);
        return config;
    }

    public static void addShutdownHookAndBlock(final Managed service) throws InterruptedException {
        Thread.currentThread().setUncaughtExceptionHandler((t, e) -> {
            try {
                service.stop();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                service.stop();
            } catch (final Exception ignored) {
            }
        }));
        Thread.currentThread().join();
    }

}
