package org.example;

import io.dropwizard.lifecycle.Managed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;

import static org.example.domain.Schemas.Topics.ORDERS;
import static org.example.domain.Schemas.Topics.ORDER_VALIDATION;
import static org.example.util.MicroserviceUtils.addShutdownHookAndBlock;
import static org.example.util.MicroserviceUtils.startStreams;

public class ValidationsAggregatorService implements Managed {

    private KafkaStreams streams;

    public static void main(String[] args) throws Exception {
        var service = new ValidationsAggregatorService();
        service.start();
        addShutdownHookAndBlock(service);
    }

    @Override
    public void start() {
        streams = processStreams();
        startStreams(streams);
    }

    private KafkaStreams processStreams() {
        final var builder = new StreamsBuilder();
        final var validation = builder.stream(
                ORDER_VALIDATION.name(),
                Consumed.with(ORDER_VALIDATION.keySerde(), ORDER_VALIDATION.valueSerde())
        );
        final var orders = builder.stream(
                ORDERS.name(),
                Consumed.with(ORDERS.keySerde(), ORDER_VALIDATION.valueSerde())
        );
        return null;
    }

    @Override
    public void stop() {
        if (streams != null) {
            streams.close();
        }
    }

}
