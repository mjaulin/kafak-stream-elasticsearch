package org.example;

import io.dropwizard.lifecycle.Managed;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.example.avro.Order;

import java.time.Duration;

import static org.example.avro.OrderState.*;
import static org.example.avro.OrderValidationResult.FAIL;
import static org.example.avro.OrderValidationResult.PASS;
import static org.example.common.MicroserviceUtils.*;
import static org.example.common.Schemas.Topics.ORDERS;
import static org.example.common.Schemas.Topics.ORDERS_VALIDATION;

@Slf4j
public class ValidationsAggregatorService implements Managed {

    private static final int NUMBER_OF_RULES = 2;
    private KafkaStreams streams;

    public static void main(String[] args) throws InterruptedException {
        var service = new ValidationsAggregatorService();
        service.start();
        addShutdownHookAndBlock(service);
    }

    @Override
    public void start() {
        streams = startStreams(processStreams(), baseStreamsConfig("validation-aggregation"));
        log.info("Service started");
    }

    private StreamsBuilder processStreams() {
        final var builder = new StreamsBuilder();
        final var validations = builder.stream(
                ORDERS_VALIDATION.name(),
                Consumed.with(ORDERS_VALIDATION.keySerde(), ORDERS_VALIDATION.valueSerde())
        );
        final var orders = builder.stream(
                ORDERS.name(),
                Consumed.with(ORDERS.keySerde(), ORDERS.valueSerde())
        ).filter((id, order) -> CREATED.equals(order.getState()));

        validations
            .groupByKey(Grouped.with(ORDERS_VALIDATION.keySerde(), ORDERS_VALIDATION.valueSerde()))
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5)))
            .aggregate(
                () -> 0L,
                (id, result, total) -> PASS.equals(result.getValidationResult()) ? (total + 1) : total,
                (k, a, b) -> b == null ? a : b, // include a merger as we're using session windows.
                Materialized.with(null, Serdes.Long())
            )
            .toStream((windowKey, total) -> windowKey.key())
            .filter((k, total) -> total != null && total >= NUMBER_OF_RULES)
            .join(
                orders,
                (id, order) -> Order.newBuilder(order).setState(VALIDATED).build(),
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
                StreamJoined.with(ORDERS.keySerde(), Serdes.Long(), ORDERS.valueSerde())
            ).to(ORDERS.name(), Produced.with(ORDERS.keySerde(), ORDERS.valueSerde()));

        validations.filter((id, rule) -> FAIL.equals(rule.getValidationResult()))
            .join(
                orders,
                (id, order) -> Order.newBuilder(order).setState(FAILED).build(),
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
                StreamJoined.with(ORDERS.keySerde(), ORDERS_VALIDATION.valueSerde(), ORDERS.valueSerde())
            )
            .groupByKey(Grouped.with(ORDERS.keySerde(), ORDERS.valueSerde()))
            .reduce((order, old) -> order)
            .toStream().to(ORDERS.name(), Produced.with(ORDERS.keySerde(), ORDERS.valueSerde()));

        return builder;
    }

    @Override
    public void stop() {
        if (streams != null) {
            streams.close();
        }
        log.info("Service stopped");
    }

}
