package org.example;

import io.dropwizard.lifecycle.Managed;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.example.avro.OrderState;
import org.example.avro.OrderValidation;
import org.example.avro.OrderValue;

import java.time.Duration;

import static org.example.avro.OrderValidationResult.FAIL;
import static org.example.avro.OrderValidationResult.PASS;
import static org.example.avro.OrderValidationType.FRAUD_CHECK;
import static org.example.common.MicroserviceUtils.*;
import static org.example.common.Schemas.ORDER_VALUE_SERDE;
import static org.example.common.Schemas.Topics.ORDERS;
import static org.example.common.Schemas.Topics.ORDERS_VALIDATION;

@Slf4j
public class FraudService implements Managed {

    private KafkaStreams streams;
    private static final int FRAUD_LIMIT = 2000;

    public static void main(String[] args) throws InterruptedException {
        var service = new FraudService();
        service.start();
        addShutdownHookAndBlock(service);
    }

    @Override
    public void start() {
        var config = baseStreamsConfig("fraud-service");
        streams = startStreams(processStreams(), config);
        log.info("Service started");
    }

    private StreamsBuilder processStreams() {
        final var builder = new StreamsBuilder();
        final var orders = builder
                .stream(ORDERS.name(), Consumed.with(ORDERS.keySerde(), ORDERS.valueSerde()))
                .filter((id, order) -> OrderState.CREATED.equals(order.getState()));
        final var aggregate = orders
                .groupBy((id, order) -> order.getCustomerId(), Grouped.with(Serdes.Integer(), ORDERS.valueSerde()))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofHours(1)))
                .aggregate(OrderValue::new,
                        (custId, order, total) -> new OrderValue(order, total.getValue() + order.getQuantity() * order.getPrice()),
                        (k, a, b) -> new OrderValue(b.getOrder(), (a == null ? 0D : a.getValue()) + b.getValue()),
                        Materialized.with(null, ORDER_VALUE_SERDE)
                );
        final var ordersWithTotal = aggregate
                .toStream((windowedKey, orderValue) -> windowedKey.key())
                .filter((k, v) -> v != null)
                .selectKey((id, orderValue) -> orderValue.getOrder().getId());
        final var forks = ordersWithTotal.split(Named.as("limit-"))
                .branch((id, orderValue) -> orderValue.getValue() >= FRAUD_LIMIT, Branched.as("above"))
                .branch((id, orderValue) -> orderValue.getValue() < FRAUD_LIMIT, Branched.as("below"))
                .noDefaultBranch();

        forks.get("limit-above").mapValues(
                        orderValue -> new OrderValidation(orderValue.getOrder().getId(), FRAUD_CHECK, FAIL)
                )
                .to(ORDERS_VALIDATION.name(), Produced.with(ORDERS_VALIDATION.keySerde(), ORDERS_VALIDATION.valueSerde()));

        forks.get("limit-below").mapValues(
                        orderValue -> new OrderValidation(orderValue.getOrder().getId(), FRAUD_CHECK, PASS)
                )
                .to(ORDERS_VALIDATION.name(), Produced.with(ORDERS_VALIDATION.keySerde(), ORDERS_VALIDATION.valueSerde()));

        //disable caching to ensure a complete aggregate changelog. This is a little trick we need to apply
        //as caching in Kafka Streams will conflate subsequent updates for the same key. Disabling caching ensures
        //we get a complete "changelog" from the aggregate(...) step above (i.e. every input event will have a
        //corresponding output event.
        final var props = baseStreamsConfig("fraud-service");
        props.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

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
