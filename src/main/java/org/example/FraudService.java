package org.example;

import io.dropwizard.lifecycle.Managed;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.example.domain.Order;
import org.example.domain.OrderValidation;
import org.example.domain.OrderValue;

import java.time.Duration;
import java.util.Properties;

import static org.example.domain.OrderValidation.OrderValidationResult.FAIL;
import static org.example.domain.OrderValidation.OrderValidationResult.PASS;
import static org.example.domain.OrderValidation.OrderValidationType.FRAUD_CHECK;
import static org.example.domain.Schemas.ORDER_VALUE_SERDE;
import static org.example.domain.Schemas.Topics.ORDERS;
import static org.example.domain.Schemas.Topics.ORDER_VALIDATION;
import static org.example.util.MicroserviceUtils.*;

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
        streams = processStreams();
        startStreams(streams);
    }

    private KafkaStreams processStreams() {
        final var builder = new StreamsBuilder();
        final var orders = builder
                .stream(ORDERS.name(), Consumed.with(ORDERS.keySerde(), ORDERS.valueSerde()))
                .filter((id, order) -> Order.OrderState.CREATED.equals(order.getState()));
        final var aggregate = orders
                .groupBy((id, order) -> order.getCustomerId(), Grouped.with(Serdes.Long(), ORDERS.valueSerde()))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofHours(1)))
                .aggregate(OrderValue::new,
                        (custId, order, total) -> new OrderValue(order, total.getValue() + order.getQuantity() * order.getPrice()),
                        (k, a, b) -> new OrderValue(b.getOrder(), (a == null ? 0D: a.getValue()) + b.getValue()),
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
                .to(ORDER_VALIDATION.name(), Produced.with(ORDER_VALIDATION.keySerde(), ORDER_VALIDATION.valueSerde()));

        forks.get("limit-below").mapValues(
                        orderValue -> new OrderValidation(orderValue.getOrder().getId(), FRAUD_CHECK, PASS)
                )
                .to(ORDER_VALIDATION.name(), Produced.with(ORDER_VALIDATION.keySerde(), ORDER_VALIDATION.valueSerde()));

        //disable caching to ensure a complete aggregate changelog. This is a little trick we need to apply
        //as caching in Kafka Streams will conflate subsequent updates for the same key. Disabling caching ensures
        //we get a complete "changelog" from the aggregate(...) step above (i.e. every input event will have a
        //corresponding output event.
        final Properties props = baseStreamsConfig("fraud-service");
        props.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        return new KafkaStreams(builder.build(), props);
    }

    @Override
    public void stop() {
        if (streams != null) {
            streams.close();
        }
    }

}
