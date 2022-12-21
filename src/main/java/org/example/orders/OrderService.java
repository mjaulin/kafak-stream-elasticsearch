package org.example.orders;

import io.dropwizard.lifecycle.Managed;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.example.avro.Order;
import org.example.common.MicroserviceUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.example.common.MicroserviceUtils.baseStreamsConfig;
import static org.example.common.MicroserviceUtils.createProducer;
import static org.example.common.Schemas.Topics.ORDERS;

@Slf4j
public class OrderService implements Managed {

    private KafkaProducer<String, Order> producer;
    private KafkaStreams streams;
    private static final String ORDERS_STORE_NAME = "orders-store";

    // TODO : In a real implementation we would need to (a) support outstanding requests for the same Id/filter from
    // different users and (b) periodically purge old entries from this map.
    private final Map<String, List<FilteredResponse<Order>>> outstandingRequests = new ConcurrentHashMap<>();

    void createOrder(final Order order, final Consumer<Throwable> callback) {
        log.debug("Create {}", order);
        producer.send(new ProducerRecord<>(ORDERS.name(), order.getId(), order), (r, e) -> callback.accept(e));
    }

    void fetchOrder(final String id, final Consumer<Order> callback, final Predicate<Order> predicate) {
        log.info("fetch order {}", id);
        try {
            final var order = streams
                    .store(StoreQueryParameters.fromNameAndType(ORDERS_STORE_NAME, QueryableStoreTypes.<String, Order>keyValueStore()))
                    .get(id);
            if (order == null || !predicate.test(order)) {
                log.info("Delaying get as order not present for id {}", id);
                outstandingRequests.computeIfAbsent(id, k -> new ArrayList<>())
                        .add(new FilteredResponse<>(callback, predicate));
            } else {
                callback.accept(order);
            }
        } catch (final InvalidStateStoreException e) {
            //Store not ready so delay
            outstandingRequests.computeIfAbsent(id, k -> new ArrayList<>())
                    .add(new FilteredResponse<>(callback, predicate));
        }
    }

    private KafkaStreams startStreams() {
        final var builder = new StreamsBuilder();
        builder.table(ORDERS.name(), Consumed.with(ORDERS.keySerde(), ORDERS.valueSerde()), Materialized.as(ORDERS_STORE_NAME))
                .toStream().foreach((id, order) -> outstandingRequests.getOrDefault(id, List.of())
                        .stream().filter(fr -> fr.predicate.test(order))
                        .forEach(fr -> fr.callback.accept(order)));
        return MicroserviceUtils.startStreams(builder, baseStreamsConfig("order-service"));
    }

    @Override
    public void start() {
        producer = createProducer(ORDERS, "order-sender");
        streams = startStreams();
        log.info("Service started");
    }

    @Override
    public void stop() {
        if (producer != null) {
            producer.close();
        }
        if (streams != null) {
            streams.close();
        }
        log.info("Service stopped");
    }

    record FilteredResponse<V>(Consumer<V> callback, Predicate<V> predicate) {}

}
