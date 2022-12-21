package org.example.inventory;

import io.dropwizard.lifecycle.Managed;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.example.avro.OrderState;
import org.example.avro.Product;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.example.common.MicroserviceUtils.*;
import static org.example.common.Schemas.Topics.*;

@Slf4j
public class InventoryService implements Managed {

    private KafkaProducer<Product, Integer> producer;
    private KafkaStreams streams;

    public static final String RESERVED_STOCK_STORE_NAME = "store-of-reserved-stock";

    void newDelivery(final Map<Product, Integer> delivery, Runnable callback) throws ExecutionException, InterruptedException {
        if (log.isDebugEnabled()) {
            log.debug("Create {}", delivery.keySet().stream().map(key -> key + "=" + delivery.get(key))
                    .collect(Collectors.joining(", ", "{", "}")));
        }
        var send = delivery.entrySet().stream()
                .map(p -> {
                    log.debug("Send key={} value={}", p.getKey(), p.getValue());
                    return producer.send(new ProducerRecord<>(WAREHOUSE_INVENTORY.name(), p.getKey(), p.getValue()));
                })
                .map(f -> CompletableFuture.supplyAsync(() -> {
                    try {
                        return f.get();
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                }))
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(send).thenAccept(v -> callback.run()).get();
    }


    @Override
    public void start() {
        producer = createProducer(WAREHOUSE_INVENTORY, "inventory-sender");
        streams = startStreams(processStream(), baseStreamsConfig("inventory-service"));
        log.info("Service started");
    }

    private StreamsBuilder processStream() {
        final var builder = new StreamsBuilder();
        final var orders = builder.stream(ORDERS.name(), Consumed.with(ORDERS.keySerde(), ORDERS.valueSerde()));
        final var inventory = builder.table(WAREHOUSE_INVENTORY.name(), Consumed.with(WAREHOUSE_INVENTORY.keySerde(), WAREHOUSE_INVENTORY.valueSerde()));

        final var reservedStock = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(RESERVED_STOCK_STORE_NAME), WAREHOUSE_INVENTORY.keySerde(), Serdes.Long()
        );

        builder.addStateStore(reservedStock);

        orders.selectKey((id, order) -> order.getProduct())
                .filter((id, order) -> OrderState.CREATED.equals(order.getState()))
                .join(inventory, KeyValue::new, Joined.with(WAREHOUSE_INVENTORY.keySerde(), ORDERS.valueSerde(), Serdes.Integer()))
                .transform(InventoryValidator::new, RESERVED_STOCK_STORE_NAME)
                .to(ORDERS_VALIDATION.name(), Produced.with(ORDERS_VALIDATION.keySerde(), ORDERS_VALIDATION.valueSerde()));

        return builder;
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

}
