package org.example.inventory;

import io.dropwizard.lifecycle.Managed;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.example.domain.Order;

import static org.example.domain.Schemas.Topics.*;
import static org.example.util.MicroserviceUtils.baseStreamsConfig;

@Slf4j
public class InventoryService implements Managed {

    public static final String WAREHOUSE_INVENTORY_STORE_NAME = "warehouse-inventory-store";
    private KafkaStreams streams;

    @Override
    public void start() {
        streams = processStreams();
        streams.cleanUp(); // don't do this in prod as it clears your state stores
        streams.start();
    }

    private KafkaStreams processStreams() {
        final var builder = new StreamsBuilder();
        final var orders = builder.stream(
                ORDERS.name(),
                Consumed.with(ORDERS.keySerde(), ORDERS.valueSerde())
        );
        final var wharehouseInventory = builder.table(
                WAREHOUSE_INVENTORY.name(),
                Consumed.with(WAREHOUSE_INVENTORY.keySerde(), WAREHOUSE_INVENTORY.valueSerde())
        );
        final var reservedStock = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(WAREHOUSE_INVENTORY_STORE_NAME),
                WAREHOUSE_INVENTORY.keySerde(), WAREHOUSE_INVENTORY.valueSerde()
        );
        builder.addStateStore(reservedStock);

        orders.selectKey((id, order) -> order.getProduct())
                .filter((id, order) -> Order.OrderState.CREATED.equals(order.getState()))
                .join(wharehouseInventory, KeyValue::new,
                        Joined.with(WAREHOUSE_INVENTORY.keySerde(), ORDERS.valueSerde(), WAREHOUSE_INVENTORY.valueSerde())
                )
                .transform(InventoryValidator::new, WAREHOUSE_INVENTORY_STORE_NAME)
                .to(ORDER_VALIDATION.name(), Produced.with(ORDER_VALIDATION.keySerde(), ORDER_VALIDATION.valueSerde()));

        return new KafkaStreams(builder.build(), baseStreamsConfig("inventory-service"));
    }

    @Override
    public void stop() {
        if (streams != null) {
            streams.close();
        }
    }

}
