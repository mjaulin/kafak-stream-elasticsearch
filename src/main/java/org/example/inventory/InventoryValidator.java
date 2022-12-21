package org.example.inventory;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.avro.Order;
import org.example.avro.OrderValidation;
import org.example.avro.Product;

import java.util.Optional;

import static org.example.avro.OrderValidationResult.FAIL;
import static org.example.avro.OrderValidationResult.PASS;
import static org.example.avro.OrderValidationType.INVENTORY_CHECK;
import static org.example.inventory.InventoryService.RESERVED_STOCK_STORE_NAME;

class InventoryValidator implements Transformer<Product, KeyValue<Order, Integer>, KeyValue<String, OrderValidation>> {

    private KeyValueStore<Product, Long> reservedStocksStore;

    @Override
    public void init(ProcessorContext context) {
        reservedStocksStore = context.getStateStore(RESERVED_STOCK_STORE_NAME);
    }

    @Override
    public KeyValue<String, OrderValidation> transform(Product key, KeyValue<Order, Integer> value) {

        final var order = value.key;
        final var inventory = value.value;
        final var reserved = Optional.ofNullable(reservedStocksStore.get(key)).orElse(0L);

        final var result = inventory - reserved - order.getQuantity() >= 0 ? PASS : FAIL;
        if (result == PASS) {
            reservedStocksStore.put(key, reserved + order.getQuantity());
        }

        return KeyValue.pair(order.getId(), new OrderValidation(order.getId(), INVENTORY_CHECK, result));
    }

    @Override
    public void close() {
        // nothing to do
    }

}
