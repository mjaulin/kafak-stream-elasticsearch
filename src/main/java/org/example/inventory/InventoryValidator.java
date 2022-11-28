package org.example.inventory;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.domain.Order;
import org.example.domain.OrderValidation;
import org.example.domain.Product;

import java.util.Optional;

import static org.example.domain.OrderValidation.OrderValidationResult.FAIL;
import static org.example.domain.OrderValidation.OrderValidationResult.PASS;
import static org.example.domain.OrderValidation.OrderValidationType.INVENTORY_CHECK;
import static org.example.inventory.InventoryService.WAREHOUSE_INVENTORY_STORE_NAME;

public class InventoryValidator implements Transformer<Product, KeyValue<Order, Long>, KeyValue<String, OrderValidation>> {

    private KeyValueStore<Product, Long> reservedStockStore;

    @Override
    public void init(ProcessorContext context) {
        reservedStockStore = context.getStateStore(WAREHOUSE_INVENTORY_STORE_NAME);
    }

    @Override
    public KeyValue<String, OrderValidation> transform(final Product product,
                                                       final KeyValue<Order, Long> orderAndStock) {
        final OrderValidation.OrderValidationResult result;
        final var order = orderAndStock.key;
        final var warehouseStock = orderAndStock.value;

        final var reserved = Optional.ofNullable(reservedStockStore.get(product)).orElse(0L);
        if (warehouseStock - reserved - order.getQuantity() >= 0) {
            reservedStockStore.put(product, reserved + order.getQuantity());
            result = PASS;
        } else {
            result = FAIL;
        }
        return new KeyValue<>(order.getId(), new OrderValidation(order.getId(), INVENTORY_CHECK, result));
    }

    @Override
    public void close() {
        // nothing to do
    }

}
