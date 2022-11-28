package org.example.domain;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.example.util.serdes.JacksonSerde;
import org.example.util.serdes.ProductSerde;

import java.util.HashMap;
import java.util.Map;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Schemas {

    public static final JacksonSerde<OrderValue> ORDER_VALUE_SERDE = new JacksonSerde<>(OrderValue.class);

    public record Topic<K, V>(String name, Serde<K> keySerde, Serde<V> valueSerde) {
        public Topic(String name, Serde<K> keySerde, Serde<V> valueSerde) {
            this.name = name;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
            Topics.ALL.put(name, this);
        }

        public String toString() {
            return name;
        }
    }

    public static class Topics {
        public final static Map<String, Topic<?, ?>> ALL = new HashMap<>();
        public static Topic<String, Order> ORDERS;
        public static Topic<Product, Long> WAREHOUSE_INVENTORY;
        public static Topic<String, OrderValidation> ORDER_VALIDATION;

        static {
            createTopics();
        }

        private static void createTopics() {
            ORDERS = new Topic<>("orders", Serdes.String(), new JacksonSerde<>(Order.class));
            WAREHOUSE_INVENTORY = new Topic<>("warehouse-inventory", new ProductSerde(), Serdes.Long());
            ORDER_VALIDATION = new Topic<>("order-validation", Serdes.String(), new JacksonSerde<>(OrderValidation.class));
        }
    }

}
