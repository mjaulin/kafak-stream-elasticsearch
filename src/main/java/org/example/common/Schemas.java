package org.example.common;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.example.avro.*;

import java.util.HashMap;
import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Schemas {

    public static SpecificAvroSerde<OrderValue> ORDER_VALUE_SERDE = new SpecificAvroSerde<>();

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
        public static Topic<Product, Integer> WAREHOUSE_INVENTORY;
        public static Topic<String, OrderValidation> ORDERS_VALIDATION;
        public static Topic<String, Payment> PAYMENTS;
        public static Topic<Integer, Customer> CUSTOMERS;

        static {
            createTopics();
        }

        private static void createTopics() {
            ORDERS = new Topic<>("orders", Serdes.String(), new SpecificAvroSerde<>());
            WAREHOUSE_INVENTORY = new Topic<>("warehouse-inventory", new ProductTypeSerde(), Serdes.Integer());
            ORDERS_VALIDATION = new Topic<>("orders-validation", Serdes.String(), new SpecificAvroSerde<>());
            PAYMENTS = new Topic<>("payments", Serdes.String(), new SpecificAvroSerde<>());
            CUSTOMERS = new Topic<>("customers", Serdes.Integer(), new SpecificAvroSerde<>());
            Topics.ALL.values().forEach(topic -> {
                configureSerdes(topic.keySerde(), true);
                configureSerdes(topic.valueSerde(), false);
            });
            configureSerdes(ORDER_VALUE_SERDE, false);
        }

        private static void configureSerdes(Serde<?> serde, boolean isKey) {
            if (serde instanceof SpecificAvroSerde) {
                serde.configure(Map.of(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"), isKey);
            }
        }

    }

}
