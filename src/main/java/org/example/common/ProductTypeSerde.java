package org.example.common;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.example.avro.Product;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ProductTypeSerde implements Serde<Product> {
    @Override
    public Serializer<Product> serializer() {
        return (topic, product) -> product.toString().getBytes(UTF_8);
    }

    @Override
    public Deserializer<Product> deserializer() {
        return (topic, data) -> Product.valueOf(new String(data, UTF_8));
    }
}
