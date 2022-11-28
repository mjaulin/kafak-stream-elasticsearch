package org.example.util.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.example.domain.Product;

import java.nio.charset.StandardCharsets;

public class ProductSerde implements Serde<Product> {

    private final Serializer<Product> serializer = new ProductSerializer();
    private final Deserializer<Product> deserializer = new ProductDeserializer();

    @Override
    public Serializer<Product> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Product> deserializer() {
        return deserializer;
    }

    public static class ProductSerializer implements Serializer<Product> {

        @Override
        public byte[] serialize(String s, Product product) {
            return product.toString().getBytes(StandardCharsets.UTF_8);
        }

    }

    public static class ProductDeserializer implements Deserializer<Product> {

        @Override
        public Product deserialize(String s, byte[] bytes) {
            return Product.valueOf(new String(bytes, StandardCharsets.UTF_8));
        }

    }

}
