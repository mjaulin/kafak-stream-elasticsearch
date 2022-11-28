package org.example.util.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

public class JacksonSerde<T> implements Serde<T> {

    private final Serializer<T> serializer;
    private final Deserializer<T> deserializer;

    public JacksonSerde(final Class<T> serdeForType) {
        final var objMapper = new ObjectMapper();
        this.serializer = new JacksonSerializer<>(objMapper);
        this.deserializer = new JaksonDeserializer<>(objMapper, serdeForType);
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }

    @RequiredArgsConstructor
    public static class JacksonSerializer<T> implements Serializer<T> {

        private final ObjectMapper objectMapper;

        @Override
        public byte[] serialize(String s, T object) {
            if (object == null) {
                return null;
            }
            try {
                return objectMapper.writeValueAsBytes(object);
            } catch (JsonProcessingException e) {
                throw new SerializationException("Error serializing message", e);
            }
        }

    }

    @RequiredArgsConstructor
    public static class JaksonDeserializer<T> implements Deserializer<T> {

        private final ObjectMapper objectMapper;
        private final Class<T> forType;

        @Override
        public T deserialize(String s, byte[] bytes) {
            if (bytes == null) {
                return null;
            }
            try {
                return objectMapper.readValue(bytes, forType);
            } catch (IOException e) {
                throw new SerializationException("Error deserializing message", e);
            }
        }

    }

}
