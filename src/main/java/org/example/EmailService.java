package org.example;

import io.dropwizard.lifecycle.Managed;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.example.avro.Customer;
import org.example.avro.Order;
import org.example.avro.Payment;

import java.io.Closeable;
import java.time.Duration;

import static org.example.common.MicroserviceUtils.*;
import static org.example.common.Schemas.Topics.*;

@Slf4j
public class EmailService implements Managed {

    private KafkaStreams streams;

    public static void main(String[] args) throws InterruptedException {
        var service = new EmailService();
        service.start();
        addShutdownHookAndBlock(service);
    }

    @Override
    public void start() {
        var config = baseStreamsConfig("email-service");
        streams = startStreams(processStreams(), config);
        log.info("Service started");
    }

    private StreamsBuilder processStreams() {
        final var builder = new StreamsBuilder();

        final var orders = builder.stream(ORDERS.name(), Consumed.with(ORDERS.keySerde(), ORDERS.valueSerde()));
        final var payments = builder.stream(PAYMENTS.name(), Consumed.with(PAYMENTS.keySerde(), PAYMENTS.valueSerde()))
                .selectKey((s, payment) -> payment.getOrderId());
        final var customers = builder.globalTable(CUSTOMERS.name(), Consumed.with(CUSTOMERS.keySerde(), CUSTOMERS.valueSerde()));

        orders.join(
                payments,
                EmailTuple::new,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
                StreamJoined.with(ORDERS.keySerde(), ORDERS.valueSerde(), PAYMENTS.valueSerde())
        ).join(
                customers, (k, tuple) -> tuple.order.getCustomerId(), EmailTuple::setCustomer
        ).peek(
                (k, tuple) -> log.info("Sending email: {}", tuple)
        );

        return builder;
    }



    @Override
    public void stop() {
        if (streams != null) {
            streams.close();
        }
        log.info("Service stopped");
    }

    @ToString
    @RequiredArgsConstructor
    static class EmailTuple {
        private final Order order;
        private final Payment payment;
        private Customer customer;

        EmailTuple setCustomer(Customer customer) {
            this.customer = customer;
            return this;
        }
    }

}
