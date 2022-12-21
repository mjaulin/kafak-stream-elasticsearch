package org.example.payments;

import io.dropwizard.lifecycle.Managed;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.avro.Payment;

import java.util.function.Consumer;

import static org.example.common.MicroserviceUtils.createProducer;
import static org.example.common.Schemas.Topics.PAYMENTS;

@Slf4j
public class PaymentService implements Managed {

    private KafkaProducer<String, Payment> producer;

    void createPayment(Payment payment, Consumer<Throwable> callback) {
        producer.send(new ProducerRecord<>(PAYMENTS.name(), payment.getId(), payment), (r, e) -> callback.accept(e));
    }

    @Override
    public void start() {
        producer = createProducer(PAYMENTS, "payment-service");
        log.info("Service started");
    }

    @Override
    public void stop() {
        if (producer != null) {
            producer.close();
        }
        log.info("Service stopped");
    }

}
