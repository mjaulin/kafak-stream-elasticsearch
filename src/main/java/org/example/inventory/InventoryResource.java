package org.example.inventory;

import io.dropwizard.lifecycle.Managed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.domain.Product;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.example.domain.Schemas.Topics.WAREHOUSE_INVENTORY;
import static org.example.util.MicroserviceUtils.createProducer;
import static org.example.util.MicroserviceUtils.setTimeout;

@Path("/inventory")
@Slf4j
@RequiredArgsConstructor
public class InventoryResource implements Managed {

    private KafkaProducer<Product, Long> producer;

    @Override
    public void start() {
        producer = createProducer(WAREHOUSE_INVENTORY, "inventory-sender");
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public void submitOrder(final Map<String, Long> inventory, @Suspended final AsyncResponse response) throws ExecutionException, InterruptedException {
        setTimeout(response);
        var send = inventory.entrySet().stream()
                .map(e -> Arrays.stream(Product.values()).filter(v -> v.name().equals(e.getKey().toUpperCase()))
                        .findFirst()
                        .map(v -> Map.entry(v, e.getValue()))
                )
                .filter(Optional::isPresent).map(Optional::get)
                .map(p -> producer.send(new ProducerRecord<>(WAREHOUSE_INVENTORY.name(), p.getKey(), p.getValue())))
                .map(f -> CompletableFuture.supplyAsync(() -> {
                    try {
                        return f.get();
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                }))
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(send).thenAccept(v -> response.resume(Response.ok().build())).get();
    }

    @Override
    public void stop() {
        if (producer != null) {
            producer.close();
        }
    }

}
