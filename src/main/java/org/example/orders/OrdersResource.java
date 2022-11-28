package org.example.orders;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import io.dropwizard.lifecycle.Managed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticsearch.client.RestClient;
import org.example.domain.Order;

import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.example.domain.Order.OrderState.CREATED;
import static org.example.domain.Schemas.Topics.ORDERS;
import static org.example.util.MicroserviceUtils.*;


@Path("/orders")
@Slf4j
@RequiredArgsConstructor
public class OrdersResource implements Managed {

    private RestClient restClient;
    private ElasticsearchAsyncClient elasticsearchClient;
    private KafkaProducer<String, Order> producer;

    @GET
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public void getOrder(@PathParam("id") final String id, @Suspended final AsyncResponse response) throws ExecutionException, InterruptedException {
        setTimeout(response);
        elasticsearchClient.search(
                s -> s
                        .index("orders")
                        .query(q -> q.match(t -> t.field("id").query(id))),
                Order.class
        ).thenAccept(r -> {
            final var hits = r.hits().hits();
            if (hits.size() != 1) {
                response.resume(Response.status(Response.Status.NOT_FOUND));
                return;
            }
            response.resume(Response.ok(hits.get(0).source()).build());
        }).get();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public void submitOrder(final Order order, @Suspended final AsyncResponse response) {
        setTimeout(response);
        order.setId(UUID.randomUUID().toString());
        order.setState(CREATED);
        if (order.getPrice() < 0 || order.getQuantity() < 0 || order.getProduct() == null || order.getCustomerId() == null) {
            response.resume(Response.status(400).entity("Invalid request"));
            return;
        }
        producer.send(new ProducerRecord<>(ORDERS.name(), order.getId(), order), (r, e) -> {
            if (e != null) {
                response.resume(e);
                return;
            }
            try {
                final var uri = Response.created(new URI("/orders/" + order.getId()));
                response.resume(uri.build());
            } catch (final URISyntaxException e2) {
                log.error(e2.getMessage(), e2);
            }
        });
    }

    @Override
    public void start() {
        producer = createProducer(ORDERS, "order-sender");
        restClient = RestClient.builder(new HttpHost("localhost", 9200)).build();
        elasticsearchClient = new ElasticsearchAsyncClient(new RestClientTransport(restClient, new JacksonJsonpMapper()));
    }

    @Override
    public void stop() throws IOException {
        if (producer != null) {
            producer.close();
        }
        if (restClient != null) {
            restClient.close();
        }
    }

}
