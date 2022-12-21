package org.example.orders;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.avro.OrderState;

import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

import static org.example.common.MicroserviceUtils.setTimeout;


@Path("/orders")
@Slf4j
@RequiredArgsConstructor
public class OrdersResource {

    private final OrderService service;

    @GET
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public void getOrder(@PathParam("id") final String id, @Suspended final AsyncResponse response) {
        setTimeout(response);
        service.fetchOrder(id, order -> response.resume(new OrderBean(order)), v -> true);
    }

    @GET
    @Path("/{id}/validated")
    @Produces(MediaType.APPLICATION_JSON)
    public void getOrderValidated(@PathParam("id") final String id, @Suspended final AsyncResponse response) {
        setTimeout(response);
        service.fetchOrder(id, order -> response.resume(new OrderBean(order)),
                v -> v.getState() == OrderState.VALIDATED || v.getState() == OrderState.FAILED);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public void submitOrder(final OrderBean bean, @Suspended final AsyncResponse response) {
        setTimeout(response);
        var order = bean.toOrder();
        order.setId(UUID.randomUUID().toString());
        order.setState(OrderState.CREATED);
        if (order.getPrice() < 0 || order.getQuantity() < 0 || order.getProduct() == null || order.getCustomerId() == -1L) {
            log.error("Invalid order {}", order);
            response.resume(Response.status(400).entity("Invalid request"));
            return;
        }
        log.debug("Send {}", order);
        service.createOrder(order, e -> {
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

}
