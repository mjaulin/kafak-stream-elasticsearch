package org.example.payments;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.UUID;

import static org.example.common.MicroserviceUtils.setTimeout;


@Path("/payments")
@Slf4j
@RequiredArgsConstructor
public class PaymentsResource {

    private final PaymentService service;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public void submitOrder(final PaymentBean bean, @Suspended final AsyncResponse response) {
        setTimeout(response);
        var payment = bean.toPayment();
        payment.setId(UUID.randomUUID().toString());
        log.debug("Send {}", payment);
        service.createPayment(payment, e -> {
            if (e != null) {
                response.resume(e);
                return;
            }
            response.resume(Response.ok().build());
        });
    }

}
