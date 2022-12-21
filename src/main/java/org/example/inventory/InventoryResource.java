package org.example.inventory;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.avro.Product;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.example.common.MicroserviceUtils.setTimeout;

@Path("/inventory")
@Slf4j
@RequiredArgsConstructor
public class InventoryResource {

    private final InventoryService service;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public void newDelivery(final Map<Product, Integer> delivery, @Suspended final AsyncResponse response) throws ExecutionException, InterruptedException {
        setTimeout(response);
        service.newDelivery(delivery, () -> response.resume(Response.ok().build()));
    }

}
