package org.example;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.configuration.ResourceConfigurationSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import lombok.extern.slf4j.Slf4j;
import org.example.inventory.InventoryResource;
import org.example.inventory.InventoryService;
import org.example.orders.OrderService;
import org.example.orders.OrdersResource;
import org.example.payments.PaymentService;
import org.example.payments.PaymentsResource;

import java.util.Arrays;
import java.util.stream.Stream;

@Slf4j
public class All extends Application<Configuration> {

    public static void main(String[] args) throws Exception {
        var appArgs = Stream.concat(Stream.of("server", "/config-all.yml"), Arrays.stream(args))
                .toArray(String[]::new);
        new All().run(appArgs);
    }

    @Override
    public void initialize(Bootstrap<Configuration> bootstrap) {
        super.initialize(bootstrap);
        bootstrap.setConfigurationSourceProvider(new ResourceConfigurationSourceProvider());
    }

    @Override
    public void run(Configuration configuration, Environment environment) {
        final var orderService = new OrderService();
        final var paymentService = new PaymentService();
        final var inventoryService = new InventoryService();
        final var emailService = new EmailService();
        final var validationsAggregatorService = new ValidationsAggregatorService();
        final var fraudService = new FraudService();

        environment.lifecycle().manage(orderService);
        environment.lifecycle().manage(paymentService);
        environment.lifecycle().manage(inventoryService);
        environment.lifecycle().manage(emailService);
        environment.lifecycle().manage(validationsAggregatorService);
        environment.lifecycle().manage(fraudService);

        final var ordersResource = new OrdersResource(orderService);
        final var paymentsResource = new PaymentsResource(paymentService);
        final var inventoryResource = new InventoryResource(inventoryService);

        environment.jersey().register(ordersResource);
        environment.jersey().register(paymentsResource);
        environment.jersey().register(inventoryResource);

        log.info("App running");
    }

}
