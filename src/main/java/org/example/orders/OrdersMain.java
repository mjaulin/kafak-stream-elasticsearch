package org.example.orders;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.configuration.ResourceConfigurationSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.stream.Stream;

@Slf4j
public class OrdersMain extends Application<Configuration> {

    public static void main(String[] args) throws Exception {
        var appArgs = Stream.concat(Stream.of("server", "/config-orders.yml"), Arrays.stream(args))
                .toArray(String[]::new);
        new OrdersMain().run(appArgs);
    }

    @Override
    public void initialize(Bootstrap<Configuration> bootstrap) {
        super.initialize(bootstrap);
        bootstrap.setConfigurationSourceProvider(new ResourceConfigurationSourceProvider());
    }

    @Override
    public void run(Configuration configuration, Environment environment) {
        final var service = new OrderService();
        environment.lifecycle().manage(service);
        final var resource = new OrdersResource(service);
        environment.jersey().register(resource);
        log.info("App running");
    }

}
