package org.example.orders;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Environment;

public class OrdersMain extends Application<Configuration> {

    public static void main(String[] args) throws Exception {
        new OrdersMain().run(args);
    }

    @Override
    public void run(Configuration configuration, Environment environment) {
        final var resource = new OrdersResource();
        environment.lifecycle().manage(resource);
        environment.jersey().register(resource);
    }

}
