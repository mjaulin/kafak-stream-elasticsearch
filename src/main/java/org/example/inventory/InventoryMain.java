package org.example.inventory;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Environment;

public class InventoryMain extends Application<Configuration> {

    public static void main(String[] args) throws Exception {
        new InventoryMain().run(args);
    }

    @Override
    public void run(Configuration configuration, Environment environment) {
        final var service = new InventoryService();
        final var resource = new InventoryResource();
        environment.lifecycle().manage(resource);
        environment.lifecycle().manage(service);
        environment.jersey().register(resource);
    }

}
