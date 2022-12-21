package org.example.payments;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.configuration.ResourceConfigurationSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import java.util.Arrays;
import java.util.stream.Stream;

public class PaymentsMain extends Application<Configuration> {

    public static void main(String[] args) throws Exception {
        var appArgs = Stream.concat(Stream.of("server", "/config-payments.yml"), Arrays.stream(args))
                .toArray(String[]::new);
        new PaymentsMain().run(appArgs);
    }

    @Override
    public void initialize(Bootstrap<Configuration> bootstrap) {
        super.initialize(bootstrap);
        bootstrap.setConfigurationSourceProvider(new ResourceConfigurationSourceProvider());
    }

    @Override
    public void run(Configuration configuration, Environment environment) {
        final var service = new PaymentService();
        environment.lifecycle().manage(service);
        final var resource = new PaymentsResource(service);
        environment.jersey().register(resource);
    }

}
