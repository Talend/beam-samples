package org.apache.beam.samples.iot;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;

import java.util.Random;
import java.util.logging.Logger;

public class MqttInjector {

    private final static Logger LOGGER = Logger.getLogger("name");

    public static void main(String args[]) throws Exception {
        CamelContext context = new DefaultCamelContext();

        final Processor processor = new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                Random rand = new Random();
                exchange.getIn().setBody("Event " + rand.nextInt(), String.class);
            }
        };

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("timer:fire?period=5000")
                        .process(processor)
                        .to("mqtt:beam?publishTopicName=BEAM");
            }
        });

        context.start();

        while (true);
    }
}
