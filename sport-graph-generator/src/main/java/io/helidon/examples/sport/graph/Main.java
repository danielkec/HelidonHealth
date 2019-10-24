/*
 * Copyright (c) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.helidon.examples.sport.graph;

import io.helidon.config.Config;
import io.helidon.media.jsonp.server.JsonSupport;
import io.helidon.messaging.kafka.SimpleKafkaConsumer;
import io.helidon.messaging.kafka.SimpleKafkaProducer;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerConfiguration;
import io.helidon.webserver.StaticContentSupport;
import io.helidon.webserver.WebServer;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * The application main class.
 */
public final class Main {

    private static Logger LOG = Logger.getLogger(Main.class.getName());

    /**
     * Cannot be instantiated.
     */
    private Main() {
    }

    /**
     * Application main entry point.
     *
     * @param args command line arguments.
     * @throws IOException if there are problems reading logging properties
     */
    public static void main(final String[] args) throws IOException {
        startServer();
    }

    /**
     * Start the server.
     *
     * @return the created {@link io.helidon.webserver.WebServer} instance
     * @throws IOException if there are problems reading logging properties
     */
    static WebServer startServer() throws IOException {

        // load logging configuration
        setupLogging();

        // By default this will pick up application.yaml from the classpath
        Config config = Config.create();

        // Get webserver config from the "server" section of application.yaml
        ServerConfiguration serverConfig =
                ServerConfiguration.create(config.get("server"));

        // Set up kafka consumers
        SimpleKafkaConsumer<Long, String> graphQueueConsumer = new SimpleKafkaConsumer<>("graph-queue-consumer", config);
        SimpleKafkaProducer<Long, String> jobDoneProducer = new SimpleKafkaProducer<>("job-done-producer", Config.create());

        // Set up services
        GraphService graphService = new GraphService(graphQueueConsumer, jobDoneProducer, config);

        WebServer server = WebServer.create(serverConfig, createRouting(graphService, config));
        server.start()
                .thenAccept(ws -> LOG.info(String.format("WEB server is up! http://localhost:%s/console", ws.port())))
                .exceptionally(t -> {
                    LOG.severe(String.format("Startup failed: %s", t.getMessage()));
                    t.printStackTrace(System.err);
                    return null;
                });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down ...");
            graphQueueConsumer.close();
            server.shutdown();
        }));

        // Server threads are not daemon. No need to block. Just react.
        return server;
    }

    /**
     * Creates new {@link io.helidon.webserver.Routing}.
     *
     * @param graphService
     * @param config       configuration of this server
     * @return routing configured with JSON support, a health check, and a service
     */
    private static Routing createRouting(GraphService graphService, Config config) {
        return Routing.builder()
                .register(JsonSupport.create())
                .register("/graph", graphService)
                .register("/console", StaticContentSupport.builder("console").welcomeFileName("index.html").build())
                .build();
    }

    /**
     * Configure logging from logging.properties file.
     */
    private static void setupLogging() throws IOException {
        try (InputStream is = Main.class.getResourceAsStream("/logging.properties")) {
            LogManager.getLogManager().readConfiguration(is);
        }
    }

}
