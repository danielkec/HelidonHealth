package io.helidon.examples.sport.graph;

import io.helidon.config.Config;
import io.helidon.messaging.kafka.SimpleKafkaClient;

public class TestConsumer {

    public static void main(String[] args) {
        SimpleKafkaClient
                .<Long, String>createConsumer("job-done-consumer", Config.create())
                .consumeAsync(r -> System.out.println(r.value()));
    }

}
