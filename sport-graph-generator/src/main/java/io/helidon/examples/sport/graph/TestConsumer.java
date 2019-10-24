package io.helidon.examples.sport.graph;

import io.helidon.config.Config;
import io.helidon.messaging.kafka.SimpleKafkaConsumer;

public class TestConsumer {

    public static void main(String[] args) throws InterruptedException {
        SimpleKafkaConsumer<Long, String> consumer = new SimpleKafkaConsumer<>("job-done-consumer", Config.create());
        consumer.consumeAsync(r -> System.out.println(r.value()));

        Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

        Thread.sleep(60 * 60 * 1000);
    }

}
