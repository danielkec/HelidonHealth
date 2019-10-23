package io.helidon.messaging.kafka;

import io.helidon.config.Config;

public final class SimpleKafkaClient {

    private SimpleKafkaClient() {
    }

    public static <K, V> SimpleKafkaConsumer<K, V> createConsumer(String consumerConfigId, Config config) {
        return new SimpleKafkaConsumer<K, V>(consumerConfigId, config);
    }

    public static <K, V> SimpleKafkaProducer<K, V> createProducer(String producerConfigId, Config config) {
        return new SimpleKafkaProducer<K, V>(producerConfigId, config);
    }
}
