package io.helidon.messaging.kafka;

import io.helidon.config.Config;
import io.helidon.config.ConfigSources;
import io.helidon.config.spi.ConfigNode;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public final class TestKafkaConfig {

    private List<HybridConfig> hybridConfigs = new ArrayList<>();
    private String bootstrapServers;

    private TestKafkaConfig(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public static TestKafkaConfig create(String bootstrapServers) {
        return new TestKafkaConfig(bootstrapServers);
    }

    public TestKafkaConfig consumer(String name, String topicName) {
        return consumer(name, topicName, null);
    }

    public TestKafkaConfig consumer(String name, String topicName, Consumer<HybridConfig> consumerFnc) {
        HybridConfig consumerConfig = new HybridConfig(name);
        consumerConfig
                .add("topic_name", topicName)
                .add("bootstrap_servers", bootstrapServers)
                .add("key_deserializer", LongDeserializer.class.getName())
                .add("value_deserializer", StringDeserializer.class.getName());
        Optional.ofNullable(consumerFnc).ifPresent(c -> c.accept(consumerConfig));
        hybridConfigs.add(consumerConfig);
        return this;
    }

    public TestKafkaConfig producer(String name, String topicName) {
        return producer(name, topicName, null);
    }

    public TestKafkaConfig producer(String name, String topicName, Consumer<HybridConfig> consumerFnc) {
        HybridConfig producerConfig = new HybridConfig(name);
        producerConfig
                .add("topic_name", topicName)
                .add("bootstrap_servers", bootstrapServers)
                .add("key_serializer", LongSerializer.class.getName())
                .add("value_serializer", StringSerializer.class.getName());
        Optional.ofNullable(consumerFnc).ifPresent(c -> c.accept(producerConfig));
        hybridConfigs.add(producerConfig);
        return this;
    }

    public Config build() {
        ConfigNode.ObjectNode.Builder b = ConfigNode.ObjectNode.builder();
        hybridConfigs.forEach(h -> b.addObject(h.name, h.build()));
        ConfigNode.ObjectNode objectNode = b.build();
        return Config.builder()
                .sources(
                        ConfigSources.create(
                                ConfigNode.ObjectNode.builder()
                                        .addObject("kafka", objectNode).build()
                        )
                ).build();
    }

    public class HybridConfig {
        private final ConfigNode.ObjectNode.Builder builder;
        private String name;

        private HybridConfig(String name) {
            this.name = name;
            this.builder = ConfigNode.ObjectNode.builder();
        }

        public HybridConfig add(String key, String value) {
            this.builder.addValue(key, value);
            return this;
        }

        private ConfigNode.ObjectNode build() {
            return this.builder.build();
        }
    }
}
