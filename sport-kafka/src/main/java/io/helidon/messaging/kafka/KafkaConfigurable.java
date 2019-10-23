package io.helidon.messaging.kafka;

import io.helidon.config.Config;

import java.util.Properties;

public abstract class KafkaConfigurable {

    public static final String TOPIC_NAME = "topic.name";
    public static final String GROUP_ID = "group.id";

    private final Properties properties = new Properties();

    KafkaConfigurable(String confName, Config config) {
        config.get("kafka").get(confName).asNodeList().get()
                .forEach(this::addProperty);
    }

    private void addProperty(Config c) {
        String key = c.key().name().replaceAll("_", ".");
        this.properties.setProperty(key, c.asString().get());
    }

    protected String getProperty(String key) {
        return this.properties.getProperty(key);
    }

    protected void setProperty(String key, String value) {
        this.properties.setProperty(key, value);
    }

    protected Properties getProperties() {
        return this.properties;
    }
}
