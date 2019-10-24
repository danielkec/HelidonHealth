/*
 * Copyright (c) 2019 Oracle and/or its affiliates. All rights reserved.
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

package io.helidon.messaging.kafka;

import io.helidon.config.Config;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Prepare Kafka properties from Helidon {@link io.helidon.config.Config Config},
 * underscores in keys are translated to dots.
 * <p>
 * See example with YAML configuration:
 * <pre>{@code
 * kafka:
 *   job-done-producer:
 *     topic_name: graph-done,different_topic
 *     bootstrap_servers: firstBroker:9092,secondBroker:9092
 *     key_serializer: org.apache.kafka.common.serialization.LongSerializer
 *     value_serializer: org.apache.kafka.common.serialization.StringSerializer
 *
 *   job-done-consumer:
 *     topic_name: graph-done,different_topic
 *     bootstrap_servers: firstBroker:9092,secondBroker:9092
 *     key_deserializer: org.apache.kafka.common.serialization.LongDeserializer
 *     value_deserializer: org.apache.kafka.common.serialization.StringDeserializer
 * }</pre>
 * <p>
 *
 * @see io.helidon.config.Config
 */
public class KafkaConfigProperties extends Properties {

    /**
     * Topic or topics delimited by commas
     */
    static final String TOPIC_NAME = "topic.name";

    /**
     * Consumer group id
     */
    static final String GROUP_ID = "group.id";

    /**
     * Prepare Kafka properties from Helidon {@link io.helidon.config.Config Config},
     * underscores in keys are translated to dots.
     *
     * @param confName name of the configuration key under kafka
     *                 (e.g. job-done-producer from {@link KafkaConfigProperties example})
     * @param config   parent config of kafka key
     */
    KafkaConfigProperties(String confName, Config config) {
        config.get("kafka").get(confName).asNodeList().get().forEach(this::addProperty);
    }

    private void addProperty(Config c) {
        String key = c.key().name().replaceAll("_", ".");
        this.setProperty(key, c.asString().get());
    }

    protected List<String> getTopicNameList() {
        return Arrays.stream(getProperty(TOPIC_NAME)
                .split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }
}
