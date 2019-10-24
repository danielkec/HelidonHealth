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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * Simple Kafka consumer covering basic use-cases.
 * Configurable by Helidon {@link io.helidon.config.Config Config},
 * For more info about configuration see {@link KafkaConfigProperties}
 * <p/>
 * Usage:
 * <pre>{@code
 *   try (SimpleKafkaConsumer<Long, String> c = new SimpleKafkaConsumer<>("job-done-consumer", Config.create())) {
 *         c.consumeAsync(r -> System.out.println(r.value()));
 *   }
 * }</pre>
 *
 * @param <K> Key type
 * @param <V> Value type
 * @see KafkaConfigProperties
 * @see io.helidon.config.Config
 */
public class SimpleKafkaConsumer<K, V> implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaConsumer.class.getName());
    private final KafkaConfigProperties properties;

    private AtomicBoolean closed = new AtomicBoolean(false);
    private String consumerId;
    private ExecutorService executorService;
    private List<String> topicNameList;
    private KafkaConsumer<K, V> consumer;

    /**
     * Kafka consumer created from {@link io.helidon.config.Config config} under kafka->consumerId,
     * see configuration {@link KafkaConfigProperties example}.
     *
     * @param consumerId key in configuration
     * @param config     Helidon {@link io.helidon.config.Config config}
     * @see KafkaConfigProperties
     * @see io.helidon.config.Config
     */
    public SimpleKafkaConsumer(String consumerId, Config config) {
        this(consumerId, config, null);
    }

    /**
     * Kafka consumer created from {@link io.helidon.config.Config config} under kafka->consumerId,
     * see configuration {@link KafkaConfigProperties example}.
     *
     * @param consumerId      key in configuration
     * @param config          Helidon {@link io.helidon.config.Config config}
     * @param consumerGroupId Custom group.id, can be null, overrides group.id from configuration
     * @see KafkaConfigProperties
     * @see io.helidon.config.Config
     */
    public SimpleKafkaConsumer(String consumerId, Config config, String consumerGroupId) {
        properties = new KafkaConfigProperties(consumerId, config);
        properties.setProperty(KafkaConfigProperties.GROUP_ID, getOrGenerateGroupId(consumerGroupId));
        this.topicNameList = properties.getTopicNameList();
        this.consumerId = consumerId;
        consumer = new KafkaConsumer<>(properties);
    }

    /**
     * Execute supplied consumer for each received record
     *
     * @param function to be executed for each received record
     */
    public Future<?> consumeAsync(Consumer<ConsumerRecord<K, V>> function) {
        return this.consumeAsync(Executors.newWorkStealingPool(), null, function);
    }

    /**
     * Execute supplied consumer by provided executor service for each received record
     *
     * @param executorService Custom executor service used for spinning up polling thread and record consuming threads
     * @param customTopics    Can be null, list of topics appended to the list from configuration
     * @param function        Consumer method executed in new thread for each received record
     * @return The Future's get method will return null when consumer is closed
     */
    public Future<?> consumeAsync(ExecutorService executorService, List<String> customTopics,
                                  Consumer<ConsumerRecord<K, V>> function) {
        LOGGER.info(String.format("Initiating kafka consumer %s listening to topics: %s with groupId: %s",
                consumerId, topicNameList, properties.getProperty(KafkaConfigProperties.GROUP_ID)));

        List<String> mergedTopics = new ArrayList<>();
        mergedTopics.addAll(properties.getTopicNameList());
        mergedTopics.addAll(Optional.ofNullable(customTopics).orElse(Collections.emptyList()));

        if (mergedTopics.isEmpty()) {
            throw new InvalidKafkaConsumerState("No topic names provided in configuration or by parameter.");
        }

        validateConsumer();
        this.executorService = executorService;
        return executorService.submit(() -> {
            consumer.subscribe(topicNameList);
            try {
                while (!closed.get()) {
                    ConsumerRecords<K, V> consumerRecords = consumer.poll(Duration.ofSeconds(5));
                    consumerRecords.forEach(cr -> executorService.execute(() -> function.accept(cr)));
                }
            } catch (WakeupException ex) {
                if (!closed.get()) {
                    throw ex;
                }
            } finally {
                LOGGER.info("Closing consumer" + consumerId);
                consumer.close();
            }
        });
    }

    private void validateConsumer() {
        if (this.closed.get()) {
            throw new InvalidKafkaConsumerState("Invalid consumer state, already closed");
        }
        if (this.executorService != null) {
            throw new InvalidKafkaConsumerState("Invalid consumer state, already consuming");
        }
    }

    /**
     * Close consumer gracefully. Stops polling loop,
     * wakes possible blocked poll and shuts down executor service
     */
    @Override
    public void close() {
        this.closed.set(true);
        this.consumer.wakeup();
        this.executorService.shutdown();
    }

    /**
     * Use supplied customGroupId if not null
     * or take it from configuration if exist
     * or generate random in this order
     *
     * @param customGroupId custom group.id, overrides group.id from configuration
     */
    protected String getOrGenerateGroupId(String customGroupId) {
        return Optional.ofNullable(customGroupId)
                .orElse(Optional.ofNullable(properties.getProperty(KafkaConfigProperties.GROUP_ID))
                        .orElse(UUID.randomUUID().toString()));
    }

}
