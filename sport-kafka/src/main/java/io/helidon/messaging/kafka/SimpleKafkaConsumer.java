package io.helidon.messaging.kafka;

import io.helidon.config.Config;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class SimpleKafkaConsumer<K, V> extends KafkaConfigurable implements Closeable {

    private static Logger LOG = Logger.getLogger(SimpleKafkaClient.class.getName());

    private List<String> topicNameList;
    private KafkaConsumer<K, V> consumer;
    private boolean closed = false;
    private String consumerId;
    private ExecutorService executorService;

    public SimpleKafkaConsumer(String consumerId, Config config) {
        super(consumerId, config);
        this.consumerId = consumerId;
        this.topicNameList = getTopicNameList();
        this.setProperty(GROUP_ID, getOrGenerateGroupId());
        consumer = new KafkaConsumer<>(getProperties());
    }

    public void consumeAsync(Consumer<ConsumerRecord<K, V>> c) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        this.consumeAsync(c, executorService);
    }

    public void consumeAsync(Consumer<ConsumerRecord<K, V>> c, ExecutorService executorService) {
        checkInitialization();
        LOG.info(String.format("Initiating kafka consumer %s listening to topics: %s with groupId: %s",
                consumerId, topicNameList, getProperty(GROUP_ID)));
        this.executorService = executorService;
        executorService.execute(() -> {
            consumer.subscribe(topicNameList);
            while (!closed) {
                ConsumerRecords<K, V> consumerRecords = consumer.poll(Duration.ofSeconds(5));
                consumerRecords.forEach(c);
                consumer.commitAsync();
            }
        });
    }

    private void checkInitialization() {
        if (this.executorService != null) {
            throw new RuntimeException("Invalid consumer state, already consuming");
        }
        if (this.closed) {
            throw new RuntimeException("Invalid consumer state, already closed");
        }
    }

    @Override
    public void close() {
        this.closed = true;
        this.executorService.shutdown();
        this.consumer.close();
    }

    /**
     * Get group.id from configuration or generate random if missing
     */
    protected String getOrGenerateGroupId() {
        return Optional.ofNullable(this.getProperty(GROUP_ID))
                .orElse(UUID.randomUUID().toString());
    }

    private List<String> getTopicNameList() {
        return Arrays.stream(getProperty(TOPIC_NAME)
                .split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }
}
