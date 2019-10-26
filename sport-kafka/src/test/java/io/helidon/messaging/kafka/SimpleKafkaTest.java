package io.helidon.messaging.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import io.helidon.config.Config;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;


public class SimpleKafkaTest {

    public static final String TEST_PRODUCER = "test-producer";
    public static final String TEST_CONSUMER_1 = "test-consumer-1";
    public static final String TEST_CONSUMER_2 = "test-consumer-2";
    public static final String TEST_CONSUMER_3 = "test-consumer-3";
    public static final String TEST_MESSAGE = "this is first test message";
    public static final String TEST_MESSAGE_2 = "this is second test message";

    @RegisterExtension
    public static final SharedKafkaTestResource kafkaResource = new SharedKafkaTestResource();
    public static final String TEST_TOPIC = "graph-done";

    @BeforeAll
    static void setUp() {
        kafkaResource.getKafkaTestUtils().createTopic(TEST_TOPIC, 10, (short) 1);
    }

    @Test
    public void sendAndReceive() throws ExecutionException, InterruptedException, TimeoutException {
        Config config = TestKafkaConfig.create(kafkaResource.getKafkaConnectString())
                .consumer(TEST_CONSUMER_1, TEST_TOPIC)
                .producer(TEST_PRODUCER, TEST_TOPIC)
                .build();

        // Consumer
        SimpleKafkaConsumer<Long, String> consumer = new SimpleKafkaConsumer<>(TEST_CONSUMER_1, config);
        Future<?> consumerClosedFuture = consumer.consumeAsync(r -> {
            assertEquals(TEST_MESSAGE, r.value());
            consumer.close();
        });

        consumer.waitForPartitionAssigment(10, TimeUnit.SECONDS);

        // Producer
        SimpleKafkaProducer<Long, String> producer = new SimpleKafkaProducer<>(TEST_PRODUCER, config);
        producer.produceAsync(TEST_MESSAGE);

        try {
            consumerClosedFuture.get(10, TimeUnit.SECONDS);
            producer.close();
        } catch (TimeoutException e) {
            fail("Didn't receive test message in time");
        }
    }

    @Test
    public void queueBySameConsumerGroup() throws ExecutionException, InterruptedException, TimeoutException {
        Config config = TestKafkaConfig.create(kafkaResource.getKafkaConnectString())
                .consumer(TEST_CONSUMER_1, TEST_TOPIC, c -> c.add("group_id", "XXX"))
                .consumer(TEST_CONSUMER_2, TEST_TOPIC, c -> c.add("group_id", "XXX"))
                .producer(TEST_PRODUCER, TEST_TOPIC)
                .build();

        List<String> receiviedByConsumer1 = Collections.synchronizedList(new ArrayList<>(4));
        List<String> receiviedByConsumer2 = Collections.synchronizedList(new ArrayList<>(4));

        CountDownLatch messagesCountingLatch = new CountDownLatch(4);

        // Consumer 1
        SimpleKafkaConsumer<Long, String> consumer1 = new SimpleKafkaConsumer<>(TEST_CONSUMER_1, config);
        consumer1.consumeAsync(r -> {
            messagesCountingLatch.countDown();
            receiviedByConsumer1.add(r.value());
        });

        // Consumer 2
        SimpleKafkaConsumer<Long, String> consumer2 = new SimpleKafkaConsumer<>(TEST_CONSUMER_2, config);
        consumer2.consumeAsync(r -> {
            messagesCountingLatch.countDown();
            receiviedByConsumer2.add(r.value());
        });

        // Wait till all consumers are ready
        consumer1.waitForPartitionAssigment(10, TimeUnit.SECONDS);
        consumer2.waitForPartitionAssigment(10, TimeUnit.SECONDS);

        // Producer
        SimpleKafkaProducer<Long, String> producer = new SimpleKafkaProducer<>(TEST_PRODUCER, config);
        List<Future<RecordMetadata>> producerFutures = new ArrayList<>(4);
        producerFutures.addAll(producer.produceAsync(TEST_MESSAGE + 1));
        producerFutures.addAll(producer.produceAsync(TEST_MESSAGE + 2));
        producerFutures.addAll(producer.produceAsync(TEST_MESSAGE + 3));
        producerFutures.addAll(producer.produceAsync(TEST_MESSAGE + 4));

        // Wait for all sent(this is example usage, sent doesn't mean delivered)
        producerFutures.forEach(f -> {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                fail(e);
            }
        });

        // Wait till 4 records are delivered
        assertTrue(messagesCountingLatch.await(10, TimeUnit.SECONDS)
                , "All messages not delivered in time");

        consumer1.close();
        consumer2.close();
        producer.close();

        assertFalse(receiviedByConsumer1.isEmpty());
        assertFalse(receiviedByConsumer2.isEmpty());
        assertTrue(receiviedByConsumer1.stream().noneMatch(receiviedByConsumer2::contains));
        assertTrue(receiviedByConsumer2.stream().noneMatch(receiviedByConsumer1::contains));
    }

}
