package io.helidon.messaging.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

/**
 * https://stackoverflow.com/questions/48071988/how-to-check-if-kafka-consumer-is-ready
 * https://github.com/spring-projects/spring-kafka/blob/master/spring-kafka-test/src/main/java/org/springframework/kafka/test/utils/ContainerTestUtils.java
 */
public class PartitionsAssignedLatch extends CountDownLatch implements ConsumerRebalanceListener {

    public PartitionsAssignedLatch() {
        super(1);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // Do nothing
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        this.countDown();
    }
}
