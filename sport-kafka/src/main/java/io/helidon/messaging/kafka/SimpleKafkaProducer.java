package io.helidon.messaging.kafka;

import io.helidon.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;

import java.util.concurrent.ExecutionException;

public class SimpleKafkaProducer<K, V> extends KafkaConfigurable {

    private KafkaProducer<K, V> producer;

    public SimpleKafkaProducer(String producerId, Config config) {
        super(producerId, config);
        producer = new KafkaProducer<>(getProperties());
    }

    public RecordMetadata produce(V value) {
        return this.produce(null, null, null, value, null);
    }

    public RecordMetadata produce(Integer partition, Long timestamp, K key, V value, Iterable<Header> headers) {
        try {
            ProducerRecord<K, V> record = new ProducerRecord<>(getProperty(TOPIC_NAME), key, value);
            return producer.send(record).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
