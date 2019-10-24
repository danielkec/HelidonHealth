package io.helidon.examples.sport.graph;

import io.helidon.config.Config;
import io.helidon.messaging.kafka.SimpleKafkaProducer;

import java.util.Scanner;


public class TestProducer {


    public static void main(String[] args) {
        String demoData = new Scanner(TestProducer.class.getResourceAsStream("Afternoon_Run.json")).useDelimiter("\\A").next();
        new SimpleKafkaProducer<Long, String>("graph-queue-producer", Config.create())
                .produce(demoData);
    }
}
