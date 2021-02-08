package com.oci.kafka.consumer;

public class KafkaConsumerExample {

    public static void main(String... args) throws Exception {
        System.out.println("consumer");
        CompatibleConsumer consumer = new CompatibleConsumer();
        consumer.consume();
    }
}
