package com.oci.kafka;

public class KafkaProducerExmaple {

    public static void main(String... args) throws Exception {
        System.out.println("producer");
        CompatibleProducer producer = new CompatibleProducer();
        producer.produce();
    }
}
