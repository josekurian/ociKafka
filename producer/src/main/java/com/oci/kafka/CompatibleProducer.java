package com.oci.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class CompatibleProducer {

    public void produce() {
    /*    String authToken = System.getenv("AUTH_TOKEN");
        String tenancyName = System.getenv("TENANCY_NAME");
        String username = System.getenv("STREAMING_USERNAME");
        String streamPoolId = System.getenv("STREAM_POOL_ID");
        String topicName = System.getenv("TOPIC_NAME"); */

        String authToken = "0{S0#wGmbA1efaW>)#65";
        String tenancyName = "odpcat2020";
        String username = "oracleidentitycloudservice/seokwon.park@oracle.com";
        String streamPoolId = "ocid1.streampool.oc1.eu-frankfurt-1.amaaaaaaejk3llyanvvcjvqrb5yu5apfosln2hgs64iqmhrgd6ihl56wgwga";
        String topicName = "SeokwonStream";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "cell-1.streaming.eu-frankfurt-1.oci.oraclecloud.com:9092");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                        + tenancyName + "/"
                        + username + "/"
                        + streamPoolId + "\" "
                        + "password=\""
                        + authToken + "\";"
        );

        properties.put("retries", 5); // retries on transient errors and load balancing disconnection
        properties.put("max.request.size", 1024 * 1024); // limit request size to 1MB

        KafkaProducer producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 5; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, UUID.randomUUID().toString(), "Test record #" + i);
            producer.send(record, (md, ex) -> {
                if( ex != null ) {
                    ex.printStackTrace();
                }
                else {
                    System.out.println(
                            "Sent msg to "
                                    + md.partition()
                                    + " with offset "
                                    + md.offset()
                                    + " at "
                                    + md.timestamp()
                    );
                }
            });
        }
        producer.flush();
        producer.close();
        System.out.println("produced 5 messages");
    }
}
