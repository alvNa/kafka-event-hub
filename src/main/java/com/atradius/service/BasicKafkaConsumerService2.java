package com.atradius.service;

import com.atradius.examples.Tweet2;
import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicKafkaConsumerService2 {
    private final Properties props;
    private static final String TOPIC = "sc.platform.tweet2";

    public BasicKafkaConsumerService2(){
        props = new Properties();
        props.put("bootstrap.servers", "scsp-weu-dev-event-platform-cluster-ns.servicebus.windows.net:9093");
        // Required connection configs for Kafka producer, consumer, and admin
        props.put("security.protocol","SASL_SSL");
        props.put("sasl.mechanism","PLAIN");
        props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='$ConnectionString' password='Endpoint=sb://scsp-weu-dev-event-platform-cluster-ns.servicebus.windows.net/;SharedAccessKeyName=scsd_user;SharedAccessKey=Dz0nUR40kZ1TpQqqFb5HCxasS5sZ5Htpk+AEhHfUrHk=;EntityPath=sc.platform.tweet2';");

        //Config for the Consumer
        props.setProperty("group.id", "sc.platform.tweet2-cg");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroDeserializer");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");

        //Config for Azure Schema Registry
        props.put("schema.registry.url","https://scsp-weu-dev-event-platform-cluster-ns.servicebus.windows.net");
        props.put("schema.group","scsp-weu-dev-event-platform-cluster-ns-sg");
        props.put("value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy");
        TokenCredential tokenCredential = new DefaultAzureCredentialBuilder().build();
        props.put("schema.registry.credential", tokenCredential);
    }

    public void consume(){
        log.info("Consume events ...");
        try (KafkaConsumer<String, Tweet2> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            while (true) {
                ConsumerRecords<String, Tweet2> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, Tweet2> record : records) {
                    log.info("Received message: %s\n", record.value());
                }
            }
        }
    }

    public static void main(String[] args) {
        BasicKafkaConsumerService2 consumerService = new BasicKafkaConsumerService2();

        consumerService.consume();
    }
}
