package com.atradius.service;

import com.atradius.examples.Tweet;
import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicKafkaProducerService {

    private final Producer<String, Tweet> producer;
    private static final String TOPIC = "sc.platform.tweet";

    public BasicKafkaProducerService(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "scsp-weu-dev-event-platform-cluster-ns.servicebus.windows.net:9093");

        // Required connection configs for Kafka producer, consumer, and admin
        props.put("security.protocol","SASL_SSL");
        props.put("sasl.mechanism","PLAIN");
        props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='$ConnectionString' password='Endpoint=sb://scsp-weu-dev-event-platform-cluster-ns.servicebus.windows.net/;SharedAccessKeyName=scsd_user;SharedAccessKey=2W4yiW7f4/UX6uSXadz2BLfMPauQkrPUu+AEhLo3SNs=;EntityPath=sc.platform.tweet';");

        //Config for the Producer
        props.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
        props.put("value.serializer", com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroSerializer.class);

        //Config for Azure Schema Registry
        props.put("schema.registry.url","https://scsp-weu-dev-event-platform-cluster-ns.servicebus.windows.net");
        props.put("schema.group","scsp-weu-dev-event-platform-cluster-ns-sg");
        props.put("value.subject.name.strategy", RecordNameStrategy.class);
        TokenCredential tokenCredential = new DefaultAzureCredentialBuilder().build();
        props.put("schema.registry.credential", tokenCredential);
        props.put("acks", "all");
        props.put("retries", 2);
        props.put("batch.size", 16384);
        props.put("linger.ms", 200);

        this.producer = new KafkaProducer<>(props);
    }

    public Future<RecordMetadata> send(Tweet tweet){
        return producer.send(new ProducerRecord<>(TOPIC, tweet));
    }

    public void close(){
        producer.close();
    }

    public static void main(String[] args) {
        log.info("Starting Kafka Producer Service");
        BasicKafkaProducerService producerService = new BasicKafkaProducerService();
        Tweet tweet;

        for(int i = 0; i < 10; i++) {
            tweet = new Tweet(i, String.format("Hello World %s !", i));
            log.info("Sending tweet: {}", tweet);
            try{
                var futureResult = producerService.send(tweet);
                var result = futureResult.get();
                log.info("Offset: {}", result.offset());
            } catch (Exception e){
                log.error("Error sending tweet: {}", tweet, e);
            }
        }

        producerService.close();
    }
}
