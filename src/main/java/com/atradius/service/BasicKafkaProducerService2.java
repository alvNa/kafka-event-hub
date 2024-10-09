package com.atradius.service;

import com.atradius.examples.Country;
import com.atradius.examples.CountrySub;
import com.atradius.examples.Tweet2;
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
public class BasicKafkaProducerService2 {

    private final Producer<String, Tweet2> producer;
    private static final String TOPIC = "sc.platform.tweet2";

    public BasicKafkaProducerService2(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "scsp-weu-dev-event-platform-cluster-ns.servicebus.windows.net:9093");

        // Required connection configs for Kafka producer, consumer, and admin
        props.put("security.protocol","SASL_SSL");
        props.put("sasl.mechanism","PLAIN");
        props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='$ConnectionString' password='Endpoint=sb://scsp-weu-dev-event-platform-cluster-ns.servicebus.windows.net/;SharedAccessKeyName=scsd_user;SharedAccessKey=Dz0nUR40kZ1TpQqqFb5HCxasS5sZ5Htpk+AEhHfUrHk=;EntityPath=sc.platform.tweet2';");

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

    public Future<RecordMetadata> send(Tweet2 tweet){
        return producer.send(new ProducerRecord<>(TOPIC, tweet));
    }

    public void close(){
        producer.close();
    }

    private static Tweet2 getTweet2() {
        var country = Country.newBuilder()
                .setIso3("ESP")
                .setLanguage("ES")
                .setValue("España")
                .build();

        var countrySubIdent = CountrySub.newBuilder()
                .setCountry("ESPAÑA")
//                .setLevel1code("56639")
//                .setLevel2code("59")
//                .setLevel3code("8")
//                .setLevel4code("4215")
//                .setName("BARCELONA")
//                .setNameASCII("BARCELONA")
                .build();

        return Tweet2.newBuilder()
                .setNumber(0)
                .setDescription("Hello, World!")
                .setCountry(country)
                .setCountrySubIdent(countrySubIdent)
                .build();
    }

    public static void main(String[] args) {
        log.info("Starting Kafka Producer Service");
        BasicKafkaProducerService2 producerService = new BasicKafkaProducerService2();
        Tweet2 tweet;

        for(int i = 0; i < 10; i++) {
            tweet = getTweet2();
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
