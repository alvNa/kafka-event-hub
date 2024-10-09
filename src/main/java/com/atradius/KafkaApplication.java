package com.atradius;

import com.atradius.service.BasicKafkaConsumerService;
import com.atradius.service.BasicKafkaConsumerService2;
import com.atradius.service.BasicKafkaProducerService;
import com.atradius.service.BasicKafkaProducerService2;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaApplication {

    public static void main(String[] args) {
        //This works
        log.info("Starting Kafka Producer Service");
        BasicKafkaProducerService.main(new String[0]);

        log.info("Starting Kafka Consumer Service");
        BasicKafkaConsumerService.main(new String[0]);
        //This fails
//        log.info("Starting Kafka Producer Service 2");
//        BasicKafkaProducerService2.main(new String[0]);
//
//        log.info("Starting Kafka Consumer Service 2");
//        BasicKafkaConsumerService2.main(new String[0]);
    }
}