package com.kafkapoc.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

@SpringBootApplication
@ConfigurationProperties(prefix = "mykafka")
public class KafkaSpringNoAnnotationsApplication {
    @Autowired
    private Environment env;

    public static void main(String[] args) {
        ApplicationContext ctx = SpringApplication.run(KafkaSpringNoAnnotationsApplication.class, args);
        KafkaProducer producer = ctx.getBean(KafkaProducer.class);
        producer.send("*** hi Rahul ***");

        MyMessageListener msgListener = ctx.getBean(MyMessageListener.class);
        KafkaConsumer consumer = ctx.getBean(KafkaConsumer.class);
        consumer.start(msgListener);
    }

    @Bean
    public KafkaConsumer getConsumer(){
        return new KafkaConsumer(env.getProperty("mykafka.brokerAddress"), env.getProperty("mykafka.consumerTopic"));
    }

    @Bean
    public KafkaProducer getProducer(){
        return new KafkaProducer(env.getProperty("mykafka.brokerAddress"), env.getProperty("mykafka.producerTopic"));
    }

}
