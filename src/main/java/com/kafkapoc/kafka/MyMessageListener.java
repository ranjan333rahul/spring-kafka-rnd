package com.kafkapoc.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.listener.ConsumerAwareMessageListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MyMessageListener implements ConsumerAwareMessageListener<String, String> {

    @Override
    public void onMessage(ConsumerRecord<String, String> consumerRecord, Consumer<?, ?> consumer) {
        System.out.println("My message listener with key --> " + consumerRecord.key() + " value --> "
                + consumerRecord.value() + " from topic -->" + consumerRecord.topic() );
    }
}
