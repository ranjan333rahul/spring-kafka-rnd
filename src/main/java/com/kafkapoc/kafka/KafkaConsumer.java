package com.kafkapoc.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
@Service
@Slf4j
public class KafkaConsumer {

    @KafkaListener(topics = "myTopic",
            groupId = AppConstants.GROUP_ID)
    public void consume(String message){
        LOGGER.info(String.format("Message received -> %s", message));
    }



    @Bean(name = "delayedAbflHighVolumeDocumentUploadConsumerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, DelayedLimKafkaEventData>
    delayedAbflHighVolumeDocumentUploadListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, DelayedLimKafkaEventData> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(delayedKafkaListenerAbflHighVolumeDocUploadContainerFactory());
        factory.setConcurrency(kafkaProp.getLimCommunicationLayerDelayedAbflConcurency());
        ContainerProperties containerProperties = factory.getContainerProperties();
        containerProperties.setPollTimeout(kafkaProp.getListenerPollTimeOut());
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }


    void start(MyMessageListener msgListener) {
        ConcurrentMessageListenerContainer container =
                new ConcurrentMessageListenerContainer<>(
                        consumerFactory(brokerAddress),
                        containerProperties(topic, msgListener));

        container.start();
    }

    private DefaultKafkaConsumerFactory<String, String> consumerFactory(String brokerAddress) {
        return new DefaultKafkaConsumerFactory<>(
                consumerConfig(brokerAddress),
                new StringDeserializer(),
                new StringDeserializer());
    }

    private ContainerProperties containerProperties(String topic, MessageListener<String, String> messageListener) {
        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setMessageListener(messageListener);
        return containerProperties;
    }

    private Map<String, Object> consumerConfig(String brokerAddress) {
        Map<String, Object> myMap = new HashMap<>();
        myMap.put(BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
        myMap.put(GROUP_ID_CONFIG, "groupId");
        myMap.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        myMap.put(ENABLE_AUTO_COMMIT_CONFIG, "true");
        return myMap;
    }
}
