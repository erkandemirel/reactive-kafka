package com.reactive.kafka.config;

import com.reactive.kafka.model.ConsumerSample;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;

@Configuration
public class ReactiveKafkaConsumerConfig {

    @Bean
    public ReceiverOptions<String, ConsumerSample> kafkaReceiver(@Value(value = "${CONSUMER_TOPIC}") String topic, KafkaProperties kafkaProperties) {
        ReceiverOptions<String, ConsumerSample> basicReceiverOptions = ReceiverOptions.create(kafkaProperties.buildConsumerProperties());
        return basicReceiverOptions.subscription(Collections.singletonList(topic));
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, ConsumerSample> reactiveKafkaConsumer(ReceiverOptions<String, ConsumerSample> kafkaReceiverOptions) {
        return new ReactiveKafkaConsumerTemplate<String, ConsumerSample>(kafkaReceiverOptions);
    }
}
