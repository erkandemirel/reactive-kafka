package com.reactive.kafka.config;

import com.reactive.kafka.model.ProducerSample;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.sender.SenderOptions;

import java.util.Map;

@Configuration
public class ReactiveKafkaProducerConfig {

    @Bean
    public ReactiveKafkaProducerTemplate<String, ProducerSample> reactiveKafkaProducer(KafkaProperties properties) {
        Map<String, Object> props = properties.buildProducerProperties();
        return new ReactiveKafkaProducerTemplate<String, ProducerSample>(SenderOptions.create(props));
    }
}
