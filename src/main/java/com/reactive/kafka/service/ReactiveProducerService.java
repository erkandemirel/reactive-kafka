package com.reactive.kafka.service;

import com.reactive.kafka.model.ProducerSample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;

@Service
public class ReactiveProducerService {
    Logger log = LoggerFactory.getLogger(ReactiveProducerService.class);
    private final ReactiveKafkaProducerTemplate<String, ProducerSample> reactiveKafkaProducerTemplate;

    @Value(value = "${PRODUCER_TOPIC}")
    private String topic;

    public ReactiveProducerService(ReactiveKafkaProducerTemplate<String, ProducerSample> reactiveKafkaProducerTemplate) {
        this.reactiveKafkaProducerTemplate = reactiveKafkaProducerTemplate;
    }

    public void send(ProducerSample message) {
        log.info("send to topic={}, {}={},", topic, ProducerSample.class.getSimpleName(), message);
        reactiveKafkaProducerTemplate.send(topic, message)
                .doOnSuccess(senderResult -> log.info("sent {} offset : {}", message, senderResult.recordMetadata().offset()))
                .subscribe();
    }
}
