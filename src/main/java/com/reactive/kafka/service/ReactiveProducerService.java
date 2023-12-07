package com.reactive.kafka.service;

import com.reactive.kafka.model.ProducerSample;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class ReactiveProducerService {

    private final ReactiveKafkaProducerTemplate<String, ProducerSample> reactiveKafkaProducer;

    public void send(ProducerSample message) {
        log.info("send to topic={}, {}={},", "producer_topic", ProducerSample.class.getSimpleName(), message);
        reactiveKafkaProducer.send("producer_topic", message)
                .doOnSuccess(senderResult -> log.info("sent {} offset : {}", message, senderResult.recordMetadata().offset()))
                .subscribe();
    }
}
