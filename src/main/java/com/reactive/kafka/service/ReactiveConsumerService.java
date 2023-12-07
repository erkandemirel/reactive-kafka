package com.reactive.kafka.service;

import com.reactive.kafka.model.ConsumerSample;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
public class ReactiveConsumerService {

    Logger log = LoggerFactory.getLogger(ReactiveConsumerService.class);
    private final ReactiveKafkaConsumerTemplate<String, ConsumerSample> reactiveKafkaConsumerTemplate;

    public ReactiveConsumerService(ReactiveKafkaConsumerTemplate<String, ConsumerSample> reactiveKafkaConsumerTemplate) {
        this.reactiveKafkaConsumerTemplate = reactiveKafkaConsumerTemplate;
    }

    private Flux<ConsumerSample> consume() {
        return reactiveKafkaConsumerTemplate
                .receiveAutoAck()
                .doOnNext(consumerRecord -> log.info("received key={}, value={} from topic={}, offset={}",
                        consumerRecord.key(),
                        consumerRecord.value(),
                        consumerRecord.topic(),
                        consumerRecord.offset())
                )
                .map(ConsumerRecord::value)
                .doOnNext(message -> log.info("successfully consumed {}={}", ConsumerSample.class.getSimpleName(), message))
                .doOnError(throwable -> log.error("something bad happened while consuming : {}", throwable.getMessage()));
    }
}
