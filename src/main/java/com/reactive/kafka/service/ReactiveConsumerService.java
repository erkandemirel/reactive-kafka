package com.reactive.kafka.service;

import com.reactive.kafka.model.ConsumerSample;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
@Slf4j
@RequiredArgsConstructor
public class ReactiveConsumerService {

    private final ReactiveKafkaConsumerTemplate<String, ConsumerSample> reactiveKafkaConsumer;

    private Flux<ConsumerSample> consume() {
        return reactiveKafkaConsumer
                .receiveAutoAck()
                .doOnNext(consumerRecord -> log.info("received key={}, value={} from topic={}, offset={}",
                        consumerRecord.key(),
                        consumerRecord.value(),
                        consumerRecord.topic(),
                        consumerRecord.offset())
                )
                .map(ConsumerRecord::value)
                .doOnNext(message -> log.info("successfully consumed {}={}", ConsumerSample.class.getSimpleName(), message))
                .doOnError(throwable -> log.error("something bad happened while consuming : {}", throwable.getMessage()))
                .publish();
    }
}
