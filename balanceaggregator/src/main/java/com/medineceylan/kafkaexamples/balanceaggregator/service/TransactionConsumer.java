package com.medineceylan.kafkaexamples.balanceaggregator.service;

import com.medineceylan.kafkaexamples.balanceaggregator.kafkaclient.KafkaConsumerClient;
import org.apache.kafka.streams.KafkaStreams;

public class TransactionConsumer {

    private KafkaConsumerClient kafkaConsumerClient;

    public TransactionConsumer(KafkaConsumerClient kafkaConsumerClient){
        this.kafkaConsumerClient = kafkaConsumerClient;

    }

    public void start(){
        KafkaStreams kafkaStreams;
        kafkaStreams = kafkaConsumerClient.createTopology();
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}
