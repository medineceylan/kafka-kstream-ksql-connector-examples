package com.medineceylan.kafkaexamples.balanceaggregator;

import com.medineceylan.kafkaexamples.balanceaggregator.config.ApplicationConfig;
import com.medineceylan.kafkaexamples.balanceaggregator.kafkaclient.KafkaConsumerClient;
import com.medineceylan.kafkaexamples.balanceaggregator.service.TransactionConsumer;

public class Main {
    public static void main(String[] args) {
        ApplicationConfig appConfig = new ApplicationConfig();
        KafkaConsumerClient kafkaProducerClient =new KafkaConsumerClient(appConfig);

        TransactionConsumer transactionConsumer=new TransactionConsumer(kafkaProducerClient);
        transactionConsumer.start();
    }
}
