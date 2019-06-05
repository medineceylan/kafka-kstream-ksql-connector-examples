package com.medineceylan.kafkapoc.balanceaggregator;

import com.medineceylan.kafkapoc.balanceaggregator.config.ApplicationConfig;
import com.medineceylan.kafkapoc.balanceaggregator.kafkaclient.KafkaConsumerClient;
import com.medineceylan.kafkapoc.balanceaggregator.service.TransactionConsumer;

public class Main {
    public static void main(String[] args) {
        ApplicationConfig appConfig = new ApplicationConfig();
        KafkaConsumerClient kafkaProducerClient =new KafkaConsumerClient(appConfig);

        TransactionConsumer transactionConsumer=new TransactionConsumer(kafkaProducerClient);
        transactionConsumer.start();
    }
}
