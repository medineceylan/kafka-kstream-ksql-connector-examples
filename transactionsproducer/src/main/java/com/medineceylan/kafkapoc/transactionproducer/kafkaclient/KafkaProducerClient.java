package com.medineceylan.kafkapoc.transactionproducer.kafkaclient;

import com.medineceylan.kafkapoc.models.Transaction;
import com.medineceylan.kafkapoc.transactionproducer.config.ApplicationConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

public class KafkaProducerClient implements Runnable {


    private String topicName;
    private ArrayBlockingQueue<Transaction> transactionsQueue;
    private int accountSize;
    KafkaProducer<Long, Transaction> kafkaProducer;
    Logger logger = LoggerFactory.getLogger(KafkaProducerClient.class.getName());


    public KafkaProducerClient(ApplicationConfig appConfig, ArrayBlockingQueue<Transaction> transactionsQueue) {

        this.topicName = appConfig.getTopicName();
        this.transactionsQueue = transactionsQueue;
        this.accountSize = appConfig.getAccountSize();
        this.kafkaProducer = createKafkaProducer(appConfig);

    }

    public KafkaProducer<Long, Transaction> createKafkaProducer(ApplicationConfig appConfig) {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.setProperty(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl());

        return new KafkaProducer<Long, Transaction>(props);

    }


    @Override
    public void run() {

        int transactionsCount = 0;
        try {
            while (transactionsQueue.size() > 0) {
                Transaction transaction = transactionsQueue.poll();
                transactionsCount += 1;
                logger.info("Sending transaction " + transactionsCount + ": " + transaction);
                kafkaProducer.send(new ProducerRecord<>(topicName, transaction));
            }

        } finally {
            close();
        }
    }


    public void close() {
        logger.info("stopping application...");
        logger.info("closing producer...");
        kafkaProducer.close();
        logger.info("done!");

    }

}

