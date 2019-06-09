package com.medineceylan.kafkapoc.frauddetector;

import com.medineceylan.kafkapoc.models.Transaction;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class FraudMain {

    private Logger log = LoggerFactory.getLogger(FraudMain.class.getSimpleName());
    private ApplicationConfig appConfig;

    public static void main(String[] args) {
        FraudMain fraudMain = new FraudMain();
        fraudMain.start();
    }

    private FraudMain() {
        appConfig =  new ApplicationConfig(ConfigFactory.load(ApplicationConfig.class.getClassLoader()));
    }

    private void start() {
        Properties config = getKafkaStreamsConfig();
        KafkaStreams streams = createTopology(config);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private Properties getKafkaStreamsConfig() {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appConfig.getApplicationId());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        // Exactly once processing!!
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl());

        return config;
    }

    private KafkaStreams createTopology(Properties config) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<Bytes, Transaction> transactions = builder.stream(appConfig.getSourceTopicName());

        KStream<Bytes, Transaction>[] branches = transactions.branch(
                (k, transaction) -> isValidTransaction(transaction),
                (k, transaction) -> !isValidTransaction(transaction)

        );

        KStream<Bytes, Transaction> validTranscations = branches[0];
        KStream<Bytes, Transaction> fraudTransactions = branches[1];

        validTranscations.peek((k, transaction) -> log.info("Valid: " + transaction.getCustomerId())).to(appConfig.getValidTopicName());
        fraudTransactions.peek((k, transaction) -> log.info("!! Fraud !!: " + transaction.getCustomerId())).to(appConfig.getFraudTopicName());

        return new KafkaStreams(builder.build(), config);
    }


    private boolean isValidTransaction(Transaction transaction) {

        return transaction.getIsFraud().booleanValue();
    }

}
