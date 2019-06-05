package com.medineceylan.kafkapoc.balanceaggregator.kafkaclient;

import com.medineceylan.kafkapoc.balanceaggregator.config.ApplicationConfig;
import com.medineceylan.kafkapoc.balanceaggregator.service.TransactionTimestampExtractor;
import com.medineceylan.kafkapoc.models.AccountBalance;
import com.medineceylan.kafkapoc.models.Transaction;
import com.medineceylan.kafkapoc.models.TransactionType;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerClient {

    private String applicationId;
    private String bootstrapServers;
    private String schemaRegistryUrl;
    private String transactionTopicName;
    private String balanceTopicName;

    Logger logger = LoggerFactory.getLogger(KafkaConsumerClient.class.getName());


    public KafkaConsumerClient(ApplicationConfig config) {
        this.applicationId = config.getApplicationID();
        this.bootstrapServers = config.getBootstrapServers();
        this.schemaRegistryUrl = config.getSchemaRegistryUrl();
        this.transactionTopicName = config.getTransactionTopicName();
        this.balanceTopicName = config.getBalanceTopicName();

    }


    public KafkaStreams createTopology() {

        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.setProperty(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);


        // define a few serdes that will be useful to us later
        SpecificAvroSerde transactionSpecificAvroSerde = new SpecificAvroSerde<Transaction>();
        SpecificAvroSerde accountBalanceSpecificAvroSerde = new SpecificAvroSerde<AccountBalance>();

        transactionSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);
        accountBalanceSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);


        Serde<Long> longSerde = Serdes.Long();
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();


        // we build our stream with a timestamp extractor

        KStream<String, Transaction> stream = builder.stream(
                transactionTopicName,
                Consumed.with(longSerde,
                        transactionSpecificAvroSerde,
                        new TransactionTimestampExtractor(),
                        null)
        );

        //with select key you can change key but this causes repartitioning

        KStream<String, Transaction> reKeyedValidTransactions = stream.selectKey((key, value) -> value.getCustomerId());


        //With branches you divide this stream into new 2 kstream
        KStream<String, Transaction>[] branches = reKeyedValidTransactions.branch(
                new Predicate<String, Transaction>() {
                    @Override
                    public boolean test(String key, Transaction value) {
                        return value.getIsFraud().booleanValue() == true;

                    }
                }//first predicate is collecting fraud ones
                ,
                new Predicate<String, Transaction>() {
                    @Override
                    public boolean test(String key, Transaction value) {
                        return value.getIsFraud().booleanValue() == false;

                    }
                } //second predicate is collecting not fraud ones
        );


        // we build a long term topology (since inception)
        branches[1].groupByKey().<AccountBalance>aggregate(
                this::emptyAccountBalance,
                this::transactionAggregator,
                Materialized.<String, AccountBalance, KeyValueStore<Bytes, byte[]>>as("account-balance-table")
                        .withValueSerde(accountBalanceSpecificAvroSerde)
        ).toStream().to(balanceTopicName, Produced.with(stringSerde, accountBalanceSpecificAvroSerde));

        return new KafkaStreams(builder.build(), props);
    }


    private AccountBalance emptyAccountBalance() {
        return AccountBalance.newBuilder().setCustomerId("N/A").setAmount(0.0).build();
    }


    private AccountBalance transactionAggregator(String key, Transaction newTransaction, AccountBalance currentBalance) {
        AccountBalance.Builder accountBalanceBuilder = AccountBalance.newBuilder(currentBalance);

        double newBalance = 0.0;
        if (newTransaction.getTransactionType() == TransactionType.OPENING) {
            newBalance = accountBalanceBuilder.getAmount() + newTransaction.getAmount();
        } else if (newTransaction.getTransactionType() == TransactionType.DEPOSIT) {
            newBalance = accountBalanceBuilder.getAmount() + newTransaction.getAmount();
        } else if (newTransaction.getTransactionType() == TransactionType.WITHDRAW) {
            newBalance = accountBalanceBuilder.getAmount() - newTransaction.getAmount();
        }

        accountBalanceBuilder.setCustomerId(newTransaction.getCustomerId());
        accountBalanceBuilder.setAmount(newBalance);

        System.out.println(newTransaction);
        System.out.println(newBalance);


        return accountBalanceBuilder.build();
    }
}