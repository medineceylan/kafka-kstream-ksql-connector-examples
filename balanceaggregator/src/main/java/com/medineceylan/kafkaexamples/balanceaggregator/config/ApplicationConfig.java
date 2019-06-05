package com.medineceylan.kafkaexamples.balanceaggregator.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.Getter;

@Getter
public class ApplicationConfig {

    String applicationID;
    String bootstrapServers;
    String schemaRegistryUrl;
    String transactionTopicName;
    String balanceTopicName;

    public ApplicationConfig() {

        Config config = ConfigFactory.load(ApplicationConfig.class.getClassLoader());

        applicationID = config.getString("kafka.streams.application.id");
        bootstrapServers = config.getString("kafka.bootstrap.servers");
        schemaRegistryUrl = config.getString("kafka.schema.registry.url");
        transactionTopicName = config.getString("kafka.topic.transactions.name");
        balanceTopicName = config.getString("kafka.topic.balance.name");

    }

}
