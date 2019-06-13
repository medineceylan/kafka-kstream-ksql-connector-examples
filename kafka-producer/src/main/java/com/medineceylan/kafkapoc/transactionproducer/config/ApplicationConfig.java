package com.medineceylan.kafkapoc.transactionproducer.config;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.Getter;

@Getter
public class ApplicationConfig {

    String bootstrapServers;
    String schemaRegistryUrl;
    String topicName;
    Double openingBalance;
    Integer accountSize;
    Integer queueSize;
    Integer transactionLimitFrom;
    Integer transactionLimitUntil;


    public ApplicationConfig() {

        Config config = ConfigFactory.load(ApplicationConfig.class.getClassLoader());

        bootstrapServers = config.getString("kafka.bootstrap.servers");
        schemaRegistryUrl = config.getString("kafka.schema.registry.url");
        topicName = config.getString("kafka.topic.name");
        openingBalance = config.getDouble("app.transactions.data.openingBalance");
        accountSize = config.getInt("app.transactions.data.accountSize");
        queueSize = config.getInt("app.transactions.data.queueSize");
        transactionLimitFrom = config.getInt("app.transactions.data.transactionLimitFrom");
        transactionLimitUntil = config.getInt("app.transactions.data.transactionLimitUntil");

    }

}
