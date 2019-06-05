package com.medineceylan.kafkapoc.frauddetector;

import com.typesafe.config.Config;
import lombok.Getter;

@Getter
public class ApplicationConfig {

    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final String sourceTopicName;
    private final String validTopicName;
    private final String fraudTopicName;
    private final String applicationId;

    public ApplicationConfig(Config config) {


        this.bootstrapServers = config.getString("kafka.bootstrap.servers");
        this.schemaRegistryUrl = config.getString("kafka.schema.registry.url");
        this.sourceTopicName = config.getString("kafka.source.topic.name");
        this.validTopicName = config.getString("kafka.valid.topic.name");
        this.fraudTopicName = config.getString("kafka.fraud.topic.name");
        this.applicationId = config.getString("kafka.streams.application.id");
    }


}
