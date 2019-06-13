package com.streaming;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {


    public static void main(String[] args) {

        validateArgs(args);
        int numConsumers = Integer.parseInt(args[0]);
        String groupId = args[1];
        List<String> topics = Arrays.asList(args[2].split(","));
        final String bootstrapServers = args[3];
        final String autoOffsetReset = args[4];
        final String enableAutoCommit = args[5];
        final String fetchMinByteConfig = args[6];
        //final String sessionTimeoutMsConfig = args[7];
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);


        final List<ThroughputConsumer> consumers = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {
            ThroughputConsumer consumer = new ThroughputConsumer(i, groupId, topics, bootstrapServers, autoOffsetReset, enableAutoCommit, fetchMinByteConfig);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (ThroughputConsumer consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private static void validateArgs(String[] args) {
        if (args.length != 7) {
            System.err.println("Please supply arguments: <consumer-number> <group-id> <topics-name> <bootstrap-server><auto.ofset.rset><enable.auto.commit><fetch.min.byte.commit>");//<session.timeout.ms>");
            System.exit(1);
        }
    }
}
