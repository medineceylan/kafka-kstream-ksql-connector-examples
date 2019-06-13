package com.streaming;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streaming.model.Message;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;

public class ThroughputConsumer implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;
    private final int id;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    final Map<String, Long> highestLatency = new ConcurrentHashMap<String, Long>();
    long recordSize = 0;
    long minCreatedTime = System.currentTimeMillis();
    long totalLatency = 0;
    long totalElapsedTime = 0;

    public ThroughputConsumer(int id,
                              String groupId,
                              List<String> topics, String bootstrapServers, String autoOffsetReset, String enableAutoConfig,
                              String fetchMinBytes) {
        this.id = id;
        this.topics = topics;
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        if (autoOffsetReset != null) {
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        }
        if (enableAutoConfig != null) {
            props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoConfig);
        }
        if (fetchMinBytes != null) {
            props.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        }
//        if (sessionTimeoutMs != null) {
//            props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
//        }

        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {

        consumer.subscribe(topics);
        recordSize = 0;
        minCreatedTime = System.currentTimeMillis();
        totalLatency = 0;
        totalElapsedTime = 0;
        TimerTask repeatedTask = new TimerTask() {
            public void run() {
                System.out.println(
                        " Total message: " + recordSize +
                                " Avg Latency: " + String.valueOf((double) totalLatency / recordSize) + " ms/messages " +
                                " Total Latency: " + String.valueOf(totalElapsedTime) + " ms " +
                                " Avg Throughput: " + String.valueOf((double) recordSize / totalElapsedTime) + " messages/ms "
                );
            }
        };
        Timer timer = new Timer("Timer");

        long delay = 1000L;
        long period = 1000L;
        timer.scheduleAtFixedRate(repeatedTask, delay, period);

        try {

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));

                for (ConsumerRecord<String, String> record : records) {
                    recordSize += 1;
                    final Message message = objectMapper.readValue(record.value(), Message.class);
                    if (message.getCreated() < minCreatedTime) {
                        minCreatedTime = message.getCreated();
                    }
                    final Long currentLatency = System.currentTimeMillis() - message.getCreated();
                    totalLatency += currentLatency;

                    if (!highestLatency.containsKey(message.getBatch())) {

                        highestLatency.put(message.getBatch(), currentLatency);
                        System.out.println(format("Highest latency for batch %s : %s", message.getBatch(), highestLatency.get(message.getBatch())));
                    } else {
                        final Long highestLat = highestLatency.get(message.getBatch());

                        if (currentLatency > highestLat) {
                            highestLatency.put(message.getBatch(), currentLatency);
                            System.out.println(format("Highest latency for batch %s : %sms", message.getBatch(), highestLatency.get(message.getBatch())));

                        }
                    }

                    totalElapsedTime = System.currentTimeMillis() - minCreatedTime;
                    System.out.println("Created: " + (System.currentTimeMillis() - message.getCreated()) + " Throughput: " + String.valueOf((double) recordSize / totalElapsedTime) + " count/ms");
                }
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            consumer.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
}
