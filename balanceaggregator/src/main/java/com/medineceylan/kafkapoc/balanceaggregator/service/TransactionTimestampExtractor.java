package com.medineceylan.kafkapoc.balanceaggregator.service;

import com.medineceylan.kafkapoc.models.Transaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class TransactionTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        Transaction transaction = (Transaction)record.value();
       long timestamp = transaction.getTransactionTime().getMillis();
         if (timestamp < 0) {
            // Invalid timestamp!  Attempt to estimate a new timestamp,
            // otherwise fall back to wall-clock time (processing-time).
            if (previousTimestamp >= 0) {
                return previousTimestamp;
            } else {
                return System.currentTimeMillis();
            }
        } else {
           return timestamp;
        }

    }
}



