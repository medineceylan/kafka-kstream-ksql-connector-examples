package com.medineceylan.kafkaexamples.transactionproducer.service;

import com.medineceylan.kafkaexamples.models.Transaction;
import com.medineceylan.kafkaexamples.models.TransactionType;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.IntStream;

public class TransactionGenerator {

    private final int accountSize;
    private final int queueSize;

    ArrayBlockingQueue<Transaction> msgQueue;
    private double openingBalance;
    Logger logger = LoggerFactory.getLogger(TransactionGenerator.class.getName());


    public TransactionGenerator(int accountSize, double openingBalance, int queueSize) {
        this.accountSize = accountSize;
        this.openingBalance = openingBalance;
        this.queueSize = queueSize;
        msgQueue = new ArrayBlockingQueue<Transaction>(queueSize);

    }

    public ArrayBlockingQueue<Transaction> generate() {

        IntStream.range(1, accountSize+1)
                .mapToObj(i -> generateForEachCustomer())
                .flatMap(x -> x.stream())
                .forEach(transaction -> {
                    try {
                        msgQueue.put(transaction);

                    } catch (InterruptedException e) {
                        logger.error("Error while adding transaction to msgque", e);
                    }
                });

        return msgQueue;

    }


    public List<Transaction> generateForEachCustomer() {

        String customerID = String.valueOf(new Random().nextInt(500000));
        Double transactionFee = 2.5;
        Transaction fraudTransaction = new Transaction(TransactionType.WITHDRAW, customerID, 99.0, true, new DateTime());

        List<Transaction> transactions = new ArrayList<>();

        transactions.add(new Transaction(TransactionType.OPENING, customerID, openingBalance, false, new DateTime()));
        transactions.add(new Transaction(TransactionType.WITHDRAW, customerID, transactionFee, false, new DateTime()));
        transactions.add(new Transaction(TransactionType.WITHDRAW, customerID, transactionFee, false, new DateTime()));
        transactions.add(new Transaction(TransactionType.WITHDRAW, customerID, transactionFee, false, new DateTime()));
        transactions.add(fraudTransaction);
        transactions.add(new Transaction(TransactionType.WITHDRAW, customerID, transactionFee, false, new DateTime()));
        transactions.add(new Transaction(TransactionType.WITHDRAW, customerID, transactionFee, false, new DateTime()));
        transactions.add(new Transaction(TransactionType.DEPOSIT, customerID, 10.0, false, new DateTime()));
        transactions.add(new Transaction(TransactionType.WITHDRAW, customerID, transactionFee, false, new DateTime()));
        transactions.add(new Transaction(TransactionType.WITHDRAW, customerID, transactionFee, false, new DateTime()));
        transactions.add(new Transaction(TransactionType.WITHDRAW, customerID, transactionFee, false, new DateTime()));
        transactions.add(fraudTransaction);
        transactions.add(new Transaction(TransactionType.WITHDRAW, customerID, transactionFee, false, new DateTime()));
        transactions.add(new Transaction(TransactionType.WITHDRAW, customerID, transactionFee, false, new DateTime()));
        transactions.add(new Transaction(TransactionType.WITHDRAW, customerID, transactionFee, false, new DateTime()));
        transactions.add(new Transaction(TransactionType.DEPOSIT, customerID, 5.0, false, new DateTime()));
        transactions.add(new Transaction(TransactionType.WITHDRAW, customerID, transactionFee, false, new DateTime()));
        transactions.add(new Transaction(TransactionType.WITHDRAW, customerID, transactionFee, false, new DateTime()));
        transactions.add(new Transaction(TransactionType.WITHDRAW, customerID, transactionFee, false, new DateTime()));
        transactions.add(fraudTransaction);


        return transactions;
    }

}
