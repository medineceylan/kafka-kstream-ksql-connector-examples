package com.medineceylan.kafkaexamples.transactionproducer;

import com.medineceylan.kafkaexamples.models.Transaction;
import com.medineceylan.kafkaexamples.transactionproducer.config.ApplicationConfig;
import com.medineceylan.kafkaexamples.transactionproducer.kafkaclient.KafkaProducerClient;
import com.medineceylan.kafkaexamples.transactionproducer.service.TransactionGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

public class Main {

    private Logger log = LoggerFactory.getLogger(Main.class.getSimpleName());
    KafkaProducerClient kafkaProducerClient;
    TransactionGenerator transactionGenerator;
    ApplicationConfig appConfig;
    // thread safe queue which blocks when full.
    private ExecutorService executor;

    public static void main(String[] args) {

        Main app = new Main();
        app.start();

    }

    private Main() {

        executor = Executors.newFixedThreadPool(1);
        appConfig = new ApplicationConfig();
        transactionGenerator = new TransactionGenerator(appConfig.getAccountSize(), appConfig.getOpeningBalance(),appConfig.getQueueSize());
        ArrayBlockingQueue<Transaction> transactionsQueue  = transactionGenerator.generate();
        kafkaProducerClient = new KafkaProducerClient(appConfig, transactionsQueue);
    }


    private void start() {

        log.info("Application started!");
        executor.submit(kafkaProducerClient);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (!executor.isShutdown()) {
                log.info("Shutdown requested");
                shutdown();
            }
        }));



    }

    private void shutdown() {
        if (!executor.isShutdown()) {
            log.info("Shutting down");
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(2000, TimeUnit.MILLISECONDS)) { //optional *
                    log.warn("Executor did not terminate in the specified time."); //optional *
                    List<Runnable> droppedTasks = executor.shutdownNow(); //optional **
                    log.warn("Executor was abruptly shut down. " + droppedTasks.size() + " tasks will not be executed."); //optional **
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}

