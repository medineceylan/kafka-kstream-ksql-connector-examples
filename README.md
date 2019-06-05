# kafka-poc

 - confluent stack
 - kafka broker
 - kafka connnector
 - kafka streams

```sh
confluent start
```


__*create topics*__
```sh
kafka-topics --create --topic transactions --partitions 5 --replication-factor 1 --zookeeper localhost:2181
kafka-topics --create --topic account-balance --partitions 5 --replication-factor 1 --zookeeper localhost:2181
kafka-topics --create --topic transactions-valid --partitions 5 --replication-factor 1 --zookeeper localhost:2181
kafka-topics --create --topic transactions-fraud --partitions 5 --replication-factor 1 --zookeeper localhost:2181
```

__*postgresql*__
```sh
docker run -d -p 5432:5432 --name my-postgres -e POSTGRES_PASSWORD=postgres postgres:9.6
```

__*register connectors*__
###kafkaconnector

```sh
export PATH=/Users/medineceylan/Desktop/DEV/confluent-5.2.1/bin:$PATH  
confluent load SinkTopics -d kafkaconnector/SinkTopicsInDb.properties
```

NOTE: if you want to unload connector
```sh
confluent unload SinkTopics

```

### transactionsproducer

```sh
mvn clean package
java -jar target/transactionsproducer-1.0-SNAPSHOT-jar-with-dependencies.jar

```


### balanceaggregator `with kafka-streams` 

```sh
mvn clean package
java -jar target/balanceaggregator-1.0-SNAPSHOT-jar-with-dependencies.jar
```


__*if you want to see consumer result from confluent console*__

./bin/kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic account-balance --from-beginning

 
 ###frauddetector
 
 ```sh
 mvn clean package
 java -jar ./target/frauddetector-1.0-SNAPSHOT-jar-with-dependencies.jar
 ```
 
 
```sh
confluent stop
```


