# kafka-poc

 - confluent stack
 - kafka broker
 - kafka connnector for sink
 - kafka streams

```sh
confluent start
```


__*create topics*__
```sh
kafka-topics --create --topic transactions --partitions 5 --replication-factor 1 --zookeeper localhost:2181
kafka-topics --create --topic account-balance --partitions 5 --replication-factor 1 --zookeeper localhost:2181
kafka-topics --create --topic valid-transactions --partitions 5 --replication-factor 1 --zookeeper localhost:2181 
kafka-topics --create --topic fraud-transactions --partitions 5 --replication-factor 1 --zookeeper localhost:2181
```

__*postgresql*__
```sh
docker run -d -p 5432:5432 --name my-postgres -e POSTGRES_PASSWORD=postgres postgres:9.6
```
----*if you want to use docker-compose*--

docker-compose down 
docker-compose up


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


__*if you want to see consumer results from confluent console*__

kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic account-balance --from-beginning 
kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic transactions --from-beginning
kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic valid-transactions --from-beginning;
kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic fraud-transactions --from-beginning 

kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic account-balance-ll --from-beginning 

_*register connectors*__

###kafkaconnector

```sh
export PATH=/Users/medineceylan/Desktop/DEV/confluent-5.2.1/bin:$PATH  
confluent load SinkTopics -d kafkaconnector/SinkTopicsInDb.properties
confluent load SinkTopicsWithKsql -d kafkaconnector/SinkTopicsWithKsqlInDb.properties
```

NOTE: if you want to unload connector
```sh
confluent unload SinkTopics

```

 
 ###frauddetector
 
 ```sh
 mvn clean package
 java -jar target/frauddetector-1.0-SNAPSHOT-jar-with-dependencies.jar
 ```
 
 ### stop confluent
 
```sh
confluent stop
```


### balance-aggregator-with-ksql `with ksql` 

