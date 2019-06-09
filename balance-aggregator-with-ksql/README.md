###balance aggregator with ksql:

--With this module we will calculate each customers balance.
 And put it to account-balance-ll on postgresql via kafkaconnector
 Customers transactions should be generated already with transactions producer.
 
###preprequisites:
 
 1.Confluent should be run:
 
###check whether transactions topic is ready or not:
 
 ```sh
kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic transactions --from-beginning

kafka-topics --zookeeper 127.0.0.1:2181 --topic transactions --describe
 ```
 
 
 
###start with KSQL to stream transactions:
 
__*first configure ksql-server.properties*__
 
 in confluent 
 
 ```sh
 nano {your_confluent_path}/confluent-5.2.1/etc/ksql/ksql-server.properties
 
 ```
 
__*add these to ksql-server.properties*__
 
  bootstrap.servers = localhost:9092
  schema.registry.url = http://localhost:8081
  auto.offset.reset = earliest
  
  
__*stop ksql on confluent side and start*__
  
  
###connect ksql cli
  
  ksql http://localhost:8088
  
  
###create a stream from transactions topic with avro value format  with time extractor

   ```sh
CREATE STREAM transactions_stream_ksql WITH 
( KAFKA_TOPIC='transactions', VALUE_FORMAT='AVRO',TIMESTAMP='transaction_time');

 ```

###you can look at your new stream

```sh
Select * from transactions_stream_ksql;
 ```
 
###rekeyed with customerID;

 ```sh
CREATE STREAM rekeyed_transactions_stream 
AS select * from transactions_stream_ksql
PARTITION BY customer_id;

__*You can look at your new stream*__

Select * from rekeyed_transactions_stream;

 ```
 
###filter and get only non fraud values and update :

 ```sh

create table withdraws  as Select customer_id,sum(amount) as amount from rekeyed_transactions_stream 
where is_fraud=false and transaction_type='WITHDRAW' group by customer_id;

create table non_withdraws as Select customer_id,sum(amount) as amount  
from rekeyed_transactions_stream where is_fraud=false and transaction_type!='WITHDRAW' group by customer_id;


CREATE table account_blnc_ksql WITH (kafka_topic='account-balance-k', value_format='AVRO',PARTITIONS=5,key=customer_id)   as Select withdraws.customer_id as customer_id, 
(non_withdraws.amount-withdraws.amount) as balance 
from non_withdraws inner join withdraws  ON ( non_withdraws.customer_id=withdraws.customer_id);


Select * from account_blnc_ksql;
Describe account_blnc_ksql;

 ```

__*Now you can reach your topic and send it to postgresql via connector:*__

 ```sh
confluent load SinkTopicsWithKsql -d kafkaconnector/SinkTopicsWithKsqlInDb.properties

 ```
 


 


