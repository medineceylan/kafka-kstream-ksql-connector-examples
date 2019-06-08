#balance aggregator with ksql:

--With this module we will aggreage each customers balance.
 And put it to account-balance-ksql on postgresql via kafkaconnector
 Customers transactions should be generated already with transactions producer.
 
 #preprequisites:
 
 1.Confluent should be run:
 
 #check whether transactions topic is ready or not:
 
 kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic transactions --from-beginning

 
 kafka-topics --zookeeper 127.0.0.1:2181 --topic transactions --describe
 
 
 
 
 ###start with KSQL to stream transactions:
 
 --------*first configure ksql-server.properties*------------------
 
 in confluent 
 
 {your_confluent_path}/confluent-5.2.1/etc/ksql/ksql-server.properties
 
 add these to ksql-server.properties:
 
 
  bootstrap.servers = localhost:9092
  schema.registry.url = http://localhost:8081
  auto.offset.reset = earliest
  
  
  ### stop ksql on confluent side and start
  
  
  
  ###connect ksql
  
  ksql http://localhost:8088
  
  
  ###create a stream from transactions topic with avro value format  with time extractor
  
CREATE STREAM transactions_stream_ksql WITH ( KAFKA_TOPIC='transactions', VALUE_FORMAT='AVRO',TIMESTAMP='transaction_time');


##you can look at your new stream
Select * from transactions_stream_ksql;

##rekeyed with customerID;

CREATE STREAM rekeyed_transactions_stream 
AS select * from transactions_stream_ksql
PARTITION BY customer_id;

##you can look at your new stream

Select * from rekeyed_transactions_stream;

##filter and get only non fraud values and update :



create table withdraws  as Select customer_id,sum(amount) as amount from rekeyed_transactions_stream 
where is_fraud=false and transaction_type='WITHDRAW' group by customer_id;

create table non_withdraws as Select customer_id,sum(amount) as amount  
from rekeyed_transactions_stream where is_fraud=false and transaction_type!='WITHDRAW' group by customer_id;


CREATE table account_blnc_ksql WITH (kafka_topic='account-balance-k', value_format='AVRO',PARTITIONS=5,key=customer_id)   as Select withdraws.customer_id as customer_id, 
(non_withdraws.amount-withdraws.amount) as balance 
from non_withdraws inner join withdraws  ON ( non_withdraws.customer_id=withdraws.customer_id);



Select * from account_blnc_ksql;
Describe account_blnc_ksql;


Now you can reach your topic and send it to postgresql via connector: account-balance-ksq
 
 


 


