
CREATE STREAM transactions_stream_ksql
WITH ( KAFKA_TOPIC='transactions', VALUE_FORMAT='AVRO',TIMESTAMP='transaction_time');


CREATE STREAM rekeyed_transactions_stream
AS select * from transactions_stream_ksql
PARTITION BY customer_id;


create table withdraws  as Select customer_id,sum(amount) as amount from rekeyed_transactions_stream
where is_fraud=false and transaction_type='WITHDRAW' group by customer_id;

create table non_withdraws as Select customer_id,sum(amount) as amount
from rekeyed_transactions_stream where is_fraud=false and transaction_type!='WITHDRAW' group by customer_id;


CREATE table account_blnc_ksql WITH (kafka_topic='account-balance-ll', value_format='AVRO',PARTITIONS=5)
as Select withdraws.customer_id as customer_id,
(non_withdraws.amount-withdraws.amount) as balance
from non_withdraws inner join withdraws  ON
( non_withdraws.customer_id=withdraws.customer_id);


