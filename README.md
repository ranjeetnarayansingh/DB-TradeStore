# DB-TradeStore
There are thousands of trades flowing into one store. In this solution i used kafka as source.
the data will fetch from the kafka server and after the transforming of data on the basis on provided conditions
The Transformation operations are performed.
To make the data processing near real time i used Spark Streaming.
the transformed data will persisted into the MySQL DB.


System Requirment :

JAVA : 1.8

KAFKA > 0.10

MYSQL >5.7.33

Spark : 3.0.1

Scala : 2.12

Main Class : 
StartStore

Start Kafka Zookeeper --  bin/zookeeper-server-start.sh config/zookeeper.properties

Start Kafka Server -- bin/kafka-server-start.sh config/server.properties

create Topic -- bin/kafka-topics.sh --create --topic testone --bootstrap-server localhost:9092

Test Producer : bin/kafka-console-producer.sh --topic testone --bootstrap-server localhost:9092

Note: topic name should be the same provided in to code

Producer Record Sample -- 

{"tradeId":"T1","version":"1","counterPartyId":"CP-1","bookId":"B1","maturityDate":"13/04/2021"}

{"tradeId":"T2","version":"1","counterPartyId":"CP-2","bookId":"B1","maturityDate":"12/04/2021"}

{"tradeId":"T3","version":"1","counterPartyId":"CP-3","bookId":"B1","maturityDate":"12/04/2021"}

{"tradeId":"T4","version":"1","counterPartyId":"CP-4","bookId":"B1","maturityDate":"12/04/2021"}

{"tradeId":"T3","version":"2","counterPartyId":"CP-3","bookId":"B1","maturityDate":"12/04/2021"}

{"tradeId":"T3","version":"1","counterPartyId":"CP-4","bookId":"B1","maturityDate":"12/04/2021"}
