# DB-TradeStore

System Requirment :

KAFKA > 0.10

MYSQL >5.7.33

Main Class : 
StartStore

Start Kafka Zookeeper --  bin/zookeeper-server-start.sh config/zookeeper.properties

Start Kafka Server -- bin/kafka-server-start.sh config/server.properties

create Topic -- bin/kafka-topics.sh --create --topic testone --bootstrap-server localhost:9092


Note: topic name should be the same provided in to code

Producer Record Sample -- 
{"tradeId":"T1","version":"1","counterPartyId":"CP-1","bookId":"B1","maturityDate":"13/04/2021"}
{"tradeId":"T2","version":"1","counterPartyId":"CP-2","bookId":"B1","maturityDate":"12/04/2021"}
{"tradeId":"T3","version":"1","counterPartyId":"CP-3","bookId":"B1","maturityDate":"12/04/2021"}
{"tradeId":"T4","version":"1","counterPartyId":"CP-4","bookId":"B1","maturityDate":"12/04/2021"}
{"tradeId":"T3","version":"2","counterPartyId":"CP-3","bookId":"B1","maturityDate":"12/04/2021"}
{"tradeId":"T3","version":"1","counterPartyId":"CP-4","bookId":"B1","maturityDate":"12/04/2021"}
