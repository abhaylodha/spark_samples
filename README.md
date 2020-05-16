# spark_samples
Sample Spark Programs

*) Command line arguments for Spark Spark Receiver :
  SparkReceiver local

*) Command line arguments for KafkaProducer :
   100 test localhost:9092

For Kafka :

*) Start Zookeeper :

F:\Kafka_Install\kafka_2.12-2.3.0\bin\windows\zookeeper-server-start.bat F:\Kafka_Install\kafka_2.12-2.3.0\config\zookeeper.properties
OR
zookeeper-server-start.bat F:\Kafka_Install\kafka_2.12-2.3.0\config\zookeeper.properties

*) Start Kafka Broker :

F:\Kafka_Install\kafka_2.12-2.3.0\bin\windows\kafka-server-start.bat F:\Kafka_Install\kafka_2.12-2.3.0\config\server.properties
OR
kafka-server-start.bat F:\Kafka_Install\kafka_2.12-2.3.0\config\server.properties

*) Test Kafka instances :
zookeeper-shell.bat localhost:2181 ls /brokers/ids

*) Creating a new topic in Kafka :
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

*) Produce a message :
kafka-console-producer.bat --broker-list localhost:9092 --topic test

*) Consume a message :
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test --from-beginning

*) Stop Kafka Server :
kafka-server-stop.bat F:\Kafka_Install\kafka_2.12-2.3.0\config\server.properties

*) Stop Zookeeper Server :
zookeeper-server-stop.bat F:\Kafka_Install\kafka_2.12-2.3.0\config\zookeeper.properties

*) Odd Line - second char...... Even Line - second last....

