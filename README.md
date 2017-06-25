Session Spectator
=================
## Track User Behavior Across Dynamic Windows

It is important to track user's actions taken during a time period. This will allow the bussiness owner to observe and analyse the behavior of each website (product) user. 

<p align="center">
<img align="center" src="https://github.com/amirzainali/sessionization/blob/master/pipeline.png" width="600" />
</p>

Table of Contents
=================

  * [Summary](#session-spectator)
  * [Table of Contents](#table-of-contents)
  * [Dependency](#dependency)
  * [Installation](#installation)
  * [Usage](#usage)
    * [STDIN](#stdin)


Dependency
==========
This program is tested with the followings:

- Python version 2.7
- Java version 
- Apache Flink version 1.2.1
- Apache ZooKeeper version 3.4.9 
- Apache Kafka version 0.9.0.1
- Redis version 3.2.6
- PyKafka


Installation
============

You can follow the official webpage of each platform for instruction. If you want to test the librdkafka extention of PyKafka, probably, the easiest way to install librdkafka on your Debian based machine would be to follow the instructions presented at [confluent installation](http://docs.confluent.io/current/installation.html)

This program has been installed and test locally and on AWS. After installing all the required packages to run the program locally 

	$FLINK_HOME/bin/start-local.sh 
	
	$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
	
	$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
	
	$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topicname
	
	$SESSIONIZATION_HOME/consumer/mvn clean package
	$FLINK_HOME/bin/flink run -c consumer.Windows  $SESSIONIZATION_HOME/consumer/targer/consumer*.jar
	
For installing the required packages on AWS please follow [Pegasus Instruction](https://github.com/InsightDataScience/pegasus) 



Usage
=====


