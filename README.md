Session Spectator
=================
## Track User Behavior Across Dynamic Windows

In Lagrangian description of flow we follow the fluid particles in individual level while using Eulerian description we focus on a location rather than individual particle. How is this relevant to the field of Data Engineering? Imagine we want to track the user activity of a web-page. We can either track their visit using sliding/tumbling windows (Eulerian description) or track the activities using dynamically changing session windows (Lagrangian description). A session window is defined as a window that starts when a user visits the page for the first time and continues as long as the user was not idle for more than a specified period. Note that using sliding/tumbling windows, it is more difficult to access user's history. However using session windows makes it easy to find what a user had done during the past sessions. This will allow the business owner to observe and analyze the behavior of each website (product) user, and personalize their product for the specific individual. This is crucial for successful marketing.

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


