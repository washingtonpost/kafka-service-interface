# Node.js Sample
## Setup
Assumes you already have a Kafka cluster running.

Step 1: Build docker images.
* Build kafka-service-interface docker image (docker-compose build)
* Build this node.js sample docker image (docker-compose build)

Step 2: Download Kafka
* http://kafka.apache.org/downloads (This sample uses 0.9.0.1)
* Unzip the download (tar -xzf kafka_*.tgz)
* Enter the kafka folder (cd kafka_*)

Step 4: Start Kafka command-line Producer
* bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic kafkaserviceinterfacesample.topic1

Step 5: Start Kafka command-line Consumers
* bin/kafka-console-consumer.sh --zookeeper 127.0.0.1:2181 --topic kafkaserviceinterfacesample.failure1
* bin/kafka-console-consumer.sh --zookeeper 127.0.0.1:2181 --topic kafkaserviceinterfacesample.dead1

## Run Sample
* Run this node.js docker image (docker-compose up)
* Send messages to the Kafka command-line producer.

You should see the messages show up in the running docker image.
You should also see the messages show up 3 times in the failed topic, one for each retry, and finally in the
dead topic once.