# kafka-service-interface
Docker service that exposes Kafka producer and consumer API's

This service provides an easy way to add Kafka producer and consumer logic to your application.  Communication between this Kafka service
and other docker compose services is done through http callbacks.

The officially supported Kafka client libraries are in Java.  Client libraries in other languages have been written but
some of then seem inadiquate, buggy, or outdated.  This approach lets you easily integrate Kafka into any other language
while getting the benefits of the officially supported client libraries.

To see how this can be added to your application see the [samples](samples) directory.

## Example Configuration
### Docker Compose
Add the Kafka Service Interface service.
```services:
     kafkaserviceinterfacesample:
       build: .
       links:
         - kafkaserviceinterface
       environment:
         KAFKA_PUBLISH_URL: 'http://kafkaserviceinterface:8080/kafka/publish'
         KAFKA_TOPIC: 'kafkaserviceinterfacesample.topic1'
         KAFKA_TOPIC_TEST: 'kafkaserviceinterfacesample.test1'

     kafkaserviceinterface:
       image: quay.io/washpost/kafka-service-interface
       volumes:
         - ./config:/config
       environment:
         app.name: 'Kafka Service Interface Node.js Sample'
         vpc: 'arc2'
         config.location: '/config/config.yml'

   version: '2'
```

### config.yml
The config.yml file is used to configure the Kafka producer and consumers
```
kafka:
  host: "kafka.arc2.nile.works"
  port: "9092"
kafka.producer:
  publish.path: "/kafka/publish"
  port: 8080
  properties:
    client.id: "test"
    acks: "1"
    retries: "5"
    retry.backoff.ms: "1000"
    reconnect.backoff.ms: "1000"
    key.serializer: "org.apache.kafka.common.serialization.StringSerializer"
    value.serializer: "org.apache.kafka.common.serialization.StringSerializer"
    batch.size: "32000"
    linger.ms: "10"
kafka.consumers:
  -
    topics: "kafkaserviceinterfacesample.topic1"
    callback.url: "http://kafkaserviceinterfacesample:3000/kafka-consumption-callback"
    failure.topic: "kafkaserviceinterfacesample.failure1"
    failure.retries: 3
    dead.topic: "kafkaserviceinterfacesample.dead1"
    dead.callback.url: "http://kafkaserviceinterfacesample:3000/dead-callback"
    properties:
      group.id: "kafkaserviceinterfacesample.test"
      max.partition.fetch.bytes: "1000000"
      enable.auto.commit: "false"
      key.deserializer: "org.apache.kafka.common.serialization.StringDeserializer"
      value.deserializer: "org.apache.kafka.common.serialization.StringDeserializer"
```