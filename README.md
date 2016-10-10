# kafka-service-interface
Docker service that exposes Kafka producer and consumer API's

This service provides an easy way to add Kafka producer and consumer logic to your application.  Communication between this Kafka service
and other docker compose services is done through http callbacks.

The officially supported Kafka client libraries are in Java.  Client libraries in other languages have been written but
some of then seem inadiquate, buggy, or outdated.  This approach lets you easily integrate Kafka into any other language
while getting the benefits of the officially supported client libraries.

To see how this can be added to your application see the [samples](samples) directory.