package com.washingtonpost.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.Unirest;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static spark.Spark.port;
import static spark.Spark.post;

/**
 * Initiate the Kafka producer and consumers as defined in the <strong>config.location</strong> yaml file.
 */
public class KafkaServiceInterface {
    final static Logger logger = Logger.getLogger(KafkaServiceInterface.class);

    public static void main(String args[]) {
        logger.info("KafkaServiceInterface started!");

        //
        // Set up Kafka producer WS
        //
        port(Configuration.get().getKafkaProducer().port);
        post(Configuration.get().getKafkaProducer().publishPath, (req, res) -> {
            boolean success = false;
            try {
                ObjectMapper mapper = new ObjectMapper();
                logger.info(req.body());
                JsonNode message = mapper.readTree(req.body());

                //
                // Publish to kafka
                //
                success = KafkaProducer.get().send(message.get("topic").asText(), message.get("key").asText(), message.get("message").asText());
            } catch (Exception e){
                logger.error("Unable to publish message to kafka.", e);
            }
            if (success)
                res.status(200);
            else
                res.status(500);
            return res;
        });

        // Unirest connection timeout settings
        Unirest.setTimeouts(5 * 60 * 1000, 30 * 1000);

        //
        // Setup the Kafka consumers.
        //
        ExecutorService executor = Executors.newFixedThreadPool(Configuration.get().getKafkaConsumers().size()*2);
        for (Configuration.KafkaConsumer consumer : Configuration.get().getKafkaConsumers()) {
            executor.submit(new KafkaConsumerRunner(consumer));
            if (consumer.failureTopic != null) {
                executor.submit(new KafkaConsumerFailedRunner(consumer));
            }
        }
    }
}
