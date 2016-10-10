package com.washingtonpost.kafka;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by findleyr on 10/6/16.
 */
public class KafkaConsumerFailedRunner extends KafkaConsumerRunner {
    final static Logger logger = Logger.getLogger(KafkaConsumerFailedRunner.class);
    public KafkaConsumerFailedRunner(Configuration.KafkaConsumer consumerConfig) {
        logger.info("Failure Kafka Consumer Started! topics = "+consumerConfig.failureTopic);
        this.consumerConfig = consumerConfig;
        this.topics = Arrays.asList(consumerConfig.failureTopic.split(","));
        List<String> tags = new ArrayList<>();
        for (String topic : this.topics)
            tags.add("topic:"+topic);
        this.topicTags = tags.toArray(new String[0]);
    }

    /**
     * Attempt to process failed messages.
     * Try them one at a time.
     * If one fails then increment the <i>retries</i> and put it back on the failed topic.
     * If it fails more than the <i>failure.retries</i> then send it to the <i>dead</i> topic.
     *
     * @param partitionRecords
     * @throws Exception
     */
    protected void processMessages(List<ConsumerRecord<String, String>> partitionRecords) throws Exception {
        for (ConsumerRecord<String, String> record : partitionRecords) {
            Date now = new Date();
            ObjectNode message = (ObjectNode) mapper.readTree(record.value());

            // Retry the message.
            ArrayNode singleMessages = mapper.createArrayNode();
            singleMessages.add(message);
            boolean success = sendForProcessing(singleMessages);

            if (!success) {
                // Failed.
                boolean isDead = false;
                if (!message.has("retries")) {
                    // First retry.
                    message.put("retries", 1);
                } else {
                    int retries = message.get("retries").asInt();
                    if (retries < consumerConfig.failureRetries) {
                        message.put("retries", retries + 1);
                    } else {
                        isDead = true;
                    }
                }
                if (isDead) {
                    message.put("dead_on", now.getTime());
                    message.put("dead_on_date", dateFormat.format(now));
                    if (consumerConfig.deadTopic != null) {
                        if (!KafkaProducer.get().send(consumerConfig.deadTopic, message.get("key").asText(), mapper.writeValueAsString(message))) {
                            throw new Exception("Unable to publish to dead topic");
                        }
                    }
                    if (consumerConfig.deadCallbackUrl != null) {
                        if (!sendDeadMessage(message)) {
                            throw new Exception("Unable to publish to dead callback");
                        }
                    }
                } else {
                    message.put("last_retry_on", now.getTime());
                    message.put("last_retry_on_date", dateFormat.format(now));
                    if (!KafkaProducer.get().send(consumerConfig.failureTopic, message.get("key").asText(), mapper.writeValueAsString(message))) {
                        throw new Exception("Unable to publish to failed topic");
                    }
                }
            }
        }
    }

    /**
     * Actually send the messages to the callback.url
     *
     * @return
     * @throws Exception
     */
    protected boolean sendDeadMessage(ObjectNode message) {
        try {
            HttpResponse<String> response = Unirest.post(consumerConfig.deadCallbackUrl)
                    .header("accept", "application/json")
                    .header("Content-Type", "application/json")
                    .body(mapper.writeValueAsBytes(message))
                    .asString();
            logger.debug("Response: " + response.getBody());
            return response.getStatus() == 200;
        } catch (Exception e) {
            logger.error(e);
            return false;
        }
    }
}
