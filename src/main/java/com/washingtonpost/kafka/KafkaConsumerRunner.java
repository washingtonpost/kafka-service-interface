package com.washingtonpost.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.washingtonpost.stats.StatsDService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka consumer running on a single thread.
 */
public class KafkaConsumerRunner implements Runnable {
    final static Logger logger = Logger.getLogger(KafkaConsumerRunner.class);
    protected final DateFormat dateFormat = DateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.FULL);

    protected Configuration.KafkaConsumer consumerConfig;
    private final AtomicBoolean closed = new AtomicBoolean(true);
    private KafkaConsumer<String, String> consumer = null;
    protected List<String> topics;
    private String[] topicTags;

    protected static final ObjectMapper mapper = new ObjectMapper();

    public KafkaConsumerRunner(Configuration.KafkaConsumer consumerConfig) {
        this.consumerConfig = consumerConfig;
        this.topics = Arrays.asList(consumerConfig.topics.split(","));
        List<String> tags = new ArrayList<>();
        for (String topic : topics) 
            tags.add("topic:"+topic);
        this.topicTags = (String[]) tags.toArray();
    }

    private KafkaConsumer<String, String> consumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Configuration.get().getKafka().bootstrapServers);
        props.put("group.id", consumerConfig.groupId);
        props.put("max.partition.fetch.bytes", consumerConfig.maxPartitionFetchBytes);
        props.put("enable.auto.commit", consumerConfig.enableAutoCommit);
        props.put("key.deserializer", consumerConfig.keyDeserializer);
        props.put("value.deserializer", consumerConfig.valueDeserializer);
        if (consumerConfig.maxPollRecords != null) props.put("max.poll.records", consumerConfig.maxPollRecords);
        return new KafkaConsumer<>(props);
    }

    public void run() {

        boolean subscribed = false;
        while(true) {
            //
            // First check to see if this consumer is currently closed.
            // This happens when the number of 'received' operations is too high.
            //
            if (closed.get()) {
                StatsDService.getStatsDClient().count("kafka.closed.sleep.int", 1, topicTags);
                try {
                    Thread.sleep(500);
                } catch (Exception ee) {}
                continue;
            }

            try {
                //
                // Subscribe to the topic.
                //
                if (!subscribed) {
                    subscribed = true;
                    consumer = consumer();
                    consumer.subscribe(topics);
                    logger.debug("Consumer Subscribe");
                }

                //
                // Poll for messages
                //
                Date startLatency = new Date();
                logger.debug("Consumer Poll");
                StatsDService.getStatsDClient().count("kafka.poll.int", 1, topicTags);
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Long.MAX_VALUE);
                StatsDService.getStatsDClient().count("kafka.poll.success.int", 1, topicTags);

                for (TopicPartition partition : consumerRecords.partitions()) {
                    //
                    // Store all the records within this partition
                    //
                    List<ConsumerRecord<String, String>> partitionRecords = consumerRecords.records(partition);
                    processMessages(partitionRecords);

                    //
                    // If we've gotten this far, i.e. the storage to mongo didn't fail, we can committ the offset.
                    //
                    long lastOffset = partitionRecords.get(partitionRecords.size()-1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                    logger.debug("Consumer Commit Sync, partition="+partition+", offset="+(lastOffset+1));
                }
                //
                // Record the latency of the entire consumer.poll()
                //
                Date now = new Date();
                StatsDService.getStatsDClient().histogram("kafka.poll.latency", now.getTime() - startLatency.getTime(), topicTags);

                StatsDService.getStatsDClient().count("kafka.java.error.int", 0, topicTags);
            } catch (WakeupException e) {
                // Called after consumer.wakeup(); has been executed by the count() method below.
                subscribed = false;
                consumer.unsubscribe();
                consumer.close();
                consumer = null;
                logger.warn("WakeupException: Consumer Unsubscribe");
                StatsDService.getStatsDClient().count("kafka.java.wakeup.int", 1, topicTags);
            } catch (Exception e) {
                subscribed = false;
                consumer.unsubscribe();
                consumer.close();
                consumer = null;
                logger.warn("Consumer Unsubscribe");
                try {
                    Thread.sleep(1000);
                } catch (Exception ee) {}
                logger.error("Failed during while(true) loop.", e);
                StatsDService.getStatsDClient().count("kafka.java.error.int", 1, topicTags);
            }
        }
    }

    /**
     * Send these messages to the callback.url for processing.
     * If this call fails then send them one at a time to the callback.url.
     * If a single call fails then add it to the failed.topic queue.
     *
     * @param partitionRecords
     * @throws Exception
     */
    protected void processMessages(List<ConsumerRecord<String, String>> partitionRecords) throws Exception {
        ArrayNode messages = mapper.createArrayNode();
        for (ConsumerRecord<String, String> record : partitionRecords) {
            logger.debug("topic: " + record.topic());
            logger.debug("key: " + record.key());
            logger.debug("offset: " + record.offset());
            logger.debug("partition: " + record.partition());
            Date now = new Date();
            ObjectNode message = mapper.createObjectNode();
            message.put("topic", record.topic());
            message.put("value", record.value());
            message.put("key", record.key());
            message.put("offset", record.offset());
            message.put("partition", record.partition());
            message.put("received_on", now.getTime());
            message.put("received_on_date", dateFormat.format(now));
            messages.add(message);
        }
        if (messages.size() > 0) {
            boolean success = sendForProcessing(messages);
            if (!success) {
                processMessages(messages);
            }
        }
    }

    /**
     * Send these messages to the callback.url for processing, one at a time.
     * If a single call fails then add it to the failed.topic queue.
     *
     * @param messages
     * @throws Exception
     */
    protected void processMessages(ArrayNode messages) throws Exception {
        // Try one at a time.
        for (JsonNode message : messages) {
            ArrayNode singleMessages = mapper.createArrayNode();
            singleMessages.add(message);
            boolean success = sendForProcessing(singleMessages);
            if (!success) {
                //  Post to failure queue.
                if (consumerConfig.failureTopic != null) {
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
     * @param messages
     * @return
     * @throws Exception
     */
    protected boolean sendForProcessing(ArrayNode messages) throws Exception {
        HttpResponse<String> response = Unirest.post(consumerConfig.callbackUrl)
                .header("accept", "application/json")
                .body(mapper.writeValueAsBytes(messages))
                .asString();
        logger.debug("Response: "+response.getBody());
        return response.getStatus() == 200;

        // Failed queue with multiple attempts
        // Final failed queue.  Also send to topic to store in mongo.

        // Denormalize in parallel.  Send to mongo for batch.
        // First have to split by updates.  Break for delete.
    }
}
