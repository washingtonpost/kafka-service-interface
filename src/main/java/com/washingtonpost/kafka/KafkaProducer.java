package com.washingtonpost.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Configure the Kafka consumer as defined in the <strong>config.location</strong> yaml file.  Expose this
 * Kafka producer as a singleton.
 */
public class KafkaProducer {
    final static Logger logger = Logger.getLogger(KafkaProducer.class);
    private static KafkaProducer instance;
    private org.apache.kafka.clients.producer.KafkaProducer producer;

    /**
     * Configure the Kafka consumer as defined in the <strong>config.location</strong> yaml file.  Expose this
     * Kafka producer as a singleton.
     */
    private KafkaProducer() {
        //
        // Setup Kafka producer
        //
        // http://kafka.apache.org/082/documentation.html#producerconfigs
        Properties props = new Properties();
        props.put("bootstrap.servers", Configuration.get().getKafka().bootstrapServers);
        for (String key : Configuration.get().getKafkaProducer().properties.keySet())
            props.put(key, Configuration.get().getKafkaProducer().properties.get(key));
        producer = new org.apache.kafka.clients.producer.KafkaProducer(props);
    }

    /**
     * Return the instance of this singleton.
     * @return
     */
    public static KafkaProducer get() {
        if (instance == null) instance = new KafkaProducer();
        return instance;
    }

    public static class MyCallback implements Callback {
        public boolean success = true;
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if(e != null) {
                logger.error("Unable to publish message to kafka.", e);
                success = false;
            } else
                logger.debug("The offset of the record we just sent is: " + recordMetadata.offset());
        }
    }

    /**
     * Send the message to the Kafka topic.
     *
     * @param topic
     * @param key
     * @param message
     * @return true if the message was successfully sent, false if not.
     */
    public boolean send(String topic, String key, String message) {
        MyCallback myCallback = new MyCallback();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);

        Future<RecordMetadata> response = producer.send(record, myCallback);

        // Wait her until we get a response.
        try {
            response.get();
        } catch (Exception e) {
            logger.error(e);
            return false;
        }
        return myCallback.success;
    }
}
