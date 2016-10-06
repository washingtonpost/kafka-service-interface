package com.washingtonpost.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Created by findleyr on 10/6/16.
 */
public class KafkaProducer {
    final static Logger logger = Logger.getLogger(KafkaProducer.class);
    private static KafkaProducer instance;
    private org.apache.kafka.clients.producer.KafkaProducer producer;

    private KafkaProducer() {
        //
        // Setup Kafka producer
        //
        // http://kafka.apache.org/082/documentation.html#producerconfigs
        Properties props = new Properties();
        props.put("bootstrap.servers", Configuration.get().getKafka().bootstrapServers);
        props.put("client.id", Configuration.get().getKafkaProducer().clientId);
        props.put("acks", Configuration.get().getKafkaProducer().acks);
        props.put("retries", Configuration.get().getKafkaProducer().retries);
        props.put("retry.backoff.ms", Configuration.get().getKafkaProducer().retryBackoffMs);
        props.put("reconnect.backoff.ms", Configuration.get().getKafkaProducer().reconnectBackoffMs);
        props.put("key.serializer", Configuration.get().getKafkaProducer().keySerializer);
        props.put("value.serializer", Configuration.get().getKafkaProducer().valueSerializer);
        props.put("batch.size", Configuration.get().getKafkaProducer().batchSize);
        props.put("linger.ms", Configuration.get().getKafkaProducer().lingerMs);
        producer = new org.apache.kafka.clients.producer.KafkaProducer(props);
    }

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
