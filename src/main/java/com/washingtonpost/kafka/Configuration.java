package com.washingtonpost.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Created by findleyr on 10/6/16.
 */
public class Configuration {
    final static Logger logger = Logger.getLogger(Configuration.class);
    private static Configuration instance = null;
    private Config config = null;
    private Configuration() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            String configLocation = System.getProperty("config.location");
            if (configLocation == null) configLocation = System.getenv("config.location");
            File configFile = new File(configLocation);
            if (configFile.exists())
                config = mapper.readValue(configFile, Config.class);
            else
                config = mapper.readValue(getClass().getClassLoader().getResourceAsStream(configLocation), Config.class);

            if (config.kafka.bootstrapServers == null && config.kafka.host != null && config.kafka.port != null) {
                config.kafka.bootstrapServers = fetchKafkaIPs(config.kafka.host, config.kafka.port);
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }

    public static Configuration get() {
        if (instance == null) instance = new Configuration();
        return instance;
    }

    public Kafka getKafka() {
        return config.kafka;
    }

    public KafkaProducer getKafkaProducer() {
        return config.kafkaProducer;
    }

    public List<KafkaConsumer> getKafkaConsumers() {
        return config.kafkaConsumers;
    }

    private static class Config {
        public Kafka kafka;
        @JsonProperty("kafka.producer")
        public KafkaProducer kafkaProducer;
        @JsonProperty("kafka.consumers")
        public List<KafkaConsumer> kafkaConsumers;
    }
    public static class Kafka {
        @JsonProperty("bootstrap.servers")
        public String bootstrapServers;
        public String host;
        public String port;
    }
    public static class KafkaProducer {
        @JsonProperty("client.id")
        public String clientId;
        public String acks;
        public String retries;
        @JsonProperty("retry.backoff.ms")
        public String retryBackoffMs;
        @JsonProperty("reconnect.backoff.ms")
        public String reconnectBackoffMs;
        @JsonProperty("key.serializer")
        public String keySerializer;
        @JsonProperty("value.serializer")
        public String valueSerializer;
        @JsonProperty("batch.size")
        public int batchSize;
        @JsonProperty("linger.ms")
        public String lingerMs;
        @JsonProperty("publish.path")
        public String publishPath;
        public int port;
    }
    public static class KafkaConsumer {
        @JsonProperty("group.id")
        public String groupId;
        @JsonProperty("max.partition.fetch.bytes")
        public String maxPartitionFetchBytes;
        public String topics;
        @JsonProperty("enable.auto.commit")
        public String enableAutoCommit;
        @JsonProperty("key.deserializer")
        public String keyDeserializer;
        @JsonProperty("value.deserializer")
        public String valueDeserializer;
        @JsonProperty("max.poll.records")
        public String maxPollRecords;
        @JsonProperty("callback.url")
        public String callbackUrl;
        @JsonProperty("failure.topic")
        public String failureTopic;
        @JsonProperty("failure.retries")
        public int failureRetries;
        @JsonProperty("dead.topic")
        public String deadTopic;
        @JsonProperty("dead.callback.url")
        public String deadCallbackUrl;
    }

    private static String fetchKafkaIPs(String domain, String port) {

        StringBuilder sb = new StringBuilder();
        BufferedReader reader = null;
        Process p;

        try {
            p = Runtime.getRuntime().exec("dig +short "+domain);
            p.waitFor();
            reader = new BufferedReader(
                    new InputStreamReader(p.getInputStream()));

            String line = "";
            String div = "";
            while ((line = reader.readLine()) != null) {
                sb.append(div).append(line+":"+port);
                div = ",";
            }

        } catch (Exception e) {
            logger.error(e);
        } finally {

            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    logger.error(e);
                }
            }
        }

        return sb.toString();

    }
}
