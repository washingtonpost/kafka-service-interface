package com.washingtonpost.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Parse the yaml configuration file and provide the information through a singleton.
 */
public class Configuration {
    private final static Logger LOGGER = Logger.getLogger(Configuration.class);

    final static String CONFIG_LOCATION = "config.location";
    final static String CONFIG_URL = "config.url";

    final static String COMMA = ",";
    final static String COLON = ":";

    private static Configuration instance = null;
    private Config config = null;

    /**
     * Construct this class by parsing the yaml file.
     * We also fetch the bootstrap servers if needed.
     */
    private Configuration() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            String configLocation = System.getProperty("config.location");
            if (configLocation == null) {
                configLocation = System.getenv("config.location");
            }
            if (configLocation != null) {
                LOGGER.info("Using config.location "+configLocation);
                File configFile = new File(configLocation);
                if (configFile.exists()) {
                    config = mapper.readValue(configFile, Config.class);
                } else {
                    config = mapper.readValue(getClass().getClassLoader().getResourceAsStream(configLocation), Config.class);
                }
            } else {
                String configUrl = System.getenv("config.url");
                LOGGER.info("Using config.url "+configUrl);
                HttpResponse<String> response = Unirest.get(configUrl)
                        .header("accept", "text/plain")
                        .asString();
                if (response.getStatus() != 200) {
                    throw new Exception(configUrl+" returned non 200 http response.");
                }
                String body = response.getBody();
                if (body == null || body.isEmpty()) {
                    throw new Exception(configUrl+" response is empty");
                }
                LOGGER.info(body);
                config = mapper.readValue(body, Config.class);
            }

            this.useIpAddressForBootstrapServers();
            
        } catch (Exception e) {
            LOGGER.error(e);
        }
    }

    void useIpAddressForBootstrapServers() {
        if (config.kafka.bootstrapServers == null && config.kafka.host != null && config.kafka.port != 0) {
            if (this.useIp(config.kafka.useIp)) {
                config.kafka.bootstrapServers = FETCH_KAFKA_IPS(
                    this.findHosts(config.kafka.host).collect(Collectors.toList()), 
                    config.kafka.port);
            } else {
                config.kafka.bootstrapServers = this.appendPort(config.kafka.host, config.kafka.port);
            }
        }
    }

    boolean useIp(final String value) {
        return value == null || BooleanUtils.isTrue(BooleanUtils.toBooleanObject(value));
    }

    String appendPort(final String host, final int port) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(host), "kafka host is missing");

        final int portToUse = port == 9092 ? 9092 : port;

        final Stream<String> hosts = this.findHosts(host);

        return hosts.map(h -> h.concat(COLON).concat(Integer.toString(portToUse)).concat(StringUtils.SPACE))
            .collect(Collectors.joining()).trim();
    }

    Stream<String> findHosts(final String host) {
        return Splitter.on(CharMatcher.anyOf(",| ;"))
            .trimResults()
            .omitEmptyStrings()
            .splitToStream(host);
    }

    /**
     * Get the configuration instance.  Will attempt to parse the <strong>config.location</strong> yaml file on first
     * invocation.
     *
     * @return
     */
    public static Configuration get() {
        if (instance == null) {
            instance = new Configuration();
        }
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
        public int port;
        @JsonProperty("use.ip")
        public String useIp;
    }
    public static class KafkaProducer {
        @JsonProperty("publish.path")
        public String publishPath;
        public int port;
        public Map<String, String> properties;
    }
    public static class KafkaConsumer {
        public String topics;
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
        public Map<String, String> properties;
    }

    private static String FETCH_KAFKA_IPS(final List<String> domains, int port) {

        final StringBuilder sb = new StringBuilder();
        BufferedReader reader = null;
        Process p;

        try {
            for (final String host : domains) {
                p = Runtime.getRuntime().exec(String.format("dig +short %s", host));
                p.waitFor();
                reader = new BufferedReader(
                    new InputStreamReader(p.getInputStream()));
                
                String line = StringUtils.EMPTY;
                String div = sb.length() == 0 ? StringUtils.EMPTY : COMMA;
                while ((line = reader.readLine()) != null) {
                    sb.append(div).append(line).append(COLON).append(port);
                    div = COMMA;
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error reading IP address(es)", e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    LOGGER.error(e);
                }
            }
        }
        return sb.toString();
    }
}
