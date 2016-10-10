package com.washingtonpost.kafka;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

/**
 * Created by findleyr on 10/6/16.
 */
public class ConfigurationTest {

    @Before
    public void resetSingleton() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        Field instance = Configuration.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null, null);
    }

    @Test
    public void testConfigGood() {
        System.setProperty("config.location", "config-test-good.yml");
        org.junit.Assert.assertEquals("0.0.0.0:9092,0.0.0.0:9002", Configuration.get().getKafka().bootstrapServers);
        org.junit.Assert.assertEquals("test", Configuration.get().getKafkaProducer().properties.get("client.id"));
        org.junit.Assert.assertEquals(8080, Configuration.get().getKafkaProducer().port);
        org.junit.Assert.assertEquals(2, Configuration.get().getKafkaConsumers().size());
        org.junit.Assert.assertEquals("testgroup1", Configuration.get().getKafkaConsumers().get(0).properties.get("group.id"));
        org.junit.Assert.assertEquals("testgroup2", Configuration.get().getKafkaConsumers().get(1).properties.get("group.id"));
    }

    @Test
    public void testFetchIPs() {
        System.setProperty("config.location", "config-test-ips.yml");
        org.junit.Assert.assertEquals("216.34.181.202:9092", Configuration.get().getKafka().bootstrapServers);
        org.junit.Assert.assertEquals("test", Configuration.get().getKafkaProducer().properties.get("client.id"));
    }
}
