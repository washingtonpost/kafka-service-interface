package com.washingtonpost.kafka;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import java.lang.reflect.Field;
import java.util.stream.Stream;

import com.washingtonpost.kafka.Configuration.Kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by findleyr on 10/6/16.
 */
@RunWith(JUnitParamsRunner.class)
public class ConfigurationTest {

    @Before
    public void resetSingleton() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        Field instance = Configuration.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null, null);
    }

    @Test
    public void testConfigGood() {
        System.setProperty("config_location", "config-test-good.yml");

        final Configuration config = Configuration.get();
        
        assertEquals("0.0.0.0:9092,0.0.0.0:9002", config.getKafka().bootstrapServers);
        assertEquals("test", config.getKafkaProducer().properties.get("client.id"));
        assertEquals(8080, config.getKafkaProducer().port);
        assertEquals(2, config.getKafkaConsumers().size());
        assertEquals("testgroup1", config.getKafkaConsumers().get(0).properties.get("group.id"));
        assertEquals("testgroup2", config.getKafkaConsumers().get(1).properties.get("group.id"));
    }

    @Test
    public void shouldCallConstructorUsingConfigLocationSingleDomainInSystemProperty() {
        System.setProperty(Configuration.CONFIG_LOCATION, "config-single-broker.yml");
        
        final Kafka result = Configuration.get().getKafka();
        final String bootstrap = result.bootstrapServers;

        assertEquals("should return expected port: 9092", 9092, result.port);
        assertEquals("should return expected host", "kafka.arc2.nile.works", result.host);
        assertTrue("should contain IP:PORT 10.67.0.106:9092", bootstrap.contains("10.67.0.106:9092"));
        assertTrue("should contain IP:PORT 10.67.0.178:9092", bootstrap.contains("10.67.0.178:9092"));
        assertTrue("should contain IP:PORT 10.67.0.106:9092", bootstrap.contains("10.67.0.125:9092"));
        assertTrue("should contain IP:PORT 10.67.0.180:9092", bootstrap.contains("10.67.0.180:9092"));
        assertTrue("should contain IP:PORT 10.67.0.46:9092", bootstrap.contains("10.67.0.46:9092"));
        assertTrue("should contain IP:PORT 10.67.0.23:9092", bootstrap.contains("10.67.0.23:9092"));
    }

    @Test
    public void shouldCallConstructorUsingConfigLocationMultipleDomainInSystemProperty() {
        System.setProperty(Configuration.CONFIG_LOCATION, "config-multi-broker.yml");
        
        final Kafka kafka = Configuration.get().getKafka();

        assertEquals("should return expected port: 9094", 9094, kafka.port);
        assertEquals("should return expected host", "kafka.tgam.nile.works kafka.washpost.nile.works", kafka.host);
        assertEquals(kafka.bootstrapServers, 4, StringUtils.countMatches(kafka.bootstrapServers, String.format(":%s", kafka.port)));
    }

    private Object[] manyFalse() {
        return ArrayUtils.toArray("false", "False", "FALSE", "n", "N", "no", "NO", "No", "0");
    }
    @Test
    @Parameters(method = "manyFalse")
    public void shouldReturnFalseFromUseIp(final String value) {
        final boolean result = Configuration.get().useIp(value);

        assertFalse(String.format("should return false for %s", value), result);
    }

    private Object[] manyTrue() {
        return ArrayUtils.toArray("true", "True", "TRUE", "YES", "yes", "Yes", "y", "1", null);
    }

    @Test
    @Parameters(method = "manyTrue")
    public void shouldReturnTrueFromUseIp(final String value) {
        final boolean result = Configuration.get().useIp(value);

        assertTrue(String.format("should return true for %s", value), result);
    }

    private Object[] delimitedHosts() {
        return new Object[] {
            new String("host.1;host.2"),
            new String("host.1 host.2"),
            new String("host.1\\|host.2"),
            new String("host.1\\,host.2")
        };
    }

    @Test
    @Parameters(method = "delimitedHosts")
    public void shouldSplitHosts(final String value) {
        final Stream<String> result = Configuration.get().findHosts(value);
        final long count = result.count();

        assertEquals(String.format("should return %s elements in array", count), 2, count);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForNullHostInAppendPort() {
        Configuration.get().appendPort(null, 21);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForEmptyHostInAppendPort() {
        Configuration.get().appendPort(StringUtils.EMPTY, 21);
    }

    @Test
    public void shouldBuildSingleHostInAppendPort() {
        final String result = Configuration.get().appendPort("my.host.com", 21);

        assertEquals("should return host:port format", "my.host.com:21", result);
    }
}
