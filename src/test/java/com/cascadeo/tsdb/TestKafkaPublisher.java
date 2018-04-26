package com.cascadeo.tsdb;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

public class TestKafkaPublisher {
    private KafkaPublisher kafkaPublisher;

    @Before
    public void initializeTest() {
        kafkaPublisher = new KafkaPublisher();
        kafkaPublisher.loadConfig("./src/test/resources/com/cascadeo/tsdb/kafkaplugin.conf");
    }

    @Test
    public void testMetricBlacklist() {
        assertTrue(kafkaPublisher.filterMetric("metric5", "/absolute/path0/metric5"));
        assertTrue(kafkaPublisher.filterMetric("metric6", "/absolute/path0/metric6"));
        assertTrue(kafkaPublisher.filterMetric("metric7", "/absolute/path0/metric7"));
    }

    @Test
    public void testMetricLowlist() {
        kafkaPublisher.initializeCache();
        try {
            assertFalse(kafkaPublisher.filterMetric("metric1", "/absolute/path0/metric1"));
            TimeUnit.SECONDS.sleep(10);
            assertTrue(kafkaPublisher.filterMetric("metric1", "/absolute/path0/metric1"));
            TimeUnit.SECONDS.sleep(10);
            assertTrue(kafkaPublisher.filterMetric("metric1", "/absolute/path0/metric1"));
            TimeUnit.SECONDS.sleep(10);
            assertFalse(kafkaPublisher.filterMetric("metric1", "/absolute/path0/metric1"));
        } catch (InterruptedException iEx) {
            fail("Exception thrown: " + iEx.getMessage());
        }
    }

    public void testMetricLowListTwoSources() {
        kafkaPublisher.initializeCache();
        try {
            assertFalse(kafkaPublisher.filterMetric("metric1", "/absolute/path0/metric1"));
            assertFalse(kafkaPublisher.filterMetric("metric1", "/absolute/path1/metric1"));
            TimeUnit.SECONDS.sleep(10);
            assertTrue(kafkaPublisher.filterMetric("metric1", "/absolute/path0/metric1"));
            assertTrue(kafkaPublisher.filterMetric("metric1", "/absolute/path1/metric1"));
            TimeUnit.SECONDS.sleep(10);
            assertTrue(kafkaPublisher.filterMetric("metric1", "/absolute/path0/metric1"));
            assertTrue(kafkaPublisher.filterMetric("metric1", "/absolute/path1/metric1"));
            TimeUnit.SECONDS.sleep(10);
            assertFalse(kafkaPublisher.filterMetric("metric1", "/absolute/path0/metric1"));
            assertFalse(kafkaPublisher.filterMetric("metric1", "/absolute/path1/metric1"));
        } catch (InterruptedException iEx) {
            fail("Exception thrown: " + iEx.getMessage());
        }
    }

    @Test
    public void testMetricMediumList() {
        kafkaPublisher.initializeCache();
        try {
            assertFalse(kafkaPublisher.filterMetric("metric3", "/absolute/path0/metric3"));
            TimeUnit.SECONDS.sleep(5);
            assertTrue(kafkaPublisher.filterMetric("metric3", "/absolute/path0/metric3"));
            TimeUnit.SECONDS.sleep(5);
            assertFalse(kafkaPublisher.filterMetric("metric3", "/absolute/path0/metric3"));
        } catch (InterruptedException iEx) {
            fail("Exception thrown: " + iEx.getMessage());
        }
    }

    @Test
    public void testMetricMediumListTwoSource() {
        kafkaPublisher.initializeCache();
        try {
            assertFalse(kafkaPublisher.filterMetric("metric3", "/absolute/path0/metric3"));
            assertFalse(kafkaPublisher.filterMetric("metric3", "/absolute/path1/metric3"));
            TimeUnit.SECONDS.sleep(5);
            assertTrue(kafkaPublisher.filterMetric("metric3", "/absolute/path0/metric3"));
            assertTrue(kafkaPublisher.filterMetric("metric3", "/absolute/path1/metric3"));
            TimeUnit.SECONDS.sleep(5);
            assertFalse(kafkaPublisher.filterMetric("metric3", "/absolute/path0/metric3"));
            assertFalse(kafkaPublisher.filterMetric("metric3", "/absolute/path1/metric3"));
        } catch (InterruptedException iEx) {
            fail("Exception thrown: " + iEx.getMessage());
        }
    }

    @Test
    public void testMetricsMixedList() {
        kafkaPublisher.initializeCache();
        try {
            assertFalse(kafkaPublisher.filterMetric("metric1", "/absolute/path0/metric1"));
            assertFalse(kafkaPublisher.filterMetric("metric3", "/absolute/path0/metric3"));
            TimeUnit.SECONDS.sleep(5);
            assertTrue(kafkaPublisher.filterMetric("metric1", "/absolute/path0/metric1"));
            assertTrue(kafkaPublisher.filterMetric("metric3", "/absolute/path0/metric3"));
            TimeUnit.SECONDS.sleep(5);
            assertTrue(kafkaPublisher.filterMetric("metric1", "/absolute/path0/metric1"));
            assertFalse(kafkaPublisher.filterMetric("metric3", "/absolute/path0/metric3"));
            TimeUnit.SECONDS.sleep(20);
            assertFalse(kafkaPublisher.filterMetric("metric1", "/absolute/path0/metric1"));
            assertFalse(kafkaPublisher.filterMetric("metric3", "/absolute/path0/metric3"));
        } catch (InterruptedException iEx) {
            fail("Exception thrown: " + iEx.getMessage());
        }
    }

    @Test
    public void getKafkaTopic() {
        assertEquals("defaultTopic", kafkaPublisher.getKafkTopic("/absolute/path0"));
    }

    @Test
    public void getKafkaTopicReturnsTopic1() {
        assertEquals("topic1", kafkaPublisher.getKafkTopic("/absolute/path1"));
        assertEquals("topic1", kafkaPublisher.getKafkTopic("/absolute/path2"));
    }
}