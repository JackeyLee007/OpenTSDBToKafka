package com.cascadeo.tsdb;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class TestKafkaPluginConfig {
    private KafkaPluginConfig kafkaPluginConfig;

    @Before
    public void initialize() {
        kafkaPluginConfig = new KafkaPluginConfig("./src/test/resources/com/cascadeo/tsdb/kafkaplugin.conf");
    }

    @Test
    public void testGetBootstrapServers() {
        assertEquals("kafkaServer1:9092,kafkaServer2:9092,kafkaServer3:9092", kafkaPluginConfig.getBootstrapServers());
    }

    @Test
    public void testGetDefaultTopic() {
        assertEquals("defaultTopic", kafkaPluginConfig.getDefaultTopic());
    }

    @Test
    public void testGetKafkaTopic() {
        // topic1
        assertEquals("topic1", kafkaPluginConfig.getKafkaTopic("/absolute/path1"));
        assertEquals("topic1", kafkaPluginConfig.getKafkaTopic("/absolute/path2"));

        // topic2
        assertEquals("topic2", kafkaPluginConfig.getKafkaTopic("/absolute/path3"));
        assertEquals("topic2", kafkaPluginConfig.getKafkaTopic("/absolute/path4"));

        // topic3
        assertEquals("topic3", kafkaPluginConfig.getKafkaTopic("/absolute/path5"));
        assertEquals("topic3", kafkaPluginConfig.getKafkaTopic("/absolute/path6"));
        assertEquals("topic3", kafkaPluginConfig.getKafkaTopic("/absolute/path7"));
    }

    @Test
    public void testGetKafkaTopicDefault() {
        // Default Topic
        assertEquals("defaultTopic", kafkaPluginConfig.getKafkaTopic("/absolute/path8"));
    }

    @Test
    public void testIsBlackListedTrue() {

        // Low
        assertTrue(kafkaPluginConfig.isBlacklisted("metric1", "low"));
        assertTrue(kafkaPluginConfig.isBlacklisted("metric2", "low"));

        // Medium
        assertTrue(kafkaPluginConfig.isBlacklisted("metric3", "medium"));
        assertTrue(kafkaPluginConfig.isBlacklisted("metric4", "medium"));

        // Ignore List
        assertTrue(kafkaPluginConfig.isBlacklisted("metric5"));
        assertTrue(kafkaPluginConfig.isBlacklisted("metric6"));
        assertTrue(kafkaPluginConfig.isBlacklisted("metric7"));
    }

    @Test
    public void testIsBlackListedFalse() {

        // Low
        assertFalse(kafkaPluginConfig.isBlacklisted("metric3", "low"));
        assertFalse(kafkaPluginConfig.isBlacklisted("metric4", "low"));

        // Medium
        assertFalse(kafkaPluginConfig.isBlacklisted("metric1", "medium"));
        assertFalse(kafkaPluginConfig.isBlacklisted("metric2", "medium"));

        // Ignore List
        assertFalse(kafkaPluginConfig.isBlacklisted("metric1"));
        assertFalse(kafkaPluginConfig.isBlacklisted("metric2"));
        assertFalse(kafkaPluginConfig.isBlacklisted("metric3"));

        // Metric not on any of the list
        assertFalse(kafkaPluginConfig.isBlacklisted("metric8", "low"));
        assertFalse(kafkaPluginConfig.isBlacklisted("metric8", "medium"));
        assertFalse(kafkaPluginConfig.isBlacklisted("metric8"));

    }
    
}