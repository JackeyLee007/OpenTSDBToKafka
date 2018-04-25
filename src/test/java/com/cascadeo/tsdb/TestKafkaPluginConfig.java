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
    public void testGetMetricFrequencyLow() {
        assertEquals(MetricFrequencyType.METRIC_LOW, kafkaPluginConfig.getMetricFrequencyType("metric1"));
        assertEquals(MetricFrequencyType.METRIC_LOW, kafkaPluginConfig.getMetricFrequencyType("metric2"));
    }

    @Test
    public void testGetMetricFrequencyMedium() {
        assertEquals(MetricFrequencyType.METRIC_MEDIUM, kafkaPluginConfig.getMetricFrequencyType("metric3"));
        assertEquals(MetricFrequencyType.METRIC_MEDIUM, kafkaPluginConfig.getMetricFrequencyType("metric4"));
    }

    @Test
    public void testGetMetricFrequencyIgnore() {
        assertEquals(MetricFrequencyType.METRIC_IGNORE, kafkaPluginConfig.getMetricFrequencyType("metric5"));
        assertEquals(MetricFrequencyType.METRIC_IGNORE, kafkaPluginConfig.getMetricFrequencyType("metric6"));
        assertEquals(MetricFrequencyType.METRIC_IGNORE, kafkaPluginConfig.getMetricFrequencyType("metric7"));
    }

    @Test
    public void testGetMetricFrequencyDefault() {
        assertEquals(MetricFrequencyType.METRIC_DEFAULT, kafkaPluginConfig.getMetricFrequencyType("metric8"));
        assertEquals(MetricFrequencyType.METRIC_DEFAULT, kafkaPluginConfig.getMetricFrequencyType("metric9"));
    }

}