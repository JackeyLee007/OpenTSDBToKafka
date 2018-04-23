package com.cascadeo.tsdb;

import static org.junit.Assert.*;

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
    public void isBlackListedReturnsFalse() {
        assertFalse(kafkaPublisher.isBlackListed("metric8"));
    }

    @Test
    public void isBlackListedReturnsTrue() {
        assertTrue(kafkaPublisher.isBlackListed("metric5"));
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