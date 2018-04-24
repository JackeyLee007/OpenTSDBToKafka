package com.cascadeo.tsdb;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaPluginConfig {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaPluginConfig.class.getName());

    private List<String> bootstrapServers;
    private Map<String, Object> kafkaTopics;

    private List<String> metricsLowList;
    private List<String> metricsMediumList;
    private List<String> metricsIgnoreList;

    public KafkaPluginConfig(String configFile) {
        loadConfig(configFile);
    }

    @SuppressWarnings("unchecked")
    public void loadConfig(String configFile) {

        try {
            ObjectMapper objectMapper = new ObjectMapper();

            Map<String, Object> kafkaConfigMap;
            kafkaConfigMap = objectMapper.readValue(new File(configFile), new TypeReference<Map<String, Object>>() {
            });

            if (kafkaConfigMap.containsKey("bootstrapServers")) {
                bootstrapServers = (List<String>) kafkaConfigMap.get("bootstrapServers");
            }

            if (kafkaConfigMap.containsKey("kafkaTopics")) {
                kafkaTopics = (Map<String, Object>) kafkaConfigMap.get("kafkaTopics");
            }

            if (kafkaConfigMap.containsKey("metricsBlackList")) {
                Map<String, Object> metricsBlacklist = (Map<String, Object>) kafkaConfigMap.get("metricsBlackList");

                if (metricsBlacklist.containsKey("metrics-low-list")) {
                    metricsLowList = (List<String>) metricsBlacklist.get("metrics-low-list");
                }

                if (metricsBlacklist.containsKey("metrics-medium-list")) {
                    metricsMediumList = (List<String>) metricsBlacklist.get("metrics-medium-list");
                }

                if (metricsBlacklist.containsKey("metrics-medium-list")) {
                    metricsIgnoreList = (List<String>) metricsBlacklist.get("metrics-ignore-list");
                }
            }

        } catch (JsonGenerationException jsonGEx) {
            LOG.error("Error occurred on JSON generation: " + jsonGEx.getMessage());
        } catch (JsonMappingException jsonMEx) {
            LOG.error("Error occurred on JSON mapping: " + jsonMEx.getMessage());
        } catch (JsonParseException jsonPEx) {
            LOG.error("Error occurred on JSON parsing: " + jsonPEx.getMessage());
        } catch (IOException ioEx) {
            LOG.error("IOException occurred: " + ioEx.getMessage());
        }
    }

    public String getDefaultTopic() {
        String defaultTopic = "";

        if (kafkaTopics.containsKey("default")) {
            defaultTopic = (String) kafkaTopics.get("default");
        }

        return defaultTopic;
    }

    @SuppressWarnings("unchecked")
    public String getKafkaTopic(String rrdPath) {
        String kafkaTopic = "";

        // Map<String, Object> kafkaTopicMap = objectMapper.convertValue(kafkaTopics.get("topics"), Map.class);
        Map<String, Object> kafkaTopicMap = (Map<String, Object>) kafkaTopics.get("topics");
        for (String topic : kafkaTopicMap.keySet()) {
            if (!kafkaTopic.isEmpty()) {
                break;
            }

            // List<String> paths = objectMapper.convertValue(kafkaTopicMap.get(topic), List.class);
            List<String> paths = (List<String>) kafkaTopicMap.get(topic);
            for (String path : paths) {
                if (rrdPath.toLowerCase().contains(path.toLowerCase())) {
                    kafkaTopic = topic;
                    break;
                }
            }
        }

        if (kafkaTopic.isEmpty()) {
            kafkaTopic = getDefaultTopic();
        }

        return kafkaTopic;
    }

    public String getBootstrapServers() {
        return String.join(",", bootstrapServers);
    }

    public Boolean isBlacklisted(String metric, String type) {
        Boolean listed = false;
        List<String> metricList;

        if (type == "low") {
            metricList = metricsLowList;
        } else if (type == "medium") {
            metricList = metricsMediumList;
        } else {
            metricList = metricsIgnoreList;
        }

        if (metricList.contains(metric)) {
            listed = true;
        }

        return listed;
    }

    public Boolean isBlacklisted(String metric) {
        return isBlacklisted(metric, "");
    }

}