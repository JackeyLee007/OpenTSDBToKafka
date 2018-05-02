package com.cascadeo.tsdb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
    private Map<String, Object> kafkaSecurity;
    private Map<String, Object> kafkaTopics;

    private List<String> metricsLowList;
    private List<String> metricsMediumList;
    private List<String> metricsIgnoreList;
    private Integer metricsLowRate = 0;
    private Integer metricsMediumRate = 0;

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
            } else {
                bootstrapServers = new ArrayList<String>();
            }

            if (kafkaConfigMap.containsKey("security")) {
                kafkaSecurity = (Map<String, Object>) kafkaConfigMap.get("security");
            } else {
                kafkaSecurity = new HashMap<String, Object>();
            }

            if (kafkaConfigMap.containsKey("kafkaTopics")) {
                kafkaTopics = (Map<String, Object>) kafkaConfigMap.get("kafkaTopics");
            } else {
                kafkaTopics = new HashMap<String, Object>();
            }

            if (kafkaConfigMap.containsKey("metricsFrequency")) {
                Map<String, Object> metricsFrequencyMap = (Map<String, Object>) kafkaConfigMap.get("metricsFrequency");

                if (metricsFrequencyMap.containsKey("metrics-low-list")) {
                    metricsLowList = (List<String>) metricsFrequencyMap.get("metrics-low-list");
                } else {
                    metricsLowList = new ArrayList<String>();
                }

                if (metricsFrequencyMap.containsKey("metrics-medium-list")) {
                    metricsMediumList = (List<String>) metricsFrequencyMap.get("metrics-medium-list");
                } else {
                    metricsMediumList = new ArrayList<String>();
                }

                if (metricsFrequencyMap.containsKey("metrics-ignore-list")) {
                    metricsIgnoreList = (List<String>) metricsFrequencyMap.get("metrics-ignore-list");
                } else {
                    metricsIgnoreList = new ArrayList<String>();
                }

                if (metricsFrequencyMap.containsKey("metrics-low-rate")) {
                    metricsLowRate = (Integer) metricsFrequencyMap.get("metrics-low-rate");
                }

                if (metricsLowRate <= 0) {
                    metricsLowRate = 3600;
                }

                if (metricsFrequencyMap.containsKey("metrics-medium-rate")) {
                    metricsMediumRate = (Integer) metricsFrequencyMap.get("metrics-medium-rate");
                }

                if (metricsMediumRate <= 0) {
                    metricsMediumRate = 900;
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

    public String getSecurityProtocol() {
        String securityProtocol = "";

        if (kafkaSecurity.containsKey("protocol")) {
            securityProtocol = (String) kafkaSecurity.get("protocol");
        }

        return securityProtocol;
    }

    public String getSecurityTrustStoreLocation() {
        String securityTrustStoreLocation = "";

        if (kafkaSecurity.containsKey("trustStoreLocation")) {
            securityTrustStoreLocation = (String) kafkaSecurity.get("trustStoreLocation");
        }

        return securityTrustStoreLocation;
    }

    public String getSecurityTrustStorePassword() {
        String securityTrustStorePassword = "";

        if (kafkaSecurity.containsKey("trustStorePassword")) {
            securityTrustStorePassword = (String) kafkaSecurity.get("trustStorePassword");
        }

        return securityTrustStorePassword;
    }

    public String getSecurityKeyStoreLocation() {
        String securityKeyStoreLocation = "";

        if (kafkaSecurity.containsKey("keyStoreLocation")) {
            securityKeyStoreLocation = (String) kafkaSecurity.get("keyStoreLocation");
        }

        return securityKeyStoreLocation;
    }

    public String getSecurityKeyStorePassword() {
        String securityKeyStorePassword = "";

        if (kafkaSecurity.containsKey("keyStorePassword")) {
            securityKeyStorePassword = (String) kafkaSecurity.get("keyStorePassword");
        }

        return securityKeyStorePassword;
    }

    public String getSecurityKeyPassword() {
        String securityKeyPassword = "";

        if (kafkaSecurity.containsKey("keyPassword")) {
            securityKeyPassword = (String) kafkaSecurity.get("keyPassword");
        }

        return securityKeyPassword;
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

        // Map<String, Object> kafkaTopicMap =
        // objectMapper.convertValue(kafkaTopics.get("topics"), Map.class);
        Map<String, Object> kafkaTopicMap = (Map<String, Object>) kafkaTopics.get("topics");
        for (String topic : kafkaTopicMap.keySet()) {
            if (!kafkaTopic.isEmpty()) {
                break;
            }

            // List<String> paths = objectMapper.convertValue(kafkaTopicMap.get(topic),
            // List.class);
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

    public MetricFrequencyType getMetricFrequencyType(String metric) {
        MetricFrequencyType metricFreqType = MetricFrequencyType.METRIC_DEFAULT;

        if (metricsLowList.contains(metric)) {
            metricFreqType = MetricFrequencyType.METRIC_LOW;
        } else if (metricsMediumList.contains(metric)) {
            metricFreqType = MetricFrequencyType.METRIC_MEDIUM;
        } else if (metricsIgnoreList.contains(metric)) {
            metricFreqType = MetricFrequencyType.METRIC_IGNORE;
        }
        return metricFreqType;
    }

    public int getMetricLowRate() {
        return metricsLowRate;
    }

    public int getMetricMediumRate() {
        return metricsMediumRate;
    }
}