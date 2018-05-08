Open TSDB Real Time Publisher for Kafka
========================

Real Time Publisher Plugin for OpenTSDB 2.3 to send metrics to a Kafka Cluster 

# Reference

The implementation of this plugin was based on OpenTsdbSkylinePublisher plugin. You can find the implementation of OpenTsdbSkylinePublisher plugin here:

https://github.com/gutefrage/OpenTsdbSkylinePublisher

# Building the plugin

## Without Dependencies 
mvn clean install

## With Dependencies (Recommended)
mvn clean compile assembly:single

## Run tests
mvn test

## Using maven docker
docker run -it --rm --name opentsdb-publisher -v "$(pwd)":/your_absolute_source_path -w /your_absolute_source_path maven:3.3-jdk-8 mvn _command_

# Usage 
Add these settings to your opentsdb.conf:
```
tsd.core.plugin_path = your_plugin_path 
tsd.rtpublisher.enable = True  
tsd.rtpublisher.plugin = com.cascadeo.tsdb.KafkaPublisher
tsd.plugin.kafkapublisher.conf = plugin_configuration_file_path
tsd.plugin.kafkapublisher.source = source_hostname
```

# Sample Plugin Configuration File
```
{
    "bootstrapServers" : [
        "kafkaServer1:9092",
        "kafkaServer2:9092",
        "kafkaServer3:9092"
    ],

    "security" : {
        "protocol" : "ssl",
        "trustStoreLocation" : "myTrustStoreLocation",
        "trustStorePassword" : "myTrustStorePassword",
        "keyStoreLocation" : "myKeyStoreLocation",
        "keyStorePassword" : "myKeyStorePassword",
        "keyPassword" : "myKeyPassword"
    },

    "kafkaTopics" : {
        "default" : "defaultTopic",
        "topics" : {
            "topic1" : ["path1", "path2"],
            "topic2" : ["path3", "path4"],
            "topic3" : ["path5", "path6", "path7"]            
        }
    },
    
    "metricsFrequency" : {
        "metrics-low-rate" : 3600,
        "metrics-medium-rate" : 900,
        "metrics-low-list" : ["metric1", "metric2"],
        "metrics-medium-list" : ["metric3", "metric4"],
        "metrics-ignore-list" : ["metric5", "metric6", "metric7"]
    }
}
```

## Note:
* _metricsFrequency.metrics-low-rate_ is in ___seconds___.
* _metricsFrequency.metrics-medium-rate_ is in ___seconds___.


