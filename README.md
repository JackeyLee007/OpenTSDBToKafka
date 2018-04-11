Open TSDB Real Time Publisher for Kafka
========================

Real Time Publisher Plugin for OpenTSDB 2.3 to send metrics to a Kafka Cluster 

# Reference

The implementation of this plugin was based on OpenTsdbSkylinePublisher plugin. You can find the implementation of OpenTsdbSkylinePublisher plugin here:

https://github.com/gutefrage/OpenTsdbSkylinePublisher

# Building with Maven Docker

## Without Dependencies
docker run -it --rm --name opentsdb-publisher -v "$(pwd)":/your_absolute_source_path -w /your_absolute_source_path maven:3.3-jdk-8 mvn clean install

## With Dependencies (Recommended)
docker run -it --rm --name opentsdb-publisher -v "$(pwd)":/your_absolute_source_path -w /your_absolute_source_path maven:3.3-jdk-8 mvn clean compile assembly:single

Note: The output will be on /your_absolute_source_path/target


# Usage 

Add these settings to your opentsdb.conf:
```
tsd.core.plugin_path = your_plugin_path 
tsd.rtpublisher.enable = True  
tsd.rtpublisher.plugin = com.cascadeo.tsdb.KafkaPublisher
tsd.plugin.kafkapublisher.bootstrapservers = kafka1:9092,kafka2:9092,kafka3:9092
tsd.plugin.kafkapublisher.topic = kafka_topic
```
  



