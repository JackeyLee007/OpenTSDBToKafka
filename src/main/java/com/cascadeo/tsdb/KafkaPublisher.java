package com.cascadeo.tsdb;

import java.util.Map;
import java.util.Properties;

import com.stumbleupon.async.Deferred;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.RTPublisher;

/**
 * 
 */
public class KafkaPublisher extends RTPublisher {
    private Producer<String, String> producer;
    private Properties kafkaConfigProps;
    private KafkaPluginConfig kafkaPluginConfig;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaPublisher.class.getName());

    public void initialize(final TSDB tsdb) {
        LOG.info("Initializing " + KafkaPublisher.class.getName());

        // Load Plugin Configuration File here
        String kafkaConfFile = tsdb.getConfig().getString("tsd.plugin.kafkapublisher.conf");
        loadConfig(kafkaConfFile);
        

        kafkaConfigProps = new Properties();
        kafkaConfigProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaPluginConfig.getBootstrapServers());
        kafkaConfigProps.put(ProducerConfig.ACKS_CONFIG, "all");
        kafkaConfigProps.put(ProducerConfig.RETRIES_CONFIG, 0);
        kafkaConfigProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaConfigProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(kafkaConfigProps);
    }

    public Deferred<Object> shutdown() {
        producer.close();
        return Deferred.fromResult(null);
    }

    public String version() {
        return "0.1.0";
    }

    public void collectStats(final StatsCollector collector) {

    }

    public Deferred<Object> publishAnnotation(Annotation annotation) {
        return Deferred.fromResult(null);
    }

    public Deferred<Object> publishDataPoint(final String metric, final long timestamp, final long value,
            final Map<String, String> tags, final byte[] tsuid) {

        publishDataPointHelper(metric, timestamp, value, tags, tsuid);

        return Deferred.fromResult(null);
    }

    public Deferred<Object> publishDataPoint(final String metric, final long timestamp, final double value,
            final Map<String, String> tags, final byte[] tsuid) {

        publishDataPointHelper(metric, timestamp, value, tags, tsuid);

        return Deferred.fromResult(null);
    }

    public void publishDataPointHelper(final String metric, final long timestamp, final Number value,
            final Map<String, String> tags, final byte[] tsuid) {

        DataPointMessageBuilder dpMsgBuilder = new DataPointMessageBuilder(metric, timestamp, value, tags, tsuid);

        String rrdPath = dpMsgBuilder.getRRDPath();
        if (rrdPath.isEmpty()) {
            return;
        }

        if (isBlackListed(metric)) {
            return;
        }

        // Check frequency if we should send the metric

        String kafkaTopic = getKafkTopic(rrdPath);
        if (kafkaTopic.isEmpty()) {
            return;
        }

        // Sending
        dpMsgBuilder.setCollector("zenperfdataforwarder");
        dpMsgBuilder.setMessageType("zenoss_ts_data");
        dpMsgBuilder.setMessageVersion("2018041200");

        String dpMsgStr = dpMsgBuilder.generate();

        KafkaSendCallback kafkaSendCallback = new KafkaSendCallback();
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(kafkaTopic, dpMsgStr);
        producer.send(producerRecord, kafkaSendCallback);
    }

    public String getKafkTopic(String rrdPath) {
        return kafkaPluginConfig.getKafkaTopic(rrdPath);
    }

    public Boolean isBlackListed(String metric) {
        return kafkaPluginConfig.isBlacklisted(metric);
    }

    public void loadConfig(String configFile) {
        kafkaPluginConfig = new KafkaPluginConfig(configFile);
    }

    private static class KafkaSendCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                LOG.error("Error while producing message to topic :" + e.getMessage());
            } else {
                String message = String.format("sent message to topic:%s partition:%s  offset:%s",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                LOG.info(message);
            }
        }
    }
}
