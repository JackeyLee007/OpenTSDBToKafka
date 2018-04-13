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
    private String kafkaTopic;
    private DataPointMessageBuilder dataPointMessageBuilder;
    private Boolean withKeysOnly;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaPublisher.class.getName());

    public void initialize(final TSDB tsdb) {
        LOG.info("Initializing " + KafkaPublisher.class.getName());

        withKeysOnly = tsdb.getConfig().getBoolean("tsd.plugin.kafkapublisher.with_keys_only");

        kafkaTopic = tsdb.getConfig().getString("tsd.plugin.kafkapublisher.topic");
        LOG.info("Kafka Topic = " + kafkaTopic);

        kafkaConfigProps = new Properties();
        String kafkaBootStrapServers = tsdb.getConfig().getString("tsd.plugin.kafkapublisher.bootstrapservers");
        LOG.info("Kafka Servers = " + kafkaBootStrapServers);
        kafkaConfigProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServers);

        kafkaConfigProps.put(ProducerConfig.ACKS_CONFIG, "all");
        kafkaConfigProps.put(ProducerConfig.RETRIES_CONFIG, 0);
        kafkaConfigProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaConfigProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(kafkaConfigProps);

        // Message builder
        dataPointMessageBuilder = new DataPointMessageBuilder("zenperfdataforwarder", "zenoss_ts_data", "2018041200");
        // Show debugging data
        dataPointMessageBuilder.setShowOpenTSDBData(true);
    }

    public Deferred<Object> shutdown() {
        producer.close();
        return Deferred.fromResult(null);
    }

    public String version() {
        return "0.0.2";
    }

    public void collectStats(final StatsCollector collector) {

    }

    public Deferred<Object> publishAnnotation(Annotation annotation) {
        return Deferred.fromResult(null);
    }

    public Deferred<Object> publishDataPoint(final String metric, final long timestamp, final long value,
            final Map<String, String> tags, final byte[] tsuid) {

        if (withKeysOnly) {
            if (!tags.containsKey("key")) {
                return Deferred.fromResult(null);
            }
        }

        KafkaSendCallback kafkaSendCallback = new KafkaSendCallback();

        String dataPointMesgStr = dataPointMessageBuilder.generateDataPointString(metric, timestamp, value, tags,
                tsuid);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(kafkaTopic,
                dataPointMesgStr);
        producer.send(producerRecord, kafkaSendCallback);

        return Deferred.fromResult(null);
    }

    public Deferred<Object> publishDataPoint(final String metric, final long timestamp, final double value,
            final Map<String, String> tags, final byte[] tsuid) {

        if (withKeysOnly) {
            if (!tags.containsKey("key")) {
                return Deferred.fromResult(null);
            }
        }

        KafkaSendCallback kafkaSendCallback = new KafkaSendCallback();

        String dataPointMesgStr = dataPointMessageBuilder.generateDataPointString(metric, timestamp, value, tags,
                tsuid);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(kafkaTopic,
                dataPointMesgStr);
        producer.send(producerRecord, kafkaSendCallback);

        return Deferred.fromResult(null);
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
