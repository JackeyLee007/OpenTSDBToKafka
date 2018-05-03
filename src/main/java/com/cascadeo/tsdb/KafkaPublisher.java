package com.cascadeo.tsdb;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.stumbleupon.async.Deferred;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SslConfigs;
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

    private LoadingCache<String, Integer> metricsLowCache;
    private LoadingCache<String, Integer> metricsMediumCache;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaPublisher.class.getName());

    public void initialize(final TSDB tsdb) {
        LOG.info("Initializing " + KafkaPublisher.class.getName());

        String kafkaConfFile = tsdb.getConfig().getString("tsd.plugin.kafkapublisher.conf");
        loadConfig(kafkaConfFile);
        initializeCache();
        intializeKafkaProducer();
    }

    public Deferred<Object> shutdown() {
        if (producer != null) {
            producer.close();
        } else {
            LOG.error("Unable to close a null producer");
        }

        return Deferred.fromResult(null);
    }

    public String version() {
        return "0.1.1";
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

        if (producer != null) {
            DataPointMessageBuilder dpMsgBuilder = new DataPointMessageBuilder(metric, timestamp, value, tags, tsuid);

            String rrdPath = dpMsgBuilder.getRRDPath();
            if (rrdPath.isEmpty()) {
                return;
            }

            if (filterMetric(metric, rrdPath)) {
                return;
            }

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

        } else {
            LOG.error("Unable to publish message as the producer is null");
        }

    }

    public void intializeKafkaProducer() {
        kafkaConfigProps = new Properties();

        kafkaConfigProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaPluginConfig.getBootstrapServers());
        setKafkaSecurity(kafkaConfigProps);

        kafkaConfigProps.put(ProducerConfig.ACKS_CONFIG, "all");
        kafkaConfigProps.put(ProducerConfig.RETRIES_CONFIG, 0);
        kafkaConfigProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaConfigProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(kafkaConfigProps);

        if (producer != null) {
            LOG.info("Producer is now available");
        } else {
            LOG.error("Unable to create producer");
        }
    }

    private void setKafkaSecurity(Properties kafkaProps) {
        String protocol = kafkaPluginConfig.getSecurityProtocol();
        String trustStoreLocation = kafkaPluginConfig.getSecurityTrustStoreLocation();
        String trustStorePassword = kafkaPluginConfig.getSecurityTrustStorePassword();
        String keyStoreLocation = kafkaPluginConfig.getSecurityKeyStoreLocation();
        String keyStorePassword = kafkaPluginConfig.getSecurityKeyStorePassword();
        String keyPassword = kafkaPluginConfig.getSecurityKeyPassword();

        if ((protocol.equalsIgnoreCase("ssl")) && !trustStoreLocation.isEmpty() && !trustStorePassword.isEmpty()
                && !keyStoreLocation.isEmpty() && !keyStorePassword.isEmpty() && !keyPassword.isEmpty()) {
            LOG.info("SSL is enabled");
            kafkaProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            kafkaProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStoreLocation);
            kafkaProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword);

            kafkaProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStoreLocation);
            kafkaProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword);
            kafkaProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
        } else {
            LOG.info("SSL is disabled");
        }
    }

    public String getKafkTopic(String rrdPath) {
        return kafkaPluginConfig.getKafkaTopic(rrdPath);
    }

    public Boolean filterMetric(String metric, String rrdPath) {
        Boolean filterMetric = false;

        MetricFrequencyType metricFrequencyType = kafkaPluginConfig.getMetricFrequencyType(metric);

        switch (metricFrequencyType) {
        case METRIC_DEFAULT:
            filterMetric = false;
            break;
        case METRIC_IGNORE:
            LOG.debug("Blocking metric: " + metric);
            filterMetric = true;
            break;
        case METRIC_LOW:
            filterMetric = lowCacheFilter(rrdPath);
            break;
        case METRIC_MEDIUM:
            filterMetric = mediumCacheFilter(rrdPath);
            break;
        default:
            break;
        }

        return filterMetric;
    }

    private Boolean lowCacheFilter(String rrdPath) {
        Boolean filterLowMetric = true;

        if (metricsLowCache.getIfPresent(rrdPath) == null) {
            metricsLowCache.put(rrdPath, 0);
            filterLowMetric = false;
        } else {
            LOG.debug("Path is sent recently: " + rrdPath);
        }

        return filterLowMetric;
    }

    private Boolean mediumCacheFilter(String rrdPath) {
        Boolean filterMediumMetric = true;

        if (metricsMediumCache.getIfPresent(rrdPath) == null) {
            metricsMediumCache.put(rrdPath, 0);
            filterMediumMetric = false;
        } else {
            LOG.debug("Path is sent recently: " + rrdPath);
        }

        return filterMediumMetric;
    }

    public void loadConfig(String configFile) {
        kafkaPluginConfig = new KafkaPluginConfig(configFile);
    }

    public void initializeCache() {
        // Create low and medium caches
        metricsLowCache = CacheBuilder.newBuilder()
                .expireAfterWrite(kafkaPluginConfig.getMetricLowRate(), TimeUnit.SECONDS)
                .build(new CacheLoader<String, Integer>() {
                    @Override
                    public Integer load(String path) throws Exception {
                        return 0;
                    }

                });

        metricsMediumCache = CacheBuilder.newBuilder()
                .expireAfterWrite(kafkaPluginConfig.getMetricMediumRate(), TimeUnit.SECONDS)
                .build(new CacheLoader<String, Integer>() {
                    @Override
                    public Integer load(String path) throws Exception {
                        return 0;
                    }
                });
    }

    private static class KafkaSendCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                LOG.error("Error while producing message to topic :" + e.getMessage());
            } else {
                String message = String.format("sent message to topic:%s partition:%s  offset:%s",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                LOG.debug(message);
            }
        }
    }
}
