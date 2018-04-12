package com.cascadeo.tsdb;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataPointMessageBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(DataPointMessageBuilder.class);
    private String collectorName;
    private String messageType;
    private String messageVersion;

    private Boolean showOpenTSDBData = false;

    public DataPointMessageBuilder(String name, String type, String version) {
        collectorName = name;
        messageType = type;
        messageVersion = version;
    }

    /**
     * @return the showOpenTSDBData
     */
    public Boolean getShowOpenTSDBData() {
        return showOpenTSDBData;
    }

    /**
     * @param showOpenTSDBData the showOpenTSDBData to set
     */
    public void setShowOpenTSDBData(Boolean showOpenTSDBData) {
        this.showOpenTSDBData = showOpenTSDBData;
    }

    public String generateDataPointString(final String metric, final long timestamp, final Number value,
            final Map<String, String> tags, final byte[] tsuid) {

        Map<String, Object> dataPointMsg = new HashMap<String, Object>();

        // DEBUGGING DATA
        if (getShowOpenTSDBData()) {
            Map<String, Object> debugData = new HashMap<String, Object>();
            debugData.put("metric", metric);
            debugData.put("timestamp", timestamp);
            debugData.put("value", value);
            debugData.put("tags", tags);

            StringBuilder tsuidBuilder = new StringBuilder();
            for (byte bTSUID : tsuid) {
                tsuidBuilder.append(String.format("%02X", bTSUID));
            }
            debugData.put("tsuid", tsuidBuilder.toString());

            dataPointMsg.put("opentsdb_data", debugData);
        }

        // ZENOSS TS DATA
        Map<String, Object> zenossPerfDataMsg = new HashMap<String, Object>();

        // Extract just the metric. OpenTSDB metric is this format device/metric
        String[] metricArray = metric.split("/");
        String zenossMetric = new String();

        if (metricArray.length > 0) {
            zenossMetric = metricArray[metricArray.length - 1];
            zenossPerfDataMsg.put("metric", zenossMetric.toLowerCase());
        } else {
            zenossPerfDataMsg.put("metric", metric.toLowerCase());
        }

        zenossPerfDataMsg.put("value", value);

        if (tags.containsKey("device")) {
            zenossPerfDataMsg.put("device", tags.get("device").toLowerCase());
        } else {
            zenossPerfDataMsg.put("device", "");
        }

        String zenossComponentID = new String();
        if (tags.containsKey("key")) {
            String key = tags.get("key");
            // Extract the component ID
            String[] keyArray = key.split("/");
            if (keyArray.length > 1) {
                zenossComponentID = keyArray[keyArray.length - 1];
                zenossPerfDataMsg.put("componentid", zenossComponentID.toLowerCase());
            } else {
                zenossPerfDataMsg.put("componentid", "");
            }

            // Complete the rrd path by adding the zenoss metric
            if (zenossMetric.isEmpty()) {
                zenossPerfDataMsg.put("rrdpath", key.toLowerCase());
            } else {
                String rrdpath = key + "/" + zenossMetric;
                zenossPerfDataMsg.put("rrdpath", rrdpath.toLowerCase());
            }

        } else {
            zenossPerfDataMsg.put("componentid", "");
            zenossPerfDataMsg.put("rrdpath", "");
        }

        dataPointMsg.put("zenoss_ts_data", zenossPerfDataMsg);

        // HOSTNAME DATA
        dataPointMsg.put("hostname", "");

        // SOURCE DATA
        dataPointMsg.put("source", collectorName);

        // HEADER DATA        
        // Map<String, Object> headerData = new HashMap<String, Object>();
        // headerData.put("messageVersion", messageVersion);
        // headerData.put("messageType", messageType);        

        // // ISO 8601 Date Formatter
        // TimeZone utcTZ = TimeZone.getTimeZone("UTC");
        // DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm.SSS'Z'");
        // dateFormatter.setTimeZone(utcTZ);

        // long timestamp_ms = timestamp * 1000;
        // headerData.put("indexTimestamp", dateFormatter.format(timestamp_ms));

        // long sentTime = (new Date()).getTime();
        // headerData.put("sentTimestamp", dateFormatter.format(sentTime));
        // dataPointMsg.put("header", headerData);

        // ISO 8601 Date Formatter
        TimeZone utcTZ = TimeZone.getTimeZone("UTC");
        DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm.SSS'Z'");
        dateFormatter.setTimeZone(utcTZ);

        long timestamp_ms = timestamp * 1000;
        long sentTime = (new Date()).getTime();

        dataPointMsg.put("_messageVersion", messageVersion);
        dataPointMsg.put("_messageType", messageType);
        dataPointMsg.put("_indexTimestamp", dateFormatter.format(timestamp_ms));
        dataPointMsg.put("_sentTimestamp", dateFormatter.format(sentTime));        

        if (messageType == "zenoss_ts_data") {
            float forwarderLagSeconds = (float)(sentTime - timestamp_ms) / 1000;
            dataPointMsg.put("forwarderLagSeconds", forwarderLagSeconds);
        }

        String jsonStrRet = "";

        try {
            jsonStrRet = new ObjectMapper().writeValueAsString(dataPointMsg);
        } catch (JsonProcessingException e) {
            LOG.error("Failed on the conversion of the Map to string: \n" + e.getMessage());
        }
        
        return jsonStrRet;
    }
}