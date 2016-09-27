package org.github.etacassiopeia.kafka.connect.hdfs;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class HdfsSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(HdfsSinkTask.class);
    private DataWriter hdfsWriter;

    public HdfsSinkTask() {

    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        Set<TopicPartition> assignment = context.assignment();;
        try {
            HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);
             hdfsWriter = new DataWriter(connectorConfig, context);
            recover(assignment);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start HdfsSinkConnector due to configuration error.", e);
        } catch (ConnectException e) {
            log.info("Couldn't start HdfsSinkConnector:", e);
            log.info("Shutting down HdfsSinkConnector.");
            if (hdfsWriter != null) {
                hdfsWriter.close(assignment);
                hdfsWriter.stop();
            }
        }
    }

    @Override
    public void stop() throws ConnectException {
        if (hdfsWriter != null) {
            hdfsWriter.stop();
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) throws ConnectException {
        try {
            hdfsWriter.write(records);
        } catch (ConnectException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // Do nothing as the connector manages the offset
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        hdfsWriter.open(partitions);
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        hdfsWriter.close(partitions);
    }

    private void recover(Set<TopicPartition> assignment) {
        for (TopicPartition tp: assignment) {
            hdfsWriter.recover(tp);
        }
    }
}
