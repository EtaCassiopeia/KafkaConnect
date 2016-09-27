package org.github.etacassiopeia.kafka.connect.hdfs.partitioner;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Map;

public class DefaultPartitioner implements Partitioner {

    @Override
    public void configure(Map<String, Object> config) {

    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        return "partition=" + String.valueOf(sinkRecord.kafkaPartition());
    }

    @Override
    public String generatePartitionedPath(String topic, String encodedPartition) {
        return topic + "/" + encodedPartition;
    }

    @Override
    public List<String> partitionFields() {
        return null;
    }
}
