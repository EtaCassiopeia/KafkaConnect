package org.github.etacassiopeia.kafka.connect.hdfs.partitioner;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Map;

/**
 * Partition incoming records, and generates directories and file names in which to store the
 * incoming records.
 */
public interface Partitioner {
    void configure(Map<String, Object> config);
    String encodePartition(SinkRecord sinkRecord);
    String generatePartitionedPath(String topic, String encodedPartition);
    List<String> partitionFields();
}
