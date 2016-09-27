package org.github.etacassiopeia.kafka.connect.hdfs.partitioner;

import com.google.protobuf.InvalidProtocolBufferException;
import ir.sahab.sepehr.common.proto.metadata.schema.ProtoRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.github.etacassiopeia.kafka.connect.hdfs.parquet.ParquetRecordWriterProvider;

import java.util.List;
import java.util.Map;

/**
 * <h1>ProtocolPartitioner</h1>
 * The ProtocolPartitioner
 *
 * @author Mohsen Zainalpour
 * @version 1.0
 * @since 16/08/16
 */
public class ProtocolPartitioner implements Partitioner {

    private final static Logger logger = LogManager.getLogger(ProtocolPartitioner.class);

    @Override
    public void configure(Map<String, Object> config) {

    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        try {
            ProtoRecord.Log log = ProtoRecord.Log.parseFrom((byte[]) sinkRecord.value());
            return log.hasProtocol() ? log.getProtocol().toString() : "unknown";
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return "unknown";
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
