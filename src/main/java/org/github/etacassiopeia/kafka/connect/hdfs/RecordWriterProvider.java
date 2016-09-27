package org.github.etacassiopeia.kafka.connect.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;

public interface RecordWriterProvider {
    String getExtension();
    RecordWriter<SinkRecord> getRecordWriter(Configuration conf, String fileName, SinkRecord record) throws IOException;
}