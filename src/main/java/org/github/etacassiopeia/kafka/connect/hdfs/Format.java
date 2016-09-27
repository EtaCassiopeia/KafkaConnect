package org.github.etacassiopeia.kafka.connect.hdfs;

public interface Format {
    RecordWriterProvider getRecordWriterProvider();
}
