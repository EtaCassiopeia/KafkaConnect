package org.github.etacassiopeia.kafka.connect.hdfs.parquet;


import org.github.etacassiopeia.kafka.connect.hdfs.Format;
import org.github.etacassiopeia.kafka.connect.hdfs.RecordWriterProvider;

public class ParquetFormat implements Format {
    public RecordWriterProvider getRecordWriterProvider() {
        return new ParquetRecordWriterProvider();
    }
}