package org.github.etacassiopeia.kafka.connect.hdfs.sequence;

import org.github.etacassiopeia.kafka.connect.hdfs.Format;
import org.github.etacassiopeia.kafka.connect.hdfs.RecordWriterProvider;

/**
 * <h1>SequenceFormat</h1>
 * The SequenceFormat
 *
 * @author Mohsen Zainalpour
 * @version 1.0
 * @since 18/08/16
 */
public class SequenceFormat implements Format {
    public RecordWriterProvider getRecordWriterProvider() {
        return new SequenceRecordWriterProvider();
    }
}