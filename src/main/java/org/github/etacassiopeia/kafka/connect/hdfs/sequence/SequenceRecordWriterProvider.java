package org.github.etacassiopeia.kafka.connect.hdfs.sequence;

import ir.sahab.sepehr.common.proto.metadata.schema.ProtoRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.github.etacassiopeia.kafka.connect.hdfs.RecordWriter;
import org.github.etacassiopeia.kafka.connect.hdfs.RecordWriterProvider;

import java.io.IOException;

/**
 * <h1>SequenceRecordWriterProvider</h1>
 * The SequenceRecordWriterProvider
 *
 * @author Mohsen Zainalpour
 * @version 1.0
 * @since 18/08/16
 */
public class SequenceRecordWriterProvider implements RecordWriterProvider {
    private final static Logger logger = LogManager.getLogger(SequenceRecordWriterProvider.class);

    private final static String EXTENSION = ".seq";

    @Override
    public String getExtension() {
        return EXTENSION;
    }

    @Override
    public RecordWriter<SinkRecord> getRecordWriter(Configuration conf, final String fileName, SinkRecord record)
            throws IOException {

        Path path = new Path(fileName);

        try {

            SequenceFile.Writer.Option optPath = SequenceFile.Writer.file(path);
            SequenceFile.Writer.Option optKey = SequenceFile.Writer.keyClass(BytesWritable.class);
            SequenceFile.Writer.Option optValue = SequenceFile.Writer.valueClass(BytesWritable.class);
            final SequenceFile.Writer writer = SequenceFile.createWriter(conf, optPath, optKey, optValue);

            return new RecordWriter<SinkRecord>() {
                @Override
                public void write(SinkRecord record) throws IOException {
                    ProtoRecord.Log log = ProtoRecord.Log.parseFrom((byte[]) record.value());
                    BytesWritable keyBytesWritable = new BytesWritable(longToBytes(System.currentTimeMillis()));
                    BytesWritable valueBytesWritable = new BytesWritable(log.toByteArray());
                    writer.append(keyBytesWritable, valueBytesWritable);
                }

                @Override
                public void close() throws IOException {
                    writer.close();
                }
            };
        } catch (Exception exc) {
            logger.error("could not create sequence writer", exc);
        }

        return null;
    }

    private byte[] longToBytes(long l) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte) (l & 0xFF);
            l >>= 8;
        }
        return result;
    }
}
