package org.github.etacassiopeia.kafka.connect.hdfs.parquet;

import ir.sahab.sepehr.common.proto.metadata.schema.ProtoRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoWriteSupport;
import org.github.etacassiopeia.kafka.connect.hdfs.RecordWriter;
import org.github.etacassiopeia.kafka.connect.hdfs.RecordWriterProvider;

import java.io.IOException;

public class ParquetRecordWriterProvider implements RecordWriterProvider {

    private final static Logger logger = LogManager.getLogger(ParquetRecordWriterProvider.class);

    private final static String EXTENSION = ".parquet";

    @Override
    public String getExtension() {
        return EXTENSION;
    }

    @Override
    public RecordWriter<SinkRecord> getRecordWriter(Configuration conf, final String fileName, SinkRecord record)
            throws IOException {
        CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;

        int blockSize = 256 * 1024 * 1024;
        int pageSize = 64 * 1024;

        Path path = new Path(fileName);

        ParquetWriter.Builder parquetBuilder = new ParquetWriter.Builder(path) {
            @Override
            protected ParquetWriter.Builder self() {
                return this;
            }

            @Override
            protected WriteSupport getWriteSupport(Configuration conf) {
                return new ProtoWriteSupport<>(ProtoRecord.Log.class);
            }
        };

        try {
            final ParquetWriter<ProtoRecord.Log> writer = parquetBuilder.withPageSize(pageSize)
                    .withRowGroupSize(blockSize)
                    .withCompressionCodec(compressionCodecName)
                    .withConf(conf)
                    .build();

            return new RecordWriter<SinkRecord>() {
                @Override
                public void write(SinkRecord record) throws IOException {
                    byte[] value = (byte[]) record.value();
                    writer.write(ProtoRecord.Log.parseFrom(value));
                }

                @Override
                public void close() throws IOException {
                    writer.close();
                }
            };
        } catch (Exception exc) {
            logger.error("could not create parquet writer", exc);
        }
        
        return null;
    }
}

