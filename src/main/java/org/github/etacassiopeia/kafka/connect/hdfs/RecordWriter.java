package org.github.etacassiopeia.kafka.connect.hdfs;

import java.io.IOException;

public interface RecordWriter<V> {
    void write(V value) throws IOException;
    void close() throws IOException;
}