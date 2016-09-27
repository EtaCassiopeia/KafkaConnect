package org.github.etacassiopeia.kafka.connect.hdfs.wal;

import org.apache.kafka.connect.errors.ConnectException;

public interface WAL {
    String beginMarker = "BEGIN";
    String endMarker = "END";
    void acquireLease() throws ConnectException;
    void append(String tempFile, String committedFile) throws ConnectException;
    void apply() throws ConnectException;
    void truncate() throws ConnectException;
    void close() throws ConnectException;
    String getLogFile();
}

