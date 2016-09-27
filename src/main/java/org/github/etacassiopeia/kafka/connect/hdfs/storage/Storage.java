package org.github.etacassiopeia.kafka.connect.hdfs.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.kafka.common.TopicPartition;
import org.github.etacassiopeia.kafka.connect.hdfs.wal.WAL;

import java.io.IOException;

public interface Storage {
    boolean exists(String filename) throws IOException;
    boolean mkdirs(String filename) throws IOException;
    void append(String filename, Object object) throws IOException;
    void delete(String filename) throws IOException;
    void commit(String tempFile, String committedFile) throws IOException;
    void close() throws IOException;
    WAL wal(String topicsDir, TopicPartition topicPart);
    FileStatus[] listStatus(String path, PathFilter filter) throws IOException;
    FileStatus[] listStatus(String path) throws IOException;
    String url();
    Configuration conf();
}

