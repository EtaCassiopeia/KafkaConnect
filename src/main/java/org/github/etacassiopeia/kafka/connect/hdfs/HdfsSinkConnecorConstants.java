package org.github.etacassiopeia.kafka.connect.hdfs;

public class HdfsSinkConnecorConstants {

    public static final String COMMMITTED_FILENAME_SEPARATOR = "+";

    public static final String COMMMITTED_FILENAME_SEPARATOR_REGEX = "[\\.|\\+]";

    // +tmp is a invalid topic name, naming the tmp directory this way to avoid conflicts.
    public static final String TEMPFILE_DIRECTORY = "/+tmp/";
}

