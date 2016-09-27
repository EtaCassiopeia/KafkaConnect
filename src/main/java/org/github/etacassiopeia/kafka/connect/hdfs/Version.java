package org.github.etacassiopeia.kafka.connect.hdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Version {
    private static final Logger log = LoggerFactory.getLogger(Version.class);
    private static String version = "unknown";

    public static String getVersion() {
        return version;
    }
}
