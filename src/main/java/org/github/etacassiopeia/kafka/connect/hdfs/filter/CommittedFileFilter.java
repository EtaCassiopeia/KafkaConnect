package org.github.etacassiopeia.kafka.connect.hdfs.filter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CommittedFileFilter implements PathFilter {
    private static Pattern pattern = Pattern.compile("[a-zA-Z0-9\\._\\-]+\\+\\d+\\+\\d+\\+\\d+(.\\w+)?");

    @Override
    public boolean accept(Path path) {
        String filename = path.getName();
        Matcher m = pattern.matcher(filename);
        return m.matches();
    }
}

