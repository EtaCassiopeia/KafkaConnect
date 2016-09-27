package org.github.etacassiopeia.kafka.connect.hdfs.filter;

import org.apache.hadoop.fs.Path;
import org.github.etacassiopeia.kafka.connect.hdfs.HdfsSinkConnecorConstants;

public class TopicCommittedFileFilter extends CommittedFileFilter {
    private String topic;

    public TopicCommittedFileFilter(String topic) {
        this.topic = topic;
    }

    @Override
    public boolean accept(Path path) {
        if (!super.accept(path)) {
            return false;
        }
        String filename = path.getName();
        String[] parts = filename.split(HdfsSinkConnecorConstants.COMMMITTED_FILENAME_SEPARATOR_REGEX);
        String topic = parts[0];
        return topic.equals(this.topic);
    }
}