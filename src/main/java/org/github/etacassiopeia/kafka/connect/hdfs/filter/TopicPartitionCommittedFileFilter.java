package org.github.etacassiopeia.kafka.connect.hdfs.filter;

import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.github.etacassiopeia.kafka.connect.hdfs.HdfsSinkConnecorConstants;

public class TopicPartitionCommittedFileFilter extends CommittedFileFilter {
    private TopicPartition tp;

    public TopicPartitionCommittedFileFilter(TopicPartition tp) {
        this.tp = tp;
    }

    @Override
    public boolean accept(Path path) {
        if (!super.accept(path)) {
            return false;
        }
        String filename = path.getName();
        String[] parts = filename.split(HdfsSinkConnecorConstants.COMMMITTED_FILENAME_SEPARATOR_REGEX);
        String topic = parts[0];
        int partition = Integer.parseInt(parts[1]);
        return topic.equals(tp.topic()) && partition == tp.partition();
    }
}

