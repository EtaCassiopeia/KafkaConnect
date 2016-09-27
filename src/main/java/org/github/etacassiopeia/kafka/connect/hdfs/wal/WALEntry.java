package org.github.etacassiopeia.kafka.connect.hdfs.wal;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WALEntry implements Writable {

    private String name;

    public WALEntry(String name) {
        this.name = name;
    }

    public WALEntry() {
        name = null;
    }

    public String getName() {
        return name;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name = Text.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, name);
    }

}

