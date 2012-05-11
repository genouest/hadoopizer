package org.genouest.hadoopizer.formats;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SAMRecordWriter<K, V> extends RecordWriter<K, V> {

    DataOutputStream out;

    public SAMRecordWriter(DataOutputStream out) {

        super();
        this.out = out;
    }

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {

        String line = key.toString() + "\t" + value.toString() + "\n";
        out.write(line.getBytes());
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        out.close();
    }

}
