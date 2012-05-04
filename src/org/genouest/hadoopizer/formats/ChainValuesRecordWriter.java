package org.genouest.hadoopizer.formats;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ChainValuesRecordWriter extends RecordWriter<Text, Text> {

    DataOutputStream out;

    public ChainValuesRecordWriter(DataOutputStream out) {

        super();
        this.out = out;
    }

    @Override
    public void write(Text key, Text value) throws IOException, InterruptedException {

        out.writeChars(value.toString()); // FIXME do we need a newline between each record?
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        out.close();
    }

}
