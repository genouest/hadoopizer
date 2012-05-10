package org.genouest.hadoopizer.formats;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SAMRecordWriter extends RecordWriter<Text, Text> {

    DataOutputStream out;

    public SAMRecordWriter(DataOutputStream out) {

        super();
        this.out = out;
    }

    @Override
    public void write(Text key, Text value) throws IOException, InterruptedException {

        String line = key.toString() + "\t" + value.toString() + "\n"; // FIXME this is specific to SAM!!
        out.write(line.getBytes());
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        out.close();
    }

}
