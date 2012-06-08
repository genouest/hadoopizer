package org.genouest.hadoopizer.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.genouest.hadoopizer.io.ObjectWritableComparable;

public class FastqRecordWriter extends HadoopizerRecordWriter {

    private DataOutputStream out;
    private Path headerTempFile;
    private Configuration conf;

    public FastqRecordWriter(DataOutputStream out, TaskAttemptContext context, Path headerTempFile) {

        this.out = out;
        
        // we cannot prepend the header now because it is not filled yet
        this.headerTempFile = headerTempFile;
        this.conf = context.getConfiguration();
        
        writeHeader(headerTempFile, out, context.getConfiguration());
    }

    @Override
    public void write(ObjectWritableComparable key, ObjectWritable value) throws IOException, InterruptedException {

        if (headerTempFile != null) {
            writeHeader(headerTempFile, out, conf);
            headerTempFile = null;
        }
        
        String line = "@" + key.get().toString() + "\n";
        line += value.get().toString() + "\n";
        out.write(line.getBytes());
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        out.close();
    }

}
