package org.genouest.hadoopizer.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SAMRecordWriter<K, V> extends RecordWriter<K, V> {

    DataOutputStream out;

    public SAMRecordWriter(DataOutputStream out, TaskAttemptContext context) {

        super();
        this.out = out;
        
        // Write the file header
        Configuration conf = context.getConfiguration();

        String headerFileName = conf.get("hadoopizer.temp.header.file");
        Path headerFile = new Path(headerFileName);
        try {
            FileSystem fs = headerFile.getFileSystem(conf);
            FSDataInputStream in = fs.open(headerFile);
            
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) > 0) {
                out.write(buffer, 0, bytesRead);
            }
            
            in.close();
        } catch (IOException e) {
            System.err.println("Failed to copy output header from " + headerFileName + " to output file");
            e.printStackTrace();
            System.exit(1);
        }
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
