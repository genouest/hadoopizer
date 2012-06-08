package org.genouest.hadoopizer.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.genouest.hadoopizer.io.ObjectWritableComparable;

public class FastqOutputFormat extends HadoopizerOutputFormat {

    @Override
    public RecordWriter<ObjectWritableComparable, ObjectWritable> getRecordWriter(TaskAttemptContext context, Path path, CompressionCodec codec) throws IOException, InterruptedException {
        
        Configuration conf = context.getConfiguration();
        
        FileSystem fs = path.getFileSystem(conf);

        FSDataOutputStream out = fs.create(path, true);
        if (codec == null) {
            return new FastqRecordWriter(out, context, getHeaderTempFile(conf));
        }
        else {
            return new FastqRecordWriter(new DataOutputStream(codec.createOutputStream(out)), context, getHeaderTempFile(conf));
        }
    }
    
    @Override
    public String getId() {
        
        return "fastq";
    }

    @Override
    public String getExtension() {
        
        return "fastq";
    }
}
