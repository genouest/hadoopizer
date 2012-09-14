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

public class FastaOutputFormat extends HadoopizerOutputFormat {

    @Override
    public RecordWriter<ObjectWritableComparable, ObjectWritable> getRecordWriter(TaskAttemptContext context, Path path, CompressionCodec codec) throws IOException, InterruptedException {
        
        Configuration conf = context.getConfiguration();

        FileSystem fs = path.getFileSystem(conf);

        FSDataOutputStream out = fs.create(path, true);
        if (codec == null) {
            return new FastaRecordWriter(out, context, getHeaderTempFile(conf, path));
        }
        else {
            return new FastaRecordWriter(new DataOutputStream(codec.createOutputStream(out)), context, getHeaderTempFile(conf, path));
        }
    }

    @Override
    public String getId() {
        
        return "fasta";
    }

    @Override
    public String getExtension() {
        
        return "fasta";
    }
}
