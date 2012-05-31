package org.genouest.hadoopizer.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FastaOutputFormat<K, V> extends HadoopizerOutputFormat<K, V> {

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context, Path path, CompressionCodec codec) throws IOException, InterruptedException {
        
        Configuration conf = context.getConfiguration();

        FileSystem fs = path.getFileSystem(conf);

        FSDataOutputStream out = fs.create(path, true);
        if (codec == null) {
            return new FastaRecordWriter<K, V>(out);
        }
        else {
            return new FastaRecordWriter<K, V>(new DataOutputStream(codec.createOutputStream(out)));
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
