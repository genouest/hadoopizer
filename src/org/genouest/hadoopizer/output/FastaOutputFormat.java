package org.genouest.hadoopizer.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class FastaOutputFormat<K, V> extends FileOutputFormat<K, V> implements HadoopizerOutputFormat { // TODO test this
    
    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        
        Configuration conf = context.getConfiguration();
        
        boolean compress = getCompressOutput(context);
        CompressionCodec codec = null;
        String extension = ".fasta";
        
        if (compress) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(context, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            extension += codec.getDefaultExtension();
        }

        Path path = getDefaultWorkFile(context, extension);
        FileSystem fs = path.getFileSystem(conf);

        FSDataOutputStream out = fs.create(path, false);
        if (!compress) {
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

}
