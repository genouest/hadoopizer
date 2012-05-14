package org.genouest.hadoopizer.formats;

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

public class SAMOutputFormat<K, V> extends FileOutputFormat<K, V> implements HadoopizerOutputFormat {
    
    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        
        Configuration conf = context.getConfiguration();
        
        boolean compress = getCompressOutput(context);
        CompressionCodec codec = null;
        String extension = ".sam";
        
        if (compress) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(context, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            extension += codec.getDefaultExtension();
        }

        Path path = getDefaultWorkFile(context, extension);
        FileSystem fs = path.getFileSystem(conf);

        FSDataOutputStream out = fs.create(path, false);
        if (!compress) {
            return new SAMRecordWriter<K, V>(out, context);
        }
        else {
            return new SAMRecordWriter<K, V>(new DataOutputStream(codec.createOutputStream(out)), context);
        }
    }

    @Override
    public String getId() {
        
        return "sam";
    }

}
