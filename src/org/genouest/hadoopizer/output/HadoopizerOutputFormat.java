package org.genouest.hadoopizer.output;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public abstract class HadoopizerOutputFormat<K, V> extends FileOutputFormat<K, V> { 
    
    /**
     * @return An ID identifying the type of data that can be written by the current OutputFormat
     */
    public abstract String getId();
    
    /**
     * @return The file extension (without the initial dot) for output files
     */
    public abstract String getExtension();
    
    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        
        Configuration conf = context.getConfiguration();
        
        boolean compress = getCompressOutput(context);
        CompressionCodec codec = null;
        String extension = getExtension();
        
        if (compress) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(context, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            extension += codec.getDefaultExtension();
        }

        Path path = getDefaultWorkFile(context, extension);
        
        return getRecordWriter(context, path, codec);
    }
    
    /**
     * Get the RecordWriter for the given task
     * 
     * @param context the information about the current task
     * @param path path of the file to write to
     * @param codec compression codec to use (null if no compression)
     * @return a RecordWriter to write output to a file
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract RecordWriter<K, V> getRecordWriter(TaskAttemptContext context, Path path, CompressionCodec codec) throws IOException, InterruptedException;
}
