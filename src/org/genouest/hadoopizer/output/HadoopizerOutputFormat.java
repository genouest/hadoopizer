package org.genouest.hadoopizer.output;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.genouest.hadoopizer.io.ObjectWritableComparable;

public abstract class HadoopizerOutputFormat extends FileOutputFormat<ObjectWritableComparable, ObjectWritable> { 

    private Path headerTempFile;
    
    /**
     * Get the ID identifying the type of data that can be written by the current OutputFormat
     * 
     * @return The id
     */
    public abstract String getId();
    
    /**
     * Get the file extension for output file
     * 
     * @return The file extension (without the initial dot) for output files
     */
    public abstract String getExtension();

    /**
     * Get a file where output file header can be saved
     * 
     * @param conf The conf configuration
     * @return A Path object for the temporary header file
     */
    public Path getHeaderTempFile(Configuration conf) {
        
        if (headerTempFile == null) {
            // No special temp file defined to save header content, use the default one
            String headerFileName = conf.get("hadoopizer.temp.output.header.file"); // FIXME add output id
            if (headerFileName != null &&  !headerFileName.isEmpty())
                headerTempFile = new Path(headerFileName);
        }
        
        return headerTempFile;
    }

    /**
     * Set a file where output file header can be saved
     * 
     * @param headerTempFile The path where output header should be saved
     */
    public void setHeaderTempFile(Path headerTempFile) {
        this.headerTempFile = headerTempFile;
    }
    
    @Override
    public RecordWriter<ObjectWritableComparable, ObjectWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        
        Configuration conf = context.getConfiguration();
        
        boolean compress = getCompressOutput(context);
        CompressionCodec codec = null;
        String extension = "." + getExtension();
        
        if (compress) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(context, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            extension += codec.getDefaultExtension();
        }

        Path path = getDefaultWorkFile(context, extension); // FIXME add id to filename to avoid overwriting when using multiple output
        
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
    public abstract RecordWriter<ObjectWritableComparable, ObjectWritable> getRecordWriter(TaskAttemptContext context, Path path, CompressionCodec codec) throws IOException, InterruptedException;
}
