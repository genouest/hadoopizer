package org.genouest.hadoopizer.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.genouest.hadoopizer.Hadoopizer;
import org.genouest.hadoopizer.JobConfig;
import org.genouest.hadoopizer.JobInputFile;
import org.genouest.hadoopizer.SplitableJobInput;
import org.genouest.hadoopizer.io.ObjectWritableComparable;

public class MultipleInputFormat extends FileInputFormat<ObjectWritableComparable, ObjectWritable> {

    private Path headerTempFile;

    @Override
    public RecordReader<ObjectWritableComparable, ObjectWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        
        FileSplit fs = (FileSplit) split;
        Path filename = fs.getPath();
        Hadoopizer.logger.info("Looking for input format class for file: " + filename.toString());
        

        // Write data chunk to a temporary input file
        // This input file will be used in the command line launched in the cleanup step
        String xmlConfig = conf.get("hadoopizer.job.config");
        JobConfig config = new JobConfig();
        config.load(xmlConfig);
        
        SplitableJobInput splitable = (SplitableJobInput) config.getSplitableInput();
        for (JobInputFile file : splitable.getFiles()) {
            if (file.getUrl().toString().compareTo(filename.toString()) == 0) {
                HadoopizerInputFormat inf = file.getFileInputFormat();
                
                Hadoopizer.logger.info("Found an input format class: " + inf.getClass().getCanonicalName());
                return inf.createRecordReader(split, context);
            }
        }
        
        System.err.println("Could not find an input format class for: " + filename);
        System.exit(1);
        return null;
    }

    @Override
    // TODO adapt
    protected boolean isSplitable(JobContext context, Path filename) {
        
        CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(filename);
        
        return codec == null;
    }

    /**
     * Get a file where input file header can be saved
     * 
     * @param conf The conf configuration
     * @return A Path object for the temporary header file
     */
    // TODO adapt
    public Path getHeaderTempFile(Configuration conf) {
        
        if (headerTempFile == null) {
            // No special temp file defined to save header content, use the default one
            String headerFileName = conf.get("hadoopizer.temp.input.header.file"); // FIXME add input id
            if (headerFileName != null &&  !headerFileName.isEmpty())
                headerTempFile = new Path(headerFileName);
        }
        
        return headerTempFile;
    }
    
    /**
     * Set a file where input file header can be saved
     * 
     * @param headerTempFile The path where input header should be saved
     */
    // TODO adapt
    public void setHeaderTempFile(Path headerTempFile) {
        
        this.headerTempFile = headerTempFile;
    }
}
